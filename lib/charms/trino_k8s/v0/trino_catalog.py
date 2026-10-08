# Copyright 2023 Canonical Ltd.
# See LICENSE file for licensing details.

"""Library for the trino_catalog relation.

This library provides the TrinoCatalogProvider and TrinoCatalogRequirer classes that
handle the provider and the requirer sides of the trino_catalog interface.

A requirer can be related to several Trino applications. `get_trino_info()` and
`get_credentials()` without arguments read the first relation only. To read
every relation, use the per-relation accessors, which skip (and log) relations
that are incomplete, malformed or whose secret cannot be read:

```python
requirer = TrinoCatalogRequirer(self, relation_name="trino-catalog")

info_by_relation = requirer.get_all_trino_info()
credentials_by_relation = requirer.get_all_credentials()
for relation_id, info in info_by_relation.items():
    credentials = credentials_by_relation.get(relation_id)
    if not credentials:
        continue
    username, password = credentials
    connect(info["remote_app"], info["trino_url"], username, password)
```
"""

import json
import logging
from typing import Dict, List, Optional, Tuple

from ops.charm import CharmBase
from ops.framework import Object
from ops.model import ModelError, Relation, SecretNotFoundError

# The unique Charmhub library identifier, never change it
LIBID = "8855efa80c9a407991dafe157a762305"

# Increment this major API version when introducing breaking changes
LIBAPI = 0

# Increment this PATCH version before using `charmcraft publish-lib` or reset
# to 0 if you are raising the major API version
LIBPATCH = 6

logger = logging.getLogger(__name__)


class TrinoCatalog:
    """Represents a Trino catalog."""

    def __init__(self, name: str, connector: str = "", description: str = ""):
        """Initialize a TrinoCatalog.

        Args:
            name: Catalog name (e.g., "marketing", "sales")
            connector: Optional connector type (e.g., "postgresql", "mysql", "bigquery")
            description: Optional description of the catalog
        """
        self.name = name
        self.connector = connector
        self.description = description

    def to_dict(self) -> dict:
        """Convert to dictionary for serialization.

        Returns:
            Dictionary with name, connector, and description keys
        """
        return {
            "name": self.name,
            "connector": self.connector,
            "description": self.description,
        }

    @classmethod
    def from_dict(cls, data: dict) -> "TrinoCatalog":
        """Create TrinoCatalog from dictionary.

        Args:
            data: Dictionary with name, optional connector, and optional description

        Returns:
            TrinoCatalog instance
        """
        return cls(
            name=data["name"],
            connector=data.get("connector", ""),
            description=data.get("description", ""),
        )

    def __repr__(self) -> str:
        """Return a string representation for debugging.

        Returns:
            String representation of the TrinoCatalog object.
        """
        return (
            f"TrinoCatalog(name={self.name}, connector={self.connector}, "
            f"description={self.description})"
        )

    def __eq__(self, other) -> bool:
        """Compare two catalogs for equality.

        Args:
            other: Object to compare with.

        Returns:
            True if catalogs are equal, False otherwise.
        """
        if not isinstance(other, TrinoCatalog):
            return False
        return (
            self.name == other.name
            and self.connector == other.connector
            and self.description == other.description
        )


class TrinoCatalogProvider(Object):
    """Provider side of the trino_catalog relation.

    This library handles the relation lifecycle and data updates.
    The charm is responsible for providing the actual data (url, catalogs, secret).
    """

    def __init__(self, charm: CharmBase, relation_name: str = "trino-catalog"):
        """Initialize the TrinoCatalogProvider.

        Args:
            charm: The charm instance.
            relation_name: Name of the relation.
        """
        super().__init__(charm, relation_name)
        self.charm = charm
        self.relation_name = relation_name

    def update_relation_data(
        self,
        relation,
        trino_url: str,
        trino_catalogs: List[TrinoCatalog],
        trino_credentials_secret_id: str,
    ) -> bool:
        """Update relation data for a specific relation.

        Args:
            relation: The relation to update
            trino_url: Trino URL (e.g., "trino.example.com:443")
            trino_catalogs: List of TrinoCatalog objects
            trino_credentials_secret_id: Juju secret ID containing Trino users

        Returns:
            True if successful, False otherwise
        """
        logger.info("Updating trino-catalog relation %s", relation)

        if not trino_url:
            logger.debug("Trino URL not provided, skipping relation update")
            return False

        if not trino_credentials_secret_id:
            logger.debug("Trino credentials secret ID not provided, skipping relation update")
            return False

        # Get current values from databag
        current_data = relation.data[self.charm.app]
        current_url = current_data.get("trino_url")
        current_catalogs_str = current_data.get("trino_catalogs")
        current_secret_id = current_data.get("trino_credentials_secret_id")

        # Get new values
        new_url = trino_url
        try:
            new_catalogs_str = json.dumps(
                sorted(
                    [c.to_dict() for c in trino_catalogs],
                    key=lambda x: x["name"],
                )
            )
        except (TypeError, KeyError) as e:
            logger.error(
                "Failed to serialize catalogs for relation %s: %s",
                relation.id,
                str(e),
            )
            return False

        new_secret_id = trino_credentials_secret_id

        # Detect changes
        url_changed = current_url != new_url
        catalogs_changed = current_catalogs_str != new_catalogs_str
        secret_id_changed = current_secret_id != new_secret_id

        # If nothing changed, skip update
        if not (url_changed or catalogs_changed or secret_id_changed):
            logger.debug("No changes for relation %s, skipping update", relation.id)
            return True

        # Update relation databag
        relation.data[self.charm.app].update(
            {
                "trino_url": new_url,
                "trino_catalogs": new_catalogs_str,
                "trino_credentials_secret_id": new_secret_id,
            }
        )

        # Log what changed
        changes = []
        if url_changed:
            changes.append("URL")
        if catalogs_changed:
            changes.append("catalogs")
        if secret_id_changed:
            changes.append("credentials")

        logger.info(
            "Updated trino-catalog relation %s: %s changed",
            relation.id,
            ", ".join(changes),
        )
        return True


class TrinoCatalogRequirer(Object):
    """Requirer side of the trino_catalog relation."""

    def __init__(self, charm: CharmBase, relation_name: str = "trino-catalog"):
        """Initialize the TrinoCatalogRequirer.

        Args:
            charm: The charm instance.
            relation_name: Name of the relation.
        """
        super().__init__(charm, relation_name)
        self.charm = charm
        self.relation_name = relation_name

        self.framework.observe(
            charm.on[relation_name].relation_created,
            self._on_relation_created,
        )

    def _on_relation_created(self, event) -> None:
        """Publish app name so the provider can build a readable username."""
        if not self.charm.unit.is_leader():
            return
        event.relation.data[self.charm.app]["app_name"] = self.charm.app.name

    def _first_relation(self) -> Optional[Relation]:
        """Return the first relation, or None if there is none."""
        relations = self.charm.model.relations.get(self.relation_name, [])
        return relations[0] if relations else None

    def _read_relation_info(self, relation: Relation) -> Optional[dict]:
        """Read and parse the provider data of one relation.

        Args:
            relation: The relation to read.

        Returns:
            Dictionary with trino_url, trino_catalogs (List[TrinoCatalog]),
            and trino_credentials_secret_id, or None if the data is missing
            (logged at debug level) or malformed (logged at warning level).
        """
        if not relation.app:
            logger.debug(
                "Relation %s skipped: remote application not available",
                relation.id,
            )
            return None

        relation_data = relation.data[relation.app]

        trino_url = relation_data.get("trino_url")
        trino_catalogs_str = relation_data.get("trino_catalogs")
        trino_credentials_secret_id = relation_data.get("trino_credentials_secret_id")

        missing = [
            name
            for name, value in (
                ("trino_url", trino_url),
                ("trino_catalogs", trino_catalogs_str),
                ("trino_credentials_secret_id", trino_credentials_secret_id),
            )
            if not value
        ]
        if missing:
            logger.debug(
                "Relation %s (%s) skipped: data not yet published: %s",
                relation.id,
                relation.app.name,
                ", ".join(missing),
            )
            return None

        try:
            catalogs_list = json.loads(trino_catalogs_str)
            trino_catalogs = [TrinoCatalog.from_dict(c) for c in catalogs_list]
        except (json.JSONDecodeError, KeyError, TypeError) as e:
            logger.warning(
                "Relation %s (%s) skipped: malformed trino_catalogs: %r",
                relation.id,
                relation.app.name,
                e,
            )
            return None

        return {
            "trino_url": trino_url,
            "trino_catalogs": trino_catalogs,
            "trino_credentials_secret_id": trino_credentials_secret_id,
        }

    @staticmethod
    def _with_origin(info: dict, relation: Relation) -> dict:
        """Return info extended with the relation ID and remote app name."""
        return {
            **info,
            "relation_id": relation.id,
            "remote_app": relation.app.name,
        }

    def get_trino_info(self, relation: Optional[Relation] = None) -> Optional[dict]:
        """Get current Trino connection information.

        Args:
            relation: The relation to read. Defaults to the first relation, in
                which case the result only has the three keys below. When a
                relation is given, the result also has relation_id and
                remote_app.

        Returns:
            Dictionary with trino_url, trino_catalogs (List[TrinoCatalog]),
            and trino_credentials_secret_id, or None if not available.
        """
        if relation is None:
            relation = self._first_relation()
            if relation is None:
                return None
            return self._read_relation_info(relation)

        info = self._read_relation_info(relation)
        return self._with_origin(info, relation) if info else None

    def get_all_trino_info(self) -> Dict[int, dict]:
        """Get Trino connection information for every complete relation.

        Returns:
            Mapping of relation ID to a dictionary with trino_url,
            trino_catalogs (List[TrinoCatalog]), trino_credentials_secret_id,
            relation_id and remote_app. Incomplete or malformed relations are
            left out.
        """
        all_info = {}
        for relation in self.charm.model.relations.get(self.relation_name, []):
            info = self._read_relation_info(relation)
            if info:
                all_info[relation.id] = self._with_origin(info, relation)
        return all_info

    def get_credentials(self, relation: Optional[Relation] = None) -> Optional[tuple]:
        """Get Trino credentials from the per-relation secret.

        Args:
            relation: The relation to read. Defaults to the first relation.

        Returns:
            Tuple of (username, password) or None if not available.

        Raises:
            SecretNotFoundError: If the secret does not exist.
            ModelError: If permission is denied to access the secret.
        """
        if relation is None:
            relation = self._first_relation()
            if relation is None:
                return None

        trino_info = self._read_relation_info(relation)
        if not trino_info:
            return None

        try:
            secret = self.charm.model.get_secret(id=trino_info["trino_credentials_secret_id"])
            credentials = secret.get_content(refresh=True)
        except SecretNotFoundError:
            logger.error(
                "Secret '%s' not found.",
                trino_info["trino_credentials_secret_id"],
            )
            raise
        except ModelError as e:
            logger.error(
                "Failed to access secret '%s': %s",
                trino_info["trino_credentials_secret_id"],
                str(e),
            )
            raise

        username = credentials.get("username")
        password = credentials.get("password")
        if not username or not password:
            logger.error("Secret missing username or password fields.")
            return None

        return (username, password)

    def get_all_credentials(self) -> Dict[int, Tuple[str, str]]:
        """Get Trino credentials for every relation that has readable ones.

        Relations with incomplete data, an unreadable secret or missing
        credential fields are logged and left out.

        Returns:
            Mapping of relation ID to (username, password).
        """
        all_credentials = {}
        for relation in self.charm.model.relations.get(self.relation_name, []):
            try:
                credentials = self.get_credentials(relation)
            except (SecretNotFoundError, ModelError):
                logger.warning(
                    "Relation %s skipped: credentials secret not readable",
                    relation.id,
                )
                continue
            if credentials:
                all_credentials[relation.id] = credentials
        return all_credentials
