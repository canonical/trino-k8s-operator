#!/usr/bin/env python3
# Copyright 2025 Canonical Ltd.
# See LICENSE file for licensing details.

"""Library for the trino_catalog relation.

This library provides the TrinoCatalogProvider and TrinoCatalogRequirer classes that
handle the provider and the requirer sides of the trino_catalog interface.
"""

import json
import logging
from typing import List, Optional

from ops.charm import CharmBase, RelationChangedEvent, SecretChangedEvent
from ops.framework import EventBase, EventSource, Object, ObjectEvents

logger = logging.getLogger(__name__)

# Increment this PATCH version before using `charmcraft publish-lib`
# or reset to 0 if you are raising the major API version
LIBID = "26bd3a191e5840aa98864bd5d3564e9f"
LIBAPI = 0
LIBPATCH = 1


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
        """String representation for debugging."""
        return f"TrinoCatalog(name={self.name}, connector={self.connector}, description={self.description})"

    def __eq__(self, other) -> bool:
        """Compare two catalogs for equality."""
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

    Note: The credentials secret is model-owned and must be manually granted
    to each requirer application using: juju grant-secret <secret> <requirer-app>
    """

    def __init__(self, charm: CharmBase, relation_name: str = "trino-catalog"):
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
        if not trino_url:
            logger.warning("Trino URL not provided, skipping relation update")
            return False

        if not trino_credentials_secret_id:
            logger.warning(
                "Trino credentials secret ID not provided, skipping relation update"
            )
            return False

        try:
            # Get current values from databag
            current_data = relation.data[self.charm.app]
            current_url = current_data.get("trino_url")
            current_catalogs_str = current_data.get("trino_catalogs")
            current_secret_id = current_data.get("trino_credentials_secret_id")

            # Get new values
            new_url = trino_url
            new_catalogs_str = json.dumps(
                sorted(
                    [c.to_dict() for c in trino_catalogs],
                    key=lambda x: x["name"],
                )
            )
            new_secret_id = trino_credentials_secret_id

            # Detect changes
            url_changed = current_url != new_url
            catalogs_changed = current_catalogs_str != new_catalogs_str
            secret_id_changed = current_secret_id != new_secret_id

            # If nothing changed, skip update
            if not (
                url_changed
                or catalogs_changed
                or secret_id_changed
            ):
                logger.debug(
                    "No changes for relation %s, skipping update", relation.id
                )
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

        except Exception as e:
            logger.error(
                "Failed to update relation %s: %s", relation.id, str(e)
            )
            return False

    def update_all_relations(
        self,
        trino_url: str,
        trino_catalogs: List[TrinoCatalog],
        trino_credentials_secret_id: str,
    ) -> None:
        """Update all trino-catalog relations with the provided data.

        Args:
            trino_url: Trino URL
            trino_catalogs: List of TrinoCatalog objects
            trino_credentials_secret_id: Juju secret ID containing Trino users
        """
        logger.debug("INTERFACE UPDATE ALL RELATIONS")
        for relation in self.charm.model.relations.get(self.relation_name, []):
            self.update_relation_data(
                relation=relation,
                trino_url=trino_url,
                trino_catalogs=trino_catalogs,
                trino_credentials_secret_id=trino_credentials_secret_id,
            )

    def update_secret_data(self, secret_id: str) -> None:
        """Update secret for all relations using the given secret_id.

        When the secret changes, updating the databag triggers
        relation-changed on the requirer side, which emits credentials_changed event.

        Args:
            secret_id: The Juju secret ID to match
        """

        for relation in self.charm.model.relations.get(self.relation_name, []):
            relation_data = relation.data[self.charm.app]
            stored_secret_id = relation_data.get("trino_credentials_secret_id")

            if stored_secret_id == secret_id:
                logger.debug(
                    "Updating secret for relation %s",
                    relation.id,
                )
                # Changing databag values triggers relation-changed on
                # requirer side which then emits credentials-changed event
                relation_data.update(
                    {"trino_credentials_secret_id": stored_secret_id}
                )


class TrinoCatalogsChangedEvent(EventBase):
    """Event emitted when Trino catalogs change."""

    def __init__(
        self, handle, trino_catalogs: List[TrinoCatalog], relation_id: int
    ):
        super().__init__(handle)
        self.trino_catalogs = trino_catalogs
        self.relation_id = relation_id

    def snapshot(self):
        """Save event data."""
        return {
            "trino_catalogs": [c.to_dict() for c in self.trino_catalogs],
            "relation_id": self.relation_id,
        }

    def restore(self, snapshot):
        """Restore event data."""
        self.trino_catalogs = [
            TrinoCatalog.from_dict(c) for c in snapshot["trino_catalogs"]
        ]
        self.relation_id = snapshot["relation_id"]


class TrinoUrlChangedEvent(EventBase):
    """Event emitted when Trino URL changes."""

    def __init__(self, handle, trino_url: str, relation_id: int):
        super().__init__(handle)
        self.trino_url = trino_url
        self.relation_id = relation_id

    def snapshot(self):
        """Save event data."""
        return {
            "trino_url": self.trino_url,
            "relation_id": self.relation_id,
        }

    def restore(self, snapshot):
        """Restore event data."""
        self.trino_url = snapshot["trino_url"]
        self.relation_id = snapshot["relation_id"]


class TrinoCredentialsChangedEvent(EventBase):
    """Event emitted when Trino credentials change (ID or content)."""

    def __init__(
        self,
        handle,
        trino_credentials_secret_id: str,
        relation_id: int,
    ):
        super().__init__(handle)
        self.trino_credentials_secret_id = trino_credentials_secret_id
        self.relation_id = relation_id

    def snapshot(self):
        """Save event data."""
        return {
            "trino_credentials_secret_id": self.trino_credentials_secret_id,
            "relation_id": self.relation_id,
        }

    def restore(self, snapshot):
        """Restore event data."""
        self.trino_credentials_secret_id = snapshot[
            "trino_credentials_secret_id"
        ]
        self.relation_id = snapshot["relation_id"]


class TrinoCatalogRequirerEvents(ObjectEvents):
    """Events for TrinoCatalogRequirer."""

    catalogs_changed = EventSource(TrinoCatalogsChangedEvent)
    url_changed = EventSource(TrinoUrlChangedEvent)
    credentials_changed = EventSource(TrinoCredentialsChangedEvent)


class TrinoCatalogRequirer(Object):
    """Requirer side of the trino_catalog relation."""

    on = TrinoCatalogRequirerEvents()

    def __init__(self, charm: CharmBase, relation_name: str = "trino-catalog"):
        super().__init__(charm, relation_name)
        self.charm = charm
        self.relation_name = relation_name

        # Store previous values to detect changes
        self._previous_url = None
        self._previous_catalogs = None
        self._previous_secret_id = None

        self.framework.observe(
            self.charm.on[relation_name].relation_changed,
            self._on_relation_changed,
        )

    def _on_relation_changed(self, event: RelationChangedEvent) -> None:
        """Handle relation changed event.

        Compares current values with previous values and emits
        specific events only for actual changes.
        """
        if not event.relation.app:
            return

        relation_data = event.relation.data[event.relation.app]

        # Get current values
        trino_url = relation_data.get("trino_url")
        trino_catalogs_str = relation_data.get("trino_catalogs")
        trino_credentials_secret_id = relation_data.get(
            "trino_credentials_secret_id"
        )

        if not all(
            [trino_url, trino_catalogs_str, trino_credentials_secret_id]
        ):
            logger.debug("Not all required data available yet")
            return

        # Parse catalogs into TrinoCatalog objects
        try:
            catalogs_list = json.loads(trino_catalogs_str)
            trino_catalogs = [TrinoCatalog.from_dict(c) for c in catalogs_list]
        except (json.JSONDecodeError, KeyError) as e:
            logger.error("Failed to parse trino_catalogs: %s", str(e))
            return

        # Detect changes and emit specific events

        # URL changed
        if self._previous_url != trino_url:
            logger.info("Trino URL changed: %s", trino_url)
            self.on.url_changed.emit(
                trino_url=trino_url,
                relation_id=event.relation.id,
            )
            self._previous_url = trino_url

        # Catalogs changed
        if self._previous_catalogs != trino_catalogs_str:
            logger.info("Trino catalogs changed")
            self.on.catalogs_changed.emit(
                trino_catalogs=trino_catalogs,
                relation_id=event.relation.id,
            )
            self._previous_catalogs = trino_catalogs_str

        # Credentials changed
        credentials_changed = (
            self._previous_secret_id != trino_credentials_secret_id
        )

        if credentials_changed:
            logger.info("Trino credentials changed")
            self.on.credentials_changed.emit(
                trino_credentials_secret_id=trino_credentials_secret_id,
                relation_id=event.relation.id,
            )
            self._previous_secret_id = trino_credentials_secret_id

    def get_trino_info(self) -> Optional[dict]:
        """Get current Trino connection information.

        Returns:
            Dictionary with trino_url, trino_catalogs (List[TrinoCatalog]),
            and trino_credentials_secret_id, or None if not available.
        """
        relations = self.charm.model.relations.get(self.relation_name, [])
        if not relations:
            return None

        relation = relations[0]
        if not relation.app:
            return None

        relation_data = relation.data[relation.app]

        trino_url = relation_data.get("trino_url")
        trino_catalogs_str = relation_data.get("trino_catalogs")
        trino_credentials_secret_id = relation_data.get(
            "trino_credentials_secret_id"
        )

        if not all(
            [trino_url, trino_catalogs_str, trino_credentials_secret_id]
        ):
            return None

        try:
            catalogs_list = json.loads(trino_catalogs_str)
            trino_catalogs = [TrinoCatalog.from_dict(c) for c in catalogs_list]
        except (json.JSONDecodeError, KeyError):
            return None

        return {
            "trino_url": trino_url,
            "trino_catalogs": trino_catalogs,
            "trino_credentials_secret_id": trino_credentials_secret_id,
        }

    def get_credentials(self) -> Optional[tuple]:
        """Get Trino credentials to use from the secret.

        Returns the first user credentials for simplicity. This can be
        extended with custom logic if needed to select different users.

        Note: The requirer application must be granted access to the secret
        using: juju grant-secret <secret> <requirer-app>

        Returns:
            Tuple of (username, password) or None if not available.
        """
        trino_info = self.get_trino_info()
        if not trino_info:
            return None

        try:
            secret = self.charm.model.get_secret(
                id=trino_info["trino_credentials_secret_id"]
            )
            credentials = secret.get_content()
            users_data = credentials.get("users", "")

            # Parse "user: password" format
            users = {}
            for line in users_data.strip().split("\n"):
                if ":" in line:
                    username, password = line.split(":", 1)
                    users[username.strip()] = password.strip()

            if not users:
                return None

            # Return first user - can be extended with different logic
            first_username = next(iter(users.keys()))
            return (first_username, users[first_username])

        except Exception as e:
            logger.error("Failed to get credentials: %s", str(e))
            return None
