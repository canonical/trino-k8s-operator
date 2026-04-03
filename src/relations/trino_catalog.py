# Copyright 2023 Canonical Ltd.
# See LICENSE file for licensing details.

"""Trino Catalog relation handler.

Manages the trino-catalog relation for the Trino charm.
Parses catalog configuration and shares structured catalog information.
"""

import logging
from typing import List, Optional

import yaml
from charms.trino_k8s.v0.trino_catalog import (
    TrinoCatalog,
    TrinoCatalogProvider,
)
from ops.charm import CharmBase
from ops.framework import Object
from ops.model import SecretNotFoundError

from literals import (
    POSTGRESQL_RELATION_NAME,
    TRINO_CATALOG_SECRET_PREFIX,
    TRINO_PORTS,
)
from utils import generate_password

logger = logging.getLogger(__name__)


class TrinoCatalogRelationHandler(Object):
    """Handles the trino-catalog relation for the Trino charm.

    Parses catalog-config to extract structured catalog information
    and shares it with requirers via the trino-catalog relation.

    Creates per-relation users and app-owned secrets so credential
    sharing works across model boundaries (CMR).
    """

    def __init__(
        self, charm: CharmBase, relation_name: str = "trino-catalog"
    ) -> None:
        """Construct.

        Args:
            charm: The charm to attach the hooks to.
            relation_name: The name of the relation.
        """
        super().__init__(charm, "trino-catalog-provider")
        self.charm = charm
        self.relation_name = relation_name

        # Initialize the provider library
        self.provider = TrinoCatalogProvider(self.charm)

        # Observe relation events
        self.framework.observe(
            charm.on[self.relation_name].relation_created,
            self._on_relation_created,
        )
        self.framework.observe(
            charm.on[self.relation_name].relation_broken,
            self._on_relation_broken,
        )

    def _on_relation_created(self, event):
        """Handle trino-catalog relation created."""
        if not self.charm.state.is_ready():
            event.defer()
            return

        self.reconcile_trino_catalog_relations()

    def _on_relation_broken(self, event):
        """Handle trino-catalog relation broken: clean up the per-relation secret."""
        label = f"{TRINO_CATALOG_SECRET_PREFIX}{event.relation.id}"
        try:
            secret = self.charm.model.get_secret(label=label)
            secret.remove_all_revisions()
            logger.info(
                "Removed secret for broken relation %s", event.relation.id
            )
        except SecretNotFoundError:
            pass

        self.charm._update_password_db_and_restart()

    def _get_url(self) -> Optional[str]:
        """Get the Trino URL from configuration.

        Returns:
            Trino URL or None if not configured
        """
        # Use external hostname with HTTPS port when nginx ingress is related
        nginx_relation = self.charm.model.get_relation("nginx-route")
        if nginx_relation:
            external_hostname = self.charm.config.get("external-hostname")
            if not external_hostname:
                return None
            port = TRINO_PORTS["HTTPS"]
            return f"{external_hostname}:{port}"

        # Use internal service URL with HTTP port when no nginx ingress
        host = self.charm.app.name
        port = TRINO_PORTS["HTTP"]
        namespace = self.charm.model.name
        return f"{host}.{namespace}.svc.cluster.local:{port}"

    def _get_catalogs(self) -> List[TrinoCatalog]:
        """Get structured catalog information from catalog-config.

        Parses the catalog-config YAML to extract catalog name, connector type,
        and description for each configured catalog. Also includes PostgreSQL
        dynamic catalogs from postgresql-catalog-config if the relation exists.

        Returns:
            List of TrinoCatalog objects
        """
        catalog_config_str = self.charm.config.get("catalog-config", "")

        if not catalog_config_str:
            logger.debug("No catalog-config set")
            catalogs = []
        else:
            catalogs = self._get_static_catalogs(catalog_config_str)

        # Include PostgreSQL dynamic catalogs if the relation exists
        catalogs.extend(self._get_postgresql_catalogs())

        return catalogs

    def _get_static_catalogs(self, catalog_config_str) -> List[TrinoCatalog]:
        """Parse catalog-config YAML into TrinoCatalog objects.

        Args:
            catalog_config_str: The raw catalog-config YAML string.

        Returns:
            List of TrinoCatalog objects
        """
        try:
            config = yaml.safe_load(catalog_config_str)
        except yaml.YAMLError as e:
            logger.error("Failed to parse catalog-config YAML: %s", str(e))
            return []

        catalogs_dict = config.get("catalogs", {})
        backends_dict = config.get("backends", {})

        if not catalogs_dict:
            logger.debug("No catalogs found in catalog-config")
            return []

        # Build structured catalog list
        catalog_list = []

        for catalog_name, catalog_config in catalogs_dict.items():
            # Get backend name from catalog config
            backend_name = catalog_config.get("backend")

            # Connector is optional - try to get it from backend
            connector = ""
            if backend_name:
                backend_config = backends_dict.get(backend_name)
                if backend_config:
                    connector = backend_config.get("connector", "")

            # Create TrinoCatalog object
            catalog = TrinoCatalog(
                name=catalog_name,
                connector=connector,
                description="",
            )

            catalog_list.append(catalog)

        logger.debug("Parsed %s catalogs from config", len(catalog_list))

        return catalog_list

    def _get_postgresql_catalogs(self) -> List[TrinoCatalog]:
        """Get catalog names from postgresql-catalog-config.

        Only included when the postgresql relation exists.

        Returns:
            List of TrinoCatalog objects for dynamic PG catalogs.
        """
        if not self.charm.model.relations.get(POSTGRESQL_RELATION_NAME):
            return []

        raw = self.charm.config.get("postgresql-catalog-config")
        if not raw:
            return []

        try:
            config = yaml.safe_load(raw)
        except yaml.YAMLError:
            return []

        if not isinstance(config, dict):
            return []

        catalogs = []
        for entry in config.values():
            if not isinstance(entry, dict):
                continue
            for key in ("ro_catalog_name", "rw_catalog_name"):
                name = entry.get(key)
                if name:
                    catalogs.append(
                        TrinoCatalog(name=name, connector="postgresql")
                    )

        return catalogs

    def _get_relation_secret(self, relation):
        """Get or create a per-relation user and app-owned secret.

        Args:
            relation: The Juju relation.

        Returns:
            Tuple of (secret, created) where created is True if a new secret
            was made. Returns (None, False) if creation failed.
        """
        label = f"{TRINO_CATALOG_SECRET_PREFIX}{relation.id}"

        # Try to find existing secret by label
        try:
            secret = self.charm.model.get_secret(label=label)
            secret.grant(relation)
            return secret, False
        except SecretNotFoundError:
            pass

        # Create new user and secret
        if relation.app is None:
            return None, False

        app_name = relation.data[relation.app].get("app_name")
        if not app_name:
            logger.debug(
                "Relation %s: app_name not yet available, deferring secret creation",
                relation.id,
            )
            return None, False

        # Create secret with the readable username
        username = f"app-{app_name}-{relation.id}"
        password = generate_password()
        secret = self.charm.app.add_secret(
            {"username": username, "password": password},
            label=label,
        )
        secret.grant(relation)

        logger.info(
            "Created per-relation user %r for relation %s",
            username,
            relation.id,
        )
        return secret, True

    def _update_relation(self, event) -> None:
        """Update a specific trino-catalog relation.

        Creates a per-relation secret if needed, then updates the databag.

        Args:
            event: The relation created event.
        """
        if not self.charm.state.is_ready():
            event.defer()
            return

        if not self.charm.unit.is_leader():
            return

        # Get Trino URL
        url = self._get_url()
        if not url:
            logger.debug("Trino external-hostname not configured")
            return

        # Get structured catalog information
        catalogs = self._get_catalogs()

        # Get per-relation user and secret
        secret, _created = self._get_relation_secret(event.relation)
        if secret is None:
            return

        # Update relation via library
        self.provider.update_relation_data(
            relation=event.relation,
            trino_url=url,
            trino_catalogs=catalogs,
            trino_credentials_secret_id=secret.id,
        )

    def reconcile_trino_catalog_relations(self) -> None:
        """Reconcile all trino-catalog relations.

        Creates per-relation users and app-owned secrets, then updates
        each relation databag with current URL, catalogs, and secret ID.
        If new users were created, triggers password.db update and restart.
        """
        if not self.charm.state.is_ready():
            return

        if not self.charm.unit.is_leader():
            return

        # Get Trino URL
        url = self._get_url()
        if not url:
            logger.debug("Trino URL not available, skipping reconciliation")
            return

        # Get structured catalog information
        catalogs = self._get_catalogs()

        users_changed = False

        for relation in self.charm.model.relations.get(self.relation_name, []):
            # Get per-relation user and secret
            secret, created = self._get_relation_secret(relation)
            if secret is None:
                continue

            if created:
                users_changed = True

            # Update relation databag
            self.provider.update_relation_data(
                relation=relation,
                trino_url=url,
                trino_catalogs=catalogs,
                trino_credentials_secret_id=secret.id,
            )

        if users_changed:
            self.charm._update_password_db_and_restart()

    def get_relation_credentials(self) -> dict:
        """Get credentials for all per-relation users.

        Returns:
            Dict mapping username to password for all active relations.
        """
        credentials = {}
        for relation in self.charm.model.relations.get(self.relation_name, []):
            label = f"{TRINO_CATALOG_SECRET_PREFIX}{relation.id}"
            try:
                secret = self.charm.model.get_secret(label=label)
                content = secret.get_content(refresh=True)
                credentials[content["username"]] = content["password"]
            except SecretNotFoundError:
                logger.debug("No secret found for relation %s", relation.id)
        return credentials
