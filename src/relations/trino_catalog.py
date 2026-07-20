# Copyright 2023 Canonical Ltd.
# See LICENSE file for licensing details.

"""Trino Catalog relation handler.

Manages the trino-catalog relation for the Trino charm.
Parses catalog configuration and shares structured catalog information.
"""

import logging
from typing import List, Optional
from urllib.parse import urlparse

import yaml
from charms.trino_k8s.v0.trino_catalog import (
    TrinoCatalog,
    TrinoCatalogProvider,
)
from ops.charm import CharmBase
from ops.framework import Object
from ops.model import SecretNotFoundError
from pydantic import ValidationError

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

    def __init__(self, charm: CharmBase, relation_name: str = "trino-catalog") -> None:
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

    @staticmethod
    def _secret_label(relation_id: int) -> str:
        """Build the label of the secret for the relation."""
        return f"{TRINO_CATALOG_SECRET_PREFIX}{relation_id}"

    def remove_relation_secret(self, relation_id: int) -> None:
        """Remove the per-relation secret for a departing trino-catalog relation.

        Args:
            relation_id: The ID of the relation being removed.
        """
        if not self.charm.unit.is_leader():
            return
        label = self._secret_label(relation_id)
        try:
            secret = self.charm.model.get_secret(label=label)
            secret.remove_all_revisions()
            logger.info("Removed secret for departed relation %s", relation_id)
        except SecretNotFoundError:
            pass

    def _get_url(self) -> Optional[str]:
        """Get the Trino URL from the stored ingress URL or fall back to the internal service URL.

        Returns:
            Trino URL as `host:port`, or None if ingress is related but not yet ready.
        """
        ingress_url = self.charm.ingress.url
        if ingress_url:
            parsed = urlparse(ingress_url)
            if parsed.path.strip("/"):
                logger.warning(
                    "Ingress URL '%s' contains a path prefix '%s'. "
                    "Only host-based (subdomain) routing is supported; "
                    "the path will be dropped from the advertised trino-catalog URL.",
                    ingress_url,
                    parsed.path,
                )
            port = parsed.port or (443 if parsed.scheme == "https" else 80)
            return f"{parsed.hostname}:{port}"

        # Fall back to the internal Kubernetes service URL when no ingress is related.
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
        catalog_config_str = self.charm.config.catalog_config or ""

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

        raw = self.charm.config.postgresql_catalog_config
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
                    catalogs.append(TrinoCatalog(name=name, connector="postgresql"))

        return catalogs

    def _get_relation_secret(self, relation):
        """Get or create a per-relation user and app-owned secret.

        Args:
            relation: The Juju relation.

        Returns:
            Tuple of (secret, created) where created is True if a new secret
            was made. Returns (None, False) if creation failed.
        """
        label = self._secret_label(relation.id)

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

    def _parse_exclusions(self) -> dict:
        """Parse the catalog-exclusions config.

        Returns:
            Dict mapping app_name to set of excluded catalog names.
            Empty dict if config is unset or empty.
        """
        raw = self.charm.config.catalog_exclusions
        if not raw:
            return {}
        try:
            parsed = yaml.safe_load(raw)
        except yaml.YAMLError:
            return {}
        if not isinstance(parsed, dict):
            return {}
        return {
            app_name: set(catalogs)
            for app_name, catalogs in parsed.items()
            if isinstance(catalogs, list)
        }

    def reconcile_trino_catalog_relations(self) -> None:
        """Reconcile all trino-catalog relations.

        Creates per-relation users and app-owned secrets, updates each relation
        databag with current URL, catalogs, and secret ID. Secrets for departed
        relations are removed by the relation-broken handler. The charm
        reconciler rebuilds password.db from the resulting credentials, so no
        restart is triggered here.
        """
        if not self.charm.state.is_ready():
            return

        if not self.charm.unit.is_leader():
            return

        try:
            _ = self.charm.config
        except ValidationError:
            logger.warning("Skipping trino catalog reconciliation: charm config is invalid")
            return

        active_relations = self.charm.model.relations.get(self.relation_name, [])

        # Get Trino URL
        url = self._get_url()
        if not url:
            logger.debug("Trino URL not available, skipping reconciliation")
            return

        # Get structured catalog information
        catalogs = self._get_catalogs()
        exclusions = self._parse_exclusions()

        for relation in active_relations:
            # Get per-relation user and secret
            secret, _created = self._get_relation_secret(relation)
            if secret is None:
                continue

            # Filter catalogs based on per-app exclusions
            app_name = relation.data[relation.app].get("app_name") if relation.app else None
            excluded = exclusions.get(app_name, set())
            filtered_catalogs = (
                [c for c in catalogs if c.name not in excluded] if excluded else catalogs
            )

            # Update relation databag
            self.provider.update_relation_data(
                relation=relation,
                trino_url=url,
                trino_catalogs=filtered_catalogs,
                trino_credentials_secret_id=secret.id,
            )

    def get_relation_credentials(self) -> dict:
        """Get credentials for all per-relation users.

        Returns:
            Dict mapping username to password for all active relations.
        """
        credentials = {}
        for relation in self.charm.model.relations.get(self.relation_name, []):
            label = self._secret_label(relation.id)
            try:
                secret = self.charm.model.get_secret(label=label)
                content = secret.get_content(refresh=True)
                credentials[content["username"]] = content["password"]
            except SecretNotFoundError:
                logger.debug("No secret found for relation %s", relation.id)
        return credentials
