#!/usr/bin/env python3
# Copyright 2025 Canonical Ltd.
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
from ops.charm import CharmBase, SecretChangedEvent
from ops.framework import Object

from literals import TRINO_PORTS

logger = logging.getLogger(__name__)


class TrinoCatalogRelationHandler(Object):
    """Handles the trino-catalog relation for the Trino charm.

    Parses catalog-config to extract structured catalog information
    and shares it with requirers via the trino-catalog relation.

    Note: The model-owned secret (user-secret-id) must be manually granted
    to each requirer application using: juju grant-secret <secret> <requirer-app>
    """

    def __init__(
        self, charm: CharmBase, relation_name: str = "trino-catalog"
    ) -> None:
        """Construct.

        Args:
            charm: The charm to attach the hooks to.
        """
        super().__init__(charm, "trino-catalog-provider")
        self.charm = charm
        self.relation_name = relation_name

        # Initialize the provider library
        self.provider = TrinoCatalogProvider(self.charm)

        # Observe relation events
        self.framework.observe(
            charm.on[self.relation_name].relation_created,
            self._on_relation_changed,
        )
        self.framework.observe(
            charm.on[self.relation_name].relation_changed,
            self._on_relation_changed,
        )

    def _on_relation_changed(self, event):
        """Handle trino-catalog relation created."""
        if not self.charm.state.is_ready():
            event.defer()
            return

        self._update_relation(event.relation)

    def _get_url(self) -> Optional[str]:
        """Get the Trino URL from configuration.

        Returns:
            Trino URL or None if not configured
        """
        external_hostname = self.charm.config.get("external-hostname")
        if not external_hostname:
            return None

        port = TRINO_PORTS["HTTPS"]
        return f"{external_hostname}:{port}"

    def _get_catalogs(self) -> List[TrinoCatalog]:
        """Get structured catalog information from catalog-config.

        Parses the catalog-config YAML to extract catalog name, connector type,
        and description for each configured catalog.

        Returns:
            List of TrinoCatalog objects
        """
        catalog_config_str = self.charm.config.get("catalog-config", "")

        if not catalog_config_str:
            logger.debug("No catalog-config set")
            return []

        try:
            config = yaml.safe_load(catalog_config_str)

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
                    else:
                        logger.debug(
                            "Backend %s not found for catalog %s",
                            backend_name,
                            catalog_name,
                        )
                else:
                    logger.debug(
                        "No backend specified for catalog %s", catalog_name
                    )

                # Create TrinoCatalog object
                catalog = TrinoCatalog(
                    name=catalog_name,
                    connector=connector,
                    description="",
                )

                catalog_list.append(catalog)

            logger.debug("Parsed %s catalogs from config", len(catalog_list))

            return catalog_list

        except yaml.YAMLError as e:
            logger.error("Failed to parse catalog-config YAML: %s", str(e))
            return []
        except Exception as e:
            logger.error("Failed to process catalog-config: %s", str(e))
            return []

    def _update_relation(self, event) -> None:
        """Update a specific trino-catalog relation.

        Args:
            event: the relation changed or config changed event.
        """
        if not self.charm.state.is_ready():
            event.defer()
            return

        if not self.charm.unit.is_leader():
            return

        # Get Trino URL
        url = self._get_url()
        if not url:
            logger.debug("Trino URL not available yet")
            return

        # Get structured catalog information
        catalogs = self._get_catalogs()

        # Get credentials secret ID
        secret_id = self.charm.config.get("user-secret-id")
        if not secret_id:
            logger.debug("user-secret-id not configured")
            return

        # Update relation via library
        # Note: The library will put the secret ID in the databag
        # Admin must grant access: juju grant-secret <secret> <requirer-app>
        self.provider.update_relation_data(
            relation=event.relation,
            trino_url=url,
            trino_catalogs=catalogs,
            trino_credentials_secret_id=secret_id,
        )

    def update_all_relations(self) -> None:
        """Update all trino-catalog relations."""
        logger.debug("UPDATE ALL RELATIONS STARTED")
        if not self.charm.unit.is_leader():
            return
        logger.debug("UPDATE ALL RELATIONS ONGOING")

        # Get Trino URL
        url = self._get_url()
        if not url:
            logger.debug("Trino URL not available yet")
            return

        # Get structured catalog information
        catalogs = self._get_catalogs()

        # Get credentials secret ID (model-owned secret)
        secret_id = self.charm.config.get("user-secret-id")
        if not secret_id:
            logger.debug("user-secret-id not configured")
            return

        # Update all relations via library
        self.provider.update_all_relations(
            trino_url=url,
            trino_catalogs=catalogs,
            trino_credentials_secret_id=secret_id,
        )

    def update_secret_data(self, event: SecretChangedEvent) -> None:
        """Handle Trino credentials secret change.

        Checks if the changed secret is used in trino-catalog relations
        and updates the secret to trigger credentials_changed on requirers.
        """
        if not self.charm.state.is_ready():
            event.defer()
            return

        if not self.charm.unit.is_leader():
            return

        secret_id = event.secret.id

        # Update all relations using this secret
        updated_count = self.provider.update_secret_data(
            secret_id
        )

        if updated_count > 0:
            logger.info(
                "Trino credentials secret changed, updated %d relation(s)",
                updated_count,
            )
