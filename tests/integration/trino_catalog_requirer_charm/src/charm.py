#!/usr/bin/env python3
# Copyright 2025 Canonical Ltd.
# See LICENSE file for licensing details.

"""Minimal Trino requirer charm for testing.

Demonstrates the three granular events and TrinoCatalog usage:
- url_changed: Trino URL changed
- catalogs_changed: Catalog list/metadata changed
- credentials_changed: Credentials changed (ID or content)
"""

import logging

from charms.trino_k8s.v0.trino_catalog import (
    TrinoCatalogRequirer,
    TrinoCatalogsChangedEvent,
    TrinoCredentialsChangedEvent,
    TrinoUrlChangedEvent,
)
from ops.charm import CharmBase
from ops.main import main
from ops.model import ActiveStatus, BlockedStatus, WaitingStatus

logger = logging.getLogger(__name__)


class TrinoCatalogRequirerCharm(CharmBase):
    """Minimal charm that consumes Trino catalog relation.

    Demonstrates granular event handling for URL, catalogs, and credentials.
    """

    def __init__(self, *args):
        super().__init__(*args)

        # Initialize the requirer library
        self.trino_catalog = TrinoCatalogRequirer(
            self, relation_name="trino-catalog"
        )

        self.framework.observe(self.on.install, self._on_install)
        self.framework.observe(self.on.start, self._on_start)

        # Observe the specific events emitted by the library
        self.framework.observe(
            self.trino_catalog.on.url_changed, self._on_trino_url_changed
        )
        self.framework.observe(
            self.trino_catalog.on.catalogs_changed,
            self._on_trino_catalogs_changed,
        )
        self.framework.observe(
            self.trino_catalog.on.credentials_changed,
            self._on_trino_credentials_changed,
        )

        # Observe standard charm events
        self.framework.observe(
            self.on.trino_catalog_relation_changed,
            self._on_trino_catalog_relation_changed,
        )
        self.framework.observe(
            self.on.trino_catalog_relation_broken,
            self._on_trino_catalog_relation_broken,
        )
        self.framework.observe(self.on.update_status, self._on_update_status)

    def _on_install(self, event):
        """Handle install event."""
        self.unit.status = BlockedStatus("Waiting for trino-catalog relation")

    def _on_start(self, event):
        """Handle start event."""
        # Check if we already have a relation
        if not self.model.relations.get("trino-catalog"):
            self.unit.status = BlockedStatus(
                "Waiting for trino-catalog relation"
            )
        else:
            # Relation exists, check if we have data
            self._check_and_configure()

    def _on_update_status(self, event):
        """Handle update-status event to check credentials after grant."""
        # This fires periodically, so we can check if secret was granted
        self._check_and_configure()

    def _on_trino_catalog_relation_changed(self, event):
        """Handle relation changed event."""
        logger.info("Trino catalog relation changed: %s", event.relation.id)
        self.unit.status = WaitingStatus("Waiting for Trino data")
        # Try to configure in case data is already available
        self._check_and_configure()

    def _on_trino_catalog_relation_broken(self, event):
        """Handle relation broken event."""
        logger.info("Trino catalog relation broken: %s", event.relation.id)
        self.unit.status = BlockedStatus("Trino relation removed")

    def _on_trino_url_changed(self, event: TrinoUrlChangedEvent):
        """Handle Trino URL change."""
        logger.info("Trino URL changed to: %s", event.trino_url)

        # Try to configure with new URL
        self._check_and_configure()

    def _on_trino_catalogs_changed(self, event: TrinoCatalogsChangedEvent):
        """Handle Trino catalogs change."""
        logger.info("Trino catalogs changed. New catalogs:")
        for catalog in event.trino_catalogs:
            logger.info(
                "  - %s (connector: %s)",
                catalog.name,
                catalog.connector,
                catalog.description,
            )

        # Try to configure with new catalogs
        self._check_and_configure()

    def _on_trino_credentials_changed(
        self, event: TrinoCredentialsChangedEvent
    ):
        """Handle Trino credentials change."""
        logger.info(
            "Trino credentials changed (secret: %s)",
            event.trino_credentials_secret_id,
        )

        # Try to configure with new credentials
        self._check_and_configure()

    def _check_and_configure(self):
        """Check if we have all required data and configure if so."""
        # Get Trino info
        trino_info = self.trino_catalog.get_trino_info()

        if not trino_info:
            logger.warning("No Trino info available yet")
            self.unit.status = WaitingStatus("Waiting for Trino data")
            return

        # Try to get credentials
        credentials = self.trino_catalog.get_credentials()

        if not credentials:
            self.unit.status = BlockedStatus(
                "Cannot access credentials secret"
            )
            return

        # We have everything, configure!
        self._configure_application()

    def _configure_application(self):
        """Configure the application with Trino connection info.

        This is where you would actually configure your application
        to connect to Trino using the provided information.
        """
        # Get all Trino information
        trino_info = self.trino_catalog.get_trino_info()
        if not trino_info:
            logger.warning("No Trino info available yet")
            self.unit.status = WaitingStatus("Waiting for Trino data")
            return

        # Get credentials
        credentials = self.trino_catalog.get_credentials()
        if not credentials:
            logger.error("Cannot access credentials secret")
            self.unit.status = BlockedStatus(
                "Cannot access credentials secret"
            )
            return

        username, password = credentials

        # Log the configuration (in real charm, you'd write config files, etc.)
        logger.info("Configuring application with Trino connection:")
        logger.info("  URL: %s", trino_info["trino_url"])
        logger.info("  Username: %s", username)
        logger.info("  Available catalogs:")
        for catalog in trino_info["trino_catalogs"]:
            logger.info(
                "    - %s (connector: %s)",
                catalog.name,
                catalog.connector,
                catalog.description,
            )
        self.unit.status = ActiveStatus("Connected to Trino")


if __name__ == "__main__":
    main(TrinoCatalogRequirerCharm)
