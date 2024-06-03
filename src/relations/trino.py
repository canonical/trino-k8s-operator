# Copyright 2023 Canonical Ltd.
# See LICENSE file for licensing details.

"""Ranger client relation hooks & helpers."""


import logging

from ops.charm import CharmBase
from ops.framework import Object
from ops.model import ActiveStatus, BlockedStatus, MaintenanceStatus

from log import log_event_handler

logger = logging.getLogger(__name__)


class TrinoRelationHandler(Object):
    """Defines functionality for the 'provides' side of the 'trino' relation.

    Hook events observed:
        - relation-updated
        - relation-broken
    """

    def __init__(
        self, charm: CharmBase, relation_name: str = "trino"
    ) -> None:
        """Construct TrinoRelationHandler object.

        Args:
            charm: the charm for which this relation is provided
            relation_name: the name of the relation
        """
        self.relation_name = relation_name

        super().__init__(charm, self.relation_name)
        self.framework.observe(
            charm.on[self.relation_name].relation_changed,
            self._on_relation_changed,
        )
        self.charm = charm

    @log_event_handler(logger)
    def _on_relation_changed(self, event):
        """Handle Trino relation changed event.

        Args:
            event: relation changed event.
        """
        if not self.charm.unit.is_leader():
            return

        self._handle_coordinator(event)
        self._handle_worker(event)


    def _handle_coordinator(self, event):
        if not self.charm.config["charm-function"] == "coordinator":
            return

        relation = self.charm.model.get_relation(
            self.relation_name, event.relation.id
        )
        relation.data[self.charm.app].update(
                {
                    "discovery-uri": self.charm.config.get("discovery-uri"),
                    "catalog-config": self.charm.config.get("catalog-config"),
                })

    def _handle_worker(self, event):
        if not self.charm.config["charm-function"] == "worker":
            return

        self.charm._state.discovery_uri = self.event.get("discovery_uri")
        self.charm._state.catalog_config = self.event.get("catalog-config")
