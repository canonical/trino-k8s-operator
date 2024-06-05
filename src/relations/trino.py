# Copyright 2023 Canonical Ltd.
# See LICENSE file for licensing details.

"""Ranger client relation hooks & helpers."""


import logging

from ops.charm import CharmBase
from ops.framework import Object

from literals import CATALOG_DIR
from log import log_event_handler

logger = logging.getLogger(__name__)


class TrinoRelationHandler(Object):
    """Defines functionality for the relation between the Trino coordinator and worker.

    Hook events observed:
        - relation-updated
        - relation-broken
    """

    def __init__(self, charm: CharmBase, relation_name: str) -> None:
        """Construct TrinoRelationHandler object.

        Args:
            charm: the charm for which this relation is provided
            relation_name: the name of the relation
        """
        self.relation_name = relation_name

        super().__init__(charm, self.relation_name)
        self.framework.observe(
            charm.on[self.relation_name].relation_created,
            self._handle_coordinator,
        )
        self.framework.observe(
            charm.on[self.relation_name].relation_changed,
            self._on_relation_changed,
        )
        self.framework.observe(
            charm.on[self.relation_name].relation_broken,
            self._on_relation_broken,
        )
        self.charm = charm

    @log_event_handler(logger)
    def _on_relation_changed(self, event):
        """Handle Trino relation changed event.

        Args:
            event: relation changed event.
        """
        self._handle_coordinator(event)
        self._handle_worker_relation_changed(event)

    def _handle_coordinator(self, event):
        """Coordinator adds to relation databag.

        Args:
            event: the relation changed or update config event.
        """
        if not self.charm.unit.is_leader():
            return

        if not self.charm.config["charm-function"] == "coordinator":
            return

        coordinator_relations = self.model.relations["trino-coordinator"]
        if not coordinator_relations:
            return

        for relation in coordinator_relations:
            relation.data[self.charm.app].update(
                {
                    "discovery-uri": self.charm.config.get(
                        "discovery-uri", ""
                    ),
                    "catalog-config": self.charm.config.get(
                        "catalog-config", ""
                    ),
                }
            )

    def _handle_worker_relation_changed(self, event):
        """Worker updates `state` based on relation event data.

        Args:
            event: the relation changed event.
        """
        if not self.charm.config["charm-function"] == "worker":
            return

        event_data = event.relation.data[event.app]

        if self.charm.unit.is_leader():
            self.charm.state.discovery_uri = event_data.get("discovery-uri")
            self.charm.state.catalog_config = event_data.get("catalog-config")

        self.charm._update(event)

    def _on_relation_broken(self, event):
        """Worker updates `state` following relation broken event.

        Args:
            event: the relation broken event.
        """
        if not self.charm.config["charm-function"] == "worker":
            return

        container = self.charm.model.unit.get_container(self.charm.name)
        if not container.can_connect():
            event.defer()
            return

        catalog_path = self.charm.trino_abs_path.joinpath(CATALOG_DIR)
        if container.exists(catalog_path):
            container.remove_path(catalog_path, recursive=True)

        if self.charm.unit.is_leader():
            self.charm.state.discovery_uri = None
            self.charm.state.catalog_config = None

        self.charm._update(event)

    def _validate(self):
        """Check if the trino worker connection is available.

        Raises:
            ValueError: if the worker is not ready.
        """
        if self.charm.state.discovery_uri is None:
            raise ValueError("Missing Trino coordinator relation.")
