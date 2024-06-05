# Copyright 2023 Canonical Ltd.
# See LICENSE file for licensing details.

"""Trino coordinator relation hooks & helpers."""


import logging

from ops.charm import CharmBase
from ops.framework import Object

from log import log_event_handler

logger = logging.getLogger(__name__)


class TrinoCoordinator(Object):
    """Defines coordinator functionality for the relation between the Trino coordinator and worker.

    Hook events observed:
        - relation-created
        - relation-updated
        - relation-broken
    """

    def __init__(
        self, charm: CharmBase, relation_name: str = "trino-coordinator"
    ) -> None:
        """Construct TrinoRelationHandler object.

        Args:
            charm: the charm for which this relation is provided
            relation_name: the name of the relation
        """
        self.charm = charm
        self.relation_name = relation_name

        super().__init__(charm, self.relation_name)
        self.framework.observe(
            charm.on[self.relation_name].relation_created,
            self._on_relation_changed,
        )
        self.framework.observe(
            charm.on[self.relation_name].relation_changed,
            self._on_relation_changed,
        )
        self.framework.observe(
            charm.on[self.relation_name].relation_broken,
            self._on_relation_broken,
        )

    @log_event_handler(logger)
    def _on_relation_changed(self, event):
        """Coordinator adds to relation databag.

        Args:
            event: the relation changed or update config event.
        """
        if not self.charm.unit.is_leader():
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
        self.charm._update(event)

    def _on_relation_broken(self, event):
        """Coordinator updates and re-validates relations on relation broken.

        Args:
            event: the relation broken event.
        """
        self.charm._update(event)

    def _validate(self):
        """Check if the trino coordinator relation is available.

        Raises:
            ValueError: if the coordinator is not ready.
        """
        if not self.model.relations["trino-coordinator"]:
            raise ValueError("Missing Trino worker relation.")
