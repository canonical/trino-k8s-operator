# Copyright 2023 Canonical Ltd.
# See LICENSE file for licensing details.

"""Trino worker relation hooks & helpers."""


import logging

from ops.charm import CharmBase
from ops.framework import Object

from literals import CATALOG_DIR, SECRET_LABEL
from log import log_event_handler

logger = logging.getLogger(__name__)


class TrinoWorker(Object):
    """Defines worker functionality for the relation between the Trino coordinator and worker.

    Hook events observed:
        - relation-created
        - relation-updated
        - relation-broken
    """

    def __init__(
        self, charm: CharmBase, relation_name: str = "trino-worker"
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
            charm.on.secret_changed,
            self._on_secret_changed,
        )
        self.framework.observe(
            charm.on[self.relation_name].relation_broken,
            self._on_relation_broken,
        )

    @log_event_handler(logger)
    def _on_relation_changed(self, event):
        """Worker updates `state` based on relation event data.

        Args:
            event: the relation changed event.
        """
        if not self.charm.state.is_ready():
            event.defer()
            return

        if not self.charm.unit.is_leader():
            return

        event_data = event.relation.data[event.app]
        self.charm.state.discovery_uri = event_data.get("discovery-uri")
        self.charm.state.user_secret_id = event_data.get("user-secret-id")

        secret_id = event_data.get("catalog-secret-id")
        if not secret_id:
            self.charm._update(event)
            return

        secret = self.model.get_secret(id=secret_id, label=SECRET_LABEL)
        content = secret.get_content()
        self.charm.state.catalog_config = content["catalogs"]

        self.charm._update(event)

    @log_event_handler(logger)
    def _on_secret_changed(self, event):
        """Handle secret changed hook.

        Args:
            event: the secret changed event.
        """
        if not self.charm.state.is_ready():
            event.defer()
            return

        if not self.charm.unit.is_leader():
            return

        if not event.secret.label == SECRET_LABEL:
            return

        content = event.secret.get_content(refresh=True)
        self.charm.state.catalog_config = content["catalogs"]
        self.charm._update(event)

    def _on_relation_broken(self, event):
        """Worker updates `state` following relation broken event.

        Args:
            event: the relation broken event.
        """
        if not self.charm.state.is_ready():
            return

        container = self.charm.model.unit.get_container(self.charm.name)
        if not container.can_connect():
            event.defer()
            return

        catalog_path = self.charm.trino_abs_path.joinpath(CATALOG_DIR)
        if container.exists(catalog_path):
            container.remove_path(catalog_path, recursive=True)

        if not self.charm.unit.is_leader():
            return

        self.charm.state.discovery_uri = ""
        self.charm.state.catalog_config = ""

        self.charm._update(event)

    def _validate(self):
        """Check if the trino worker relation is available.

        Raises:
            ValueError: if the relation is incorrectly configured.
                      : if there is no coordinator relation.
        """
        if self.model.relations["trino-coordinator"]:
            raise ValueError("Incorrect trino relation configuration.")

        if not self.model.relations["trino-worker"]:
            raise ValueError("Missing Trino coordinator relation.")
