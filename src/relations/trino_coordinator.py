# Copyright 2023 Canonical Ltd.
# See LICENSE file for licensing details.

"""Trino coordinator relation hooks & helpers."""


import logging

from ops.charm import CharmBase
from ops.framework import Object
from ops.model import SecretNotFoundError

from literals import SECRET_LABEL
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
        """Handle the relation changed event.

        Args:
            event: the relation changed or update config event.
        """
        if not self.charm.state.is_ready():
            event.defer()
            return

        self._update_coordinator_relation_data(event)

    def _update_coordinator_relation_data(self, event):
        """Update the `trino-coordinator` relation databag.

        Args:
            event: the relation changed or config changed event.
        """
        if not self.charm.state.is_ready():
            event.defer()
            return

        if not self.charm.config["charm-function"] == "coordinator":
            return

        coordinator_relations = self.model.relations["trino-coordinator"]

        relation_data = {"discovery-uri": self.charm.config["discovery-uri"]}

        for relation in coordinator_relations:
            if self.charm.config.get("catalog-config"):
                relation_data, secret = self._update_juju_secret(relation_data)
                secret.grant(relation)
            relation.data[self.charm.app].update(relation_data)

        self.charm._update(event)

    def _update_juju_secret(self, relation_data):
        """Create a juju secret with the catalog-config data.

        Args:
            relation_data: the data to be put in the relation databag.

        Returns:
            relation_data: updated relation data with the secret values.
            secret: the juju secret created.
        """
        content = {"catalogs": self.charm.config["catalog-config"]}
        try:
            secret = self.model.get_secret(label=SECRET_LABEL)
            secret.set_content(content)
        except SecretNotFoundError:
            secret = self.charm.app.add_secret(content, label=SECRET_LABEL)

        relation_data.update({"catalog-secret-id": secret.id})
        return relation_data, secret

    def _on_relation_broken(self, event):
        """Coordinator updates and re-validates relations on relation broken.

        Args:
            event: the relation broken event.
        """
        try:
            secret = self.model.get_secret(label=SECRET_LABEL)
            secret.remove_all_revisions()
        except SecretNotFoundError:
            logger.debug(f"No secret found with label {SECRET_LABEL!r}")

        self.charm._update(event)

    def _validate(self):
        """Check if the trino coordinator relation is available.

        Raises:
            ValueError: if the coordinator is not ready.
        """
        if self.model.relations["trino-worker"]:
            raise ValueError("Incorrect trino relation configuration.")

        if not self.model.relations["trino-coordinator"]:
            raise ValueError("Missing Trino worker relation.")
