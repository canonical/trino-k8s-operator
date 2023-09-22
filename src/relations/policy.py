# Copyright 2023 Canonical Ltd.
# See LICENSE file for licensing details.

"""Defines postgres relation event handling methods."""

import logging

from ops import framework

from log import log_event_handler

logger = logging.getLogger(__name__)


class PolicyRelationHandler(framework.Object):
    """Client for trino policy relations."""

    def __init__(self, charm, relation_name="policy"):
        """Construct.

        Args:
            charm: The charm to attach the hooks to.
            relation_name: the name of the relation defaults to policy.
        """
        super().__init__(charm, "policy")
        self.charm = charm
        self.relation_name = relation_name

        # Handle database relation.
        self.framework.observe(
            charm.on[self.relation_name].relation_created,
            self._on_relation_created,
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
    def _on_relation_created(self, event):
        """Handle policy relation created.

        Args:
            event: relation created event.
        """
        if not self.charm.unit.is_leader():
            return

        host = self.charm.config["external-hostname"]
        if host == "trino-k8s":
            port = 8080
        else:
            port = 8443
        service_name = (
            self.charm.config.get("ranger-service-name")
            or f"relation_{event.relation.id}"
        )
        service = {
            "name": service_name,
            "type": "trino",
            "jdbc.driverClassName": "io.trino.jdbc.TrinoDriver",
            "jdbc.url": f"jdbc:trino://{host}:{port}",
        }
        if event.relation:
            event.relation.data[event.app].update(service)

    @log_event_handler(logger)
    def _on_relation_changed(self, event):
        """Handle policy relation changed.

        Args:
            event: relation changed event.
        """
        if not self.charm.unit.is_leader():
            return

        policy_manager_url = event.relation.data[event.app].get(
            "policy_manager_url", None
        )
        if not policy_manager_url:
            return

        self.charm._state.policy_manager_url = policy_manager_url
        self.charm._update(event)

    def _on_relation_broken(self, event):
        """Handle policy relation broken.

        Args:
            event: relation broken event.
        """
        if not self.charm.unit.is_leader():
            return

        self.charm._state.policy_manager_url = None
        self.charm._update(event)
