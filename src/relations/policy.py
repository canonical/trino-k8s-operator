# Copyright 2023 Canonical Ltd.
# See LICENSE file for licensing details.

"""Defines policy relation event handling methods."""

import logging

from ops import framework
from ops.model import MaintenanceStatus
from ops.pebble import ExecError

from literals import RANGER_PLUGIN_FILES, TRINO_HOME, TRINO_PORTS
from log import log_event_handler
from utils import handle_exec_error, render

logger = logging.getLogger(__name__)


class PolicyRelationHandler(framework.Object):
    """Client for trino policy relations."""

    def __init__(self, charm, relation_name="policy"):
        """Construct.

        Args:
            charm: The charm to attach the hooks to.
            relation_name: The name of the relation defaults to policy.
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
            event: The relation created event.
        """
        if not self.charm.unit.is_leader():
            return

        service = self._prepare_service(event)

        if event.relation:
            event.relation.data[self.charm.app].update(service)

    @log_event_handler(logger)
    def _on_relation_changed(self, event):
        """Handle policy relation changed.

        Args:
            event: Relation changed event.
        """
        if not self.charm.unit.is_leader():
            return

        container = self.charm.model.unit.get_container(self.charm.name)
        if not container.can_connect():
            event.defer()
            return

        self.charm.state.policy_manager_url = event.relation.data[
            event.app
        ].get("policy_manager_url")
        if not self.charm.state.policy_manager_url:
            return

        self.charm.state.policy_relation = f"relation_{event.relation.id}"
        self._configure_ranger_plugin(container)

        self.charm._restart_trino()

    def _prepare_service(self, event):
        """Prepare service to be created in Ranger.

        Args:
            event: Relation created event

        Returns:
            service: Service values to be set in relation databag
        """
        host = self.charm.app.name
        port = TRINO_PORTS["HTTP"]
        namespace = self.model.name
        uri = f"{host}.{namespace}.svc.cluster.local:{port}"

        service_name = (
            self.charm.config.get("ranger-service-name")
            or f"relation_{event.relation.id}"
        )
        service = {
            "name": service_name,
            "type": "trino",
            "jdbc.driverClassName": "io.trino.jdbc.TrinoDriver",
            "jdbc.url": f"jdbc:trino://{uri}",
        }
        return service

    @log_event_handler(logger)
    def _on_relation_broken(self, event):
        """Handle policy relation broken.

        Args:
            event: Relation broken event.

        Raises:
            ExecError: When failure to disable Ranger plugin.
        """
        if not self.charm.unit.is_leader():
            return

        container = self.charm.model.unit.get_container(self.charm.name)
        if not container.can_connect():
            event.defer()
            return

        try:
            self.charm.state.ranger_enabled = False
            logger.info("Ranger plugin disabled successfully")
        except ExecError as err:
            raise ExecError(f"Unable to disable Ranger plugin: {err}") from err

        self.charm._update(event)

    @handle_exec_error
    def _configure_ranger_plugin(self, container):
        """Enable the ranger plugin for Trino.

        Args:
            container: The application container.
        """
        self._push_plugin_files(
            container,
            self.charm.state.policy_manager_url,
            self.charm.state.policy_relation,
        )
        self.charm.state.ranger_enabled = True
        logger.info("Ranger plugin is enabled.")

    def _push_plugin_files(
        self, container, policy_manager_url, policy_relation
    ):
        """Configure the Ranger plugin install.properties file.

        Args:
            container: The application container
            policy_manager_url: The url of the policy manager
            policy_relation: The relation name and id of policy relation
        """
        opensearch = self.charm.state.opensearch or {}
        if opensearch.get("is_enabled") and not container.exists(
            "/opensearch.crt"
        ):
            self.charm.opensearch_relation_handler.update_certificates()
        policy_context = {
            "TRINO_HOME": TRINO_HOME,
            "POLICY_MGR_URL": policy_manager_url,
            "REPOSITORY_NAME": self.charm.config.get("ranger-service-name")
            or policy_relation,
            "RANGER_RELATION": True,
            "OPENSEARCH_INDEX": opensearch.get("index"),
            "OPENSEARCH_HOST": opensearch.get("host"),
            "OPENSEARCH_PORT": opensearch.get("port"),
            "OPENSEARCH_PWD": opensearch.get("password"),
            "OPENSEARCH_USER": opensearch.get("username"),
            "OPENSEARCH_ENABLED": opensearch.get("is_enabled"),
        }
        for template, file in RANGER_PLUGIN_FILES.items():
            content = render(template, policy_context)
            path = self.charm.trino_abs_path.joinpath(file)
            container.push(path, content, make_dirs=True, permissions=0o744)

    def restart_ranger_plugin(self, event):
        """Restart Ranger plugin for Trino.

        Args:
            event: the relation changed event.
        """
        container = self.charm.model.unit.get_container(self.charm.name)
        if not container.can_connect():
            event.fail("Failed to connect to the container")
            return

        self.charm.unit.status = MaintenanceStatus("Restarting Ranger plugin")
        self._configure_ranger_plugin(container)
