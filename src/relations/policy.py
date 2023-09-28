# Copyright 2023 Canonical Ltd.
# See LICENSE file for licensing details.

"""Defines policy relation event handling methods."""

import logging

from ops import framework
from ops.model import BlockedStatus
from ops.pebble import ExecError

from literals import (
    APP_NAME,
    JAVA_ENV,
    RANGER_PLUGIN_FILE,
    RANGER_PLUGIN_PATH,
    RANGER_POLICY_PATH,
    RANGER_PROPERTIES_PATH,
)
from log import log_event_handler
from utils import render

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

    def _prepare_service(self, event):
        """Prepare service to be created in Ranger.

        Args:
            event: relation created event

        Returns:
            service: service values to be set in relation databag
        """
        host = self.charm.config["external-hostname"]
        if host == APP_NAME:
            uri = f"{host}:8080"
        else:
            uri = host

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
    def _on_relation_created(self, event):
        """Handle policy relation created.

        Args:
            event: relation created event.
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
            event: relation changed event.
        """
        container = self.charm.model.unit.get_container(self.charm.name)
        if not container.can_connect():
            return

        policy_manager_url = event.relation.data[event.app].get(
            "policy_manager_url", None
        )
        if not policy_manager_url:
            return

        policy_relation = f"relation_{event.relation.id}"

        try:
            self._unpack_plugin(container)
            self._configure_plugin_properties(
                container, policy_manager_url, policy_relation
            )
            self._enable_plugin(container)
        except ExecError as err:
            logger.error(err)
            self.charm.unit.status = BlockedStatus(
                "Failed to enable Ranger plugin."
            )
            return
        self.charm._restart_trino(container)

    def _enable_plugin(self, container):
        """Enable ranger plugin.

        Args:
            container: application container

        Raises:
            ExecError: in case unable to enable trino plugin
        """
        command = [
            "bash",
            "enable-trino-plugin.sh",
        ]
        try:
            container.exec(
                command, working_dir=RANGER_PLUGIN_PATH, environment=JAVA_ENV
            ).wait_output()
        except ExecError as err:
            logger.error(err.stdout)
            raise

    def _unpack_plugin(self, container):
        """Unpack ranger plugin tar.

        Args:
            container: application container

        Raises:
            ExecError: in case unable to enable trino plugin
        """
        if container.exists(RANGER_PLUGIN_PATH):
            return

        command = [
            "tar",
            "xf",
            "ranger-2.4.0-trino-plugin.tar.gz",
        ]
        try:
            container.exec(command, working_dir="/root").wait_output()
        except ExecError as err:
            logger.error(err.stdout)
            raise

    def _configure_plugin_properties(
        self, container, policy_manager_url, policy_relation
    ):
        """Configure the Ranger plugin install.properties file.

        Args:
            container: The application container
            policy_manager_url: The url of the policy manager
            policy_relation: The relation name and id of policy relation
        """
        policy_context = {
            "POLICY_MGR_URL": policy_manager_url,
            "REPOSITORY_NAME": self.charm.config.get("ranger-service-name")
            or policy_relation,
        }
        properties = render(RANGER_PLUGIN_FILE, policy_context)
        container.push(
            RANGER_PROPERTIES_PATH,
            properties,
            make_dirs=True,
            permissions=0o744,
        )

    def _disable_ranger_plugin(self, container):
        """Disable ranger plugin.

        Args:
            container: application container

        Raises:
            ExecError: in case unable to enable trino plugin
        """
        command = [
            "bash",
            "disable-trino-plugin.sh",
        ]
        try:
            container.exec(
                command, working_dir=RANGER_PLUGIN_PATH, environment=JAVA_ENV
            ).wait_output()
        except ExecError as err:
            logger.error(err.stdout)
            raise

    @log_event_handler(logger)
    def _on_relation_broken(self, event):
        """Handle policy relation broken.

        Args:
            event: relation broken event.
        """
        container = self.charm.model.unit.get_container(self.charm.name)
        if not container.can_connect():
            return

        if not container.exists(RANGER_PLUGIN_PATH):
            return

        self._disable_ranger_plugin(container)

        if container.exists(RANGER_POLICY_PATH):
            container.remove_path(RANGER_POLICY_PATH, recursive=True)

        if container.exists(RANGER_PLUGIN_PATH):
            container.remove_path(RANGER_PLUGIN_PATH, recursive=True)

        self.charm._restart_trino(container)
