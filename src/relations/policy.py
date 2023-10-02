# Copyright 2023 Canonical Ltd.
# See LICENSE file for licensing details.

"""Defines policy relation event handling methods."""

import logging

from ops import framework
from ops.model import BlockedStatus
from ops.pebble import ExecError

from literals import (
    JAVA_ENV,
    RANGER_PLUGIN_FILE,
    RANGER_PLUGIN_VERSION,
    RANGER_POLICY_PATH,
    TRINO_PORTS,
)
from log import log_event_handler
from utils import render

logger = logging.getLogger(__name__)


class PolicyRelationHandler(framework.Object):
    """Client for trino policy relations.

    Attributes:
        plugin_version: the version of the Ranger plugin
        ranger_plugin_path: the path of the unpacked ranger plugin
    """

    plugin_version = RANGER_PLUGIN_VERSION["path"]
    ranger_plugin_path = f"/root/ranger-{plugin_version}-trino-plugin"

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

        service = self._prepare_service(event)

        if event.relation:
            event.relation.data[self.charm.app].update(service)

    @log_event_handler(logger)
    def _on_relation_changed(self, event):
        """Handle policy relation changed.

        Args:
            event: relation changed event.
        """
        if not self.charm.unit.is_leader():
            return

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

    def _prepare_service(self, event):
        """Prepare service to be created in Ranger.

        Args:
            event: relation created event

        Returns:
            service: service values to be set in relation databag
        """
        host = self.charm.config["external-hostname"]
        port = TRINO_PORTS["HTTP"]
        uri = f"{host}:{port}"

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
            event: relation broken event.
        """
        if not self.charm.unit.is_leader():
            return

        container = self.charm.model.unit.get_container(self.charm.name)
        if not container.can_connect():
            return

        if not container.exists(self.ranger_plugin_path):
            return

        self._disable_ranger_plugin(container)

        if container.exists(RANGER_POLICY_PATH):
            container.remove_path(RANGER_POLICY_PATH, recursive=True)

        if container.exists(self.ranger_plugin_path):
            container.remove_path(self.ranger_plugin_path, recursive=True)

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
                command,
                working_dir=self.ranger_plugin_path,
                environment=JAVA_ENV,
            ).wait_output()
            logger.info("Ranger plugin enabled successfully")
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
        if container.exists(self.ranger_plugin_path):
            return

        tar_version = RANGER_PLUGIN_VERSION["tar"]

        command = [
            "tar",
            "xf",
            f"ranger-{tar_version}-trino-plugin.tar.gz",
        ]
        try:
            container.exec(command, working_dir="/root").wait_output()
            logger.info("Ranger plugin unpacked successfully")
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
        ranger_properties_path = f"/root/ranger-{self.plugin_version}-trino-plugin/install.properties"
        policy_context = {
            "POLICY_MGR_URL": policy_manager_url,
            "REPOSITORY_NAME": self.charm.config.get("ranger-service-name")
            or policy_relation,
        }
        properties = render(RANGER_PLUGIN_FILE, policy_context)
        container.push(
            ranger_properties_path,
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
                command,
                working_dir=self.ranger_plugin_path,
                environment=JAVA_ENV,
            ).wait_output()
            logger.info("Ranger plugin disabled successfully")
        except ExecError as err:
            logger.error(err.stdout)
            raise
