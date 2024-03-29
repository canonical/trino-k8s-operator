#!/usr/bin/env python3
# Copyright 2023 Canonical Ltd.
# See LICENSE file for licensing details.

"""Charm the service.

Refer to the following post for a quick-start guide that will help you
develop a new k8s charm using the Operator Framework:

https://discourse.charmhub.io/t/4208
"""

import logging

from charms.nginx_ingress_integrator.v0.nginx_route import require_nginx_route
from ops.charm import CharmBase, ConfigChangedEvent, PebbleReadyEvent
from ops.main import main
from ops.model import (
    ActiveStatus,
    BlockedStatus,
    MaintenanceStatus,
    WaitingStatus,
)

from connector import TrinoConnector
from literals import (
    AUTHENTICATOR_PATH,
    AUTHENTICATOR_PROPERTIES,
    CATALOG_PATH,
    CONF_PATH,
    CONFIG_JINJA,
    CONFIG_PATH,
    LOG_JINJA,
    LOG_PATH,
    PASSWORD_DB_PATH,
    RUN_TRINO_COMMAND,
    SYSTEM_CONNECTORS,
    TRINO_PORTS,
)
from log import log_event_handler
from relations.policy import PolicyRelationHandler
from state import State
from utils import bcrypt_pwd, generate_password, push, render

# Log messages can be retrieved using juju debug-log
logger = logging.getLogger(__name__)


class TrinoK8SCharm(CharmBase):
    """Charm the service.

    Attrs:
        state: used to store data that is persisted across invocations.
        external_hostname: DNS listing used for external connections.
    """

    @property
    def external_hostname(self):
        """Return the DNS listing used for external connections."""
        return self.config["external-hostname"] or self.app.name

    def __init__(self, *args):
        """Construct.

        Args:
            args: Ignore.
        """
        super().__init__(*args)
        self.name = "trino"
        self.state = State(self.app, lambda: self.model.get_relation("peer"))
        self.connector = TrinoConnector(self)
        self.policy = PolicyRelationHandler(self)

        # Handle basic charm lifecycle
        self.framework.observe(self.on.install, self._on_install)
        self.framework.observe(
            self.on.trino_pebble_ready, self._on_pebble_ready
        )
        self.framework.observe(self.on.config_changed, self._on_config_changed)
        self.framework.observe(self.on.restart_action, self._on_restart)
        self.framework.observe(
            self.on.peer_relation_changed, self._on_peer_relation_changed
        )

        # Handle Ingress
        self._require_nginx_route()

    def _require_nginx_route(self):
        """Require nginx-route relation based on current configuration."""
        require_nginx_route(
            charm=self,
            service_hostname=self.external_hostname,
            service_name=self.app.name,
            service_port=TRINO_PORTS["HTTP"],
            tls_secret_name=self.config["tls-secret-name"],
            backend_protocol="HTTP",
        )

    @log_event_handler(logger)
    def _on_install(self, event):
        """Install Trino.

        Args:
            event: The event triggered when the relation changed.
        """
        self.unit.status = MaintenanceStatus(f"{self.name} unit provisioned.")

    @log_event_handler(logger)
    def _on_pebble_ready(self, event: PebbleReadyEvent):
        """Define and start a workload using the Pebble API.

        Args:
            event: The event triggered when the relation changed.
        """
        self._update(event)

    @log_event_handler(logger)
    def _on_config_changed(self, event: ConfigChangedEvent):
        """Handle changed configuration.

        Args:
            event: The event triggered when the relation changed.
        """
        self.unit.status = WaitingStatus("configuring trino")
        self._update(event)

    @log_event_handler(logger)
    def _on_peer_relation_changed(self, event):
        """Handle changed peer relation.

        Args:
            event: The event triggered when the peer relation changed
        """
        if not self.state.is_ready():
            self.unit.status = WaitingStatus("Waiting for peer relation.")
            event.defer()
            return

        container = self.unit.get_container(self.name)
        if not container.can_connect():
            event.defer()
            return

        self.unit.status = WaitingStatus("updating peers")
        container = self.unit.get_container(self.name)
        try:
            current_connectors = self._get_current_connectors(container)
        except RuntimeError as err:
            self.unit.status = BlockedStatus(str(err))
            return

        target_connectors = self.state.connectors or {}
        if target_connectors == current_connectors:
            return

        self._handle_diff(current_connectors, target_connectors, container)

    def _get_current_connectors(self, container):
        """Create a dictionary of existing connector configurations.

        Args:
            container: Trino container

        Returns:
            properties: A dictionary of existing connector configurations
        """
        properties = {}
        files = container.list_files(CATALOG_PATH, pattern="*.properties")
        file_names = [f.name for f in files]
        property_names = [file_name.split(".")[0] for file_name in file_names]

        for item in property_names:
            path = f"{CATALOG_PATH}/{item}.properties"
            config = container.pull(path).read()
            properties[item] = config

        return properties

    def _handle_diff(self, current, target, container):
        """Handle differences between state and unit connectors.

        Args:
            current: existing unit connectors
            target: intended connectors from _state
            container: Trino container
        """
        for key, config in target.items():
            if key not in current:
                path = f"{CATALOG_PATH}/{key}.properties"
                container.push(path, config, make_dirs=True)

        for key in current.keys():
            if key not in target.keys() and key not in SYSTEM_CONNECTORS:
                path = f"{CATALOG_PATH}/{key}.properties"
                container.remove_path(path)

        self._restart_trino(container)

    def _restart_trino(self, container):
        """Restart Trino.

        Args:
            container: Trino container
        """
        self.unit.status = MaintenanceStatus("restarting trino")
        container.restart(self.name)
        self.unit.status = ActiveStatus()

    @log_event_handler(logger)
    def _on_restart(self, event):
        """Restart Trino, action handler.

        Args:
            event:The event triggered by the restart action
        """
        container = self.unit.get_container(self.name)
        if not container.can_connect():
            event.defer()
            return

        self.unit.status = MaintenanceStatus("restarting trino")
        self._restart_trino(container)
        self._enable_password_auth(container)

        event.set_results({"result": "trino successfully restarted"})

    def _push_file(
        self, container, context, jinja_file, path, permission=0o644
    ):
        """Pushes files to application.

        Args:
            container: The application container
            context: The subset of config values for the file
            jinja_file: The template file
            path: The path for file in the application
            permission: File permission (default 0o644)
        """
        properties = render(jinja_file, context)
        container.push(
            path, properties, make_dirs=True, permissions=permission
        )

    def _enable_password_auth(self, container):
        """Create necessary properties and db files for authentication.

        Args:
            container: The application container
        """
        push(container, AUTHENTICATOR_PROPERTIES, AUTHENTICATOR_PATH)
        password = bcrypt_pwd(self.config["trino-password"])
        push(container, f"trino:{password}", PASSWORD_DB_PATH)

    def _create_truststore_password(self):
        """Create truststore password if it does not exist."""
        if not self.state.truststore_password:
            truststore_password = generate_password()
            self.state.truststore_password = truststore_password

    def _open_service_port(self):
        """Open port 8080 on Trino coordinator."""
        if self.config["charm-function"] in ["coordinator", "all"]:
            self.model.unit.open_port(port=8080, protocol="tcp")
        else:
            self.model.unit.close_port(port=8080, protocol="tcp")

    def _validate_config_params(self):
        """Validate that configuration is valid.

        Raises:
            ValueError: in case of invalid log configuration
                        in case of invalid trino-password
                        in case of web-proxy as empty string
        """
        valid_log_levels = ["info", "debug", "warn", "error"]

        log_level = self.model.config["log-level"].lower()
        if log_level not in valid_log_levels:
            raise ValueError(f"config: invalid log level {log_level!r}")

        trino_password = self.model.config["trino-password"]
        if not trino_password.strip():
            raise ValueError(f"conf: invalid password {trino_password!r}")

        web_proxy = self.config.get("web-proxy")
        if web_proxy and not web_proxy.strip():
            raise ValueError("Web-proxy value cannot be an empty string")

    def _create_environment(self):
        """Create application environment.

        Returns:
            env: a dictionary of trino environment variables
        """
        env = {
            "LOG_LEVEL": self.config["log-level"],
            "DEFAULT_PASSWORD": self.config["trino-password"],
            "OAUTH_CLIENT_ID": self.config.get("google-client-id"),
            "OAUTH_CLIENT_SECRET": self.config.get("google-client-secret"),
            "WEB_PROXY": self.config.get("web-proxy"),
            "SSL_PWD": self.state.truststore_password,
            "SSL_PATH": f"{CONF_PATH}/truststore.jks",
            "CHARM_FUNCTION": self.config["charm-function"],
            "DISCOVERY_URI": self.config["discovery-uri"],
            "APPLICATION_NAME": self.app.name,
        }
        return env

    def _update(self, event):
        """Update the Trino server configuration and replan its execution.

        Args:
            event: The event triggered when the relation changed.
        """
        container = self.unit.get_container(self.name)
        if not container.can_connect():
            event.defer()
            return

        if not self.state.is_ready():
            self.unit.status = WaitingStatus("Waiting for peer relation.")
            event.defer()
            return

        try:
            self._validate_config_params()
        except (RuntimeError, ValueError) as err:
            self.unit.status = BlockedStatus(str(err))
            return

        logger.info("configuring trino")
        self._create_truststore_password()
        self._enable_password_auth(container)
        self._open_service_port()

        env = self._create_environment()
        self._push_file(container, env, LOG_JINJA, LOG_PATH)
        self._push_file(container, env, CONFIG_JINJA, CONFIG_PATH)

        logger.info("planning trino execution")
        pebble_layer = {
            "summary": "trino layer",
            "description": "pebble config layer for trino",
            "services": {
                self.name: {
                    "override": "replace",
                    "summary": "trino server",
                    "command": RUN_TRINO_COMMAND,
                    "startup": "enabled",
                    "environment": env,
                }
            },
        }
        container.add_layer(self.name, pebble_layer, combine=True)
        container.replan()

        self.unit.status = ActiveStatus()


if __name__ == "__main__":
    main(TrinoK8SCharm)
