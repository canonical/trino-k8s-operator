#!/usr/bin/env python3
# Copyright 2023 Canonical Ltd
# See LICENSE file for licensing details.

"""Charm the service.

Refer to the following post for a quick-start guide that will help you
develop a new k8s charm using the Operator Framework:

https://discourse.charmhub.io/t/4208
"""

import logging

from ops.charm import (ActionEvent, CharmBase, ConfigChangedEvent,
                       PebbleReadyEvent)
from ops.main import main
from ops.model import (ActiveStatus, BlockedStatus, MaintenanceStatus,
                       WaitingStatus)


from literals import CONF_PATH, CONFIG_JINJA, CONFIG_PATH, LOG_PATH, LOG_JINJA, TRINO_PORTS, CATALOG_PATH, CONNECTOR_FIELDS
from log import log_event_handler
from state import State
from tls import TrinoTLS
from charms.nginx_ingress_integrator.v0.nginx_route import require_nginx_route
from utils import render, string_to_dict, validate_membership, validate_jdbc_pattern

# Log messages can be retrieved using juju debug-log
logger = logging.getLogger(__name__)


class TrinoK8SCharm(CharmBase):
    """Charm the service."""

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
        self._state = State(self.app, lambda: self.model.get_relation("peer"))
        self.tls = TrinoTLS(self)

        # Handle basic charm lifecycle
        self.framework.observe(self.on.install, self._on_install)
        self.framework.observe(self.on.trino_pebble_ready, self._on_pebble_ready)
        self.framework.observe(self.on.config_changed, self._on_config_changed)
        self.framework.observe(self.on.restart_action, self._on_restart)
        self.framework.observe(self.on.add_database_action, self._on_add_database)

        # Handle Ingress
        self._require_nginx_route()

    def _require_nginx_route(self):
        """Require nginx-route relation based on current configuration."""
        require_nginx_route(
            charm=self,
            service_hostname=self.external_hostname,
            service_name=self.app.name,
            service_port=TRINO_PORTS["HTTPS"],
            tls_secret_name=self.config["tls-secret-name"],
            backend_protocol="HTTPS",
        )

    @log_event_handler(logger)
    def _on_install(self, event):
        """Install Trino.

        Args:
            event: The event triggered when the relation changed.
        """
        self.unit.status = MaintenanceStatus("installing trino")

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

    def _restart_trino(self, container):
        """Restart Trino.

        Args:
            container: Trino container
        """
        self.unit.status = MaintenanceStatus("restarting trino")
        container.restart(self.name)
        self.unit.status = ActiveStatus()

    def ready_to_start(self):
        """Check if TLS is enabled and peer relations established
        
        Returns:
            True if TLS enabled and peer relation established, else False.
        """
        if not self._state.is_ready():
            self.unit.status = WaitingStatus("Waiting for peer relation.")
            return False
        
        if not self._state.tls == "enabled":
            self.unit.status = BlockedStatus("Needs a certificates relation for TLS")
            return False

        return True

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
        event.set_results({"result": "trino successfully restarted"})

    @log_event_handler(logger)
    def _on_add_database(self, event: ActionEvent):
        """Connect a new database, action handler.

        Args:
            event: The event triggered by the connect-database action
        """
        if not self.unit.is_leader():
            return

        container = self.unit.get_container(self.name)
        if not container.can_connect():
            event.defer()
            return
        
        db_name = event.params["db-name"]
        conn_string = event.params["db-config"]
        conn_input = string_to_dict(conn_string)

        if container.exists(f"{CATALOG_PATH}/{db_name}.properties"):
            event.fail(f"Failed to add {db_name}, database already exists")
            return

        try:
            conn_name = conn_input["connector.name"]
        except ValueError as err:
            logger.exception(err)
            event.fail(f"Failed to add {db_name}, invalid configuration")
            return
        
        if not self._validate_connection(conn_input, conn_name):
            event.fail(f"Failed to add {db_name}, invalid configuration")
            return
        
        config = ""
        for key, value in conn_input.items():
            config += f"{key}={value}\n"
        

        path = f"{CATALOG_PATH}/{db_name}.properties"
        container.push(path, config, make_dirs=True)
        self._add_connector_to_state(config, db_name)
        logging.info(self._state.connectors)
        self._restart_trino(container)
        event.set_results({"result": "database successfully added"})


    def _validate_connection(self, conn_input, conn_name):
        """Validate values for connector configuration.
        
        Args:
            db_config: connector configuration provided by user"""
        connector_fields = CONNECTOR_FIELDS.get(conn_name)
        try:
            validate_membership(connector_fields, conn_input, conn_name)
            if conn_name == "postgresql":
                validate_jdbc_pattern(conn_input, conn_name)
            return True
        except ValueError as err:
            logger.exception(err)
            return False
    
    def _add_connector_to_state(self, config, db_name):
        if self._state.connectors:
            connectors = self._state.connectors
        else:
            connectors = {}
        db_name = f"{db_name}"
        connectors[db_name] = config
        self._state.connectors = connectors

    @log_event_handler(logger)
    def _configure_ranger_plugin(self, container):
        """Prepare Ranger plugin.

        Args:
            container: The application container

        Raises:
            RuntimeError: ranger-trino-plugin.tar.gz is not present
        """
        ranger_version = self.config['ranger-version']
        path = f"/root/ranger-{ranger_version}-trino-plugin.tar.gz"
        if not container.exists(path):
            raise RuntimeError(f"ranger-plugin: no {path!r}, check the image")

        policy_context = {"POLICY_MGR_URL": self.config['policy-mgr-url']}
        jinja_file = "plugin-install.jinja"
        trino_path = "/root/install.properties"
        self._push_file(container, policy_context, jinja_file, trino_path)

        entrypoint_context = {"RANGER_VERSION": self.config['ranger-version']}
        jinja_file = "trino-entrypoint.jinja"
        trino_path = "/trino-entrypoint.sh"
        self._push_file(container, entrypoint_context, jinja_file, trino_path, 0o744)
        command = "/trino-entrypoint.sh"
        return command

    def _push_file(self, container, context, jinja_file, path, permission=0o644):
        """Pushes files to application.

        Args:
            container: The application container
            context: The subset of config values for the file
            jinja_file: The template file
            path: The path for file in the application
            permission: File permission (default 0o644)

        Returns:
            A dictionary of variables for jinja file
        """
        properties = render(jinja_file, context)
        container.push(path, properties, make_dirs=True, permissions=permission)

    def _validate_config_params(self, container):
        """Validate that configuration is valid.

        Raises:
            ValueError: in case of invalid log configuration.
            ValueError: in case of Google ID not provided
            ValueError: in case of Google secret not provided
            RuntimeError: in case keystore does not exist
        """
        valid_log_levels = ["info", "debug", "warn", "error"]

        log_level = self.model.config["log-level"].lower()
        if log_level not in valid_log_levels:
            raise ValueError(f"config: invalid log level {log_level!r}")
        
        google_id = self.config.get('google-client-id')
        google_secret = self.config.get('google-client-secret')

        if google_id is None:
            raise ValueError("Google ID not provided for Oauth")
        if google_secret is None:
            raise ValueError("Google secret not provided for Oauth")

        path = f"{CONF_PATH}/keystore.p12"
        if not container.exists(path):
            raise RuntimeError(f"{path} does not exist, check TLS relation")

    def get_params(self):
        """ Creates Jinja file specific dictionaries from relevant config values.

        Returns:
            log_context: A dictionary of log options
            config_context: A dictionary of config file options
        """
        log_options = {"log-level": "LOG_LEVEL"}
        log_context = {config_key: self.config[key] for key, config_key in log_options.items()}

        config_options = {
            "google-client-id": "OAUTH_CLIENT_ID",
            "google-client-secret": "OAUTH_CLIENT_SECRET",
        }
        config_context = {config_key: self.config[key] for key, config_key in config_options.items()}
        config_context.update({
            "KEYSTORE_PASS": self._state.keystore_password,
            "KEYSTORE_PATH": f"{CONF_PATH}/keystore.p12",
        })
        return log_context, config_context
        

    def _update(self, event):
        """Update the Trino server configuration and replan its execution.

        Args:
            event: The event triggered when the relation changed.
        """
        container = self.unit.get_container(self.name)
        if not container.can_connect():
            event.defer()
            return

        if not self.ready_to_start():
            event.defer()
            return

        try:
            self._validate_config_params(container)
        except (RuntimeError, ValueError) as err:
            self.unit.status = BlockedStatus(str(err))
            return

        logger.info("configuring trino")
        log_context, config_context = self.get_params()
        self._push_file(container, log_context, LOG_JINJA, LOG_PATH)
        self._push_file(container, config_context, CONFIG_JINJA, CONFIG_PATH)

        env = {}
        for params in [config_context, log_context]:
            env.update(params)

        if self.config['ranger-acl-enabled']:
            try:
                command = self._configure_ranger_plugin(container)
            except RuntimeError as err:
                self.unit.status = BlockedStatus(str(err))
                return
        else:
            command = "/usr/lib/trino/bin/run-trino"

        logger.info("planning trino execution")
        pebble_layer = {
            "summary": "trino layer",
            "description": "pebble config layer for trino",
            "services": {
                self.name: {
                    "override": "replace",
                    "summary": "trino server",
                    "command": command,
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
