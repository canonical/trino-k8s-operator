#!/usr/bin/env python3
# Copyright 2023 Canonical Ltd
# See LICENSE file for licensing details.

"""Charm the service.

Refer to the following post for a quick-start guide that will help you
develop a new k8s charm using the Operator Framework:

https://discourse.charmhub.io/t/4208
"""

import logging
import os
import re

from jinja2 import Environment, FileSystemLoader
from ops.charm import (ActionEvent, CharmBase, PebbleReadyEvent, ConfigChangedEvent)
from ops.model import (ActiveStatus, BlockedStatus, MaintenanceStatus, WaitingStatus)
from ops.main import main
from log import log_event_handler
from tls import TrinoTLS
from state import State
from literals import CONF_PATH

# Log messages can be retrieved using juju debug-log
logger = logging.getLogger(__name__)


def render(template_name, context):
    """Render the template with the given name using the given context dict.

    Args:
        template_name: File name to read the template from.
        context: Dict used for rendering.

    Returns:
        A dict containing the rendered template.
    """
    charm_dir = os.path.abspath(
        os.path.join(os.path.dirname(__file__), os.pardir)
    )
    loader = FileSystemLoader(os.path.join(charm_dir, "templates"))
    return (
        Environment(loader=loader, autoescape=True)
        .get_template(template_name)
        .render(**context)
    )


class TrinoK8SCharm(CharmBase):
    """Charm the service."""

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
        self.framework.observe(self.on.add_database_action, self._on_add_database)
        self.framework.observe(self.on.remove_database_action, self._on_remove_database)
        self.framework.observe(self.on.restart_action, self._on_restart)

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

    def _validate_db_conn_params(
        self, container, db_name, db_conn_string, db_type
    ):
        """Validate the db parameters are valid for connection.

        Args:
            container: Trino server container
            db_name: Name of database to connect
            db_conn_string: JDBC string of database
            db_type: Type of database either postgresql or mysql

        Raises:
            ValueError: In case db-conn-string is invalid
                        In case db-type does not match valid type
                        In case db-name already exists
        """
        if not re.match("jdbc:[a-z0-9]+:(?s:.*)$", db_conn_string):
            raise ValueError(
                f"connect-database: {db_conn_string!r} has an invalid format"
            )

        valid_db_types = ["mysql", "postgresql"]
        if db_type not in valid_db_types:
            raise ValueError(
                f"connect-database: invalid database type {db_type!r}"
            )

        if container.exists(f"/etc/trino/catalog/{db_name}.properties"):
            raise ValueError(f"connect-database: {db_name!r} already exists!")

    @log_event_handler(logger)
    def _on_add_database(self, event: ActionEvent):
        """Connect a new database, action handler.

        Args:
            event: The event triggered by the connect-database action
        """
        container = self.unit.get_container(self.name)
        if not container.can_connect():
            event.defer()
            return

        db_type = event.params["db-type"]
        db_name = event.params["db-name"]
        db_conn_string = event.params["db-conn-string"]

        try:
            self._validate_db_conn_params(
                container, db_name, db_conn_string, db_type
            )
        except ValueError as err:
            logger.exception(err)
            event.fail(f"Failed to add {db_name}")
            return

        db_context = {
            "DB_TYPE": db_type,
            "DB_CONN_STRING": db_conn_string,
            "DB_USER": event.params["db-user"],
            "DB_PWD": event.params["db-pwd"],
        }
        self._push_file(
            container,
            db_context,
            "db-conn.jinja",
            f"/etc/trino/catalog/{db_name}.properties",
        )
        self._restart_trino(container)
        event.set_results({"result": "database successfully added"})

    def _validate_db_remove_params(self, container, db_name, db_user, db_pwd):
        """Validate the db parameters are valid for removal.

        Args:
            container: Trino server container
            db_name: Name of database to connect
            db_user: Database username
            db_pwd: Database password

        Raises:
            ValueError: In case database does not exist
                        In case credentials are not valid
        """
        path = f"/etc/trino/catalog/{db_name}.properties"
        if (container.exists(path) is False):
            raise ValueError(f"remove-database: {db_name!r} does not exist!")

        db_config = container.pull(path).read()
        conn_user = re.search(r"connection-user=(.*)", db_config).group(1)
        conn_pwd = re.search(r"connection-password=(.*)", db_config).group(1)

        if conn_user != db_user or conn_pwd != db_pwd:
            raise ValueError(
                f"remove-database: credentials do not match for {db_name!r}"
            )

    @log_event_handler(logger)
    def _on_remove_database(self, event):
        """Remove an existing database connection, action handler.

        Args:
            event: The event triggered by the remove-database action
        """
        container = self.unit.get_container(self.name)
        if not container.can_connect():
            event.defer()
            return

        db_name = event.params["db-name"]
        db_user = event.params["db-user"]
        db_pwd = event.params["db-pwd"]

        try:
            self._validate_db_remove_params(
                container, db_name, db_user, db_pwd
            )
        except ValueError as err:
            logger.exception(err)
            event.fail(f"Failed to remove {db_name}")
            return

        container.remove_path(f"/etc/trino/catalog/{db_name}.properties")
        self._restart_trino(container)
        event.set_results({"result": "database successfully removed"})

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
    def _configure_ranger_plugin(self, container):
        """Prepares Ranger plugin.

        Args:
            container: The application container

        Raises:
            RuntimeError: ranger-trino-plugin.tar.gz is not present
        """
        RANGER_VERSION = self.config['ranger-version']
        path = f"/root/ranger-{RANGER_VERSION}-trino-plugin.tar.gz"
        if (container.exists(path) is False):
            raise RuntimeError(f"ranger-plugin: {path!r} does not exist, check your Trino image!")

        policy_context = {"POLICY_MGR_URL": RANGER_VERSION}
        jinja_file = "plugin-install.jinja"
        trino_path = "/root/install.properties"
        self._push_file(container, policy_context, jinja_file, trino_path)

        entrypoint_context = {"RANGER_VERSION": self.config['ranger-version']}
        jinja_file = "trino-entrypoint.jinja"
        trino_path = "/trino-entrypoint.sh"
        self._push_file(container, entrypoint_context, jinja_file, trino_path, 0o744)

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
        return context

    def _validate_config_params(self):
        """Validate that configuration is valid.

        Raises:
            ValueError: in case of invalid configuration.
        """
        valid_log_levels = ["info", "debug", "warn", "error"]

        log_level = self.model.config["log-level"].lower()
        if log_level not in valid_log_levels:
            raise ValueError(f"config: invalid log level {log_level!r}")

    def _update(self, event):
        """Update the Trino server configuration and replan its execution.

        Args:
            event: The event triggered when the relation changed.
        """
        try:
            self._validate_config_params()
        except ValueError as err:
            self.unit.status = BlockedStatus(str(err))
            return
        
        container = self.unit.get_container(self.name)
        if not container.can_connect():
            event.defer()
            return

        logger.info("configuring trino")
        log_options = {"log-level": "LOG_LEVEL"}
        log_context = {config_key: self.config[key] for key, config_key in log_options.items()}
        _ = self._push_file(container, log_context, "logging.jinja","/etc/trino/log.properties")
        
        config_options = {
            "google-client-id": "OAUTH_CLIENT_ID",
            "google-client-secret": "OAUTH_CLIENT_SECRET",
        }
        config_context = {config_key: self.config[key] for key, config_key in config_options.items()}
        config_context.update({
            "KEYSTORE_PASS": self._state.keystore_password,
            "KEYSTORE_PATH": f"{CONF_PATH}/keystore.p12",
        })
        config_env = self._push_file(container, config_context, "config.jinja", "/etc/trino/config.properties")

        if self.config['ranger-acl'] is True:
            try:
                self._configure_ranger_plugin(container)
                command = "/trino-entrypoint.sh"
            except RuntimeError as err:
                logger.exception(err)
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
                    "environment": config_env,
                }
            },
        }
        container.add_layer(self.name, pebble_layer, combine=True)
        container.replan()

        self.unit.status = ActiveStatus()


if __name__ == "__main__":
    main(TrinoK8SCharm)
