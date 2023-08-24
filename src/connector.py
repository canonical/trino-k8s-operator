# Copyright 2023 Canonical Ltd.
# See LICENSE file for licensing details.

"""Manager for handling Trino Connectors via actions."""


import logging

from ops.charm import ActionEvent
from ops.framework import Object

from literals import CATALOG_PATH, CONF_PATH, CONNECTOR_FIELDS
from log import log_event_handler
from utils import string_to_dict, validate_jdbc_pattern, validate_membership

logger = logging.getLogger(__name__)


class TrinoConnector(Object):
    """Handler for managing the client and unit connectors and TLS certs."""

    def __init__(self, charm):
        """Construct.

        Args:
            charm: Ignore.
        """
        super().__init__(charm, "connector")
        self.charm = charm
        self.framework.observe(
            self.charm.on.add_connector_action, self._on_add_connector
        )
        self.framework.observe(
            self.charm.on.remove_connector_action, self._on_remove_connector
        )

    @log_event_handler(logger)
    def _on_add_connector(self, event: ActionEvent):
        """Add a new connector, action handler.

        Args:
            event: The event triggered by the add-connector action
        """
        container = self.charm.model.unit.get_container(self.charm.name)
        if not container.can_connect():
            event.defer()
            return

        conn_name = event.params["conn-name"]
        conn_string = event.params["conn-config"]
        conn_cert = event.params.get("conn-cert")
        conn_input = string_to_dict(conn_string)

        if container.exists(f"{CATALOG_PATH}/{conn_name}.properties"):
            event.fail(f"Failed to add {conn_name}, connector already exists")
            return

        try:
            conn_type = conn_input["connector.name"]
        except ValueError as err:
            logger.exception(err)
            event.fail(f"Failed to add {conn_name}, invalid configuration")
            return

        if not self._is_valid_connection(conn_input, conn_type):
            event.fail(f"Failed to add {conn_name}, invalid configuration")
            return

        if conn_cert:
            container.push(
                f"{CONF_PATH}/{conn_name}.crt", conn_cert, make_dirs=True
            )
            self._add_cert_to_truststore(container, conn_name)

        path = f"{CATALOG_PATH}/{conn_name}.properties"
        container.push(path, conn_string, make_dirs=True)
        self._add_connector_to_state(conn_string, conn_name)
        self.charm._restart_trino(container)
        event.set_results({"result": "connector successfully added"})

    def _add_cert_to_truststore(self, container, conn_name):
        """Add CA to JKS truststore.

        Args:
            container: Trino container
            conn_name: certificate file name
        """
        command = [
            "keytool",
            "-import",
            "-v",
            "-alias",
            conn_name,
            "-file",
            f"{conn_name}.crt",
            "-keystore",
            "truststore.jks",
            "-storepass",
            self.charm._state.truststore_password,
            "-noprompt",
        ]
        container.exec(command, working_dir=CONF_PATH).wait_output()
        container.remove_path(f"{CONF_PATH}/{conn_name}.crt")

    def _is_valid_connection(self, conn_input, conn_type):
        """Validate configuration for connector.

        Additional validation required for the postgresql connector,
        incorrect formatting of this connector will prevent the Trino
        application from starting. This does not appear to be the case
        for other connectors.

        Args:
            conn_input: The connector configuration provided by user
            conn_type: The type of connector ie. postgresql

        Returns:
            bool: True/False on successful validation
        """
        connector_fields = CONNECTOR_FIELDS.get(conn_type)
        try:
            validate_membership(connector_fields, conn_input)
            if conn_type == "postgresql":
                validate_jdbc_pattern(conn_input, conn_type)
            return True
        except ValueError as err:
            logger.exception(err)
            return False

    def _add_connector_to_state(self, config, conn_name):
        """Add connector name and configuration to charm._state.

        Args:
            config: The configuration of the connector
            conn_name: The name of the connector
        """
        if self.charm._state.connectors:
            connectors = self.charm._state.connectors
        else:
            connectors = {}
        connectors[conn_name] = config
        self.charm._state.connectors = connectors

    def _delete_cert_from_truststore(self, container, conn_name):
        """Delete CA from JKS truststore.

        Args:
            container: Trino container
            conn_name: certificate file name
        """
        command = [
            "keytool",
            "-delete",
            "-v",
            "-alias",
            conn_name,
            "-keystore",
            "truststore.jks",
            "-storepass",
            self.charm._state.truststore_password,
            "-noprompt",
        ]
        container.exec(command, working_dir=CONF_PATH).wait_output()

    @log_event_handler(logger)
    def _on_remove_connector(self, event: ActionEvent):
        """Remove an existing connector, action handler.

        Args:
            event: The event triggered by the remove-connector action
        """
        container = self.charm.model.unit.get_container(self.charm.name)
        if not container.can_connect():
            event.defer()
            return

        conn_name = event.params["conn-name"]
        conn_string = event.params["conn-config"]
        conn_cert = event.params.get("conn-cert")

        path = f"{CATALOG_PATH}/{conn_name}.properties"
        if not container.exists(path):
            event.fail(
                f"Failed to remove {conn_name}, connector does not exist"
            )
            return

        existing_connectors = self.charm._state.connectors
        connector_to_remove = None
        for key, value in existing_connectors.items():
            if key == conn_name and value == conn_string:
                container.remove_path(path=path)
                connector_to_remove = key

        if not connector_to_remove:
            event.fail(f"Failed to remove {conn_name}, invalid configuration")
            return

        del existing_connectors[connector_to_remove]
        if conn_cert:
            self._delete_cert_from_truststore(container, conn_name)

        self.charm._state.connectors = existing_connectors
        self.charm._restart_trino(container)
        event.set_results({"result": "connector successfully removed"})
