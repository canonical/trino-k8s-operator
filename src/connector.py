# Copyright 2023 Canonical Ltd.
# See LICENSE file for licensing details.

"""Manager for handling Trino Connectors via actions."""


import logging

from ops.charm import ActionEvent
from ops.framework import Object

from literals import CATALOG_PATH, CONNECTOR_FIELDS
from log import log_event_handler
from utils import string_to_dict, validate_membership, validate_jdbc_pattern

logger = logging.getLogger(__name__)


class TrinoConnector(Object):
    """Handler for managing the client and unit TLS keys/certs."""

    def __init__(self, charm):
        super().__init__(charm, "connector")
        self.charm = charm
        self.framework.observe(self.charm.on.add_connector_action, self._on_add_connector)
        self.framework.observe(self.charm.on.remove_connector_action, self._on_remove_connector)

    @log_event_handler(logger)
    def _on_add_connector(self, event: ActionEvent):
        """Add a new connector, action handler.

        Args:
            event: The event triggered by the add-connector action
        """
        if not self.charm.unit.is_leader():
            return

        container = self.charm.model.unit.get_container(self.charm.name)
        if not container.can_connect():
            event.defer()
            return

        conn_name = event.params["conn-name"]
        conn_string = event.params["conn-config"]
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

        if not self._validate_connection(conn_input, conn_type):
            event.fail(f"Failed to add {conn_name}, invalid configuration")
            return

        config = ""
        for key, value in conn_input.items():
            config += f"{key}={value}\n"

        path = f"{CATALOG_PATH}/{conn_name}.properties"
        container.push(path, config, make_dirs=True)
        self.charm._restart_trino(container)

        self._add_connector_to_state(conn_string, conn_name)
        event.set_results({"result": "connector successfully added"})

    def _validate_connection(self, conn_input, conn_type):
        """Validate configuration for connector.

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
        """Adds connector name and configuration to charm._state.
        
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

    @log_event_handler(logger)
    def _on_remove_connector(self, event: ActionEvent):
        """Remove an existing connector, action handler.

        Args:
            event: The event triggered by the remove-connector action

        """
        if not self.charm.unit.is_leader():
            return

        container = self.charm.model.unit.get_container(self.charm.name)
        if not container.can_connect():
            event.defer()
            return

        conn_name = event.params["conn-name"]
        conn_string = event.params["conn-config"]

        path = f"{CATALOG_PATH}/{conn_name}.properties"
        if not container.exists(path):
            event.fail(f"Failed to remove {conn_name}, connector does not exist")
            return

        existing_connectors = self.charm._state.connectors
        connectors_to_remove = []
        for key, value in existing_connectors.items():
            if key == conn_name and value == conn_string:
                container.remove_path(path=path)
                connectors_to_remove.append(key)
            else:
                event.fail(f"Failed to remove {conn_name}, invalid configuration")
                return

        for connector in connectors_to_remove:
            del existing_connectors[connector]

        self.charm._restart_trino(container)
        self.charm._state.connectors = existing_connectors
        event.set_results({"result": "connector successfully removed"})
