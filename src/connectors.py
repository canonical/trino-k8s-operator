import logging
from ops.charm import ActionEvent
from log import log_event_handler
from literals import CATALOG_PATH
from utils import string_to_dict, read_json, check_required_params, validate_jdbc_pattern
import re

logger = logging.getLogger(__name__)

class TrinoConnectors(Object):
    """Handler for managing the client and unit TLS keys/certs."""

    def __init__(self, charm):
        super().__init__(charm, "connectors")
        self.charm = charm
        
        self.framework.observe(self.on.add_database_action, self._on_add_database)
        # self.framework.observe(self.on.remove_database_action, self._on_remove_database)

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

        conn_config = event.params["db-config"]
        db_name = event.params["db-name"]

        try:
            self._validate_connection_params(conn_config)
        except ValueError as err:
            logger.exception(err)
            event.fail(f"Failed to add {db_name}")
            return
        
        path = f"{CATALOG_PATH}/{db_name}.properties"
        container.push(path, conn_config, make_dirs=True)

        self._restart_trino(container)
        event.set_results({"result": "database successfully added"})


    def _validate_connection(event, conn_config):
        """Validate values for connector configureation.
        
        Args:
            db_config: connector configuration provided by user"""
        required_fields = read_json("connectory-required-fields")
        conn_dict = string_to_dict(conn_config)
        conn_name = conn_dict["connector.name"]
        params = required_fields.get(conn_name)
        try:
            check_required_params(params, conn_dict, conn_name)
            if conn_name == "postgresql":
                validate_jdbc_pattern(conn_dict, conn_name)
        except ValueError as err:
            logger.exception(err)
            event.fail(f"Failed to add {conn_name}")
            return
