# Copyright 2023 Canonical Ltd.
# See LICENSE file for licensing details.

"""Define the Trino server Postgresql relation."""

import logging

from charms.data_platform_libs.v0.data_interfaces import DatabaseCreatedEvent
from ops import framework
from ops.model import ActiveStatus
from log import log_event_handler
from literals import CATALOG_PATH
from utils import render, push
import re

logger = logging.getLogger(__name__)



class Database(framework.Object):
    """Client for trino:database relations."""

    def __init__(self, charm):
        """Construct.

        Args:
            charm: The charm to attach the hooks to.
        """
        super().__init__(charm, "database")
        self.charm = charm
        self.framework.observe(charm.postgresql_db.on.database_created, self._on_database_changed)
        self.framework.observe(charm.postgresql_db.on.endpoints_changed, self._on_database_changed)
        self.framework.observe(charm.on.postgresql_db_relation_departed, self._on_database_relation_departed)

        self.framework.observe(charm.mysql_db.on.database_created, self._on_database_changed)
        self.framework.observe(charm.mysql_db.on.endpoints_changed, self._on_database_changed)
        self.framework.observe(charm.on.mysql_db_relation_broken, self._on_database_relation_departed)

    @log_event_handler(logger)
    def _on_database_changed(self, event: DatabaseCreatedEvent) -> None:
        # Handle the created database

        if not self.charm.unit.is_leader():
            return

        rel_name = event.relation.name
        cluster_name = event.relation.app.name
        db_name = event.database

        user = event.username
        password = event.password
        host, port = event.endpoints.split(",", 1)[0].split(":")

        # TODO: Add validation that event values have been received.
        # TODO: Add validation that a database with this name does not already exist
        self.charm.unit.status = ActiveStatus("received database credentials")

        db_context = self._create_db_context(user, password, host, port, db_name, rel_name, cluster_name)

        self._add_config_file(event, db_name, db_context, cluster_name)
        self._add_database_to_state(db_context, cluster_name, db_name)
        self.charm._update(event)

    
    def _create_db_context(self, user, password, host, port, db_name, rel_name, cluster_name):
        db_type = re.match(r"(.+?)_db", rel_name).group(1)
        if db_type == "mysql":
            conn_string = f"jdbc:{db_type}://{host}:{port}"
        if db_type == "postgresql":
            conn_string = f"jdbc:{db_type}://{host}:{port}/{db_name}"
        
        return {
            "CLUSTER": cluster_name,
            "DB_TYPE": db_type,
            "DB_CONN_STRING": conn_string,
            "DB_NAME": db_name,
            "DB_PORT": port,
            "DB_HOST": host,
            "DB_USER": user,
            "DB_PSWD": password,
        }

    def _add_config_file(self, event, name, db_context, cluster_name):
        
        container = self.charm.model.unit.get_container(self.charm.name)
        if not container.can_connect():
            event.defer()
            return

        db_file= render("db-conn.jinja", db_context)
        path = f"{CATALOG_PATH}/{cluster_name}-{name}.properties"
        push(container, db_file, path)


    def _add_database_to_state(self, db_env, cluster_name, db_name):
        if self.charm._state.database_connections:
            database_connections = self.charm._state.database_connections
        else:
            database_connections = {}
        unique_id = f"{cluster_name}-{db_name}"
        database_connections[unique_id] = db_env
        self.charm._state.database_connections = database_connections

    @log_event_handler(logger)
    def _on_database_relation_departed(self, event):
        if not self.charm.unit.is_leader():
            return

        container = self.charm.model.unit.get_container(self.charm.name)
        if not container.can_connect():
            event.defer()
            return

        cluster_name = event.relation.app.name
        database_connections = self.charm._state.database_connections
        databases_to_remove = []
        for database, values in database_connections.items():
            if values["CLUSTER"] == cluster_name:
                logging.info(f"removing {database} from configuration")
                if container.exists(f"{CATALOG_PATH}/{database}.properties"):
                    container.remove_path(path=f"{CATALOG_PATH}/{database}.properties")
                databases_to_remove.append(database)
        
        for relation in databases_to_remove:
            del database_connections[relation]

        self.charm._state.database_connections = database_connections
        self.charm._update(event)
    