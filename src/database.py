# Copyright 2023 Canonical Ltd.
# See LICENSE file for licensing details.

"""Define the Trino server Postgresql relation."""

import logging

from charms.data_platform_libs.v0.data_interfaces import DatabaseCreatedEvent, DatabaseRequires
from ops import framework
from ops.model import ActiveStatus
from log import log_event_handler
from literals import CATALOG_PATH
from utils import render, push

logger = logging.getLogger(__name__)



class Postgresql(framework.Object):
    """Client for trino:database relations."""

    def __init__(self, charm):
        """Construct.

        Args:
            charm: The charm to attach the hooks to.
        """
        super().__init__(charm, "database")
        self.charm = charm
        self.framework.observe(charm.database.on.database_created, self._on_database_changed)
        self.framework.observe(charm.database.on.endpoints_changed, self._on_database_changed)
        # self.framework.observe(charm.on.database_relation_broken, self._on_database_relation_broken)

    @log_event_handler(logger)
    def _on_database_changed(self, event: DatabaseCreatedEvent) -> None:
        # Handle the created database

        if not self.charm.unit.is_leader():
            return

        user = event.username
        password = event.password
        host, port = event.endpoints.split(",", 1)[0].split(":")
        name = event.database

        # TODO: Add validation that event values have been received.
        # TODO: Add validation that a database with this name does not already exist
        self.charm.unit.status = ActiveStatus("received database credentials")

        db_context = self._create_db_context(user, password, host, port, name)
        rel_name = event.relation.name
        self._add_config_file(event, name, db_context)
        self._update_database_connections(db_context, rel_name)
        self.charm._update(event)

    
    def _create_db_context(self, user, password, host, port, name):
        conn_string = f"jdbc:postgresql://{host}:{port}/{name}"
        return {
            "DB_TYPE": "postgresql",
            "DB_CONN_STRING": conn_string,
            "DB_NAME": name,
            "DB_PORT": port,
            "DB_HOST": host,
            "DB_USER": user,
            "DB_PSWD": password,
        }

    def _add_config_file(self, event, name, db_context):
        
        container = self.charm.model.unit.get_container(self.charm.name)
        if not container.can_connect():
            event.defer()
            return
       
        db_file= render("db-conn.jinja", db_context)
        path = f"{CATALOG_PATH}/{name}.properties"
        push(container, db_file, path)


    def _update_database_connections(self, db_env, rel_name):
        if self.charm._state.database_connections:
            database_connections = self.charm._state.database_connections
        else:
            database_connections = {}
        database_connections[rel_name] = db_env
        self.charm._state.database_connections = database_connections


    def _on_database_relation_broken(self, event):
        if not self.charm.unit.is_leader():
            return

        container = self.charm.model.unit.get_container(self.charm.name)
        if not container.can_connect():
            event.defer()
            return

        name = event.database
        if container.exists(f"{CATALOG_PATH}/{name}"):
                container.remove_path(path=f"{CATALOG_PATH}/{name}")
        self._update_database_connections(event.relation.name, None)
        self.charm._update(event)