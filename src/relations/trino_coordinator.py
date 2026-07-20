# Copyright 2023 Canonical Ltd.
# See LICENSE file for licensing details.

"""Trino coordinator relation hooks & helpers."""

import json
import logging

from ops.charm import CharmBase
from ops.framework import Object
from ops.model import SecretNotFoundError

from literals import (
    INT_COMMS_SECRET_LABEL,
    INT_COMMS_SECRET_RELATION_KEY,
    POSTGRESQL_SECRET_LABEL,
    POSTGRESQL_SECRET_RELATION_KEY,
)
from utils import generate_password

logger = logging.getLogger(__name__)


class TrinoCoordinator(Object):
    """Defines coordinator functionality for the relation between the Trino coordinator and worker.

    Event observation is centralized in the charm; this object exposes logic
    methods invoked by the charm reconciler.
    """

    def __init__(self, charm: CharmBase, relation_name: str = "trino-coordinator") -> None:
        """Construct TrinoRelationHandler object.

        Args:
            charm: the charm for which this relation is provided
            relation_name: the name of the relation
        """
        self.charm = charm
        self.relation_name = relation_name

        super().__init__(charm, self.relation_name)

    def _get_or_create_int_comms_secret(self):
        """Get or create the singleton app-owned Juju secret for internal communication.

        Reuses the existing value from peer state when present so that upgrades
        do not rotate the cluster secret.  Only generates a new value when neither
        peer state nor an existing app secret carries one.

        Returns:
            The Juju secret object, or None if the unit is not the leader.
        """
        if not self.charm.unit.is_leader():
            return None

        try:
            secret = self.charm.model.get_secret(label=INT_COMMS_SECRET_LABEL)
            return secret
        except SecretNotFoundError:
            pass

        # Reuse the value already persisted in peer state, or generate a fresh one.
        # This is relevant when upgrading the charm revision.
        existing_value = self.charm.state.int_comms_secret or generate_password()
        secret = self.charm.app.add_secret(
            {"secret": existing_value},
            label=INT_COMMS_SECRET_LABEL,
        )
        logger.info("Created Juju secret for internal comms")
        return secret

    def _get_or_create_pg_secret(self, env_vars: dict):
        """Get or refresh the app-owned Juju secret holding PostgreSQL passwords.

        Unlike the internal-comms secret, the PostgreSQL password map is dynamic:
        it changes as the provider rotates credentials or catalogs are added and
        removed. The content is refreshed so workers converge on the new revision
        via secret-changed.

        Args:
            env_vars: Mapping of env var name to password. An empty map means no
                PostgreSQL catalogs, so no secret is managed.

        Returns:
            The Juju secret object, or None when the unit is not the leader or
            there are no PostgreSQL passwords to share.
        """
        if not self.charm.unit.is_leader() or not env_vars:
            return None

        # Juju secret keys must be lowercase alphanumerics, but env var names
        # (e.g. PG_PASS_MYDB) are not, so the map is JSON-encoded under one key.
        content = {"envvars": json.dumps(env_vars, sort_keys=True)}
        try:
            secret = self.charm.model.get_secret(label=POSTGRESQL_SECRET_LABEL)
            if secret.get_content(refresh=True) != content:
                secret.set_content(content)
            return secret
        except SecretNotFoundError:
            secret = self.charm.app.add_secret(content, label=POSTGRESQL_SECRET_LABEL)
            logger.info("Created Juju secret for PostgreSQL catalog passwords")
            return secret

    def update_coordinator_relation_data(self):
        """Write coordinator data to the relation databag for workers."""
        if not self.charm.state.is_ready():
            return

        if self.charm.config.charm_function != "coordinator":
            return

        # This is a list that contains every relation to the 'trino-coordinator' endpoint
        # In theory it is not limited but in practice this will have 0 or 1 items
        coordinator_relations = self.model.relations["trino-coordinator"]

        pg_env_vars = self.charm.postgresql_catalog_handler.get_postgresql_env_vars()

        int_comms_secret = None
        pg_secret = None
        if self.charm.unit.is_leader():
            int_comms_secret = self._get_or_create_int_comms_secret()
            pg_secret = self._get_or_create_pg_secret(pg_env_vars)

        relation_data = {
            "discovery-uri": self.charm._coordinator_discovery_uri,
            "user-secret-id": self.charm.config.user_secret_id or "",
            "catalogs": self.charm.config.catalog_config or "",
        }

        for relation in coordinator_relations:
            app_databag = relation.data[self.charm.app]
            if int_comms_secret is not None:
                int_comms_secret.grant(relation)
                relation_data[INT_COMMS_SECRET_RELATION_KEY] = int_comms_secret.id
            if pg_secret is not None:
                pg_secret.grant(relation)
                relation_data[POSTGRESQL_SECRET_RELATION_KEY] = pg_secret.id
            else:
                app_databag.pop(POSTGRESQL_SECRET_RELATION_KEY, None)
            app_databag.update(relation_data)

    def _validate(self):
        """Check if the trino coordinator relation is available.

        Raises:
            ValueError: if the coordinator is not ready.
        """
        if self.model.relations["trino-worker"]:
            raise ValueError("Incorrect trino relation configuration.")

        if not self.model.relations["trino-coordinator"]:
            raise ValueError("Missing Trino worker relation.")
