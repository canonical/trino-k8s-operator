# Copyright 2023 Canonical Ltd.
# See LICENSE file for licensing details.

"""Trino coordinator relation hooks & helpers."""

import json
import logging

from ops.charm import CharmBase
from ops.framework import Object
from ops.model import SecretNotFoundError

from literals import INT_COMMS_SECRET_LABEL, INT_COMMS_SECRET_RELATION_KEY
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
        if self.charm.unit.is_leader():
            int_comms_secret = self._get_or_create_int_comms_secret()

        relation_data = {
            "discovery-uri": self.charm._coordinator_discovery_uri,
            "user-secret-id": self.charm.config.user_secret_id or "",
            "catalogs": self.charm.config.catalog_config or "",
            "postgresql-secrets": json.dumps(pg_env_vars),
        }

        for relation in coordinator_relations:
            if int_comms_secret is not None:
                int_comms_secret.grant(relation)
                relation_data[INT_COMMS_SECRET_RELATION_KEY] = int_comms_secret.id
            relation.data[self.charm.app].update(relation_data)

    def _validate(self):
        """Check if the trino coordinator relation is available.

        Raises:
            ValueError: if the coordinator is not ready.
        """
        if self.model.relations["trino-worker"]:
            raise ValueError("Incorrect trino relation configuration.")

        if not self.model.relations["trino-coordinator"]:
            raise ValueError("Missing Trino worker relation.")
