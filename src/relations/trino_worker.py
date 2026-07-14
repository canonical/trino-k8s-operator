# Copyright 2023 Canonical Ltd.
# See LICENSE file for licensing details.

"""Trino worker relation hooks & helpers."""

import json
import logging

from ops.charm import CharmBase
from ops.framework import Object
from ops.model import SecretNotFoundError

from literals import INT_COMMS_SECRET_RELATION_KEY, POSTGRESQL_SECRET_RELATION_KEY

logger = logging.getLogger(__name__)


class TrinoWorker(Object):
    """Defines worker functionality for the relation between the Trino coordinator and worker.

    Event observation is centralized in the charm; this object exposes logic
    methods invoked by the charm reconciler.
    """

    def __init__(self, charm: CharmBase, relation_name: str = "trino-worker") -> None:
        """Construct TrinoRelationHandler object.

        Args:
            charm: the charm for which this relation is provided
            relation_name: the name of the relation
        """
        self.charm = charm
        self.relation_name = relation_name

        super().__init__(charm, self.relation_name)

    def gather_from_coordinator(self):
        """Persist coordinator-published data from the relation databag.

        Reads the `trino-worker` relation databag via the model so the worker
        can be reconciled from current state rather than an event payload.
        Only the leader persists to peer state.
        """
        if not self.charm.unit.is_leader():
            return

        relation = self.charm.model.get_relation(self.relation_name)
        if relation is None or relation.app is None:
            return

        event_data = relation.data[relation.app]
        self.charm.state.discovery_uri = event_data.get("discovery-uri")
        self.charm.state.user_secret_id = event_data.get("user-secret-id")
        self.charm.state.catalog_config = event_data.get("catalogs")
        self.charm.state.int_comms_secret_id = event_data.get(INT_COMMS_SECRET_RELATION_KEY, "")

    def _resolve_int_comms_secret(self, secret_id: str) -> str | None:
        """Resolve the internal communication secret value by Juju secret ID.

        Args:
            secret_id: the Juju secret ID published by the coordinator.

        Returns:
            The shared secret string, or None if not yet resolvable.
        """
        try:
            secret = self.charm.model.get_secret(id=secret_id)
            value = secret.get_content(refresh=True).get("secret")
            if not value:
                logger.warning("int-comms-secret id %r has no 'secret' field", secret_id)
            return value
        except SecretNotFoundError:
            logger.warning("int-comms-secret id %r could not be resolved", secret_id)
            return None

    def get_postgresql_secrets_from_coordinator(self) -> dict:
        """Resolve PG password env vars from the coordinator's Juju secret.

        The coordinator publishes the secret id in the relation databag and
        grants the secret to the relation, so only the id crosses the databag.

        Returns:
            Dict mapping env var names to password values, empty if unavailable.
        """
        relation = self.charm.model.get_relation(self.relation_name)
        if relation is None or relation.app is None:
            return {}
        secret_id = relation.data[relation.app].get(POSTGRESQL_SECRET_RELATION_KEY)
        if not secret_id:
            return {}
        try:
            secret = self.charm.model.get_secret(id=secret_id)
            content = secret.get_content(refresh=True)
        except SecretNotFoundError:
            logger.warning("postgresql-secrets id %r could not be resolved", secret_id)
            return {}
        # The env var map is JSON-encoded under a single Juju-valid secret key.
        return json.loads(content.get("envvars", "{}"))

    def _validate(self):
        """Check if the trino worker relation is available.

        Raises:
            ValueError: if the relation is incorrectly configured.
                      : if there is no coordinator relation.
        """
        if self.model.relations["trino-coordinator"]:
            raise ValueError("Incorrect trino relation configuration.")

        if not self.model.relations["trino-worker"]:
            raise ValueError("Missing Trino coordinator relation.")
