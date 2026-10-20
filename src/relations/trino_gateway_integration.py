# Copyright 2026 Canonical Ltd.
# See LICENSE file for licensing details.

"""The `trino-gateway-integration` requirer endpoint: users requested by Trino Gateway.

The relation has no charm library; this module defines the Trino side of its shape. It mirrors
the provider side in the Trino Gateway charm. All fields are in the application databags.

Trino Gateway (provider) to Trino (requirer):
    `requested_users`: JSON object mapping each requested username to the ID of a Juju secret
    granted to the relation. The secret holds `username` and `password`. A user is withdrawn by
    removing its key.

Trino to Trino Gateway:
    `trino_url`: the bare `host:port` address Trino also publishes on `trino-catalog`.
    `registered_users`: JSON list of the requested usernames written to `password.db`.
    `refused_users`: JSON object mapping each refused username to a human-readable reason.

The first holder of a username keeps it. The leader records the holders as claims in the peer
application databag; every coordinator unit merges the claimed users into `password.db`.
"""

import json
import logging
from collections.abc import Collection
from typing import Any

import pydantic
from ops.framework import Object
from ops.model import ModelError, Relation

from literals import (
    GATEWAY_PENDING_USERS_KEY,
    GATEWAY_REFUSAL_EXISTING_USER,
    GATEWAY_REFUSAL_OTHER_GATEWAY,
    GATEWAY_REFUSAL_WORKER,
    GATEWAY_REFUSED_USERS_KEY,
    GATEWAY_REGISTERED_USERS_KEY,
    GATEWAY_TRINO_URL_KEY,
    GATEWAY_USER_CLAIMS_KEY,
    TRINO_GATEWAY_RELATION_NAME,
)

logger = logging.getLogger(__name__)

Refusals = dict[int, dict[str, str]]


class _GatewayAppData(pydantic.BaseModel):
    """The application databag of a Trino Gateway."""

    requested_users: dict[str, str] = {}


def _decode(value: str) -> Any:
    """Decode a databag value as JSON, falling back to the raw string."""
    try:
        return json.loads(value)
    except json.JSONDecodeError:
        return value


class TrinoGatewayRelationHandler(Object):
    """Handles the `trino-gateway-integration` relations.

    Event observation is centralized in the charm; this object exposes logic
    methods invoked by the charm reconciler.
    """

    def __init__(self, charm, relation_name: str = TRINO_GATEWAY_RELATION_NAME) -> None:
        """Construct.

        Args:
            charm: The charm to attach the hooks to.
            relation_name: The name of the relation.
        """
        super().__init__(charm, "trino-gateway-integration")
        self.charm = charm
        self.relation_name = relation_name
        self._requests: dict[int, dict[str, str]] | None = None

    @property
    def relations(self) -> list[Relation]:
        """Return the relations on the endpoint, sorted by relation ID."""
        return sorted(self.charm.model.relations[self.relation_name], key=lambda r: r.id)

    @property
    def is_related(self) -> bool:
        """Report whether any Trino Gateway is related."""
        return bool(self.relations)

    @property
    def _is_worker(self) -> bool:
        return self.charm.config.charm_function == "worker"

    def requests(self) -> dict[int, dict[str, str]]:
        """Read what each Gateway requests.

        Malformed data, or a relation without a remote application, counts as an empty request.

        Returns:
            The requests as relation ID to {username: secret ID}.
        """
        if self._requests is not None:
            return self._requests

        by_relation: dict[int, dict[str, str]] = {}
        for relation in self.relations:
            by_relation[relation.id] = {}
            if relation.app is None:
                continue
            try:
                data = relation.load(_GatewayAppData, relation.app, decoder=_decode)
            except ValueError:
                logger.warning(
                    "Ignoring malformed %s data in relation %s",
                    self.relation_name,
                    relation.id,
                )
                continue
            by_relation[relation.id] = dict(data.requested_users)
        self._requests = by_relation
        return by_relation

    def _load_claims(self) -> dict[str, int]:
        """Return the saved claims as {username: relation ID}."""
        if not self.charm.state.is_ready():
            return {}
        claims = getattr(self.charm.state, GATEWAY_USER_CLAIMS_KEY)
        return dict(claims) if isinstance(claims, dict) else {}

    def _active_claims(self) -> dict[str, int]:
        """Return the claims whose relation still requests the username."""
        by_relation = self.requests()
        return {
            username: relation_id
            for username, relation_id in self._load_claims().items()
            if username in by_relation.get(relation_id, {})
        }

    def plan_claims(self, trino_users: Collection[str]) -> Refusals:
        """Decide which relation owns each requested username. Only the leader may call this.

        Args:
            trino_users: The usernames that already exist on this Trino.

        Returns:
            The refused usernames and their reasons, by relation ID. Refusals are recomputed on
            every run and never stored.
        """
        by_relation = self.requests()
        if self._is_worker:
            return {
                relation_id: dict.fromkeys(requested, GATEWAY_REFUSAL_WORKER)
                for relation_id, requested in by_relation.items()
            }

        saved = self._load_claims()
        claims = self._active_claims()
        refusals: Refusals = {relation_id: {} for relation_id in by_relation}
        for relation_id, requested in sorted(by_relation.items()):
            for username in sorted(requested):
                if claims.get(username) == relation_id:
                    continue
                if username in trino_users:
                    refusals[relation_id][username] = GATEWAY_REFUSAL_EXISTING_USER
                elif username in claims:
                    refusals[relation_id][username] = GATEWAY_REFUSAL_OTHER_GATEWAY
                else:
                    claims[username] = relation_id

        if claims != saved:
            logger.debug(
                "Trino Gateway user claims changed: added %s, dropped %s",
                sorted(set(claims) - set(saved)),
                sorted(set(saved) - set(claims)),
            )
            setattr(self.charm.state, GATEWAY_USER_CLAIMS_KEY, claims)
        return refusals

    def claimed_credentials(self) -> dict[str, str]:
        """Read the passwords of the claimed users from the Gateway secrets.

        A user whose secret cannot be read, or lacks a password, stays pending: it is neither
        registered nor refused, and the next reconcile tries again.

        Returns:
            The password of each readable claimed user, by username.
        """
        if self._is_worker:
            return {}

        by_relation = self.requests()
        credentials = {}
        for username, relation_id in self._active_claims().items():
            password = self._read_password(by_relation[relation_id][username])
            if password is not None:
                credentials[username] = password
        return credentials

    def claimed_usernames(self) -> list[str]:
        """Return the sorted usernames currently claimed by the Gateways."""
        return [] if self._is_worker else sorted(self._active_claims())

    def _read_password(self, secret_id: str) -> str | None:
        try:
            secret = self.charm.model.get_secret(id=secret_id)
            return secret.get_content(refresh=True).get("password")
        except ModelError:
            return None

    def publish_url(self) -> None:
        """Publish the Trino URL on every relation. Only the leader may call this."""
        if self._is_worker:
            return
        url = self.charm.trino_catalog.get_url()
        if not url:
            return
        for relation in self.relations:
            relation.data[self.charm.app][GATEWAY_TRINO_URL_KEY] = url

    def publish_users(self, written: Collection[str], refusals: Refusals) -> None:
        """Publish the registered and refused users. Only the leader may call this.

        Args:
            written: The Gateway usernames that were written to `password.db`.
            refusals: The refusals planned by `plan_claims`, by relation ID.
        """
        claims = {} if self._is_worker else self._active_claims()
        pending = sorted(set(claims) - set(written))
        self._log_pending(pending)

        for relation in self.relations:
            registered = sorted(
                username
                for username, relation_id in claims.items()
                if relation_id == relation.id and username in written
            )
            refused = refusals.get(relation.id, {})
            databag = relation.data[self.charm.app]
            self._log_refusals(relation.id, databag.get(GATEWAY_REFUSED_USERS_KEY), refused)
            databag[GATEWAY_REGISTERED_USERS_KEY] = json.dumps(registered)
            databag[GATEWAY_REFUSED_USERS_KEY] = json.dumps(refused, sort_keys=True)

    def _log_pending(self, pending: list[str]) -> None:
        previous = getattr(self.charm.state, GATEWAY_PENDING_USERS_KEY) or []
        if previous == pending:
            return
        logger.debug(
            "Trino Gateway users pending: %s, no longer pending: %s",
            sorted(set(pending) - set(previous)),
            sorted(set(previous) - set(pending)),
        )
        setattr(self.charm.state, GATEWAY_PENDING_USERS_KEY, pending)

    @staticmethod
    def _log_refusals(relation_id: int, published: str | None, refused: dict[str, str]) -> None:
        try:
            previous = json.loads(published) if published else {}
        except json.JSONDecodeError:
            previous = {}
        if previous != refused:
            logger.debug(
                "Trino Gateway refusals changed in relation %s: %s", relation_id, sorted(refused)
            )
