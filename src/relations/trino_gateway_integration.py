# Copyright 2023 Canonical Ltd.
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
    `registered_users`: JSON list of the accepted usernames, sorted.
    `refused_users`: JSON object mapping each refused username to a human-readable reason.

Nothing is stored between hooks. Every hook computes the desired state from the current
relation data and secrets, writes `password.db` and publishes the reply.

Glossary:
    request: a username, with the ID of the secret holding its password, that a Gateway asks
        for in `requested_users`.
    accepted: a request that is written to `password.db`. The Gateway sees it as registered.
    refused: a request that cannot be honored. The Gateway sees it with the reason.
    pending: a request whose secret cannot be read yet. It is neither accepted nor refused,
        and a later hook picks it up.
    desired databag: the content our application databag should have on one relation.
"""

import json
import logging
from collections import Counter
from collections.abc import Callable, Mapping
from dataclasses import dataclass, field
from typing import Any, Optional

import pydantic
from ops.framework import Object
from ops.model import ModelError, Relation, SecretNotFoundError

from literals import (
    GATEWAY_REFUSAL_EXISTING_USER,
    GATEWAY_REFUSAL_INVALID_USERNAME,
    GATEWAY_REFUSAL_LONG_PASSWORD,
    GATEWAY_REFUSAL_MULTIPLE_GATEWAYS,
    GATEWAY_REFUSED_USERS_KEY,
    GATEWAY_REGISTERED_USERS_KEY,
    GATEWAY_TRINO_URL_KEY,
    PASSWORD_DB,
    TRINO_GATEWAY_RELATION_NAME,
)
from utils import has_valid_password_length, is_valid_username, write_password_db

logger = logging.getLogger(__name__)

_PUBLISHED_KEYS = (
    GATEWAY_TRINO_URL_KEY,
    GATEWAY_REGISTERED_USERS_KEY,
    GATEWAY_REFUSED_USERS_KEY,
)


@dataclass(frozen=True)
class UsersPlan:
    """The desired state computed from the current inputs.

    Attributes:
        password_db: Every user to write to `password.db` with its plaintext password.
        replies: The desired reply per relation ID, as encoded databag values. Empty for a
            worker.
        pending: The usernames still waiting for a readable secret, per relation ID.
        gateway_users: The accepted Gateway usernames, sorted.
    """

    password_db: dict[str, str]
    replies: dict[int, dict[str, str]]
    pending: dict[int, list[str]] = field(default_factory=dict)
    gateway_users: list[str] = field(default_factory=list)


def plan_users(
    is_coordinator: bool,
    trino_users: Mapping[str, str],
    requests: Mapping[int, Mapping[str, str]],
    read_password: Callable[[str], Optional[str]],
) -> UsersPlan:
    """Decide which requested users are accepted, refused or pending.

    Pure: ownership is recomputed from the inputs on every call.

    Args:
        is_coordinator: Whether this application serves clients. Workers take no part.
        trino_users: The configured or default users plus the `trino-catalog` users, with
            their plaintext passwords.
        requests: The requests per relation ID, as {username: secret ID}.
        read_password: Returns the password held by a secret, or None if it cannot be read.

    Returns:
        The plan.
    """
    if not is_coordinator:
        return UsersPlan(
            password_db=dict(trino_users), replies={relation_id: {} for relation_id in requests}
        )

    requesters = Counter(username for requested in requests.values() for username in requested)
    gateway_passwords: dict[str, str] = {}
    refused: dict[int, dict[str, str]] = {relation_id: {} for relation_id in requests}
    pending: dict[int, list[str]] = {}
    accepted: dict[int, list[str]] = {relation_id: [] for relation_id in requests}

    for relation_id, requested in requests.items():
        for username, secret_id in sorted(requested.items()):
            if not is_valid_username(username):
                refused[relation_id][username] = GATEWAY_REFUSAL_INVALID_USERNAME
            elif username in trino_users:
                refused[relation_id][username] = GATEWAY_REFUSAL_EXISTING_USER
            elif requesters[username] > 1:
                # Gateway usernames embed a hash of the model UUID and application name, so
                # two Gateways only request the same name if that contract is broken. Without
                # stored ownership there is no fair way to pick one, so none of them gets it.
                refused[relation_id][username] = GATEWAY_REFUSAL_MULTIPLE_GATEWAYS
            else:
                password = read_password(secret_id)
                if password is None:
                    pending.setdefault(relation_id, []).append(username)
                elif not has_valid_password_length(password):
                    refused[relation_id][username] = GATEWAY_REFUSAL_LONG_PASSWORD
                else:
                    gateway_passwords[username] = password
                    accepted[relation_id].append(username)

    replies = {
        relation_id: {
            GATEWAY_REGISTERED_USERS_KEY: json.dumps(sorted(accepted[relation_id])),
            GATEWAY_REFUSED_USERS_KEY: json.dumps(refused[relation_id], sort_keys=True),
        }
        for relation_id in requests
    }
    return UsersPlan(
        password_db={**trino_users, **gateway_passwords},
        replies=replies,
        pending=pending,
        gateway_users=sorted(gateway_passwords),
    )


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
        self._requests: Optional[dict[int, dict[str, str]]] = None

    @property
    def relations(self) -> list[Relation]:
        """Return the relations on the endpoint, sorted by relation ID."""
        return sorted(self.charm.model.relations[self.relation_name], key=lambda r: r.id)

    @property
    def is_related(self) -> bool:
        """Report whether any Trino Gateway is related."""
        return bool(self.relations)

    @property
    def _is_coordinator(self) -> bool:
        return self.charm.config.charm_function in ("coordinator", "all")

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

    def _read_password(self, secret_id: str) -> Optional[str]:
        """Read the password held by a granted secret.

        Args:
            secret_id: The ID of the Gateway user's secret.

        Returns:
            The password, or None if the secret or its password cannot be read.
        """
        try:
            secret = self.charm.model.get_secret(id=secret_id)
            return secret.get_content(refresh=True).get("password")
        except (SecretNotFoundError, ModelError):
            return None

    def plan(self, trino_users: Mapping[str, str]) -> UsersPlan:
        """Compute the desired users state from the current relation data and secrets.

        Args:
            trino_users: The configured or default users plus the `trino-catalog` users.

        Returns:
            The plan.
        """
        return plan_users(self._is_coordinator, trino_users, self.requests(), self._read_password)

    def reconcile(self, container, trino_users: Mapping[str, str]) -> UsersPlan:
        """Write `password.db` and, on the leader, publish the reply to every Gateway.

        Every unit writes the file itself, since the plan only depends on data every unit can
        read. Only the leader publishes, and what it confirms is what it wrote.

        Args:
            container: The Trino container.
            trino_users: The configured or default users plus the `trino-catalog` users.

        Returns:
            The plan that was applied.
        """
        plan = self.plan(trino_users)
        db_path = str(self.charm.trino_abs_path.joinpath(PASSWORD_DB))
        write_password_db(container, plan.password_db, db_path)

        for relation_id, usernames in plan.pending.items():
            logger.debug("Trino Gateway users pending in relation %s: %s", relation_id, usernames)

        if self.charm.unit.is_leader():
            self._publish(plan)
        return plan

    def _desired_databag(self, relation: Relation, plan: UsersPlan) -> dict[str, str]:
        """Build the content our application databag should have on a relation.

        Args:
            relation: The relation to build the databag for.
            plan: The users plan.

        Returns:
            The databag keys to set. A worker gets empty values, which remove the keys.
        """
        if not self._is_coordinator:
            return dict.fromkeys(_PUBLISHED_KEYS, "")

        desired = dict(plan.replies[relation.id])
        url = self.charm.trino_catalog.get_url()
        if url:
            desired[GATEWAY_TRINO_URL_KEY] = url
        return desired

    def _publish(self, plan: UsersPlan) -> None:
        """Publish the desired databag on every relation. Only called on the leader.

        Args:
            plan: The users plan.
        """
        for relation in self.relations:
            databag = relation.data[self.charm.app]
            desired = self._desired_databag(relation, plan)
            self._log_changes(relation.id, databag, desired)
            databag.update(desired)

    def _log_changes(
        self, relation_id: int, current: Mapping[str, str], desired: Mapping[str, str]
    ):
        """Log what publishing changes, derived from the current and desired databags.

        Args:
            relation_id: The ID of the relation.
            current: Our current application databag.
            desired: The desired content.
        """
        old_registered = set(_decode_or_default(current.get(GATEWAY_REGISTERED_USERS_KEY), []))
        new_registered = set(_decode_or_default(desired.get(GATEWAY_REGISTERED_USERS_KEY), []))
        old_refused = _decode_or_default(current.get(GATEWAY_REFUSED_USERS_KEY), {})
        new_refused = _decode_or_default(desired.get(GATEWAY_REFUSED_USERS_KEY), {})

        registered = sorted(new_registered - old_registered)
        withdrawn = sorted(old_registered - new_registered)
        refusals = {
            username: reason
            for username, reason in new_refused.items()
            if old_refused.get(username) != reason
        }
        if registered:
            logger.debug(
                "Registered Trino Gateway users in relation %s: %s", relation_id, registered
            )
        if withdrawn:
            logger.debug("Withdrew Trino Gateway users in relation %s: %s", relation_id, withdrawn)
        if refusals:
            logger.debug("Refused Trino Gateway users in relation %s: %s", relation_id, refusals)


def _decode_or_default(value: Optional[str], default: Any) -> Any:
    """Decode a databag value as JSON, returning the default if it is missing or malformed."""
    if not value:
        return default
    try:
        decoded = json.loads(value)
    except json.JSONDecodeError:
        return default
    return decoded if isinstance(decoded, type(default)) else default
