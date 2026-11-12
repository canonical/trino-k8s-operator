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

The Gateway sends each password in plaintext rather than as a hash. Trino owns `password.db`,
its format and its hashing settings, so hashing stays in one place; if each relation sent
hashes, a change to Trino's hashing would need matching changes in every related charm. The
plaintext only travels inside a Juju secret granted to the relation.

Nothing is stored between hooks except `password.db` itself. Every hook computes the desired
state from the current relation data and secrets, writes `password.db` and publishes the reply.
When a secret exists but cannot be read, the user's entry in the deployed `password.db` is
kept, like the deployed settings of other relations whose secret cannot be read.

Glossary:
    request: a username, with the ID of the secret holding its password, that a Gateway asks
        for in `requested_users`.
    accepted: a request that is written to `password.db`, and that is confirmed once Trino has
        had time to reload the file. The Gateway sees it as registered.
    refused: a request that cannot be honored. The Gateway sees it with the reason.
    pending: a request whose secret cannot be read yet and that has no deployed entry. It is
        neither accepted nor refused, and a later hook picks it up.
    retained: an accepted request whose secret cannot be read, kept with its deployed entry.
    desired databag: the content our application databag should have on one relation.
"""

import json
import logging
import time
from collections import Counter
from collections.abc import Callable, Mapping
from dataclasses import dataclass, field
from typing import Any, Optional

import pydantic
from ops.framework import Object
from ops.model import ModelError, Relation, SecretNotFoundError
from ops.pebble import ConnectionError as PebbleConnectionError
from ops.pebble import PathError

from literals import (
    GATEWAY_REFUSAL_EXISTING_USER,
    GATEWAY_REFUSAL_INVALID_USERNAME,
    GATEWAY_REFUSAL_LONG_PASSWORD,
    GATEWAY_REFUSAL_MISSING_SECRET,
    GATEWAY_REFUSAL_MULTIPLE_GATEWAYS,
    GATEWAY_REFUSED_USERS_KEY,
    GATEWAY_REGISTERED_USERS_KEY,
    GATEWAY_TRINO_URL_KEY,
    PASSWORD_DB,
    PASSWORD_DB_REFRESH_SECONDS,
    TRINO_GATEWAY_RELATION_NAME,
)
from utils import SecretReadError, has_valid_password_length, is_valid_username, write_password_db

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
        password_db: Every user to write to `password.db` with its plaintext password: the
            local credentials plus the accepted Gateway credentials.
        retained_hashes: The retained Gateway users with their deployed `password.db` hash.
        replies: The desired reply per relation ID, as encoded databag values. Empty for a
            worker.
        pending: The usernames still waiting for a readable secret, per relation ID.
        gateway_users: The accepted Gateway usernames, sorted.
        read_error: The first error from a secret that exists but cannot be read.
    """

    password_db: dict[str, str]
    replies: dict[int, dict[str, str]]
    retained_hashes: dict[str, str] = field(default_factory=dict)
    pending: dict[int, list[str]] = field(default_factory=dict)
    gateway_users: list[str] = field(default_factory=list)
    read_error: Optional[SecretReadError] = None


def _refusal_before_read(
    username: str,
    secret_id: str,
    local_credentials: Mapping[str, str],
    request_count_by_username: Mapping[str, int],
) -> Optional[str]:
    """Return why a request is refused without reading its secret, or None."""
    if not is_valid_username(username):
        return GATEWAY_REFUSAL_INVALID_USERNAME
    if username in local_credentials:
        return GATEWAY_REFUSAL_EXISTING_USER
    if request_count_by_username[username] > 1:
        # Gateway usernames embed a hash of the model UUID and application name, so two
        # Gateways only request the same name if that contract is broken. Without stored
        # ownership there is no fair way to pick one, so none of them gets it.
        return GATEWAY_REFUSAL_MULTIPLE_GATEWAYS
    if not secret_id:
        return GATEWAY_REFUSAL_MISSING_SECRET
    return None


def plan_users(
    is_coordinator: bool,
    local_credentials: Mapping[str, str],
    requests: Mapping[int, Mapping[str, str]],
    read_password: Callable[[str], Optional[str]],
    deployed_hashes: Mapping[str, str],
) -> UsersPlan:
    """Decide which requested users are accepted, refused or pending.

    Pure: ownership is recomputed from the inputs on every call.

    Args:
        is_coordinator: Whether this application serves clients. Workers take no part.
        local_credentials: The configured or default users plus the `trino-catalog` users, with
            their plaintext passwords.
        requests: The requests per relation ID, as {username: secret ID}.
        read_password: Returns the password held by a secret, or None if the secret or its
            password does not exist. Raises `SecretReadError` if the secret cannot be read.
        deployed_hashes: The entries of the deployed `password.db`, as {username: hash}.

    Returns:
        The plan.
    """
    if not is_coordinator:
        return UsersPlan(
            password_db=dict(local_credentials),
            replies={relation_id: {} for relation_id in requests},
        )

    request_count_by_username = Counter(
        username for requested in requests.values() for username in requested
    )
    gateway_credentials: dict[str, str] = {}
    retained_hashes: dict[str, str] = {}
    read_error: Optional[SecretReadError] = None
    refused: dict[int, dict[str, str]] = {relation_id: {} for relation_id in requests}
    pending: dict[int, list[str]] = {}
    accepted: dict[int, list[str]] = {relation_id: [] for relation_id in requests}

    for relation_id, requested in requests.items():
        for username, secret_id in sorted(requested.items()):
            reason = _refusal_before_read(
                username, secret_id, local_credentials, request_count_by_username
            )
            if reason:
                refused[relation_id][username] = reason
                continue
            try:
                password = read_password(secret_id)
            except SecretReadError as err:
                read_error = read_error or err
                if username in deployed_hashes:
                    retained_hashes[username] = deployed_hashes[username]
                    accepted[relation_id].append(username)
                else:
                    pending.setdefault(relation_id, []).append(username)
                continue
            if password is None:
                pending.setdefault(relation_id, []).append(username)
            elif not has_valid_password_length(password):
                refused[relation_id][username] = GATEWAY_REFUSAL_LONG_PASSWORD
            else:
                gateway_credentials[username] = password
                accepted[relation_id].append(username)

    replies = {
        relation_id: {
            GATEWAY_REGISTERED_USERS_KEY: json.dumps(sorted(accepted[relation_id])),
            GATEWAY_REFUSED_USERS_KEY: json.dumps(refused[relation_id], sort_keys=True),
        }
        for relation_id in requests
    }
    return UsersPlan(
        password_db={**local_credentials, **gateway_credentials},
        replies=replies,
        retained_hashes=retained_hashes,
        pending=pending,
        gateway_users=sorted([*gateway_credentials, *retained_hashes]),
        read_error=read_error,
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
        self.read_error: Optional[SecretReadError] = None

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
            The password, or None if the secret or its password does not exist.

        Raises:
            SecretReadError: when `secret-get` fails for another reason.
        """
        try:
            secret = self.charm.model.get_secret(id=secret_id)
            return secret.get_content(refresh=True).get("password")
        except SecretNotFoundError:
            return None
        except ModelError as err:
            raise SecretReadError(self.relation_name, err) from err

    def _deployed_hashes(self, container) -> dict[str, str]:
        """Read the entries of the deployed `password.db`.

        Args:
            container: The Trino container.

        Returns:
            The entries as {username: hash}, empty if the file cannot be read.
        """
        db_path = str(self.charm.trino_abs_path.joinpath(PASSWORD_DB))
        try:
            content = container.pull(db_path).read()
        except (PathError, PebbleConnectionError):
            return {}
        entries = (line.partition(":") for line in content.splitlines())
        return {user: password_hash for user, sep, password_hash in entries if sep}

    def plan(self, container, local_credentials: Mapping[str, str]) -> UsersPlan:
        """Compute the desired users state from the current relation data and secrets.

        Args:
            container: The Trino container.
            local_credentials: The configured or default users plus the `trino-catalog` users.

        Returns:
            The plan.
        """
        return plan_users(
            self._is_coordinator,
            local_credentials,
            self.requests(),
            self._read_password,
            self._deployed_hashes(container),
        )

    def reconcile(self, container, local_credentials: Mapping[str, str]) -> UsersPlan:
        """Write `password.db` and, on the leader, publish the reply to every Gateway.

        Every unit writes the file itself, since the plan only depends on data every unit can
        read. Only the leader publishes, and what it confirms is what it wrote, once Trino has
        had time to reload it.

        Args:
            container: The Trino container.
            local_credentials: The configured or default users plus the `trino-catalog` users.

        Returns:
            The plan that was applied.
        """
        plan = self.plan(container, local_credentials)
        self.read_error = plan.read_error
        if plan.retained_hashes:
            logger.warning(
                "Keeping deployed Trino Gateway users whose secret cannot be read: %s",
                sorted(plan.retained_hashes),
            )
        db_path = str(self.charm.trino_abs_path.joinpath(PASSWORD_DB))
        salt_key = f"{self.charm.model.uuid}/{self.charm.app.name}"
        if write_password_db(
            container, plan.password_db, db_path, salt_key, plan.retained_hashes
        ).pushed:
            logger.debug("Updated %s", db_path)

        if self.charm.unit.is_leader():
            for relation_id, usernames in plan.pending.items():
                logger.debug(
                    "Trino Gateway users pending in relation %s: %s", relation_id, usernames
                )
            if self._is_coordinator and self.relations:
                self._wait_for_reload(container, db_path)
            self._publish(plan)
        return plan

    def _wait_for_reload(self, container, db_path: str) -> None:
        """Wait until Trino has had time to reload `password.db` since its content last changed.

        Trino re-reads the file lazily, at most once per refresh period, and a confirmation
        must mean the user can log in. The file is only pushed when its content changes, so its
        modification time is when the current content landed; a retried hook waits only for
        what remains. Waiting on the leader alone relies on the coordinator running a single
        unit.

        Args:
            container: The Trino container.
            db_path: The path to `password.db`.
        """
        last_modified = container.list_files(db_path)[0].last_modified.timestamp()
        delay = last_modified + PASSWORD_DB_REFRESH_SECONDS + 1 - time.time()
        if delay > 0:
            logger.debug("Waiting %.1fs for Trino to reload %s", delay, db_path)
            time.sleep(delay)

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
        desired.update(self._desired_url())
        return desired

    def _desired_url(self) -> dict[str, str]:
        """Build the `trino_url` part of our databag, the same URL `trino-catalog` publishes.

        Returns:
            The URL key to set, or nothing when there is no URL, which keeps the published one.
        """
        url = self.charm.trino_catalog.get_url()
        return {GATEWAY_TRINO_URL_KEY: url} if url else {}

    def publish_url(self) -> None:
        """Keep `trino_url` current when the local credentials cannot be read.

        Users are left as they are: `password.db` is not written and `registered_users` and
        `refused_users` are untouched. Only the leader publishes, and a worker clears our keys.
        """
        if not self.charm.unit.is_leader():
            return
        for relation in self.relations:
            if self._is_coordinator:
                desired = self._desired_url()
            else:
                desired = dict.fromkeys(_PUBLISHED_KEYS, "")
            relation.data[self.charm.app].update(desired)

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
