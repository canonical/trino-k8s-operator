#!/usr/bin/env python3
# Copyright 2023 Canonical Ltd.
# See LICENSE file for licensing details.

"""Unit tests for the Trino Gateway integration."""

import dataclasses
import json
import logging
import os
import time
from unittest import mock

import bcrypt
import pytest
from ops.model import ActiveStatus, BlockedStatus
from ops.pebble import PathError
from ops.testing import Mount, Secret

import literals
import utils
from literals import INT_COMMS_SECRET_LABEL, PASSWORD_DB_REFRESH_SECONDS, TRUSTSTORE_SECRET_LABEL
from relations.trino_gateway_integration import plan_users
from tests.unit.helpers import (
    build_coordinator_state,
    build_worker_state,
    carry_forward,
    gateway_relation,
    ingress_relation,
    observer_secret,
    trino_container,
)

CATALOG_URL = "trino-k8s.trino-model.svc.cluster.local:8080"
GATEWAY_USER = "gw-0123456789abcdef-trino-gateway-k8s-7"  # nosec B105
GATEWAY_PASSWORD = "gateway-password"  # nosec B105
TRINO_USERS = {"existing": "existing-password"}  # nosec B105
SALT_KEY = "model-uuid/trino-k8s"
DB_PATH = "/etc/password.db"


def _gateway_secret(username=GATEWAY_USER, password=GATEWAY_PASSWORD):
    """Build the secret a Gateway grants for one of its users."""
    return observer_secret({"username": username, "password": password})


def _planner(requests, secrets=None, users=TRINO_USERS, is_coordinator=True):
    """Run the planner with a mapping of readable secrets."""
    secrets = secrets or {}
    return plan_users(is_coordinator, users, requests, secrets.get)


def _password_db(path):
    """Parse `password.db` into a mapping of username to hash."""
    return dict(line.split(":", 1) for line in (path / "password.db").read_text().splitlines())


def _container(tmp_path):
    return trino_container(mounts={"home": Mount(location="/usr/lib/trino/etc", source=tmp_path)})


class _FakeContainer:
    """A container whose files live in memory, counting pushes."""

    def __init__(self, files=None):
        self.files = dict(files or {})
        self.pushes = 0

    def pull(self, path):
        if path not in self.files:
            raise PathError("not-found", f"stat {path}: no such file or directory")
        return mock.Mock(read=mock.Mock(return_value=self.files[path]))

    def push(self, path, content, **kwargs):
        self.files[path] = content
        self.pushes += 1


def _write(container, credentials):
    return utils.write_password_db(container, credentials, DB_PATH, SALT_KEY)


def _reply(state, relation):
    return state.get_relation(relation.id).local_app_data


def _with_requests(state, relation_id, requested):
    """Return the state with what the Gateway on a relation requests replaced."""
    relation = state.get_relation(relation_id)
    updated = dataclasses.replace(
        relation, remote_app_data={"requested_users": json.dumps(requested)}
    )
    return dataclasses.replace(state, relations=(state.relations - {relation}) | {updated})


def test_planner_accepts_user_with_readable_secret():
    """A requested user whose secret is readable is accepted and registered."""
    plan = _planner({1: {"gw-a": "s1"}}, {"s1": "pw"})

    assert plan.password_db == {**TRINO_USERS, "gw-a": "pw"}
    assert plan.replies[1] == {"registered_users": '["gw-a"]', "refused_users": "{}"}
    assert plan.gateway_users == ["gw-a"]
    assert not plan.pending


def test_planner_encodes_replies_sorted():
    """Registered users are a sorted list and refused users have sorted keys."""
    plan = _planner(
        {1: {"gw-b": "s2", "gw-a": "s1", "existing": "s3", "bad:name": "s4"}},
        {"s1": "pw", "s2": "pw", "s3": "pw", "s4": "pw"},
    )

    assert plan.replies[1]["registered_users"] == '["gw-a", "gw-b"]'
    assert json.loads(plan.replies[1]["refused_users"]) == {
        "bad:name": "invalid username",
        "existing": "username already exists on this Trino",
    }
    assert plan.replies[1]["refused_users"].index("bad:name") < plan.replies[1][
        "refused_users"
    ].index("existing")


@pytest.mark.parametrize("name", ["existing", "trino", "catalog-user"])
def test_planner_refuses_existing_users(name):
    """Configured, default and trino-catalog users cannot be requested."""
    users = {"existing": "a", "trino": "b", "catalog-user": "c"}

    plan = _planner({1: {name: "s1"}}, {"s1": "other"}, users=users)

    assert json.loads(plan.replies[1]["refused_users"]) == {
        name: "username already exists on this Trino"
    }
    assert plan.password_db == users
    assert plan.gateway_users == []


def test_planner_refuses_name_requested_by_several_gateways():
    """A name requested on more than one relation is refused on all of them."""
    plan = _planner(
        {1: {"gw-a": "s1", "gw-b": "s2"}, 2: {"gw-a": "s3"}},
        {"s1": "one", "s2": "two", "s3": "three"},
    )

    reason = "username requested by more than one Trino Gateway"
    assert json.loads(plan.replies[1]["refused_users"]) == {"gw-a": reason}
    assert json.loads(plan.replies[2]["refused_users"]) == {"gw-a": reason}
    assert plan.replies[1]["registered_users"] == '["gw-b"]'
    assert plan.replies[2]["registered_users"] == "[]"
    assert "gw-a" not in plan.password_db


@pytest.mark.parametrize("name", ["", "a:b", "a b", "a\nb", "a\tb", "a\x00b", "a\rb"])
def test_planner_refuses_invalid_usernames(name):
    """Names that could corrupt `password.db` are refused."""
    plan = _planner({1: {name: "s1"}}, {"s1": "pw"})

    assert json.loads(plan.replies[1]["refused_users"]) == {name: "invalid username"}
    assert plan.password_db == TRINO_USERS


def test_planner_leaves_unreadable_secret_pending():
    """A user whose secret cannot be read is neither registered nor refused."""
    plan = _planner({1: {"gw-a": "missing", "gw-b": "s1"}}, {"s1": "pw"})

    assert plan.pending == {1: ["gw-a"]}
    assert plan.replies[1] == {"registered_users": '["gw-b"]', "refused_users": "{}"}
    assert "gw-a" not in plan.password_db


@pytest.mark.parametrize("password", ["x" * 73, "é" * 37])
def test_planner_refuses_password_longer_than_72_bytes(password):
    """Bcrypt cannot hash more than 72 bytes, counted in UTF-8."""
    plan = _planner({1: {"gw-a": "s1"}}, {"s1": password})

    assert json.loads(plan.replies[1]["refused_users"]) == {
        "gw-a": "password longer than 72 bytes"
    }
    assert "gw-a" not in plan.password_db


def test_planner_accepts_password_of_72_bytes():
    """The 72-byte limit is inclusive."""
    plan = _planner({1: {"gw-a": "s1"}}, {"s1": "x" * 72})

    assert plan.gateway_users == ["gw-a"]


def test_planner_worker_takes_no_part():
    """A worker accepts, refuses and leaves pending nothing."""
    plan = _planner({1: {"gw-a": "s1", "existing": "s2"}}, {"s1": "pw"}, is_coordinator=False)

    assert plan.password_db == TRINO_USERS
    assert plan.replies == {1: {}}
    assert not plan.pending
    assert plan.gateway_users == []


def test_writer_pushes_one_hash_per_user():
    """The file is built in memory and pushed once, with verifiable bcrypt hashes."""
    container = mock.Mock()
    credentials = {"alice": "alice-pw", "bob": "bob-pw"}  # nosec B105

    result = _write(container, credentials)

    assert result == utils.PasswordDbWrite(written=["alice", "bob"], pushed=True)
    container.push.assert_called_once()
    (path, content), kwargs = container.push.call_args
    assert path == "/etc/password.db"
    assert kwargs == {"make_dirs": True, "permissions": 0o644}
    container.exec.assert_not_called()
    hashes = dict(line.split(":", 1) for line in content.splitlines())
    assert set(hashes) == {"alice", "bob"}
    for user, password in credentials.items():
        assert bcrypt.checkpw(password.encode(), hashes[user].encode())
    assert content.endswith("\n")


def test_writer_hashes_with_production_cost(monkeypatch):
    """Production hashes are bcrypt `$2b$` at cost 10."""
    monkeypatch.setattr("utils.BCRYPT_COST", literals.BCRYPT_COST)
    container = mock.Mock()

    _write(container, {"alice": "pw"})

    assert container.push.call_args.args[1].startswith("alice:$2b$10$")


def test_writer_pushes_empty_file_without_users():
    """Trino must always find the file, even without users."""
    container = _FakeContainer()

    assert _write(container, {}) == utils.PasswordDbWrite(written=[], pushed=True)

    assert container.files == {DB_PATH: ""}


def test_writer_skips_unstorable_users_without_logging_passwords(caplog):
    """Invalid names and over-long passwords are skipped and warned about by name only."""
    container = mock.Mock()
    credentials = {
        "good": "good-pw",
        "bad:name": "colon-password",
        "long": "long-password" + "x" * 80,
    }  # nosec B105

    with caplog.at_level(logging.DEBUG):
        written = _write(container, credentials).written

    assert written == ["good"]
    assert container.push.call_args.args[1].count("\n") == 1
    assert "bad:name" in caplog.text
    assert "'long'" in caplog.text
    assert "colon-password" not in caplog.text
    assert "long-password" not in caplog.text


def test_salt_is_stable_per_key_and_username():
    """The same key and username always give the same salt; anything else changes it."""
    salt = utils._derive_bcrypt_salt(SALT_KEY, "alice")

    assert salt == utils._derive_bcrypt_salt(SALT_KEY, "alice")
    assert salt != utils._derive_bcrypt_salt(SALT_KEY, "bob")
    assert salt != utils._derive_bcrypt_salt("other-model/trino-k8s", "alice")


def test_salt_is_a_valid_bcrypt_salt_with_the_configured_cost(monkeypatch):
    """Bcrypt keeps the derived salt as given and the hash verifies."""
    salt = utils._derive_bcrypt_salt(SALT_KEY, "alice")
    assert salt.startswith(b"$2b$04$")
    assert len(salt) == 29

    password_hash = bcrypt.hashpw(b"pw", salt)
    assert password_hash.startswith(salt)
    assert bcrypt.checkpw(b"pw", password_hash)

    monkeypatch.setattr("utils.BCRYPT_COST", literals.BCRYPT_COST)
    assert utils._derive_bcrypt_salt(SALT_KEY, "alice").startswith(b"$2b$10$")


def test_same_password_gives_different_hashes_per_user():
    """Two users with the same password do not share a hash."""
    container = _FakeContainer()

    _write(container, {"alice": "pw", "bob": "pw"})

    hashes = dict(line.split(":", 1) for line in container.files[DB_PATH].splitlines())
    assert hashes["alice"] != hashes["bob"]


def test_writer_does_not_push_unchanged_credentials():
    """Writing the same credentials twice pushes once."""
    container = _FakeContainer()
    credentials = {"alice": "alice-pw", "bob": "bob-pw"}  # nosec B105

    assert _write(container, credentials).pushed
    assert _write(container, credentials) == utils.PasswordDbWrite(
        written=["alice", "bob"], pushed=False
    )
    assert container.pushes == 1


@pytest.mark.parametrize(
    "changed",
    [
        {"alice": "rotated-pw"},
        {"alice": "alice-pw", "carol": "carol-pw"},
        {},
    ],
    ids=["rotated", "added", "removed"],
)
def test_writer_pushes_changed_credentials(changed):
    """A rotated password, an added user and a removed user each change the file."""
    container = _FakeContainer()
    _write(container, {"alice": "alice-pw"})

    assert _write(container, changed).pushed
    assert container.pushes == 2


def test_writer_pushes_missing_file():
    """A missing file counts as empty, so even an empty database is pushed."""
    container = _FakeContainer()

    assert _write(container, {"alice": "pw"}).pushed
    assert DB_PATH in container.files


@pytest.mark.parametrize(
    "credentials",
    [{123: "pw"}, {True: "pw"}, {"num-password": 123}, {"none-password": None}],
)
def test_writer_skips_non_string_users(caplog, credentials):
    """YAML can produce non-string names and passwords; they are skipped, never raise."""
    container = mock.Mock()

    with caplog.at_level(logging.WARNING):
        written = _write(container, {"good": "pw", **credentials}).written

    assert written == ["good"]
    assert "Skipping user" in caplog.text


def test_invalid_usernames_lists_offending_users_sorted():
    """Invalid names, non-strings and unstorable passwords are listed by name only."""
    credentials = {
        "good": "pw",
        "b:ad": "pw",
        123: "pw",
        "long": "x" * 73,
        "num": 5,
    }  # nosec B105

    assert utils.invalid_usernames(credentials) == ["123", "b:ad", "long", "num"]


def test_invalid_configured_users_block_but_others_are_written(ctx, tmp_path):
    """Invalid users in `user-secret-id` block the charm; every other user is still written."""
    secret = _gateway_secret()
    gateway = gateway_relation({GATEWAY_USER: secret.id})
    operator_users = observer_secret(
        {"users": "good: good-pw\nbad:name: colon-pw\n123: number-pw\nnone-pw:\n"}
    )
    state_in, _ = build_coordinator_state(
        config={"user-secret-id": operator_users.id},
        container=_container(tmp_path),
        extra_relations=(gateway,),
        extra_secrets=(secret, operator_users),
    )

    state_out = ctx.run(ctx.on.relation_changed(gateway), state_in)

    assert set(_password_db(tmp_path)) == {"good", GATEWAY_USER}
    assert state_out.unit_status == BlockedStatus(
        "invalid users in user secret: 123, bad:name, none-pw"
    )
    for password in ("good-pw", "colon-pw", "number-pw"):
        assert password not in state_out.unit_status.message
    assert json.loads(_reply(state_out, gateway)["registered_users"]) == [GATEWAY_USER]


def test_all_mode_writes_accepted_users_and_replies(ctx, tmp_path):
    """Accepted Gateway users are written to `password.db` and confirmed to the Gateway."""
    secret = _gateway_secret()
    gateway = gateway_relation({GATEWAY_USER: secret.id})
    state_in, _ = build_coordinator_state(
        container=_container(tmp_path), extra_relations=(gateway,), extra_secrets=(secret,)
    )

    state_out = ctx.run(ctx.on.relation_changed(gateway), state_in)

    db = _password_db(tmp_path)
    assert set(db) == {"example_user", "another_user", GATEWAY_USER}
    assert bcrypt.checkpw(GATEWAY_PASSWORD.encode(), db[GATEWAY_USER].encode())
    assert _reply(state_out, gateway) == {
        "trino_url": CATALOG_URL,
        "registered_users": json.dumps([GATEWAY_USER]),
        "refused_users": "{}",
    }
    assert state_out.unit_status == ActiveStatus("Status check: UP")


def test_coordinator_function_writes_accepted_users(ctx, tmp_path):
    """A coordinator application serves Gateway users like an `all` application."""
    secret = _gateway_secret()
    gateway = gateway_relation({GATEWAY_USER: secret.id})
    state_in, _ = build_coordinator_state(
        config={"charm-function": "all"},
        container=_container(tmp_path),
        extra_relations=(gateway,),
        extra_secrets=(secret,),
    )

    ctx.run(ctx.on.config_changed(), state_in)

    assert GATEWAY_USER in _password_db(tmp_path)


def test_existing_user_is_refused_and_keeps_credentials(ctx, tmp_path):
    """Requesting a configured user is refused and its password is unchanged."""
    secret = _gateway_secret(username="example_user", password="stolen")  # nosec
    gateway = gateway_relation({"example_user": secret.id})
    state_in, _ = build_coordinator_state(
        container=_container(tmp_path), extra_relations=(gateway,), extra_secrets=(secret,)
    )

    state_out = ctx.run(ctx.on.config_changed(), state_in)

    db = _password_db(tmp_path)
    assert bcrypt.checkpw(b"ubuntu123", db["example_user"].encode())
    reply = _reply(state_out, gateway)
    assert reply["registered_users"] == "[]"
    assert json.loads(reply["refused_users"]) == {
        "example_user": "username already exists on this Trino"
    }


def test_operator_user_with_gateway_prefix_wins(ctx, tmp_path):
    """A user added to `user-secret-id` after a Gateway user of that name takes it over."""
    secret = _gateway_secret(password="gateway-pw")  # nosec
    gateway = gateway_relation({GATEWAY_USER: secret.id})
    operator_users = observer_secret({"users": f"{GATEWAY_USER}: operator-pw\n"})
    state_in, _ = build_coordinator_state(
        config={"user-secret-id": operator_users.id},
        container=_container(tmp_path),
        extra_relations=(gateway,),
        extra_secrets=(secret, operator_users),
    )

    state_out = ctx.run(ctx.on.config_changed(), state_in)

    assert bcrypt.checkpw(b"operator-pw", _password_db(tmp_path)[GATEWAY_USER].encode())
    assert _reply(state_out, gateway)["registered_users"] == "[]"


def test_name_requested_by_two_gateways_is_refused_on_both(ctx, tmp_path):
    """Without stored ownership, a name requested twice is given to nobody."""
    secret = _gateway_secret()
    first = gateway_relation({GATEWAY_USER: secret.id}, remote_app_name="gateway-a")
    second = gateway_relation({GATEWAY_USER: secret.id}, remote_app_name="gateway-b")
    state_in, _ = build_coordinator_state(
        container=_container(tmp_path), extra_relations=(first, second), extra_secrets=(secret,)
    )

    state_out = ctx.run(ctx.on.config_changed(), state_in)

    assert GATEWAY_USER not in _password_db(tmp_path)
    for relation in (first, second):
        reply = _reply(state_out, relation)
        assert reply["registered_users"] == "[]"
        assert json.loads(reply["refused_users"]) == {
            GATEWAY_USER: "username requested by more than one Trino Gateway"
        }


def test_non_leader_writes_password_db_but_no_relation_data(ctx, tmp_path):
    """Every unit writes `password.db`, only the leader publishes."""
    secret = _gateway_secret()
    gateway = gateway_relation({GATEWAY_USER: secret.id})
    int_comms = Secret(
        tracked_content={"secret": "int-comms"},  # nosec
        owner="app",
        label=INT_COMMS_SECRET_LABEL,
    )
    truststore = Secret(
        tracked_content={"password": "truststore"},  # nosec
        owner="app",
        label=TRUSTSTORE_SECRET_LABEL,
    )
    state_in, _ = build_coordinator_state(
        leader=False,
        container=_container(tmp_path),
        extra_relations=(gateway,),
        extra_secrets=(secret, int_comms, truststore),
    )

    state_out = ctx.run(ctx.on.relation_changed(gateway), state_in)

    assert GATEWAY_USER in _password_db(tmp_path)
    assert _reply(state_out, gateway) == {}


def test_withdrawn_user_is_removed(ctx, tmp_path):
    """Removing the key from `requested_users` removes the user and its confirmation."""
    secret = _gateway_secret()
    gateway = gateway_relation({GATEWAY_USER: secret.id})
    state_in, _ = build_coordinator_state(
        container=_container(tmp_path), extra_relations=(gateway,), extra_secrets=(secret,)
    )
    configured = carry_forward(ctx.run(ctx.on.relation_changed(gateway), state_in))
    assert GATEWAY_USER in _password_db(tmp_path)

    withdrawn = _with_requests(configured, gateway.id, {})
    state_out = ctx.run(ctx.on.relation_changed(withdrawn.get_relation(gateway.id)), withdrawn)

    assert GATEWAY_USER not in _password_db(tmp_path)
    assert _reply(state_out, gateway)["registered_users"] == "[]"


def test_relation_broken_removes_users(ctx, tmp_path):
    """Breaking the relation withdraws its users from `password.db`."""
    secret = _gateway_secret()
    gateway = gateway_relation({GATEWAY_USER: secret.id})
    state_in, _ = build_coordinator_state(
        container=_container(tmp_path), extra_relations=(gateway,), extra_secrets=(secret,)
    )
    configured = carry_forward(ctx.run(ctx.on.relation_changed(gateway), state_in))
    assert GATEWAY_USER in _password_db(tmp_path)

    ctx.run(ctx.on.relation_broken(configured.get_relation(gateway.id)), configured)

    assert set(_password_db(tmp_path)) == {"example_user", "another_user"}


def test_secret_rotation_updates_password(ctx, tmp_path):
    """A rotated Gateway password is written on the next hook."""
    secret = Secret(
        tracked_content={"username": GATEWAY_USER, "password": GATEWAY_PASSWORD},
        latest_content={"username": GATEWAY_USER, "password": "rotated-password"},  # nosec
        owner=None,
    )
    gateway = gateway_relation({GATEWAY_USER: secret.id})
    state_in, _ = build_coordinator_state(
        container=_container(tmp_path), extra_relations=(gateway,), extra_secrets=(secret,)
    )

    ctx.run(ctx.on.secret_changed(secret), state_in)

    assert bcrypt.checkpw(b"rotated-password", _password_db(tmp_path)[GATEWAY_USER].encode())


def test_unreadable_secret_leaves_user_pending(ctx, tmp_path):
    """A secret that cannot be read yet leaves the user out of `password.db` and the reply."""
    gateway = gateway_relation({GATEWAY_USER: "secret:unknown"})
    state_in, _ = build_coordinator_state(
        container=_container(tmp_path), extra_relations=(gateway,)
    )

    state_out = ctx.run(ctx.on.relation_changed(gateway), state_in)

    assert GATEWAY_USER not in _password_db(tmp_path)
    assert _reply(state_out, gateway) == {
        "trino_url": CATALOG_URL,
        "registered_users": "[]",
        "refused_users": "{}",
    }


@pytest.mark.parametrize("data", ["not json {", '["a"]', '{"gw-a": null}', '"text"'])
def test_malformed_data_counts_as_empty_request(ctx, tmp_path, caplog, data):
    """Malformed Gateway data is warned about once, without values, and withdraws its users."""
    gateway = gateway_relation()
    gateway = dataclasses.replace(gateway, remote_app_data={"requested_users": data})
    state_in, _ = build_coordinator_state(
        container=_container(tmp_path), extra_relations=(gateway,)
    )

    with caplog.at_level(logging.WARNING):
        state_out = ctx.run(ctx.on.relation_changed(gateway), state_in)

    warnings = [r for r in caplog.records if "malformed" in r.getMessage()]
    assert len(warnings) == 1
    assert str(gateway.id) in warnings[0].getMessage()
    assert data not in caplog.text
    assert _reply(state_out, gateway)["registered_users"] == "[]"


def test_url_follows_ingress(ctx, tmp_path):
    """The published URL is the one `trino-catalog` publishes, including with ingress."""
    gateway = gateway_relation({})
    state_in, _ = build_coordinator_state(
        container=_container(tmp_path),
        extra_relations=(gateway, ingress_relation("https://trino.example")),
    )

    state_out = ctx.run(ctx.on.config_changed(), state_in)

    assert _reply(state_out, gateway)["trino_url"] == "trino.example:443"

    without_ingress = carry_forward(state_out)
    without_ingress = dataclasses.replace(
        without_ingress,
        relations={r for r in without_ingress.relations if r.endpoint != "ingress"},
    )
    state_out = ctx.run(ctx.on.config_changed(), without_ingress)

    assert _reply(state_out, gateway)["trino_url"] == CATALOG_URL


def test_missing_url_keeps_published_url(ctx, tmp_path):
    """Without a URL, the existing `trino_url` is left untouched."""
    gateway = gateway_relation({}, local_app_data={"trino_url": "old.example:8080"})
    state_in, _ = build_coordinator_state(
        container=_container(tmp_path), extra_relations=(gateway,)
    )

    with mock.patch(
        "relations.trino_catalog.TrinoCatalogRelationHandler.get_url", return_value=None
    ):
        state_out = ctx.run(ctx.on.config_changed(), state_in)

    assert _reply(state_out, gateway)["trino_url"] == "old.example:8080"
    assert _reply(state_out, gateway)["registered_users"] == "[]"


def test_later_reconcile_error_still_writes_users_and_replies(ctx, tmp_path):
    """A configuration error after the users step does not hold back Gateway users."""
    secret = _gateway_secret()
    gateway = gateway_relation({GATEWAY_USER: secret.id})
    state_in, _ = build_coordinator_state(
        config={"additional-jvm-options": "-Dhttp.proxyHost=bad"},
        container=_container(tmp_path),
        extra_relations=(gateway,),
        extra_secrets=(secret,),
    )

    with mock.patch("charm.TrinoK8SCharm._build_base_environment") as build:
        from utils import ProxyConfigError

        build.side_effect = ProxyConfigError("bad proxy")
        state_out = ctx.run(ctx.on.config_changed(), state_in)

    assert GATEWAY_USER in _password_db(tmp_path)
    assert json.loads(_reply(state_out, gateway)["registered_users"]) == [GATEWAY_USER]
    assert state_out.get_container("trino").plan.to_dict() == {}


PUBLISHED_USERS = {"registered_users": '["kept"]', "refused_users": '{"gw-x": "reason"}'}


def test_unreadable_user_secret_keeps_url_in_sync(ctx, tmp_path, gateway_sleep):
    """Without readable configured users, only `trino_url` follows `trino-catalog`."""
    secret = _gateway_secret()
    gateway = gateway_relation(
        {GATEWAY_USER: secret.id},
        local_app_data={"trino_url": CATALOG_URL, **PUBLISHED_USERS},
    )
    state_in, _ = build_coordinator_state(
        config={"user-secret-id": "secret:unreadable"},
        container=_container(tmp_path),
        extra_relations=(gateway, ingress_relation("https://trino.example")),
        extra_secrets=(secret,),
    )

    state_out = ctx.run(ctx.on.config_changed(), state_in)

    assert _reply(state_out, gateway) == {"trino_url": "trino.example:443", **PUBLISHED_USERS}
    assert not (tmp_path / "password.db").exists()
    assert state_out.get_container("trino").plan.to_dict() == {}
    assert isinstance(state_out.unit_status, BlockedStatus)
    gateway_sleep.assert_not_called()


def test_unreadable_user_secret_follows_ingress_like_trino_catalog(ctx, tmp_path):
    """With an ingress change and an unreadable user secret, both relations agree."""
    gateway = gateway_relation({}, local_app_data={"trino_url": CATALOG_URL, **PUBLISHED_USERS})
    state_in, _ = build_coordinator_state(
        config={"user-secret-id": "secret:unreadable"},
        container=_container(tmp_path),
        extra_relations=(gateway, ingress_relation("https://trino.example")),
    )

    state_out = ctx.run(ctx.on.config_changed(), state_in)

    assert _reply(state_out, gateway)["trino_url"] == "trino.example:443"
    assert {k: _reply(state_out, gateway)[k] for k in PUBLISHED_USERS} == PUBLISHED_USERS


def test_unreadable_user_secret_without_url_keeps_published_url(ctx, tmp_path):
    """Without a URL, the existing `trino_url` is left untouched in the URL-only path."""
    published = {"trino_url": "old.example:8080", **PUBLISHED_USERS}
    gateway = gateway_relation({}, local_app_data=published)
    state_in, _ = build_coordinator_state(
        config={"user-secret-id": "secret:unreadable"},
        container=_container(tmp_path),
        extra_relations=(gateway,),
    )

    with mock.patch(
        "relations.trino_catalog.TrinoCatalogRelationHandler.get_url", return_value=None
    ):
        state_out = ctx.run(ctx.on.config_changed(), state_in)

    assert _reply(state_out, gateway) == published


def test_unreadable_user_secret_non_leader_writes_nothing(ctx, tmp_path):
    """A non-leader neither writes `password.db` nor publishes in the URL-only path."""
    gateway = gateway_relation({}, local_app_data={"trino_url": "old.example:8080"})
    int_comms = Secret(
        tracked_content={"secret": "int-comms"},  # nosec
        owner="app",
        label=INT_COMMS_SECRET_LABEL,
    )
    truststore = Secret(
        tracked_content={"password": "truststore"},  # nosec
        owner="app",
        label=TRUSTSTORE_SECRET_LABEL,
    )
    state_in, _ = build_coordinator_state(
        leader=False,
        config={"user-secret-id": "secret:unreadable"},
        container=_container(tmp_path),
        extra_relations=(gateway, ingress_relation("https://trino.example")),
        extra_secrets=(int_comms, truststore),
    )

    state_out = ctx.run(ctx.on.config_changed(), state_in)

    assert _reply(state_out, gateway) == {"trino_url": "old.example:8080"}
    assert not (tmp_path / "password.db").exists()


def test_unreadable_user_secret_worker_clears_keys(ctx, tmp_path):
    """A worker clears our keys in the URL-only path too."""
    stale = {"trino_url": "old:8080", **PUBLISHED_USERS}
    gateway = gateway_relation({}, local_app_data=stale)
    state_in, _ = build_worker_state(container=_container(tmp_path), extra_relations=(gateway,))

    with mock.patch(
        "charm.TrinoK8SCharm._compute_credentials", side_effect=ValueError("unreadable")
    ):
        state_out = ctx.run(ctx.on.config_changed(), state_in)

    assert _reply(state_out, gateway) == {}
    assert not (tmp_path / "password.db").exists()


def test_pebble_not_ready_leaves_databags_untouched(ctx):
    """A reschedule must not withdraw confirmations while Pebble is unreachable."""
    secret = _gateway_secret()
    published = {"trino_url": "old:8080", "registered_users": '["kept"]', "refused_users": "{}"}
    gateway = gateway_relation({GATEWAY_USER: secret.id}, local_app_data=published)
    state_in, _ = build_coordinator_state(
        container=trino_container(can_connect=False),
        extra_relations=(gateway,),
        extra_secrets=(secret,),
    )

    state_out = ctx.run(ctx.on.update_status(), state_in)

    assert _reply(state_out, gateway) == published


def test_worker_with_gateway_relation_is_blocked_and_clears_keys(ctx, tmp_path):
    """Workers do not take part: stale keys are removed and the charm is blocked."""
    secret = _gateway_secret()
    stale = {"trino_url": "old:8080", "registered_users": '["a"]', "refused_users": "{}"}
    gateway = gateway_relation({GATEWAY_USER: secret.id}, local_app_data=stale)
    state_in, _ = build_worker_state(
        container=_container(tmp_path), extra_relations=(gateway,), extra_secrets=(secret,)
    )

    state_out = ctx.run(ctx.on.config_changed(), state_in)

    assert GATEWAY_USER not in _password_db(tmp_path)
    assert _reply(state_out, gateway) == {}
    assert state_out.unit_status == BlockedStatus(
        "trino-gateway-integration requires a coordinator"
    )


def test_gateway_users_do_not_restart_trino(ctx, tmp_path):
    """Adding a Gateway user never changes the Pebble plan, so Trino is not restarted."""
    secret = _gateway_secret()
    gateway = gateway_relation({})
    state_in, _ = build_coordinator_state(
        container=_container(tmp_path), extra_relations=(gateway,), extra_secrets=(secret,)
    )
    configured = carry_forward(ctx.run(ctx.on.config_changed(), state_in))
    plan_before = configured.get_container("trino").plan.to_dict()

    requested = _with_requests(configured, gateway.id, {GATEWAY_USER: secret.id})
    state_out = ctx.run(ctx.on.relation_changed(requested.get_relation(gateway.id)), requested)

    assert GATEWAY_USER in _password_db(tmp_path)
    assert state_out.get_container("trino").plan.to_dict() == plan_before
    assert "HASH_PASSWORD_DB" not in plan_before["services"]["trino"]["environment"]


def test_without_relation_password_db_holds_configured_users(ctx, tmp_path):
    """Without the relation, `password.db` holds the configured users."""
    state_in, _ = build_coordinator_state(container=_container(tmp_path))

    ctx.run(ctx.on.config_changed(), state_in)

    assert set(_password_db(tmp_path)) == {"example_user", "another_user"}


def test_default_user_is_written_without_secret(ctx, tmp_path):
    """Without `user-secret-id`, the default user is written."""
    state_in, _ = build_coordinator_state(
        config={"user-secret-id": ""}, container=_container(tmp_path)
    )

    ctx.run(ctx.on.config_changed(), state_in)

    assert set(_password_db(tmp_path)) == {"trino"}


def test_passwords_never_leak(ctx, tmp_path, caplog):
    """No password appears in logs, status or the Pebble environment."""
    secret = _gateway_secret()
    gateway = gateway_relation({GATEWAY_USER: secret.id})
    state_in, _ = build_coordinator_state(
        container=_container(tmp_path), extra_relations=(gateway,), extra_secrets=(secret,)
    )

    with caplog.at_level(logging.DEBUG):
        state_out = ctx.run(ctx.on.relation_changed(gateway), state_in)

    visible = caplog.text + state_out.unit_status.message
    visible += json.dumps(state_out.get_container("trino").plan.to_dict(), default=str)
    visible += json.dumps(dict(_reply(state_out, gateway)))
    assert GATEWAY_PASSWORD not in visible


def test_changes_are_logged_at_debug(ctx, tmp_path, caplog):
    """Registrations and refusals are logged with their reasons."""
    secret = _gateway_secret()
    gateway = gateway_relation(
        {GATEWAY_USER: secret.id, "example_user": secret.id, "gw-pending": "secret:unknown"}
    )
    state_in, _ = build_coordinator_state(
        container=_container(tmp_path), extra_relations=(gateway,), extra_secrets=(secret,)
    )

    with caplog.at_level(logging.DEBUG):
        ctx.run(ctx.on.relation_changed(gateway), state_in)

    text = caplog.text
    assert f"Registered Trino Gateway users in relation {gateway.id}: ['{GATEWAY_USER}']" in text
    assert "username already exists on this Trino" in text
    assert f"Trino Gateway users pending in relation {gateway.id}: ['gw-pending']" in text


def test_pending_users_are_logged_on_leader_only(ctx, tmp_path, caplog):
    """A non-leader takes no action on pending users, so it does not log them."""
    gateway = gateway_relation({"gw-pending": "secret:unknown"})
    int_comms = Secret(
        tracked_content={"secret": "int-comms"},  # nosec
        owner="app",
        label=INT_COMMS_SECRET_LABEL,
    )
    truststore = Secret(
        tracked_content={"password": "truststore"},  # nosec
        owner="app",
        label=TRUSTSTORE_SECRET_LABEL,
    )
    state_in, _ = build_coordinator_state(
        leader=False,
        container=_container(tmp_path),
        extra_relations=(gateway,),
        extra_secrets=(int_comms, truststore),
    )

    with caplog.at_level(logging.DEBUG):
        ctx.run(ctx.on.relation_changed(gateway), state_in)

    assert "pending" not in caplog.text


def _fresh_coordinator(tmp_path, leader=True, with_gateway=True):
    """Build a coordinator state with a Gateway that requests one user."""
    secret = _gateway_secret()
    gateway = gateway_relation({GATEWAY_USER: secret.id})
    extra_secrets = [secret]
    if not leader:
        extra_secrets += [
            Secret(
                tracked_content={"secret": "int-comms"},  # nosec
                owner="app",
                label=INT_COMMS_SECRET_LABEL,
            ),
            Secret(
                tracked_content={"password": "truststore"},  # nosec
                owner="app",
                label=TRUSTSTORE_SECRET_LABEL,
            ),
        ]
    state_in, _ = build_coordinator_state(
        leader=leader,
        container=_container(tmp_path),
        extra_relations=(gateway,) if with_gateway else (),
        extra_secrets=tuple(extra_secrets),
    )
    return state_in, gateway


def _set_age(tmp_path, seconds):
    """Make `password.db` look like it was written the given number of seconds ago."""
    then = time.time() - seconds
    os.utime(tmp_path / "password.db", (then, then))


def test_leader_waits_for_reload_before_confirming(ctx, tmp_path, gateway_sleep):
    """After a change, the leader waits for Trino to reload the file, then publishes."""
    state_in, gateway = _fresh_coordinator(tmp_path)

    state_out = ctx.run(ctx.on.relation_changed(gateway), state_in)

    gateway_sleep.assert_called_once()
    (delay,) = gateway_sleep.call_args.args
    assert PASSWORD_DB_REFRESH_SECONDS < delay <= PASSWORD_DB_REFRESH_SECONDS + 1
    assert json.loads(_reply(state_out, gateway)["registered_users"]) == [GATEWAY_USER]


def test_no_wait_when_file_is_unchanged_and_old(ctx, tmp_path, gateway_sleep):
    """An unchanged file written more than a refresh period ago needs no wait."""
    state_in, gateway = _fresh_coordinator(tmp_path)
    configured = carry_forward(ctx.run(ctx.on.relation_changed(gateway), state_in))
    _set_age(tmp_path, 100)
    gateway_sleep.reset_mock()

    ctx.run(ctx.on.update_status(), configured)

    gateway_sleep.assert_not_called()


def test_retried_hook_waits_only_the_remaining_time(ctx, tmp_path, gateway_sleep):
    """A file written recently but unchanged in this hook waits only for what remains."""
    state_in, gateway = _fresh_coordinator(tmp_path)
    configured = carry_forward(ctx.run(ctx.on.relation_changed(gateway), state_in))
    _set_age(tmp_path, 2)
    gateway_sleep.reset_mock()

    ctx.run(ctx.on.update_status(), configured)

    gateway_sleep.assert_called_once()
    (delay,) = gateway_sleep.call_args.args
    assert PASSWORD_DB_REFRESH_SECONDS - 2 < delay <= PASSWORD_DB_REFRESH_SECONDS - 1


def test_non_leader_does_not_wait(ctx, tmp_path, gateway_sleep):
    """Only the leader confirms users, so only it waits."""
    state_in, gateway = _fresh_coordinator(tmp_path, leader=False)

    ctx.run(ctx.on.relation_changed(gateway), state_in)

    gateway_sleep.assert_not_called()


def test_no_wait_without_gateway_relation(ctx, tmp_path, gateway_sleep):
    """Without a Gateway, no confirmation depends on the reload."""
    state_in, _ = _fresh_coordinator(tmp_path, with_gateway=False)

    ctx.run(ctx.on.config_changed(), state_in)

    gateway_sleep.assert_not_called()


def test_worker_does_not_wait(ctx, tmp_path, gateway_sleep):
    """A worker confirms nothing, so it never waits."""
    secret = _gateway_secret()
    gateway = gateway_relation({GATEWAY_USER: secret.id})
    state_in, _ = build_worker_state(
        container=_container(tmp_path), extra_relations=(gateway,), extra_secrets=(secret,)
    )

    ctx.run(ctx.on.config_changed(), state_in)

    gateway_sleep.assert_not_called()


def test_refresh_period_is_rendered_from_the_constant(ctx, tmp_path):
    """Trino reloads `password.db` every `PASSWORD_DB_REFRESH_SECONDS`."""
    state_in, _ = build_coordinator_state(container=_container(tmp_path))

    ctx.run(ctx.on.config_changed(), state_in)

    properties = (tmp_path / "password-authenticator.properties").read_text()
    assert f"file.refresh-period={PASSWORD_DB_REFRESH_SECONDS}s" in properties.splitlines()


def test_list_system_users_reports_gateway_users(ctx, tmp_path):
    """The action lists the accepted Gateway users."""
    secret = _gateway_secret()
    gateway = gateway_relation({GATEWAY_USER: secret.id, "example_user": secret.id})
    state_in, _ = build_coordinator_state(
        container=_container(tmp_path), extra_relations=(gateway,), extra_secrets=(secret,)
    )

    ctx.run(ctx.on.action("list-system-users"), state_in)

    assert ctx.action_results["gateway-users"] == GATEWAY_USER


def test_list_system_users_without_gateway_users(ctx, tmp_path):
    """The action reports no Gateway users when there are none."""
    state_in, _ = build_coordinator_state(container=_container(tmp_path))

    ctx.run(ctx.on.action("list-system-users"), state_in)

    assert ctx.action_results["gateway-users"] == "(none)"
