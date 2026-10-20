# Copyright 2026 Canonical Ltd.
# See LICENSE file for licensing details.

"""Unit tests for the `trino-gateway-integration` relation."""

import dataclasses
import json
import logging

import pytest
from ops.model import ActiveStatus, BlockedStatus
from ops.testing import PeerRelation, Relation, Secret

from literals import (
    GATEWAY_CONFLICT_BLOCKED_MESSAGE,
    GATEWAY_REFUSAL_EXISTING_USER,
    GATEWAY_REFUSAL_OTHER_GATEWAY,
    GATEWAY_REFUSAL_WORKER,
    GATEWAY_WORKER_BLOCKED_MESSAGE,
)
from tests.unit.helpers import (
    build_coordinator_state,
    build_worker_state,
    carry_forward,
    ingress_relation,
    observer_secret,
    peer_state_value,
)

ENDPOINT = "trino-gateway-integration"
GW_USER_A = "gw-0123456789abcdef-superset-1"
GW_USER_B = "gw-fedcba9876543210-datahub-2"
GW_PASSWORD_A = "gateway-password-a"  # nosec B105
GW_PASSWORD_B = "gateway-password-b"  # nosec B105
CONFIGURED_USERS = ("example_user", "another_user")


def _secret(username, password):
    return observer_secret({"username": username, "password": password})


def _gateway(app, requested):
    """Build a Gateway relation requesting `{username: Secret | secret ID}`."""
    ids = {u: s.id if isinstance(s, Secret) else s for u, s in requested.items()}
    return Relation(
        ENDPOINT,
        remote_app_name=app,
        remote_app_data={"requested_users": json.dumps(ids)},
    )


def _gateway_a():
    secret = _secret(GW_USER_A, GW_PASSWORD_A)
    return secret, _gateway("gateway-a", {GW_USER_A: secret})


def _services(state):
    return state.get_container("trino").plan.to_dict()["services"]


def _written_users(ctx):
    """Return {username: password} passed to `htpasswd` since the last reset."""
    return {
        call.command[-2]: call.command[-1]
        for call in ctx.exec_history.get("trino", [])
        if call.command[0] == "htpasswd"
    }


def _databag(state, relation):
    return state.get_relation(relation.id).local_app_data


def _registered(state, relation):
    return json.loads(_databag(state, relation).get("registered_users", "null"))


def _refused(state, relation):
    return json.loads(_databag(state, relation).get("refused_users", "null"))


def _run(ctx, relations=(), secrets=(), **kwargs):
    state_in, ids = build_coordinator_state(
        extra_relations=relations, extra_secrets=secrets, **kwargs
    )
    return ctx.run(ctx.on.update_status(), state_in), ids


def _next(ctx, state, event=None, relations=(), secrets=(), drop=()):
    """Run a follow-up event on `state`, replacing the relations on the endpoints touched."""
    state = carry_forward(state)
    replaced = {r.endpoint for r in relations} | set(drop)
    kept = [r for r in state.relations if r.endpoint not in replaced]
    state = dataclasses.replace(
        state,
        relations={*kept, *relations},
        secrets=frozenset(state.secrets) | set(secrets),
    )
    ctx.exec_history.clear()
    return ctx.run(event or ctx.on.update_status(), state)


def test_requested_user_is_written_and_registered(ctx):
    """A requested user authenticates through `password.db` and is confirmed."""
    secret, relation = _gateway_a()

    state_out, ids = _run(ctx, [relation], [secret])

    assert _written_users(ctx)[GW_USER_A] == GW_PASSWORD_A
    assert set(CONFIGURED_USERS) <= set(_written_users(ctx))
    assert _registered(state_out, relation) == [GW_USER_A]
    assert _refused(state_out, relation) == {}
    assert peer_state_value(
        state_out.get_relation(ids.peer_relation.id), "gateway_user_claims"
    ) == {GW_USER_A: relation.id}
    assert state_out.unit_status == ActiveStatus("Status check: UP")


def test_several_gateways_can_be_related(ctx):
    """The endpoint has no limit and every Gateway gets its users registered."""
    secret_a, relation_a = _gateway_a()
    secret_b = _secret(GW_USER_B, GW_PASSWORD_B)
    relation_b = _gateway("gateway-b", {GW_USER_B: secret_b})

    state_out, _ = _run(ctx, [relation_a, relation_b], [secret_a, secret_b])

    assert _registered(state_out, relation_a) == [GW_USER_A]
    assert _registered(state_out, relation_b) == [GW_USER_B]
    assert {GW_USER_A, GW_USER_B} <= set(_written_users(ctx))


def test_pending_user_is_neither_registered_nor_refused(ctx):
    """A user whose secret cannot be read yet is retried until it can."""
    secret, _ = _gateway_a()
    relation = _gateway("gateway-a", {GW_USER_A: "secret:not-granted-yet"})

    state_out, ids = _run(ctx, [relation])

    assert GW_USER_A not in _written_users(ctx)
    assert _registered(state_out, relation) == []
    assert _refused(state_out, relation) == {}
    claims = peer_state_value(state_out.get_relation(ids.peer_relation.id), "gateway_user_claims")
    assert claims == {GW_USER_A: relation.id}

    granted = _gateway("gateway-a", {GW_USER_A: secret})
    granted = dataclasses.replace(granted, id=relation.id)
    state_out = _next(ctx, state_out, relations=[granted], secrets=[secret])

    assert _written_users(ctx)[GW_USER_A] == GW_PASSWORD_A
    assert _registered(state_out, granted) == [GW_USER_A]


def test_secret_without_password_stays_pending(ctx):
    """A secret lacking the `password` key does not break the reconcile."""
    secret = observer_secret({"username": GW_USER_A})
    relation = _gateway("gateway-a", {GW_USER_A: secret})

    state_out, _ = _run(ctx, [relation], [secret])

    assert GW_USER_A not in _written_users(ctx)
    assert _registered(state_out, relation) == []
    assert _refused(state_out, relation) == {}


@pytest.mark.parametrize("raw", ["not json", '["a"]', '"text"', "null"])
def test_malformed_request_counts_as_empty(ctx, caplog, raw):
    """Malformed data is ignored with a single WARNING that holds no values."""
    relation = Relation(
        ENDPOINT, remote_app_name="gateway-a", remote_app_data={"requested_users": raw}
    )

    with caplog.at_level(logging.WARNING):
        state_out, _ = _run(ctx, [relation])

    warnings = [
        r for r in caplog.records if r.levelno == logging.WARNING and "malformed" in r.message
    ]
    assert len(warnings) == 1
    assert str(relation.id) in warnings[0].message
    assert raw not in warnings[0].message
    assert _registered(state_out, relation) == []
    assert state_out.unit_status == ActiveStatus("Status check: UP")


def test_relation_without_remote_application_data_is_empty(ctx):
    """A relation with nothing requested publishes an empty reply."""
    relation = Relation(ENDPOINT, remote_app_name="gateway-a")

    state_out, _ = _run(ctx, [relation])

    assert _registered(state_out, relation) == []
    assert _refused(state_out, relation) == {}


def test_withdrawn_user_is_removed(ctx):
    """Removing a key from `requested_users` drops the claim and the user."""
    secret, relation = _gateway_a()
    state_out, ids = _run(ctx, [relation], [secret])

    withdrawn = dataclasses.replace(_gateway("gateway-a", {}), id=relation.id)
    state_out = _next(ctx, state_out, relations=[withdrawn])

    assert GW_USER_A not in _written_users(ctx)
    assert _registered(state_out, withdrawn) == []
    claims = peer_state_value(state_out.get_relation(ids.peer_relation.id), "gateway_user_claims")
    assert claims == {}


def test_relation_broken_removes_users(ctx):
    """A relation being broken is already gone from the model, so its users go."""
    secret, relation = _gateway_a()
    state_out, ids = _run(ctx, [relation], [secret])
    assert GW_USER_A in _written_users(ctx)

    ctx.exec_history.clear()
    state_out = ctx.run(
        ctx.on.relation_broken(state_out.get_relation(relation.id)), carry_forward(state_out)
    )

    assert GW_USER_A not in _written_users(ctx)
    assert set(CONFIGURED_USERS) <= set(_written_users(ctx))
    claims = peer_state_value(state_out.get_relation(ids.peer_relation.id), "gateway_user_claims")
    assert claims == {}


@pytest.mark.parametrize("name", CONFIGURED_USERS)
def test_request_for_configured_user_is_refused(ctx, name):
    """An existing `user-secret-id` user cannot be taken over."""
    secret = _secret(name, "attacker-password")  # nosec B106
    relation = _gateway("gateway-a", {name: secret})

    state_out, ids = _run(ctx, [relation], [secret])

    assert _refused(state_out, relation) == {name: GATEWAY_REFUSAL_EXISTING_USER}
    assert _registered(state_out, relation) == []
    assert _written_users(ctx)[name] != "attacker-password"
    assert peer_state_value(
        state_out.get_relation(ids.peer_relation.id), "gateway_user_claims"
    ) in (
        None,
        {},
    )
    assert state_out.unit_status == ActiveStatus("Status check: UP")


def test_request_for_default_user_is_refused(ctx):
    """A default user is protected like a configured one."""
    secret = _secret("trino", "attacker-password")  # nosec B106
    relation = _gateway("gateway-a", {"trino": secret})
    state_in, _ = build_coordinator_state(extra_relations=[relation], extra_secrets=[secret])
    config = {k: v for k, v in state_in.config.items() if k != "user-secret-id"}
    state_in = dataclasses.replace(state_in, config=config)

    state_out = ctx.run(ctx.on.update_status(), state_in)

    assert _refused(state_out, relation) == {"trino": GATEWAY_REFUSAL_EXISTING_USER}
    assert _written_users(ctx)["trino"] != "attacker-password"


def test_request_for_catalog_relation_user_is_refused(ctx):
    """A per-relation `trino-catalog` user cannot be taken over."""
    catalog = Relation(
        "trino-catalog", remote_app_name="superset", remote_app_data={"app_name": "superset"}
    )
    state_out, _ = _run(ctx, [catalog])
    catalog_user = f"app-superset-{catalog.id}"
    password = _written_users(ctx)[catalog_user]

    secret = _secret(catalog_user, "attacker-password")  # nosec B106
    relation = _gateway("gateway-a", {catalog_user: secret})
    state_out = _next(ctx, state_out, relations=[relation], secrets=[secret])

    assert _refused(state_out, relation) == {catalog_user: GATEWAY_REFUSAL_EXISTING_USER}
    assert _written_users(ctx)[catalog_user] == password


def test_first_gateway_keeps_a_contested_name(ctx):
    """The second Gateway requesting a claimed name is refused, whatever its relation ID."""
    secret_a = _secret(GW_USER_A, GW_PASSWORD_A)
    secret_b = _secret(GW_USER_A, "second-gateway-password")  # nosec B106
    first = _gateway("gateway-a", {GW_USER_A: secret_a})
    state_out, _ = _run(ctx, [first], [secret_a])

    second = _gateway("gateway-b", {GW_USER_A: secret_b})
    state_out = _next(ctx, state_out, relations=[first, second], secrets=[secret_b])

    assert _written_users(ctx)[GW_USER_A] == GW_PASSWORD_A
    assert _registered(state_out, first) == [GW_USER_A]
    assert _registered(state_out, second) == []
    assert _refused(state_out, second) == {GW_USER_A: GATEWAY_REFUSAL_OTHER_GATEWAY}

    # The loser takes the name once the holder withdraws.
    state_out = _next(
        ctx,
        state_out,
        relations=[dataclasses.replace(_gateway("gateway-a", {}), id=first.id), second],
    )
    assert _written_users(ctx)[GW_USER_A] == "second-gateway-password"
    assert _registered(state_out, second) == [GW_USER_A]
    assert _refused(state_out, second) == {}


def test_contested_name_in_one_run_goes_to_lowest_relation_id(ctx):
    """With nothing claimed yet, the lowest relation ID wins."""
    secret_a = _secret(GW_USER_A, GW_PASSWORD_A)
    secret_b = _secret(GW_USER_A, "second-gateway-password")  # nosec B106
    first = _gateway("gateway-a", {GW_USER_A: secret_a})
    second = _gateway("gateway-b", {GW_USER_A: secret_b})
    low, high = sorted((first, second), key=lambda r: r.id)

    state_out, _ = _run(ctx, [first, second], [secret_a, secret_b])

    assert _registered(state_out, low) == [GW_USER_A]
    assert _refused(state_out, high) == {GW_USER_A: GATEWAY_REFUSAL_OTHER_GATEWAY}


def test_claimed_user_wins_over_later_configured_user(ctx):
    """A later `user-secret-id` user of the same name is dropped and the charm blocks."""
    secret, relation = _gateway_a()
    state_out, _ = _run(ctx, [relation], [secret])
    assert _registered(state_out, relation) == [GW_USER_A]

    users = observer_secret(
        {"users": f"example_user: ubuntu123\n{GW_USER_A}: configured-password\n"}
    )
    state = carry_forward(state_out)
    state = dataclasses.replace(
        state,
        config={**state.config, "user-secret-id": users.id},
        secrets=frozenset(state.secrets) | {users},
    )
    ctx.exec_history.clear()
    state_out = ctx.run(ctx.on.config_changed(), state)

    assert _written_users(ctx)[GW_USER_A] == GW_PASSWORD_A
    assert _registered(state_out, relation) == [GW_USER_A]
    assert state_out.unit_status == BlockedStatus(
        GATEWAY_CONFLICT_BLOCKED_MESSAGE.format(names=GW_USER_A)
    )
    assert "example_user" in _written_users(ctx)


def test_unreadable_user_secret_pauses_claim_planning(ctx):
    """Names are neither claimed nor refused while `user-secret-id` is unreadable."""
    secret, relation = _gateway_a()
    bad_user = observer_secret({"users": ""})

    state_out, ids = _run(
        ctx, [relation], [secret, bad_user], config={"user-secret-id": bad_user.id}
    )

    assert (
        peer_state_value(state_out.get_relation(ids.peer_relation.id), "gateway_user_claims")
        is None
    )
    assert _registered(state_out, relation) is None
    assert _refused(state_out, relation) is None
    assert isinstance(state_out.unit_status, BlockedStatus)


def test_early_reconcile_exit_keeps_previous_confirmations(ctx):
    """No new names are confirmed when the reconcile stops before `password.db`."""
    secret, relation = _gateway_a()
    state_out, _ = _run(ctx, [relation], [secret])
    assert _registered(state_out, relation) == [GW_USER_A]

    secret_b = _secret(GW_USER_B, GW_PASSWORD_B)
    both = dataclasses.replace(
        state_out.get_relation(relation.id),
        remote_app_data=_gateway(
            "gateway-a", {GW_USER_A: secret, GW_USER_B: secret_b}
        ).remote_app_data,
    )
    bad_user = observer_secret({"users": ""})
    state = carry_forward(state_out)
    state = dataclasses.replace(state, config={**state.config, "user-secret-id": bad_user.id})
    state_out = _next(ctx, state, relations=[both], secrets=[secret_b, bad_user])

    assert _registered(state_out, both) == [GW_USER_A]
    assert GW_USER_B not in _written_users(ctx)


def test_published_url_matches_trino_catalog(ctx):
    """Both relations carry the same URL: without ingress, with ingress and after revocation."""
    secret, gateway = _gateway_a()
    catalog = Relation(
        "trino-catalog", remote_app_name="superset", remote_app_data={"app_name": "superset"}
    )

    def urls(state):
        return (
            _databag(state, gateway)["trino_url"],
            _databag(state, catalog)["trino_url"],
        )

    state_out, _ = _run(ctx, [gateway, catalog], [secret])
    gateway_url, catalog_url = urls(state_out)
    assert gateway_url == catalog_url
    assert gateway_url == "trino-k8s.trino-model.svc.cluster.local:8080"

    ingress = ingress_relation(url="https://trino.example.com/")
    state_out = _next(ctx, state_out, relations=[ingress])
    gateway_url, catalog_url = urls(state_out)
    assert gateway_url == catalog_url == "trino.example.com:443"

    state_out = _next(ctx, state_out, drop=["ingress"])
    gateway_url, catalog_url = urls(state_out)
    assert gateway_url == catalog_url == "trino-k8s.trino-model.svc.cluster.local:8080"


def test_without_relation_credentials_are_unchanged(ctx):
    """With no Gateway related, the written users are exactly the configured ones."""
    state_out, ids = _run(ctx)

    assert _written_users(ctx) == {"example_user": "ubuntu123", "another_user": "ubuntu345"}
    assert (
        peer_state_value(state_out.get_relation(ids.peer_relation.id), "gateway_user_claims")
        is None
    )
    assert (
        peer_state_value(state_out.get_relation(ids.peer_relation.id), "gateway_pending_users")
        is None
    )
    assert state_out.unit_status == ActiveStatus("Status check: UP")


def test_all_function_registers_requested_users(ctx):
    """A coordinator and worker in one application (`all`) also authenticates Gateway users."""
    secret, relation = _gateway_a()

    state_out, _ = _run(ctx, [relation], [secret], config={"charm-function": "all"})

    assert _written_users(ctx)[GW_USER_A] == GW_PASSWORD_A
    assert _registered(state_out, relation) == [GW_USER_A]


def test_worker_refuses_every_request_and_blocks(ctx):
    """A worker authenticates nobody: it refuses requests, publishes no URL and blocks."""
    secret, relation = _gateway_a()
    state_in, _ = build_worker_state(extra_relations=[relation], extra_secrets=[secret])

    state_out = ctx.run(ctx.on.update_status(), state_in)

    assert GW_USER_A not in _written_users(ctx)
    assert _registered(state_out, relation) == []
    assert _refused(state_out, relation) == {GW_USER_A: GATEWAY_REFUSAL_WORKER}
    assert "trino_url" not in _databag(state_out, relation)
    assert state_out.unit_status == BlockedStatus(GATEWAY_WORKER_BLOCKED_MESSAGE)


def test_non_leader_writes_users_but_publishes_nothing(ctx):
    """Every unit writes the claimed users, only the leader talks to the Gateway."""
    secret, relation = _gateway_a()
    state_out, ids = _run(ctx, [relation], [secret])
    peer = state_out.get_relation(ids.peer_relation.id)

    state_in, _ = build_coordinator_state(
        leader=False, extra_relations=[relation], extra_secrets=state_out.secrets
    )
    peers = {r for r in state_in.relations if not isinstance(r, PeerRelation)}
    state_in = dataclasses.replace(state_in, relations={*peers, peer})
    ctx.exec_history.clear()
    state_out = ctx.run(ctx.on.update_status(), state_in)

    assert _written_users(ctx)[GW_USER_A] == GW_PASSWORD_A
    assert "registered_users" not in _databag(state_out, relation)
    assert "trino_url" not in _databag(state_out, relation)


def test_gateway_password_rotation_does_not_restart_trino(ctx):
    """A rotated password reaches `password.db` and leaves the Pebble plan untouched."""
    secret, relation = _gateway_a()
    state_out, _ = _run(ctx, [relation], [secret])
    env_before = dict(_services(state_out)["trino"]["environment"])

    rotated = dataclasses.replace(
        secret,
        latest_content={"username": GW_USER_A, "password": "rotated"},  # nosec B105
    )
    state = carry_forward(state_out)
    state = dataclasses.replace(
        state, secrets=frozenset(s for s in state.secrets if s.id != secret.id) | {rotated}
    )
    ctx.exec_history.clear()
    state_out = ctx.run(ctx.on.secret_changed(rotated), state)

    assert _written_users(ctx)[GW_USER_A] == "rotated"
    assert dict(_services(state_out)["trino"]["environment"]) == env_before


def test_credential_changes_never_alter_the_pebble_plan(ctx):
    """Adding or removing any user leaves the plan identical, so Trino never restarts."""
    secret, relation = _gateway_a()
    plain, _ = _run(ctx)
    plan_before = _services(plain)["trino"]
    assert "HASH_PASSWORD_DB" not in plan_before["environment"]

    related = _next(ctx, plain, relations=[relation], secrets=[secret])
    assert GW_USER_A in _written_users(ctx)
    assert _services(related)["trino"] == plan_before

    withdrawn = _next(
        ctx, related, relations=[dataclasses.replace(_gateway("gateway-a", {}), id=relation.id)]
    )
    assert GW_USER_A not in _written_users(ctx)
    assert _services(withdrawn)["trino"] == plan_before


def test_passwords_never_leak(ctx, caplog):
    """Gateway passwords appear in no log, status, databag or environment variable."""
    secret, relation = _gateway_a()

    with caplog.at_level(logging.DEBUG):
        state_out, ids = _run(ctx, [relation], [secret])

    assert GW_PASSWORD_A not in caplog.text
    assert GW_PASSWORD_A not in state_out.unit_status.message
    assert GW_PASSWORD_A not in json.dumps(_services(state_out))
    assert GW_PASSWORD_A not in json.dumps(dict(_databag(state_out, relation)))
    assert GW_PASSWORD_A not in json.dumps(
        dict(state_out.get_relation(ids.peer_relation.id).local_app_data)
    )


def test_unchanged_reconcile_logs_nothing_from_the_handler(ctx, caplog):
    """Only changes are logged."""
    secret, relation = _gateway_a()
    state_out, _ = _run(ctx, [relation], [secret])
    caplog.clear()

    with caplog.at_level(logging.DEBUG, logger="relations.trino_gateway_integration"):
        _next(ctx, state_out)

    assert not [r for r in caplog.records if r.name == "relations.trino_gateway_integration"]


def test_list_system_users_shows_gateway_users(ctx):
    """The action reports the claimed Gateway users."""
    secret, relation = _gateway_a()
    state_out, _ = _run(ctx, [relation], [secret])

    ctx.run(ctx.on.action("list-system-users"), carry_forward(state_out))

    assert ctx.action_results["gateway-users"] == GW_USER_A


def test_list_system_users_without_gateway_users(ctx):
    """The action reports `(none)` without Gateway users."""
    state_in, _ = build_coordinator_state()

    ctx.run(ctx.on.action("list-system-users"), state_in)

    assert ctx.action_results["gateway-users"] == "(none)"
