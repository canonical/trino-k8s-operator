# Copyright 2023 Canonical Ltd.
# See LICENSE file for licensing details.

"""Unit tests for the multi-relation `TrinoCatalogRequirer` library."""

import json
import logging
from unittest import mock

import pytest
from charms.trino_k8s.v0.trino_catalog import (
    LIBAPI,
    LIBPATCH,
    TrinoCatalog,
    TrinoCatalogRequirer,
)
from ops import CharmBase
from ops.model import ModelError, SecretNotFoundError
from ops.testing import Context, Relation, Secret, State

ENDPOINT = "trino-catalog"
LIB_LOGGER = "charms.trino_k8s.v0.trino_catalog"
CATALOGS = [{"name": "sales", "connector": "postgresql", "description": "Sales"}]


class RequirerCharm(CharmBase):
    """Minimal charm that only hosts the requirer library."""

    def __init__(self, *args):
        super().__init__(*args)
        self.requirer = TrinoCatalogRequirer(self, relation_name=ENDPOINT)


@pytest.fixture
def requirer_ctx():
    """Return a Scenario context for the minimal requirer charm."""
    return Context(
        RequirerCharm,
        meta={
            "name": "requirer",
            "requires": {ENDPOINT: {"interface": "trino_catalog", "limit": 5}},
        },
    )


def _secret(suffix=""):
    return Secret(
        tracked_content={"username": f"user{suffix}", "password": f"pass{suffix}"}, owner=None
    )


def _provider_relation(app, secret=None, drop=None, **overrides):
    """Return a relation to `app` whose credentials live in `secret`, minus the `drop` field."""
    data = {
        "trino_url": f"{app}.example.com:8080",
        "trino_catalogs": json.dumps(CATALOGS),
        "trino_credentials_secret_id": secret.id if secret else "secret:missing",
    }
    data.update(overrides)
    data.pop(drop, None)
    return Relation(ENDPOINT, remote_app_name=app, remote_app_data=data)


def _run(ctx, state, check):
    """Run `check(requirer)` inside a charm event and return its result."""
    with ctx(ctx.on.update_status(), state) as manager:
        return check(manager.charm.requirer)


def test_libpatch_bumped_without_api_change():
    """Only the patch version is bumped."""
    assert LIBAPI == 0
    assert LIBPATCH == 6


def test_no_relations(requirer_ctx):
    """Every accessor returns an empty result when nothing is related."""
    state = State()
    assert _run(requirer_ctx, state, lambda r: r.get_trino_info()) is None
    assert _run(requirer_ctx, state, lambda r: r.get_credentials()) is None
    assert _run(requirer_ctx, state, lambda r: r.get_all_trino_info()) == {}
    assert _run(requirer_ctx, state, lambda r: r.get_all_credentials()) == {}


def test_single_relation(requirer_ctx):
    """One relation yields info and credentials identifying the origin."""
    secret = _secret()
    relation = _provider_relation("trino-a", secret)
    state = State(relations=[relation], secrets=[secret])

    info = _run(requirer_ctx, state, lambda r: r.get_all_trino_info())
    credentials = _run(requirer_ctx, state, lambda r: r.get_all_credentials())

    assert list(info) == [relation.id]
    assert info[relation.id]["relation_id"] == relation.id
    assert info[relation.id]["remote_app"] == "trino-a"
    assert info[relation.id]["trino_url"] == "trino-a.example.com:8080"
    assert info[relation.id]["trino_catalogs"] == [TrinoCatalog.from_dict(CATALOGS[0])]
    assert credentials == {relation.id: ("user", "pass")}


def test_several_relations(requirer_ctx):
    """Every complete relation is returned with its own data and credentials."""
    secrets = [_secret(f"-{app}") for app in ("a", "b", "c")]
    relations = [
        _provider_relation(f"trino-{app}", secret) for app, secret in zip(("a", "b", "c"), secrets)
    ]
    state = State(relations=relations, secrets=secrets)

    info = _run(requirer_ctx, state, lambda r: r.get_all_trino_info())
    credentials = _run(requirer_ctx, state, lambda r: r.get_all_credentials())

    assert set(info) == {relation.id for relation in relations}
    for relation, app in zip(relations, ("a", "b", "c")):
        assert info[relation.id]["remote_app"] == f"trino-{app}"
        assert info[relation.id]["relation_id"] == relation.id
        assert credentials[relation.id] == (f"user-{app}", f"pass-{app}")


def test_specific_relation_accessors(requirer_ctx):
    """A specific relation can be read regardless of its position."""
    secrets = [_secret("-a"), _secret("-b")]
    relations = [
        _provider_relation("trino-a", secrets[0]),
        _provider_relation("trino-b", secrets[1]),
    ]
    state = State(relations=relations, secrets=secrets)

    with requirer_ctx(requirer_ctx.on.update_status(), state) as manager:
        requirer = manager.charm.requirer
        second = manager.charm.model.get_relation(ENDPOINT, relations[1].id)
        info = requirer.get_trino_info(second)
        credentials = requirer.get_credentials(second)

    assert info["remote_app"] == "trino-b"
    assert info["relation_id"] == relations[1].id
    assert credentials == ("user-b", "pass-b")


@pytest.mark.parametrize("missing", ["trino_url", "trino_catalogs", "trino_credentials_secret_id"])
def test_partial_data_is_skipped(requirer_ctx, caplog, missing):
    """A relation with unpublished fields is skipped at debug level."""
    secret = _secret()
    complete = _provider_relation("trino-ok", secret)
    partial = _provider_relation("trino-partial", secret, drop=missing)
    state = State(relations=[partial, complete], secrets=[secret])

    with caplog.at_level(logging.DEBUG, logger=LIB_LOGGER):
        info = _run(requirer_ctx, state, lambda r: r.get_all_trino_info())
        credentials = _run(requirer_ctx, state, lambda r: r.get_all_credentials())

    assert list(info) == [complete.id]
    assert list(credentials) == [complete.id]
    skipped = [r for r in caplog.records if f"Relation {partial.id} " in r.getMessage()]
    assert skipped
    assert all(r.levelno == logging.DEBUG for r in skipped)
    assert "not yet published" in skipped[0].getMessage()


@pytest.mark.parametrize(
    "bad_catalogs",
    ["{not json", json.dumps([{"connector": "x"}]), json.dumps({"name": "x"}), "null"],
)
def test_malformed_catalogs_are_skipped(requirer_ctx, caplog, bad_catalogs):
    """A relation with malformed `trino_catalogs` is skipped at warning level."""
    secret = _secret()
    complete = _provider_relation("trino-ok", secret)
    malformed = _provider_relation("trino-bad", secret, trino_catalogs=bad_catalogs)
    state = State(relations=[malformed, complete], secrets=[secret])

    with caplog.at_level(logging.DEBUG, logger=LIB_LOGGER):
        info = _run(requirer_ctx, state, lambda r: r.get_all_trino_info())
        credentials = _run(requirer_ctx, state, lambda r: r.get_all_credentials())

    assert list(info) == [complete.id]
    assert list(credentials) == [complete.id]
    skipped = [r for r in caplog.records if f"Relation {malformed.id} " in r.getMessage()]
    assert skipped
    assert all(r.levelno == logging.WARNING for r in skipped)
    assert "malformed" in skipped[0].getMessage()


def test_missing_secret_does_not_block_others(requirer_ctx, caplog):
    """A missing secret on one relation leaves the others untouched."""
    secret = _secret()
    healthy = _provider_relation("trino-ok", secret)
    broken = _provider_relation("trino-broken")
    state = State(relations=[broken, healthy], secrets=[secret])

    with caplog.at_level(logging.WARNING, logger=LIB_LOGGER):
        credentials = _run(requirer_ctx, state, lambda r: r.get_all_credentials())
        info = _run(requirer_ctx, state, lambda r: r.get_all_trino_info())

    assert credentials == {healthy.id: ("user", "pass")}
    assert set(info) == {healthy.id, broken.id}
    assert any(f"Relation {broken.id} " in r.getMessage() for r in caplog.records)


@pytest.mark.parametrize("error", [SecretNotFoundError("gone"), ModelError("denied")])
def test_unreadable_secret_does_not_block_others(requirer_ctx, error):
    """Secret errors on one relation are contained per relation."""
    secret = _secret()
    broken_secret = _secret("-broken")
    healthy = _provider_relation("trino-ok", secret)
    broken = _provider_relation("trino-broken", broken_secret)
    state = State(relations=[broken, healthy], secrets=[secret, broken_secret])

    with requirer_ctx(requirer_ctx.on.update_status(), state) as manager:
        model = manager.charm.model
        real_get_secret = model.get_secret

        def get_secret(**kwargs):
            if kwargs["id"] == broken_secret.id:
                raise error
            return real_get_secret(**kwargs)

        with mock.patch.object(model, "get_secret", side_effect=get_secret):
            credentials = manager.charm.requirer.get_all_credentials()

    assert credentials == {healthy.id: ("user", "pass")}


def test_secret_without_credential_fields_is_skipped(requirer_ctx):
    """A secret lacking username or password leaves the relation out."""
    bad_secret = Secret(tracked_content={"username": "user"}, owner=None)
    good_secret = _secret()
    bad = _provider_relation("trino-bad", bad_secret)
    good = _provider_relation("trino-ok", good_secret)
    state = State(relations=[bad, good], secrets=[bad_secret, good_secret])

    credentials = _run(requirer_ctx, state, lambda r: r.get_all_credentials())

    assert credentials == {good.id: ("user", "pass")}


def test_works_on_non_leader_units(requirer_ctx):
    """The accessors do not require leadership."""
    secret = _secret()
    relation = _provider_relation("trino-a", secret)
    state = State(relations=[relation], secrets=[secret], leader=False)

    assert list(_run(requirer_ctx, state, lambda r: r.get_all_trino_info())) == [relation.id]
    assert list(_run(requirer_ctx, state, lambda r: r.get_all_credentials())) == [relation.id]


def test_backward_compatible_get_trino_info(requirer_ctx):
    """The no-argument accessor reads the first relation with the old keys."""
    secret = _secret()
    first = _provider_relation("trino-a", secret)
    second = _provider_relation("trino-b", secret, trino_url="other:8080")
    state = State(relations=[first, second], secrets=[secret])

    info = _run(requirer_ctx, state, lambda r: r.get_trino_info())

    assert info == {
        "trino_url": "trino-a.example.com:8080",
        "trino_catalogs": [TrinoCatalog.from_dict(CATALOGS[0])],
        "trino_credentials_secret_id": secret.id,
    }


def test_backward_compatible_incomplete_first_relation(requirer_ctx):
    """The no-argument accessors do not fall through to later relations."""
    secret = _secret()
    first = _provider_relation("trino-a", secret, drop="trino_url")
    second = _provider_relation("trino-b", secret)
    state = State(relations=[first, second], secrets=[secret])

    assert _run(requirer_ctx, state, lambda r: r.get_trino_info()) is None
    assert _run(requirer_ctx, state, lambda r: r.get_credentials()) is None


def test_backward_compatible_get_credentials(requirer_ctx):
    """The no-argument accessor returns a tuple for the first relation."""
    secret = _secret()
    state = State(relations=[_provider_relation("trino-a", secret)], secrets=[secret])

    assert _run(requirer_ctx, state, lambda r: r.get_credentials()) == ("user", "pass")


def test_backward_compatible_get_credentials_raises_missing_secret(requirer_ctx):
    """The no-argument accessor still re-raises `SecretNotFoundError`."""
    state = State(relations=[_provider_relation("trino-a")])

    with pytest.raises(SecretNotFoundError):
        _run(requirer_ctx, state, lambda r: r.get_credentials())


def test_backward_compatible_get_credentials_raises_model_error(requirer_ctx):
    """The no-argument accessor still re-raises `ModelError`."""
    secret = _secret()
    state = State(relations=[_provider_relation("trino-a", secret)], secrets=[secret])

    with requirer_ctx(requirer_ctx.on.update_status(), state) as manager:
        with mock.patch.object(
            manager.charm.model, "get_secret", side_effect=ModelError("denied")
        ):
            with pytest.raises(ModelError):
                manager.charm.requirer.get_credentials()
