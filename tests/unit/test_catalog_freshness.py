# Copyright 2023 Canonical Ltd.
# See LICENSE file for licensing details.
#
# Learn more about testing at: https://juju.is/docs/sdk/testing


"""Trino charm catalog freshness unit tests."""

# pylint:disable=protected-access

import dataclasses
import logging

from ops.model import ActiveStatus, BlockedStatus, Container
from ops.testing import Mount

from relations.postgresql_catalog import PostgresqlCatalogRelationHandler
from tests.unit.helpers import (
    BIGQUERY_SECRET,
    CATALOG_INVENTORY_PATH,
    MODEL_HTTPS_PROXY,
    MODEL_HTTPS_PROXY_HOST,
    MODEL_HTTPS_PROXY_PORT,
    POSTGRESQL_2_CATALOG_PATH,
    UPDATED_JVM_OPTIONS,
    USER_JVM_STRING,
    build_coordinator_state,
    build_worker_state,
    carry_forward,
    create_added_catalog_config,
    failed_inventory_exec,
    ingress_relation,
    inventory_command,
    oauth_relation,
    observer_secret,
    refresh_catalog_execs,
    trino_container,
    workload_path,
)

logger = logging.getLogger(__name__)


def _plan_environment(state, container="trino"):
    """Return the rendered Pebble service environment for `container`."""
    return state.get_container(container).plan.to_dict()["services"][container]["environment"]


def _catalog_dir(tmp_path):
    """Return the local path backing the mounted catalog directory."""
    return tmp_path / "catalog"


def test_config_changed(ctx, monkeypatch):
    """The pebble plan changes according to config changes.

    Also exercises proxy configuration: a model `juju-https-proxy` combines
    with `additional-jvm-options` (JVM precedence rules), while the OAuth
    proxy property is derived from model config alone.
    """
    monkeypatch.setenv("JUJU_CHARM_HTTPS_PROXY", MODEL_HTTPS_PROXY)
    oauth_secret = observer_secret({"secret": "test-client-secret"})  # nosec B105
    state_in, _ = build_coordinator_state(
        config={
            "charm-function": "all",
            "additional-jvm-options": USER_JVM_STRING,
        },
        extra_relations=(
            oauth_relation(oauth_secret.id),
            ingress_relation("https://trino.example"),
        ),
        extra_secrets=(oauth_secret,),
    )

    state_out = ctx.run(ctx.on.config_changed(), state_in)

    want_services = {
        "trino": {
            "override": "replace",
            "summary": "trino server",
            "command": "./entrypoint.sh",
            "startup": "enabled",
            "on-check-failure": {"up": "restart"},
            "environment": {
                "PASSWORD_DB_PATH": "/usr/lib/trino/etc/password.db",  # nosec
                "LOG_LEVEL": "info",
                "OAUTH_CLIENT_ID": "client-123",
                "OAUTH_CLIENT_SECRET": "test-client-secret",  # nosec
                "OAUTH_ISSUER_URL": "https://idp.example",
                "OAUTH_AUTHORIZATION_ENDPOINT": "https://idp.example/oauth2/auth",
                "OAUTH_TOKEN_ENDPOINT": "https://idp.example/oauth2/token",
                "OAUTH_USERINFO_ENDPOINT": "https://idp.example/userinfo",
                "OAUTH_JWKS_ENDPOINT": "https://idp.example/.well-known/jwks.json",
                "OAUTH_SCOPES": "openid profile email",
                "OAUTH_HTTP_PROXY": f"{MODEL_HTTPS_PROXY_HOST}:{MODEL_HTTPS_PROXY_PORT}",
                "OAUTH_HTTP_PROXY_SECURE": None,
                "CHARM_FUNCTION": "all",
                "DISCOVERY_URI": "http://trino-k8s.trino-model.svc.cluster.local:8080",
                "APPLICATION_NAME": "trino-k8s",
                "TRINO_HOME": "/usr/lib/trino/etc",
                "JMX_PORT": 9081,
                "METRICS_PORT": 9090,
                "OAUTH_USER_MAPPING": None,
                "RANGER_RELATION": False,
                "RESOURCE_GROUPS_CONFIG": None,
                "SESSION_PROPERTY_MANAGER_CONFIG": None,
                "ACL_ACCESS_MODE": "owner",
                "ACL_CATALOG_PATTERN": ".*",
                "ACL_USER_PATTERN": ".*",
                "JAVA_TRUSTSTORE_PWD": "truststore_pwd",  # nosec
                "INT_COMMS_SECRET": "int_comms_secret",  # nosec
                "USER_SECRET_ID": "secret:secret-id",  # nosec
                "JVM_OPTIONS": UPDATED_JVM_OPTIONS,
                "COORDINATOR_REQUEST_TIMEOUT": "10m",
                "COORDINATOR_CONNECT_TIMEOUT": "30s",
                "WORKER_REQUEST_TIMEOUT": "30s",
                "MAX_CONCURRENT_QUERIES": 50,
                "QUERY_MAX_CPU_TIME": None,
                "QUERY_MAX_MEMORY_PER_NODE": None,
                "QUERY_MAX_MEMORY": None,
                "QUERY_MAX_TOTAL_MEMORY": None,
                "MEMORY_HEAP_HEADROOM_PER_NODE": None,
                "QUERY_MAX_RUN_TIME": None,
            },
        }
    }

    got_services = state_out.get_container("trino").plan.to_dict()["services"]

    # The truststore password and the internal-comms secret are randomly
    # generated, and are normalised here to compare the rest of the plan.
    environment = got_services["trino"]["environment"]
    environment["JAVA_TRUSTSTORE_PWD"] = "truststore_pwd"  # nosec
    environment["INT_COMMS_SECRET"] = "int_comms_secret"  # nosec
    environment["USER_SECRET_ID"] = "secret:secret-id"  # nosec

    # Per-file content hashes and the aggregate catalog hash drive Pebble
    # restarts; assert they are present as freshness triggers, then drop
    # them to compare the stable environment.
    hash_keys = {key for key in environment if key.startswith("HASH_")}
    assert hash_keys
    assert "CATALOG_STATE_HASH" in environment
    for key in hash_keys:
        del environment[key]
    del environment["CATALOG_STATE_HASH"]

    assert got_services == want_services
    assert state_out.unit_status == ActiveStatus("Status check: UP")


def test_catalog_added(ctx, tmp_path):
    """Adding a static catalog changes `CATALOG_STATE_HASH` exactly once."""
    container = trino_container(
        mounts={"home": Mount(location="/usr/lib/trino/etc", source=tmp_path)}
    )
    state_in, ids = build_coordinator_state(container=container)

    first = ctx.run(ctx.on.config_changed(), state_in)
    first_hash = _plan_environment(first)["CATALOG_STATE_HASH"]
    mtimes_before = {p.name: p.stat().st_mtime_ns for p in _catalog_dir(tmp_path).iterdir()}

    mid = refresh_catalog_execs(carry_forward(first), ctx)
    extended_catalog_config = create_added_catalog_config(
        ids.postgresql,
        ids.mysql,
        ids.redshift,
        ids.bigquery,
        ids.gsheets,
    )
    mid = dataclasses.replace(
        mid, config={**mid.config, "catalog-config": extended_catalog_config}
    )

    second = ctx.run(ctx.on.config_changed(), mid)

    assert workload_path(second, ctx, POSTGRESQL_2_CATALOG_PATH).exists()
    mtimes_after = {p.name: p.stat().st_mtime_ns for p in _catalog_dir(tmp_path).iterdir()}
    unchanged = mtimes_before.keys() & mtimes_after.keys()
    assert all(mtimes_before[name] == mtimes_after[name] for name in unchanged)
    assert _plan_environment(second)["CATALOG_STATE_HASH"] != first_hash


def test_catalog_changed(ctx, tmp_path):
    """Changing a static catalog's content changes `CATALOG_STATE_HASH` exactly once."""
    container = trino_container(
        mounts={"home": Mount(location="/usr/lib/trino/etc", source=tmp_path)}
    )
    state_in, _ = build_coordinator_state(container=container)

    first = ctx.run(ctx.on.config_changed(), state_in)
    first_hash = _plan_environment(first)["CATALOG_STATE_HASH"]
    postgresql_path = tmp_path / "catalog" / "postgresql-1.properties"
    mysql_mtime_before = (tmp_path / "catalog" / "mysql.properties").stat().st_mtime_ns

    mid = refresh_catalog_execs(carry_forward(first), ctx)
    changed_config = state_in.config["catalog-config"].replace(
        "database: example", "database: changed"
    )
    mid = dataclasses.replace(mid, config={**mid.config, "catalog-config": changed_config})

    second = ctx.run(ctx.on.config_changed(), mid)

    assert "changed" in postgresql_path.read_text()
    mysql_mtime_after = (tmp_path / "catalog" / "mysql.properties").stat().st_mtime_ns
    assert mysql_mtime_before == mysql_mtime_after
    assert _plan_environment(second)["CATALOG_STATE_HASH"] != first_hash


def test_catalog_removed(ctx, tmp_path):
    """The catalog directory is updated to remove existing catalogs."""
    # We need the mount for permanence because `ctx.run` is called twice.
    container = trino_container(
        mounts={"home": Mount(location="/usr/lib/trino/etc", source=tmp_path)}
    )
    state_in, _ = build_coordinator_state(container=container)

    # Establish the catalogs on disk.
    first = ctx.run(ctx.on.config_changed(), state_in)
    first_hash = _plan_environment(first)["CATALOG_STATE_HASH"]
    mid = carry_forward(first)
    assert (tmp_path / "catalog" / "postgresql-1.properties").exists()

    # Clear the catalog configuration and reconcile, refreshing the mocked
    # inventory exec so it reflects the catalog files just written.
    mid = refresh_catalog_execs(mid, ctx)
    mid = dataclasses.replace(mid, config={**mid.config, "catalog-config": ""})
    second = ctx.run(ctx.on.config_changed(), mid)

    assert not (tmp_path / "catalog" / "postgresql-1.properties").exists()
    assert not (tmp_path / "catalog" / "bigquery.properties").exists()
    assert _plan_environment(second)["CATALOG_STATE_HASH"] != first_hash


def test_worker_fetches_latest_catalog_on_relation_change(ctx):
    """The worker uses the latest catalog advertised on relation change."""
    state_in, ids = build_worker_state()
    old_catalog = ids.catalog_config

    extended_catalog_config = create_added_catalog_config(
        ids.postgresql,
        ids.mysql,
        ids.redshift,
        ids.bigquery,
        ids.gsheets,
    )

    # Advertise the extended catalog config on the worker relation.
    worker_relation = dataclasses.replace(
        ids.worker_relation,
        remote_app_data={
            **ids.worker_relation.remote_app_data,
            "catalogs": extended_catalog_config,
        },
    )
    relations = {
        relation for relation in state_in.relations if relation.id != ids.worker_relation.id
    }
    relations.add(worker_relation)
    state_in = dataclasses.replace(state_in, relations=relations)

    with ctx(ctx.on.relation_changed(worker_relation), state_in) as mgr:
        mgr.run()
        catalog_config = mgr.charm._effective_catalog_config()

    assert catalog_config == extended_catalog_config
    assert catalog_config != old_catalog


def test_unchanged_catalogs_reconcile_is_a_no_op(ctx, tmp_path, caplog):
    """A reconciliation with unchanged catalogs performs no catalog writes."""
    container = trino_container(
        mounts={"home": Mount(location="/usr/lib/trino/etc", source=tmp_path)}
    )
    state_in, _ = build_coordinator_state(container=container)

    first = ctx.run(ctx.on.config_changed(), state_in)
    mtimes_before = {p.name: p.stat().st_mtime_ns for p in _catalog_dir(tmp_path).iterdir()}
    first_hash = _plan_environment(first)["CATALOG_STATE_HASH"]

    mid = refresh_catalog_execs(carry_forward(first), ctx)
    caplog.clear()

    with caplog.at_level(logging.DEBUG, logger="catalog_planner"):
        second = ctx.run(ctx.on.config_changed(), mid)

    mtimes_after = {p.name: p.stat().st_mtime_ns for p in _catalog_dir(tmp_path).iterdir()}
    assert mtimes_before == mtimes_after
    assert _plan_environment(second)["CATALOG_STATE_HASH"] == first_hash

    planner_records = [r for r in caplog.records if r.name == "catalog_planner"]
    assert [r.getMessage() for r in planner_records] == ["catalogs unchanged"]
    assert not any(r.levelno == logging.INFO for r in planner_records)


def test_credential_file_rotation_changes_hash(ctx, tmp_path):
    """Rotating a connector credential secret changes `CATALOG_STATE_HASH`."""
    container = trino_container(
        mounts={"home": Mount(location="/usr/lib/trino/etc", source=tmp_path)}
    )
    state_in, ids = build_coordinator_state(container=container)

    first = ctx.run(ctx.on.config_changed(), state_in)
    first_hash = _plan_environment(first)["CATALOG_STATE_HASH"]
    properties_before = (tmp_path / "catalog" / "bigquery.properties").read_text()

    mid = refresh_catalog_execs(carry_forward(first), ctx)
    rotated_content = {"service-accounts": BIGQUERY_SECRET.replace("key123", "key456")}
    bigquery_secret = next(s for s in mid.secrets if s.id == ids.bigquery)
    rotated_secret = dataclasses.replace(
        bigquery_secret, tracked_content=rotated_content, latest_content=rotated_content
    )
    mid = dataclasses.replace(
        mid, secrets={s for s in mid.secrets if s.id != ids.bigquery} | {rotated_secret}
    )

    second = ctx.run(ctx.on.config_changed(), mid)

    # The rendered `.properties` file is unaffected; only the credential
    # file content, which the aggregate hash also covers, changed.
    assert (tmp_path / "catalog" / "bigquery.properties").read_text() == properties_before
    assert _plan_environment(second)["CATALOG_STATE_HASH"] != first_hash


def test_duplicate_catalog_claim_blocks_without_mutation(ctx, tmp_path, monkeypatch):
    """A name claimed by both static and dynamic state blocks with no mutation."""
    container = trino_container(
        mounts={"home": Mount(location="/usr/lib/trino/etc", source=tmp_path)}
    )
    state_in, _ = build_coordinator_state(container=container)

    first = ctx.run(ctx.on.config_changed(), state_in)
    mtimes_before = {p.name: p.stat().st_mtime_ns for p in _catalog_dir(tmp_path).iterdir()}
    first_hash = _plan_environment(first)["CATALOG_STATE_HASH"]

    mid = refresh_catalog_execs(carry_forward(first), ctx)
    monkeypatch.setattr(
        PostgresqlCatalogRelationHandler,
        "render_dynamic_catalogs",
        lambda self: {"mysql": {"connector.name": "postgresql"}},
    )

    second = ctx.run(ctx.on.config_changed(), mid)

    mtimes_after = {p.name: p.stat().st_mtime_ns for p in _catalog_dir(tmp_path).iterdir()}
    assert mtimes_before == mtimes_after
    assert _plan_environment(second)["CATALOG_STATE_HASH"] == first_hash
    assert second.unit_status == BlockedStatus(
        "catalog name(s) claimed by both static and dynamic state: mysql"
    )


def test_failed_inventory_blocks_mutation_and_plan(ctx, tmp_path):
    """A failed inventory snapshot leaves the workload and plan unchanged."""
    container = trino_container(
        mounts={"home": Mount(location="/usr/lib/trino/etc", source=tmp_path)}
    )
    state_in, _ = build_coordinator_state(container=container)

    first = ctx.run(ctx.on.config_changed(), state_in)
    mtimes_before = {p.name: p.stat().st_mtime_ns for p in _catalog_dir(tmp_path).iterdir()}
    first_hash = _plan_environment(first)["CATALOG_STATE_HASH"]

    mid = refresh_catalog_execs(carry_forward(first), ctx)
    cont = mid.get_container("trino")
    catalog_command = tuple(inventory_command(CATALOG_INVENTORY_PATH))
    other_execs = {e for e in cont.execs if e.command_prefix != catalog_command}
    broken_cont = dataclasses.replace(
        cont, execs=frozenset(other_execs | {failed_inventory_exec(CATALOG_INVENTORY_PATH)})
    )
    mid = dataclasses.replace(mid, containers={broken_cont})

    second = ctx.run(ctx.on.config_changed(), mid)

    mtimes_after = {p.name: p.stat().st_mtime_ns for p in _catalog_dir(tmp_path).iterdir()}
    assert mtimes_before == mtimes_after
    assert _plan_environment(second)["CATALOG_STATE_HASH"] == first_hash


def test_plan_size_is_independent_of_catalog_count(ctx, tmp_path):
    """The plan carries a single catalog hash regardless of how many catalogs exist."""
    container = trino_container(
        mounts={"home": Mount(location="/usr/lib/trino/etc", source=tmp_path)}
    )
    state_in, ids = build_coordinator_state(container=container)

    first = ctx.run(ctx.on.config_changed(), state_in)
    first_env = _plan_environment(first)

    mid = refresh_catalog_execs(carry_forward(first), ctx)
    extended_catalog_config = create_added_catalog_config(
        ids.postgresql,
        ids.mysql,
        ids.redshift,
        ids.bigquery,
        ids.gsheets,
    )
    mid = dataclasses.replace(
        mid, config={**mid.config, "catalog-config": extended_catalog_config}
    )

    second = ctx.run(ctx.on.config_changed(), mid)
    second_env = _plan_environment(second)

    assert not [key for key in first_env if key.startswith("HASH_CATALOG")]
    assert first_env.keys() == second_env.keys()
    assert "CATALOG_STATE_HASH" in first_env


def test_replica_suffix_collision_blocks(ctx, tmp_path, monkeypatch):
    """A dynamic claim on a suffix-expanded static catalog name blocks the unit."""
    container = trino_container(
        mounts={"home": Mount(location="/usr/lib/trino/etc", source=tmp_path)}
    )
    state_in, _ = build_coordinator_state(container=container)

    first = ctx.run(ctx.on.config_changed(), state_in)
    first_hash = _plan_environment(first)["CATALOG_STATE_HASH"]
    mtimes_before = {p.name: p.stat().st_mtime_ns for p in _catalog_dir(tmp_path).iterdir()}

    mid = refresh_catalog_execs(carry_forward(first), ctx)
    monkeypatch.setattr(
        PostgresqlCatalogRelationHandler,
        "render_dynamic_catalogs",
        lambda self: {"postgresql-1_developer": {"connector.name": "postgresql"}},
    )

    second = ctx.run(ctx.on.config_changed(), mid)

    mtimes_after = {p.name: p.stat().st_mtime_ns for p in _catalog_dir(tmp_path).iterdir()}
    assert mtimes_before == mtimes_after
    assert _plan_environment(second)["CATALOG_STATE_HASH"] == first_hash
    assert second.unit_status == BlockedStatus(
        "catalog name(s) claimed by both static and dynamic state: postgresql-1_developer"
    )


def test_unrenderable_catalog_config_blocks_without_mutation(ctx, tmp_path):
    """A catalog referencing a missing backend blocks with the workload untouched."""
    container = trino_container(
        mounts={"home": Mount(location="/usr/lib/trino/etc", source=tmp_path)}
    )
    state_in, _ = build_coordinator_state(container=container)

    first = ctx.run(ctx.on.config_changed(), state_in)
    first_hash = _plan_environment(first)["CATALOG_STATE_HASH"]
    mtimes_before = {p.name: p.stat().st_mtime_ns for p in _catalog_dir(tmp_path).iterdir()}

    mid = refresh_catalog_execs(carry_forward(first), ctx)
    broken_config = state_in.config["catalog-config"].replace(
        "backend: dwh", "backend: missing-backend", 1
    )
    mid = dataclasses.replace(mid, config={**mid.config, "catalog-config": broken_config})

    second = ctx.run(ctx.on.config_changed(), mid)

    mtimes_after = {p.name: p.stat().st_mtime_ns for p in _catalog_dir(tmp_path).iterdir()}
    assert mtimes_before == mtimes_after
    assert _plan_environment(second)["CATALOG_STATE_HASH"] == first_hash
    assert second.unit_status == BlockedStatus("invalid catalog configuration")


def test_worker_without_coordinator_clears_charm_owned_dirs(ctx, tmp_path):
    """A worker with no coordinator relation drops catalogs and credentials."""
    catalog_dir = tmp_path / "catalog"
    catalog_dir.mkdir()
    (catalog_dir / "stale.properties").write_text("connector.name=postgresql\n")
    credential_dir = tmp_path / "credentials"
    credential_dir.mkdir()
    (credential_dir / "stale.json").write_text("{}")

    container = trino_container(
        mounts={"home": Mount(location="/usr/lib/trino/etc", source=tmp_path)}
    )
    state_in, ids = build_worker_state(container=container)
    state_in = dataclasses.replace(
        state_in,
        relations={r for r in state_in.relations if r.id != ids.worker_relation.id},
    )

    ctx.run(ctx.on.config_changed(), state_in)

    assert not catalog_dir.exists()
    assert not credential_dir.exists()


def test_dynamic_password_rotation_replans_once_without_sql(ctx, tmp_path, monkeypatch):
    """Rotating a PostgreSQL password replans once and issues no catalog SQL."""
    dynamic_props = {
        "connector.name": "postgresql",
        "connection-url": "jdbc:postgresql://db:5432/mydb",
        "connection-user": "trino",
        "connection-password": "${ENV:MYDB}",
        "query.comment-format": "dynamic catalog",
    }
    calls = []
    monkeypatch.setattr(
        PostgresqlCatalogRelationHandler,
        "render_dynamic_catalogs",
        lambda self: {"pgdyn": dict(dynamic_props)},
    )
    monkeypatch.setattr(PostgresqlCatalogRelationHandler, "is_trino_ready", lambda self: True)
    monkeypatch.setattr(
        PostgresqlCatalogRelationHandler,
        "create_catalog",
        lambda self, name, properties: calls.append(("create", name)),
    )
    monkeypatch.setattr(
        PostgresqlCatalogRelationHandler,
        "drop_catalog",
        lambda self, name: calls.append(("drop", name)),
    )
    monkeypatch.setattr(
        PostgresqlCatalogRelationHandler,
        "get_postgresql_env_vars",
        lambda self: {"MYDB": "pwd-1"},
    )

    container = trino_container(
        mounts={"home": Mount(location="/usr/lib/trino/etc", source=tmp_path)}
    )
    state_in, _ = build_coordinator_state(container=container)

    first = ctx.run(ctx.on.config_changed(), state_in)
    assert calls == [("create", "pgdyn")]
    first_hash = _plan_environment(first)["CATALOG_STATE_HASH"]

    # Trino owns the dynamic catalog file, so stand in for the server having
    # written it after the CREATE CATALOG statement above.
    (tmp_path / "catalog" / "pgdyn.properties").write_text(
        "".join(f"{key}={value}\n" for key, value in dynamic_props.items())
    )
    mid = refresh_catalog_execs(carry_forward(first), ctx)
    monkeypatch.setattr(
        PostgresqlCatalogRelationHandler,
        "get_postgresql_env_vars",
        lambda self: {"MYDB": "pwd-2"},
    )
    calls.clear()

    replans = []
    original_replan = Container.replan
    monkeypatch.setattr(
        Container,
        "replan",
        lambda self: (replans.append(self.name), original_replan(self))[1],
    )

    second = ctx.run(ctx.on.config_changed(), mid)

    assert calls == []
    assert replans == ["trino"]
    second_env = _plan_environment(second)
    assert second_env["MYDB"] == "pwd-2"
    assert second_env["CATALOG_STATE_HASH"] == first_hash
