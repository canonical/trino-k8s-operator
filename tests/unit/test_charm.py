# Copyright 2023 Canonical Ltd.
# See LICENSE file for licensing details.
#
# Learn more about testing at: https://juju.is/docs/sdk/testing


"""Trino charm unit tests."""

# pylint:disable=protected-access,too-many-public-methods

import dataclasses
import json
import logging
from unittest import mock

import pytest
import yaml
from ops.model import (
    ActiveStatus,
    BlockedStatus,
    MaintenanceStatus,
    SecretNotFoundError,
    WaitingStatus,
)
from ops.pebble import CheckLevel, CheckStartup, CheckStatus, Layer
from ops.testing import CheckInfo, Mount, PeerRelation, Relation, State

from relations.postgresql_catalog import PostgresqlCatalogRelationHandler
from tests.unit.helpers import (
    BIGQUERY_CATALOG_PATH,
    DEFAULT_JVM_STRING,
    POSTGRESQL_1_CATALOG_PATH,
    POSTGRESQL_1_DEVELOPER_CATALOG_PATH,
    POSTGRESQL_REPLICA_SECRET,
    POSTGRESQL_REPLICA_SECRET_WITH_PARAMS,
    SERVER_PORT,
    build_coordinator_state,
    build_worker_state,
    carry_forward,
    create_single_catalog_config,
    ingress_relation,
    oauth_relation,
    observer_secret,
    peer_state_value,
    trino_container,
    workload_path,
)

mock_incomplete_pebble_plan = {"services": {"trino": {"override": "replace"}}}

logger = logging.getLogger(__name__)


def _services(state):
    """Return the rendered Pebble services for the trino container."""
    return state.get_container("trino").plan.to_dict()["services"]


def test_initial_plan(ctx):
    """The initial pebble plan is empty."""
    state = State(
        leader=True,
        relations={PeerRelation("peer")},
        containers={trino_container()},
    )
    with ctx(ctx.on.update_status(), state) as mgr:
        initial_plan = mgr.charm.unit.get_container("trino").get_plan().to_dict()
        assert initial_plan == {}


def test_waiting_on_peer_relation_not_ready(ctx):
    """The charm is blocked without a peer relation."""
    container = trino_container()
    state = State(leader=True, containers={container})

    state_out = ctx.run(ctx.on.pebble_ready(container), state)

    # No plans are set yet.
    assert state_out.get_container("trino").plan.to_dict() == {}

    # The WaitingStatus is set with a message.
    assert state_out.unit_status == WaitingStatus("waiting for peer relation")


def test_ready(ctx):
    """The pebble plan is correctly generated when the charm is ready."""
    state_in, _ = build_coordinator_state()

    state_out = ctx.run(ctx.on.config_changed(), state_in)

    # The status reflects the healthy `up` check reported by Scenario.
    assert state_out.unit_status == ActiveStatus("Status check: UP")

    # The plan is generated after config is applied.
    want_plan = {
        "services": {
            "trino": {
                "override": "replace",
                "summary": "trino server",
                "command": "./entrypoint.sh",
                "startup": "enabled",
                "on-check-failure": {"up": "restart"},
                "environment": {
                    "PASSWORD_DB_PATH": "/usr/lib/trino/etc/password.db",  # nosec
                    "LOG_LEVEL": "info",
                    "OAUTH_CLIENT_ID": None,
                    "OAUTH_CLIENT_SECRET": None,  # nosec
                    "OAUTH_ISSUER_URL": None,
                    "OAUTH_AUTHORIZATION_ENDPOINT": None,
                    "OAUTH_TOKEN_ENDPOINT": None,
                    "OAUTH_USERINFO_ENDPOINT": None,
                    "OAUTH_JWKS_ENDPOINT": None,
                    "OAUTH_SCOPES": None,
                    "OAUTH_HTTP_PROXY": None,
                    "OAUTH_HTTP_PROXY_SECURE": None,
                    "CHARM_FUNCTION": "coordinator",
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
                    "JVM_OPTIONS": DEFAULT_JVM_STRING,
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
        },
    }
    got_services = _services(state_out)
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

    assert got_services == want_plan["services"]


def test_ingress_publishes_app_data(ctx):
    """The charm publishes the correct app data to the ingress relation databag.

    The IngressPerAppRequirer library publishes app data on ingress relation events.
    We fire a relation-joined event on the ingress endpoint and verify the databag.
    """
    ingress = Relation("ingress", remote_app_name="traefik-k8s")
    state_in, _ = build_coordinator_state(extra_relations=(ingress,))

    state_out = ctx.run(ctx.on.relation_changed(ingress, remote_unit=0), state_in)

    app_data = state_out.get_relation(ingress.id).local_app_data
    assert app_data.get("port") == str(int(SERVER_PORT))
    assert app_data.get("strip-prefix") == "true"
    assert app_data.get("redirect-https") == "true"
    assert app_data.get("model") is not None
    assert app_data.get("name") is not None


def test_deprecated_config_no_validation_error(ctx):
    """Deprecated config options external-hostname and tls-secret-name are accepted as no-ops."""
    state_in, _ = build_coordinator_state(
        config={"external-hostname": "trino.example.com", "tls-secret-name": "my-tls-secret"},
    )

    state_out = ctx.run(ctx.on.config_changed(), state_in)

    # Charm must not block on these deprecated values.
    assert state_out.unit_status != BlockedStatus(
        "tls-secret-name must be a valid Kubernetes resource name "
        "(lowercase alphanumeric and hyphens), got 'my-tls-secret'"
    )
    assert not isinstance(state_out.unit_status, BlockedStatus)


def test_invalid_config_value(ctx):
    """The charm blocks if an invalid config value is provided."""
    # Seed the container with the previously-applied (valid) plan so we can
    # assert the invalid value is not propagated to the running plan.
    container = trino_container(
        layers={
            "trino": Layer(
                {
                    "services": {
                        "trino": {
                            "override": "replace",
                            "command": "./entrypoint.sh",
                            "environment": {"LOG_LEVEL": "info"},
                        }
                    }
                }
            )
        }
    )
    state_in, _ = build_coordinator_state(config={"log-level": "all-logs"}, container=container)

    state_out = ctx.run(ctx.on.config_changed(), state_in)

    # The change is not applied to the plan.
    got_log_level = _services(state_out)["trino"]["environment"]["LOG_LEVEL"]
    assert got_log_level == "info"

    # The BlockStatus is set with a message.
    assert state_out.unit_status == BlockedStatus("config: invalid log level 'all-logs'")


def test_incorrect_relation(ctx):
    """The charm blocks if the coordinator relation is not added."""
    state_in, _ = build_coordinator_state(config={"charm-function": "worker"})

    state_out = ctx.run(ctx.on.config_changed(), state_in)

    assert state_out.unit_status == BlockedStatus("Incorrect trino relation configuration.")


def test_catalog_invalid_config(ctx):
    """The charm blocks when catalog-config is missing required top-level keys."""
    state_in, _ = build_coordinator_state(config={"catalog-config": "catalog: incorrect"})

    state_out = ctx.run(ctx.on.config_changed(), state_in)

    assert isinstance(state_out.unit_status, BlockedStatus)
    assert "catalog-config" in state_out.unit_status.message


def test_postgresql_catalog_config_bad_prefix(ctx):
    """The charm blocks when a postgresql-catalog-config entry has an invalid database_prefix."""
    bad_config = "pg-app:\n  database_prefix: mydb\n  ro_catalog_name: mycat\n"
    state_in, _ = build_coordinator_state(config={"postgresql-catalog-config": bad_config})

    state_out = ctx.run(ctx.on.config_changed(), state_in)

    assert isinstance(state_out.unit_status, BlockedStatus)
    assert "database_prefix" in state_out.unit_status.message


def test_postgresql_catalog_config_no_catalog_name(ctx):
    """The charm blocks when a postgresql-catalog-config entry has no catalog name."""
    bad_config = "pg-app:\n  database_prefix: mydb*\n"
    state_in, _ = build_coordinator_state(config={"postgresql-catalog-config": bad_config})

    state_out = ctx.run(ctx.on.config_changed(), state_in)

    assert isinstance(state_out.unit_status, BlockedStatus)
    assert "ro_catalog_name" in state_out.unit_status.message


def test_postgresql_catalog_config_duplicate_catalog_names(ctx):
    """The charm blocks when two postgresql-catalog-config entries share a catalog name."""
    duplicate_config = (
        "pg-app-a:\n  database_prefix: db_a*\n  ro_catalog_name: shared_cat\n"
        "pg-app-b:\n  database_prefix: db_b*\n  ro_catalog_name: shared_cat\n"
    )
    state_in, _ = build_coordinator_state(config={"postgresql-catalog-config": duplicate_config})

    state_out = ctx.run(ctx.on.config_changed(), state_in)

    assert isinstance(state_out.unit_status, BlockedStatus)
    assert "Duplicate" in state_out.unit_status.message
    assert "shared_cat" in state_out.unit_status.message


def test_postgresql_catalog_config_clashes_with_static(ctx):
    """The charm blocks when a postgresql-catalog-config name clashes with catalog-config."""
    static_config = yaml.dump(
        {
            "catalogs": {"my_static_cat": {"backend": "pg"}},
            "backends": {"pg": {"connector": "postgresql"}},
        }
    )
    pg_config = "pg-app:\n  database_prefix: db*\n  ro_catalog_name: my_static_cat\n"
    state_in, _ = build_coordinator_state(
        config={"catalog-config": static_config, "postgresql-catalog-config": pg_config}
    )

    state_out = ctx.run(ctx.on.config_changed(), state_in)

    assert isinstance(state_out.unit_status, BlockedStatus)
    assert "clashes with catalog-config" in state_out.unit_status.message


def test_session_property_manager_invalid_config(ctx):
    """The charm blocks when the session property manager JSON is invalid."""
    state_in, _ = build_coordinator_state(
        config={"session-property-manager-config": '{"group":"broken"'}
    )

    state_out = ctx.run(ctx.on.config_changed(), state_in)

    assert "Expecting ',' delimiter" in state_out.unit_status.message
    assert isinstance(state_out.unit_status, BlockedStatus)


def test_update_status_up(ctx):
    """The charm updates the unit status to active based on UP status."""
    state_in, _ = build_coordinator_state()
    mid = ctx.run(ctx.on.config_changed(), state_in)

    container = dataclasses.replace(
        mid.get_container("trino"),
        check_infos={
            CheckInfo(
                "up",
                status=CheckStatus.UP,
                level=CheckLevel.UNSET,
                startup=CheckStartup.UNSET,
                threshold=None,
            )
        },
    )
    state = dataclasses.replace(mid, containers={container})

    state_out = ctx.run(ctx.on.update_status(), state)

    assert state_out.unit_status == ActiveStatus("Status check: UP")


def test_update_status_down(ctx):
    """The charm updates the unit status to maintenance based on DOWN status."""
    state_in, _ = build_coordinator_state()
    mid = ctx.run(ctx.on.config_changed(), state_in)

    container = dataclasses.replace(
        mid.get_container("trino"),
        check_infos={
            CheckInfo(
                "up",
                status=CheckStatus.DOWN,
                level=CheckLevel.UNSET,
                startup=CheckStartup.UNSET,
                threshold=None,
            )
        },
    )
    state = dataclasses.replace(mid, containers={container})

    state_out = ctx.run(ctx.on.update_status(), state)

    assert state_out.unit_status == MaintenanceStatus("Status check: DOWN")


def test_incomplete_pebble_plan(ctx):
    """The charm re-applies the pebble plan if incomplete."""
    container = trino_container(layers={"trino": Layer(mock_incomplete_pebble_plan)})
    state_in, _ = build_coordinator_state(container=container)

    state_out = ctx.run(ctx.on.update_status(), state_in)

    assert state_out.unit_status == ActiveStatus("Status check: UP")
    assert state_out.get_container("trino").plan.to_dict() != mock_incomplete_pebble_plan


def test_trino_coordinator_relation(ctx):
    """Test trino relation.

    The coordinator and worker Trino charms relate correctly.
    """
    state_in, ids = build_coordinator_state()

    state_out = ctx.run(ctx.on.config_changed(), state_in)

    relation_data = state_out.get_relation(ids.coordinator_relation.id).local_app_data
    assert relation_data["discovery-uri"] == "http://trino-k8s.trino-model.svc.cluster.local:8080"
    assert relation_data["catalogs"] == ids.catalog_config


def test_trino_coordinator_relation_discovery_uri_override(ctx):
    """When discovery-uri config is set, the override is published to workers.

    Workers in cross-cluster or multi-network topologies need the coordinator
    to advertise a reachable address rather than the cluster-local default.
    """
    state_in, ids = build_coordinator_state(
        config={"discovery-uri": "http://trino.example.com:8080"}
    )

    state_out = ctx.run(ctx.on.config_changed(), state_in)

    relation_data = state_out.get_relation(ids.coordinator_relation.id).local_app_data
    assert relation_data["discovery-uri"] == "http://trino.example.com:8080"

    # The override is also reflected in the coordinator's own Pebble environment.
    environment = _services(state_out)["trino"]["environment"]
    assert environment["DISCOVERY_URI"] == "http://trino.example.com:8080"


def test_trino_coordinator_relation_broken(ctx):
    """Test trino relation broken.

    The coordinator catalog-config secret cannot be resolved.
    """
    state_in, ids = build_coordinator_state()

    with ctx(ctx.on.relation_broken(ids.coordinator_relation), state_in) as mgr:
        mgr.run()
        with pytest.raises(SecretNotFoundError):
            mgr.charm.model.get_secret(label="catalog-config")


def test_trino_worker_relation_created(ctx):
    """Test trino relation creation.

    The coordinator and worker Trino charms relate correctly.
    """
    state_in, ids = build_worker_state()

    state_out = ctx.run(ctx.on.relation_changed(ids.worker_relation), state_in)

    assert workload_path(state_out, ctx, BIGQUERY_CATALOG_PATH).exists()
    assert workload_path(state_out, ctx, POSTGRESQL_1_CATALOG_PATH).exists()


def test_worker_uses_password_authentication(ctx):
    """A worker without OAuth environment fields renders password-only authentication."""
    state_in, ids = build_worker_state()

    state_out = ctx.run(ctx.on.relation_changed(ids.worker_relation), state_in)

    config = workload_path(state_out, ctx, "/usr/lib/trino/etc/config.properties").read_text()
    assert "http-server.authentication.type=PASSWORD" in config
    assert "http-server.authentication.type=oauth2,PASSWORD" not in config
    assert "http-server.authentication.oauth2." not in config


def test_trino_worker_relation_broken(ctx, tmp_path):
    """Test trino relation broken.

    The coordinator and worker Trino charms relation is broken.
    """
    container = trino_container(
        mounts={"home": Mount(location="/usr/lib/trino/etc", source=tmp_path)}
    )
    state_in, ids = build_worker_state(container=container)

    # Establish the worker catalogs on disk via a relation-changed event.
    mid = ctx.run(ctx.on.relation_changed(ids.worker_relation), state_in)
    assert (tmp_path / "catalog" / "postgresql-1.properties").exists()

    ctx.run(ctx.on.relation_broken(ids.worker_relation), mid)

    assert not (tmp_path / "catalog" / "postgresql-1.properties").exists()


def test_trino_single_node_deployment(ctx):
    """Test pebble plan is created with single node deployment."""
    state = State(
        leader=True,
        config={"charm-function": "all"},
        relations={PeerRelation("peer")},
        containers={trino_container()},
    )

    state_out = ctx.run(ctx.on.config_changed(), state)

    # There is a valid pebble plan.
    assert _services(state_out)["trino"]["environment"]["CHARM_FUNCTION"] == "all"

    # The status reflects the healthy `up` check reported by Scenario.
    assert state_out.unit_status == ActiveStatus("Status check: UP")


def test_resource_management_config(ctx):
    """Test resource management configuration variables.

    The charm includes resource management variables in the environment
    with the correct values when configured.
    """
    state_in, _ = build_coordinator_state(
        config={
            "query-max-cpu-time": "1h",
            "query-max-memory-per-node": "2GB",
            "query-max-memory": "10GB",
            "query-max-total-memory": "15GB",
            "memory-heap-headroom-per-node": "1GB",
        }
    )

    state_out = ctx.run(ctx.on.config_changed(), state_in)

    environment = _services(state_out)["trino"]["environment"]
    assert environment["QUERY_MAX_CPU_TIME"] == "1h"
    assert environment["QUERY_MAX_MEMORY_PER_NODE"] == "2GB"
    assert environment["QUERY_MAX_MEMORY"] == "10GB"
    assert environment["QUERY_MAX_TOTAL_MEMORY"] == "15GB"
    assert environment["MEMORY_HEAP_HEADROOM_PER_NODE"] == "1GB"


def test_session_property_manager_files_created(ctx):
    """The charm writes the session property manager files when configured."""
    session_property_config = (
        '[{"group":"global.*","sessionProperties":{"query_max_execution_time":"8h"}}]'
    )
    state_in, _ = build_coordinator_state(
        config={"session-property-manager-config": session_property_config}
    )

    state_out = ctx.run(ctx.on.config_changed(), state_in)

    properties_path = "/usr/lib/trino/etc/session-property-config.properties"
    json_path = "/usr/lib/trino/etc/session-property-config.json"

    assert workload_path(state_out, ctx, properties_path).exists()
    json_file = workload_path(state_out, ctx, json_path)
    assert json_file.exists()
    assert json_file.read_text() == session_property_config


def test_session_property_manager_files_removed(ctx, tmp_path):
    """The charm removes the session property manager files when unset."""
    # Pre-populate the manager files so the empty config must remove them.
    (tmp_path / "session-property-config.properties").write_text("stale")
    (tmp_path / "session-property-config.json").write_text("[]")
    container = trino_container(
        mounts={"home": Mount(location="/usr/lib/trino/etc", source=tmp_path)}
    )
    state_in, _ = build_coordinator_state(
        config={"session-property-manager-config": ""}, container=container
    )

    ctx.run(ctx.on.config_changed(), state_in)

    assert not (tmp_path / "session-property-config.properties").exists()
    assert not (tmp_path / "session-property-config.json").exists()


def test_resource_group_manager_files_created(ctx):
    """The charm writes the resource group manager files when configured."""
    resource_groups_config = (
        '{"rootGroups":[{"name":"global","softMemoryLimit":"80%",'
        '"hardConcurrencyLimit":10,"maxQueued":10}],"selectors":'
        '[{"user":".*","group":"global"}]}'
    )
    state_in, _ = build_coordinator_state(
        config={"resource-groups-config": resource_groups_config}
    )

    state_out = ctx.run(ctx.on.config_changed(), state_in)

    properties_path = "/usr/lib/trino/etc/resource-groups.properties"
    json_path = "/usr/lib/trino/etc/resource-groups.json"

    assert workload_path(state_out, ctx, properties_path).exists()
    json_file = workload_path(state_out, ctx, json_path)
    assert json_file.exists()
    assert json_file.read_text() == resource_groups_config


def test_resource_group_manager_files_removed(ctx, tmp_path):
    """The charm removes the resource group manager files when unset."""
    (tmp_path / "resource-groups.properties").write_text("stale")
    (tmp_path / "resource-groups.json").write_text("{}")
    container = trino_container(
        mounts={"home": Mount(location="/usr/lib/trino/etc", source=tmp_path)}
    )
    state_in, _ = build_coordinator_state(
        config={"resource-groups-config": ""}, container=container
    )

    ctx.run(ctx.on.config_changed(), state_in)

    assert not (tmp_path / "resource-groups.properties").exists()
    assert not (tmp_path / "resource-groups.json").exists()


def test_per_replica_params_override_backend_params(ctx):
    """Per-replica params override backend params in rendered catalog files.

    The rw replica and ro replica must get the targetServerType declared
    in their respective replica params, not a shared value from the backend.
    """
    pg_secret = observer_secret({"replicas": POSTGRESQL_REPLICA_SECRET_WITH_PARAMS})
    catalog_config = create_single_catalog_config(pg_secret.id)
    state_in, _ = build_coordinator_state(
        config={"catalog-config": catalog_config}, extra_secrets=(pg_secret,)
    )

    state_out = ctx.run(ctx.on.config_changed(), state_in)

    ro_props = workload_path(state_out, ctx, POSTGRESQL_1_CATALOG_PATH).read_text()
    rw_props = workload_path(state_out, ctx, POSTGRESQL_1_DEVELOPER_CATALOG_PATH).read_text()

    assert "targetServerType=preferSecondary" in ro_props
    assert "targetServerType=primary" in rw_props
    assert "targetServerType=preferSecondary" not in rw_props


def test_backend_params_applied_when_replica_params_absent(ctx):
    """Backend params are used for all replicas when no per-replica params are set.

    Verifies the fallback path: replicas without their own params inherit
    the backend-level params unchanged.
    """
    pg_secret = observer_secret({"replicas": POSTGRESQL_REPLICA_SECRET})
    catalog_config = create_single_catalog_config(
        pg_secret.id, backend_params="ssl=false&targetServerType=primary"
    )
    state_in, _ = build_coordinator_state(
        config={"catalog-config": catalog_config}, extra_secrets=(pg_secret,)
    )

    state_out = ctx.run(ctx.on.config_changed(), state_in)

    ro_props = workload_path(state_out, ctx, POSTGRESQL_1_CATALOG_PATH).read_text()
    rw_props = workload_path(state_out, ctx, POSTGRESQL_1_DEVELOPER_CATALOG_PATH).read_text()

    assert "targetServerType=primary" in ro_props
    assert "targetServerType=primary" in rw_props


def test_replica_params_override_backend_params_when_both_present(ctx):
    """Replica params take precedence over backend params when both are declared.

    Verifies the override path: even when the backend has params, each
    replica's own params replace them entirely for that catalog file.
    """
    pg_secret = observer_secret({"replicas": POSTGRESQL_REPLICA_SECRET_WITH_PARAMS})
    catalog_config = create_single_catalog_config(
        pg_secret.id, backend_params="ssl=false&targetServerType=primary"
    )
    state_in, _ = build_coordinator_state(
        config={"catalog-config": catalog_config}, extra_secrets=(pg_secret,)
    )

    state_out = ctx.run(ctx.on.config_changed(), state_in)

    ro_props = workload_path(state_out, ctx, POSTGRESQL_1_CATALOG_PATH).read_text()
    rw_props = workload_path(state_out, ctx, POSTGRESQL_1_DEVELOPER_CATALOG_PATH).read_text()

    assert "targetServerType=preferSecondary" in ro_props
    assert "targetServerType=primary" not in ro_props
    assert "targetServerType=primary" in rw_props
    assert "targetServerType=preferSecondary" not in rw_props


def test_coordinator_publishes_int_comms_secret_id(ctx):
    """Coordinator writes int-comms-secret-id to the relation databag instead of plaintext.

    Asserts that:
    - the coordinator relation databag contains `int-comms-secret-id`
    - the relation databag does NOT contain any plaintext secret value
    """
    state_in, ids = build_coordinator_state()

    state_out = ctx.run(ctx.on.config_changed(), state_in)

    relation_data = state_out.get_relation(ids.coordinator_relation.id).local_app_data

    # The secret ID must be present.
    assert "int-comms-secret-id" in relation_data
    assert relation_data["int-comms-secret-id"].startswith("secret:")

    # The raw int-comms value must NOT appear in the relation databag.
    assert "int-comms-secret" not in relation_data
    assert "int_comms_secret" not in relation_data


def test_coordinator_int_comms_secret_is_singleton(ctx):
    """Calling update_coordinator_relation_data twice reuses the same secret."""
    state_in, ids = build_coordinator_state()

    with ctx(ctx.on.config_changed(), state_in) as mgr:
        mgr.run()
        relation = mgr.charm.model.get_relation("trino-coordinator")
        first_id = relation.data[mgr.charm.app].get("int-comms-secret-id")

        # Trigger a second update cycle (e.g. config changed).
        mgr.charm.trino_coordinator.update_coordinator_relation_data()
        second_id = relation.data[mgr.charm.app].get("int-comms-secret-id")

    assert first_id == second_id, "Singleton secret ID must not change between updates"


def test_coordinator_int_comms_secret_preserves_existing_value(ctx):
    """When peer state already carries an int-comms value, the Juju secret reuses it."""
    # Pre-seed the peer state with a known value (JSON-encoded as the state store does).
    peer = PeerRelation(
        "peer",
        local_app_data={"int_comms_secret": '"pre-existing-secret-value"'},  # nosec
    )
    # can_connect False so update-status returns early without creating the secret.
    state = State(
        leader=True,
        relations={peer},
        containers={trino_container(can_connect=False)},
    )

    with ctx(ctx.on.update_status(), state) as mgr:
        mgr.run()
        secret = mgr.charm.trino_coordinator._get_or_create_int_comms_secret()
        assert secret is not None
        content = secret.get_content(refresh=True)
        assert content["secret"] == "pre-existing-secret-value"  # nosec

        # Calling it again must return the same secret (singleton).
        secret2 = mgr.charm.trino_coordinator._get_or_create_int_comms_secret()
        assert secret2 is not None
        content2 = secret2.get_content(refresh=True)
        assert content2["secret"] == "pre-existing-secret-value"  # nosec


LEGACY_STATE_SEED = {
    "opensearch": {"username": "u", "password": "p"},  # nosec B105 sensitive
    "opensearch_certificate": "ca-cert",
    "discovery_uri": "http://old-coordinator:8080",
    "catalog_config": "old-catalog-config",
    "user_secret_id": "secret:olduser",  # nosec B105
    "int_comms_secret_id": "secret:oldid",  # nosec B105
    "ranger_enabled": True,
    "policy_manager_url": "http://old-ranger",
    "java_truststore_pwd": "old-truststore-pw",  # nosec B105
    "int_comms_secret": "old-int-comms-value",  # nosec B105
}


def _seed_peer_state(state_in, ids, values):
    """Replace the peer relation with one pre-seeded with JSON-encoded state."""
    seeded = PeerRelation(
        "peer",
        local_app_data={key: json.dumps(value) for key, value in values.items()},
    )
    relations = {rel for rel in state_in.relations if rel.id != ids.peer_relation.id}
    relations.add(seeded)
    return dataclasses.replace(state_in, relations=relations), seeded


def test_leader_purges_legacy_state_on_reconcile(ctx):
    """Leader drops obsolete and fallback peer keys once secrets are confirmed."""
    state_in, ids = build_coordinator_state()
    state_in, seeded = _seed_peer_state(state_in, ids, LEGACY_STATE_SEED)

    state_out = ctx.run(ctx.on.config_changed(), state_in)

    peer = state_out.get_relation(seeded.id)
    for key in LEGACY_STATE_SEED:
        assert peer_state_value(peer, key) is None, f"{key!r} should be purged"


def test_non_leader_keeps_legacy_state(ctx):
    """A non-leader never mutates peer state, so legacy keys survive."""
    state_in, ids = build_coordinator_state(leader=False)
    state_in, seeded = _seed_peer_state(state_in, ids, LEGACY_STATE_SEED)

    state_out = ctx.run(ctx.on.config_changed(), state_in)

    peer = state_out.get_relation(seeded.id)
    for key, value in LEGACY_STATE_SEED.items():
        assert peer_state_value(peer, key) == value, f"{key!r} must be preserved"


def test_worker_resolves_int_comms_secret_from_coordinator(ctx):
    """Worker reads int-comms-secret from coordinator.

    Worker reads int-comms-secret-id live from the trino-worker relation databag
    and resolves the secret value at render time via _get_int_comms_secret_value.
    """
    state_in, ids = build_worker_state()

    with ctx(ctx.on.relation_changed(ids.worker_relation), state_in) as mgr:
        mgr.run()

        # The coordinator databag must carry the secret *ID* (not the plaintext value).
        secret_id = mgr.charm.trino_worker.get_coordinator_data()["int_comms_secret_id"]
        assert secret_id is not None
        assert secret_id.startswith("secret:")

        # _get_int_comms_secret_value must resolve to the actual secret content.
        assert mgr.charm._get_int_comms_secret_value() == "test-int-comms-secret"  # nosec


def test_worker_waits_when_int_comms_secret_id_absent(ctx):
    """Worker goes into WaitingStatus when int-comms-secret-id is not yet in relation data."""
    state_in, ids = build_worker_state(include_int_comms=False)

    state_out = ctx.run(ctx.on.relation_changed(ids.worker_relation), state_in)

    assert state_out.unit_status == WaitingStatus(
        "waiting for coordinator to publish internal communication secret"
    )


def test_worker_no_plaintext_secret_in_relation_databag(ctx):
    """Worker never writes a plaintext internal communication secret to relation data.

    This is the cross-model / cross-controller safety invariant: the databag
    carries only the Juju secret ID, not the raw value.
    """
    state_in, ids = build_worker_state()

    state_out = ctx.run(ctx.on.relation_changed(ids.worker_relation), state_in)

    app_data = dict(state_out.get_relation(ids.worker_relation.id).local_app_data)
    for key, value in app_data.items():
        if "int-comms" in key.lower() and not key.endswith("-id"):
            raise AssertionError(
                f"Plaintext int-comms field {key!r} found in worker app relation data: {value!r}"
            )


def test_oauth_provider_data_configures_trino_and_registers_client(ctx):
    """OAuth credentials and issuer are read from the relation provider data."""
    client_secret = observer_secret({"secret": "shhh"})  # nosec B105
    oauth = oauth_relation(client_secret.id, scope="openid email")
    ingress = ingress_relation("https://trino.example/")
    state_in, _ = build_coordinator_state(
        extra_relations=(oauth, ingress),
        extra_secrets=(client_secret,),
    )

    state_out = ctx.run(ctx.on.relation_changed(oauth), state_in)

    environment = _services(state_out)["trino"]["environment"]
    assert environment["OAUTH_CLIENT_ID"] == "client-123"
    assert environment["OAUTH_CLIENT_SECRET"] == "shhh"  # nosec
    assert environment["OAUTH_ISSUER_URL"] == "https://idp.example"
    assert environment["OAUTH_AUTHORIZATION_ENDPOINT"] == "https://idp.example/oauth2/auth"
    assert environment["OAUTH_TOKEN_ENDPOINT"] == "https://idp.example/oauth2/token"
    assert environment["OAUTH_USERINFO_ENDPOINT"] == "https://idp.example/userinfo"
    assert environment["OAUTH_JWKS_ENDPOINT"] == "https://idp.example/.well-known/jwks.json"
    assert environment["OAUTH_SCOPES"] == "openid email"

    relation_data = state_out.get_relation(oauth.id).local_app_data
    assert relation_data["redirect_uri"] == "https://trino.example/oauth2/callback"
    assert relation_data["scope"] == "openid profile email"
    assert json.loads(relation_data["grant_types"]) == ["authorization_code"]

    config = workload_path(state_out, ctx, "/usr/lib/trino/etc/config.properties").read_text()
    assert "http-server.authentication.oauth2.issuer=https://idp.example" in config
    assert "http-server.authentication.oauth2.auth-url=https://idp.example/oauth2/auth" in config
    assert "http-server.authentication.oauth2.token-url=https://idp.example/oauth2/token" in config
    assert "http-server.authentication.oauth2.userinfo-url=https://idp.example/userinfo" in config
    assert (
        "http-server.authentication.oauth2.jwks-url=https://idp.example/.well-known/jwks.json"
    ) in config
    assert "http-server.authentication.oauth2.oidc.discovery=false" in config
    assert "http-server.authentication.oauth2.scopes=openid,email" in config
    assert "accounts.google.com" not in config


def test_oauth_without_ingress_url_waits(ctx):
    """An OAuth relation waits until ingress publishes the callback base URL."""
    state_in, _ = build_coordinator_state(extra_relations=(oauth_relation(),))

    state_out = ctx.run(ctx.on.config_changed(), state_in)

    assert state_out.unit_status == WaitingStatus("waiting for ingress URL for OAuth")


def test_oauth_with_http_ingress_blocks(ctx):
    """An OAuth callback must use HTTPS."""
    state_in, _ = build_coordinator_state(
        extra_relations=(oauth_relation(), ingress_relation("http://trino.example")),
    )

    state_out = ctx.run(ctx.on.config_changed(), state_in)

    assert state_out.unit_status == BlockedStatus("OAuth requires an HTTPS ingress URL")


def test_oauth_waits_for_provider_registration(ctx):
    """A related provider without client credentials leaves the charm waiting."""
    state_in, _ = build_coordinator_state(
        extra_relations=(oauth_relation(), ingress_relation("https://trino.example")),
    )

    state_out = ctx.run(ctx.on.config_changed(), state_in)

    assert state_out.unit_status == WaitingStatus("waiting for OAuth provider registration")


def test_oauth_relation_on_worker_blocks(ctx):
    """OAuth is supported only by coordinator-capable applications."""
    state_in, _ = build_worker_state(extra_relations=(oauth_relation(),))

    state_out = ctx.run(ctx.on.config_changed(), state_in)

    assert state_out.unit_status == BlockedStatus("oauth relation requires a coordinator")


def test_non_leader_does_not_publish_oauth_client_config(ctx):
    """Only the leader writes OAuth client registration data."""
    oauth = oauth_relation()
    state_in, _ = build_coordinator_state(
        leader=False,
        extra_relations=(oauth, ingress_relation("https://trino.example")),
    )

    state_out = ctx.run(ctx.on.relation_created(oauth), state_in)

    assert "redirect_uri" not in state_out.get_relation(oauth.id).local_app_data


def test_oauth_client_secret_rotation_reconfigures_trino(ctx):
    """A provider-owned client secret revision triggers reconciliation."""
    client_secret = observer_secret({"secret": "old-secret"})  # nosec B105
    client_secret = dataclasses.replace(
        client_secret,
        latest_content={"secret": "new-secret"},  # nosec B105
    )
    oauth = oauth_relation(client_secret.id)
    state_in, _ = build_coordinator_state(
        extra_relations=(oauth, ingress_relation("https://trino.example")),
        extra_secrets=(client_secret,),
    )

    state_out = ctx.run(ctx.on.secret_changed(client_secret), state_in)

    environment = _services(state_out)["trino"]["environment"]
    assert environment["OAUTH_CLIENT_SECRET"] == "new-secret"  # nosec B105


def test_oauth_relation_removal_restores_password_only(ctx, tmp_path):
    """Breaking OAuth removes its environment and renders password-only auth."""
    client_secret = observer_secret({"secret": "shhh"})  # nosec B105
    oauth = oauth_relation(client_secret.id)
    container = trino_container(
        mounts={"home": Mount(location="/usr/lib/trino/etc", source=tmp_path)}
    )
    state_in, _ = build_coordinator_state(
        container=container,
        extra_relations=(oauth, ingress_relation("https://trino.example")),
        extra_secrets=(client_secret,),
    )
    configured = carry_forward(ctx.run(ctx.on.config_changed(), state_in))
    configured_oauth = configured.get_relation(oauth.id)

    state_out = ctx.run(ctx.on.relation_broken(configured_oauth), configured)

    environment = _services(state_out)["trino"]["environment"]
    assert environment["OAUTH_CLIENT_ID"] is None
    config = (tmp_path / "config.properties").read_text()
    assert "http-server.authentication.type=PASSWORD" in config
    assert "http-server.authentication.type=oauth2,PASSWORD" not in config


def test_malformed_user_secret_blocks(ctx):
    """A user secret whose `users` field is not a mapping blocks the charm."""
    bad_user = observer_secret({"users": ""})
    state_in, _ = build_coordinator_state(
        config={"user-secret-id": bad_user.id}, extra_secrets=(bad_user,)
    )

    state_out = ctx.run(ctx.on.config_changed(), state_in)

    assert isinstance(state_out.unit_status, BlockedStatus)
    assert "must be a mapping" in state_out.unit_status.message


def test_coordinator_publishes_pg_secret_id(ctx):
    """Coordinator publishes a PG secret id, never the plaintext passwords."""
    state_in, ids = build_coordinator_state()

    with mock.patch.object(
        PostgresqlCatalogRelationHandler,
        "get_postgresql_env_vars",
        return_value={"PG_PASS_TESTDB": "super-secret-pw"},  # nosec
    ):
        state_out = ctx.run(ctx.on.config_changed(), state_in)

    relation_data = state_out.get_relation(ids.coordinator_relation.id).local_app_data
    assert relation_data["postgresql-secrets-id"].startswith("secret:")
    for value in relation_data.values():
        assert "super-secret-pw" not in value


def test_coordinator_no_pg_secret_id_without_catalogs(ctx):
    """Coordinator omits the PG secret id when there are no PostgreSQL catalogs."""
    state_in, ids = build_coordinator_state()

    state_out = ctx.run(ctx.on.config_changed(), state_in)

    relation_data = state_out.get_relation(ids.coordinator_relation.id).local_app_data
    assert "postgresql-secrets-id" not in relation_data


def test_worker_resolves_pg_secret_from_coordinator(ctx):
    """Worker resolves PG password env vars from the coordinator's granted secret."""
    state_in, ids = build_worker_state(
        postgresql_secrets={"PG_PASS_TESTDB": "super-secret-pw"}  # nosec
    )

    with ctx(ctx.on.relation_changed(ids.worker_relation), state_in) as mgr:
        mgr.run()
        resolved = mgr.charm.trino_worker.get_postgresql_secrets_from_coordinator()

    assert resolved == {"PG_PASS_TESTDB": "super-secret-pw"}  # nosec


def test_worker_pg_secret_empty_without_id(ctx):
    """Worker returns an empty map when the coordinator publishes no PG secret."""
    state_in, ids = build_worker_state()

    with ctx(ctx.on.relation_changed(ids.worker_relation), state_in) as mgr:
        mgr.run()
        resolved = mgr.charm.trino_worker.get_postgresql_secrets_from_coordinator()

    assert resolved == {}


# ---------------------------------------------------------------------------
# Proxy configuration sourced from the Juju model.
#
# These tests exercise `_build_base_environment`, the rendered `config.properties`
# / `jvm.config` content, and unit status. Pure-logic cases for parsing and
# deriving proxy settings live in tests/unit/test_config.py.
# ---------------------------------------------------------------------------

CONFIG_PROPERTIES_PATH = "/usr/lib/trino/etc/config.properties"
JVM_CONFIG_PATH = "/usr/lib/trino/etc/jvm.config"

ZZUSERZZ = "ZZUSERZZ"  # nosec B105
ZZPASSZZ = "ZZPASSZZ"  # nosec B105


def _oauth_ready_state(**kwargs):
    """Build a coordinator state with a fully registered OAuth relation.

    Args:
        kwargs: forwarded to `build_coordinator_state`.

    Returns:
        A `(State, ids)` tuple as returned by `build_coordinator_state`, with
        an OAuth relation and HTTPS ingress already wired so the OAuth branch
        of `config.jinja` is taken.
    """
    oauth_secret = observer_secret({"secret": "test-client-secret"})  # nosec B105
    extra_relations = (
        oauth_relation(oauth_secret.id),
        ingress_relation("https://trino.example"),
        *kwargs.pop("extra_relations", ()),
    )
    extra_secrets = (oauth_secret, *kwargs.pop("extra_secrets", ()))
    return build_coordinator_state(
        extra_relations=extra_relations, extra_secrets=extra_secrets, **kwargs
    )


def test_jvm_proxy_flags_identical_across_roles(ctx, monkeypatch):
    """Identical JVM proxy flags are derived for coordinator, worker and all."""
    monkeypatch.setenv("JUJU_CHARM_HTTPS_PROXY", "http://p:3128")

    coordinator_state, _ = build_coordinator_state(config={"charm-function": "coordinator"})
    all_state, _ = build_coordinator_state(config={"charm-function": "all"})
    worker_state, _ = build_worker_state()

    def jvm_options(state_in):
        state_out = ctx.run(ctx.on.config_changed(), state_in)
        return _services(state_out)["trino"]["environment"]["JVM_OPTIONS"]

    coordinator_flags = jvm_options(coordinator_state)
    all_flags = jvm_options(all_state)
    worker_flags = jvm_options(worker_state)

    assert "-Dhttps.proxyHost=p" in coordinator_flags
    assert "-Dhttps.proxyPort=3128" in coordinator_flags
    assert coordinator_flags == all_flags == worker_flags


def test_model_derived_flags_reach_jvm_options_with_defaults(ctx, monkeypatch):
    """Model-derived flags appear in JVM_OPTIONS alongside DEFAULT_JVM_OPTIONS."""
    monkeypatch.setenv("JUJU_CHARM_HTTPS_PROXY", "http://p:3128")
    state_in, _ = build_coordinator_state()

    state_out = ctx.run(ctx.on.config_changed(), state_in)

    jvm_options = _services(state_out)["trino"]["environment"]["JVM_OPTIONS"]
    assert DEFAULT_JVM_STRING in jvm_options
    assert "-Dhttps.proxyHost=p" in jvm_options
    assert "-Dhttps.proxyPort=3128" in jvm_options


def test_dropped_cidr_warning_logged_on_every_reconcile(ctx, monkeypatch, caplog):
    """The dropped-CIDR warning is not suppressed on a second reconcile."""
    monkeypatch.setenv("JUJU_CHARM_NO_PROXY", "localhost,10.0.0.0/8")
    state_in, _ = build_coordinator_state()

    with caplog.at_level(logging.WARNING):
        mid = ctx.run(ctx.on.config_changed(), state_in)
        caplog.clear()
        ctx.run(ctx.on.config_changed(), mid)

    warnings = [
        r for r in caplog.records if r.levelno == logging.WARNING and "10.0.0.0/8" in r.message
    ]
    assert len(warnings) == 1


# --- Batch 4: precedence with additional-jvm-options -----------------------


def test_override_changes_jvm_only_oauth_property_unchanged(ctx, monkeypatch):
    """An override changes jvm.config but leaves the OAuth property untouched."""
    monkeypatch.setenv("JUJU_CHARM_HTTPS_PROXY", "http://p1:3128")
    state_in, _ = _oauth_ready_state(
        config={"additional-jvm-options": "-Dhttps.proxyHost=p2 -Dhttps.proxyPort=8080"}
    )

    state_out = ctx.run(ctx.on.config_changed(), state_in)

    jvm_config = workload_path(state_out, ctx, JVM_CONFIG_PATH).read_text()
    assert "-Dhttps.proxyHost=p2" in jvm_config
    assert "-Dhttps.proxyPort=8080" in jvm_config
    assert "-Dhttps.proxyHost=p1" not in jvm_config

    config = workload_path(state_out, ctx, CONFIG_PROPERTIES_PATH).read_text()
    assert "oauth2-jwk.http-client.http-proxy=p1:3128" in config


def test_override_host_without_port_blocks(ctx, monkeypatch):
    """An override supplying proxyHost without proxyPort blocks."""
    monkeypatch.setenv("JUJU_CHARM_HTTPS_PROXY", "http://p1:3128")
    state_in, _ = build_coordinator_state(
        config={"additional-jvm-options": "-Dhttps.proxyHost=p2"}
    )

    state_out = ctx.run(ctx.on.config_changed(), state_in)

    assert isinstance(state_out.unit_status, BlockedStatus)
    assert "additional-jvm-options" in state_out.unit_status.message


def test_override_port_without_host_blocks(ctx, monkeypatch):
    """An override supplying proxyPort without proxyHost blocks."""
    monkeypatch.setenv("JUJU_CHARM_HTTPS_PROXY", "http://p1:3128")
    state_in, _ = build_coordinator_state(
        config={"additional-jvm-options": "-Dhttps.proxyPort=8080"}
    )

    state_out = ctx.run(ctx.on.config_changed(), state_in)

    assert isinstance(state_out.unit_status, BlockedStatus)
    assert "additional-jvm-options" in state_out.unit_status.message


def test_override_without_model_proxy_passes_through(ctx):
    """No model proxy, override supplies both host and port: no error."""
    state_in, _ = build_coordinator_state(
        config={"additional-jvm-options": "-Dhttps.proxyHost=p -Dhttps.proxyPort=8080"}
    )

    state_out = ctx.run(ctx.on.config_changed(), state_in)

    assert not isinstance(state_out.unit_status, BlockedStatus)
    jvm_options = _services(state_out)["trino"]["environment"]["JVM_OPTIONS"]
    assert "-Dhttps.proxyHost=p" in jvm_options
    assert "-Dhttps.proxyPort=8080" in jvm_options


def test_override_replaces_only_matching_family(ctx, monkeypatch):
    """Overriding the http family leaves the https family model-derived."""
    monkeypatch.setenv("JUJU_CHARM_HTTP_PROXY", "http://phttp:80")
    monkeypatch.setenv("JUJU_CHARM_HTTPS_PROXY", "https://phttps:443")
    state_in, _ = build_coordinator_state(
        config={"additional-jvm-options": "-Dhttp.proxyHost=other -Dhttp.proxyPort=8080"}
    )

    state_out = ctx.run(ctx.on.config_changed(), state_in)

    jvm_options = _services(state_out)["trino"]["environment"]["JVM_OPTIONS"]
    assert "-Dhttp.proxyHost=other" in jvm_options
    assert "-Dhttp.proxyPort=8080" in jvm_options
    assert "-Dhttps.proxyHost=phttps" in jvm_options
    assert "-Dhttps.proxyPort=443" in jvm_options


def test_override_nonproxyhosts_wins(ctx, monkeypatch):
    """An override of nonProxyHosts wins over the model-derived value."""
    monkeypatch.setenv("JUJU_CHARM_NO_PROXY", "localhost,127.0.0.1")
    state_in, _ = build_coordinator_state(
        config={"additional-jvm-options": "-Dhttp.nonProxyHosts=override.example"}
    )

    state_out = ctx.run(ctx.on.config_changed(), state_in)

    jvm_options = _services(state_out)["trino"]["environment"]["JVM_OPTIONS"]
    assert "-Dhttp.nonProxyHosts=override.example" in jvm_options
    assert "localhost" not in jvm_options


def test_override_unrelated_option_coexists(ctx, monkeypatch):
    """An unrelated override option coexists with the derived proxy flags."""
    monkeypatch.setenv("JUJU_CHARM_HTTPS_PROXY", "http://p:3128")
    state_in, _ = build_coordinator_state(config={"additional-jvm-options": "-Xmx4G"})

    state_out = ctx.run(ctx.on.config_changed(), state_in)

    jvm_options = _services(state_out)["trino"]["environment"]["JVM_OPTIONS"]
    assert "-Xmx4G" in jvm_options
    assert "-Dhttps.proxyHost=p" in jvm_options


def test_override_credentials_passed_through_unchanged(ctx):
    """Credential flags in additional-jvm-options pass through unrejected.

    `additional-jvm-options` is an operator-controlled escape hatch and is
    never inspected for credentials.
    """
    state_in, _ = build_coordinator_state(
        config={
            "additional-jvm-options": (
                f"-Dhttp.proxyUser={ZZUSERZZ} -Dhttp.proxyPassword={ZZPASSZZ} "  # nosec
                "-Dhttp.proxyHost=p -Dhttp.proxyPort=80"
            )
        }
    )

    state_out = ctx.run(ctx.on.config_changed(), state_in)

    assert not isinstance(state_out.unit_status, BlockedStatus)
    jvm_options = _services(state_out)["trino"]["environment"]["JVM_OPTIONS"]
    assert ZZUSERZZ in jvm_options
    assert ZZPASSZZ in jvm_options


def test_model_proxy_reaches_jvm_options_when_override_unset(ctx, monkeypatch):
    """Model-derived flags still reach JVM_OPTIONS when the override is unset.

    Load-bearing: the previous short-circuit skipped the merge entirely
    when additional-jvm-options was empty, which would drop every
    model-derived flag in the default (no-override) case.
    """
    monkeypatch.setenv("JUJU_CHARM_HTTPS_PROXY", "http://p:3128")
    state_in, _ = build_coordinator_state()

    state_out = ctx.run(ctx.on.config_changed(), state_in)

    jvm_options = _services(state_out)["trino"]["environment"]["JVM_OPTIONS"]
    assert "-Dhttps.proxyHost=p" in jvm_options
    assert "-Dhttps.proxyPort=3128" in jvm_options


# --- Batch 5: OAuth proxy property rendering --------------------------------


def test_http_proxy_no_secure_line(ctx, monkeypatch):
    """A plaintext model proxy renders the property without `.secure`."""
    monkeypatch.setenv("JUJU_CHARM_HTTPS_PROXY", "http://p:3128")
    state_in, _ = _oauth_ready_state()

    state_out = ctx.run(ctx.on.config_changed(), state_in)

    config = workload_path(state_out, ctx, CONFIG_PROPERTIES_PATH).read_text()
    assert "oauth2-jwk.http-client.http-proxy=p:3128" in config
    assert "oauth2-jwk.http-client.http-proxy.secure" not in config


def test_https_proxy_emits_secure_true(ctx, monkeypatch):
    """An https:// model proxy renders the property and `.secure=true`."""
    monkeypatch.setenv("JUJU_CHARM_HTTPS_PROXY", "https://p:8443")
    state_in, _ = _oauth_ready_state()

    state_out = ctx.run(ctx.on.config_changed(), state_in)

    config = workload_path(state_out, ctx, CONFIG_PROPERTIES_PATH).read_text()
    assert "oauth2-jwk.http-client.http-proxy=p:8443" in config
    assert "oauth2-jwk.http-client.http-proxy.secure=true" in config


def test_falls_back_to_http_proxy_setting(ctx, monkeypatch):
    """Only `juju-http-proxy` set: the property is derived from it."""
    monkeypatch.setenv("JUJU_CHARM_HTTP_PROXY", "http://p:80")
    state_in, _ = _oauth_ready_state()

    state_out = ctx.run(ctx.on.config_changed(), state_in)

    config = workload_path(state_out, ctx, CONFIG_PROPERTIES_PATH).read_text()
    assert "oauth2-jwk.http-client.http-proxy=p:80" in config


def test_https_proxy_preferred_over_http(ctx, monkeypatch):
    """Both model proxies set: `juju-https-proxy` is preferred."""
    monkeypatch.setenv("JUJU_CHARM_HTTP_PROXY", "http://plain-proxy:80")
    monkeypatch.setenv("JUJU_CHARM_HTTPS_PROXY", "https://secure-proxy:443")
    state_in, _ = _oauth_ready_state()

    state_out = ctx.run(ctx.on.config_changed(), state_in)

    config = workload_path(state_out, ctx, CONFIG_PROPERTIES_PATH).read_text()
    assert "oauth2-jwk.http-client.http-proxy=secure-proxy:443" in config
    assert "plain-proxy" not in config


def test_override_only_no_oauth_property(ctx):
    """Proxy supplied only via additional-jvm-options: jvm.config only."""
    state_in, _ = _oauth_ready_state(
        config={"additional-jvm-options": "-Dhttps.proxyHost=p -Dhttps.proxyPort=8080"}
    )

    state_out = ctx.run(ctx.on.config_changed(), state_in)

    jvm_config = workload_path(state_out, ctx, JVM_CONFIG_PATH).read_text()
    assert "-Dhttps.proxyHost=p" in jvm_config

    config = workload_path(state_out, ctx, CONFIG_PROPERTIES_PATH).read_text()
    assert "oauth2-jwk.http-client.http-proxy" not in config


def test_override_keeps_model_derived_oauth_property(ctx, monkeypatch):
    """Model proxy set and overridden: the property keeps the model value."""
    monkeypatch.setenv("JUJU_CHARM_HTTPS_PROXY", "http://p1:3128")
    state_in, _ = _oauth_ready_state(
        config={"additional-jvm-options": "-Dhttps.proxyHost=p2 -Dhttps.proxyPort=8080"}
    )

    state_out = ctx.run(ctx.on.config_changed(), state_in)

    config = workload_path(state_out, ctx, CONFIG_PROPERTIES_PATH).read_text()
    assert "oauth2-jwk.http-client.http-proxy=p1:3128" in config


def test_no_proxy_configured_no_property(ctx):
    """No proxy configured anywhere: no OAuth proxy property lines."""
    state_in, _ = _oauth_ready_state()

    state_out = ctx.run(ctx.on.config_changed(), state_in)

    config = workload_path(state_out, ctx, CONFIG_PROPERTIES_PATH).read_text()
    assert "oauth2-jwk.http-client.http-proxy" not in config


def test_oauth_disabled_no_proxy_properties(ctx, monkeypatch):
    """Proxy configured but OAuth disabled: no proxy properties rendered."""
    monkeypatch.setenv("JUJU_CHARM_HTTPS_PROXY", "http://p:3128")
    state_in, _ = build_coordinator_state()

    state_out = ctx.run(ctx.on.config_changed(), state_in)

    config = workload_path(state_out, ctx, CONFIG_PROPERTIES_PATH).read_text()
    assert "oauth2-jwk.http-client.http-proxy" not in config


def test_worker_no_oauth_property_but_jvm_flags_present(ctx, monkeypatch):
    """A worker never renders the OAuth property, but keeps its JVM flags."""
    monkeypatch.setenv("JUJU_CHARM_HTTPS_PROXY", "http://p:3128")
    state_in, _ = build_worker_state()

    state_out = ctx.run(ctx.on.config_changed(), state_in)

    config = workload_path(state_out, ctx, CONFIG_PROPERTIES_PATH).read_text()
    assert "oauth2-jwk.http-client.http-proxy" not in config

    jvm_options = _services(state_out)["trino"]["environment"]["JVM_OPTIONS"]
    assert "-Dhttps.proxyHost=p" in jvm_options


def test_secure_never_emitted_without_http_proxy(ctx):
    """`.secure` never appears unless `http-proxy` is also set."""
    state_in, _ = _oauth_ready_state()

    state_out = ctx.run(ctx.on.config_changed(), state_in)

    config = workload_path(state_out, ctx, CONFIG_PROPERTIES_PATH).read_text()
    assert "oauth2-jwk.http-client.http-proxy.secure" not in config


def test_override_and_model_diverge_across_both_artifacts(ctx, monkeypatch):
    """Jvm.config follows the override while config.properties follows model config.

    Asserted in a single test so a future regression that recouples the two
    derivations cannot pass by splitting the assertions across two tests.
    """
    monkeypatch.setenv("JUJU_CHARM_HTTPS_PROXY", "http://p1:3128")
    state_in, _ = _oauth_ready_state(
        config={"additional-jvm-options": "-Dhttps.proxyHost=p2 -Dhttps.proxyPort=8080"}
    )

    state_out = ctx.run(ctx.on.config_changed(), state_in)

    jvm_config = workload_path(state_out, ctx, JVM_CONFIG_PATH).read_text()
    config = workload_path(state_out, ctx, CONFIG_PROPERTIES_PATH).read_text()

    assert "-Dhttps.proxyHost=p2" in jvm_config
    assert "-Dhttps.proxyPort=8080" in jvm_config
    assert "oauth2-jwk.http-client.http-proxy=p1:3128" in config


def test_ipv6_bare_in_jvm_bracketed_in_oauth(ctx, monkeypatch):
    """An IPv6 model proxy is bare in jvm.config and bracketed in the OAuth property."""
    monkeypatch.setenv("JUJU_CHARM_HTTP_PROXY", "http://[::1]:3128")
    state_in, _ = _oauth_ready_state()

    state_out = ctx.run(ctx.on.config_changed(), state_in)

    jvm_config = workload_path(state_out, ctx, JVM_CONFIG_PATH).read_text()
    config = workload_path(state_out, ctx, CONFIG_PROPERTIES_PATH).read_text()

    assert "-Dhttp.proxyHost=::1" in jvm_config
    assert "oauth2-jwk.http-client.http-proxy=[::1]:3128" in config


# --- Batch 6: unit status and error handling --------------------------------


def test_credential_bearing_https_proxy_blocks_without_sentinels(ctx, monkeypatch, caplog):
    """A credential-bearing model proxy blocks; neither sentinel leaks in status or logs."""
    monkeypatch.setenv("JUJU_CHARM_HTTPS_PROXY", f"http://{ZZUSERZZ}:{ZZPASSZZ}@proxy:3128")
    state_in, _ = build_coordinator_state()

    with caplog.at_level(logging.DEBUG):
        state_out = ctx.run(ctx.on.config_changed(), state_in)

    assert isinstance(state_out.unit_status, BlockedStatus)
    assert "juju-https-proxy" in state_out.unit_status.message
    assert ZZUSERZZ not in state_out.unit_status.message
    assert ZZPASSZZ not in state_out.unit_status.message
    assert ZZUSERZZ not in caplog.text
    assert ZZPASSZZ not in caplog.text


def test_unparsable_http_proxy_blocks_naming_setting(ctx, monkeypatch):
    """An unparsable JUJU_CHARM_HTTP_PROXY blocks, naming `juju-http-proxy`."""
    monkeypatch.setenv("JUJU_CHARM_HTTP_PROXY", "http://proxy:notaport")
    state_in, _ = build_coordinator_state()

    state_out = ctx.run(ctx.on.config_changed(), state_in)

    assert isinstance(state_out.unit_status, BlockedStatus)
    assert "juju-http-proxy" in state_out.unit_status.message


def test_incomplete_override_blocks_naming_additional_jvm_options(ctx):
    """An incomplete host/port override blocks, naming additional-jvm-options."""
    state_in, _ = build_coordinator_state(config={"additional-jvm-options": "-Dhttp.proxyHost=p"})

    state_out = ctx.run(ctx.on.config_changed(), state_in)

    assert isinstance(state_out.unit_status, BlockedStatus)
    assert "additional-jvm-options" in state_out.unit_status.message


def test_reconcile_logs_and_returns_early_without_crash_or_replan(ctx, monkeypatch, caplog):
    """An R6 failure is logged; reconcile returns early with no replan."""
    monkeypatch.setenv("JUJU_CHARM_HTTP_PROXY", "socks5://proxy:1080")
    state_in, _ = build_coordinator_state()

    with caplog.at_level(logging.ERROR):
        state_out = ctx.run(ctx.on.config_changed(), state_in)

    assert state_out.get_container("trino").plan.to_dict() == {}
    assert any(r.levelno == logging.ERROR for r in caplog.records)


def test_running_unit_not_reconfigured_with_broken_proxy(ctx, monkeypatch):
    """A running unit keeps its last-good plan when the proxy config breaks."""
    state_in, _ = build_coordinator_state()
    good = carry_forward(ctx.run(ctx.on.config_changed(), state_in))
    good_plan = good.get_container("trino").plan.to_dict()

    monkeypatch.setenv("JUJU_CHARM_HTTP_PROXY", "socks5://proxy:1080")
    broken_out = ctx.run(ctx.on.config_changed(), good)

    assert broken_out.get_container("trino").plan.to_dict() == good_plan


def test_valid_proxy_configuration_reaches_active(ctx, monkeypatch):
    """A valid proxy configuration reports no proxy-related BlockedStatus."""
    monkeypatch.setenv("JUJU_CHARM_HTTPS_PROXY", "http://p:3128")
    state_in, _ = build_coordinator_state()

    state_out = ctx.run(ctx.on.config_changed(), state_in)

    assert state_out.unit_status == ActiveStatus("Status check: UP")


def test_blocked_unit_recovers_after_correction(ctx, monkeypatch):
    """Correcting a blocked proxy configuration returns the unit to active."""
    monkeypatch.setenv("JUJU_CHARM_HTTP_PROXY", "socks5://proxy:1080")
    state_in, _ = build_coordinator_state()
    blocked_out = ctx.run(ctx.on.config_changed(), state_in)
    assert isinstance(blocked_out.unit_status, BlockedStatus)

    monkeypatch.setenv("JUJU_CHARM_HTTP_PROXY", "http://proxy:1080")
    fixed_out = ctx.run(ctx.on.config_changed(), blocked_out)

    assert fixed_out.unit_status == ActiveStatus("Status check: UP")
    jvm_options = _services(fixed_out)["trino"]["environment"]["JVM_OPTIONS"]
    assert "-Dhttp.proxyHost=proxy" in jvm_options


def test_scheme_less_model_proxy_blocks(ctx, monkeypatch):
    """A scheme-less model proxy URL blocks, naming the offending setting."""
    monkeypatch.setenv("JUJU_CHARM_HTTPS_PROXY", "proxy.corp:3128")
    state_in, _ = build_coordinator_state()

    state_out = ctx.run(ctx.on.config_changed(), state_in)

    assert isinstance(state_out.unit_status, BlockedStatus)
    assert "juju-https-proxy" in state_out.unit_status.message


# --- Batch 7: credential leakage guards --------------------------------------


def test_credential_bearing_proxy_blocks_before_rendering(ctx, monkeypatch):
    """A credential-bearing model proxy blocks before config.properties renders."""
    container = trino_container()
    monkeypatch.setenv("JUJU_CHARM_HTTPS_PROXY", f"http://{ZZUSERZZ}:{ZZPASSZZ}@proxy:3128")
    state_in, _ = build_coordinator_state(container=container)

    state_out = ctx.run(ctx.on.config_changed(), state_in)

    assert isinstance(state_out.unit_status, BlockedStatus)
    fs_root = state_out.get_container("trino").get_filesystem(ctx)
    assert not (fs_root / CONFIG_PROPERTIES_PATH.lstrip("/")).exists()


def test_rendered_config_never_contains_proxy_credential_properties(ctx, monkeypatch):
    """Config.properties never contains http-proxy.user or .password properties."""
    monkeypatch.setenv("JUJU_CHARM_HTTPS_PROXY", "https://p:8443")
    state_in, _ = _oauth_ready_state()

    state_out = ctx.run(ctx.on.config_changed(), state_in)

    config = workload_path(state_out, ctx, CONFIG_PROPERTIES_PATH).read_text()
    assert "http-proxy.password" not in config
    assert "http-proxy.user" not in config


# --- Batch 8: operational behaviour -----------------------------------------


def test_proxy_change_triggers_restart_via_hash_change(ctx, monkeypatch):
    """A model proxy change alters the Pebble layer environment (restart trigger)."""
    state_in, _ = build_coordinator_state()
    before = carry_forward(ctx.run(ctx.on.config_changed(), state_in))
    before_env = _services(before)["trino"]["environment"]

    monkeypatch.setenv("JUJU_CHARM_HTTPS_PROXY", "http://p:3128")
    after = ctx.run(ctx.on.update_status(), before)
    after_env = _services(after)["trino"]["environment"]

    assert before_env["JVM_OPTIONS"] != after_env["JVM_OPTIONS"]


def test_unchanged_proxy_configuration_no_restart(ctx, monkeypatch):
    """An unchanged proxy configuration leaves the Pebble environment stable."""
    monkeypatch.setenv("JUJU_CHARM_HTTPS_PROXY", "http://p:3128")
    state_in, _ = build_coordinator_state()
    before = carry_forward(ctx.run(ctx.on.config_changed(), state_in))
    before_env = dict(_services(before)["trino"]["environment"])

    after = ctx.run(ctx.on.update_status(), before)
    after_env = dict(_services(after)["trino"]["environment"])

    assert before_env["JVM_OPTIONS"] == after_env["JVM_OPTIONS"]


def test_no_status_message_exposes_proxy_value(ctx, monkeypatch):
    """A configured proxy is never exposed via unit status or an action; logs only."""
    monkeypatch.setenv("JUJU_CHARM_HTTPS_PROXY", "http://p-secret-host:3128")
    state_in, _ = build_coordinator_state()

    state_out = ctx.run(ctx.on.config_changed(), state_in)

    assert "p-secret-host" not in str(state_out.unit_status)


def test_pebble_layer_env_has_derived_proxy_values(ctx, monkeypatch):
    """Derived proxy values are present in the Pebble layer environment."""
    monkeypatch.setenv("JUJU_CHARM_HTTPS_PROXY", "http://p:3128")
    state_in, _ = _oauth_ready_state()

    state_out = ctx.run(ctx.on.config_changed(), state_in)

    environment = _services(state_out)["trino"]["environment"]
    assert "-Dhttps.proxyHost=p" in environment["JVM_OPTIONS"]
    assert environment["OAUTH_HTTP_PROXY"] == "p:3128"
