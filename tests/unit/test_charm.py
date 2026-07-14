# Copyright 2023 Canonical Ltd.
# See LICENSE file for licensing details.
#
# Learn more about testing at: https://juju.is/docs/sdk/testing


"""Trino charm unit tests."""

# pylint:disable=protected-access,too-many-public-methods

import dataclasses
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
    create_single_catalog_config,
    observer_secret,
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
                    "WEB_PROXY": None,
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

    # Per-file content hashes drive Pebble restarts; assert they are present as
    # freshness triggers, then drop them to compare the stable environment.
    hash_keys = {key for key in environment if key.startswith("HASH_")}
    assert hash_keys
    for key in hash_keys:
        del environment[key]

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


def test_worker_resolves_int_comms_secret_from_coordinator(ctx):
    """Worker reads int-comms-secret from coordinator.

    Worker reads int-comms-secret-id from relation, stores the ID in state, and
    resolves the secret value at render time via _get_int_comms_secret_value.
    """
    state_in, ids = build_worker_state()

    with ctx(ctx.on.relation_changed(ids.worker_relation), state_in) as mgr:
        mgr.run()

        # The worker's peer state must carry the secret *ID* (not the plaintext value).
        secret_id = mgr.charm.state.int_comms_secret_id
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


def test_oidc_credentials_resolved_from_secret(ctx):
    """OAuth credentials are read from the oidc-secret-id Juju secret."""
    oidc = observer_secret(
        {"google-client-id": "client-123", "google-client-secret": "shhh"}  # nosec
    )
    state_in, _ = build_coordinator_state(
        config={"oidc-secret-id": oidc.id}, extra_secrets=(oidc,)
    )

    state_out = ctx.run(ctx.on.config_changed(), state_in)

    environment = _services(state_out)["trino"]["environment"]
    assert environment["OAUTH_CLIENT_ID"] == "client-123"
    assert environment["OAUTH_CLIENT_SECRET"] == "shhh"  # nosec


@pytest.mark.parametrize("option", ["google-client-id", "google-client-secret"])
def test_deprecated_oidc_plaintext_blocks(ctx, option):
    """Setting a deprecated plaintext OIDC option blocks the charm."""
    state_in, _ = build_coordinator_state(config={option: "some-value"})

    state_out = ctx.run(ctx.on.config_changed(), state_in)

    assert isinstance(state_out.unit_status, BlockedStatus)
    assert "deprecated" in state_out.unit_status.message


def test_oidc_secret_id_unresolvable_blocks(ctx):
    """An oidc-secret-id that cannot be resolved blocks the charm."""
    # The secret is referenced but never granted to (added to) the state.
    oidc = observer_secret(
        {"google-client-id": "client-123", "google-client-secret": "shhh"}  # nosec
    )
    state_in, _ = build_coordinator_state(config={"oidc-secret-id": oidc.id})

    state_out = ctx.run(ctx.on.config_changed(), state_in)

    assert isinstance(state_out.unit_status, BlockedStatus)
    assert "oidc-secret-id" in state_out.unit_status.message


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
