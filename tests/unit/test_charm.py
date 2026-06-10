# Copyright 2023 Canonical Ltd.
# See LICENSE file for licensing details.
#
# Learn more about testing at: https://juju.is/docs/sdk/testing


"""Trino charm unit tests."""

# pylint:disable=protected-access,too-many-public-methods

import logging
from unittest import TestCase, mock

from ops.model import (
    ActiveStatus,
    BlockedStatus,
    MaintenanceStatus,
    SecretNotFoundError,
    WaitingStatus,
)
from ops.pebble import CheckStatus
from ops.testing import Harness

from charm import TrinoK8SCharm
from tests.unit.helpers import (
    BIGQUERY_CATALOG_PATH,
    DEFAULT_JVM_STRING,
    POSTGRESQL_1_CATALOG_PATH,
    POSTGRESQL_1_DEVELOPER_CATALOG_PATH,
    POSTGRESQL_REPLICA_SECRET,
    POSTGRESQL_REPLICA_SECRET_WITH_PARAMS,
    SERVER_PORT,
    create_catalog_config,
    create_single_catalog_config,
    make_relation_event,
    simulate_lifecycle_coordinator,
    simulate_lifecycle_worker,
)

mock_incomplete_pebble_plan = {"services": {"trino": {"override": "replace"}}}

logger = logging.getLogger(__name__)


class TestCharm(TestCase):
    """Unit tests.

    Attrs:
        maxDiff: Specifies max difference shown by failed tests.
    """

    maxDiff = None

    @mock.patch("charm.KubernetesStatefulsetPatch")
    def setUp(self, _):
        """Set up for the unit tests."""
        self.harness = Harness(TrinoK8SCharm)
        self.addCleanup(self.harness.cleanup)
        self.harness.set_can_connect("trino", True)
        self.harness.set_leader(True)
        self.harness.set_model_name("trino-model")
        self.harness.add_network("10.0.0.10", endpoint="peer")
        self.harness.begin()

    def test_initial_plan(self):
        """The initial pebble plan is empty."""
        harness = self.harness
        initial_plan = harness.get_container_pebble_plan("trino").to_dict()
        self.assertEqual(initial_plan, {})

    def test_waiting_on_peer_relation_not_ready(self):
        """The charm is blocked without a peer relation."""
        harness = self.harness

        # Simulate pebble readiness.
        container = harness.model.unit.get_container("trino")
        harness.handle_exec("trino", ["htpasswd"], result=0)
        harness.charm.on.trino_pebble_ready.emit(container)

        # No plans are set yet.
        got_plan = harness.get_container_pebble_plan("trino").to_dict()
        self.assertEqual(got_plan, {})

        # The BlockStatus is set with a message.
        self.assertEqual(
            harness.model.unit.status,
            BlockedStatus("peer relation not ready"),
        )

    def test_ready(self):
        """The pebble plan is correctly generated when the charm is ready."""
        harness = self.harness
        (
            _,
            postgresql_secret_id,
            mysql_secret_id,
            redshift_secret_id,
            bigquery_secret_id,
            gsheets_secret_id,
        ) = simulate_lifecycle_coordinator(harness)
        catalog_config = create_catalog_config(
            postgresql_secret_id,
            mysql_secret_id,
            redshift_secret_id,
            bigquery_secret_id,
            gsheets_secret_id,
        )

        # Asserts status is active
        self.assertEqual(
            harness.model.unit.status,
            MaintenanceStatus("replanning application"),
        )

        # The plan is generated after pebble is ready.
        want_plan = {
            "services": {
                "trino": {
                    "override": "replace",
                    "summary": "trino server",
                    "command": "./entrypoint.sh",
                    "startup": "enabled",
                    "on-check-failure": {"up": "ignore"},
                    "environment": {
                        "CATALOG_CONFIG": catalog_config,
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
        got_plan = harness.get_container_pebble_plan("trino").to_dict()
        environment = got_plan["services"]["trino"]["environment"]
        environment["JAVA_TRUSTSTORE_PWD"] = "truststore_pwd"  # nosec
        environment["INT_COMMS_SECRET"] = "int_comms_secret"  # nosec
        environment["USER_SECRET_ID"] = "secret:secret-id"  # nosec

        self.assertEqual(got_plan["services"], want_plan["services"])

        # The service was started.
        service = harness.model.unit.get_container("trino").get_service("trino")
        self.assertTrue(service.is_running())

        # The ActiveStatus is set with no message.
        self.assertEqual(
            harness.model.unit.status,
            MaintenanceStatus("replanning application"),
        )

    def test_ingress(self):
        """Test ingress relation.

        The charm relates correctly to the nginx ingress charm
        and can be configured.
        """
        harness = self.harness

        simulate_lifecycle_coordinator(harness)

        nginx_route_relation_id = harness.add_relation("nginx-route", "ingress")
        harness.charm._require_nginx_route()

        assert harness.get_relation_data(nginx_route_relation_id, harness.charm.app) == {
            "service-namespace": harness.charm.model.name,
            "service-hostname": harness.charm.app.name,
            "service-name": harness.charm.app.name,
            "service-port": SERVER_PORT,
            "backend-protocol": "HTTP",
            "tls-secret-name": "trino-tls",
        }

    def test_invalid_config_value(self):
        """The charm blocks if an invalid config value is provided."""
        harness = self.harness
        simulate_lifecycle_coordinator(harness)

        # Update the config with an invalid value.
        self.harness.update_config({"log-level": "all-logs"})

        # The change is not applied to the plan.
        want_log_level = "info"
        got_log_level = harness.get_container_pebble_plan("trino").to_dict()["services"]["trino"][
            "environment"
        ]["LOG_LEVEL"]
        self.assertEqual(got_log_level, want_log_level)

        # The BlockStatus is set with a message.
        self.assertEqual(
            harness.model.unit.status,
            BlockedStatus("config: invalid log level 'all-logs'"),
        )

    def test_incorrect_relation(self):
        """The charm blocks if the coordinator relation is not added.."""
        harness = self.harness
        simulate_lifecycle_coordinator(harness)

        self.harness.update_config({"charm-function": "worker"})

        # The BlockStatus is set with a message.
        self.assertEqual(
            harness.model.unit.status,
            BlockedStatus("Incorrect trino relation configuration."),
        )

    def test_catalog_invalid_config(self):
        """The charm blocks when catalog-config is missing required top-level keys."""
        harness = self.harness
        simulate_lifecycle_coordinator(harness)

        self.harness.update_config({"catalog-config": "catalog: incorrect"})

        self.assertIsInstance(harness.model.unit.status, BlockedStatus)
        self.assertIn("catalog-config", harness.model.unit.status.message)

    def test_postgresql_catalog_config_bad_prefix(self):
        """The charm blocks when a postgresql-catalog-config entry has an invalid database_prefix."""  # noqa: E501
        harness = self.harness
        simulate_lifecycle_coordinator(harness)

        bad_config = "pg-app:\n  database_prefix: mydb\n  ro_catalog_name: mycat\n"
        self.harness.update_config({"postgresql-catalog-config": bad_config})

        self.assertIsInstance(harness.model.unit.status, BlockedStatus)
        self.assertIn("database_prefix", harness.model.unit.status.message)

    def test_postgresql_catalog_config_no_catalog_name(self):
        """The charm blocks when a postgresql-catalog-config entry has no catalog name."""
        harness = self.harness
        simulate_lifecycle_coordinator(harness)

        bad_config = "pg-app:\n  database_prefix: mydb*\n"
        self.harness.update_config({"postgresql-catalog-config": bad_config})

        self.assertIsInstance(harness.model.unit.status, BlockedStatus)
        self.assertIn("ro_catalog_name", harness.model.unit.status.message)

    def test_postgresql_catalog_config_duplicate_catalog_names(self):
        """The charm blocks when two postgresql-catalog-config entries share a catalog name."""
        harness = self.harness
        simulate_lifecycle_coordinator(harness)

        duplicate_config = (
            "pg-app-a:\n  database_prefix: db_a*\n  ro_catalog_name: shared_cat\n"
            "pg-app-b:\n  database_prefix: db_b*\n  ro_catalog_name: shared_cat\n"
        )
        self.harness.update_config({"postgresql-catalog-config": duplicate_config})

        self.assertIsInstance(harness.model.unit.status, BlockedStatus)
        self.assertIn("Duplicate", harness.model.unit.status.message)
        self.assertIn("shared_cat", harness.model.unit.status.message)

    def test_postgresql_catalog_config_clashes_with_static(self):
        """The charm blocks when a postgresql-catalog-config name clashes with catalog-config."""
        harness = self.harness
        simulate_lifecycle_coordinator(harness)

        import yaml

        static_config = yaml.dump(
            {
                "catalogs": {"my_static_cat": {"backend": "pg"}},
                "backends": {"pg": {"connector": "postgresql"}},
            }
        )
        pg_config = "pg-app:\n  database_prefix: db*\n  ro_catalog_name: my_static_cat\n"
        self.harness.update_config(
            {"catalog-config": static_config, "postgresql-catalog-config": pg_config}
        )

        self.assertIsInstance(harness.model.unit.status, BlockedStatus)
        self.assertIn("clashes with catalog-config", harness.model.unit.status.message)

    def test_session_property_manager_invalid_config(self):
        """The charm blocks when the session property manager JSON is invalid."""
        harness = self.harness
        simulate_lifecycle_coordinator(harness)

        self.harness.update_config({"session-property-manager-config": '{"group":"broken"'})

        self.assertIn(
            "Expecting ',' delimiter",
            harness.model.unit.status.message,
        )
        self.assertIsInstance(harness.model.unit.status, BlockedStatus)

    def test_update_status_up(self):
        """The charm updates the unit status to active based on UP status."""
        harness = self.harness

        simulate_lifecycle_coordinator(harness)

        container = harness.model.unit.get_container("trino")
        container.get_check = mock.Mock(status="up")
        container.get_check.return_value.status = CheckStatus.UP
        harness.charm.on.update_status.emit()

        self.assertEqual(harness.model.unit.status, ActiveStatus("Status check: UP"))

    def test_update_status_down(self):
        """The charm updates the unit status to maintenance based on DOWN status."""
        harness = self.harness

        simulate_lifecycle_coordinator(harness)

        container = harness.model.unit.get_container("trino")
        container.get_check = mock.Mock(status="up")
        container.get_check.return_value.status = CheckStatus.DOWN
        harness.charm.on.update_status.emit()

        self.assertEqual(harness.model.unit.status, MaintenanceStatus("Status check: DOWN"))

    def test_incomplete_pebble_plan(self):
        """The charm re-applies the pebble plan if incomplete."""
        harness = self.harness
        simulate_lifecycle_coordinator(harness)

        container = harness.model.unit.get_container("trino")
        container.add_layer("trino", mock_incomplete_pebble_plan, combine=True)
        harness.charm.on.update_status.emit()

        self.assertEqual(
            harness.model.unit.status,
            MaintenanceStatus("replanning application"),
        )
        plan = harness.get_container_pebble_plan("trino").to_dict()
        assert plan != mock_incomplete_pebble_plan

    def test_trino_coordinator_relation(self):
        """Test trino relation.

        The coordinator and worker Trino charms relate correctly.
        """
        harness = self.harness

        (
            rel_id,
            postgresql_secret_id,
            mysql_secret_id,
            redshift_secret_id,
            bigquery_secret_id,
            gsheets_secret_id,
        ) = simulate_lifecycle_coordinator(harness)

        catalog_config = create_catalog_config(
            postgresql_secret_id,
            mysql_secret_id,
            redshift_secret_id,
            bigquery_secret_id,
            gsheets_secret_id,
        )
        relation_data = harness.get_relation_data(rel_id, harness.charm.app)

        assert (
            relation_data["discovery-uri"] == "http://trino-k8s.trino-model.svc.cluster.local:8080"
        )
        assert relation_data["catalogs"] == catalog_config

    def test_trino_coordinator_relation_discovery_uri_override(self):
        """When discovery-uri config is set, the override is published to workers.

        Workers in cross-cluster or multi-network topologies need the coordinator
        to advertise a reachable address rather than the cluster-local default.
        """
        harness = self.harness
        harness.update_config({"discovery-uri": "http://trino.example.com:8080"})

        (rel_id, *_) = simulate_lifecycle_coordinator(harness)

        relation_data = harness.get_relation_data(rel_id, harness.charm.app)
        assert relation_data["discovery-uri"] == "http://trino.example.com:8080"

        # The override is also reflected in the coordinator's own Pebble environment.
        got_plan = harness.get_container_pebble_plan("trino").to_dict()
        environment = got_plan["services"]["trino"]["environment"]
        assert environment["DISCOVERY_URI"] == "http://trino.example.com:8080"

    def test_trino_coordinator_relation_broken(self):
        """Test trino relation.

        The coordinator and worker Trino charms relate correctly.
        """
        harness = self.harness

        rel_id = simulate_lifecycle_coordinator(harness)

        data = {"trino-coordinator": {}}
        event = make_relation_event("trino-coordinator", rel_id, data)
        with self.assertRaises(SecretNotFoundError):
            harness.charm.trino_coordinator._on_relation_broken(event)
            harness.model.get_secret(label="catalog-config")

    def test_trino_worker_relation_created(self):
        """Test trino relation creation.

        The coordinator and worker Trino charms relate correctly.
        """
        harness = self.harness
        container, _, _, _, _, _, _ = simulate_lifecycle_worker(harness)

        self.assertTrue(container.exists(BIGQUERY_CATALOG_PATH))
        self.assertTrue(container.exists(POSTGRESQL_1_CATALOG_PATH))

    def test_trino_worker_relation_broken(self):
        """Test trino relation broken.

        The coordinator and worker Trino charms relation is broken.
        """
        harness = self.harness
        container, event, _, _, _, _, _ = simulate_lifecycle_worker(harness)

        harness.charm.trino_worker._on_relation_broken(event)
        self.assertFalse(container.exists(POSTGRESQL_1_CATALOG_PATH))

    def test_trino_single_node_deployment(self):
        """Test pebble plan is created with single node deployment."""
        harness = self.harness
        harness.add_relation("peer", "trino")

        harness.handle_exec("trino", ["/bin/sh"], result="/usr/lib/jvm/java-25-openjdk-amd64/")
        harness.handle_exec("trino", ["keytool"], result=0)
        container = harness.model.unit.get_container("trino")
        harness.handle_exec("trino", ["htpasswd"], result=0)
        harness.charm.on.trino_pebble_ready.emit(container)
        harness.update_config({"charm-function": "all"})

        # There is a valid pebble plan.
        got_plan = harness.get_container_pebble_plan("trino").to_dict()
        assert got_plan["services"]["trino"]["environment"]["CHARM_FUNCTION"] == "all"

        # The MaintenanceStatus is set.
        self.assertEqual(
            harness.model.unit.status,
            MaintenanceStatus("replanning application"),
        )

    def test_resource_management_config(self):
        """Test resource management configuration variables.

        The charm includes resource management variables in the environment
        with the correct values when configured.
        """
        harness = self.harness
        simulate_lifecycle_coordinator(harness)

        # Update config with resource management values
        harness.update_config(
            {
                "query-max-cpu-time": "1h",
                "query-max-memory-per-node": "2GB",
                "query-max-memory": "10GB",
                "query-max-total-memory": "15GB",
                "memory-heap-headroom-per-node": "1GB",
            }
        )

        # Check that variables are present in environment with correct values
        got_plan = harness.get_container_pebble_plan("trino").to_dict()
        environment = got_plan["services"]["trino"]["environment"]

        self.assertEqual(environment["QUERY_MAX_CPU_TIME"], "1h")
        self.assertEqual(environment["QUERY_MAX_MEMORY_PER_NODE"], "2GB")
        self.assertEqual(environment["QUERY_MAX_MEMORY"], "10GB")
        self.assertEqual(environment["QUERY_MAX_TOTAL_MEMORY"], "15GB")
        self.assertEqual(environment["MEMORY_HEAP_HEADROOM_PER_NODE"], "1GB")

    def test_session_property_manager_files_created(self):
        """The charm writes the session property manager files when configured."""
        harness = self.harness
        simulate_lifecycle_coordinator(harness)

        session_property_config = (
            '[{"group":"global.*","sessionProperties":{"query_max_execution_time":"8h"}}]'
        )
        harness.update_config({"session-property-manager-config": session_property_config})

        container = harness.model.unit.get_container("trino")
        properties_path = "/usr/lib/trino/etc/session-property-config.properties"
        json_path = "/usr/lib/trino/etc/session-property-config.json"

        self.assertTrue(container.exists(properties_path))
        self.assertTrue(container.exists(json_path))
        self.assertEqual(container.pull(json_path).read(), session_property_config)

    def test_session_property_manager_files_removed(self):
        """The charm removes the session property manager files when unset."""
        harness = self.harness
        simulate_lifecycle_coordinator(harness)

        harness.update_config({"session-property-manager-config": '[{"user":"admin"}]'})
        harness.update_config({"session-property-manager-config": ""})

        container = harness.model.unit.get_container("trino")
        properties_path = "/usr/lib/trino/etc/session-property-config.properties"
        json_path = "/usr/lib/trino/etc/session-property-config.json"

        self.assertFalse(container.exists(properties_path))
        self.assertFalse(container.exists(json_path))

    def test_resource_group_manager_files_created(self):
        """The charm writes the resource group manager files when configured."""
        harness = self.harness
        simulate_lifecycle_coordinator(harness)

        resource_groups_config = (
            '{"rootGroups":[{"name":"global","softMemoryLimit":"80%",'
            '"hardConcurrencyLimit":10,"maxQueued":10}],"selectors":'
            '[{"user":".*","group":"global"}]}'
        )
        harness.update_config({"resource-groups-config": resource_groups_config})

        container = harness.model.unit.get_container("trino")
        properties_path = "/usr/lib/trino/etc/resource-groups.properties"
        json_path = "/usr/lib/trino/etc/resource-groups.json"

        self.assertTrue(container.exists(properties_path))
        self.assertTrue(container.exists(json_path))
        self.assertEqual(container.pull(json_path).read(), resource_groups_config)

    def test_resource_group_manager_files_removed(self):
        """The charm removes the resource group manager files when unset."""
        harness = self.harness
        simulate_lifecycle_coordinator(harness)

        harness.update_config(
            {
                "resource-groups-config": (
                    '{"rootGroups":[{"name":"global","softMemoryLimit":"80%",'
                    '"hardConcurrencyLimit":10,"maxQueued":10}],"selectors":'
                    '[{"user":".*","group":"global"}]}'
                )
            }
        )
        harness.update_config({"resource-groups-config": ""})

        container = harness.model.unit.get_container("trino")
        properties_path = "/usr/lib/trino/etc/resource-groups.properties"
        json_path = "/usr/lib/trino/etc/resource-groups.json"

        self.assertFalse(container.exists(properties_path))
        self.assertFalse(container.exists(json_path))

    def test_per_replica_params_override_backend_params(self):
        """Per-replica params override backend params in rendered catalog files.

        The rw replica and ro replica must get the targetServerType declared
        in their respective replica params, not a shared value from the backend.
        """
        harness = self.harness
        simulate_lifecycle_coordinator(harness)

        postgresql_secret_id = harness.add_model_secret(
            "trino-k8s",
            {"replicas": POSTGRESQL_REPLICA_SECRET_WITH_PARAMS},
        )
        catalog_config = create_single_catalog_config(postgresql_secret_id)
        harness.update_config({"catalog-config": catalog_config})

        container = harness.model.unit.get_container("trino")

        ro_props = container.pull(POSTGRESQL_1_CATALOG_PATH).read()
        rw_props = container.pull(POSTGRESQL_1_DEVELOPER_CATALOG_PATH).read()

        self.assertIn("targetServerType=preferSecondary", ro_props)
        self.assertIn("targetServerType=primary", rw_props)
        self.assertNotIn("targetServerType=preferSecondary", rw_props)

    def test_backend_params_applied_when_replica_params_absent(self):
        """Backend params are used for all replicas when no per-replica params are set.

        Verifies the fallback path: replicas without their own params inherit
        the backend-level params unchanged.
        """
        harness = self.harness
        simulate_lifecycle_coordinator(harness)

        postgresql_secret_id = harness.add_model_secret(
            "trino-k8s",
            {"replicas": POSTGRESQL_REPLICA_SECRET},
        )
        catalog_config = create_single_catalog_config(
            postgresql_secret_id,
            backend_params="ssl=false&targetServerType=primary",
        )
        harness.update_config({"catalog-config": catalog_config})

        container = harness.model.unit.get_container("trino")

        ro_props = container.pull(POSTGRESQL_1_CATALOG_PATH).read()
        rw_props = container.pull(POSTGRESQL_1_DEVELOPER_CATALOG_PATH).read()

        # Both replicas should carry the backend's params unchanged
        self.assertIn("targetServerType=primary", ro_props)
        self.assertIn("targetServerType=primary", rw_props)

    def test_replica_params_override_backend_params_when_both_present(self):
        """Replica params take precedence over backend params when both are declared.

        Verifies the override path: even when the backend has params, each
        replica's own params replace them entirely for that catalog file.
        """
        harness = self.harness
        simulate_lifecycle_coordinator(harness)

        postgresql_secret_id = harness.add_model_secret(
            "trino-k8s",
            {"replicas": POSTGRESQL_REPLICA_SECRET_WITH_PARAMS},
        )
        catalog_config = create_single_catalog_config(
            postgresql_secret_id,
            backend_params="ssl=false&targetServerType=primary",
        )
        harness.update_config({"catalog-config": catalog_config})

        container = harness.model.unit.get_container("trino")

        ro_props = container.pull(POSTGRESQL_1_CATALOG_PATH).read()
        rw_props = container.pull(POSTGRESQL_1_DEVELOPER_CATALOG_PATH).read()

        # Replica params should win, backend has targetServerType=primary for both,
        # but replica declares preferSecondary for ro and primary for rw
        self.assertIn("targetServerType=preferSecondary", ro_props)
        self.assertNotIn("targetServerType=primary", ro_props)
        self.assertIn("targetServerType=primary", rw_props)
        self.assertNotIn("targetServerType=preferSecondary", rw_props)

    def test_coordinator_publishes_int_comms_secret_id(self):
        """Coordinator writes int-comms-secret-id to the relation databag instead of plaintext.

        Asserts that:
        - the coordinator relation databag contains `int-comms-secret-id`
        - the relation databag does NOT contain any plaintext secret value
        """
        harness = self.harness
        (rel_id, *_) = simulate_lifecycle_coordinator(harness)

        relation_data = harness.get_relation_data(rel_id, harness.charm.app)

        # The secret ID must be present.
        assert "int-comms-secret-id" in relation_data
        assert relation_data["int-comms-secret-id"].startswith("secret:")

        # The raw int-comms value must NOT appear in the relation databag.
        assert "int-comms-secret" not in relation_data
        assert "int_comms_secret" not in relation_data

    def test_coordinator_int_comms_secret_is_singleton(self):
        """Calling update_coordinator_relation_data twice reuses the same secret."""
        harness = self.harness
        (rel_id, *_) = simulate_lifecycle_coordinator(harness)

        first_id = harness.get_relation_data(rel_id, harness.charm.app).get("int-comms-secret-id")

        # Trigger a second update cycle (e.g. config changed).
        harness.charm.trino_coordinator.update_coordinator_relation_data()
        second_id = harness.get_relation_data(rel_id, harness.charm.app).get("int-comms-secret-id")

        assert first_id == second_id, "Singleton secret ID must not change between updates"

    def test_coordinator_int_comms_secret_preserves_existing_value(self):
        """When peer state already carries an int-comms value, the Juju secret reuses it."""
        harness = self.harness
        # Add the peer relation first so state is accessible.
        harness.add_relation("peer", "trino")

        # Pre-seed the peer state with a known value.
        harness.charm.state.int_comms_secret = "pre-existing-secret-value"  # nosec

        # Directly invoke the singleton helper — it must reuse the pre-seeded value.
        secret = harness.charm.trino_coordinator._get_or_create_int_comms_secret()
        assert secret is not None
        content = secret.get_content(refresh=True)
        assert content["secret"] == "pre-existing-secret-value"  # nosec

        # Calling it again must return the same secret (singleton).
        secret2 = harness.charm.trino_coordinator._get_or_create_int_comms_secret()
        assert secret2 is not None
        # Both invocations must carry the pre-seeded value (no rotation).
        content2 = secret2.get_content(refresh=True)
        assert content2["secret"] == "pre-existing-secret-value"  # nosec

    def test_worker_resolves_int_comms_secret_from_coordinator(self):
        """Worker reads int-comms-secret from coordinator.

        Worker reads int-comms-secret-id from relation, stores the ID in state, and
        resolves the secret value at render time via _get_int_comms_secret_value.
        """
        harness = self.harness
        (container, event, *_) = simulate_lifecycle_worker(harness)

        # The worker's peer state must carry the secret *ID* (not the plaintext value).
        secret_id = harness.charm.state.int_comms_secret_id
        assert secret_id is not None
        assert secret_id.startswith("secret:")

        # _get_int_comms_secret_value must resolve to the actual secret content.
        assert harness.charm._get_int_comms_secret_value() == "test-int-comms-secret"  # nosec

    def test_worker_waits_when_int_comms_secret_id_absent(self):
        """Worker goes into WaitingStatus when int-comms-secret-id is not yet in relation data."""
        harness = self.harness
        harness.add_relation("peer", "trino")

        # Mock exec so _update does not fail.
        harness.handle_exec("trino", ["keytool"], result=0)
        harness.handle_exec("trino", ["htpasswd"], result=0)
        harness.handle_exec("trino", ["/bin/sh"], result="/usr/lib/jvm/java-25-openjdk-amd64/")
        harness.update_config({"charm-function": "worker"})

        container = harness.model.unit.get_container("trino")
        harness.charm.on.trino_pebble_ready.emit(container)

        rel_id = harness.add_relation("trino-worker", "trino-k8s")
        harness.add_relation_unit(rel_id, "trino-k8s-worker/0")

        # Relation data WITHOUT int-comms-secret-id.
        data = {
            "trino-worker": {
                "discovery-uri": "http://trino-k8s:8080",
                "catalogs": "",
            }
        }
        event = make_relation_event("trino-worker", rel_id, data)
        harness.charm.trino_worker._on_relation_changed(event)

        self.assertEqual(
            harness.model.unit.status,
            WaitingStatus("waiting for coordinator to publish internal communication secret"),
        )

    def test_worker_no_plaintext_secret_in_relation_databag(self):
        """Worker never writes a plaintext internal communication secret to relation data.

        This is the cross-model / cross-controller safety invariant: the databag
        carries only the Juju secret ID, not the raw value.
        """
        harness = self.harness
        simulate_lifecycle_worker(harness)

        relation = harness.charm.model.get_relation("trino-worker")
        assert relation is not None

        # Only inspect the worker app's own databag — the coordinator's bag is
        # populated via make_relation_event (a fake dict) and is not accessible
        # through the real relation in the worker harness.
        app_data = dict(relation.data[harness.charm.app])
        for key, value in app_data.items():
            if "int-comms" in key.lower() and not key.endswith("-id"):
                raise AssertionError(
                    f"Plaintext int-comms field {key!r} "
                    f"found in worker app relation data: {value!r}"
                )
