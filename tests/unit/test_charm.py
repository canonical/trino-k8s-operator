# Copyright 2023 Canonical Ltd.
# See LICENSE file for licensing details.
#
# Learn more about testing at: https://juju.is/docs/sdk/testing


"""Trino charm unit tests."""

# pylint:disable=protected-access

import logging
from unittest import TestCase, mock

from ops.model import (
    ActiveStatus,
    BlockedStatus,
    MaintenanceStatus,
    SecretNotFoundError,
)
from ops.pebble import CheckStatus
from ops.testing import Harness

from charm import TrinoK8SCharm
from tests.unit.helpers import (
    BIGQUERY_CATALOG_PATH,
    DEFAULT_JVM_STRING,
    POSTGRESQL_1_CATALOG_PATH,
    SERVER_PORT,
    create_catalog_config,
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
                        "PASSWORD_DB_PATH": "/usr/lib/trino/etc/password.db",
                        "LOG_LEVEL": "info",
                        "OAUTH_CLIENT_ID": None,
                        "OAUTH_CLIENT_SECRET": None,
                        "WEB_PROXY": None,
                        "CHARM_FUNCTION": "coordinator",
                        "DISCOVERY_URI": "http://trino-k8s:8080",
                        "APPLICATION_NAME": "trino-k8s",
                        "TRINO_HOME": "/usr/lib/trino/etc",
                        "JMX_PORT": 9081,
                        "METRICS_PORT": 9090,
                        "OAUTH_USER_MAPPING": None,
                        "RANGER_RELATION": False,
                        "ACL_ACCESS_MODE": "all",
                        "ACL_CATALOG_PATTERN": ".*",
                        "ACL_USER_PATTERN": ".*",
                        "JAVA_TRUSTSTORE_PWD": "truststore_pwd",
                        "USER_SECRET_ID": "secret:secret-id",
                        "JVM_OPTIONS": DEFAULT_JVM_STRING,
                        "COORDINATOR_REQUEST_TIMEOUT": "10m",
                        "COORDINATOR_CONNECT_TIMEOUT": "30s",
                        "WORKER_REQUEST_TIMEOUT": "30s",
                        "MAX_CONCURRENT_QUERIES": 5,
                    },
                }
            },
        }
        got_plan = harness.get_container_pebble_plan("trino").to_dict()
        environment = got_plan["services"]["trino"]["environment"]
        environment["JAVA_TRUSTSTORE_PWD"] = "truststore_pwd"  # nosec
        environment["USER_SECRET_ID"] = "secret:secret-id"  # nosec

        self.assertEqual(got_plan["services"], want_plan["services"])

        # The service was started.
        service = harness.model.unit.get_container("trino").get_service(
            "trino"
        )
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

        nginx_route_relation_id = harness.add_relation(
            "nginx-route", "ingress"
        )
        harness.charm._require_nginx_route()

        assert harness.get_relation_data(
            nginx_route_relation_id, harness.charm.app
        ) == {
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
        got_log_level = harness.get_container_pebble_plan("trino").to_dict()[
            "services"
        ]["trino"]["environment"]["LOG_LEVEL"]
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
        """The catalog directory is updated to add the new catalog."""
        harness = self.harness
        simulate_lifecycle_coordinator(harness)

        with self.assertRaises(KeyError):
            self.harness.update_config(
                {"catalog-config": "catalog: incorrect"}
            )

    def test_update_status_up(self):
        """The charm updates the unit status to active based on UP status."""
        harness = self.harness

        simulate_lifecycle_coordinator(harness)

        container = harness.model.unit.get_container("trino")
        container.get_check = mock.Mock(status="up")
        container.get_check.return_value.status = CheckStatus.UP
        harness.charm.on.update_status.emit()

        self.assertEqual(
            harness.model.unit.status, ActiveStatus("Status check: UP")
        )

    def test_update_status_down(self):
        """The charm updates the unit status to maintenance based on DOWN status."""
        harness = self.harness

        simulate_lifecycle_coordinator(harness)

        container = harness.model.unit.get_container("trino")
        container.get_check = mock.Mock(status="up")
        container.get_check.return_value.status = CheckStatus.DOWN
        harness.charm.on.update_status.emit()

        self.assertEqual(
            harness.model.unit.status, MaintenanceStatus("Status check: DOWN")
        )

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
        secret = harness.model.get_secret(label="catalog-config")
        content = secret.get_content()

        assert relation_data["discovery-uri"] == "http://trino-k8s:8080"
        assert "secret:" in relation_data["catalog-secret-id"]
        assert content["catalogs"] == catalog_config

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
        container, _, _, _, _, _, _, _ = simulate_lifecycle_worker(harness)

        self.assertTrue(container.exists(BIGQUERY_CATALOG_PATH))
        self.assertTrue(container.exists(POSTGRESQL_1_CATALOG_PATH))

    def test_trino_worker_relation_broken(self):
        """Test trino relation broken.

        The coordinator and worker Trino charms relation is broken.
        """
        harness = self.harness
        container, event, _, _, _, _, _, _ = simulate_lifecycle_worker(harness)

        harness.charm.trino_worker._on_relation_broken(event)
        self.assertFalse(container.exists(POSTGRESQL_1_CATALOG_PATH))

    def test_trino_single_node_deployment(self):
        """Test pebble plan is created with single node deployment."""
        harness = self.harness
        harness.add_relation("peer", "trino")

        harness.handle_exec(
            "trino", ["/bin/sh"], result="/usr/lib/jvm/java-21-openjdk-amd64/"
        )
        harness.handle_exec("trino", ["keytool"], result=0)
        container = harness.model.unit.get_container("trino")
        harness.handle_exec("trino", ["htpasswd"], result=0)
        harness.charm.on.trino_pebble_ready.emit(container)
        harness.update_config({"charm-function": "all"})

        # There is a valid pebble plan.
        got_plan = harness.get_container_pebble_plan("trino").to_dict()
        assert (
            got_plan["services"]["trino"]["environment"]["CHARM_FUNCTION"]
            == "all"
        )

        # The MaintenanceStatus is set.
        self.assertEqual(
            harness.model.unit.status,
            MaintenanceStatus("replanning application"),
        )
