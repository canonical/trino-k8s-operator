# Copyright 2023 Canonical Ltd.
# See LICENSE file for licensing details.
#
# Learn more about testing at: https://juju.is/docs/sdk/testing


"""Trino charm unit tests."""

# pylint:disable=protected-access

import json
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
from unit.helpers import (
    BIGQUERY_CATALOG_PATH,
    BIGQUERY_SECRET,
    DEFAULT_JVM_STRING,
    POSTGRESQL_1_CATALOG_PATH,
    POSTGRESQL_2_CATALOG_PATH,
    POSTGRESQL_REPLICA_CERT,
    POSTGRESQL_REPLICA_SECRET,
    SERVER_PORT,
    TEST_USERS,
    UPDATED_JVM_OPTIONS,
    USER_JVM_STRING,
)

from charm import TrinoK8SCharm
from state import State

mock_incomplete_pebble_plan = {"services": {"trino": {"override": "replace"}}}

logger = logging.getLogger(__name__)


class TestCharm(TestCase):
    """Unit tests.

    Attrs:
        maxDiff: Specifies max difference shown by failed tests.
    """

    maxDiff = None

    def setUp(self):
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
            bigquery_secret_id,
        ) = simulate_lifecycle_coordinator(harness)
        catalog_config = create_catalog_config(
            postgresql_secret_id, bigquery_secret_id
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

    def test_config_changed(self):
        """The pebble plan changes according to config changes."""
        harness = self.harness
        (
            _,
            postgresql_secret_id,
            bigquery_secret_id,
        ) = simulate_lifecycle_coordinator(harness)

        catalog_config = create_catalog_config(
            postgresql_secret_id, bigquery_secret_id
        )

        # Update the config.
        self.harness.update_config(
            {
                "google-client-id": "test-client-id",
                "google-client-secret": "test-client-secret",
                "web-proxy": "proxy:port",
                "charm-function": "all",
                "additional-jvm-options": USER_JVM_STRING,
            }
        )

        # The new plan reflects the change.
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
                        "OAUTH_CLIENT_ID": "test-client-id",
                        "OAUTH_CLIENT_SECRET": "test-client-secret",
                        "WEB_PROXY": "proxy:port",
                        "CHARM_FUNCTION": "all",
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
                        "JVM_OPTIONS": UPDATED_JVM_OPTIONS,
                    },
                }
            },
        }
        got_plan = harness.get_container_pebble_plan("trino").to_dict()
        environment = got_plan["services"]["trino"]["environment"]
        environment["JAVA_TRUSTSTORE_PWD"] = "truststore_pwd"  # nosec
        environment["USER_SECRET_ID"] = "secret:secret-id"  # nosec
        self.assertEqual(got_plan["services"], want_plan["services"])

        # The MaintenanceStatus is set.
        self.assertEqual(
            harness.model.unit.status,
            MaintenanceStatus("replanning application"),
        )

    def test_catalog_added(self):
        """The catalog directory is updated to add the new catalog."""
        harness = self.harness
        (
            _,
            postgresql_secret_id,
            bigquery_secret_id,
        ) = simulate_lifecycle_coordinator(harness)

        catalog_config = create_added_catalog_config(
            postgresql_secret_id, bigquery_secret_id
        )

        # Update the config.
        self.harness.update_config({"catalog-config": catalog_config})

        # Validate catalog.properties file created.
        container = harness.model.unit.get_container("trino")
        self.assertTrue(container.exists(POSTGRESQL_2_CATALOG_PATH))
        self.assertTrue(container.exists(BIGQUERY_CATALOG_PATH))

    def test_catalog_invalid_config(self):
        """The catalog directory is updated to add the new catalog."""
        harness = self.harness
        simulate_lifecycle_coordinator(harness)

        self.harness.update_config({"catalog-config": "catalog: incorrect"})

        self.assertEqual(
            harness.model.unit.status,
            BlockedStatus("Invalid catalog-config schema"),
        )

    def test_catalog_removed(self):
        """The catalog directory is updated to remove existing catalogs."""
        harness = self.harness
        simulate_lifecycle_coordinator(harness)

        # Update the config.
        self.harness.update_config({"catalog-config": ""})

        # Validate catalog.properties file created.
        container = harness.model.unit.get_container("trino")
        self.assertFalse(container.exists(POSTGRESQL_1_CATALOG_PATH))
        self.assertFalse(container.exists(BIGQUERY_CATALOG_PATH))

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
            bigquery_secret_id,
        ) = simulate_lifecycle_coordinator(harness)

        catalog_config = create_catalog_config(
            postgresql_secret_id, bigquery_secret_id
        )
        relation_data = harness.get_relation_data(rel_id, harness.charm.app)
        secret = harness.model.get_secret(label="catalog-config")
        content = secret.get_content()

        assert relation_data["discovery-uri"] == "http://trino-k8s:8080"
        assert "secret:" in relation_data["catalog-secret-id"]
        assert content["catalogs"] == catalog_config

    def test_trino_worker_secret_changed(self):
        """Test the worker catalogs change when the secret content changes."""
        harness = self.harness
        (
            container,
            _,
            postgresql_secret_id,
            bigquery_secret_id,
            secret_id,
        ) = simulate_lifecycle_worker(harness)

        catalog_config = create_added_catalog_config(
            postgresql_secret_id, bigquery_secret_id
        )
        secret = harness.model.get_secret(id=secret_id)

        secret.set_content({"catalogs": catalog_config})
        harness.charm.on.secret_changed.emit(
            label="catalog-config", id=secret_id
        )

        self.assertTrue(container.exists(POSTGRESQL_1_CATALOG_PATH))

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
        container, _, _, _, _ = simulate_lifecycle_worker(harness)

        self.assertTrue(container.exists(BIGQUERY_CATALOG_PATH))
        self.assertTrue(container.exists(POSTGRESQL_1_CATALOG_PATH))

    def test_trino_worker_relation_broken(self):
        """Test trino relation broken.

        The coordinator and worker Trino charms relation is broken.
        """
        harness = self.harness
        container, event, _, _, _ = simulate_lifecycle_worker(harness)

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


def simulate_lifecycle_worker(harness):
    """Establish a relation between Trino worker and coordinator.

    Args:
        harness: ops.testing.Harness object used to simulate trino relation.

    Returns:
        container: the trino application container.
        event: the relation event.
    """
    # Simulate peer relation readiness.
    harness.add_relation("peer", "trino")

    # Simulate pebble readiness.
    harness.handle_exec("trino", ["keytool"], result=0)
    harness.handle_exec("trino", ["htpasswd"], result=0)
    harness.handle_exec(
        "trino", ["/bin/sh"], result="/usr/lib/jvm/java-21-openjdk-amd64/"
    )
    harness.update_config({"charm-function": "worker"})

    # Add catalog secrets
    bigquery_secret_id = harness.add_model_secret(
        "trino-k8s",
        {"service-accounts": BIGQUERY_SECRET},
    )

    postgresql_secret_id = harness.add_model_secret(
        "trino-k8s",
        {
            "replicas": POSTGRESQL_REPLICA_SECRET,
            "cert": POSTGRESQL_REPLICA_CERT,
        },
    )
    catalog_config = create_catalog_config(
        postgresql_secret_id, bigquery_secret_id
    )

    container = harness.model.unit.get_container("trino")
    harness.charm.on.trino_pebble_ready.emit(container)

    secret_id = harness.add_model_secret(
        "trino-k8s",
        {"catalogs": catalog_config},
    )
    rel_id = harness.add_relation("trino-worker", "trino-k8s")
    harness.add_relation_unit(rel_id, "trino-k8s-worker/0")

    data = {
        "trino-worker": {
            "discovery-uri": "http://trino-k8s:8080",
            "catalog-secret-id": secret_id,
        }
    }
    event = make_relation_event("trino-worker", rel_id, data)
    harness.charm.trino_worker._on_relation_changed(event)
    return (
        container,
        event,
        postgresql_secret_id,
        bigquery_secret_id,
        secret_id,
    )


def simulate_lifecycle_coordinator(harness):
    """Simulate a healthy charm life-cycle.

    Args:
        harness: ops.testing.Harness object used to simulate charm lifecycle.

    Returns:
        rel_id: the relation ID of the trino coordinator:worker relation.
    """
    # Simulate peer relation readiness.
    harness.add_relation("peer", "trino")

    # Simulate pebble readiness.
    container = harness.model.unit.get_container("trino")
    harness.handle_exec("trino", ["htpasswd"], result=0)
    harness.handle_exec(
        "trino", ["/bin/sh"], result="/usr/lib/jvm/java-21-openjdk-amd64/"
    )
    secret_id = harness.add_model_secret(
        "trino-k8s",
        {"users": TEST_USERS},
    )

    harness.charm.on.trino_pebble_ready.emit(container)

    # Add catalog secrets
    bigquery_secret_id = harness.add_model_secret(
        "trino-k8s",
        {"service-accounts": BIGQUERY_SECRET},
    )

    postgresql_secret_id = harness.add_model_secret(
        "trino-k8s",
        {
            "replicas": POSTGRESQL_REPLICA_SECRET,
            "cert": POSTGRESQL_REPLICA_CERT,
        },
    )
    catalog_config = create_catalog_config(
        postgresql_secret_id, bigquery_secret_id
    )

    # Add worker and coordinator relation
    harness.handle_exec("trino", ["keytool"], result=0)
    harness.update_config({"catalog-config": catalog_config})
    rel_id = harness.add_relation("trino-coordinator", "trino-k8s-worker")

    harness.update_config({"user-secret-id": secret_id})
    return rel_id, postgresql_secret_id, bigquery_secret_id


def make_relation_event(app, rel_id, data):
    """Create and return a mock policy created event.

        The event is generated by the relation with postgresql_db

    Args:
        app: name of the application.
        rel_id: relation id.
        data: relation data.

    Returns:
        Event dict.
    """
    return type(
        "Event",
        (),
        {
            "app": app,
            "relation": type(
                "Relation",
                (),
                {
                    "data": data,
                    "id": rel_id,
                },
            ),
        },
    )


def create_catalog_config(postgresql_secret_id, bigquery_secret_id):
    """Create and return catalog-config value.

    Args:
        postgresql_secret_id: the juju secret id for postgresql
        bigquery_secret_id: the juju secret id for bigquery

    Returns:
        the catalog configuration.
    """
    return f"""\
    catalogs:
        postgresql-1:
            backend: dwh
            database: example
            secret-id: {postgresql_secret_id}
        bigquery:
            backend: bigquery
            project: project-12345
            secret-id: {bigquery_secret_id}
    backends:
        dwh:
            connector: postgresql
            url: jdbc:postgresql://example.com:5432
            params: ssl=true&sslmode=require&sslrootcert={{SSL_PATH}}&sslrootcertpassword={{SSL_PWD}}
            config: |
                case-insensitive-name-matching=true
                decimal-mapping=allow_overflow
                decimal-rounding-mode=HALF_UP
        bigquery:
            connector: bigquery
            config: |
                bigquery.case-insensitive-name-matching=true
    """


def create_added_catalog_config(postgresql_secret_id, bigquery_secret_id):
    """Create and return catalog-config value, with added catalog.

    Args:
        postgresql_secret_id: the juju secret id for postgresql
        bigquery_secret_id: the juju secret id for bigquery

    Returns:
        the catalog configuration, with an added catalog.
    """
    return f"""\
    catalogs:
        postgresql-1:
            backend: dwh
            database: example
            secret-id: {postgresql_secret_id}
        postgresql-2:
            backend: dwh
            database: updated-db
            secret-id: {postgresql_secret_id}
        bigquery:
            backend: bigquery
            project: project-12345
            secret-id: {bigquery_secret_id}
    backends:
        dwh:
            connector: postgresql
            url: jdbc:postgresql://example.com:5432
            params: ssl=true&sslmode=require&sslrootcert={{SSL_PATH}}&sslrootcertpassword={{SSL_PWD}}
            config: |
                case-insensitive-name-matching=true
                decimal-mapping=allow_overflow
                decimal-rounding-mode=HALF_UP
        bigquery:
            connector: bigquery
            config: |
                bigquery.case-insensitive-name-matching=true
    """


class TestState(TestCase):
    """Unit tests for state.

    Attrs:
        maxDiff: Specifies max difference shown by failed tests.
    """

    maxDiff = None

    def test_get(self):
        """It is possible to retrieve attributes from the state."""
        state = make_state({"foo": json.dumps("bar")})
        self.assertEqual(state.foo, "bar")
        self.assertIsNone(state.bad)

    def test_set(self):
        """It is possible to set attributes in the state."""
        data = {"foo": json.dumps("bar")}
        state = make_state(data)
        state.foo = 42
        state.list = [1, 2, 3]
        self.assertEqual(state.foo, 42)
        self.assertEqual(state.list, [1, 2, 3])
        self.assertEqual(data, {"foo": "42", "list": "[1, 2, 3]"})

    def test_del(self):
        """It is possible to unset attributes in the state."""
        data = {"foo": json.dumps("bar"), "answer": json.dumps(42)}
        state = make_state(data)
        del state.foo
        self.assertIsNone(state.foo)
        self.assertEqual(data, {"answer": "42"})
        # Deleting a name that is not set does not error.
        del state.foo

    def test_is_ready(self):
        """The state is not ready when it is not possible to get relations."""
        state = make_state({})
        self.assertTrue(state.is_ready())

        state = State("myapp", lambda: None)
        self.assertFalse(state.is_ready())


def make_state(data):
    """Create state object.

    Args:
        data: Data to be included in state.

    Returns:
        State object with data.
    """
    app = "myapp"
    rel = type("Rel", (), {"data": {app: data}})()
    return State(app, lambda: rel)
