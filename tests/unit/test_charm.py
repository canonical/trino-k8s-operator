# Copyright 2023 Canonical Ltd.
# See LICENSE file for licensing details.
#
# Learn more about testing at: https://juju.is/docs/sdk/testing


"""Trino charm unit tests."""

# pylint:disable=protected-access

import json
import logging
from unittest import TestCase, mock

from ops import testing
from ops.model import (
    ActiveStatus,
    BlockedStatus,
    MaintenanceStatus,
    WaitingStatus,
)
from ops.pebble import CheckStatus
from ops.testing import Harness

from charm import TrinoK8SCharm
from state import State

SERVER_PORT = "8080"
TEST_CATALOG_CONFIG = """\
example-db: |
  connector.name=postgresql
  connection-url=jdbc:postgresql://host.com:5432/database
  connection-user=testing
  connection-password=test
"""
TEST_CATALOG_PATH = "/usr/lib/trino/etc/catalog/example-db.properties"
RANGER_PROPERTIES_PATH = "/usr/lib/ranger/install.properties"
POLICY_MGR_URL = "http://ranger-k8s:6080"
GROUP_MANAGEMENT = """\
        users:
          - name: user1
            firstname: One
            lastname: User
            email: user1@canonical.com
        memberships:
          - groupname: commercial-systems
            users: [user1]
        groups:
          - name: commercial-systems
            description: commercial systems team
"""
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
        harness.charm.on.trino_pebble_ready.emit(container)

        # No plans are set yet.
        got_plan = harness.get_container_pebble_plan("trino").to_dict()
        self.assertEqual(got_plan, {})

        # The BlockStatus is set with a message.
        self.assertEqual(
            harness.model.unit.status,
            WaitingStatus("Waiting for peer relation."),
        )

    def test_ready(self):
        """The pebble plan is correctly generated when the charm is ready."""
        harness = self.harness
        simulate_lifecycle(harness)

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
                        "CATALOG_CONFIG": None,
                        "DEFAULT_PASSWORD": "ubuntu123",
                        "PASSWORD_DB_PATH": "/usr/lib/trino/etc/password.db",
                        "LOG_LEVEL": "info",
                        "OAUTH_CLIENT_ID": None,
                        "OAUTH_CLIENT_SECRET": None,
                        "WEB_PROXY": None,
                        "SSL_PATH": "/usr/lib/trino/etc/conf/truststore.jks",
                        "SSL_PWD": "truststore123",
                        "CHARM_FUNCTION": "coordinator",
                        "DISCOVERY_URI": "http://trino-k8s:8080",
                        "APPLICATION_NAME": "trino-k8s",
                        "TRINO_HOME": "/usr/lib/trino/etc",
                    },
                }
            },
        }
        got_plan = harness.get_container_pebble_plan("trino").to_dict()
        got_plan["services"]["trino"]["environment"][
            "SSL_PWD"
        ] = "truststore123"  # nosec
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

        simulate_lifecycle(harness)

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
        simulate_lifecycle(harness)

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

    def test_config_changed(self):
        """The pebble plan changes according to config changes."""
        harness = self.harness
        simulate_lifecycle(harness)

        # Update the config.
        self.harness.update_config(
            {
                "google-client-id": "test-client-id",
                "google-client-secret": "test-client-secret",
                "web-proxy": "proxy:port",
                "charm-function": "worker",
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
                        "CATALOG_CONFIG": None,
                        "DEFAULT_PASSWORD": "ubuntu123",
                        "PASSWORD_DB_PATH": "/usr/lib/trino/etc/password.db",
                        "LOG_LEVEL": "info",
                        "OAUTH_CLIENT_ID": "test-client-id",
                        "OAUTH_CLIENT_SECRET": "test-client-secret",
                        "WEB_PROXY": "proxy:port",
                        "SSL_PATH": "/usr/lib/trino/etc/conf/truststore.jks",
                        "SSL_PWD": "truststore123",
                        "CHARM_FUNCTION": "worker",
                        "DISCOVERY_URI": "http://trino-k8s:8080",
                        "APPLICATION_NAME": "trino-k8s",
                        "TRINO_HOME": "/usr/lib/trino/etc",
                    },
                }
            },
        }
        got_plan = harness.get_container_pebble_plan("trino").to_dict()
        got_plan["services"]["trino"]["environment"][
            "SSL_PWD"
        ] = "truststore123"  # nosec
        self.assertEqual(got_plan["services"], want_plan["services"])

        # The ActiveStatus is set with no message.
        self.assertEqual(
            harness.model.unit.status,
            MaintenanceStatus("replanning application"),
        )

    def test_catalog_added(self):
        """The catalog directory is updated to add the new catalog."""
        harness = self.harness
        simulate_lifecycle(harness)

        # Update the config.
        self.harness.update_config({"catalog-config": TEST_CATALOG_CONFIG})

        # Validate catalog.properties file created.
        container = harness.model.unit.get_container("trino")
        self.assertTrue(container.exists(TEST_CATALOG_PATH))

    def test_catalog_removed(self):
        """The catalog directory is updated to remove existing catalogs."""
        harness = self.harness
        simulate_lifecycle(harness)

        # Update the config.
        self.harness.update_config({"catalog-config": None})

        # Validate catalog.properties file created.
        container = harness.model.unit.get_container("trino")
        self.assertFalse(container.exists(TEST_CATALOG_PATH))

    def test_policy_relation_created(self):
        """Add policy relation."""
        harness = self.harness
        simulate_lifecycle(harness)

        rel_id = harness.add_relation("policy", "trino-k8s")
        harness.add_relation_unit(rel_id, "trino-k8s/0")

        data = {self.harness.charm.app: {}}
        event = make_policy_relation_event(rel_id, data)
        harness.charm.policy._on_relation_created(event)

        relation_data = self.harness.get_relation_data(rel_id, "trino-k8s")
        assert relation_data == {
            "name": f"relation_{rel_id}",
            "type": "trino",
            "jdbc.driverClassName": "io.trino.jdbc.TrinoDriver",
            "jdbc.url": "jdbc:trino://trino-k8s:8080",
        }

    def test_policy_relation_changed(self):
        """Add policy_manager_url to the relation databag."""
        harness = self.harness
        simulate_lifecycle(harness)
        container = harness.model.unit.get_container("trino")

        # Create the relation
        rel_id = harness.add_relation("policy", "trino-k8s")
        harness.add_relation_unit(rel_id, "trino-k8s/0")

        # Create handlers for Container.exec() commands
        for command in [
            "bash",
            "tar",
            "useradd",
            "groupadd",
            "usermod",
            "deluser",
        ]:
            harness.handle_exec("trino", [command], result=0)
        harness.handle_exec("trino", ["getent"], handler=group_handler)

        # Create and emit the policy `_on_relation_changed` event.
        data = {
            "ranger-k8s": {
                "policy_manager_url": POLICY_MGR_URL,
                "user-group-configuration": GROUP_MANAGEMENT,
            },
        }
        event = make_policy_relation_event(rel_id, data)
        harness.charm.policy._on_relation_changed(event)

        self.assertTrue(
            event.relation.data["ranger-k8s"]["user-group-configuration"]
        )
        self.assertTrue(container.exists(RANGER_PROPERTIES_PATH))

    def test_policy_relation_broken(self):
        """Add policy_manager_url to the relation databag."""
        harness = self.harness
        simulate_lifecycle(harness)

        rel_id = harness.add_relation("policy", "trino-k8s")
        harness.add_relation_unit(rel_id, "trino-k8s/0")
        harness.handle_exec("trino", ["bash"], result=0)

        data = {"ranger-k8s": {}}
        event = make_policy_relation_event(rel_id, data)
        harness.charm.policy._on_relation_broken(event)

        self.assertFalse(
            event.relation.data["ranger-k8s"].get("user-group-configuration")
        )

    def test_update_status_up(self):
        """The charm updates the unit status to active based on UP status."""
        harness = self.harness

        simulate_lifecycle(harness)

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

        simulate_lifecycle(harness)

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
        simulate_lifecycle(harness)

        container = harness.model.unit.get_container("trino")
        container.add_layer("trino", mock_incomplete_pebble_plan, combine=True)
        harness.charm.on.update_status.emit()

        self.assertEqual(
            harness.model.unit.status,
            MaintenanceStatus("replanning application"),
        )
        plan = harness.get_container_pebble_plan("trino").to_dict()
        assert plan != mock_incomplete_pebble_plan


def simulate_lifecycle(harness):
    """Simulate a healthy charm life-cycle.

    Args:
        harness: ops.testing.Harness object used to simulate charm lifecycle.
    """
    # Simulate peer relation readiness.
    harness.add_relation("peer", "trino")

    # Simulate pebble readiness.
    container = harness.model.unit.get_container("trino")
    harness.charm.on.trino_pebble_ready.emit(container)


def make_policy_relation_event(rel_id, data):
    """Create and return a mock policy created event.

        The event is generated by the relation with postgresql_db

    Args:
        rel_id: relation id.
        data: relation data.

    Returns:
        Event dict.
    """
    return type(
        "Event",
        (),
        {
            "app": "ranger-k8s",
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


def group_handler(args):
    """Execution handler for getent command.

    Args:
        args: execution arguments.

    Returns:
        The execution result.
    """
    if args.command == ["getent", "passwd"]:
        out = "user2:x:1002:1002:ranger:/home/user2:/bin/sh"
    elif args.command == ["getent", "group"]:
        out = "marketing:x:1004:user2"
    return testing.ExecResult(stdout=out)


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
