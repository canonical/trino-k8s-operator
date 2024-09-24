# Copyright 2023 Canonical Ltd.
# See LICENSE file for licensing details.
#
# Learn more about testing at: https://juju.is/docs/sdk/testing


"""Trino policy unit tests."""

# pylint:disable=protected-access

import logging
from unittest import TestCase, mock

from ops.model import ActiveStatus, MaintenanceStatus
from ops.pebble import CheckStatus
from ops.testing import Harness
from unit.helpers import (
    POLICY_MGR_URL,
    RANGER_LIB,
    RANGER_PROPERTIES_PATH,
    TEST_CATALOG_CONFIG,
)

from charm import TrinoK8SCharm

logger = logging.getLogger(__name__)

OPENSEARCH_RELATION_CHANGED_DATA = {
    "opensearch": {
        "secret-tls": "secretid2",
        "secret-user": "secretid",
        "endpoints": "opensearch-host:port",
    }
}
OPENSEARCH_RELATION_BROKEN_DATA: dict = {"opensearch": {}}
USER_SECRET_CONTENT = {
    "username": "testuser",
    "password": "testpassword",
    "tls-ca": """-----BEGIN CERTIFICATE-----
    MIIC+DCCAeCgAwIBAgIJAKJdWfG2zRAQMA0GCSqGSIb3DQEBCwUAMIGPMQswCQYD
    -----END CERTIFICATE-----
    -----BEGIN CERTIFICATE-----
    AIBC+LCCAuCgAPIBAgIuAKJdWWG2zRAQMA0GFSqGSIP3DQEBCiUAMIGPMQswCQYC
    -----END CERTIFICATE-----""",
}


class TestPolicy(TestCase):
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

    def test_policy_relation_created(self):
        """Add policy relation."""
        harness = self.harness
        rel_id = simulate_lifecycle(harness)

        relation_data = self.harness.get_relation_data(rel_id, "trino-k8s")
        assert relation_data == {
            "name": f"relation_{rel_id}",
            "type": "trino",
            "jdbc.driverClassName": "io.trino.jdbc.TrinoDriver",
            "jdbc.url": "jdbc:trino://trino-k8s.trino-model.svc.cluster.local:8080",
        }

    def test_policy_relation_changed(self):
        """Add policy_manager_url to the relation databag."""
        harness = self.harness
        rel_id = simulate_lifecycle(harness)
        container = harness.model.unit.get_container("trino")

        # Create and emit the policy `_on_relation_changed` event.
        data = {
            "ranger-k8s": {
                "policy_manager_url": POLICY_MGR_URL,
            },
        }
        event = make_relation_event("ranger-k8s", rel_id, data)
        harness.charm.policy._on_relation_changed(event)

        self.assertTrue(container.exists(RANGER_PROPERTIES_PATH))

    def test_policy_relation_broken(self):
        """Add policy_manager_url to the relation databag."""
        harness = self.harness
        rel_id = simulate_lifecycle(harness)

        data = {"ranger-k8s": {}}
        event = make_relation_event("ranger-k8s", rel_id, data)
        harness.charm.policy._on_relation_broken(event)

        self.assertFalse(
            event.relation.data["ranger-k8s"].get("user-group-configuration")
        )

    def test_restore_ranger_plugin(self):
        """Restore plugin if lost."""
        harness = self.harness
        self.test_policy_relation_changed()
        container = harness.model.unit.get_container("trino")
        container.remove_path(RANGER_LIB, recursive=True)
        harness.charm.on.trino_pebble_ready.emit(container)
        assert container.exists(RANGER_LIB)

    @mock.patch("charm.OpensearchRelationHandler.get_secret_content")
    def opensearch_setup(
        self,
        mock_get_secret_content,
        harness,
        data,
    ):
        """Common setup for Openseatch relation changed and broken tests.

        Args:
            mock_get_secret_content: the mocked method for accessing juju secrets.
            harness: ops.testing.Harness object used to simulate charm lifecycle.
            data: the opensearch relation data.

        Returns:
            rel_id: the opensearch relation id.
        """
        mock_get_secret_content.return_value = USER_SECRET_CONTENT

        ranger_rel_id = simulate_lifecycle(harness)
        data = {
            "ranger-k8s": {
                "policy_manager_url": POLICY_MGR_URL,
            },
        }
        event = make_relation_event("ranger-k8s", ranger_rel_id, data)
        harness.charm.policy._on_relation_changed(event)

        rel_id = harness.add_relation("opensearch", "opensearch-app")
        harness.add_relation_unit(rel_id, "opensearch-app/0")
        harness.handle_exec("trino", ["keytool"], result=0)
        event = make_relation_event(
            "opensearch", rel_id, OPENSEARCH_RELATION_CHANGED_DATA
        )
        harness.charm.opensearch_relation_handler._on_index_created(event)
        return rel_id

    def test_on_opensearch_index_created(self):
        """Test handling of opensearch relation changed events."""
        harness = self.harness
        self.opensearch_setup(
            harness=harness, data=OPENSEARCH_RELATION_CHANGED_DATA
        )

        self.assertEqual(
            harness.model.unit.status,
            MaintenanceStatus("Restarting Ranger plugin"),
        )
        container = harness.model.unit.get_container("trino")
        self.assertTrue(container.exists("/opensearch.crt"))
        ranger_config = container.pull(
            "/usr/lib/ranger/install.properties"
        ).read()
        self.assertTrue("XAAUDIT.ELASTICSEARCH.USER=testuser" in ranger_config)

        container.get_check = mock.Mock(status="up")
        container.get_check.return_value.status = CheckStatus.UP
        harness.charm.on.update_status.emit()
        self.assertEqual(
            harness.model.unit.status, ActiveStatus("Status check: UP")
        )

    def test_on_opensearch_relation_broken(self):
        """Test handling of broken relations with opensearch."""
        harness = self.harness
        rel_id = self.opensearch_setup(
            harness=harness, data=OPENSEARCH_RELATION_CHANGED_DATA
        )
        data = OPENSEARCH_RELATION_BROKEN_DATA
        event = make_relation_event("opensearch", rel_id, data)
        self.harness.charm.opensearch_relation_handler._on_relation_broken(
            event
        )
        self.assertEqual(
            harness.model.unit.status,
            MaintenanceStatus("Restarting Ranger plugin"),
        )
        container = harness.model.unit.get_container("trino")
        self.assertFalse(container.exists("/opensearch.crt"))
        ranger_config = container.pull(
            "/usr/lib/ranger/install.properties"
        ).read()
        self.assertFalse(
            "XAAUDIT.ELASTICSEARCH.USER=testuser" in ranger_config
        )
        container.get_check = mock.Mock(status="up")
        container.get_check.return_value.status = CheckStatus.UP
        harness.charm.on.update_status.emit()
        self.assertEqual(
            harness.model.unit.status, ActiveStatus("Status check: UP")
        )


def simulate_lifecycle(harness):
    """Simulate a healthy charm life-cycle.

    Args:
        harness: ops.testing.Harness object used to simulate charm lifecycle.

    Returns:
        rel_id: Ranger relation id.
    """
    # Simulate peer relation readiness.
    harness.add_relation("peer", "trino")

    # Simulate pebble readiness.
    container = harness.model.unit.get_container("trino")
    harness.handle_exec("trino", ["htpasswd"], result=0)
    harness.handle_exec(
        "trino", ["/bin/sh"], result="/usr/lib/jvm/java-21-openjdk-amd64/"
    )
    harness.charm.on.trino_pebble_ready.emit(container)

    # Add worker and coordinator relation
    harness.handle_exec("trino", ["keytool"], result=0)
    harness.update_config({"catalog-config": TEST_CATALOG_CONFIG})
    harness.add_relation("trino-coordinator", "trino-k8s-worker")

    rel_id = harness.add_relation("policy", "trino-k8s")
    harness.add_relation_unit(rel_id, "trino-k8s/0")

    data = {harness.charm.app: {}}
    event = make_relation_event("ranger-k8s", rel_id, data)
    harness.handle_exec("trino", ["bash"], result=0)
    harness.charm.policy._on_relation_created(event)
    return rel_id


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
                {"data": data, "id": rel_id},
            ),
        },
    )
