# Copyright 2023 Canonical Ltd.
# See LICENSE file for licensing details.
#
# Learn more about testing at: https://juju.is/docs/sdk/testing


"""Trino charm unit tests."""

# pylint:disable=protected-access

import logging
from unittest import TestCase, mock

from ops.model import MaintenanceStatus
from ops.testing import Harness

from charm import TrinoK8SCharm
from tests.unit.helpers import (
    BIGQUERY_CATALOG_PATH,
    POSTGRESQL_1_CATALOG_PATH,
    POSTGRESQL_2_CATALOG_PATH,
    UPDATED_JVM_OPTIONS,
    USER_JVM_STRING,
    create_added_catalog_config,
    create_catalog_config,
    make_relation_event,
    simulate_lifecycle_coordinator,
    simulate_lifecycle_worker,
)

logger = logging.getLogger(__name__)


class TestCatalogConfigFreshness(TestCase):
    """Unit tests for Trino charm catalog freshness."""

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

    def test_config_changed(self):
        """The pebble plan changes according to config changes."""
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
            mysql_secret_id,
            redshift_secret_id,
            bigquery_secret_id,
            gsheets_secret_id,
        ) = simulate_lifecycle_coordinator(harness)

        catalog_config = create_added_catalog_config(
            postgresql_secret_id,
            mysql_secret_id,
            redshift_secret_id,
            bigquery_secret_id,
            gsheets_secret_id,
        )

        # Update the config.
        self.harness.update_config({"catalog-config": catalog_config})

        # Validate catalog.properties file created.
        container = harness.model.unit.get_container("trino")
        self.assertTrue(container.exists(POSTGRESQL_2_CATALOG_PATH))
        self.assertTrue(container.exists(BIGQUERY_CATALOG_PATH))

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

    def test_worker_fetches_latest_catalog_on_relation_change(self):
        """Test the worker catalogs change after the config and relation changes simultaneously."""
        harness = self.harness
        (
            _,
            event,
            postgresql_secret_id,
            mysql_secret_id,
            redshift_secret_id,
            bigquery_secret_id,
            gsheets_secret_id,
        ) = simulate_lifecycle_worker(harness)

        old_catalog = create_catalog_config(
            postgresql_secret_id,
            mysql_secret_id,
            redshift_secret_id,
            bigquery_secret_id,
            gsheets_secret_id,
        )

        extended_catalog_config = create_added_catalog_config(
            postgresql_secret_id,
            mysql_secret_id,
            redshift_secret_id,
            bigquery_secret_id,
            gsheets_secret_id,
        )
        new_data = dict(event.relation.data)
        new_data["trino-worker"].update({"catalogs": extended_catalog_config})

        new_event = make_relation_event(
            "trino-worker", event.relation.id, new_data
        )
        harness.charm.trino_worker._on_relation_changed(new_event)

        self.assertEqual(
            harness.charm.trino_worker.charm.state.catalog_config,
            extended_catalog_config,
            "Catalog should be updated to the latest version when relation is changed.",
        )

        self.assertNotEqual(
            harness.charm.trino_worker.charm.state.catalog_config,
            old_catalog,
            "Stale catalog should not be used after it is updated.",
        )
