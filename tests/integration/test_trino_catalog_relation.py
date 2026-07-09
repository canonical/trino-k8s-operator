# Copyright 2023 Canonical Ltd.
# See LICENSE file for licensing details.

"""Integration tests for trino-catalog relation."""

import ast
import logging
from pathlib import Path

import jubilant
import pytest
from conftest import pack_charm
from helpers import (
    APP_NAME,
    TRAEFIK_NAME,
    add_juju_secret,
    create_catalog_config,
    get_secret_id_by_label,
    get_unit,
    wait_for_app_gone,
    wait_for_apps,
)

from literals import TRINO_PORTS

logger = logging.getLogger(__name__)

REQUIRER_APP = "requirer-app"
REQUIRER_CHARM_PATH = "tests/integration/trino_catalog_requirer_charm"


@pytest.fixture(name="deploy-requirer", scope="module")
def deploy_requirer(juju: jubilant.Juju):
    """Deploy the requirer charm once for all tests in this module."""
    # Build the requirer charm
    requirer_charm = pack_charm(Path(REQUIRER_CHARM_PATH))

    # Deploy requirer charm
    juju.deploy(requirer_charm, REQUIRER_APP, num_units=1)
    wait_for_apps(juju, [REQUIRER_APP], status="blocked", timeout=1000)


@pytest.fixture(name="deploy-trino", scope="module")
def deploy_trino(juju: jubilant.Juju, charm: str, charm_image: str):
    """Deploy the trino charm once for all tests in this module."""
    # Deploy trino charm
    juju.deploy(
        charm,
        APP_NAME,
        resources={"trino-image": charm_image},
        config={"charm-function": "all"},
        trust=True,
    )
    wait_for_apps(juju, [APP_NAME], status="active", timeout=1000)


@pytest.mark.incremental
@pytest.mark.usefixtures("deploy-trino", "deploy-requirer")
class TestTrinoCatalogRelation:
    """Integration tests for the trino-catalog relation."""

    def test_01_requirer_deployment(self, juju: jubilant.Juju):
        """Test that the requirer charm has been deployed."""
        # Verify requirer is blocked waiting for relation
        assert get_unit(juju, REQUIRER_APP).workload_status.current == "blocked"

    def test_02_trino_catalog_relation_created(self, juju: jubilant.Juju):
        """Test creating the trino-catalog relation."""
        # Add the relation
        juju.integrate(f"{APP_NAME}:trino-catalog", f"{REQUIRER_APP}:trino-catalog")
        wait_for_apps(juju, [APP_NAME, REQUIRER_APP], status="active", timeout=1000)

        # Verify relation exists
        trino_catalog_relations = juju.status().apps[APP_NAME].relations.get("trino-catalog", [])
        assert len(trino_catalog_relations) > 0

    def test_03_auto_generated_credentials(self, juju: jubilant.Juju):
        """Test that per-relation credentials are auto-generated."""
        action = juju.run(f"{REQUIRER_APP}/0", "get-relation-data")
        assert action.status == "completed"

        # Verify auto-generated username follows pattern app-{app_name}-{relation_id}
        result_username = action.results.get("trino-username")
        assert result_username.startswith(f"app-{REQUIRER_APP}-"), (
            f"Expected username starting with 'app-{REQUIRER_APP}-', got '{result_username}'"
        )

        # Verify password is a non-empty random string
        result_password = action.results.get("trino-password")
        assert result_password and len(result_password) > 0, (
            "Expected non-empty auto-generated password"
        )

        # Verify internal URL (no ingress relation at this point)
        result_url = action.results.get("trino-url")
        expected_internal_url = f"{APP_NAME}.{juju.model}.svc.cluster.local:{TRINO_PORTS['HTTP']}"
        assert result_url == expected_internal_url, (
            f"Expected internal URL '{expected_internal_url}', got '{result_url}'"
        )

        logger.info(
            "Verified auto-generated credentials: username='%s', URL='%s'",
            result_username,
            result_url,
        )

    def test_04_trino_catalog_relation_set_catalogs(self, juju: jubilant.Juju):
        """Test that catalog-config changes propagate to the requirer."""
        # Create catalog secrets
        postgresql_secret_id = add_juju_secret(juju, "postgresql")
        mysql_secret_id = add_juju_secret(juju, "mysql")
        redshift_secret_id = add_juju_secret(juju, "redshift")
        bigquery_secret_id = add_juju_secret(juju, "bigquery")
        gsheets_secret_id = add_juju_secret(juju, "gsheets")

        # Grant secrets to Trino
        juju.grant_secret("postgresql-secret", APP_NAME)
        juju.grant_secret("mysql-secret", APP_NAME)
        juju.grant_secret("redshift-secret", APP_NAME)
        juju.grant_secret("bigquery-secret", APP_NAME)
        juju.grant_secret("gsheets-secret", APP_NAME)

        # Create catalog config
        catalog_config = create_catalog_config(
            postgresql_secret_id,
            mysql_secret_id,
            redshift_secret_id,
            bigquery_secret_id,
            gsheets_secret_id,
            include_bigquery=True,
        )

        # Update Trino with catalog config
        juju.config(APP_NAME, {"catalog-config": catalog_config})
        wait_for_apps(juju, [APP_NAME, REQUIRER_APP], status="active", timeout=1000)

        # Verify the requirer received the catalogs
        action = juju.run(f"{REQUIRER_APP}/0", "get-relation-data")
        assert action.status == "completed"

        # Parse the catalogs list from string representation
        catalogs_str = action.results.get("trino-catalogs", "[]")
        catalogs = ast.literal_eval(catalogs_str)
        catalogs_count = len(catalogs)

        assert catalogs_count == 5, f"Expected 5 catalogs, got {catalogs_count}"

        # Verify catalog names are present
        catalog_names = {cat["name"] for cat in catalogs}
        expected_catalogs = {
            "postgresql-1",
            "mysql",
            "redshift",
            "bigquery",
            "gsheets-1",
        }
        assert expected_catalogs.issubset(catalog_names), (
            f"Expected catalogs {expected_catalogs}, got {catalog_names}"
        )

        logger.info(
            "Verified requirer received %s catalogs: %s",
            catalogs_count,
            catalog_names,
        )

    def test_05_catalog_exclusions(self, juju: jubilant.Juju):
        """Test that catalog-exclusions filters catalogs for a specific requirer."""
        # Exclude one catalog for the requirer app
        exclusion_config = f"{REQUIRER_APP}:\n  - postgresql-1"

        juju.config(APP_NAME, {"catalog-exclusions": exclusion_config})
        wait_for_apps(juju, [APP_NAME], status="active", timeout=1000)

        # Verify the requirer receives 4 catalogs (postgresql-1 excluded)
        action = juju.run(f"{REQUIRER_APP}/0", "get-relation-data")
        assert action.status == "completed"

        catalogs_str = action.results.get("trino-catalogs", "[]")
        catalogs = ast.literal_eval(catalogs_str)
        catalog_names = {cat["name"] for cat in catalogs}

        assert "postgresql-1" not in catalog_names, (
            f"postgresql-1 should be excluded, but found in {catalog_names}"
        )
        assert len(catalogs) == 4, f"Expected 4 catalogs, got {len(catalogs)}"

        # Reset exclusions and verify all catalogs are restored
        juju.config(APP_NAME, reset=["catalog-exclusions"])
        wait_for_apps(juju, [APP_NAME], status="active", timeout=1000)

        action = juju.run(f"{REQUIRER_APP}/0", "get-relation-data")
        assert action.status == "completed"

        catalogs_str = action.results.get("trino-catalogs", "[]")
        catalogs = ast.literal_eval(catalogs_str)
        catalog_names = {cat["name"] for cat in catalogs}

        assert "postgresql-1" in catalog_names, (
            f"postgresql-1 should be restored after clearing exclusions, got {catalog_names}"
        )
        assert len(catalogs) == 5, f"Expected 5 catalogs, got {len(catalogs)}"

        logger.info("Verified catalog exclusions work correctly")

    def test_06_trino_catalog_external_url_with_ingress(self, juju: jubilant.Juju):
        """Test that the requirer receives an external URL when the ingress relation is active."""
        # Deploy traefik in subdomain mode so each app gets a per-app external hostname.
        juju.deploy(TRAEFIK_NAME, config={"routing_mode": "subdomain"}, trust=True)
        wait_for_apps(juju, [TRAEFIK_NAME], status="active", timeout=1000)

        # Relate Trino to traefik.
        juju.integrate(f"{APP_NAME}:ingress", f"{TRAEFIK_NAME}:ingress")
        wait_for_apps(juju, [APP_NAME], status="active", timeout=1000)

        # Retrieve the URL advertised to the requirer.
        action = juju.run(f"{REQUIRER_APP}/0", "get-relation-data")
        assert action.status == "completed"

        result_url = action.results.get("trino-url")
        assert result_url is not None, "trino-url was not set in the relation data"

        # The URL must have switched away from the internal service address.
        internal_url = f"{APP_NAME}.{juju.model}.svc.cluster.local:{TRINO_PORTS['HTTP']}"
        assert result_url != internal_url, (
            f"Expected an external URL but still got the internal URL '{result_url}'"
        )

        # The URL must be bare host:port with no scheme or path (host-based routing).
        assert "://" not in result_url, f"URL must not contain a scheme, got '{result_url}'"
        assert "/" not in result_url, (
            f"URL must not contain a path prefix (host-based routing required), got '{result_url}'"
        )
        logger.info("Verified requirer received external Trino URL: %s", result_url)

        # Clean up: remove ingress relation.
        juju.remove_relation(f"{APP_NAME}:ingress", f"{TRAEFIK_NAME}:ingress")
        wait_for_apps(juju, [APP_NAME], status="active", timeout=1000)

    def test_07_catalog_config_propagation(self, juju: jubilant.Juju):
        """Test that catalog-config changes propagate to the requirer."""
        # Get catalog secrets
        postgresql_secret_id = get_secret_id_by_label(juju, "postgresql-secret")
        mysql_secret_id = get_secret_id_by_label(juju, "mysql-secret")
        redshift_secret_id = get_secret_id_by_label(juju, "redshift-secret")
        bigquery_secret_id = get_secret_id_by_label(juju, "bigquery-secret")
        gsheets_secret_id = get_secret_id_by_label(juju, "gsheets-secret")

        logger.info("PostgreSQL secret ID: %s", postgresql_secret_id)
        logger.info("MySQL secret ID: %s", mysql_secret_id)
        logger.info("Redshift secret ID: %s", redshift_secret_id)
        logger.info("BigQuery secret ID: %s", bigquery_secret_id)
        logger.info("GSheets secret ID: %s", gsheets_secret_id)

        # Update to remove bigquery
        catalog_config_without_bigquery = create_catalog_config(
            postgresql_secret_id,
            mysql_secret_id,
            redshift_secret_id,
            bigquery_secret_id,
            gsheets_secret_id,
            include_bigquery=False,
        )

        juju.config(APP_NAME, {"catalog-config": catalog_config_without_bigquery})
        wait_for_apps(juju, [APP_NAME], status="active", timeout=1000)

        # Verify the requirer received the updated catalogs (without bigquery)
        action = juju.run(f"{REQUIRER_APP}/0", "get-relation-data")
        assert action.status == "completed"

        # Parse the catalogs list from string representation
        catalogs_str = action.results.get("trino-catalogs", "[]")
        catalogs = ast.literal_eval(catalogs_str)
        catalogs_count = len(catalogs)

        assert catalogs_count == 4, (
            f"Expected 4 catalogs after removing bigquery, got {catalogs_count}"
        )

        # Verify bigquery is no longer present
        catalog_names = {cat["name"] for cat in catalogs}
        assert "bigquery" not in catalog_names, (
            f"bigquery should be removed, but found in {catalog_names}"
        )
        expected_catalogs = {"postgresql-1", "mysql", "redshift", "gsheets-1"}
        assert expected_catalogs.issubset(catalog_names), (
            f"Expected catalogs {expected_catalogs}, got {catalog_names}"
        )

        logger.info(
            "Verified catalog count reduced to %s after removing bigquery: %s",
            catalogs_count,
            catalog_names,
        )

    def test_08_multiple_requirers(self, juju: jubilant.Juju):
        """Test that multiple requirers each get separate auto-generated credentials."""
        # Deploy a second requirer
        requirer_charm = pack_charm(Path(REQUIRER_CHARM_PATH))
        requirer_app_2 = "second-requirer"

        juju.deploy(requirer_charm, requirer_app_2, num_units=1)
        wait_for_apps(juju, [requirer_app_2], status="blocked", timeout=1000)

        # Relate second requirer to Trino
        juju.integrate(f"{APP_NAME}:trino-catalog", f"{requirer_app_2}:trino-catalog")
        wait_for_apps(juju, [requirer_app_2], status="active", timeout=1000)

        # Verify both requirers are connected
        trino_catalog_relations = juju.status().apps[APP_NAME].relations.get("trino-catalog", [])
        # Should have at least 2 relations
        assert len(trino_catalog_relations) >= 2

        # Verify each requirer got different credentials
        action_1 = juju.run(f"{REQUIRER_APP}/0", "get-relation-data")
        action_2 = juju.run(f"{requirer_app_2}/0", "get-relation-data")

        username_1 = action_1.results.get("trino-username")
        username_2 = action_2.results.get("trino-username")

        assert username_1.startswith(f"app-{REQUIRER_APP}-")
        assert username_2.startswith(f"app-{requirer_app_2}-")
        assert username_1 != username_2, (
            f"Each requirer should get a unique username, got '{username_1}' and '{username_2}'"
        )

        logger.info(
            "Verified separate credentials: requirer1='%s', requirer2='%s'",
            username_1,
            username_2,
        )

        # Clean up second requirer
        juju.remove_application(requirer_app_2)
        wait_for_app_gone(juju, requirer_app_2)

    def test_09_relation_broken(self, juju: jubilant.Juju):
        """Test that relation can be broken cleanly."""
        # Remove the relation
        juju.remove_relation(f"{APP_NAME}:trino-catalog", f"{REQUIRER_APP}:trino-catalog")
        wait_for_apps(juju, [REQUIRER_APP], status="blocked", timeout=1000)

        # Verify relation is removed
        trino_catalog_relations = juju.status().apps[APP_NAME].relations.get("trino-catalog", [])
        assert len(trino_catalog_relations) == 0

        # Requirer should be blocked
        assert get_unit(juju, REQUIRER_APP).workload_status.current == "blocked"
