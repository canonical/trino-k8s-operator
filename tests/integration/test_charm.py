#!/usr/bin/env python3
# Copyright 2023 Canonical Ltd.
# See LICENSE file for licensing details.

"""Trino charm integration tests."""

import logging

import pytest
from conftest import deploy  # noqa: F401, pylint: disable=W0611
from helpers import (
    APP_NAME,
    TRINO_USER,
    WORKER_NAME,
    add_juju_secret,
    create_catalog_config,
    curl_unit_ip,
    get_catalogs,
    simulate_crash_and_restart,
    update_catalog_config,
)
from pytest_operator.plugin import OpsTest

logger = logging.getLogger(__name__)


@pytest.mark.abort_on_fail
@pytest.mark.usefixtures("deploy")
class TestDeployment:
    """Integration tests for Trino charm."""

    async def test_trino_ui(self, ops_test: OpsTest):
        """Perform GET request on the Trino UI host."""
        response = await curl_unit_ip(ops_test)
        assert response.status_code == 200

    async def test_basic_client(self, ops_test: OpsTest):
        """Connects a client and executes a basic SQL query."""
        catalogs = await get_catalogs(ops_test, TRINO_USER, APP_NAME)
        logging.info(f"Found catalogs: {catalogs}")
        assert catalogs

    async def test_catalog_config(self, ops_test: OpsTest):
        """Adds a PostgreSQL and BigQuery connector and asserts catalogs added."""
        postgresql_secret_id = await add_juju_secret(ops_test, "postgresql")
        mysql_secret_id = await add_juju_secret(ops_test, "mysql")
        redshift_secret_id = await add_juju_secret(ops_test, "redshift")
        bigquery_secret_id = await add_juju_secret(ops_test, "bigquery")
        gsheet_secret_id = await add_juju_secret(ops_test, "gsheets")

        for app in ["trino-k8s", "trino-k8s-worker"]:
            await ops_test.model.grant_secret("postgresql-secret", app)
            await ops_test.model.grant_secret("mysql-secret", app)
            await ops_test.model.grant_secret("redshift-secret", app)
            await ops_test.model.grant_secret("bigquery-secret", app)
            await ops_test.model.grant_secret("gsheets-secret", app)

        catalog_config = await create_catalog_config(
            postgresql_secret_id,
            mysql_secret_id,
            redshift_secret_id,
            bigquery_secret_id,
            gsheet_secret_id,
            True,
        )
        catalogs = await update_catalog_config(
            ops_test, catalog_config, TRINO_USER
        )

        # Verify that both catalogs have been added.
        assert "postgresql-1" in str(catalogs)
        assert "mysql" in str(catalogs)
        assert "redshift" in str(catalogs)
        assert "bigquery" in str(catalogs)
        assert "gsheets-1" in str(catalogs)

        updated_catalog_config = await create_catalog_config(
            postgresql_secret_id,
            mysql_secret_id,
            redshift_secret_id,
            bigquery_secret_id,
            gsheet_secret_id,
            False,
        )

        catalogs = await update_catalog_config(
            ops_test, updated_catalog_config, TRINO_USER
        )

        # Verify that only the bigquery catalog has been removed.
        assert "postgresql-1" in str(catalogs)
        assert "mysql" in str(catalogs)
        assert "redshift" in str(catalogs)
        assert "bigquery" not in str(catalogs)
        assert "gsheets-1" in str(catalogs)

    async def test_simulate_crash(
        self, ops_test: OpsTest, charm: str, charm_image: str
    ):
        """Simulate the crash of the Trino coordinator charm.

        Args:
            ops_test: PyTest object.
            charm: charm path.
            charm_image: path to rock image to be used.
        """
        await simulate_crash_and_restart(ops_test, charm, charm_image)
        response = await curl_unit_ip(ops_test)
        assert response.status_code == 200

        catalogs = await get_catalogs(ops_test, TRINO_USER, APP_NAME)
        assert catalogs

    async def test_trino_default_policy(self, ops_test: OpsTest):
        """Update the config and verify no catalog access."""
        await ops_test.model.applications[APP_NAME].set_config(
            {"acl-mode-default": "none"}
        )

        async with ops_test.fast_forward():
            await ops_test.model.wait_for_idle(
                apps=[APP_NAME], status="active", timeout=600
            )
        catalogs = await get_catalogs(ops_test, TRINO_USER, APP_NAME)
        logging.info(f"Found catalogs: {catalogs}")
        assert not catalogs

    async def test_catalog_config_updates_after_relation_break(
        self, ops_test: OpsTest
    ):
        """Test that the worker picks up updated catalog-config after relation re-establishment."""
        # Step 1: create and apply catalog config with BigQuery
        postgresql_secret_id = await add_juju_secret(ops_test, "postgresql")
        mysql_secret_id = await add_juju_secret(ops_test, "mysql")
        redshift_secret_id = await add_juju_secret(ops_test, "redshift")
        bigquery_secret_id = await add_juju_secret(ops_test, "bigquery")
        gsheet_secret_id = await add_juju_secret(ops_test, "gsheets")

        for app in ["trino-k8s", "trino-k8s-worker"]:
            await ops_test.model.grant_secret("postgresql-secret", app)
            await ops_test.model.grant_secret("mysql-secret", app)
            await ops_test.model.grant_secret("redshift-secret", app)
            await ops_test.model.grant_secret("bigquery-secret", app)
            await ops_test.model.grant_secret("gsheets-secret", app)

        catalog_config = await create_catalog_config(
            postgresql_secret_id,
            mysql_secret_id,
            redshift_secret_id,
            bigquery_secret_id,
            gsheet_secret_id,
            True,  # include_bigquery
        )
        await update_catalog_config(ops_test, catalog_config, TRINO_USER)

        # Assert that bigquery is present
        catalogs = await get_catalogs(ops_test, TRINO_USER, APP_NAME)
        assert "bigquery" in str(catalogs)

        # Step 2: Remove relation between coordinator and worker
        await ops_test.model.applications[APP_NAME].remove_relation(
            f"{APP_NAME}:trino-coordinator", f"{WORKER_NAME}:trino-worker"
        )
        await ops_test.model.wait_for_idle(
            apps=[APP_NAME, WORKER_NAME], timeout=600
        )

        # Step 3: Update catalog config to remove bigquery (DO NOT use config-changed after this)
        updated_catalog_config = await create_catalog_config(
            postgresql_secret_id,
            mysql_secret_id,
            redshift_secret_id,
            bigquery_secret_id,
            gsheet_secret_id,
            False,  # now exclude bigquery
        )
        await update_catalog_config(ops_test, updated_catalog_config, TRINO_USER)

        # Step 4: Re-establish relation
        await ops_test.model.integrate(
            f"{APP_NAME}:trino-coordinator", f"{WORKER_NAME}:trino-worker"
        )
        async with ops_test.fast_forward():
            await ops_test.model.wait_for_idle(
                apps=[APP_NAME, WORKER_NAME], status="active", timeout=900
            )

        # Step 5: Check the catalogs (expect bigquery to be removed)
        catalogs = await get_catalogs(ops_test, TRINO_USER, APP_NAME)
        assert "bigquery" not in str(
            catalogs
        ), "BigQuery should be removed after relation re-establishment"
        assert "postgresql-1" in str(catalogs)
        assert "mysql" in str(catalogs)
        assert "redshift" in str(catalogs)
        assert "gsheets-1" in str(catalogs)
