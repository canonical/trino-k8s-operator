#!/usr/bin/env python3
# Copyright 2023 Canonical Ltd.
# See LICENSE file for licensing details.

"""Trino charm integration tests for catalog config updates."""

import logging

import pytest
from conftest import deploy  # noqa: F401, pylint: disable=W0611
from helpers import (
    APP_NAME,
    TRINO_USER,
    WORKER_NAME,
    add_juju_secret,
    create_catalog_config,
    get_catalogs,
    update_catalog_config,
)
from pytest_operator.plugin import OpsTest

logger = logging.getLogger(__name__)


@pytest.mark.abort_on_fail
@pytest.mark.usefixtures("deploy")
class TestCatalogUpdates:
    """Integration tests for catalog changes in Trino charm."""

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
        await update_catalog_config(
            ops_test, updated_catalog_config, TRINO_USER
        )

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
