#!/usr/bin/env python3
# Copyright 2023 Canonical Ltd.
# See LICENSE file for licensing details.

"""Trino charm integration tests."""

import logging
from pathlib import Path

import pytest
import pytest_asyncio
from conftest import deploy  # noqa: F401, pylint: disable=W0611
from helpers import (
    APP_NAME,
    TRINO_USER,
    add_juju_secret,
    create_catalog_config,
    curl_unit_ip,
    get_catalogs,
    simulate_crash_and_restart,
    update_catalog_config,
)
from pytest import FixtureRequest
from pytest_operator.plugin import OpsTest

logger = logging.getLogger(__name__)


@pytest.fixture(scope="module", name="charm_image")
def charm_image_fixture(request: FixtureRequest) -> str:
    """The OCI image for charm."""
    charm_image = request.config.getoption("--trino-k8s-operator-image")
    assert (
        charm_image
    ), "--trino-k8s-operator-image argument is required which should contain the name of the OCI image."
    return charm_image


@pytest_asyncio.fixture(scope="module", name="charm")
async def charm_fixture(
    request: FixtureRequest, ops_test: OpsTest
) -> str | Path:
    """Fetch the path to charm."""
    charms = request.config.getoption("--charm-file")
    if not charms:
        charm = await ops_test.build_charm(".")
        assert charm, "Charm not built"
        return charm
    return charms[0]


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
        bigquery_secret_id = await add_juju_secret(ops_test, "bigquery")
        gsheet_secret_id = await add_juju_secret(ops_test, "gsheets")

        for app in ["trino-k8s", "trino-k8s-worker"]:
            await ops_test.model.grant_secret("postgresql-secret", app)
            await ops_test.model.grant_secret("mysql-secret", app)
            await ops_test.model.grant_secret("bigquery-secret", app)
            await ops_test.model.grant_secret("gsheets-secret", app)

        catalog_config = await create_catalog_config(
            postgresql_secret_id,
            mysql_secret_id,
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
        assert "bigquery" in str(catalogs)
        assert "gsheets-1" in str(catalogs)

        updated_catalog_config = await create_catalog_config(
            postgresql_secret_id,
            mysql_secret_id,
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
