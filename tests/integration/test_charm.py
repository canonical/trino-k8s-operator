#!/usr/bin/env python3
# Copyright 2023 Canonical Ltd.
# See LICENSE file for licensing details.

"""Trino charm integration tests."""

import logging

import pytest
from conftest import deploy  # noqa: F401, pylint: disable=W0611
from helpers import (
    APP_NAME,
    CATALOG_CONFIG,
    EXAMPLE_CATALOG_NAME,
    TEMP_CATALOG_CONFIG,
    TEMP_CATALOG_NAME,
    TRINO_USER,
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

    async def test_add_catalog(self, ops_test: OpsTest):
        """Adds a PostgreSQL connector and confirms database added."""
        catalogs = await update_catalog_config(
            ops_test, CATALOG_CONFIG, TRINO_USER
        )

        # Verify that both catalogs have been added.
        assert TEMP_CATALOG_NAME in str(catalogs)
        assert EXAMPLE_CATALOG_NAME in str(catalogs)

    async def test_remove_catalog(self, ops_test: OpsTest):
        """Removes an existing connector confirms database removed."""
        catalogs = await update_catalog_config(
            ops_test, TEMP_CATALOG_CONFIG, TRINO_USER
        )

        # Verify that only the example catalog has been removed.
        assert TEMP_CATALOG_NAME in str(catalogs)
        assert EXAMPLE_CATALOG_NAME not in str(catalogs)

    async def test_simulate_crash(self, ops_test: OpsTest):
        """Simulate the crash of the Trino coordinator charm.

        Args:
            ops_test: PyTest object.
        """
        await simulate_crash_and_restart(ops_test)
        response = await curl_unit_ip(ops_test)
        assert response.status_code == 200

        catalogs = await get_catalogs(ops_test, TRINO_USER, APP_NAME)
        assert TEMP_CATALOG_NAME in str(catalogs)
