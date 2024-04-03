#!/usr/bin/env python3
# Copyright 2023 Canonical Ltd.
# See LICENSE file for licensing details.

"""Trino charm integration tests."""

import logging

import pytest
import requests
from conftest import deploy  # noqa: F401, pylint: disable=W0611
from helpers import (
    APP_NAME,
    EXAMPLE_CATALOG_CONFIG,
    EXAMPLE_CATALOG_NAME,
    TEMP_CATALOG_CONFIG,
    TEMP_CATALOG_NAME,
    TRINO_USER,
    get_catalogs,
    get_unit_url,
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
        url = await get_unit_url(
            ops_test, application=APP_NAME, unit=0, port=8080
        )
        logger.info("curling app address: %s", url)

        response = requests.get(url, timeout=300, verify=False)  # nosec
        assert response.status_code == 200

    async def test_basic_client(self, ops_test: OpsTest):
        """Connects a client and executes a basic SQL query."""
        catalogs = await get_catalogs(ops_test, TRINO_USER, APP_NAME)
        logging.info(f"trino catalogs: {catalogs}")
        assert catalogs

    async def test_add_catalog(self, ops_test: OpsTest):
        """Adds a PostgreSQL connector and confirms database added."""
        catalog_config = EXAMPLE_CATALOG_CONFIG + TEMP_CATALOG_CONFIG
        catalogs = await update_catalog_config(
            ops_test, catalog_config, TRINO_USER
        )

        # Verify that both catalogs have been added.
        assert TEMP_CATALOG_NAME in catalogs
        assert EXAMPLE_CATALOG_NAME in catalogs

    async def test_remove_catalog(self, ops_test: OpsTest):
        """Removes an existing connector confirms database removed."""
        catalogs = await update_catalog_config(
            ops_test, EXAMPLE_CATALOG_CONFIG, TRINO_USER
        )

        # Verigy that only the temp catalog has been removed.
        assert TEMP_CATALOG_NAME not in catalogs
        assert EXAMPLE_CATALOG_NAME in catalogs
