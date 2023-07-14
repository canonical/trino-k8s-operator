#!/usr/bin/env python3
# Copyright 2023 Canonical Ltd.
# See LICENSE file for licensing details.

"""Trino charm integration tests."""

import logging

import pytest
import requests
from conftest import deploy  # noqa: F401, pylint: disable=W0611
from helpers import (APP_NAME, CONN_NAME, get_catalogs, get_unit_url,
                     run_connector_action)
from pytest_operator.plugin import OpsTest

logger = logging.getLogger(__name__)


@pytest.mark.abort_on_fail
@pytest.mark.usefixtures("deploy")
class TestDeployment:
    """Integration tests for Trino charm."""

    async def test_trino_ui(self, ops_test: OpsTest):
        """Perform GET request on the Trino UI host."""
        url = await get_unit_url(ops_test, application=APP_NAME, unit=0, port=8443)
        logger.info("curling app address: %s", url)

        response = requests.get(url, timeout=300, verify=False)
        assert response.status_code == 200

    async def test_basic_client(self, ops_test: OpsTest):
        """Connects a client and executes a basic SQL query."""
        catalogs = await get_catalogs(ops_test)
        logging.info(f"trino catalogs: {catalogs}")
        assert catalogs

    async def test_add_connector_action(self, ops_test: OpsTest):
        """Adds a PostgreSQL connector and confirms database added."""
        catalogs = await run_connector_action(ops_test, "add-connector")
        assert [CONN_NAME] in catalogs

    async def test_remove_connector_action(self, ops_test: OpsTest):
        """Removes an existing connector confirms database removed."""
        catalogs = await run_connector_action(ops_test, "remove-connector")
        assert [CONN_NAME] not in catalogs
