#!/usr/bin/env python3
# Copyright 2023 Canonical Ltd.
# See LICENSE file for licensing details.

"""Temporal charm integration tests."""

import logging
import pytest
import requests
from conftest import deploy  # noqa: F401, pylint: disable=W0611
from helpers import (
    APP_NAME,
    get_unit_url,
)
import asyncio
from pytest_operator.plugin import OpsTest
from trino_client.show_catalogs import show_catalogs

logger = logging.getLogger(__name__)


@pytest.mark.abort_on_fail
@pytest.mark.usefixtures("deploy")
class TestDeployment:
    """Integration tests for Temporal charm."""

    async def test_trino_ui(self, ops_test: OpsTest):
        """Perform GET request on the Trino UI host."""
        url = await get_unit_url(ops_test, application=APP_NAME, unit=0, port=8443)
        logger.info("curling app address: %s", url)

        response = requests.get(url, timeout=300, verify=False)
        assert response.status_code == 200

    async def test_basic_client(self, ops_test: OpsTest):
        """Connects a client and executes a basic SQL query."""
        status = await ops_test.model.get_status() # noqa: F821
        address = status["applications"][APP_NAME]["units"][f"{APP_NAME}/{0}"]["address"]
        logger.info("executing query on app address: %s", address)
        catalogs = await show_catalogs(address)
        logging.info(catalogs)
        assert catalogs
