#!/usr/bin/env python3
# Copyright 2023 Canonical Ltd.
# See LICENSE file for licensing details.

"""Trino policy integration tests."""

import logging

import time
import pytest
import requests
import pytest_asyncio
from conftest import deploy  # noqa: F401, pylint: disable=W0611
from helpers import (
    APP_NAME,
    METADATA,
    CONN_CONFIG,
    CONN_NAME,
    get_catalogs,
    RANGER_NAME,
    POSTGRES_NAME,
    GROUP_MANAGEMENT,
    create_group_policy,
    get_unit_url,
)
from pytest_operator.plugin import OpsTest

logger = logging.getLogger(__name__)


@pytest.mark.skip_if_deployed
@pytest_asyncio.fixture(name="deploy", scope="module")
async def deploy(ops_test: OpsTest):
    """Add Ranger relation and apply group configuration."""
    charm = await ops_test.build_charm(".")
    resources = {
        "trino-image": METADATA["resources"]["trino-image"]["upstream-source"]
    }
    trino_config = {"charm-function": "all"}
    await ops_test.model.deploy(
        charm,
        resources=resources,
        application_name=APP_NAME,
        num_units=1,
        config=trino_config,
    )

    await ops_test.model.deploy(POSTGRES_NAME, channel="14", trust=True)
    await ops_test.model.deploy(RANGER_NAME, channel="edge")
    await ops_test.model.wait_for_idle(
        apps=[POSTGRES_NAME, APP_NAME],
        status="active",
        raise_on_blocked=False,
        timeout=1200,
    )
    await ops_test.model.wait_for_idle(
        apps=[RANGER_NAME],
        status="blocked",
        raise_on_blocked=False,
        timeout=1200,
    )
    await ops_test.model.integrate(RANGER_NAME, POSTGRES_NAME)
    await ops_test.model.set_config({"update-status-hook-interval": "1m"})
    await ops_test.model.wait_for_idle(
        apps=[POSTGRES_NAME, RANGER_NAME],
        status="active",
        raise_on_blocked=False,
        timeout=1200,
    )
    logging.info("integrating trino and ranger")
    await ops_test.model.integrate(RANGER_NAME, APP_NAME)
    await ops_test.model.wait_for_idle(
        apps=[APP_NAME, RANGER_NAME],
        status="active",
        raise_on_blocked=False,
        timeout=1200,
    )

    logging.info("updating config")
    app: Application = ops_test.model.applications.get("ranger-k8s")
    app.set_config({"user-group-configuration": GROUP_MANAGEMENT})
    await ops_test.model.wait_for_idle(
        apps=[APP_NAME, RANGER_NAME],
        status="active",
        raise_on_blocked=False,
        timeout=1200,
    )


@pytest.mark.abort_on_fail
@pytest.mark.usefixtures("deploy")
class TestPolicy:
    """Integration test for policy relation."""

    async def test_group_policy(self, ops_test: OpsTest):
        """Connects a client and executes a basic SQL query."""
        url = await get_unit_url(
            ops_test, application=RANGER_NAME, unit=0, port=6080
        )
        logging.info(f"creating test policies for {url}")
        await create_group_policy(ops_test, url)

        time.sleep(10)
        catalogs = await get_catalogs(ops_test, "user1")
        logging.info(f"trino catalogs: {catalogs}")
        assert catalogs
        catalogs = await get_catalogs(ops_test, "user2")
        assert not catalogs
