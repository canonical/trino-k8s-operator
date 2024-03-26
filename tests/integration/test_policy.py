#!/usr/bin/env python3
# Copyright 2023 Canonical Ltd.
# See LICENSE file for licensing details.

"""Trino policy integration tests."""

import logging
import time

import pytest
import pytest_asyncio
from helpers import (
    APP_NAME,
    METADATA,
    POSTGRES_NAME,
    RANGER_NAME,
    USER_WITH_ACCESS,
    USER_WITHOUT_ACCESS,
    create_policy,
    get_catalogs,
    get_unit_url,
)
from pytest_operator.plugin import OpsTest

logger = logging.getLogger(__name__)


@pytest.mark.skip_if_deployed
@pytest_asyncio.fixture(name="deploy-policy", scope="module")
async def test_policy_enforcement(ops_test: OpsTest):
    """Add Ranger relation and apply group configuration."""
    charm = await ops_test.build_charm(".")
    resources = {
        "trino-image": METADATA["resources"]["trino-image"]["upstream-source"]
    }
    trino_config = {
        "ranger-service-name": "trino-service",
        "charm-function": "all",
    }
    await ops_test.model.deploy(
        charm,
        resources=resources,
        application_name=APP_NAME,
        num_units=1,
        config=trino_config,
    )
    # Deploy and prepare Ranger admin.
    await ops_test.model.deploy(POSTGRES_NAME, channel="14", trust=True)
    await ops_test.model.deploy(RANGER_NAME, channel="edge")

    async with ops_test.fast_forward():
        await ops_test.model.wait_for_idle(
            apps=[POSTGRES_NAME],
            status="active",
            raise_on_blocked=False,
            timeout=2000,
        )

    await ops_test.model.wait_for_idle(
        apps=[RANGER_NAME],
        status="blocked",
        raise_on_blocked=False,
        timeout=2000,
    )
    await ops_test.model.integrate(RANGER_NAME, POSTGRES_NAME)

    await ops_test.model.wait_for_idle(
        apps=[POSTGRES_NAME, RANGER_NAME, APP_NAME],
        status="active",
        raise_on_blocked=False,
        timeout=2000,
    )

    # Integrate Trino and Ranger.
    await ops_test.model.integrate(RANGER_NAME, APP_NAME)
    await ops_test.model.wait_for_idle(
        apps=[APP_NAME, RANGER_NAME],
        status="active",
        raise_on_blocked=False,
        timeout=1200,
    )


@pytest.mark.abort_on_fail
@pytest.mark.usefixtures("deploy-policy")
class TestPolicyManager:
    """Integration test for Ranger policy enforcement."""

    async def test_policy_enforcement(self, ops_test):
        """Test Ranger integration."""
        # Test policy implementation.
        url = await get_unit_url(
            ops_test, application=RANGER_NAME, unit=0, port=6080
        )
        logging.info(f"creating test policies for {url}")
        await create_policy(ops_test, url)

        # wait 30 seconds for the policy to be synced.
        time.sleep(30)

        catalogs = await get_catalogs(ops_test, USER_WITH_ACCESS, APP_NAME)
        assert catalogs == [["system"]]
        logger.info(f"{USER_WITH_ACCESS} can access {catalogs}.")
        catalogs = await get_catalogs(ops_test, USER_WITHOUT_ACCESS, APP_NAME)
        logger.info(f"{USER_WITHOUT_ACCESS}, can access {catalogs}.")
        assert catalogs == []
