#!/usr/bin/env python3
# Copyright 2023 Canonical Ltd.
# See LICENSE file for licensing details.

"""Trino policy integration tests."""

import logging
import time
from pathlib import Path

import pytest
import pytest_asyncio
from helpers import (
    APP_NAME,
    POSTGRES_NAME,
    RANGER_NAME,
    USER_WITH_ACCESS,
    USER_WITHOUT_ACCESS,
    create_user,
    get_catalogs,
    get_unit_url,
    update_policies,
)
from pytest import FixtureRequest
from pytest_operator.plugin import OpsTest

logger = logging.getLogger(__name__)


@pytest.fixture(scope="module", name="charm_image")
def charm_image_fixture(request: FixtureRequest) -> str:
    """The OCI image for charm."""
    charm_image = request.config.getoption("--trino-image")
    assert (
        charm_image
    ), "--trino-image argument is required which should contain the name of the OCI image."
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


TRINO_CONFIG = {
    "ranger-service-name": "trino-service",
    "charm-function": "all",
}


@pytest.mark.skip_if_deployed
@pytest_asyncio.fixture(name="deploy-policy", scope="module")
async def deploy_policy_engine(ops_test: OpsTest):
    """Add Ranger relation and apply group configuration."""
    await ops_test.model.deploy(POSTGRES_NAME, channel="14", trust=True)
    await ops_test.model.deploy(
        RANGER_NAME, channel="edge", revision=33, trust=True
    )

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
        logger.info("Integrating Ranger and PostgreSQL.")
        await ops_test.model.integrate(RANGER_NAME, POSTGRES_NAME)

        await ops_test.model.wait_for_idle(
            apps=[POSTGRES_NAME, RANGER_NAME],
            status="active",
            raise_on_blocked=False,
            timeout=2000,
        )


@pytest.mark.abort_on_fail
@pytest.mark.usefixtures("deploy-policy")
class TestPolicyManager:
    """Integration test for Ranger policy enforcement."""

    async def test_policy_enforcement(
        self, ops_test, charm: str, charm_image: str
    ):
        """Test Ranger integration."""
        await ops_test.model.deploy(
            charm,
            resources={"trino-image": charm_image},
            application_name=APP_NAME,
            config=TRINO_CONFIG,
            trust=True,
        )

        async with ops_test.fast_forward():
            await ops_test.model.wait_for_idle(
                apps=[APP_NAME],
                status="active",
                raise_on_blocked=False,
                timeout=2000,
            )

        logger.info("Creating test user.")
        url = await get_unit_url(
            ops_test, application=RANGER_NAME, unit=0, port=6080
        )
        await create_user(url)

        # Integrate Trino and Ranger.
        logger.info("Integrating Trino and Ranger.")
        await ops_test.model.integrate(RANGER_NAME, APP_NAME)
        await ops_test.model.wait_for_idle(
            apps=[APP_NAME, RANGER_NAME],
            status="active",
            raise_on_blocked=False,
            timeout=2000,
        )
        logging.info("update default policies to authorize the new user")
        await update_policies(url)

        time.sleep(30)  # wait 30 seconds for the policy to be synced.

        catalogs = await get_catalogs(ops_test, USER_WITH_ACCESS, APP_NAME)
        assert catalogs == [["system"]]
        logger.info(f"{USER_WITH_ACCESS} can access {catalogs}.")

        catalogs = await get_catalogs(ops_test, USER_WITHOUT_ACCESS, APP_NAME)
        logger.info(f"{USER_WITHOUT_ACCESS} can not access {catalogs}.")
        assert catalogs == []
