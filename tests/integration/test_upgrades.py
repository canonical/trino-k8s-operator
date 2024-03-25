# Copyright 2023 Canonical Ltd.
# See LICENSE file for licensing details.

"""Trino charm upgrades integration tests."""

import logging

import pytest
import pytest_asyncio
import requests
import yaml
from helpers import APP_NAME, METADATA, SECURE_PWD, get_unit_url
from pytest_operator.plugin import OpsTest

logger = logging.getLogger(__name__)


@pytest.mark.skip_if_deployed
@pytest_asyncio.fixture(name="deploy-upgrade", scope="module")
async def deploy(ops_test: OpsTest):
    """Deploy the app."""
    # Deploy trino and nginx charms
    trino_config = {"trino-password": SECURE_PWD}
    await ops_test.model.deploy(APP_NAME, channel="edge", config=trino_config)

    async with ops_test.fast_forward():
        await ops_test.model.wait_for_idle(
            apps=[APP_NAME],
            status="active",
            raise_on_blocked=False,
            timeout=600,
        )
        assert (
            ops_test.model.applications[APP_NAME].units[0].workload_status
            == "active"
        )


@pytest.mark.abort_on_fail
@pytest.mark.usefixtures("deploy-upgrade")
class TestUpgrade:
    """Integration test for Trino charm upgrade from previous release."""

    async def test_upgrade(self, ops_test: OpsTest):
        """Builds the current charm and refreshes the current deployment."""
        charm = await ops_test.build_charm(".")
        resources = {
            "trino-image": METADATA["resources"]["trino-image"][
                "upstream-source"
            ]
        }
        await ops_test.model.applications[APP_NAME].refresh(
            path=str(charm), resources=resources
        )

        async with ops_test.fast_forward():
            await ops_test.model.wait_for_idle(
                apps=[APP_NAME],
                status="active",
                raise_on_blocked=False,
                timeout=600,
            )

            assert (
                ops_test.model.applications[APP_NAME].units[0].workload_status
                == "active"
            )

    async def test_ui_relation(self, ops_test: OpsTest):
        """Perform GET request on the Trino UI host."""
        url = await get_unit_url(
            ops_test, application=APP_NAME, unit=0, port=8080
        )
        logger.info("curling app address: %s", url)

        response = requests.get(url, timeout=300)
        assert response.status_code == 200

    async def test_config_unchanged(self, ops_test: OpsTest):
        """Validate config remains unchanged."""
        command = ["config", "trino-k8s"]
        returncode, stdout, stderr = await ops_test.juju(*command, check=True)
        if stderr:
            logger.error(f"{returncode}: {stderr}")
        config = yaml.safe_load(stdout)
        password = config["settings"]["trino-password"]["value"]
        assert password == SECURE_PWD
