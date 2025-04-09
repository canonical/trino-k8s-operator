# Copyright 2023 Canonical Ltd.
# See LICENSE file for licensing details.

"""Trino charm upgrades integration tests."""

import logging
from pathlib import Path

import pytest
import pytest_asyncio
import requests
import yaml
from helpers import APP_NAME, get_unit_url
from pytest import FixtureRequest
from pytest_operator.plugin import OpsTest

logger = logging.getLogger(__name__)


@pytest.fixture(scope="module", name="charm_image")
def charm_image_fixture(request: FixtureRequest) -> str:
    """The OCI image for charm."""
    charm_image = request.config.getoption("--superset-image")
    assert (
        charm_image
    ), "--superset-image argument is required which should contain the name of the OCI image."
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


@pytest.mark.skip_if_deployed
@pytest_asyncio.fixture(name="deploy-upgrade", scope="module")
async def deploy(ops_test: OpsTest):
    """Deploy the app."""
    # Deploy trino and nginx charms
    trino_config = {
        "acl-mode-default": "none",
        "charm-function": "all",
    }
    await ops_test.model.deploy(
        APP_NAME, channel="edge", config=trino_config, trust=True
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


@pytest.mark.abort_on_fail
@pytest.mark.usefixtures("deploy-upgrade")
class TestUpgrade:
    """Integration test for Trino charm upgrade from previous release."""

    async def test_upgrade(
        self, ops_test: OpsTest, charm: str, charm_image: str
    ):
        """Builds the current charm and refreshes the current deployment."""
        await ops_test.model.applications[APP_NAME].refresh(
            path=str(charm), resources={"trino-image": charm_image}
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
        acl_mode_default = config["settings"]["acl-mode-default"]["value"]
        assert acl_mode_default == "none"
