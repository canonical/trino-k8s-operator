# Copyright 2023 Canonical Ltd.
# See LICENSE file for licensing details.

"""Trino charm integration test config."""
import logging
from pathlib import Path

import pytest
import pytest_asyncio
from helpers import APP_NAME, NGINX_NAME, WORKER_CONFIG, WORKER_NAME
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
@pytest_asyncio.fixture(name="deploy", scope="module")
async def deploy(ops_test: OpsTest, charm: str, charm_image: str):
    """Deploy the app."""
    # Deploy trino and nginx charms
    async with ops_test.fast_forward():
        await ops_test.model.deploy(
            charm,
            resources={"trino-image": charm_image},
            application_name=APP_NAME,
            num_units=1,
            trust=True,
        )
        await ops_test.model.deploy(
            charm,
            resources={"trino-image": charm_image},
            application_name=WORKER_NAME,
            config=WORKER_CONFIG,
            num_units=1,
            trust=True,
        )

        await ops_test.model.deploy(NGINX_NAME, trust=True)

        await ops_test.model.wait_for_idle(
            apps=[APP_NAME, WORKER_NAME],
            status="blocked",
            raise_on_blocked=False,
            timeout=600,
        )
        await ops_test.model.wait_for_idle(
            apps=[NGINX_NAME],
            status="waiting",
            raise_on_blocked=False,
            timeout=600,
        )

        await ops_test.model.integrate(
            f"{APP_NAME}:trino-coordinator", f"{WORKER_NAME}:trino-worker"
        )

        await ops_test.model.integrate(APP_NAME, NGINX_NAME)

        await ops_test.model.wait_for_idle(
            apps=[APP_NAME, WORKER_NAME],
            status="active",
            raise_on_blocked=False,
            timeout=300,
        )
    assert (
        ops_test.model.applications[APP_NAME].units[0].workload_status
        == "active"
    )
