# Copyright 2023 Canonical Ltd.
# See LICENSE file for licensing details.

"""Trino charm integration test config."""

import logging

import pytest
import pytest_asyncio
from helpers import APP_NAME, METADATA, NGINX_NAME, WORKER_NAME
from pytest_operator.plugin import OpsTest

logger = logging.getLogger(__name__)


@pytest.mark.skip_if_deployed
@pytest_asyncio.fixture(name="deploy", scope="module")
async def deploy(ops_test: OpsTest):
    """Deploy the app."""
    charm = await ops_test.build_charm(".")
    resources = {
        "trino-image": METADATA["resources"]["trino-image"]["upstream-source"]
    }

    # Deploy trino and nginx charms
    trino_config = {"ranger-service-name": "trino-service"}
    await ops_test.model.deploy(
        charm,
        resources=resources,
        application_name=APP_NAME,
        config=trino_config,
        num_units=1,
    )
    worker_config = {"charm-function": "worker"}
    await ops_test.model.deploy(
        charm,
        resources=resources,
        application_name=WORKER_NAME,
        config=worker_config,
        num_units=1,
    )

    await ops_test.model.deploy(NGINX_NAME, trust=True)

    async with ops_test.fast_forward():
        await ops_test.model.wait_for_idle(
            apps=[APP_NAME, WORKER_NAME],
            status="active",
            raise_on_blocked=False,
            timeout=600,
        )
        await ops_test.model.wait_for_idle(
            apps=[NGINX_NAME],
            status="waiting",
            raise_on_blocked=False,
            timeout=600,
        )

        await ops_test.model.integrate(APP_NAME, NGINX_NAME)

        await ops_test.model.wait_for_idle(
            apps=[APP_NAME],
            status="active",
            raise_on_blocked=False,
            timeout=300,
        )
        assert (
            ops_test.model.applications[APP_NAME].units[0].workload_status
            == "active"
        )
