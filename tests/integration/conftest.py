# Copyright 2023 Canonical Ltd.
# See LICENSE file for licensing details.

"""Trino charm integration test config."""
import logging

import pytest
import pytest_asyncio
from helpers import (
    APP_NAME,
    BASE_DIR,
    NGINX_NAME,
    TRINO_IMAGE,
    WORKER_CONFIG,
    WORKER_NAME,
)
from pytest_operator.plugin import OpsTest

logger = logging.getLogger(__name__)


@pytest.mark.skip_if_deployed
@pytest_asyncio.fixture(name="deploy", scope="module")
async def deploy(ops_test: OpsTest):
    """Deploy the app."""
    charm = await ops_test.build_charm(BASE_DIR)

    # Deploy trino and nginx charms
    async with ops_test.fast_forward():
        await ops_test.model.deploy(
            charm,
            resources=TRINO_IMAGE,
            application_name=APP_NAME,
            num_units=1,
        )
        await ops_test.model.deploy(
            charm,
            resources=TRINO_IMAGE,
            application_name=WORKER_NAME,
            config=WORKER_CONFIG,
            num_units=1,
        )

        await ops_test.model.deploy(NGINX_NAME, trust=True)

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
