# Copyright 2023 Canonical Ltd.
# See LICENSE file for licensing details.

"""Trino charm integration test config."""

import logging

import pytest
import pytest_asyncio
from helpers import (
    APP_NAME,
    TLS_NAME,
    NGINX_NAME,
    METADATA,
    perform_trino_integrations,
)
from pytest_operator.plugin import OpsTest

logger = logging.getLogger(__name__)


@pytest.mark.skip_if_deployed
@pytest_asyncio.fixture(name="deploy", scope="module")
async def deploy(ops_test: OpsTest):
    """The app is up and running."""
    charm = await ops_test.build_charm(".")
    resources = {"trino-image": METADATA["resources"]["trino-image"]["upstream-source"]}
    tls_config = {"generate-self-signed-certificates": "true", "ca-common-name": "kafka"}
    
    # Dummy Oauth credentials for testing only
    trino_config = {"trino-password":"dummycreds123"}

    # Deploy trino, tls and nginx charms
    await ops_test.model.deploy(charm, resources=resources, application_name=APP_NAME, config=trino_config, num_units=1)
    await ops_test.model.deploy(TLS_NAME, config=tls_config, channel="edge", trust=True)
    await ops_test.model.deploy(NGINX_NAME, trust=True)


    async with ops_test.fast_forward():
        await ops_test.model.wait_for_idle(
            apps=[NGINX_NAME, TLS_NAME], status="active", raise_on_blocked=False, timeout=600
        )
        await ops_test.model.wait_for_idle(
            apps=[APP_NAME], status="blocked", raise_on_blocked=False, timeout=600
        )

        await perform_trino_integrations(ops_test)

        await ops_test.model.wait_for_idle(apps=[APP_NAME], status="active", raise_on_blocked=False, timeout=300)
        assert ops_test.model.applications[APP_NAME].units[0].workload_status == "active"
