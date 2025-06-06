# Copyright 2023 Canonical Ltd.
# See LICENSE file for licensing details.

"""Trino charm integration test config."""
import logging
import time

import pytest
import pytest_asyncio
from helpers import APP_NAME
from lightkube import Client  # pyright: ignore
from lightkube.resources.apps_v1 import StatefulSet
from pytest_operator.plugin import OpsTest

logger = logging.getLogger(__name__)


@pytest.mark.skip_if_deployed
@pytest_asyncio.fixture(name="deploy-resources", scope="module")
async def deploy(ops_test: OpsTest, charm: str, charm_image: str):
    """Deploy the app."""
    trino_config = {
        "charm-function": "all",
    }
    # Deploy trino with no resource constraints
    async with ops_test.fast_forward():
        await ops_test.model.deploy(
            charm,
            resources={"trino-image": charm_image},
            application_name=APP_NAME,
            config=trino_config,
            num_units=1,
            trust=True,
        )
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


@pytest.mark.abort_on_fail
@pytest.mark.usefixtures("deploy-resources")
class TestResources:
    """Integration test for resource limits and requests."""

    async def test_resources_set(self, ops_test):
        """Test setting resources."""
        await ops_test.model.applications[APP_NAME].set_config(
            {
                "workload-memory-requests": "1Gi",
                "workload-memory-limits": "2Gi",
                "workload-cpu-requests": "1",
                "workload-cpu-limits": "2",
            }
        )
        time.sleep(10)
        client = Client()
        statefulset = client.get(
            StatefulSet,
            name=APP_NAME,
            namespace=ops_test.model.name,
        )

        containers = statefulset.spec.template.spec.containers
        for container in containers:
            if container.name == "trino":
                current_limits = container.resources.limits or {}
                current_requests = container.resources.requests or {}
                assert current_limits == {"cpu": "1", "memory": "2Gi"}
                assert current_requests == {"cpu": "1", "memory": "1Gi"}
