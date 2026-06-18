# Copyright 2023 Canonical Ltd.
# See LICENSE file for licensing details.

"""Trino charm integration test config."""

import logging
import time

import jubilant
import pytest
from helpers import APP_NAME, get_unit, wait_for_apps
from lightkube import Client  # pyright: ignore
from lightkube.resources.apps_v1 import StatefulSet

logger = logging.getLogger(__name__)


@pytest.fixture(name="deploy-resources", scope="module")
def deploy(juju: jubilant.Juju, charm: str, charm_image: str):
    """Deploy the app."""
    trino_config = {
        "charm-function": "all",
    }
    # Deploy trino with no resource constraints
    juju.deploy(
        charm,
        APP_NAME,
        resources={"trino-image": charm_image},
        config=trino_config,
        num_units=1,
        trust=True,
    )
    wait_for_apps(juju, [APP_NAME], status="active", timeout=300)
    assert get_unit(juju, APP_NAME).workload_status.current == "active"


@pytest.mark.abort_on_fail
@pytest.mark.usefixtures("deploy-resources")
class TestResources:
    """Integration test for resource limits and requests."""

    def test_resources_set(self, juju: jubilant.Juju):
        """Test setting resources."""
        juju.config(
            APP_NAME,
            {
                "workload-memory-requests": "1Gi",
                "workload-memory-limits": "2Gi",
                "workload-cpu-requests": "1",
                "workload-cpu-limits": "2",
            },
        )
        time.sleep(10)
        client = Client()
        statefulset = client.get(
            StatefulSet,
            name=APP_NAME,
            namespace=juju.model,
        )

        containers = statefulset.spec.template.spec.containers
        for container in containers:
            if container.name == "trino":
                current_limits = container.resources.limits or {}
                current_requests = container.resources.requests or {}
                assert current_limits == {"cpu": "2", "memory": "2Gi"}
                assert current_requests == {"cpu": "1", "memory": "1Gi"}
