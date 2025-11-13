# Copyright 2023 Canonical Ltd.
# See LICENSE file for licensing details.

"""Integration tests for trino-catalog relation."""

import ast
import logging

import pytest
import pytest_asyncio
from helpers import APP_NAME, add_juju_secret, create_catalog_config
from pytest_operator.plugin import OpsTest

logger = logging.getLogger(__name__)

REQUIRER_APP = "requirer-app"
REQUIRER_CHARM_PATH = "tests/integration/trino_catalog_requirer_charm"


@pytest.mark.skip_if_deployed
@pytest_asyncio.fixture(name="deploy-requirer", scope="module")
async def deploy_requirer(ops_test: OpsTest):
    """Deploy the requirer charm once for all tests in this module."""
    # Build the requirer charm
    requirer_charm = await ops_test.build_charm(REQUIRER_CHARM_PATH)

    # Deploy requirer charm
    async with ops_test.fast_forward():
        await ops_test.model.deploy(
            requirer_charm,
            application_name=REQUIRER_APP,
            num_units=1,
        )

        await ops_test.model.wait_for_idle(
            apps=[REQUIRER_APP],
            status="blocked",
            raise_on_blocked=False,
            timeout=2000,
        )


@pytest.mark.skip_if_deployed
@pytest_asyncio.fixture(name="deploy-trino", scope="module")
async def deploy_trino(ops_test: OpsTest, charm: str, charm_image: str):
    """Deploy the trino charm once for all tests in this module."""
    # Deploy trino charm
    async with ops_test.fast_forward():
        await ops_test.model.deploy(
            charm,
            resources={"trino-image": charm_image},
            application_name=APP_NAME,
            config={
                "charm-function": "all",
            },
            trust=True,
        )

        async with ops_test.fast_forward():
            await ops_test.model.wait_for_idle(
                apps=[APP_NAME],
                status="active",
                raise_on_blocked=False,
                timeout=2000,
            )


@pytest.mark.order(1)
@pytest.mark.abort_on_fail
@pytest.mark.usefixtures("deploy-trino", "deploy-requirer")
async def test_requirer_deployment(ops_test: OpsTest):
    """Test that the requirer charm has been deployed."""
    # Verify requirer is blocked waiting for relation
    assert (
        ops_test.model.applications[REQUIRER_APP].units[0].workload_status
        == "blocked"
    )


@pytest.mark.order(2)
@pytest.mark.abort_on_fail
@pytest.mark.usefixtures("deploy-trino", "deploy-requirer")
async def test_trino_catalog_relation_created(ops_test: OpsTest):
    """Test creating the trino-catalog relation."""
    # Add the relation
    async with ops_test.fast_forward():
        await ops_test.model.integrate(
            f"{APP_NAME}:trino-catalog", f"{REQUIRER_APP}:trino-catalog"
        )

        # Wait for relation to settle
        await ops_test.model.wait_for_idle(
            apps=[APP_NAME],
            status="active",
            raise_on_blocked=False,
            timeout=600,
        )

        await ops_test.model.wait_for_idle(
            apps=[REQUIRER_APP],
            status="waiting",  # Requirer will be waiting for Trino data
            raise_on_blocked=False,
            timeout=600,
        )

    # Verify relation exists
    relations = ops_test.model.applications[APP_NAME].relations
    trino_catalog_relations = [
        r for r in relations if r.matches("trino-catalog")
    ]
    assert len(trino_catalog_relations) > 0
