# Copyright 2023 Canonical Ltd.
# See LICENSE file for licensing details.

"""Integration tests for trino-catalog relation."""

import ast
import logging

import pytest
import pytest_asyncio
from helpers import (
    APP_NAME,
    add_juju_secret,
    create_catalog_config,
    get_secret_id_by_label,
)
from pytest_operator.plugin import OpsTest

from literals import TRINO_PORTS, USER_SECRET_LABEL

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
            timeout=1000,
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

        await ops_test.model.wait_for_idle(
            apps=[APP_NAME],
            status="active",
            raise_on_blocked=False,
            timeout=1000,
        )


@pytest.mark.abort_on_fail
@pytest.mark.usefixtures("deploy-trino", "deploy-requirer")
async def test_01_requirer_deployment(ops_test: OpsTest):
    """Test that the requirer charm has been deployed."""
    # Verify requirer is blocked waiting for relation
    assert (
        ops_test.model.applications[REQUIRER_APP].units[0].workload_status
        == "blocked"
    )


@pytest.mark.abort_on_fail
@pytest.mark.usefixtures("deploy-trino", "deploy-requirer")
async def test_02_trino_catalog_relation_created(ops_test: OpsTest):
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
            timeout=1000,
        )

        await ops_test.model.wait_for_idle(
            apps=[REQUIRER_APP],
            status="waiting",  # Requirer will be waiting for Trino data
            raise_on_blocked=False,
            timeout=1000,
        )

    # Verify relation exists
    trino_catalog_relations = [
        rel
        for rel in ops_test.model.applications[APP_NAME].relations
        if rel.matches(f"{APP_NAME}:trino-catalog")
    ]
    assert len(trino_catalog_relations) > 0


@pytest.mark.abort_on_fail
@pytest.mark.usefixtures("deploy-trino", "deploy-requirer")
async def test_03_trino_catalog_relation_set_url(ops_test: OpsTest):
    """Test setting the url for Trino but requirer is still waiting for all data."""
    async with ops_test.fast_forward():
        await ops_test.model.applications[APP_NAME].set_config(
            {"external-hostname": "trino.test.com"}
        )

        # Wait for Trino to be ready
        await ops_test.model.wait_for_idle(
            apps=[APP_NAME],
            status="active",
            timeout=1000,
        )

        # Requirer will be waiting for Trino data (it needs all three: catalogs, URL, secret)
        await ops_test.model.wait_for_idle(
            apps=[REQUIRER_APP],
            status="waiting",
            raise_on_blocked=False,
            timeout=1000,
        )


@pytest.mark.abort_on_fail
@pytest.mark.usefixtures("deploy-trino", "deploy-requirer")
async def test_04_trino_catalog_relation_set_catalogs(ops_test: OpsTest):
    """Test setting the catalogs for Trino but requirer is still waiting for all data."""
    # Create catalog secrets
    postgresql_secret_id = await add_juju_secret(ops_test, "postgresql")
    mysql_secret_id = await add_juju_secret(ops_test, "mysql")
    redshift_secret_id = await add_juju_secret(ops_test, "redshift")
    bigquery_secret_id = await add_juju_secret(ops_test, "bigquery")
    gsheets_secret_id = await add_juju_secret(ops_test, "gsheets")

    # Grant secrets to Trino
    await ops_test.model.grant_secret("postgresql-secret", APP_NAME)
    await ops_test.model.grant_secret("mysql-secret", APP_NAME)
    await ops_test.model.grant_secret("redshift-secret", APP_NAME)
    await ops_test.model.grant_secret("bigquery-secret", APP_NAME)
    await ops_test.model.grant_secret("gsheets-secret", APP_NAME)

    # Create catalog config
    catalog_config = await create_catalog_config(
        postgresql_secret_id,
        mysql_secret_id,
        redshift_secret_id,
        bigquery_secret_id,
        gsheets_secret_id,
        include_bigquery=True,
    )

    # Update Trino with catalog config
    async with ops_test.fast_forward():
        await ops_test.model.applications[APP_NAME].set_config(
            {"catalog-config": catalog_config}
        )

        # Wait for Trino to be ready
        await ops_test.model.wait_for_idle(
            apps=[APP_NAME],
            status="active",
            timeout=1000,
        )

        # Requirer will be waiting for Trino data (it needs all three: catalogs, URL, secret)
        await ops_test.model.wait_for_idle(
            apps=[REQUIRER_APP],
            status="waiting",
            raise_on_blocked=False,
            timeout=1000,
        )


@pytest.mark.abort_on_fail
@pytest.mark.usefixtures("deploy-trino", "deploy-requirer")
async def test_05_trino_catalog_relation_set_secret(ops_test: OpsTest):
    """Test granting the user secret to Trino but not to requirer which will be blocked."""
    # Create user secret for Trino
    users_secret_data = (
        "app-requirer-charm: password1\nuser2: password2"  # nosec
    )
    user_secret = await ops_test.model.add_secret(
        name=USER_SECRET_LABEL,
        data_args=[f"users={users_secret_data}"],
    )
    user_secret_id = user_secret.split(":")[-1]

    # Grant and configure Trino with user secret
    async with ops_test.fast_forward():

        await ops_test.model.grant_secret(USER_SECRET_LABEL, APP_NAME)

        await ops_test.model.applications[APP_NAME].set_config(
            {"user-secret-id": user_secret_id}
        )

        # Wait for Trino to be ready
        await ops_test.model.wait_for_idle(
            apps=[APP_NAME],
            status="active",
            timeout=1000,
        )

        # Requirer has all three (catalogs, URL, secret) but hasn't been granted the secret yet
        await ops_test.model.wait_for_idle(
            apps=[REQUIRER_APP],
            status="blocked",
            raise_on_blocked=False,
            timeout=1000,
        )


@pytest.mark.abort_on_fail
@pytest.mark.usefixtures("deploy-trino", "deploy-requirer")
async def test_06_trino_catalog_relation_grant_secret(
    ops_test: OpsTest,
):
    """Test granting secret to the requirer after which it becomes active."""
    async with ops_test.fast_forward():

        await ops_test.model.grant_secret(USER_SECRET_LABEL, REQUIRER_APP)

        await ops_test.model.wait_for_idle(
            apps=[REQUIRER_APP],
            status="active",
            timeout=1000,
        )


@pytest.mark.abort_on_fail
@pytest.mark.usefixtures("deploy-trino", "deploy-requirer")
async def test_07_trino_catalog_relation_read_data(
    ops_test: OpsTest,
):
    """Test that the requirer can read catalogs, URL, and credentials from Trino."""
    # Verify the requirer received the catalogs
    action = await ops_test.model.units.get(f"{REQUIRER_APP}/0").run_action(
        "get-relation-data"
    )
    await action.wait()
    assert action.status == "completed"

    # Parse the catalogs list from string representation
    catalogs_str = action.results.get("trino-catalogs", "[]")
    catalogs = ast.literal_eval(catalogs_str)
    catalogs_count = len(catalogs)

    assert catalogs_count == 5, f"Expected 5 catalogs, got {catalogs_count}"

    # Verify catalog names are present
    catalog_names = {cat["name"] for cat in catalogs}
    expected_catalogs = {
        "postgresql-1",
        "mysql",
        "redshift",
        "bigquery",
        "gsheets-1",
    }
    assert expected_catalogs.issubset(
        catalog_names
    ), f"Expected catalogs {expected_catalogs}, got {catalog_names}"

    logger.info(
        "Verified requirer received %s catalogs: %s",
        catalogs_count,
        catalog_names,
    )

    # Verify the requirer received the URL
    result_url = action.results.get("trino-url")
    assert (
        result_url == f"trino.test.com:{TRINO_PORTS['HTTPS']}"
    ), f"Expected URL 'trino.test.com:{TRINO_PORTS['HTTPS']}', got '{result_url}'"
    logger.info("Verified requirer received Trino URL: %s", result_url)

    # Verify the requirer received the username and password
    result_username = action.results.get("trino-username")
    result_password = action.results.get("trino-password")
    assert (
        result_username == "app-requirer-charm"
    ), f"Expected username 'app-requirer-charm', got '{result_username}'"
    assert (
        result_password == "password1"  # nosec
    ), f"Expected password 'password1', got '{result_password}'"
    logger.info(
        "Verified requirer received Trino credentials: username='%s', password='%s'",
        result_username,
        result_password,
    )


@pytest.mark.abort_on_fail
@pytest.mark.usefixtures("deploy-trino", "deploy-requirer")
async def test_08_catalog_config_propagation(ops_test: OpsTest):
    """Test that catalog-config changes propagate to the requirer."""
    # Get catalog secrets
    postgresql_secret_id = await get_secret_id_by_label(
        ops_test, "postgresql-secret"
    )
    mysql_secret_id = await get_secret_id_by_label(ops_test, "mysql-secret")
    redshift_secret_id = await get_secret_id_by_label(
        ops_test, "redshift-secret"
    )
    bigquery_secret_id = await get_secret_id_by_label(
        ops_test, "bigquery-secret"
    )
    gsheets_secret_id = await get_secret_id_by_label(
        ops_test, "gsheets-secret"
    )

    logger.info("PostgreSQL secret ID: %s", postgresql_secret_id)
    logger.info("MySQL secret ID: %s", mysql_secret_id)
    logger.info("Redshift secret ID: %s", redshift_secret_id)
    logger.info("BigQuery secret ID: %s", bigquery_secret_id)
    logger.info("GSheets secret ID: %s", gsheets_secret_id)

    # Update to remove bigquery
    catalog_config_without_bigquery = await create_catalog_config(
        postgresql_secret_id,
        mysql_secret_id,
        redshift_secret_id,
        bigquery_secret_id,
        gsheets_secret_id,
        include_bigquery=False,
    )

    async with ops_test.fast_forward():
        await ops_test.model.applications[APP_NAME].set_config(
            {"catalog-config": catalog_config_without_bigquery}
        )

        await ops_test.model.wait_for_idle(
            apps=[APP_NAME],
            status="active",
            timeout=1000,
        )

    # Verify the requirer received the updated catalogs (without bigquery)
    action = await ops_test.model.units.get(f"{REQUIRER_APP}/0").run_action(
        "get-relation-data"
    )
    await action.wait()
    assert action.status == "completed"

    # Parse the catalogs list from string representation
    catalogs_str = action.results.get("trino-catalogs", "[]")
    catalogs = ast.literal_eval(catalogs_str)
    catalogs_count = len(catalogs)

    assert (
        catalogs_count == 4
    ), f"Expected 4 catalogs after removing bigquery, got {catalogs_count}"

    # Verify bigquery is no longer present
    catalog_names = {cat["name"] for cat in catalogs}
    assert (
        "bigquery" not in catalog_names
    ), f"bigquery should be removed, but found in {catalog_names}"
    expected_catalogs = {"postgresql-1", "mysql", "redshift", "gsheets-1"}
    assert expected_catalogs.issubset(
        catalog_names
    ), f"Expected catalogs {expected_catalogs}, got {catalog_names}"

    logger.info(
        "Verified catalog count reduced to %s after removing bigquery: %s",
        catalogs_count,
        catalog_names,
    )


@pytest.mark.abort_on_fail
@pytest.mark.usefixtures("deploy-trino", "deploy-requirer")
async def test_09_multiple_requirers(ops_test: OpsTest):
    """Test that multiple requirers can connect to same Trino."""
    # Deploy a second requirer
    requirer_charm = await ops_test.build_charm(REQUIRER_CHARM_PATH)
    requirer_app_2 = "second-requirer"

    async with ops_test.fast_forward():
        await ops_test.model.deploy(
            requirer_charm,
            application_name=requirer_app_2,
            num_units=1,
        )

        await ops_test.model.wait_for_idle(
            apps=[requirer_app_2],
            status="blocked",
            timeout=1000,
        )

    # Relate second requirer to Trino
    async with ops_test.fast_forward():
        await ops_test.model.integrate(
            f"{APP_NAME}:trino-catalog", f"{requirer_app_2}:trino-catalog"
        )

        await ops_test.model.wait_for_idle(
            apps=[requirer_app_2],
            status="blocked",
            raise_on_blocked=False,
            timeout=1000,
        )

    # Grant secret to second requirer
    await ops_test.model.grant_secret(USER_SECRET_LABEL, requirer_app_2)

    async with ops_test.fast_forward():
        await ops_test.model.wait_for_idle(
            apps=[requirer_app_2],
            status="active",
            timeout=1000,
        )

    # Verify both requirers are connected
    trino_catalog_relations = [
        rel
        for rel in ops_test.model.applications[APP_NAME].relations
        if rel.matches(f"{APP_NAME}:trino-catalog")
    ]
    # Should have at least 2 relations
    assert len(trino_catalog_relations) >= 2

    # Clean up second requirer
    async with ops_test.fast_forward():
        await ops_test.model.remove_application(
            requirer_app_2, block_until_done=True
        )


@pytest.mark.abort_on_fail
@pytest.mark.usefixtures("deploy-trino", "deploy-requirer")
async def test_10_relation_broken(ops_test: OpsTest):
    """Test that relation can be broken cleanly."""
    # Remove the relation
    async with ops_test.fast_forward():
        await ops_test.juju(
            "remove-relation",
            f"{APP_NAME}:trino-catalog",
            f"{REQUIRER_APP}:trino-catalog",
        )

        await ops_test.model.wait_for_idle(
            apps=[REQUIRER_APP],
            status="blocked",
            raise_on_blocked=False,
            timeout=1000,
        )

    # Verify relation is removed
    trino_catalog_relations = [
        rel
        for rel in ops_test.model.applications[APP_NAME].relations
        if rel.matches(f"{APP_NAME}:trino-catalog")
    ]
    assert len(trino_catalog_relations) == 0

    # Requirer should be blocked
    assert (
        ops_test.model.applications[REQUIRER_APP].units[0].workload_status
        == "blocked"
    )
