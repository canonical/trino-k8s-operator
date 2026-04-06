# Copyright 2023 Canonical Ltd.
# See LICENSE file for licensing details.

"""Integration tests for trino-catalog relation."""

import ast
import logging

import pytest
import pytest_asyncio
from helpers import (
    APP_NAME,
    NGINX_NAME,
    add_juju_secret,
    create_catalog_config,
    get_secret_id_by_label,
)
from pytest_operator.plugin import OpsTest

from literals import TRINO_PORTS

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
            status="active",
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
async def test_03_auto_generated_credentials(ops_test: OpsTest):
    """Test that per-relation credentials are auto-generated."""
    action = await ops_test.model.units.get(f"{REQUIRER_APP}/0").run_action(
        "get-relation-data"
    )
    await action.wait()
    assert action.status == "completed"

    # Verify auto-generated username follows pattern app-{app_name}-{relation_id}
    result_username = action.results.get("trino-username")
    assert result_username.startswith(
        f"app-{REQUIRER_APP}-"
    ), f"Expected username starting with 'app-{REQUIRER_APP}-', got '{result_username}'"

    # Verify password is a non-empty random string
    result_password = action.results.get("trino-password")
    assert (
        result_password and len(result_password) > 0
    ), "Expected non-empty auto-generated password"

    # Verify internal URL (no nginx relation at this point)
    result_url = action.results.get("trino-url")
    expected_internal_url = f"{APP_NAME}.{ops_test.model.name}.svc.cluster.local:{TRINO_PORTS['HTTP']}"
    assert (
        result_url == expected_internal_url
    ), f"Expected internal URL '{expected_internal_url}', got '{result_url}'"

    logger.info(
        "Verified auto-generated credentials: username='%s', URL='%s'",
        result_username,
        result_url,
    )


@pytest.mark.abort_on_fail
@pytest.mark.usefixtures("deploy-trino", "deploy-requirer")
async def test_04_trino_catalog_relation_set_catalogs(ops_test: OpsTest):
    """Test that catalog-config changes propagate to the requirer."""
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

        # Requirer stays active
        await ops_test.model.wait_for_idle(
            apps=[REQUIRER_APP],
            status="active",
            timeout=1000,
        )

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


@pytest.mark.abort_on_fail
@pytest.mark.usefixtures("deploy-trino", "deploy-requirer")
async def test_05_catalog_exclusions(ops_test: OpsTest):
    """Test that catalog-exclusions filters catalogs for a specific requirer."""
    # Exclude one catalog for the requirer app
    exclusion_config = f"{REQUIRER_APP}:\n  - postgresql-1"

    async with ops_test.fast_forward():
        await ops_test.model.applications[APP_NAME].set_config(
            {"catalog-exclusions": exclusion_config}
        )

        await ops_test.model.wait_for_idle(
            apps=[APP_NAME],
            status="active",
            timeout=1000,
        )

    # Verify the requirer receives 4 catalogs (postgresql-1 excluded)
    action = await ops_test.model.units.get(f"{REQUIRER_APP}/0").run_action(
        "get-relation-data"
    )
    await action.wait()
    assert action.status == "completed"

    catalogs_str = action.results.get("trino-catalogs", "[]")
    catalogs = ast.literal_eval(catalogs_str)
    catalog_names = {cat["name"] for cat in catalogs}

    assert (
        "postgresql-1" not in catalog_names
    ), f"postgresql-1 should be excluded, but found in {catalog_names}"
    assert len(catalogs) == 4, f"Expected 4 catalogs, got {len(catalogs)}"

    # Reset exclusions and verify all catalogs are restored
    async with ops_test.fast_forward():
        await ops_test.model.applications[APP_NAME].reset_config(
            ["catalog-exclusions"]
        )

        await ops_test.model.wait_for_idle(
            apps=[APP_NAME],
            status="active",
            timeout=1000,
        )

    action = await ops_test.model.units.get(f"{REQUIRER_APP}/0").run_action(
        "get-relation-data"
    )
    await action.wait()
    assert action.status == "completed"

    catalogs_str = action.results.get("trino-catalogs", "[]")
    catalogs = ast.literal_eval(catalogs_str)
    catalog_names = {cat["name"] for cat in catalogs}

    assert (
        "postgresql-1" in catalog_names
    ), f"postgresql-1 should be restored after clearing exclusions, got {catalog_names}"
    assert len(catalogs) == 5, f"Expected 5 catalogs, got {len(catalogs)}"

    logger.info("Verified catalog exclusions work correctly")


@pytest.mark.abort_on_fail
@pytest.mark.usefixtures("deploy-trino", "deploy-requirer")
async def test_06_trino_catalog_external_url_with_nginx(
    ops_test: OpsTest,
):
    """Test that the requirer receives external URL when nginx-route relation exists."""
    # Deploy nginx-ingress-integrator
    async with ops_test.fast_forward():
        await ops_test.model.deploy(NGINX_NAME, trust=True)

        await ops_test.model.wait_for_idle(
            apps=[NGINX_NAME],
            status="waiting",
            raise_on_blocked=False,
            timeout=1000,
        )

    # Relate Trino to nginx-ingress-integrator
    async with ops_test.fast_forward():
        await ops_test.model.integrate(APP_NAME, NGINX_NAME)

        await ops_test.model.wait_for_idle(
            apps=[APP_NAME],
            status="active",
            timeout=1000,
        )

    # Set external-hostname config
    async with ops_test.fast_forward():
        await ops_test.model.applications[APP_NAME].set_config(
            {"external-hostname": "trino.test.com"}
        )

        await ops_test.model.wait_for_idle(
            apps=[APP_NAME],
            status="active",
            timeout=1000,
        )

    # Verify the requirer received the external URL
    action = await ops_test.model.units.get(f"{REQUIRER_APP}/0").run_action(
        "get-relation-data"
    )
    await action.wait()
    assert action.status == "completed"

    result_url = action.results.get("trino-url")
    expected_external_url = f"trino.test.com:{TRINO_PORTS['HTTPS']}"
    assert (
        result_url == expected_external_url
    ), f"Expected external URL '{expected_external_url}', got '{result_url}'"
    logger.info(
        "Verified requirer received external Trino URL: %s", result_url
    )

    # Clean up: remove nginx relation and reset external-hostname
    async with ops_test.fast_forward():
        await ops_test.juju(
            "remove-relation",
            APP_NAME,
            NGINX_NAME,
        )

        await ops_test.model.wait_for_idle(
            apps=[APP_NAME],
            status="active",
            timeout=1000,
        )

    async with ops_test.fast_forward():
        await ops_test.model.applications[APP_NAME].reset_config(
            ["external-hostname"]
        )

        await ops_test.model.wait_for_idle(
            apps=[APP_NAME],
            status="active",
            timeout=1000,
        )


@pytest.mark.abort_on_fail
@pytest.mark.usefixtures("deploy-trino", "deploy-requirer")
async def test_07_catalog_config_propagation(ops_test: OpsTest):
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
async def test_08_multiple_requirers(ops_test: OpsTest):
    """Test that multiple requirers each get separate auto-generated credentials."""
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
            status="active",
            raise_on_blocked=False,
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

    # Verify each requirer got different credentials
    action_1 = await ops_test.model.units.get(f"{REQUIRER_APP}/0").run_action(
        "get-relation-data"
    )
    await action_1.wait()
    action_2 = await ops_test.model.units.get(
        f"{requirer_app_2}/0"
    ).run_action("get-relation-data")
    await action_2.wait()

    username_1 = action_1.results.get("trino-username")
    username_2 = action_2.results.get("trino-username")

    assert username_1.startswith(f"app-{REQUIRER_APP}-")
    assert username_2.startswith(f"app-{requirer_app_2}-")
    assert username_1 != username_2, (
        f"Each requirer should get a unique username, "
        f"got '{username_1}' and '{username_2}'"
    )

    logger.info(
        "Verified separate credentials: requirer1='%s', requirer2='%s'",
        username_1,
        username_2,
    )

    # Clean up second requirer
    async with ops_test.fast_forward():
        await ops_test.model.remove_application(
            requirer_app_2, block_until_done=True
        )


@pytest.mark.abort_on_fail
@pytest.mark.usefixtures("deploy-trino", "deploy-requirer")
async def test_09_relation_broken(ops_test: OpsTest):
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
