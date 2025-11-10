# Copyright 2023 Canonical Ltd.
# See LICENSE file for licensing details.

"""Integration tests for trino-catalog relation."""

import ast
import logging

import pytest
from helpers import (
    APP_NAME,
    WORKER_NAME,
    add_juju_secret,
    create_catalog_config,
)
from pytest_operator.plugin import OpsTest

logger = logging.getLogger(__name__)

REQUIRER_APP = "requirer-app"
REQUIRER_CHARM_PATH = "tests/integration/trino_catalog_requirer_charm"


@pytest.fixture(scope="module")
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
            timeout=600,
        )

    # Create user secret for Trino (trino-user-management)
    users_secret_data = "app-requirer-app: test1\nother-user: test2"  # nosec
    user_secret = await ops_test.model.add_secret(
        name="trino-user-management",
        data_args=[f"users={users_secret_data}"],
    )
    user_secret_id = user_secret.split(":")[-1]

    # Configure Trino with user secret
    async with ops_test.fast_forward():
        await ops_test.model.applications[APP_NAME].set_config(
            {"user-secret-id": user_secret_id}
        )

        # Wait for Trino to be ready
        await ops_test.model.wait_for_idle(
            apps=[APP_NAME, WORKER_NAME],
            status="active",
            timeout=600,
        )

    return user_secret_id


@pytest.mark.abort_on_fail
@pytest.mark.usefixtures("deploy", "deploy_requirer")
async def test_trino_catalog_relation_deployment(ops_test: OpsTest):
    """Test that the requirer charm has been deployed."""
    # Verify requirer is blocked waiting for relation
    assert (
        ops_test.model.applications[REQUIRER_APP].units[0].workload_status
        == "blocked"
    )


@pytest.mark.abort_on_fail
@pytest.mark.usefixtures("deploy", "deploy_requirer")
async def test_trino_catalog_relation_created(ops_test: OpsTest):
    """Test creating the trino-catalog relation."""
    # Add the relation
    async with ops_test.fast_forward():
        await ops_test.model.integrate(
            f"{APP_NAME}:trino-catalog", f"{REQUIRER_APP}:trino-catalog"
        )

        # Wait for relation to settle
        await ops_test.model.wait_for_idle(
            apps=[APP_NAME, REQUIRER_APP],
            status="blocked",  # Requirer will be blocked waiting for secret grant
            raise_on_blocked=False,
            timeout=600,
        )

    # Verify relation exists
    relations = ops_test.model.applications[APP_NAME].relations
    trino_catalog_relations = [
        r for r in relations if r.matches("trino-catalog")
    ]
    assert len(trino_catalog_relations) > 0


@pytest.mark.abort_on_fail
@pytest.mark.usefixtures("deploy", "deploy_requirer")
async def test_trino_catalog_relation_data_shared(ops_test: OpsTest):
    """Test that Trino shares catalog data via the relation."""
    # Configure external hostname so Trino can share URL
    async with ops_test.fast_forward():
        await ops_test.model.applications[APP_NAME].set_config(
            {"external-hostname": "trino.example.com"}
        )

        await ops_test.model.wait_for_idle(
            apps=[APP_NAME],
            status="active",
            timeout=600,
        )

    # Verify relation exists
    relations = ops_test.model.applications[APP_NAME].relations
    trino_catalog_relations = [
        r for r in relations if r.matches("trino-catalog")
    ]
    assert len(trino_catalog_relations) > 0

    # Verify the requirer received the data
    action = await ops_test.model.units.get(f"{REQUIRER_APP}/0").run_action(
        "get-relation-data"
    )
    await action.wait()
    assert action.status == "completed"

    # Check all three key pieces of data are present
    trino_url = action.results.get("trino-url")
    trino_catalogs = action.results.get("trino-catalogs")
    secret_id = action.results.get("trino-credentials-secret-id")

    assert trino_url, "Trino URL should be present in relation data"
    assert (
        "trino.example.com" in trino_url
    ), f"Expected 'trino.example.com' in URL, got {trino_url}"
    assert trino_catalogs, "Catalogs should be present in relation data"
    assert secret_id, "Secret ID should be present in relation data"

    logger.info(
        f"Verified requirer received: URL={trino_url}, catalogs={trino_catalogs}, secret={secret_id}"
    )


@pytest.mark.abort_on_fail
@pytest.mark.usefixtures("deploy", "deploy_requirer")
async def test_external_hostname_propagation(ops_test: OpsTest):
    """Test that external-hostname changes propagate to the requirer."""
    # Update external hostname
    new_hostname = "trino-new.example.com"
    async with ops_test.fast_forward():
        await ops_test.model.applications[APP_NAME].set_config(
            {"external-hostname": new_hostname}
        )

        await ops_test.model.wait_for_idle(
            apps=[APP_NAME],
            status="active",
            timeout=600,
        )

    # Verify the requirer received the updated URL
    action = await ops_test.model.units.get(f"{REQUIRER_APP}/0").run_action(
        "get-relation-data"
    )
    await action.wait()

    assert action.status == "completed"
    result_url = action.results.get("trino-url")
    assert (
        new_hostname in result_url
    ), f"Expected hostname {new_hostname} in URL, got {result_url}"

    logger.info(f"Verified requirer received updated URL: {result_url}")


@pytest.mark.abort_on_fail
@pytest.mark.usefixtures("deploy", "deploy_requirer")
async def test_catalog_config_propagation(ops_test: OpsTest):
    """Test that catalog-config changes propagate to the requirer."""
    # Create catalog secrets
    postgresql_secret_id = await add_juju_secret(ops_test, "postgresql")
    mysql_secret_id = await add_juju_secret(ops_test, "mysql")
    redshift_secret_id = await add_juju_secret(ops_test, "redshift")
    bigquery_secret_id = await add_juju_secret(ops_test, "bigquery")
    gsheets_secret_id = await add_juju_secret(ops_test, "gsheets")

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

        await ops_test.model.wait_for_idle(
            apps=[APP_NAME, WORKER_NAME],
            status="active",
            timeout=600,
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
        f"Verified requirer received {catalogs_count} catalogs: {catalog_names}"
    )

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
            apps=[APP_NAME, WORKER_NAME],
            status="active",
            timeout=600,
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
        f"Verified catalog count reduced to {catalogs_count} after removing bigquery: {catalog_names}"
    )


@pytest.mark.abort_on_fail
@pytest.mark.usefixtures("deploy", "deploy_requirer")
async def test_user_secret_propagation(ops_test: OpsTest):
    """Test that user-secret-id changes propagate to the requirer."""
    # Create a new user secret (trino-user-management)
    new_user_secret_data = "app-requirer-app: test1\nother-user: test2"  # nosec
    new_user_secret = await ops_test.model.add_secret(
        name="trino-user-management-new",
        data_args=[f"users={new_user_secret_data}"],
    )
    new_secret_id = new_user_secret.split(":")[-1]

    # Update Trino configuration
    async with ops_test.fast_forward():
        await ops_test.model.applications[APP_NAME].set_config(
            {"user-secret-id": new_secret_id}
        )

        await ops_test.model.wait_for_idle(
            apps=[APP_NAME],
            status="active",
            timeout=600,
        )

    # Grant secret to requirer
    await ops_test.juju(
        "grant-secret",
        new_secret_id,
        REQUIRER_APP,
    )

    async with ops_test.fast_forward():
        await ops_test.model.wait_for_idle(
            apps=[REQUIRER_APP],
            status="active",
            timeout=600,
        )

    # Verify the requirer received the new secret ID
    action = await ops_test.model.units.get(f"{REQUIRER_APP}/0").run_action(
        "get-relation-data"
    )
    await action.wait()
    assert action.status == "completed"

    result_secret_id = action.results.get("trino-credentials-secret-id")
    assert (
        result_secret_id == new_secret_id
    ), f"Expected secret ID {new_secret_id}, got {result_secret_id}"

    logger.info(
        f"Verified requirer received updated secret ID: {result_secret_id}"
    )


@pytest.mark.abort_on_fail
@pytest.mark.usefixtures("deploy", "deploy_requirer")
async def test_secret_grant_required(ops_test: OpsTest):
    """Test that requirer needs secret grant to access credentials."""
    # Get the current user secret ID
    trino_config = await ops_test.model.applications[APP_NAME].get_config()
    user_secret_id = trino_config["user-secret-id"]["value"]

    if not user_secret_id:
        pytest.skip("No user secret configured")

    # Grant the secret to requirer
    await ops_test.juju(
        "grant-secret",
        user_secret_id,
        REQUIRER_APP,
    )

    # Wait for requirer to become active
    async with ops_test.fast_forward():
        await ops_test.model.wait_for_idle(
            apps=[REQUIRER_APP],
            status="active",
            timeout=600,
        )

    # Verify requirer is now active (has access to credentials)
    assert (
        ops_test.model.applications[REQUIRER_APP].units[0].workload_status
        == "active"
    )


@pytest.mark.abort_on_fail
@pytest.mark.usefixtures("deploy", "deploy_requirer")
async def test_relation_broken(ops_test: OpsTest):
    """Test that relation can be broken cleanly."""
    # Remove the relation
    async with ops_test.fast_forward():
        await ops_test.juju(
            "remove-relation",
            f"{APP_NAME}:trino-catalog",
            f"{REQUIRER_APP}:trino-catalog",
        )

        await ops_test.model.wait_for_idle(
            apps=[APP_NAME, REQUIRER_APP],
            status="blocked",
            raise_on_blocked=False,
            timeout=600,
        )

    # Verify relation is removed
    relations = ops_test.model.applications[APP_NAME].relations
    trino_catalog_relations = [
        r for r in relations if r.matches("trino-catalog")
    ]
    assert len(trino_catalog_relations) == 0

    # Requirer should be blocked
    assert (
        ops_test.model.applications[REQUIRER_APP].units[0].workload_status
        == "blocked"
    )


@pytest.mark.abort_on_fail
@pytest.mark.usefixtures("deploy", "deploy_requirer")
async def test_multiple_requirers(ops_test: OpsTest):
    """Test that multiple requirers can connect to same Trino."""
    # Deploy a second requirer
    requirer_charm = await ops_test.build_charm(REQUIRER_CHARM_PATH)
    requirer_app_2 = "requirer-app-2"

    async with ops_test.fast_forward():
        await ops_test.model.deploy(
            requirer_charm,
            application_name=requirer_app_2,
            num_units=1,
        )

        await ops_test.model.wait_for_idle(
            apps=[requirer_app_2],
            status="blocked",
            timeout=600,
        )

    # Relate second requirer to Trino
    async with ops_test.fast_forward():
        await ops_test.model.integrate(
            f"{APP_NAME}:trino-catalog", f"{requirer_app_2}:trino-catalog"
        )

        await ops_test.model.wait_for_idle(
            apps=[APP_NAME, requirer_app_2],
            status="blocked",
            raise_on_blocked=False,
            timeout=600,
        )

    # Grant secret to second requirer
    trino_config = await ops_test.model.applications[APP_NAME].get_config()
    user_secret_id = trino_config["user-secret-id"]["value"]

    if user_secret_id:
        await ops_test.juju(
            "grant-secret",
            user_secret_id,
            requirer_app_2,
        )

        async with ops_test.fast_forward():
            await ops_test.model.wait_for_idle(
                apps=[requirer_app_2],
                status="active",
                timeout=600,
            )

    # Verify both requirers are connected
    relations = ops_test.model.applications[APP_NAME].relations
    trino_catalog_relations = [
        r for r in relations if r.matches("trino-catalog")
    ]
    # Should have at least 2 relations
    assert len(trino_catalog_relations) >= 2

    # Clean up second requirer
    async with ops_test.fast_forward():
        await ops_test.model.remove_application(
            requirer_app_2, block_until_done=True
        )
