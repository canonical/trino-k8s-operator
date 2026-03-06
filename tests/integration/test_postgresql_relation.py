# Copyright 2023 Canonical Ltd.
# See LICENSE file for licensing details.

"""Integration tests for the Trino-PostgreSQL relation."""

import asyncio
import logging
import time

import pytest
import yaml
from conftest import deploy  # noqa: F401, pylint: disable=W0611
from helpers import (
    APP_NAME,
    POSTGRES_NAME,
    TRINO_USER,
    WORKER_NAME,
    add_juju_secret,
    create_catalog_config,
    get_catalogs,
    update_catalog_config,
)
from pytest_operator.plugin import OpsTest

logger = logging.getLogger(__name__)

PG_CHANNEL = "16/edge"
PG_CONFIG_KEY = "postgresql-catalog-config"
CATALOG_DIR = "/usr/lib/trino/etc/catalog"


def pg_catalog_config(
    app_name,
    prefix,
    ro_name,
    rw_name=None,
    config=None,
):
    """Build a postgresql-catalog-config YAML string.

    Args:
        app_name: The PG app name (top-level key).
        prefix: The database_prefix value.
        ro_name: The ro_catalog_name value.
        rw_name: Optional rw_catalog_name value.
        config: Optional extra config string.

    Returns:
        YAML string for the config.
    """
    entry = {
        "database_prefix": prefix,
        "ro_catalog_name": ro_name,
    }
    if rw_name:
        entry["rw_catalog_name"] = rw_name
    if config:
        entry["config"] = config
    return yaml.dump({app_name: entry})


async def deploy_pg(
    ops_test, pg_name=POSTGRES_NAME, db_name="testdb", units=1
):
    """Deploy PostgreSQL and a data-integrator to create a database.

    The DI is needed because PG's prefix matching only discovers existing
    databases, it does not create them from a prefix request.

    Args:
        ops_test: PyTest object.
        pg_name: Application name for PG.
        db_name: Database name for the data-integrator to create.
        units: Number of PG units.
    """
    di_name = f"di-{pg_name}"
    await ops_test.model.deploy(
        "postgresql-k8s",
        application_name=pg_name,
        channel=PG_CHANNEL,
        num_units=units,
        trust=True,
    )
    await ops_test.model.deploy(
        "data-integrator",
        application_name=di_name,
        channel="latest/edge",
        config={"database-name": db_name},
    )
    await ops_test.model.integrate(f"{pg_name}:database", di_name)
    async with ops_test.fast_forward():
        await ops_test.model.wait_for_idle(
            apps=[pg_name],
            status="active",
            timeout=900,
            wait_for_exact_units=units,
        )
        await ops_test.model.wait_for_idle(
            apps=[di_name],
            status="active",
            timeout=900,
        )


async def wait_for_idle_pg(ops_test, pg_name=POSTGRES_NAME):
    """Wait for PG and Trino to be idle and active.

    Args:
        ops_test: PyTest object.
        pg_name: PG application name.
    """
    async with ops_test.fast_forward():
        await ops_test.model.wait_for_idle(
            apps=[APP_NAME, WORKER_NAME, pg_name],
            status="active",
            raise_on_blocked=False,
            timeout=900,
        )


async def destroy_pg(ops_test, pg_name):
    """Destroy a PG app and its associated data-integrator.

    Args:
        ops_test: PyTest object.
        pg_name: Application name for PG.
    """
    di_name = f"di-{pg_name}"
    await ops_test.model.applications[di_name].destroy()
    await ops_test.model.applications[pg_name].destroy()
    await ops_test.model.block_until(
        lambda: pg_name not in ops_test.model.applications
    )


async def relate_pg(ops_test, pg_name=POSTGRES_NAME):
    """Integrate Trino with PG and wait for idle.

    Args:
        ops_test: PyTest object.
        pg_name: PG application name.
    """
    await ops_test.model.integrate(APP_NAME, pg_name)
    await wait_for_idle_pg(ops_test, pg_name)


async def remove_pg_relation(ops_test, pg_name=POSTGRES_NAME):
    """Remove the Trino-PG relation and wait for idle.

    Args:
        ops_test: PyTest object.
        pg_name: PG application name.
    """
    await ops_test.juju("remove-relation", APP_NAME, pg_name)
    async with ops_test.fast_forward():
        await ops_test.model.wait_for_idle(
            apps=[APP_NAME], status="active", timeout=600
        )


async def set_pg_config(ops_test, config_str):
    """Set postgresql-catalog-config and wait for idle.

    Args:
        ops_test: PyTest object.
        config_str: YAML config string.
    """
    await ops_test.model.applications[APP_NAME].set_config(
        {PG_CONFIG_KEY: config_str}
    )
    async with ops_test.fast_forward():
        await ops_test.model.wait_for_idle(
            apps=[APP_NAME, POSTGRES_NAME],
            status="active",
            timeout=900,
        )


async def wait_for_catalog(ops_test, catalog_name, present=True, timeout=120):
    """Poll SHOW CATALOGS until catalog appears/disappears or timeout.

    Args:
        ops_test: PyTest object.
        catalog_name: Catalog name to check.
        present: True to wait for appearance, False for disappearance.
        timeout: Seconds before giving up.

    Returns:
        The catalogs list on success.

    Raises:
        TimeoutError: If the expected state isn't reached.
    """
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            catalogs = await get_catalogs(ops_test, TRINO_USER, APP_NAME)
            found = catalog_name in str(catalogs)
            if found == present:
                return catalogs
        except Exception:  # nosec
            pass  # nosec
        await asyncio.sleep(5)
    state = "not found" if present else "still present"
    raise TimeoutError(f"Catalog {catalog_name!r} {state} after {timeout}s")


async def get_properties_file(ops_test, catalog_name):
    """Read a catalog .properties file from the Trino container.

    Args:
        ops_test: PyTest object.
        catalog_name: Name of the catalog.

    Returns:
        File contents as string, or None if not found.
    """
    unit = ops_test.model.applications[APP_NAME].units[0]
    rc, stdout, _ = await ops_test.juju(
        "ssh",
        "--container",
        "trino",
        unit.name,
        "cat",
        f"{CATALOG_DIR}/{catalog_name}.properties",
    )
    if rc != 0:
        return None
    return stdout


async def count_hosts_in_catalog(ops_test, catalog_name):
    """Count the number of hosts in a catalog's JDBC connection URL.

    Handles Java Properties escape format (backslash-escaped colons).

    Args:
        ops_test: PyTest object.
        catalog_name: Name of the catalog.

    Returns:
        Number of hosts in the connection URL.
    """
    props = await get_properties_file(ops_test, catalog_name)
    assert props is not None, f"Properties file for {catalog_name} not found"
    url_line = [
        line for line in props.splitlines() if "connection-url" in line
    ][0]
    url = url_line.replace("\\:", ":").replace("\\=", "=")
    # Extract hosts from jdbc:postgresql://host1,host2,host3:port/db?params
    hosts_part = url.split("//")[1].split(":")[0]
    return hosts_part.count(",") + 1


@pytest.mark.abort_on_fail
@pytest.mark.usefixtures("deploy")
class TestPostgresqlRelation:
    """Integration tests for PostgreSQL relation."""

    async def test_01_missing_database_prefix(self, ops_test: OpsTest):
        """Config without database_prefix: no catalog, charm stays active."""
        config = yaml.dump({POSTGRES_NAME: {"ro_catalog_name": "test_ro"}})
        await ops_test.model.applications[APP_NAME].set_config(
            {PG_CONFIG_KEY: config}
        )
        await deploy_pg(ops_test)
        await relate_pg(ops_test)

        await wait_for_catalog(ops_test, "test_ro", present=False)
        assert (
            ops_test.model.applications[APP_NAME].units[0].workload_status
            == "active"
        )

    async def test_02_prefix_without_asterisk(self, ops_test: OpsTest):
        """Config with database_prefix missing *: no catalog."""
        config = pg_catalog_config(POSTGRES_NAME, "mydb", "test_ro")
        await set_pg_config(ops_test, config)

        await wait_for_catalog(ops_test, "test_ro", present=False)

    async def test_03_missing_ro_catalog_name(self, ops_test: OpsTest):
        """Config with database_prefix but no ro_catalog_name: no catalog."""
        config = yaml.dump({POSTGRES_NAME: {"database_prefix": "testdb*"}})
        await set_pg_config(ops_test, config)

        await wait_for_catalog(ops_test, "testdb", present=False)

    async def test_04_fix_invalid_config(self, ops_test: OpsTest):
        """After fixing invalid config, catalog is created."""
        config = pg_catalog_config(POSTGRES_NAME, "testdb*", "test_catalog")
        await set_pg_config(ops_test, config)

        await wait_for_catalog(ops_test, "test_catalog")

    async def test_05_both_ro_and_rw(self, ops_test: OpsTest):
        """Config with both ro and rw: two catalogs created."""
        config = pg_catalog_config(
            POSTGRES_NAME, "testdb*", "mydb_ro", rw_name="mydb_rw"
        )
        await set_pg_config(ops_test, config)

        await wait_for_catalog(ops_test, "mydb_ro")
        await wait_for_catalog(ops_test, "mydb_rw")

        ro_props = await get_properties_file(ops_test, "mydb_ro")
        rw_props = await get_properties_file(ops_test, "mydb_rw")
        assert ro_props is not None
        assert rw_props is not None
        assert "preferSecondary" in ro_props
        assert "targetServerType=primary" in rw_props

    async def test_06_remove_rw(self, ops_test: OpsTest):
        """Remove rw_catalog_name: RW catalog dropped, RO remains."""
        config = pg_catalog_config(POSTGRES_NAME, "testdb*", "mydb_ro")
        await set_pg_config(ops_test, config)

        await wait_for_catalog(ops_test, "mydb_ro")
        await wait_for_catalog(ops_test, "mydb_rw", present=False)

    async def test_07_add_rw(self, ops_test: OpsTest):
        """Add rw_catalog_name: RW catalog appears alongside RO."""
        config = pg_catalog_config(
            POSTGRES_NAME, "testdb*", "mydb_ro", rw_name="mydb_rw"
        )
        await set_pg_config(ops_test, config)

        await wait_for_catalog(ops_test, "mydb_ro")
        await wait_for_catalog(ops_test, "mydb_rw")

    async def test_08_all_unit_ips(self, ops_test: OpsTest):
        """JDBC URL contains all PG unit IPs (3 units)."""
        async with ops_test.fast_forward():
            await ops_test.model.applications[POSTGRES_NAME].scale(scale=3)
            await ops_test.model.wait_for_idle(
                apps=[POSTGRES_NAME],
                status="active",
                timeout=900,
                wait_for_exact_units=3,
            )

        config = pg_catalog_config(POSTGRES_NAME, "testdb*", "multihost_ro")
        await set_pg_config(ops_test, config)

        await wait_for_catalog(ops_test, "multihost_ro")

        host_count = await count_hosts_in_catalog(ops_test, "multihost_ro")
        assert host_count == 3, f"Expected 3 hosts, got {host_count}"

    async def test_09_scale_down(self, ops_test: OpsTest):
        """Scale PG down: JDBC URL updated."""
        async with ops_test.fast_forward():
            await ops_test.model.applications[POSTGRES_NAME].scale(scale=2)
            await ops_test.model.wait_for_idle(
                apps=[POSTGRES_NAME],
                status="active",
                timeout=900,
                wait_for_exact_units=2,
            )
            await ops_test.model.wait_for_idle(
                apps=[APP_NAME], status="active", timeout=600
            )

        host_count = await count_hosts_in_catalog(ops_test, "multihost_ro")
        assert host_count == 2, f"Expected 2 hosts, got {host_count}"

    async def test_10_scale_up(self, ops_test: OpsTest):
        """Scale PG back up: JDBC URL updated."""
        async with ops_test.fast_forward():
            await ops_test.model.applications[POSTGRES_NAME].scale(scale=3)
            await ops_test.model.wait_for_idle(
                apps=[POSTGRES_NAME],
                status="active",
                timeout=900,
                wait_for_exact_units=3,
            )
            await ops_test.model.wait_for_idle(
                apps=[APP_NAME], status="active", timeout=600
            )

        host_count = await count_hosts_in_catalog(ops_test, "multihost_ro")
        assert host_count == 3, f"Expected 3 hosts, got {host_count}"

    async def test_11_both_config_types(self, ops_test: OpsTest):
        """Both catalog-config and postgresql-catalog-config active."""
        postgresql_secret_id = await add_juju_secret(ops_test, "postgresql")
        for app in [APP_NAME, WORKER_NAME]:
            await ops_test.model.grant_secret("postgresql-secret", app)

        catalog_config = await create_catalog_config(
            postgresql_secret_id,
            await add_juju_secret(ops_test, "mysql"),
            await add_juju_secret(ops_test, "redshift"),
            await add_juju_secret(ops_test, "bigquery"),
            await add_juju_secret(ops_test, "gsheets"),
        )
        for app in [APP_NAME, WORKER_NAME]:
            await ops_test.model.grant_secret("mysql-secret", app)
            await ops_test.model.grant_secret("redshift-secret", app)
            await ops_test.model.grant_secret("bigquery-secret", app)
            await ops_test.model.grant_secret("gsheets-secret", app)

        await update_catalog_config(ops_test, catalog_config, TRINO_USER)

        pg_config = pg_catalog_config(POSTGRES_NAME, "testdb*", "dynamic_pg")
        await set_pg_config(ops_test, pg_config)

        await wait_for_catalog(ops_test, "dynamic_pg")
        await wait_for_catalog(ops_test, "postgresql-1")

    async def test_12_remove_relation_keeps_static(self, ops_test: OpsTest):
        """Removing PG relation does not affect static catalogs."""
        await remove_pg_relation(ops_test)

        await wait_for_catalog(ops_test, "dynamic_pg", present=False)
        await wait_for_catalog(ops_test, "postgresql-1")

        # Re-relate for next test
        await relate_pg(ops_test)

    async def test_13_remove_static_keeps_dynamic(self, ops_test: OpsTest):
        """Removing static catalog does not affect dynamic catalog."""
        await wait_for_catalog(ops_test, "dynamic_pg")

        await ops_test.model.applications[APP_NAME].reset_config(
            ["catalog-config"]
        )
        async with ops_test.fast_forward():
            await ops_test.model.wait_for_idle(
                apps=[APP_NAME], status="active", timeout=600
            )

        await wait_for_catalog(ops_test, "postgresql-1", present=False)
        await wait_for_catalog(ops_test, "dynamic_pg")

        await remove_pg_relation(ops_test)

    async def test_14_two_pg_apps(self, ops_test: OpsTest):
        """Two PG apps with separate configs: both catalogs created."""
        await deploy_pg(ops_test, pg_name="pg-second", db_name="seconddb")

        config = yaml.dump(
            {
                POSTGRES_NAME: {
                    "database_prefix": "testdb*",
                    "ro_catalog_name": "pg1_catalog",
                },
                "pg-second": {
                    "database_prefix": "seconddb*",
                    "ro_catalog_name": "pg2_catalog",
                },
            }
        )
        await ops_test.model.applications[APP_NAME].set_config(
            {PG_CONFIG_KEY: config}
        )

        await ops_test.model.integrate(APP_NAME, POSTGRES_NAME)
        await ops_test.model.integrate(APP_NAME, "pg-second")

        async with ops_test.fast_forward():
            await ops_test.model.wait_for_idle(
                apps=[APP_NAME, POSTGRES_NAME, "pg-second"],
                status="active",
                timeout=900,
            )

        await wait_for_catalog(ops_test, "pg1_catalog")
        await wait_for_catalog(ops_test, "pg2_catalog")

    async def test_15_remove_one_relation(self, ops_test: OpsTest):
        """Remove one PG relation: its catalog dropped, other unaffected."""
        await remove_pg_relation(ops_test)

        await wait_for_catalog(ops_test, "pg1_catalog", present=False)
        await wait_for_catalog(ops_test, "pg2_catalog")

    async def test_16_re_add_relation(self, ops_test: OpsTest):
        """Re-add first PG relation: catalog re-created."""
        await relate_pg(ops_test)

        await wait_for_catalog(ops_test, "pg1_catalog")
        await wait_for_catalog(ops_test, "pg2_catalog")

        # Cleanup
        await remove_pg_relation(ops_test)
        await remove_pg_relation(ops_test, "pg-second")
        await destroy_pg(ops_test, "pg-second")

    async def test_17_wrong_app_name_in_config(self, ops_test: OpsTest):
        """Config key doesn't match PG app name: no catalog, charm active."""
        config = pg_catalog_config("wrong-name", "testdb*", "missing_catalog")
        await ops_test.model.applications[APP_NAME].set_config(
            {PG_CONFIG_KEY: config}
        )
        await relate_pg(ops_test)

        await wait_for_catalog(ops_test, "missing_catalog", present=False)
        assert (
            ops_test.model.applications[APP_NAME].units[0].workload_status
            == "active"
        )

        await remove_pg_relation(ops_test)

    async def test_18_container_restart(self, ops_test: OpsTest):
        """Trino container restart: catalog persists."""
        config = pg_catalog_config(POSTGRES_NAME, "testdb*", "persist_catalog")
        await ops_test.model.applications[APP_NAME].set_config(
            {PG_CONFIG_KEY: config}
        )
        await relate_pg(ops_test)

        await wait_for_catalog(ops_test, "persist_catalog")

        unit = ops_test.model.applications[APP_NAME].units[0]
        await ops_test.juju(
            "ssh",
            "--container",
            "trino",
            unit.name,
            "/charm/bin/pebble",
            "restart",
            "trino",
        )

        async with ops_test.fast_forward():
            await ops_test.model.wait_for_idle(
                apps=[APP_NAME],
                status="active",
                timeout=600,
            )

        await wait_for_catalog(ops_test, "persist_catalog")

        # Final cleanup
        await remove_pg_relation(ops_test)
        await ops_test.model.applications[APP_NAME].reset_config(
            [PG_CONFIG_KEY]
        )
        async with ops_test.fast_forward():
            await ops_test.model.wait_for_idle(
                apps=[APP_NAME], status="active", timeout=600
            )

    async def test_19_tls_connection(self, ops_test: OpsTest):
        """Catalog uses TLS when PG has certificates enabled."""
        await ops_test.model.deploy(
            "self-signed-certificates", channel="latest/edge"
        )
        async with ops_test.fast_forward():
            await ops_test.model.wait_for_idle(
                apps=["self-signed-certificates"],
                status="active",
                timeout=600,
            )

        await ops_test.model.integrate(
            "self-signed-certificates:certificates",
            f"{POSTGRES_NAME}:certificates",
        )
        await ops_test.model.integrate(
            f"{POSTGRES_NAME}:client-certificates",
            "self-signed-certificates:certificates",
        )
        async with ops_test.fast_forward():
            await ops_test.model.wait_for_idle(
                apps=[POSTGRES_NAME, "self-signed-certificates"],
                status="active",
                timeout=900,
            )

        config = pg_catalog_config(POSTGRES_NAME, "testdb*", "tls_catalog")
        await ops_test.model.applications[APP_NAME].set_config(
            {PG_CONFIG_KEY: config}
        )
        await relate_pg(ops_test)

        await wait_for_catalog(ops_test, "tls_catalog")

        props = await get_properties_file(ops_test, "tls_catalog")
        assert props is not None
        assert "ssl=true" in props or "ssl\\=true" in props
        assert "sslmode=require" in props or "sslmode\\=require" in props

        # Cleanup
        await remove_pg_relation(ops_test)
        await ops_test.model.applications[APP_NAME].reset_config(
            [PG_CONFIG_KEY]
        )
