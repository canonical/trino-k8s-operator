# Copyright 2023 Canonical Ltd.
# See LICENSE file for licensing details.

"""Integration tests for the Trino-PostgreSQL catalog relation."""

import logging
import time

import jubilant
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
    fast_forward_ctx,
    get_catalogs,
    get_unit,
    scale,
    update_catalog_config,
    wait_for_app_gone,
    wait_for_apps,
)
from trino_client.trino_client import query_trino

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


def deploy_pg(juju: jubilant.Juju, pg_name=POSTGRES_NAME, db_name="testdb", units=1):
    """Deploy PostgreSQL and a data-integrator to create a database.

    The DI is needed because PG's prefix matching only discovers existing
    databases, it does not create them from a prefix request.

    Args:
        juju: Jubilant Juju object.
        pg_name: Application name for PG.
        db_name: Database name for the data-integrator to create.
        units: Number of PG units.
    """
    di_name = f"di-{pg_name}"
    juju.deploy("postgresql-k8s", pg_name, channel=PG_CHANNEL, num_units=units, trust=True)
    juju.deploy(
        "data-integrator",
        di_name,
        channel="latest/edge",
        config={"database-name": db_name},
    )
    juju.integrate(f"{pg_name}:database", di_name)
    wait_for_apps(juju, [pg_name], status="active", timeout=900, wait_for_exact_units=units)
    wait_for_apps(juju, [di_name], status="active", timeout=900)


def wait_for_idle_pg(juju: jubilant.Juju, pg_name=POSTGRES_NAME):
    """Wait for PG and Trino to be idle and active.

    Args:
        juju: Jubilant Juju object.
        pg_name: PG application name.
    """
    wait_for_apps(juju, [APP_NAME, WORKER_NAME, pg_name], status="active", timeout=900)


def destroy_pg(juju: jubilant.Juju, pg_name):
    """Destroy a PG app and its associated data-integrator.

    Args:
        juju: Jubilant Juju object.
        pg_name: Application name for PG.
    """
    di_name = f"di-{pg_name}"
    juju.remove_application(di_name)
    juju.remove_application(pg_name)
    wait_for_app_gone(juju, di_name)
    wait_for_app_gone(juju, pg_name)


def relate_pg(juju: jubilant.Juju, pg_name=POSTGRES_NAME):
    """Integrate Trino with PG and wait for idle.

    Args:
        juju: Jubilant Juju object.
        pg_name: PG application name.
    """
    juju.integrate(APP_NAME, pg_name)
    wait_for_idle_pg(juju, pg_name)


def remove_pg_relation(juju: jubilant.Juju, pg_name=POSTGRES_NAME):
    """Remove the Trino-PG relation and wait for idle.

    Args:
        juju: Jubilant Juju object.
        pg_name: PG application name.
    """
    juju.remove_relation(APP_NAME, pg_name)
    wait_for_apps(juju, [APP_NAME], status="active", timeout=600)

    # Wait for the relation to be fully removed from Juju's state
    # to avoid "relation is dying, but not yet removed" errors on re-integrate.
    deadline = time.time() + 300
    while time.time() < deadline:
        status = juju.status()
        app_status = status.apps.get(APP_NAME)
        if app_status is None:
            break
        pg_relations = app_status.relations.get("postgresql", [])
        if not any(r.related_app == pg_name for r in pg_relations):
            break
        time.sleep(2)
    else:
        raise TimeoutError(f"Relation {APP_NAME}:postgresql {pg_name} still present after 300s")


def set_pg_config(juju: jubilant.Juju, config_str, expect_blocked=False):
    """Set postgresql-catalog-config and wait for idle.

    Args:
        juju: Jubilant Juju object.
        config_str: YAML config string.
        expect_blocked: When True, wait for APP_NAME to reach blocked status
            instead of active. Used when the config is intentionally invalid.
    """
    juju.config(APP_NAME, {PG_CONFIG_KEY: config_str})
    if expect_blocked:
        wait_for_apps(juju, [APP_NAME], status="blocked", timeout=900)
    else:
        wait_for_apps(juju, [APP_NAME, WORKER_NAME, POSTGRES_NAME], status="active", timeout=900)


def wait_for_catalog(juju: jubilant.Juju, catalog_name, present=True, timeout=300):
    """Poll SHOW CATALOGS until catalog appears/disappears or timeout.

    Args:
        juju: Jubilant Juju object.
        catalog_name: Catalog name to check.
        present: True to wait for appearance, False for disappearance.
        timeout: Seconds before giving up.

    Returns:
        The catalogs list on success.

    Raises:
        TimeoutError: If the expected state isn't reached.
    """
    deadline = time.time() + timeout
    # Fast-forward update-status so a failed/incomplete reconcile is retried
    # promptly while we poll, instead of at the slow default hook interval.
    with fast_forward_ctx(juju, "10s"):
        while time.time() < deadline:
            try:
                catalogs = get_catalogs(juju, TRINO_USER, APP_NAME)
                found = catalog_name in str(catalogs)
                if found == present:
                    return catalogs
            except Exception:  # nosec
                pass  # nosec
            time.sleep(5)
    state = "not found" if present else "still present"
    raise TimeoutError(f"Catalog {catalog_name!r} {state} after {timeout}s")


def query_pg_catalog(juju: jubilant.Juju, catalog_name):
    """Run SHOW SCHEMAS against a catalog to verify connectivity.

    Args:
        juju: Jubilant Juju object.
        catalog_name: Name of the Trino catalog.

    Returns:
        List of schemas
    """
    address = get_unit(juju, APP_NAME).address
    return query_trino(address, TRINO_USER, f'SHOW SCHEMAS FROM "{catalog_name}"')


def get_properties_file(juju: jubilant.Juju, catalog_name):
    """Read a catalog .properties file from the Trino container.

    Args:
        juju: Jubilant Juju object.
        catalog_name: Name of the catalog.

    Returns:
        File contents as string, or None if not found.
    """
    try:
        return juju.ssh(
            f"{APP_NAME}/0",
            "cat",
            f"{CATALOG_DIR}/{catalog_name}.properties",
            container="trino",
        )
    except Exception:  # nosec
        return None


@pytest.mark.abort_on_fail
@pytest.mark.usefixtures("deploy")
class TestPostgresqlCatalogRelation:
    """Integration tests for PostgreSQL catalog relation."""

    def test_01_missing_database_prefix(self, juju: jubilant.Juju):
        """Config without database_prefix: charm goes blocked with validation error."""
        config = yaml.dump({POSTGRES_NAME: {"ro_catalog_name": "test_ro"}})
        deploy_pg(juju)
        relate_pg(juju)
        set_pg_config(juju, config, expect_blocked=True)

        unit = get_unit(juju, APP_NAME)
        assert unit.workload_status.current == "blocked"
        assert "database_prefix" in unit.workload_status.message

    def test_02_prefix_without_asterisk(self, juju: jubilant.Juju):
        """Config with database_prefix missing *: charm goes blocked with validation error."""
        config = pg_catalog_config(POSTGRES_NAME, "mydb", "test_ro")
        set_pg_config(juju, config, expect_blocked=True)

        unit = get_unit(juju, APP_NAME)
        assert unit.workload_status.current == "blocked"
        assert "database_prefix" in unit.workload_status.message

    def test_03_missing_catalog_names(self, juju: jubilant.Juju):
        """Config with database_prefix but no catalog names: charm goes blocked."""
        config = yaml.dump({POSTGRES_NAME: {"database_prefix": "testdb*"}})
        set_pg_config(juju, config, expect_blocked=True)

        unit = get_unit(juju, APP_NAME)
        assert unit.workload_status.current == "blocked"
        assert "ro_catalog_name" in unit.workload_status.message

    def test_04_fix_invalid_config(self, juju: jubilant.Juju):
        """After fixing the invalid config from test_03, charm recovers and catalog is created."""
        config = pg_catalog_config(POSTGRES_NAME, "testdb*", "test_catalog")
        set_pg_config(juju, config)

        wait_for_catalog(juju, "test_catalog")

        schemas = query_pg_catalog(juju, "test_catalog")
        schema_names = [s[0] for s in schemas]
        assert "public" in schema_names

    def test_05_both_ro_and_rw(self, juju: jubilant.Juju):
        """Config with both ro and rw: two catalogs created."""
        config = pg_catalog_config(POSTGRES_NAME, "testdb*", "mydb_ro", rw_name="mydb_rw")
        set_pg_config(juju, config)

        wait_for_catalog(juju, "mydb_ro")
        wait_for_catalog(juju, "mydb_rw")

        ro_props = get_properties_file(juju, "mydb_ro")
        rw_props = get_properties_file(juju, "mydb_rw")
        assert ro_props is not None
        assert rw_props is not None
        assert "preferSecondary" in ro_props
        assert "primary" in rw_props

        ro_schemas = query_pg_catalog(juju, "mydb_ro")
        rw_schemas = query_pg_catalog(juju, "mydb_rw")
        assert any("public" in s for s in ro_schemas)
        assert any("public" in s for s in rw_schemas)

    def test_06_remove_rw(self, juju: jubilant.Juju):
        """Remove rw_catalog_name: RW catalog dropped, RO remains."""
        config = pg_catalog_config(POSTGRES_NAME, "testdb*", "mydb_ro")
        set_pg_config(juju, config)

        wait_for_catalog(juju, "mydb_ro")
        wait_for_catalog(juju, "mydb_rw", present=False)

    def test_07_add_rw(self, juju: jubilant.Juju):
        """Add rw_catalog_name: RW catalog appears alongside RO."""
        config = pg_catalog_config(POSTGRES_NAME, "testdb*", "mydb_ro", rw_name="mydb_rw")
        set_pg_config(juju, config)

        wait_for_catalog(juju, "mydb_ro")
        wait_for_catalog(juju, "mydb_rw")

    def test_08_pg_scaling(self, juju: jubilant.Juju):
        """Catalog URL gains replicas service address after PG scales up."""
        config = pg_catalog_config(POSTGRES_NAME, "testdb*", "multihost_ro")
        set_pg_config(juju, config)
        wait_for_catalog(juju, "multihost_ro")

        # With 1 PG unit there are no replicas, URL has only the primary
        props = get_properties_file(juju, "multihost_ro")
        assert props is not None
        assert f"{POSTGRES_NAME}-primary" in props
        assert f"{POSTGRES_NAME}-replicas" not in props

        # Scale PG to 3, replicas service should appear in the URL
        scale(juju, POSTGRES_NAME, 3)
        wait_for_apps(juju, [APP_NAME], status="active", timeout=600)

        wait_for_catalog(juju, "multihost_ro")
        props = get_properties_file(juju, "multihost_ro")
        assert props is not None
        assert f"{POSTGRES_NAME}-primary" in props
        assert f"{POSTGRES_NAME}-replicas" in props

    def test_09_both_config_types(self, juju: jubilant.Juju):
        """Both catalog-config and postgresql-catalog-config active."""
        postgresql_secret_id = add_juju_secret(juju, "postgresql")
        for app in [APP_NAME, WORKER_NAME]:
            juju.grant_secret("postgresql-secret", app)

        catalog_config = create_catalog_config(
            postgresql_secret_id,
            add_juju_secret(juju, "mysql"),
            add_juju_secret(juju, "redshift"),
            add_juju_secret(juju, "bigquery"),
            add_juju_secret(juju, "gsheets"),
        )

        apps = [APP_NAME, WORKER_NAME]
        juju.grant_secret("mysql-secret", apps)
        juju.grant_secret("redshift-secret", apps)
        juju.grant_secret("bigquery-secret", apps)
        juju.grant_secret("gsheets-secret", apps)

        update_catalog_config(juju, catalog_config, TRINO_USER)

        pg_config = pg_catalog_config(POSTGRES_NAME, "testdb*", "dynamic_pg")
        set_pg_config(juju, pg_config)

        wait_for_catalog(juju, "dynamic_pg")
        wait_for_catalog(juju, "postgresql-1")

    def test_10_remove_relation_keeps_static(self, juju: jubilant.Juju):
        """Removing PG relation does not affect static catalogs."""
        remove_pg_relation(juju)

        wait_for_catalog(juju, "dynamic_pg", present=False)
        wait_for_catalog(juju, "postgresql-1")

        # Re-relate for next test
        relate_pg(juju)

    def test_11_remove_static_keeps_dynamic(self, juju: jubilant.Juju):
        """Removing static catalog does not affect dynamic catalog."""
        wait_for_catalog(juju, "dynamic_pg")

        juju.config(APP_NAME, reset=["catalog-config"])
        wait_for_apps(juju, [APP_NAME], status="active", timeout=600)

        wait_for_catalog(juju, "postgresql-1", present=False)
        wait_for_catalog(juju, "dynamic_pg")

        remove_pg_relation(juju)

    def test_12_two_pg_apps(self, juju: jubilant.Juju):
        """Two PG apps with separate configs: both catalogs created."""
        deploy_pg(juju, pg_name="pg-second", db_name="seconddb")

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
        juju.config(APP_NAME, {PG_CONFIG_KEY: config})

        juju.integrate(APP_NAME, POSTGRES_NAME)
        juju.integrate(APP_NAME, "pg-second")

        wait_for_apps(
            juju,
            [APP_NAME, WORKER_NAME, POSTGRES_NAME, "pg-second"],
            status="active",
            timeout=900,
        )

        wait_for_catalog(juju, "pg1_catalog")
        wait_for_catalog(juju, "pg2_catalog")

    def test_13_remove_one_relation(self, juju: jubilant.Juju):
        """Remove one PG relation: its catalog dropped, other unaffected."""
        remove_pg_relation(juju)

        wait_for_catalog(juju, "pg1_catalog", present=False)
        wait_for_catalog(juju, "pg2_catalog")

    def test_14_re_add_relation(self, juju: jubilant.Juju):
        """Re-add first PG relation: catalog re-created."""
        relate_pg(juju)

        wait_for_catalog(juju, "pg1_catalog")
        wait_for_catalog(juju, "pg2_catalog")

        # Cleanup
        remove_pg_relation(juju)
        remove_pg_relation(juju, "pg-second")
        destroy_pg(juju, "pg-second")

    def test_15_wrong_app_name_in_config(self, juju: jubilant.Juju):
        """Config key doesn't match PG app name: no catalog, charm active."""
        config = pg_catalog_config("wrong-name", "testdb*", "missing_catalog")
        juju.config(APP_NAME, {PG_CONFIG_KEY: config})
        relate_pg(juju)

        wait_for_catalog(juju, "missing_catalog", present=False)
        assert get_unit(juju, APP_NAME).workload_status.current == "active"

        remove_pg_relation(juju)

    def test_16_container_restart(self, juju: jubilant.Juju):
        """Trino container restart: catalog persists."""
        config = pg_catalog_config(POSTGRES_NAME, "testdb*", "persist_catalog")
        juju.config(APP_NAME, {PG_CONFIG_KEY: config})
        relate_pg(juju)

        wait_for_catalog(juju, "persist_catalog")

        schemas = query_pg_catalog(juju, "persist_catalog")
        assert any("public" in s for s in schemas)

        juju.ssh(f"{APP_NAME}/0", "/charm/bin/pebble", "restart", "trino", container="trino")

        wait_for_apps(juju, [APP_NAME, WORKER_NAME], status="active", timeout=600)

        wait_for_catalog(juju, "persist_catalog")

        schemas = query_pg_catalog(juju, "persist_catalog")
        assert any("public" in s for s in schemas)

        # Final cleanup
        remove_pg_relation(juju)
        juju.config(APP_NAME, reset=[PG_CONFIG_KEY])
        wait_for_apps(juju, [APP_NAME], status="active", timeout=600)

    def test_17_tls_connection(self, juju: jubilant.Juju):
        """Catalog uses TLS when PG has certificates enabled."""
        juju.deploy("self-signed-certificates", channel="latest/edge")
        wait_for_apps(juju, ["self-signed-certificates"], status="active", timeout=600)

        juju.integrate(
            f"{POSTGRES_NAME}:client-certificates",
            "self-signed-certificates:certificates",
        )
        wait_for_apps(
            juju,
            [POSTGRES_NAME, "self-signed-certificates"],
            status="active",
            timeout=900,
        )

        config = pg_catalog_config(POSTGRES_NAME, "testdb*", "tls_catalog")
        juju.config(APP_NAME, {PG_CONFIG_KEY: config})
        relate_pg(juju)

        wait_for_catalog(juju, "tls_catalog")

        props = get_properties_file(juju, "tls_catalog")
        assert props is not None
        assert "ssl=true" in props or "ssl\\=true" in props
        assert "sslmode=require" in props or "sslmode\\=require" in props

        schemas = query_pg_catalog(juju, "tls_catalog")
        assert any("public" in s for s in schemas)

        # Cleanup
        remove_pg_relation(juju)
        juju.config(APP_NAME, reset=[PG_CONFIG_KEY])
