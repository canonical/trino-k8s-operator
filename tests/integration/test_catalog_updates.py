#!/usr/bin/env python3
# Copyright 2023 Canonical Ltd.
# See LICENSE file for licensing details.

"""Trino charm integration tests for catalog config updates."""

import logging

import jubilant
import pytest
from conftest import deploy  # noqa: F401, pylint: disable=W0611
from helpers import (
    APP_NAME,
    TRINO_USER,
    WORKER_NAME,
    add_juju_secret,
    create_catalog_config,
    get_catalogs,
    update_catalog_config,
    wait_for_apps,
)

logger = logging.getLogger(__name__)


@pytest.mark.incremental
@pytest.mark.usefixtures("deploy")
class TestCatalogUpdates:
    """Integration tests for catalog changes in Trino charm."""

    def test_catalog_config_updates_after_relation_break(self, juju: jubilant.Juju):
        """Test that the worker picks up updated catalog-config after relation re-establishment."""
        # Step 1: create and apply catalog config with BigQuery
        postgresql_secret_id = add_juju_secret(juju, "postgresql")
        mysql_secret_id = add_juju_secret(juju, "mysql")
        redshift_secret_id = add_juju_secret(juju, "redshift")
        bigquery_secret_id = add_juju_secret(juju, "bigquery")
        gsheet_secret_id = add_juju_secret(juju, "gsheets")

        apps = ["trino-k8s", "trino-k8s-worker"]
        juju.grant_secret("postgresql-secret", apps)
        juju.grant_secret("mysql-secret", apps)
        juju.grant_secret("redshift-secret", apps)
        juju.grant_secret("bigquery-secret", apps)
        juju.grant_secret("gsheets-secret", apps)

        catalog_config = create_catalog_config(
            postgresql_secret_id,
            mysql_secret_id,
            redshift_secret_id,
            bigquery_secret_id,
            gsheet_secret_id,
            True,  # include_bigquery
        )
        update_catalog_config(juju, catalog_config, TRINO_USER)

        # Assert that bigquery is present
        catalogs = get_catalogs(juju, TRINO_USER, APP_NAME)
        assert "bigquery" in str(catalogs)

        # Step 2: Remove relation between coordinator and worker
        juju.remove_relation(f"{APP_NAME}:trino-coordinator", f"{WORKER_NAME}:trino-worker")
        wait_for_apps(juju, [APP_NAME, WORKER_NAME], status="blocked", timeout=600)

        # Step 3: Update catalog config to remove bigquery (DO NOT use config-changed after this)
        updated_catalog_config = create_catalog_config(
            postgresql_secret_id,
            mysql_secret_id,
            redshift_secret_id,
            bigquery_secret_id,
            gsheet_secret_id,
            False,  # now exclude bigquery
        )

        juju.config(APP_NAME, {"catalog-config": updated_catalog_config})
        wait_for_apps(juju, [APP_NAME], status="blocked", timeout=300)

        # Step 4: Re-establish relation
        juju.integrate(f"{APP_NAME}:trino-coordinator", f"{WORKER_NAME}:trino-worker")
        wait_for_apps(juju, [APP_NAME, WORKER_NAME], status="active", timeout=900)

        # Step 5: Check the catalogs (expect bigquery to be removed)
        catalogs = get_catalogs(juju, TRINO_USER, APP_NAME)
        assert "bigquery" not in str(catalogs), (
            "BigQuery should be removed after relation re-establishment"
        )
        assert "postgresql-1" in str(catalogs)
        assert "mysql" in str(catalogs)
        assert "redshift" in str(catalogs)
        assert "gsheets-1" in str(catalogs)
