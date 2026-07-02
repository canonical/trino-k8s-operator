#!/usr/bin/env python3
# Copyright 2023 Canonical Ltd.
# See LICENSE file for licensing details.

"""Trino charm integration tests."""

import logging

import jubilant
import pytest
from conftest import deploy  # noqa: F401, pylint: disable=W0611
from helpers import (
    APP_NAME,
    TRINO_USER,
    add_juju_secret,
    create_catalog_config,
    curl_unit_ip,
    get_catalogs,
    simulate_crash_and_restart,
    update_catalog_config,
    wait_for_apps,
)

logger = logging.getLogger(__name__)


@pytest.mark.abort_on_fail
@pytest.mark.usefixtures("deploy")
class TestDeployment:
    """Integration tests for Trino charm."""

    def test_trino_ui(self, juju: jubilant.Juju):
        """Perform GET request on the Trino UI host."""
        response = curl_unit_ip(juju)
        assert response.status_code == 200

    def test_basic_client(self, juju: jubilant.Juju):
        """Connects a client and executes a basic SQL query."""
        catalogs = get_catalogs(juju, TRINO_USER, APP_NAME)
        logging.info(f"Found catalogs: {catalogs}")
        assert catalogs

    def test_catalog_config(self, juju: jubilant.Juju):
        """Adds a PostgreSQL and BigQuery connector and asserts catalogs added."""
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
            True,
        )
        catalogs = update_catalog_config(juju, catalog_config, TRINO_USER)

        # Verify that both catalogs have been added.
        assert "postgresql-1" in str(catalogs)
        assert "mysql" in str(catalogs)
        assert "redshift" in str(catalogs)
        assert "bigquery" in str(catalogs)
        assert "gsheets-1" in str(catalogs)

        updated_catalog_config = create_catalog_config(
            postgresql_secret_id,
            mysql_secret_id,
            redshift_secret_id,
            bigquery_secret_id,
            gsheet_secret_id,
            False,
        )

        catalogs = update_catalog_config(juju, updated_catalog_config, TRINO_USER)

        # Verify that only the bigquery catalog has been removed.
        assert "postgresql-1" in str(catalogs)
        assert "mysql" in str(catalogs)
        assert "redshift" in str(catalogs)
        assert "bigquery" not in str(catalogs)
        assert "gsheets-1" in str(catalogs)

    def test_simulate_crash(self, juju: jubilant.Juju, charm: str, charm_image: str):
        """Simulate the crash of the Trino coordinator charm.

        Args:
            juju: Jubilant Juju object.
            charm: charm path.
            charm_image: path to rock image to be used.
        """
        simulate_crash_and_restart(juju, charm, charm_image)
        response = curl_unit_ip(juju)
        assert response.status_code == 200

        catalogs = get_catalogs(juju, TRINO_USER, APP_NAME)
        assert catalogs

    def test_trino_default_policy(self, juju: jubilant.Juju):
        """Update the config and verify no catalog access."""
        juju.config(APP_NAME, {"acl-mode-default": "none"})

        wait_for_apps(juju, [APP_NAME], status="active", timeout=600)
        catalogs = get_catalogs(juju, TRINO_USER, APP_NAME)
        logging.info(f"Found catalogs: {catalogs}")
        assert not catalogs
