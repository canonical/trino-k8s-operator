#!/usr/bin/env python3
# Copyright 2023 Canonical Ltd.
# See LICENSE file for licensing details.

"""Trino policy integration tests."""

import logging
import time

import jubilant
import pytest
from helpers import (
    APP_NAME,
    POSTGRES_NAME,
    RANGER_NAME,
    USER_WITH_ACCESS,
    USER_WITHOUT_ACCESS,
    create_user,
    get_catalogs,
    get_unit_url,
    update_policies,
    wait_for_apps,
)

logger = logging.getLogger(__name__)

TRINO_CONFIG = {
    "ranger-service-name": "trino-service",
    "charm-function": "all",
}


def resolve_ranger_errors(juju: jubilant.Juju):
    """Resolve Ranger error state if any.

    Args:
        juju: The Jubilant Juju instance.
    """
    for unit_name, unit_status in juju.status().apps[RANGER_NAME].units.items():
        if unit_status.workload_status.current == "error":
            logger.info("Resolving error on %s", unit_name)
            juju.cli("resolved", "--retry", unit_name)


@pytest.fixture(name="deploy-policy", scope="module")
def deploy_policy_engine(juju: jubilant.Juju):
    """Add Ranger relation and apply group configuration."""
    juju.deploy(POSTGRES_NAME, channel="14", trust=True)
    wait_for_apps(juju, [POSTGRES_NAME], status="active", timeout=2000)

    juju.deploy(RANGER_NAME, channel="edge", revision=39, trust=True)
    wait_for_apps(juju, [RANGER_NAME], status="blocked", timeout=2000)

    logger.info("Integrating Ranger and PostgreSQL.")
    juju.integrate(RANGER_NAME, POSTGRES_NAME)
    time.sleep(60)
    resolve_ranger_errors(juju)
    wait_for_apps(juju, [POSTGRES_NAME, RANGER_NAME], status="active", timeout=2000)


@pytest.mark.abort_on_fail
@pytest.mark.usefixtures("deploy-policy")
class TestPolicyManager:
    """Integration test for Ranger policy enforcement."""

    def test_policy_enforcement(self, juju: jubilant.Juju, charm: str, charm_image: str):
        """Test Ranger integration."""
        juju.deploy(
            charm,
            APP_NAME,
            resources={"trino-image": charm_image},
            config=TRINO_CONFIG,
            trust=True,
        )

        wait_for_apps(juju, [APP_NAME], status="active", timeout=2000)

        logger.info("Creating test user.")
        url = get_unit_url(juju, application=RANGER_NAME, unit=0, port=6080)
        create_user(url)

        # Integrate Trino and Ranger.
        logger.info("Integrating Trino and Ranger.")
        juju.integrate(RANGER_NAME, APP_NAME)
        time.sleep(30)
        resolve_ranger_errors(juju)
        wait_for_apps(juju, [APP_NAME, RANGER_NAME], status="active", timeout=2000)

        logging.info("update default policies to authorize the new user")
        update_policies(url)

        time.sleep(30)  # wait 30 seconds for the policy to be synced.

        catalogs = get_catalogs(juju, USER_WITH_ACCESS, APP_NAME)
        assert catalogs == [["system"]]
        logger.info("%s can access %s.", USER_WITH_ACCESS, catalogs)

        catalogs = get_catalogs(juju, USER_WITHOUT_ACCESS, APP_NAME)
        logger.info("%s can not access %s.", USER_WITH_ACCESS, catalogs)
        assert catalogs == []
