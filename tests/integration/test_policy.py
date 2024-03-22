#!/usr/bin/env python3
# Copyright 2023 Canonical Ltd.
# See LICENSE file for licensing details.

"""Trino policy integration tests."""

import logging
import time

import pytest
from helpers import (
    APP_NAME,
    LDAP_NAME,
    POSTGRES_NAME,
    RANGER_NAME,
    USER_WITH_ACCESS,
    USER_WITHOUT_ACCESS,
    USERSYNC_NAME,
    create_policy,
    get_catalogs,
    get_unit_url,
)
from pytest_operator.plugin import OpsTest

logger = logging.getLogger(__name__)


@pytest.mark.abort_on_fail
@pytest.mark.usefixtures("deploy")
class TestPolicyManager:
    """Integration test for Ranger policy enforcement."""

    async def test_policy_enforcement(self, ops_test: OpsTest):
        """Add Ranger relation and apply group configuration."""
        # Deploy and prepare Ranger admin.
        await ops_test.model.deploy(
            POSTGRES_NAME, channel="14/stable", trust=True
        )
        await ops_test.model.deploy(RANGER_NAME, channel="edge")

        await ops_test.model.wait_for_idle(
            apps=[POSTGRES_NAME],
            status="active",
            raise_on_blocked=False,
            timeout=1200,
        )
        await ops_test.model.integrate(RANGER_NAME, POSTGRES_NAME)

        await ops_test.model.wait_for_idle(
            apps=[POSTGRES_NAME, RANGER_NAME],
            status="active",
            raise_on_blocked=False,
            timeout=1200,
        )

        # Deploy and prepare Ranger usersync.
        await ops_test.model.deploy(LDAP_NAME, channel="edge")
        await ops_test.model.wait_for_idle(
            apps=[LDAP_NAME],
            status="active",
            raise_on_blocked=False,
            timeout=1500,
        )
        action = (
            await ops_test.model.applications[LDAP_NAME]
            .units[0]
            .run_action("load-test-users")
        )
        await action.wait()

        usersync_config = {"charm-function": "usersync"}
        await ops_test.model.deploy(
            RANGER_NAME,
            channel="edge",
            config=usersync_config,
            application_name=USERSYNC_NAME,
        )
        await ops_test.model.integrate(USERSYNC_NAME, LDAP_NAME)

        # Integrate Trino and Ranger.
        logging.info("integrating trino and ranger")
        await ops_test.model.integrate(RANGER_NAME, APP_NAME)
        await ops_test.model.wait_for_idle(
            apps=[APP_NAME, RANGER_NAME, USERSYNC_NAME, LDAP_NAME],
            status="active",
            raise_on_blocked=False,
            timeout=1200,
        )
        # Test policy implementation.
        url = await get_unit_url(
            ops_test, application=RANGER_NAME, unit=0, port=6080
        )
        logging.info(f"creating test policies for {url}")
        await create_policy(ops_test, url)

        # wait 3 minutes for the policy to be synced.
        time.sleep(180)

        catalogs = await get_catalogs(ops_test, USER_WITH_ACCESS, APP_NAME)
        assert catalogs == [["system"]]
        logger.info(f"{USER_WITH_ACCESS} can access {catalogs}.")
        catalogs = await get_catalogs(ops_test, USER_WITHOUT_ACCESS, APP_NAME)
        logger.info(f"{USER_WITHOUT_ACCESS}, cam access {catalogs}.")
        assert catalogs == []
