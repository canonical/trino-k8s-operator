#!/usr/bin/env python3
# Copyright 2023 Canonical Ltd.
# See LICENSE file for licensing details.

"""Integration tests for Trino resource and session property managers."""

import json

import pytest
from conftest import deploy  # noqa: F401, pylint: disable=W0611
from helpers import APP_NAME, TRINO_USER, WORKER_NAME, run_query
from pytest_operator.plugin import OpsTest


def _resource_groups_config(user: str) -> str:
    """Build a simple file-based resource group configuration."""
    return json.dumps(
        {
            "rootGroups": [
                {
                    "name": "global",
                    "softMemoryLimit": "80%",
                    "hardConcurrencyLimit": 5,
                    "maxQueued": 10,
                    "subGroups": [
                        {
                            "name": "interactive",
                            "softMemoryLimit": "50%",
                            "hardConcurrencyLimit": 5,
                            "maxQueued": 10,
                        }
                    ],
                }
            ],
            "selectors": [{"user": user, "group": "global.interactive"}],
        }
    )


SESSION_PROPERTY_MANAGER_CONFIG = json.dumps(
    [
        {
            "group": "global.interactive",
            "sessionProperties": {"query_max_execution_time": "7m"},
        }
    ]
)


async def _set_manager_config(
    ops_test: OpsTest,
    resource_groups_config: str = "",
    session_property_manager_config: str = "",
):
    """Apply manager configuration and wait for Trino to settle."""
    await ops_test.model.applications[APP_NAME].set_config(
        {
            "resource-groups-config": resource_groups_config,
            "session-property-manager-config": session_property_manager_config,
        }
    )
    async with ops_test.fast_forward():
        await ops_test.model.wait_for_idle(
            apps=[APP_NAME, WORKER_NAME],
            status="active",
            raise_on_blocked=False,
            timeout=600,
        )


@pytest.mark.abort_on_fail
@pytest.mark.usefixtures("deploy")
class TestManagers:
    """Integration tests for Trino managers."""

    async def test_resource_group_and_session_property_managers(
        self, ops_test: OpsTest
    ):
        """Verify both managers affect query execution at runtime."""
        try:
            await _set_manager_config(
                ops_test,
                resource_groups_config=_resource_groups_config("nobody"),
                session_property_manager_config=(
                    SESSION_PROPERTY_MANAGER_CONFIG
                ),
            )

            session_rows = await run_query(
                ops_test,
                TRINO_USER,
                "SHOW SESSION LIKE 'query_max_execution_time'",
            )
            assert session_rows
            assert session_rows[0][0] == "query_max_execution_time"
            assert session_rows[0][1] != "7m"

            await _set_manager_config(
                ops_test,
                resource_groups_config=_resource_groups_config(TRINO_USER),
                session_property_manager_config=(
                    SESSION_PROPERTY_MANAGER_CONFIG
                ),
            )

            session_rows = await run_query(
                ops_test,
                TRINO_USER,
                "SHOW SESSION LIKE 'query_max_execution_time'",
            )
            assert session_rows
            assert session_rows[0][0] == "query_max_execution_time"
            assert session_rows[0][1] == "7m"
        finally:
            await _set_manager_config(ops_test)
