#!/usr/bin/env python3
# Copyright 2023 Canonical Ltd.
# See LICENSE file for licensing details.

"""Integration tests for Trino resource and session property managers."""

import json

import jubilant
import pytest
import trino.exceptions
from conftest import deploy  # noqa: F401, pylint: disable=W0611
from helpers import APP_NAME, TRINO_USER, WORKER_NAME, run_query, wait_for_apps


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


def _set_manager_config(
    juju: jubilant.Juju,
    resource_groups_config: str = "",
    session_property_manager_config: str = "",
):
    """Apply manager configuration and wait for Trino to settle."""
    juju.config(
        APP_NAME,
        {
            "resource-groups-config": resource_groups_config,
            "session-property-manager-config": session_property_manager_config,
        },
    )
    wait_for_apps(
        juju,
        [APP_NAME, WORKER_NAME],
        status="active",
        timeout=600,
    )


@pytest.mark.abort_on_fail
@pytest.mark.usefixtures("deploy")
class TestManagers:
    """Integration tests for Trino managers."""

    def test_resource_group_and_session_property_managers(self, juju: jubilant.Juju):
        """Verify both managers affect query execution at runtime."""
        try:
            _set_manager_config(
                juju,
                resource_groups_config=_resource_groups_config("nobody"),
                session_property_manager_config=(SESSION_PROPERTY_MANAGER_CONFIG),
            )

            with pytest.raises(trino.exceptions.TrinoUserError) as exc_info:
                run_query(
                    juju,
                    TRINO_USER,
                    "SHOW SESSION LIKE 'query_max_execution_time'",
                )

            assert "No matching resource group found" in str(exc_info.value)

            _set_manager_config(
                juju,
                resource_groups_config=_resource_groups_config(TRINO_USER),
                session_property_manager_config=(SESSION_PROPERTY_MANAGER_CONFIG),
            )

            session_rows = run_query(
                juju,
                TRINO_USER,
                "SHOW SESSION LIKE 'query_max_execution_time'",
            )
            assert session_rows
            assert session_rows[0][0] == "query_max_execution_time"
            assert session_rows[0][1] == "7m"
        finally:
            _set_manager_config(juju)
