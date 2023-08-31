#!/usr/bin/env python3
# Copyright 2023 Canonical Ltd.
# See LICENSE file for licensing details.

"""Trino charm integration test helpers."""

import logging
import time
from pathlib import Path

import yaml
from pytest_operator.plugin import OpsTest
from trino_client.show_catalogs import show_catalogs

logger = logging.getLogger(__name__)

CONN_NAME = "connection-test"
METADATA = yaml.safe_load(Path("./metadata.yaml").read_text())
APP_NAME = METADATA["name"]
NGINX_NAME = "nginx-ingress-integrator"
CONN_CONFIG = """connector.name=postgresql
connection-url=jdbc:postgresql://example.host.com:5432/test
connection-user=trino
connection-password=trino
"""


async def get_unit_url(
    ops_test: OpsTest, application, unit, port, protocol="http"
):
    """Return unit URL from the model.

    Args:
        ops_test: PyTest object.
        application: Name of the application.
        unit: Number of the unit.
        port: Port number of the URL.
        protocol: Transfer protocol (default: http).

    Returns:
        Unit URL of the form {protocol}://{address}:{port}
    """
    status = await ops_test.model.get_status()  # noqa: F821
    address = status["applications"][application]["units"][
        f"{application}/{unit}"
    ]["address"]
    return f"{protocol}://{address}:{port}"


async def get_catalogs(ops_test: OpsTest):
    """Return a list of catalogs from Trino charm.

    Args:
        ops_test: PyTest object

    Returns:
        catalogs: list of catalogs connected to trino
    """
    status = await ops_test.model.get_status()  # noqa: F821
    address = status["applications"][APP_NAME]["units"][f"{APP_NAME}/{0}"][
        "address"
    ]
    logger.info("executing query on app address: %s", address)
    catalogs = await show_catalogs(address)
    return catalogs


async def run_connector_action(ops_test, action, params):
    """Run connection action.

    Args:
        ops_test: PyTest
        action: either add-connection or remove-connection action
        params: action parameters

    Returns:
        catalogs: list of trino catalogs after action
    """
    action = (
        await ops_test.model.applications[APP_NAME]
        .units[0]
        .run_action(action, **params)
    )
    await action.wait()
    time.sleep(40)
    catalogs = await get_catalogs(ops_test)
    logging.info(f"action {action} run, catalogs: {catalogs}")
    return catalogs
