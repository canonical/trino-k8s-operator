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
TLS_NAME = "tls-certificates-operator"
NGINX_NAME = "nginx-ingress-integrator"
PLACEHOLDER_PWD = "testpwd123"
CONN_CONFIG = """connector.name=postgresql
connection-url=jdbc:postgresql://example.host.com:5432/test
connection-user=trino
connection-password=trino
"""


async def perform_trino_integrations(ops_test: OpsTest):
    """Integrate Trino charm with TLS and Nginx charms.

    Args:
        ops_test: PyTest object.
    """
    await ops_test.model.integrate(APP_NAME, TLS_NAME)
    await ops_test.model.wait_for_idle(
        apps=[APP_NAME], status="active", raise_on_blocked=False, timeout=180
    )

    await ops_test.model.integrate(APP_NAME, NGINX_NAME)


async def get_unit_url(
    ops_test: OpsTest, application, unit, port, protocol="https"
):
    """Return unit URL from the model.

    Args:
        ops_test: PyTest object.
        application: Name of the application.
        unit: Number of the unit.
        port: Port number of the URL.
        protocol: Transfer protocol (default: https).

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
    catalogs = await show_catalogs(address, PLACEHOLDER_PWD)
    return catalogs


async def run_connector_action(ops_test, action):
    """Run connection action.

    Args:
        ops_test: PyTest
        action: either add-connection or remove-connection action

    Returns:
        catalogs: list of trino catalogs after action
    """
    params = {
        "conn-name": CONN_NAME,
        "conn-config": CONN_CONFIG,
    }
    action = (
        await ops_test.model.applications[APP_NAME]
        .units[0]
        .run_action(action, **params)
    )
    await action.wait()
    time.sleep(30)
    catalogs = await get_catalogs(ops_test)
    logging.info(f"action {action} run, catalogs: {catalogs}")
    return catalogs
