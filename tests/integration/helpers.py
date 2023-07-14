#!/usr/bin/env python3
# Copyright 2023 Canonical Ltd.
# See LICENSE file for licensing details.

"""Trino charm integration test helpers."""

import logging
from pathlib import Path
import yaml
from pytest_operator.plugin import OpsTest

logger = logging.getLogger(__name__)

METADATA = yaml.safe_load(Path("./metadata.yaml").read_text())
APP_NAME = METADATA["name"]
TLS_NAME = "tls-certificates-operator"
NGINX_NAME = "nginx-ingress-integrator"

async def perform_trino_integrations(ops_test: OpsTest):
    """Integrate Trino charm with TLS and Nginx charms.

    Args:
        ops_test: PyTest object.
    """
    await ops_test.model.integrate(f"{APP_NAME}", f"{TLS_NAME}")
    await ops_test.model.wait_for_idle(apps=[APP_NAME], status="active", raise_on_blocked=False, timeout=180)

    await ops_test.model.integrate(f"{APP_NAME}", f"{NGINX_NAME}")
    await ops_test.model.wait_for_idle(
        apps=[APP_NAME, NGINX_NAME], status="active", raise_on_blocked=False, timeout=180)

    assert ops_test.model.applications[APP_NAME].units[0].workload_status == "active"

async def get_unit_url(ops_test: OpsTest, application, unit, port, protocol="https"):
    """Returns unit URL from the model.

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
    address = status["applications"][application]["units"][f"{application}/{unit}"]["address"]
    return f"{protocol}://{address}:{port}"
