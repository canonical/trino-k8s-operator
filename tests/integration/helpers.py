#!/usr/bin/env python3
# Copyright 2023 Canonical Ltd.
# See LICENSE file for licensing details.

"""Trino charm integration test helpers."""

import json
import logging
import os
import time
from pathlib import Path

import requests
import yaml
from apache_ranger.client.ranger_client import RangerClient
from apache_ranger.model.ranger_policy import (
    RangerPolicy,
    RangerPolicyItem,
    RangerPolicyItemAccess,
    RangerPolicyResource,
)
from pytest_operator.plugin import OpsTest
from trino_client.show_catalogs import show_catalogs

logger = logging.getLogger(__name__)


BASE_DIR = os.path.abspath(
    os.path.join(os.path.dirname(os.path.realpath(__file__)), "..", "..")
)
METADATA = yaml.safe_load(Path(f"{BASE_DIR}/metadata.yaml").read_text())
TRINO_IMAGE = {
    "trino-image": METADATA["resources"]["trino-image"]["upstream-source"]
}

# Charm name literals
APP_NAME = METADATA["name"]
WORKER_NAME = f"{APP_NAME}-worker"
POSTGRES_NAME = "postgresql-k8s"
NGINX_NAME = "nginx-ingress-integrator"

# Database configuration literals
EXAMPLE_CATALOG_NAME = "example-db"
TEMP_CATALOG_NAME = "temp-db"
EXAMPLE_CATALOG_CONFIG = """\
example-db: |
  connector.name=postgresql
  connection-url=jdbc:postgresql://host.com:5432/database
  connection-user=testing
  connection-password=test
"""
TEMP_CATALOG_CONFIG = """\
temp-db: |
  connector.name=postgresql
  connection-url=jdbc:postgresql://host.com:5432/temp-db
  connection-user=testing
  connection-password=test
"""
TRINO_USER = "trino"

# Ranger policy literals
RANGER_NAME = "ranger-k8s"
RANGER_AUTH = ("admin", "rangerR0cks!")
TRINO_SERVICE = "trino-service"
USER_WITH_ACCESS = "dev"
USER_WITHOUT_ACCESS = "user"
POLICY_NAME = "system - catalog, schema, table, column"

HEADERS = {
    "Accept": "application/json",
    "Content-Type": "application/json",
}
DEV_USER = {
    "name": USER_WITH_ACCESS,
    "password": "aP6X1HhJe6Toui!",
    "firstName": USER_WITH_ACCESS,
    "lastName": "user",
    "emailAddress": "dev@example.com",
}

# Upgrades secure password
SECURE_PWD = "Xh0DAbGvxLI3NY!"  # nosec


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


async def get_catalogs(ops_test: OpsTest, user, app_name):
    """Return a list of catalogs from Trino charm.

    Args:
        ops_test: PyTest object
        user: the user to access Trino with
        app_name: name of the application

    Returns:
        catalogs: list of catalogs connected to trino
    """
    status = await ops_test.model.get_status()  # noqa: F821
    address = status["applications"][app_name]["units"][f"{app_name}/{0}"][
        "address"
    ]
    logger.info("executing query on app address: %s", address)
    catalogs = await show_catalogs(address, user)
    return catalogs


async def update_catalog_config(ops_test, catalog_config, user):
    """Run connection action.

    Args:
        ops_test: PyTest object.
        catalog_config: The catalogs configuration value.
        user: the user to access Trino with.

    Returns:
        A string of trino catalogs.
    """
    await ops_test.model.applications[APP_NAME].set_config(
        {"catalog-config": catalog_config}
    )
    async with ops_test.fast_forward():
        await ops_test.model.wait_for_idle(status="active", timeout=600)
    catalogs = await get_catalogs(ops_test, user, APP_NAME)
    logging.info(f"Catalogs: {catalogs}")
    return str(catalogs)


async def create_user(ops_test, ranger_url):
    """Create Ranger user.

    Args:
        ops_test: PyTest object
        ranger_url: the policy manager url
    """
    url = f"{ranger_url}/service/xusers/users"
    response = requests.post(
        url, headers=HEADERS, json=DEV_USER, auth=RANGER_AUTH, timeout=20
    )
    r = json.loads(response.text)
    logger.info(r)


async def create_policy(ops_test, ranger_url):
    """Create a Ranger user policy.

    Allow user `user1` to access `system` catalog.

    Args:
        ops_test: PyTest object
        ranger_url: the polciy manager url
    """
    ranger = RangerClient(ranger_url, RANGER_AUTH)
    policy = RangerPolicy()
    policy.service = TRINO_SERVICE
    policy.name = POLICY_NAME
    policy.resources = {
        "schema": RangerPolicyResource({"values": ["*"]}),
        "catalog": RangerPolicyResource({"values": ["system"]}),
        "table": RangerPolicyResource({"values": ["*"]}),
        "column": RangerPolicyResource({"values": ["*"]}),
    }

    allow_items = RangerPolicyItem()
    allow_items.users = [USER_WITH_ACCESS]
    allow_items.accesses = [RangerPolicyItemAccess({"type": "select"})]
    policy.policyItems = [allow_items]
    ranger.create_policy(policy)
