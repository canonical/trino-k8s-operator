#!/usr/bin/env python3
# Copyright 2023 Canonical Ltd.
# See LICENSE file for licensing details.

"""Trino charm integration test helpers."""

import logging
import time
from pathlib import Path

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

RANGER_AUTH = ("admin", "rangerR0cks!")
CONN_NAME = "connection-test"
METADATA = yaml.safe_load(Path("./metadata.yaml").read_text())
APP_NAME = METADATA["name"]
WORKER_NAME = f"{APP_NAME}-worker"
POSTGRES_NAME = "postgresql-k8s"
NGINX_NAME = "nginx-ingress-integrator"
CONN_CONFIG = """connector.name=postgresql
connection-url=jdbc:postgresql://example.host.com:5432/test
connection-user=trino
connection-password=trino
"""
RANGER_NAME = "ranger-k8s"
GROUP_MANAGEMENT = """\
    trino-service:
        users:
          - name: user1
            firstname: One
            lastname: User
            email: user1@canonical.com
        memberships:
          - groupname: commercial-systems
            users: [user1]
        groups:
          - name: commercial-systems
            description: commercial systems team
"""
TRINO_USER = "trino"
TRINO_SERVICE = "trino-service"
USER_WITH_ACCESS = "user1"
USER_WITHOUT_ACCESS = "user2"
GROUP_WITH_ACCESS = "commercial-systems"
POLICY_NAME = "tpch - catalog, schema, table, column"
TRINO_POLICY = "trino-k8s-policy"


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


async def run_connector_action(ops_test, action, params, user):
    """Run connection action.

    Args:
        ops_test: PyTest object
        action: either add-connection or remove-connection action
        params: action parameters
        user: the user to access Trino with

    Returns:
        catalogs: list of trino catalogs after action
    """
    action = (
        await ops_test.model.applications[APP_NAME]
        .units[0]
        .run_action(action, **params)
    )
    await action.wait()
    time.sleep(30)
    catalogs = await get_catalogs(ops_test, user, APP_NAME)
    logging.info(f"action {action} run, catalogs: {catalogs}")
    return catalogs


async def create_group_policy(ops_test, ranger_url):
    """Create a Ranger group policy.

    Allow members of `commercial-systems` to access `tpch` catalog.

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
        "catalog": RangerPolicyResource({"values": ["tpch"]}),
        "table": RangerPolicyResource({"values": ["*"]}),
        "column": RangerPolicyResource({"values": ["*"]}),
    }

    allow_items = RangerPolicyItem()
    allow_items.groups = [GROUP_WITH_ACCESS]
    allow_items.accesses = [RangerPolicyItemAccess({"type": "select"})]
    policy.policyItems = [allow_items]
    ranger.create_policy(policy)
