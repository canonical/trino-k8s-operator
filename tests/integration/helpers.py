#!/usr/bin/env python3
# Copyright 2023 Canonical Ltd.
# See LICENSE file for licensing details.

"""Trino charm integration test helpers."""

import json
import logging
import os
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
from trino_client.trino_client import query_trino

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
BIGQUERY_SECRET = """\
project-12345: |
    {
      "type": "service_account",
      "project_id": "example-project",
      "private_key_id": "key123",
      "private_key": "-----BEGIN PRIVATE KEY-----\\nMIIEvAIBADANBgkqhkiG9w0BAQEFAASCBKYwggSiAgEAAoIBAQDk0x6IbejGjKC8\\nV7staWrwXlEqheosQeEYDRDkRLLe/Tw5LuNnw9Rids7vjjQpRNiRttNfeOHm9360\\nK29TbPMnLT4Iy56jnW/c+9PYXenHP1k4br1TcZ2cFJdYEV6xu4jT0mKoN9304SVI\\nlXzLtfQdzsFp4SqWtr9gaH4KSBzJoeE3pX7iLgvM3o4bUh+WH16ejfiLJZ1zQkYA\\nmu96criFAm5YnuoO2PRTo2KGoLamf3VDXLDYiWs2cSJxifwblos3Eh2zgxVRALfo\\nirtjlwAzSMjTXSC2nOyJmrDUsQyA3gJFr7TPlIIEUkGehy6/fU1z8B1yTh0ojpsq\\n8naueTSFAgMBAAECggEAcEYWSSKEgEcn5sG1GYcL7XyZnp+uUqDQbRicHSSID1l5\\nXyVedt9jKhzZVDkV5tnc2UI3XDTXwpfVF1noeaqPc72DHpWp9OWeqXL2csdBmX2/\\nrSzIwFSS3K5Nw+xh5hr5+9TSi289/JUr0f1nChzw9l8oD2dnmiN4qzkZ/rl7RoK1\\ng6Nj7U2u7qy2gf2vMH0MzFh/O+tQ3nLjwmNeOdF03ZxVDUGrkaTBftfbwSI5te7F\\nljeU7EgZVatjlyIXfi0p8OULXu6/xxpDZPYvIUxZjitjodxa5ZykmhVMBHjDVRbq\\n5Boh4laGdSiayBKMb7BCT/TwlQIPA1eEzWUJXJ8YUQKBgQDr3DKxAIajCP6IdTVT\\n75tHqc2TCqIS6QfM62X4NIw/ETUtiAU9+Pq33OBnivDSjbI9NPpSLbX18ttyPugn\\nPcgC0EUf2+5/EniD7khqjZLQXLK4WZXN6M15NS87cznOm9qbWway15f+iWF4qr0d\\nN8jsVypbSEicWiKUrq2IiJSMrwKBgQD4XSEHxQtmX7nk/ImhqWl1C9QkwiAlvLxy\\nGUIUwHkpbxRHE1tovT3XS9shQK3MZzMYG6d60bNIMIkpyvbN4+ptikCFsSikuAkP\\nE1865ipxCUaInbMYk3lzuNfPO4hP52pjW5r67WD6O1qjLdTsacPXCCSepEjKe+Rd\\nktUiGv+nCwKBgHCVfHD3Ek1ydqVGZX06a4GqsSFWOwURzRJo7xSqaKOWIC8qtW3e\\nkjb/rPJf5RJsZr9GsZJWlXvgQBXpp0FMAVQufEB36AEqHPLE5DZQe9sP1JOg15wh\\nWytXUsNq/hX8WT49FhZ6SOhMRYWm4ny26ya9eM930oknkUgtlVIN9/KrAoGAAJVn\\ncHc8EZ+D9k/JmwGk58uBUhzKqowI/VOl3hqdrkU+jPQ0sMhRDuJ0v11Bi0tqyVG3\\nUQiRHUhP6jM55T313RAIGshRyiFMlCZ9gMvtqZpV+hg0xYgDLwxuJWSEa3ululoK\\nwTAxnCTrj5qZ93xAI483VtAYA7HK1ZV0vsHFfAUCgYB13ErBMkV3cOFsUHOYUzXo\\nQbeIhRDthqTw4xToTsCaZnweZDEtqmnJMfRmbAqzPNbRjGjd7uH5dssqD7H3kpA5\\noywUbHhRzvJJvmk0enpnbjP6NY51goJ/WUVM4n6AZC6v3cfE9HNBAiPEaDAZT/ul\\nbDOWB1LReVCV5YytEsR/KA==\\n-----END PRIVATE KEY-----",
      "client_email": "test-380@example-project.iam.gserviceaccount.com",
      "client_id": "12345",
      "auth_uri": "https://accounts.google.com/o/oauth2/auth",
      "token_uri": "https://oauth2.googleapis.com/token",
      "auth_provider_x509_cert_url": "https://www.googleapis.com/oauth2/v1/certs",
      "client_x509_cert_url": "https://www.googleapis.com/robot/v1/metadata/x509/test-380.project.iam.gserviceaccount.com",
      "universe_domain": "googleapis.com"
    }
"""  # nosec

POSTGRESQL_REPLICA_SECRET = """\
rw:
  user: trino
  password: pwd1
  suffix: _developer
ro:
  user: trino_ro
  password: pwd2
"""  # nosec

CATALOG_QUERY = "SHOW CATALOGS"
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

# Scaling literals
WORKER_QUERY = "SELECT * FROM system.runtime.nodes"

WORKER_CONFIG = {"charm-function": "worker"}
COORDINATOR_CONFIG = {
    "charm-function": "coordinator",
}


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
    catalogs = await query_trino(address, user, CATALOG_QUERY)
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
        await ops_test.model.wait_for_idle(
            apps=[APP_NAME, WORKER_NAME], status="active", timeout=600
        )
    catalogs = await get_catalogs(ops_test, user, APP_NAME)
    logging.info(f"Catalogs: {catalogs}")
    return catalogs


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


async def scale(ops_test: OpsTest, app, units):
    """Scale the application to the provided number and wait for idle.

    Args:
        ops_test: PyTest object.
        app: Application to be scaled.
        units: Number of units required.
    """
    async with ops_test.fast_forward():
        await ops_test.model.applications[app].scale(scale=units)

        # Wait for model to settle
        await ops_test.model.wait_for_idle(
            apps=[app],
            status="active",
            idle_period=30,
            raise_on_blocked=True,
            timeout=600,
            wait_for_exact_units=units,
        )


async def get_active_workers(ops_test: OpsTest):
    """Get active trino workers.

    Args:
        ops_test: PyTest object.

    Returns:
        active_workers: list of active workers.
    """
    status = await ops_test.model.get_status()  # noqa: F821
    address = status["applications"][APP_NAME]["units"][f"{APP_NAME}/{0}"][
        "address"
    ]
    logger.info("executing query on app address: %s", address)
    result = await query_trino(address, USER_WITH_ACCESS, WORKER_QUERY)
    active_workers = [
        x for x in result if x[1].startswith("http://trino-k8s-worker")
    ]
    return active_workers


async def simulate_crash_and_restart(ops_test):
    """Simulates the crash of the Trino coordinator.

    Args:
        ops_test: PyTest object.
    """
    # Destroy charm
    await ops_test.model.applications[APP_NAME].destroy()
    await ops_test.model.block_until(
        lambda: APP_NAME not in ops_test.model.applications
    )

    # Deploy charm again
    charm = await ops_test.build_charm(BASE_DIR)
    async with ops_test.fast_forward():
        await ops_test.model.deploy(
            charm,
            resources=TRINO_IMAGE,
            application_name=APP_NAME,
            config=COORDINATOR_CONFIG,
            num_units=1,
        )

        await ops_test.model.wait_for_idle(
            apps=[APP_NAME],
            status="blocked",
            raise_on_blocked=False,
            timeout=1000,
        )

        await ops_test.model.integrate(
            f"{APP_NAME}:trino-coordinator", f"{WORKER_NAME}:trino-worker"
        )
        await ops_test.model.wait_for_idle(
            apps=[APP_NAME, WORKER_NAME],
            status="active",
            raise_on_blocked=False,
            timeout=1000,
        )


async def curl_unit_ip(ops_test):
    """Curl the coordinator unit IP.

    Args:
        ops_test: PyTest object.

    Returns:
        response: Request response.
    """
    url = await get_unit_url(ops_test, application=APP_NAME, unit=0, port=8080)
    logger.info("curling app address: %s", url)

    response = requests.get(url, timeout=300, verify=False)  # nosec
    return response


async def add_postgresql_juju_secret(ops_test: OpsTest):
    """Add Juju user secret to model.

    Args:
        ops_test: PyTest object.

    Returns:
        secret ID of created secret.
    """
    juju_secret = await ops_test.model.add_secret(
        name="postgresql-secret",
        data_args=[f"replicas={POSTGRESQL_REPLICA_SECRET}"],
    )

    secret_id = juju_secret.split(":")[-1]
    return secret_id


async def add_bigquery_juju_secret(ops_test: OpsTest):
    """Add Juju user secret to model.

    Args:
        ops_test: PyTest object.

    Returns:
        secret ID of created secret.
    """
    juju_secret = await ops_test.model.add_secret(
        name="bigquery-secret",
        data_args=[f"service-accounts={BIGQUERY_SECRET}"],
    )

    secret_id = juju_secret.split(":")[-1]
    return secret_id


async def create_catalog_config(postgresql_secret_id, bigquery_secret_id):
    """Create and return catalog-config value.

    Args:
        postgresql_secret_id: the juju secret id for postgresql
        bigquery_secret_id: the juju secret id for bigquery

    Returns:
        the catalog configuration.
    """
    return f"""\
    catalogs:
        postgresql-1:
            backend: dwh
            database: example
            secret-id: {postgresql_secret_id}
        bigquery:
            backend: bigquery
            project: project-12345
            secret-id: {bigquery_secret_id}
    backends:
        dwh:
            connector: postgresql
            url: jdbc:postgresql://example.com:5432
            params: ssl=true&sslmode=require&sslrootcert={{SSL_PATH}}&sslrootcertpassword={{SSL_PWD}}
            config: |
                case-insensitive-name-matching=true
                decimal-mapping=allow_overflow
                decimal-rounding-mode=HALF_UP
        bigquery:
            connector: bigquery
            config: |
                bigquery.case-insensitive-name-matching=true
    """


async def create_reduced_catalog_config(
    postgresql_secret_id, bigquery_secret_id
):
    """Create and return catalog-config value.

    Args:
        postgresql_secret_id: the juju secret id for postgresql
        bigquery_secret_id: the juju secret id for bigquery

    Returns:
        the catalog configuration.
    """
    return f"""\
    catalogs:
        postgresql-1:
            backend: dwh
            database: example
            secret-id: {postgresql_secret_id}
    backends:
        dwh:
            connector: postgresql
            url: jdbc:postgresql://example.com:5432
            params: ssl=true&sslmode=require&sslrootcert={{SSL_PATH}}&sslrootcertpassword={{SSL_PWD}}
            config: |
                case-insensitive-name-matching=true
                decimal-mapping=allow_overflow
                decimal-rounding-mode=HALF_UP
    """
