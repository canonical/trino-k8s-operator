#!/usr/bin/env python3
# Copyright 2023 Canonical Ltd.
# See LICENSE file for licensing details.

"""Trino charm integration test helpers."""

import logging
import os
import time
from pathlib import Path

import jubilant
import requests
import trino.exceptions
import yaml
from apache_ranger.client.ranger_client import RangerClient
from apache_ranger.client.ranger_user_mgmt_client import RangerUserMgmtClient
from apache_ranger.model.ranger_user_mgmt import RangerUser
from trino_client.trino_client import query_trino

logger = logging.getLogger(__name__)


BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(os.path.realpath(__file__)), "..", ".."))
METADATA = yaml.safe_load(Path(f"{BASE_DIR}/charmcraft.yaml").read_text())

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
"""  # nosec  # noqa: E501

GSHEETS_SECRET = """\
gsheets-1: |
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
"""  # nosec  # noqa: E501

POSTGRESQL_REPLICA_SECRET = """\
rw:
  user: trino
  password: pwd1
  suffix: _developer
  params: ssl=true&sslmode=require&sslrootcert={SSL_PATH}&sslrootcertpassword={SSL_PWD}&targetServerType=primary
ro:
  user: trino_ro
  password: pwd2
  params: ssl=true&sslmode=require&sslrootcert={SSL_PATH}&sslrootcertpassword={SSL_PWD}&targetServerType=preferSecondary
"""  # nosec  # noqa: E501

MYSQL_REPLICA_SECRET = """\
ro:
  user: trino_ro
  password: pwd3
"""  # nosec


REDSHIFT_REPLICA_SECRET = """\
ro:
  user: trino_ro
  password: pwd4
"""  # nosec

CATALOG_QUERY = "SHOW CATALOGS"
TRINO_USER = "trino"

# Ranger policy literals
RANGER_NAME = "ranger-k8s"
RANGER_AUTH = ("admin", "rangerR0cks!")
TRINO_SERVICE = "trino-service"
USER_WITH_ACCESS = "dev"
USER_WITHOUT_ACCESS = "user"

# Scaling literals
WORKER_QUERY = "SELECT * FROM system.runtime.nodes"

WORKER_CONFIG = {"charm-function": "worker", "max-concurrent-queries": 5}
COORDINATOR_CONFIG = {
    "charm-function": "coordinator",
    "max-concurrent-queries": 5,
}


def get_unit(
    juju: jubilant.Juju, application: str, unit: int = 0
) -> jubilant.statustypes.UnitStatus:
    """Return a single unit status from the current model status."""
    unit_name = f"{application}/{unit}"
    return juju.status().apps[application].units[unit_name]


def _apps_ready(
    model_status: jubilant.Status, apps: list[str], status: str, exact_units: dict[str, int]
) -> bool:
    """Return whether all selected applications match the expected state."""
    for app in apps:
        if app not in model_status.apps:
            return False

        app_status = model_status.apps[app]
        if app_status.app_status.current != status:
            return False
        if app in exact_units and len(app_status.units) != exact_units[app]:
            return False
        if any(unit.workload_status.current != status for unit in app_status.units.values()):
            return False

    return True


def _apps_in_error(
    model_status: jubilant.Status, apps: list[str], status: str, raise_on_blocked: bool
) -> bool:
    """Return whether any selected application is in a terminal wait state."""
    for app in apps:
        app_status = model_status.apps.get(app)
        if app_status is None:
            continue
        if app_status.app_status.current == "error":
            return True
        if raise_on_blocked and app_status.app_status.current == "blocked" and status != "blocked":
            return True
        for unit_status in app_status.units.values():
            if unit_status.workload_status.current == "error":
                return True
            if (
                raise_on_blocked
                and unit_status.workload_status.current == "blocked"
                and status != "blocked"
            ):
                return True

    return False


def wait_for_apps(
    juju: jubilant.Juju,
    apps: list[str],
    *,
    status: str,
    timeout: float,
    raise_on_blocked: bool = False,
    wait_for_exact_units: int | dict[str, int] | None = None,
    idle_period: int | None = None,
    delay: float = 2.0,
):
    """Approximate OpsTest wait_for_idle semantics with Jubilant waits."""
    exact_units: dict[str, int] = {}
    if isinstance(wait_for_exact_units, dict):
        exact_units = wait_for_exact_units
    elif isinstance(wait_for_exact_units, int):
        exact_units = dict.fromkeys(apps, wait_for_exact_units)

    successes = max(3, int(idle_period / delay)) if idle_period else 3

    def ready(model_status):
        return _apps_ready(model_status, apps, status, exact_units)

    def error(model_status):
        return _apps_in_error(model_status, apps, status, raise_on_blocked)

    return juju.wait(ready, error=error, delay=delay, timeout=timeout, successes=successes)


def wait_for_app_gone(juju: jubilant.Juju, app: str, timeout: float = 600, delay: float = 2.0):
    """Wait until an application no longer appears in model status."""
    deadline = time.time() + timeout
    while time.time() < deadline:
        if app not in juju.status().apps:
            return
        time.sleep(delay)
    raise TimeoutError(f"Application {app!r} still present after {timeout}s")


def get_unit_url(juju: jubilant.Juju, application, unit, port, protocol="http"):
    """Return unit URL from the model.

    Args:
        juju: Jubilant Juju object.
        application: Name of the application.
        unit: Number of the unit.
        port: Port number of the URL.
        protocol: Transfer protocol (default: http).

    Returns:
        Unit URL of the form {protocol}://{address}:{port}
    """
    address = get_unit(juju, application, unit).address
    return f"{protocol}://{address}:{port}"


def get_catalogs(juju: jubilant.Juju, user, app_name):
    """Return a list of catalogs from Trino charm.

    Args:
        juju: Jubilant Juju object.
        user: the user to access Trino with
        app_name: name of the application

    Returns:
        catalogs: list of catalogs connected to trino
    """
    try:
        catalogs = run_query(juju, user, CATALOG_QUERY, app_name)
    except trino.exceptions.TrinoUserError:
        catalogs = []
    return catalogs


def run_query(juju: jubilant.Juju, user, query, app_name=APP_NAME):
    """Run a SQL query against the Trino coordinator.

    Args:
        juju: Jubilant Juju object.
        user: the user to access Trino with.
        query: SQL query to execute.
        app_name: name of the application.

    Returns:
        Query result rows.
    """
    address = get_unit(juju, app_name).address
    logger.info("executing query on app address: %s", address)
    return query_trino(address, user, query)


def update_catalog_config(juju: jubilant.Juju, catalog_config, user):
    """Run connection action.

    Args:
        juju: Jubilant Juju object.
        catalog_config: The catalogs configuration value.
        user: the user to access Trino with.

    Returns:
        A string of trino catalogs.
    """
    juju.config(APP_NAME, {"catalog-config": catalog_config})
    wait_for_apps(juju, [APP_NAME, WORKER_NAME], status="active", timeout=600)
    catalogs = get_catalogs(juju, user, APP_NAME)
    logging.info(f"Catalogs: {catalogs}")
    return catalogs


def create_user(ranger_url):
    """Create Ranger user.

    Args:
        ranger_url: the policy manager url
    """
    ranger_client = RangerClient(ranger_url, RANGER_AUTH)
    user_client = RangerUserMgmtClient(ranger_client)
    user = RangerUser()
    user.name = USER_WITH_ACCESS
    user.firstName = "James"
    user.lastName = "Dev"
    user.emailAddress = "james.dev@canonical.com"
    user.password = "aP6X1HhJe6Toui!"  # nosec
    res = user_client.create_user(user)
    logger.info(res)


def update_policies(ranger_url):
    """Update Ranger user policy.

    Allow user `USER_WITH_ACCESS` to access `system` catalog.

    Args:
        ranger_url: the policy manager url
    """
    ranger = RangerClient(ranger_url, RANGER_AUTH)

    for policy_name in ["all - trinouser", "all - catalog", "all - queryid"]:
        policy = ranger.get_policy(TRINO_SERVICE, policy_name)
        policy.policyItems[0].users.append(USER_WITH_ACCESS)
        ranger.update_policy(TRINO_SERVICE, policy_name, policy)


def scale(juju: jubilant.Juju, app, units):
    """Scale the application to the provided number and wait for idle.

    Args:
        juju: Jubilant Juju object.
        app: Application to be scaled.
        units: Number of units required.
    """
    current_units = len(juju.status().apps[app].units)
    if units > current_units:
        juju.add_unit(app, num_units=units - current_units)
    elif units < current_units:
        juju.remove_unit(app, num_units=current_units - units)

    wait_for_apps(
        juju,
        [app],
        status="active",
        idle_period=30,
        raise_on_blocked=True,
        timeout=600,
        wait_for_exact_units=units,
    )


def get_active_workers(juju: jubilant.Juju):
    """Get active trino workers.

    Args:
        juju: Jubilant Juju object.

    Returns:
        active_workers: list of active workers.
    """
    address = get_unit(juju, APP_NAME).address
    logger.info("executing query on app address: %s", address)
    result = query_trino(address, USER_WITH_ACCESS, WORKER_QUERY)
    active_workers = [
        x
        for x in result
        # Filter by URI and state: workers being removed may still appear
        # in system.runtime.nodes but with a non-active state.
        if x[1].startswith("http://trino-k8s-worker") and x[4] == "active"
    ]
    return active_workers


def simulate_crash_and_restart(juju: jubilant.Juju, charm, charm_image):
    """Simulate the crash of the Trino coordinator.

    Args:
        juju: Jubilant Juju object.
        charm: charm path.
        charm_image: path to rock image to be used.
    """
    # Destroy charm
    juju.remove_application(APP_NAME)
    wait_for_app_gone(juju, APP_NAME)

    # Deploy charm again
    juju.deploy(
        charm,
        APP_NAME,
        resources={"trino-image": charm_image},
        config=COORDINATOR_CONFIG,
        num_units=1,
        trust=True,
    )

    wait_for_apps(
        juju,
        [APP_NAME],
        status="blocked",
        timeout=1000,
    )

    juju.integrate(f"{APP_NAME}:trino-coordinator", f"{WORKER_NAME}:trino-worker")
    wait_for_apps(
        juju,
        [APP_NAME, WORKER_NAME],
        status="active",
        timeout=1000,
    )


def curl_unit_ip(juju: jubilant.Juju):
    """Curl the coordinator unit IP.

    Args:
        juju: Jubilant Juju object.

    Returns:
        response: Request response.
    """
    url = get_unit_url(juju, application=APP_NAME, unit=0, port=8080)
    logger.info("curling app address: %s", url)

    response = requests.get(url, timeout=300, verify=False)  # nosec
    return response


def add_juju_secret(juju: jubilant.Juju, connector_type: str):
    """Add Juju user secret to model.

    Args:
        juju: Jubilant Juju object.
        connector_type: Type of connector secret to add.

    Returns:
        secret ID of created secret.

    Raises:
        ValueError: in case connector is not supported.
    """
    if connector_type == "postgresql":
        content = {"replicas": POSTGRESQL_REPLICA_SECRET}  # nosec
    elif connector_type == "mysql":
        content = {"replicas": MYSQL_REPLICA_SECRET}  # nosec
    elif connector_type == "redshift":
        content = {"replicas": REDSHIFT_REPLICA_SECRET}  # nosec
    elif connector_type == "bigquery":
        content = {"service-accounts": BIGQUERY_SECRET}  # nosec
    elif connector_type == "gsheets":
        content = {"service-accounts": GSHEETS_SECRET}  # nosec
    else:
        raise ValueError(f"Unsupported secret type: {connector_type}")

    juju_secret = juju.add_secret(name=f"{connector_type}-secret", content=content)
    secret_id = str(juju_secret).split(":")[-1]
    return secret_id


def create_catalog_config(
    postgresql_secret_id,
    mysql_secret_id,
    redshift_secret_id,
    bigquery_secret_id,
    gsheets_secret_id,
    include_bigquery=True,
):
    """Create and return catalog-config value.

    Args:
        postgresql_secret_id: the juju secret id for postgresql
        mysql_secret_id: the juju secret id for mysql
        redshift_secret_id: the juju secret id for redshift
        bigquery_secret_id: the juju secret id for bigquery
        gsheets_secret_id: the juju secret id for gsheets
        include_bigquery: flag to indicate if bigquery configuration should be included.

    Returns:
        the catalog configuration.
    """
    if include_bigquery:
        catalog_config = f"""\
        catalogs:
            postgresql-1:
                backend: dwh
                database: example
                secret-id: {postgresql_secret_id}
            mysql:
                backend: mysql
                secret-id: {mysql_secret_id}
            redshift:
                backend: redshift
                secret-id: {redshift_secret_id}
            bigquery:
                backend: bigquery
                project: project-12345
                secret-id: {bigquery_secret_id}
            gsheets-1:
                backend: gsheets
                metasheet-id: 1Es4HhWALUQjoa-bQh4a8B5HROz7dpGMfq_HbfoaW5LM
                secret-id: {gsheets_secret_id}
        backends:
            dwh:
                connector: postgresql
                url: jdbc:postgresql://example.com:5432
                params: ssl=true&sslmode=require&sslrootcert={{SSL_PATH}}&sslrootcertpassword={{SSL_PWD}}
                config: |
                    case-insensitive-name-matching=true
                    decimal-mapping=allow_overflow
                    decimal-rounding-mode=HALF_UP
            mysql:
                connector: mysql
                url: jdbc:mysql://mysql.com:3306
                params: sslMode=REQUIRED
                config: |
                    case-insensitive-name-matching=true
                    decimal-mapping=allow_overflow
                    decimal-rounding-mode=HALF_UP
            redshift:
                connector: redshift
                url: jdbc:redshift://redshift.com:5439/example
                params: SSL=TRUE
                config: |
                    case-insensitive-name-matching=true
            bigquery:
                connector: bigquery
                config: |
                    bigquery.case-insensitive-name-matching=true
                    bigquery.arrow-serialization.enabled=false
            gsheets:
                connector: gsheets
        """  # noqa: E501
    else:
        catalog_config = f"""\
        catalogs:
            postgresql-1:
                backend: dwh
                database: example
                secret-id: {postgresql_secret_id}
            mysql:
                backend: mysql
                secret-id: {mysql_secret_id}
            redshift:
                backend: redshift
                secret-id: {redshift_secret_id}
            gsheets-1:
                backend: gsheets
                metasheet-id: 1Es4HhWALUQjoa-bQh4a8B5HROz7dpGMfq_HbfoaW5LM
                secret-id: {gsheets_secret_id}
        backends:
            dwh:
                connector: postgresql
                url: jdbc:postgresql://example.com:5432
                params: ssl=true&sslmode=require&sslrootcert={{SSL_PATH}}&sslrootcertpassword={{SSL_PWD}}
                config: |
                    case-insensitive-name-matching=true
                    decimal-mapping=allow_overflow
                    decimal-rounding-mode=HALF_UP
            mysql:
                connector: mysql
                url: jdbc:mysql://mysql.com:3306
                params: sslMode=REQUIRED
                config: |
                    case-insensitive-name-matching=true
                    decimal-mapping=allow_overflow
                    decimal-rounding-mode=HALF_UP
            redshift:
                connector: redshift
                url: jdbc:redshift://redshift.com:5439/example
                params: SSL=TRUE
                config: |
                    case-insensitive-name-matching=true
            gsheets:
                connector: gsheets
        """  # noqa: E501
    return catalog_config


def get_secret_id_by_label(juju: jubilant.Juju, label: str):
    """Get the secret ID by label.

    Args:
        juju: Jubilant Juju object.
        label: Label of the secret to find.

    Returns:
        Secret ID string if found, None otherwise.
    """
    secrets = yaml.safe_load(juju.cli("secrets", "--format=yaml"))

    for secret_id, info in secrets.items():
        if info and info.get("name") == label:
            return secret_id

    return None
