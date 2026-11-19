#!/usr/bin/env python3
# Copyright 2023 Canonical Ltd.
# See LICENSE file for licensing details.

"""Trino policy integration tests."""

import logging
import time
import uuid
from pathlib import Path

import jubilant
import pytest
import trino.exceptions
from conftest import pack_charm
from helpers import (
    APP_NAME,
    POSTGRES_NAME,
    RANGER_NAME,
    USER_WITH_ACCESS,
    USER_WITHOUT_ACCESS,
    create_user,
    get_catalogs,
    get_unit_url,
    run_query,
    update_policies,
    wait_for_apps,
)

logger = logging.getLogger(__name__)

TRINO_CONFIG = {
    "ranger-service-name": "trino-service",
    "charm-function": "all",
}

OPENSEARCH_STUB_NAME = "opensearch-stub"
OPENSEARCH_STUB_PATH = Path(__file__).parent / "opensearch_stub_charm"
RANGER_AUDIT_PATH = "/usr/lib/trino/etc/ranger-trino-audit.xml"
LOG_PROPERTIES_PATH = "/usr/lib/trino/etc/log.properties"
SERVER_LOG_PATH = "/data/trino/var/log/server.log"
RANGER_AUDIT_LOGGER = "org.apache.ranger.audit.provider.BaseAuditHandler"
AUDIT_FAILURE_LINE = "Error sending message to ElasticSearch"


def _trino_logs(juju: jubilant.Juju) -> str:
    """Return Trino's server.log and the Pebble log buffer of the trino service."""
    unit = f"{APP_NAME}/0"
    server_log = juju.ssh(unit, "cat", SERVER_LOG_PATH, container="trino")
    pebble_log = juju.ssh(
        unit, "/charm/bin/pebble", "logs", "trino", "-n", "all", container="trino"
    )
    return server_log + pebble_log


def _wait_for_file_line(juju: jubilant.Juju, path: str, line: str, timeout: float = 600):
    """Wait until a file in the Trino container contains a line.

    Args:
        juju: The Jubilant Juju instance.
        path: Absolute path of the file in the Trino container.
        line: The line to wait for, ignoring surrounding whitespace.
        timeout: Seconds to wait.
    """
    deadline = time.time() + timeout
    while time.time() < deadline:
        content = juju.ssh(f"{APP_NAME}/0", "cat", path, container="trino")
        if line in (existing.strip() for existing in content.splitlines()):
            return
        time.sleep(10)
    raise AssertionError(f"{path} did not contain {line!r} within {timeout}s")


def _query_until_logged(juju: jubilant.Juju, query: str, needle: str, timeout: float = 300):
    """Run a query repeatedly until a string appears in Trino's logs.

    Args:
        juju: The Jubilant Juju instance.
        query: The query whose audit event must fail delivery.
        needle: The string to wait for in Trino's logs.
        timeout: Seconds to wait.

    Returns:
        Trino's logs once they contain the needle.
    """
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            run_query(juju, USER_WITH_ACCESS, query)
        except trino.exceptions.TrinoUserError as err:
            # Ranger audits denied requests too, so a denial still exercises delivery.
            logger.info("Query denied: %s", err)
        time.sleep(10)
        logs = _trino_logs(juju)
        if needle in logs:
            return logs
    raise AssertionError(f"{needle!r} did not appear in Trino's logs within {timeout}s")


def resolve_ranger_errors(juju: jubilant.Juju):
    """Resolve Ranger error state if any.

    Args:
        juju: The Jubilant Juju instance.
    """
    for unit_name, unit_status in juju.status().apps[RANGER_NAME].units.items():
        if unit_status.workload_status.current == "error":
            logger.info("Resolving error on %s", unit_name)
            juju.cli("resolved", "--retry", unit_name)


@pytest.fixture(name="deploy-policy", scope="module")
def deploy_policy_engine(juju: jubilant.Juju):
    """Add Ranger relation and apply group configuration."""
    juju.deploy(POSTGRES_NAME, channel="14", trust=True)
    wait_for_apps(juju, [POSTGRES_NAME], status="active", timeout=2000)

    juju.deploy(RANGER_NAME, channel="edge", revision=39, trust=True)
    wait_for_apps(juju, [RANGER_NAME], status="blocked", timeout=2000)

    logger.info("Integrating Ranger and PostgreSQL.")
    juju.integrate(RANGER_NAME, POSTGRES_NAME)
    time.sleep(60)
    resolve_ranger_errors(juju)
    wait_for_apps(juju, [POSTGRES_NAME, RANGER_NAME], status="active", timeout=2000)


@pytest.mark.incremental
@pytest.mark.usefixtures("deploy-policy")
class TestPolicyManager:
    """Integration test for Ranger policy enforcement."""

    def test_policy_enforcement(self, juju: jubilant.Juju, charm: str, charm_image: str):
        """Test Ranger integration."""
        juju.deploy(
            charm,
            APP_NAME,
            resources={"trino-image": charm_image},
            config=TRINO_CONFIG,
            trust=True,
        )

        wait_for_apps(juju, [APP_NAME], status="active", timeout=2000)

        logger.info("Creating test user.")
        url = get_unit_url(juju, application=RANGER_NAME, unit=0, port=6080)
        create_user(url)

        # Integrate Trino and Ranger.
        logger.info("Integrating Trino and Ranger.")
        juju.integrate(RANGER_NAME, APP_NAME)
        time.sleep(30)
        resolve_ranger_errors(juju)
        wait_for_apps(juju, [APP_NAME, RANGER_NAME], status="active", timeout=2000)

        logging.info("update default policies to authorize the new user")
        update_policies(url)

        time.sleep(30)  # wait 30 seconds for the policy to be synced.

        catalogs = get_catalogs(juju, USER_WITH_ACCESS, APP_NAME)
        assert catalogs == [["system"]]
        logger.info("%s can access %s.", USER_WITH_ACCESS, catalogs)

        catalogs = get_catalogs(juju, USER_WITHOUT_ACCESS, APP_NAME)
        logger.info("%s can not access %s.", USER_WITHOUT_ACCESS, catalogs)
        assert catalogs == []

    def test_failed_audit_delivery_does_not_log_event_contents(self, juju: jubilant.Juju):
        """A failed Ranger audit delivery does not write the event contents to Trino's logs.

        The stub provider points the audit destination at a closed port, so
        delivery fails whatever the OpenSearch version.
        """
        juju.deploy(pack_charm(OPENSEARCH_STUB_PATH), OPENSEARCH_STUB_NAME)
        wait_for_apps(juju, [OPENSEARCH_STUB_NAME], status="active", timeout=1000)
        juju.integrate(f"{APP_NAME}:opensearch", f"{OPENSEARCH_STUB_NAME}:opensearch-client")
        _wait_for_file_line(juju, RANGER_AUDIT_PATH, "<value>127.0.0.1</value>")
        wait_for_apps(juju, [APP_NAME], status="active", idle_period=30, timeout=1200)

        marker = f"pii-marker-{uuid.uuid4().hex}"
        query = f"SELECT '{marker}' FROM system.runtime.nodes"  # nosec B608
        _query_until_logged(juju, query, AUDIT_FAILURE_LINE)
        # Ranger logs the event dump right after the error line; give it time to flush.
        time.sleep(15)
        logs = _trino_logs(juju)

        assert AUDIT_FAILURE_LINE in logs
        assert marker not in logs

    def test_log_level_override_reenables_audit_event_dump(self, juju: jubilant.Juju):
        """Overriding the Ranger audit logger level rewrites log.properties and restarts Trino.

        Airlift reads log.properties only at startup, so the event dump
        reappearing proves the restart and that the previous test's probe
        would have caught a leak.
        """
        juju.config(APP_NAME, {"log-level-overrides": f"{RANGER_AUDIT_LOGGER}=warn"})
        _wait_for_file_line(juju, LOG_PROPERTIES_PATH, f"{RANGER_AUDIT_LOGGER}=warn")
        wait_for_apps(juju, [APP_NAME], status="active", idle_period=30, timeout=1200)

        marker = f"pii-marker-{uuid.uuid4().hex}"
        query = f"SELECT '{marker}' FROM system.runtime.nodes"  # nosec B608
        _query_until_logged(juju, query, marker)

        juju.config(APP_NAME, reset="log-level-overrides")
        _wait_for_file_line(juju, LOG_PROPERTIES_PATH, f"{RANGER_AUDIT_LOGGER}=error")
