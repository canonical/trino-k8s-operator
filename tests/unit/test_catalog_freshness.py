# Copyright 2023 Canonical Ltd.
# See LICENSE file for licensing details.
#
# Learn more about testing at: https://juju.is/docs/sdk/testing


"""Trino charm catalog freshness unit tests."""

# pylint:disable=protected-access

import dataclasses
import logging

from ops.model import ActiveStatus
from ops.testing import Mount

from tests.unit.helpers import (
    BIGQUERY_CATALOG_PATH,
    POSTGRESQL_2_CATALOG_PATH,
    UPDATED_JVM_OPTIONS,
    USER_JVM_STRING,
    build_coordinator_state,
    build_worker_state,
    carry_forward,
    create_added_catalog_config,
    peer_state_value,
    trino_container,
    workload_path,
)

logger = logging.getLogger(__name__)


def test_config_changed(ctx):
    """The pebble plan changes according to config changes."""
    state_in, _ = build_coordinator_state(
        config={
            "google-client-id": "test-client-id",
            "google-client-secret": "test-client-secret",
            "web-proxy": "proxy:port",
            "charm-function": "all",
            "additional-jvm-options": USER_JVM_STRING,
        }
    )

    state_out = ctx.run(ctx.on.config_changed(), state_in)

    want_services = {
        "trino": {
            "override": "replace",
            "summary": "trino server",
            "command": "./entrypoint.sh",
            "startup": "enabled",
            "on-check-failure": {"up": "restart"},
            "environment": {
                "PASSWORD_DB_PATH": "/usr/lib/trino/etc/password.db",  # nosec
                "LOG_LEVEL": "info",
                "OAUTH_CLIENT_ID": "test-client-id",
                "OAUTH_CLIENT_SECRET": "test-client-secret",  # nosec
                "WEB_PROXY": "proxy:port",
                "CHARM_FUNCTION": "all",
                "DISCOVERY_URI": "http://trino-k8s.trino-model.svc.cluster.local:8080",
                "APPLICATION_NAME": "trino-k8s",
                "TRINO_HOME": "/usr/lib/trino/etc",
                "JMX_PORT": 9081,
                "METRICS_PORT": 9090,
                "OAUTH_USER_MAPPING": None,
                "RANGER_RELATION": False,
                "RESOURCE_GROUPS_CONFIG": None,
                "SESSION_PROPERTY_MANAGER_CONFIG": None,
                "ACL_ACCESS_MODE": "owner",
                "ACL_CATALOG_PATTERN": ".*",
                "ACL_USER_PATTERN": ".*",
                "JAVA_TRUSTSTORE_PWD": "truststore_pwd",  # nosec
                "INT_COMMS_SECRET": "int_comms_secret",  # nosec
                "USER_SECRET_ID": "secret:secret-id",  # nosec
                "JVM_OPTIONS": UPDATED_JVM_OPTIONS,
                "COORDINATOR_REQUEST_TIMEOUT": "10m",
                "COORDINATOR_CONNECT_TIMEOUT": "30s",
                "WORKER_REQUEST_TIMEOUT": "30s",
                "MAX_CONCURRENT_QUERIES": 50,
                "QUERY_MAX_CPU_TIME": None,
                "QUERY_MAX_MEMORY_PER_NODE": None,
                "QUERY_MAX_MEMORY": None,
                "QUERY_MAX_TOTAL_MEMORY": None,
                "MEMORY_HEAP_HEADROOM_PER_NODE": None,
                "QUERY_MAX_RUN_TIME": None,
            },
        }
    }

    got_services = state_out.get_container("trino").plan.to_dict()["services"]

    # The truststore password and the internal-comms secret are randomly
    # generated, and are normalised here to compare the rest of the plan.
    environment = got_services["trino"]["environment"]
    environment["JAVA_TRUSTSTORE_PWD"] = "truststore_pwd"  # nosec
    environment["INT_COMMS_SECRET"] = "int_comms_secret"  # nosec
    environment["USER_SECRET_ID"] = "secret:secret-id"  # nosec

    # Per-file content hashes drive Pebble restarts; assert they are present as
    # freshness triggers, then drop them to compare the stable environment.
    hash_keys = {key for key in environment if key.startswith("HASH_")}
    assert hash_keys
    for key in hash_keys:
        del environment[key]

    assert got_services == want_services
    assert state_out.unit_status == ActiveStatus("Status check: UP")


def test_catalog_added(ctx):
    """The catalog directory is updated to add the new catalog."""
    state_in, ids = build_coordinator_state()
    extended_catalog_config = create_added_catalog_config(
        ids.postgresql,
        ids.mysql,
        ids.redshift,
        ids.bigquery,
        ids.gsheets,
    )
    state_in = dataclasses.replace(
        state_in,
        config={**state_in.config, "catalog-config": extended_catalog_config},
    )

    state_out = ctx.run(ctx.on.config_changed(), state_in)

    assert workload_path(state_out, ctx, POSTGRESQL_2_CATALOG_PATH).exists()
    assert workload_path(state_out, ctx, BIGQUERY_CATALOG_PATH).exists()


def test_catalog_removed(ctx, tmp_path):
    """The catalog directory is updated to remove existing catalogs."""
    # We need the mount for permanence because `ctx.run` is called twice.
    container = trino_container(
        mounts={"home": Mount(location="/usr/lib/trino/etc", source=tmp_path)}
    )
    state_in, _ = build_coordinator_state(container=container)

    # Establish the catalogs on disk.
    mid = carry_forward(ctx.run(ctx.on.config_changed(), state_in))
    assert (tmp_path / "catalog" / "postgresql-1.properties").exists()

    # Clear the catalog configuration and reconcile.
    mid = dataclasses.replace(mid, config={**mid.config, "catalog-config": ""})
    ctx.run(ctx.on.config_changed(), mid)

    assert not (tmp_path / "catalog" / "postgresql-1.properties").exists()
    assert not (tmp_path / "catalog" / "bigquery.properties").exists()


def test_worker_fetches_latest_catalog_on_relation_change(ctx):
    """The worker uses the latest catalog advertised on relation change."""
    state_in, ids = build_worker_state()
    old_catalog = ids.catalog_config

    extended_catalog_config = create_added_catalog_config(
        ids.postgresql,
        ids.mysql,
        ids.redshift,
        ids.bigquery,
        ids.gsheets,
    )

    # Advertise the extended catalog config on the worker relation.
    worker_relation = dataclasses.replace(
        ids.worker_relation,
        remote_app_data={
            **ids.worker_relation.remote_app_data,
            "catalogs": extended_catalog_config,
        },
    )
    relations = {
        relation for relation in state_in.relations if relation.id != ids.worker_relation.id
    }
    relations.add(worker_relation)
    state_in = dataclasses.replace(state_in, relations=relations)

    state_out = ctx.run(ctx.on.relation_changed(worker_relation), state_in)

    peer_relation = state_out.get_relation(ids.peer_relation.id)
    catalog_config = peer_state_value(peer_relation, "catalog_config")
    assert catalog_config == extended_catalog_config
    assert catalog_config != old_catalog
