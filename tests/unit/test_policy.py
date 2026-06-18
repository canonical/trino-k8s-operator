# Copyright 2023 Canonical Ltd.
# See LICENSE file for licensing details.
#
# Learn more about testing at: https://juju.is/docs/sdk/testing


"""Trino policy unit tests."""

# pylint:disable=protected-access

import dataclasses
import logging
import re

from ops.model import ActiveStatus, MaintenanceStatus
from ops.pebble import CheckLevel, CheckStartup, CheckStatus
from ops.testing import CheckInfo, Relation

from tests.unit.helpers import (
    POLICY_MGR_URL,
    RANGER_AUDIT_PATH,
    RANGER_SECURITY_PATH,
    build_coordinator_state,
    carry_forward,
    observer_secret,
    workload_path,
)

logger = logging.getLogger(__name__)

OPENSEARCH_TLS_CA = """-----BEGIN CERTIFICATE-----
    MIIC+DCCAeCgAwIBAgIJAKJdWfG2zRAQMA0GCSqGSIb3DQEBCwUAMIGPMQswCQYD
    -----END CERTIFICATE-----
    -----BEGIN CERTIFICATE-----
    AIBC+LCCAuCgAPIBAgIuAKJdWWG2zRAQMA0GFSqGSIP3DQEBCiUAMIGPMQswCQYC
    -----END CERTIFICATE-----"""


def _up_check_container(state):
    """Return the container with a healthy ``up`` check applied."""
    return dataclasses.replace(
        state.get_container("trino"),
        check_infos={
            CheckInfo(
                "up",
                status=CheckStatus.UP,
                level=CheckLevel.UNSET,
                startup=CheckStartup.UNSET,
                threshold=None,
            )
        },
    )


def _opensearch_setup(ctx):
    """Simulate a coordinator with Ranger and OpenSearch enabled.

    Args:
        ctx: the Scenario ``Context`` for the charm.

    Returns:
        A ``(State, Relation)`` tuple with the OpenSearch index created and the
        OpenSearch relation object.
    """
    user_secret = observer_secret({"username": "testuser", "password": "testpassword"})  # nosec B105
    tls_secret = observer_secret({"tls-ca": OPENSEARCH_TLS_CA})
    policy_rel = Relation(
        "policy",
        remote_app_name="ranger-k8s",
        remote_app_data={"policy_manager_url": POLICY_MGR_URL},
    )
    opensearch_rel = Relation(
        "opensearch",
        remote_app_name="opensearch-app",
        remote_app_data={
            "secret-user": user_secret.id,
            "secret-tls": tls_secret.id,
            "endpoints": "opensearch-host:port",
        },
    )
    state_in, _ = build_coordinator_state(
        extra_relations=[policy_rel, opensearch_rel],
        extra_secrets=[user_secret, tls_secret],
    )

    # Bootstrap the workload (Pebble layer + truststore password).
    state = carry_forward(ctx.run(ctx.on.config_changed(), state_in))

    # Enable the Ranger plugin via the policy relation.
    state = carry_forward(ctx.run(ctx.on.relation_changed(policy_rel), state))

    # Create the OpenSearch index. The OpenSearch credentials and certificate
    # are resolved from real Juju secrets shared over the relation.
    state = ctx.run(ctx.on.relation_changed(opensearch_rel), state)

    return state, opensearch_rel


def test_policy_relation_created(ctx):
    """Add policy relation."""
    policy_rel = Relation("policy", remote_app_name="ranger-k8s")
    state_in, _ = build_coordinator_state(extra_relations=[policy_rel])

    state_out = ctx.run(ctx.on.relation_created(policy_rel), state_in)

    relation_data = state_out.get_relation(policy_rel.id).local_app_data
    assert relation_data == {
        "name": f"relation_{policy_rel.id}",
        "type": "trino",
        "jdbc.driverClassName": "io.trino.jdbc.TrinoDriver",
        "jdbc.url": "jdbc:trino://trino-k8s.trino-model.svc.cluster.local:8080",
    }


def test_policy_relation_changed(ctx):
    """Add policy_manager_url to the relation databag."""
    policy_rel = Relation(
        "policy",
        remote_app_name="ranger-k8s",
        remote_app_data={"policy_manager_url": POLICY_MGR_URL},
    )
    state_in, _ = build_coordinator_state(extra_relations=[policy_rel])
    bootstrapped = carry_forward(ctx.run(ctx.on.config_changed(), state_in))

    state_out = ctx.run(ctx.on.relation_changed(policy_rel), bootstrapped)

    ranger_config = workload_path(state_out, ctx, RANGER_SECURITY_PATH).read_text()
    assert POLICY_MGR_URL in ranger_config


def test_policy_relation_broken(ctx):
    """Removing the policy relation disables the Ranger plugin."""
    policy_rel = Relation("policy", remote_app_name="ranger-k8s")
    state_in, _ = build_coordinator_state(extra_relations=[policy_rel])
    bootstrapped = carry_forward(ctx.run(ctx.on.config_changed(), state_in))

    state_out = ctx.run(ctx.on.relation_broken(policy_rel), bootstrapped)

    relation_data = state_out.get_relation(policy_rel.id).local_app_data
    assert not relation_data.get("user-group-configuration")


def test_on_opensearch_index_created(ctx):
    """Test handling of opensearch relation changed events."""
    state, _ = _opensearch_setup(ctx)

    assert state.unit_status == MaintenanceStatus("Restarting Ranger plugin")
    assert workload_path(state, ctx, "/opensearch.crt").exists()

    ranger_config = workload_path(state, ctx, RANGER_AUDIT_PATH).read_text()
    ranger_config = re.sub(r"\s", "", ranger_config)
    user_config = (
        "<name>xasecure.audit.destination.elasticsearch.user</name><value>testuser</value>"
    )
    assert user_config in ranger_config

    state = dataclasses.replace(state, containers={_up_check_container(state)})
    state_out = ctx.run(ctx.on.update_status(), state)
    assert state_out.unit_status == ActiveStatus("Status check: UP")


def test_on_opensearch_relation_broken(ctx):
    """Test handling of broken relations with opensearch."""
    state, opensearch_rel = _opensearch_setup(ctx)
    state = carry_forward(state)
    opensearch_rel = state.get_relation(opensearch_rel.id)

    # The OpenSearch certificate was installed when the index was created; seed
    # it so the broken handler can remove it (the workload filesystem is reset
    # between Scenario runs).
    with ctx(ctx.on.relation_broken(opensearch_rel), state) as mgr:
        mgr.charm.unit.get_container("trino").push(
            "/opensearch.crt", "certificate", make_dirs=True
        )
        state = mgr.run()

    assert state.unit_status == MaintenanceStatus("Restarting Ranger plugin")
    assert not workload_path(state, ctx, "/opensearch.crt").exists()

    ranger_config = workload_path(state, ctx, RANGER_AUDIT_PATH).read_text()
    assert "testuser" not in ranger_config

    state = dataclasses.replace(state, containers={_up_check_container(state)})
    state_out = ctx.run(ctx.on.update_status(), state)
    assert state_out.unit_status == ActiveStatus("Status check: UP")
