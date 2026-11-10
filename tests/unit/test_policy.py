# Copyright 2023 Canonical Ltd.
# See LICENSE file for licensing details.
#
# Learn more about testing at: https://juju.is/docs/sdk/testing


"""Trino policy unit tests."""

# pylint:disable=protected-access

import dataclasses
import logging
import re

from ops.model import ActiveStatus, BlockedStatus
from ops.pebble import CheckLevel, CheckStartup, CheckStatus
from ops.testing import CheckInfo, Relation

from tests.unit.helpers import (
    POLICY_MGR_URL,
    RANGER_AUDIT_PATH,
    RANGER_SECURITY_PATH,
    build_coordinator_state,
    carry_forward,
    failing_secret_reads,
    observer_secret,
    workload_path,
)

logger = logging.getLogger(__name__)

ACCESS_CONTROL_PATH = "/usr/lib/trino/etc/access-control.properties"

OPENSEARCH_TLS_CA = """-----BEGIN CERTIFICATE-----
    MIIC+DCCAeCgAwIBAgIJAKJdWfG2zRAQMA0GCSqGSIb3DQEBCwUAMIGPMQswCQYD
    -----END CERTIFICATE-----
    -----BEGIN CERTIFICATE-----
    AIBC+LCCAuCgAPIBAgIuAKJdWWG2zRAQMA0GFSqGSIP3DQEBCiUAMIGPMQswCQYC
    -----END CERTIFICATE-----"""


def _up_check_container(state):
    """Return the container with a healthy `up` check applied."""
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
        ctx: the Scenario `Context` for the charm.

    Returns:
        A `(State, Relation)` tuple with the OpenSearch index created and the
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
    policy_rel = state.get_relation(policy_rel.id)
    state = carry_forward(ctx.run(ctx.on.relation_changed(policy_rel), state))

    # Create the OpenSearch index. The OpenSearch credentials and certificate
    # are resolved from real Juju secrets shared over the relation.
    opensearch_rel = state.get_relation(opensearch_rel.id)
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

    policy_rel = bootstrapped.get_relation(policy_rel.id)
    state_out = ctx.run(ctx.on.relation_changed(policy_rel), bootstrapped)

    ranger_config = workload_path(state_out, ctx, RANGER_SECURITY_PATH).read_text()
    assert POLICY_MGR_URL in ranger_config


def test_ranger_service_name_defaults_to_relation(ctx):
    """Ranger service name falls back to `relation_<id>` when config is unset."""
    policy_rel = Relation(
        "policy",
        remote_app_name="ranger-k8s",
        remote_app_data={"policy_manager_url": POLICY_MGR_URL},
    )
    state_in, _ = build_coordinator_state(extra_relations=[policy_rel])
    bootstrapped = carry_forward(ctx.run(ctx.on.config_changed(), state_in))

    policy_rel = bootstrapped.get_relation(policy_rel.id)
    state_out = ctx.run(ctx.on.relation_changed(policy_rel), bootstrapped)

    ranger_config = workload_path(state_out, ctx, RANGER_SECURITY_PATH).read_text()
    expected = (
        f"<name>ranger.plugin.trino.service.name</name><value>relation_{policy_rel.id}</value>"
    )
    assert expected in re.sub(r"\s", "", ranger_config)


def test_policy_relation_broken(ctx):
    """Removing the policy relation disables the Ranger plugin."""
    policy_rel = Relation("policy", remote_app_name="ranger-k8s")
    state_in, _ = build_coordinator_state(extra_relations=[policy_rel])
    bootstrapped = carry_forward(ctx.run(ctx.on.config_changed(), state_in))

    policy_rel = bootstrapped.get_relation(policy_rel.id)
    state_out = ctx.run(ctx.on.relation_broken(policy_rel), bootstrapped)

    relation_data = state_out.get_relation(policy_rel.id).local_app_data
    assert not relation_data.get("user-group-configuration")


def test_on_opensearch_index_created(ctx):
    """Test handling of opensearch relation changed events."""
    state, _ = _opensearch_setup(ctx)

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

    state = ctx.run(ctx.on.relation_broken(opensearch_rel), state)

    ranger_config = workload_path(state, ctx, RANGER_AUDIT_PATH).read_text()
    assert "testuser" not in ranger_config

    state = dataclasses.replace(state, containers={_up_check_container(state)})
    state_out = ctx.run(ctx.on.update_status(), state)
    assert state_out.unit_status == ActiveStatus("Status check: UP")


def _opensearch_state(ctx, *, endpoints, tls_ca):
    """Bootstrap a coordinator with a custom OpenSearch endpoint and tls-ca.

    Args:
        ctx: the Scenario `Context` for the charm.
        endpoints: the raw `endpoints` value published on the relation.
        tls_ca: the raw `tls-ca` bundle stored in the OpenSearch TLS secret.

    Returns:
        A `(State, Relation)` tuple ready for a relation-changed run.
    """
    user_secret = observer_secret({"username": "testuser", "password": "testpassword"})  # nosec B105
    tls_secret = observer_secret({"tls-ca": tls_ca})
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
            "endpoints": endpoints,
        },
    )
    state_in, _ = build_coordinator_state(
        extra_relations=[policy_rel, opensearch_rel],
        extra_secrets=[user_secret, tls_secret],
    )
    state = carry_forward(ctx.run(ctx.on.config_changed(), state_in))
    policy_rel = state.get_relation(policy_rel.id)
    state = carry_forward(ctx.run(ctx.on.relation_changed(policy_rel), state))
    return state, state.get_relation(opensearch_rel.id)


def test_opensearch_portless_endpoint_disables(ctx):
    """A malformed (portless) endpoint disables OpenSearch without erroring."""
    state, opensearch_rel = _opensearch_state(
        ctx, endpoints="opensearch-host", tls_ca=OPENSEARCH_TLS_CA
    )

    state_out = ctx.run(ctx.on.relation_changed(opensearch_rel), state)

    ranger_config = workload_path(state_out, ctx, RANGER_AUDIT_PATH).read_text()
    assert "testuser" not in ranger_config


def test_opensearch_single_cert_bundle_no_error(ctx):
    """A single-cert tls-ca bundle yields no certificate but does not error."""
    single_ca = (
        "-----BEGIN CERTIFICATE-----\n"
        "MIIC+DCCAeCgAwIBAgIJAKJdWfG2zRAQMA0GCSqGSIb3DQEBCwUAMIGPMQswCQYD\n"
        "-----END CERTIFICATE-----"
    )
    state, opensearch_rel = _opensearch_state(
        ctx, endpoints="opensearch-host:port", tls_ca=single_ca
    )

    state = ctx.run(ctx.on.relation_changed(opensearch_rel), state)

    state = dataclasses.replace(state, containers={_up_check_container(state)})
    state_out = ctx.run(ctx.on.update_status(), state)
    assert state_out.unit_status == ActiveStatus("Status check: UP")


def test_opensearch_secret_timeout_keeps_running_plan(ctx):
    """A failed OpenSearch secret read keeps the running plan and blocks the unit.

    Rendering without the OpenSearch CA or credentials changes the plan hashes,
    and the replan restarts the coordinator.
    """
    state, opensearch_rel = _opensearch_setup(ctx)
    state = carry_forward(state)
    plan_before = state.get_container("trino").plan.to_dict()
    remote = state.get_relation(opensearch_rel.id).remote_app_data

    for failing in (remote["secret-tls"], remote["secret-user"]):
        with failing_secret_reads(failing):
            state_out = ctx.run(ctx.on.update_status(), state)

        assert state_out.get_container("trino").plan.to_dict() == plan_before
        assert state_out.unit_status == BlockedStatus(
            "cannot read opensearch relation secret: "
            "creating remote secret client: cannot log in: context deadline exceeded"
        )


def test_opensearch_secret_timeout_on_fresh_pod_starts_trino(ctx):
    """With no Trino service planned yet, a failed read does not keep it from starting."""
    state, opensearch_rel = _opensearch_setup(ctx)
    state = carry_forward(state)
    fresh = dataclasses.replace(state.get_container("trino"), layers={}, service_statuses={})
    state = dataclasses.replace(state, containers={fresh})
    remote = state.get_relation(opensearch_rel.id).remote_app_data

    with failing_secret_reads(remote["secret-tls"]):
        state_out = ctx.run(ctx.on.update_status(), state)

    assert "trino" in state_out.get_container("trino").plan.services
    assert state_out.unit_status.name == "blocked"


def test_opensearch_secret_timeout_config_change_keeps_audit_only(ctx):
    """A config change during a failed read applies, and only the audit config is kept."""
    state, opensearch_rel = _opensearch_setup(ctx)
    state = carry_forward(state)
    env_before = state.get_container("trino").plan.services["trino"].environment
    remote = state.get_relation(opensearch_rel.id).remote_app_data
    state = dataclasses.replace(state, config={**state.config, "log-level": "debug"})

    with failing_secret_reads(remote["secret-tls"]):
        state_out = ctx.run(ctx.on.config_changed(), state)

    env_after = state_out.get_container("trino").plan.services["trino"].environment
    assert env_after != env_before
    assert env_after["HASH_RANGER_TRINO_AUDIT_XML"] == env_before["HASH_RANGER_TRINO_AUDIT_XML"]
    assert env_after["HASH_CACERTS"] == env_before["HASH_CACERTS"]
    access_control = workload_path(state_out, ctx, ACCESS_CONTROL_PATH).read_text()
    policy_rel = next(rel for rel in state.relations if rel.endpoint == "policy")
    assert f"ranger.service.name=relation_{policy_rel.id}" in access_control
