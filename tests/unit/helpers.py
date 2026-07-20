#!/usr/bin/env python3
# Copyright 2023 Canonical Ltd.
# See LICENSE file for licensing details.

"""Literals and Scenario state builders for the Trino K8s charm unit tests."""

import dataclasses
import json
from types import SimpleNamespace

from ops.testing import Container, Exec, Model, PeerRelation, Relation, Secret, State

SERVER_PORT = "8080"

MODEL_NAME = "trino-model"

GSHEET_SECRET = """\
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
POSTGRESQL_REPLICA_SECRET = """\
rw:
  user: trino
  password: pwd1
  suffix: _developer
ro:
  user: trino_ro
  password: pwd2
"""  # nosec

POSTGRESQL_REPLICA_SECRET_WITH_PARAMS = """\
rw:
  user: trino
  password: pwd1
  suffix: _developer
  params: ssl=true&targetServerType=primary
ro:
  user: trino_ro
  password: pwd2
  params: ssl=true&targetServerType=preferSecondary
"""  # nosec
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
POSTGRESQL_REPLICA_CERT = """\
cert: |
  -----BEGIN CERTIFICATE-----
      CERTIFICATE CONTENT...
  -----END CERTIFICATE-----
"""  # nosec

POSTGRESQL_1_CATALOG_PATH = "/usr/lib/trino/etc/catalog/postgresql-1.properties"
POSTGRESQL_1_DEVELOPER_CATALOG_PATH = (
    "/usr/lib/trino/etc/catalog/postgresql-1_developer.properties"
)
POSTGRESQL_2_CATALOG_PATH = "/usr/lib/trino/etc/catalog/postgresql-2.properties"
MYSQL_CATALOG_PATH = "/usr/lib/trino/etc/catalog/mysql.properties"
REDSHIFT_CATALOG_PATH = "/usr/lib/trino/etc/catalog/redshift.properties"
BIGQUERY_CATALOG_PATH = "/usr/lib/trino/etc/catalog/bigquery.properties"
RANGER_AUDIT_PATH = "/usr/lib/trino/etc/ranger-trino-audit.xml"
RANGER_SECURITY_PATH = "/usr/lib/trino/etc/ranger-trino-security.xml"

POLICY_MGR_URL = "http://ranger-k8s:6080"

RANGER_LIB = "/usr/lib/ranger"

TEST_USERS = """\
    example_user: ubuntu123
    another_user: ubuntu345
"""

DEFAULT_JVM_STRING = " ".join(
    [
        "-Xmx2G",
        "-XX:InitialRAMPercentage=80",
        "-XX:+ExplicitGCInvokesConcurrent",
        "-XX:-OmitStackTraceInFastThrow",
        "-Djdk.attach.allowAttachSelf=true",
        "-Dfile.encoding=UTF-8",
        "-XX:+ExitOnOutOfMemoryError",
        "-XX:+HeapDumpOnOutOfMemoryError",
    ]
)

USER_JVM_STRING = "-Xmx4G -XX:InitialRAMPercentage=50 -Xxs10G"
UPDATED_JVM_OPTIONS = " ".join(
    [
        "-Xmx4G",
        "-XX:InitialRAMPercentage=50",
        "-XX:+ExplicitGCInvokesConcurrent",
        "-XX:-OmitStackTraceInFastThrow",
        "-Djdk.attach.allowAttachSelf=true",
        "-Dfile.encoding=UTF-8",
        "-XX:+ExitOnOutOfMemoryError",
        "-XX:+HeapDumpOnOutOfMemoryError",
        "-Xxs10G",
    ]
)


def trino_container(can_connect=True, **kwargs):
    """Build the Trino workload container with the standard mocked execs.

    Args:
        can_connect: whether Pebble is reachable.
        kwargs: extra keyword arguments forwarded to `Container`.

    Returns:
        A Scenario `Container` for the `trino` workload.
    """
    return Container(
        "trino",
        can_connect=can_connect,
        execs={
            Exec(["htpasswd"], return_code=0),
            Exec(["keytool"], return_code=0),
            Exec(["/bin/sh"], stdout="/usr/lib/jvm/java-25-openjdk-amd64/"),
            Exec(["bash"], return_code=0),
        },
        **kwargs,
    )


def observer_secret(content):
    """Build an observer (remotely owned, granted) Juju secret.

    Args:
        content: the secret content mapping.

    Returns:
        A Scenario `Secret` the charm can read with `get_content(refresh=True)`.
    """
    return Secret(tracked_content=content, owner=None)


def catalog_secrets():
    """Create the standard set of catalog secrets used by the lifecycle helpers.

    Returns:
        A `SimpleNamespace` with the individual secret objects, a `catalogs`
        set of all secrets, and the rendered `catalog_config` string.
    """
    bigquery = observer_secret({"service-accounts": BIGQUERY_SECRET})
    postgresql = observer_secret(
        {"replicas": POSTGRESQL_REPLICA_SECRET, "cert": POSTGRESQL_REPLICA_CERT}
    )
    mysql = observer_secret({"replicas": MYSQL_REPLICA_SECRET})
    redshift = observer_secret({"replicas": REDSHIFT_REPLICA_SECRET})
    gsheets = observer_secret({"service-accounts": GSHEET_SECRET})
    ns = SimpleNamespace(
        bigquery=bigquery,
        postgresql=postgresql,
        mysql=mysql,
        redshift=redshift,
        gsheets=gsheets,
    )
    ns.catalogs = {bigquery, postgresql, mysql, redshift, gsheets}
    ns.catalog_config = create_catalog_config(
        postgresql.id,
        mysql.id,
        redshift.id,
        bigquery.id,
        gsheets.id,
    )
    return ns


def build_coordinator_state(
    *,
    leader=True,
    config=None,
    extra_relations=(),
    extra_secrets=(),
    container=None,
):
    """Build the input `State` for a healthy Trino coordinator.

    Mirrors the end-state established by the legacy `simulate_lifecycle_coordinator`
    Harness helper: peer relation ready, catalog and user secrets present,
    `charm-function` set to `coordinator` and a `trino-coordinator` relation.

    Args:
        leader: whether the unit is the leader.
        config: extra config options merged over the coordinator defaults.
        extra_relations: additional relations to include in the state.
        extra_secrets: additional secrets to include in the state.
        container: an explicit container to use instead of the default.

    Returns:
        A `(State, SimpleNamespace)` tuple. The namespace carries the catalog
        secret IDs, the `catalog_config` string and the relation objects so
        tests can inspect the output databags.
    """
    cats = catalog_secrets()
    user = observer_secret({"users": TEST_USERS})
    coordinator_relation = Relation("trino-coordinator", remote_app_name="trino-k8s-worker")
    peer_relation = PeerRelation("peer")

    base_config = {
        "catalog-config": cats.catalog_config,
        "charm-function": "coordinator",
        "user-secret-id": user.id,
    }
    if config:
        base_config.update(config)

    state = State(
        leader=leader,
        model=Model(name=MODEL_NAME),
        config=base_config,
        containers={container or trino_container()},
        relations={peer_relation, coordinator_relation, *extra_relations},
        secrets={user, *cats.catalogs, *extra_secrets},
    )
    ids = SimpleNamespace(
        postgresql=cats.postgresql.id,
        mysql=cats.mysql.id,
        redshift=cats.redshift.id,
        bigquery=cats.bigquery.id,
        gsheets=cats.gsheets.id,
        user=user.id,
        catalog_config=cats.catalog_config,
        coordinator_relation=coordinator_relation,
        peer_relation=peer_relation,
    )
    return state, ids


def build_worker_state(
    *,
    leader=True,
    config=None,
    catalogs=None,
    include_int_comms=True,
    postgresql_secrets=None,
    extra_relations=(),
    extra_secrets=(),
    container=None,
):
    """Build the input `State` for a Trino worker related to a coordinator.

    Mirrors the end-state established by the legacy `simulate_lifecycle_worker`
    Harness helper. The `trino-worker` relation carries the coordinator's
    published data so a `relation-changed` event reproduces worker behaviour.

    Args:
        leader: whether the unit is the leader.
        config: extra config options merged over the worker defaults.
        catalogs: explicit catalog-config to advertise (defaults to the standard one).
        include_int_comms: whether the coordinator has published an int-comms secret.
        postgresql_secrets: mapping of PG password env vars the coordinator has
            published via a granted Juju secret, or None to publish none.
        extra_relations: additional relations to include in the state.
        extra_secrets: additional secrets to include in the state.
        container: an explicit container to use instead of the default.

    Returns:
        A `(State, SimpleNamespace)` tuple. The namespace carries the catalog
        secret IDs, the `catalog_config` string, the int-comms secret and the
        relation objects.
    """
    cats = catalog_secrets()
    catalog_config = cats.catalog_config if catalogs is None else catalogs
    secrets = set(cats.catalogs)

    remote_data = {
        "discovery-uri": "http://trino-k8s:8080",
        "catalogs": catalog_config,
    }
    int_comms = None
    if include_int_comms:
        int_comms = observer_secret({"secret": "test-int-comms-secret"})  # nosec B105
        secrets.add(int_comms)
        remote_data["int-comms-secret-id"] = int_comms.id

    pg_secret = None
    if postgresql_secrets:
        pg_secret = observer_secret(
            {"envvars": json.dumps(dict(postgresql_secrets), sort_keys=True)}
        )
        secrets.add(pg_secret)
        remote_data["postgresql-secrets-id"] = pg_secret.id

    worker_relation = Relation(
        "trino-worker",
        remote_app_name="trino-k8s-coordinator",
        remote_app_data=remote_data,
    )
    peer_relation = PeerRelation("peer")

    base_config = {"charm-function": "worker"}
    if config:
        base_config.update(config)

    state = State(
        leader=leader,
        model=Model(name=MODEL_NAME),
        config=base_config,
        containers={container or trino_container()},
        relations={peer_relation, worker_relation, *extra_relations},
        secrets={*secrets, *extra_secrets},
    )
    ids = SimpleNamespace(
        postgresql=cats.postgresql.id,
        mysql=cats.mysql.id,
        redshift=cats.redshift.id,
        bigquery=cats.bigquery.id,
        gsheets=cats.gsheets.id,
        catalog_config=catalog_config,
        int_comms=int_comms,
        pg_secret=pg_secret,
        worker_relation=worker_relation,
        peer_relation=peer_relation,
    )
    return state, ids


def workload_path(state, ctx, path, container="trino"):
    """Return a filesystem `Path` for a file inside the workload container.

    Args:
        state: the output `State` from `ctx.run`.
        ctx: the Scenario `Context`.
        path: the absolute in-container path.
        container: the container name.

    Returns:
        A `pathlib.Path` pointing at the mocked container file.
    """
    root = state.get_container(container).get_filesystem(ctx)
    return root / path.lstrip("/")


def peer_state_value(relation, name):
    """Decode a JSON-encoded value from the peer relation app databag.

    Args:
        relation: the peer `Relation` taken from the output state.
        name: the state attribute name.

    Returns:
        The decoded value, or `None` when absent.
    """
    raw = relation.local_app_data.get(name)
    return None if raw is None else json.loads(raw)


def carry_forward(state, container="trino"):
    """Return `state` prepared for a follow-up `ctx.run`.

    Scenario auto-populates a `CheckInfo` for plan checks with default
    attributes (e.g. `threshold=3`) that do not match the layer's check
    definition. Carrying such a container straight into another `ctx.run`
    trips the consistency checker, so the check statuses are cleared here.

    Args:
        state: the output `State` from a previous `ctx.run`.
        container: the container name to normalise.

    Returns:
        A new `State` whose container reports no check statuses.
    """
    cont = dataclasses.replace(state.get_container(container), check_infos=frozenset())
    return dataclasses.replace(state, containers={cont})


def create_catalog_config(
    postgresql_secret_id,
    mysql_secret_id,
    redshift_secret_id,
    bigquery_secret_id,
    gsheets_secret_id,
):
    """Create and return catalog-config value.

    Args:
        postgresql_secret_id: the juju secret id for postgresql
        mysql_secret_id: the juju secret id for mysql
        redshift_secret_id: the juju secret id for redshift
        bigquery_secret_id: the juju secret id for bigquery
        gsheets_secret_id: the juju secret id for googlesheets

    Returns:
        the catalog configuration.
    """
    return f"""\
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
        gsheets:
            connector: gsheets
    """  # noqa: E501


def create_added_catalog_config(
    postgresql_secret_id,
    mysql_secret_id,
    redshift_secret_id,
    bigquery_secret_id,
    gsheets_secret_id,
):
    """Create and return catalog-config value, with added catalog.

    Args:
        postgresql_secret_id: the juju secret id for postgresql
        mysql_secret_id: the juju secret id for mysql
        redshift_secret_id: the juju secret id for redshift
        bigquery_secret_id: the juju secret id for bigquery
        gsheets_secret_id: the juju secret id for googlesheets

    Returns:
        the catalog configuration, with an added catalog.
    """
    return f"""\
    catalogs:
        postgresql-1:
            backend: dwh
            database: example
            secret-id: {postgresql_secret_id}
        postgresql-2:
            backend: dwh
            database: updated-db
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
        gsheets:
            connector: gsheets
    """  # noqa: E501


def create_single_catalog_config(postgresql_secret_id, backend_params=None):
    """Create a minimal catalog-config with a single postgresql catalog.

    Args:
        postgresql_secret_id: the juju secret id for postgresql
        backend_params: optional JDBC params string for the backend. When None,
            the backend declares no params and replicas must provide their own.

    Returns:
        catalog configuration string.
    """
    params_line = f"\n            params: {backend_params}" if backend_params else ""
    return f"""\
    catalogs:
        postgresql-1:
            backend: dwh
            database: example
            secret-id: {postgresql_secret_id}
    backends:
        dwh:
            connector: postgresql
            url: jdbc:postgresql://example.com:5432{params_line}
            config: |
                case-insensitive-name-matching=true
    """
