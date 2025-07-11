#!/usr/bin/env python3
# Copyright 2023 Canonical Ltd.
# See LICENSE file for licensing details.

"""Literals used by the Trino K8s charm unit tests."""

SERVER_PORT = "8080"

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
"""  # nosec

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

POSTGRESQL_1_CATALOG_PATH = (
    "/usr/lib/trino/etc/catalog/postgresql-1.properties"
)
POSTGRESQL_2_CATALOG_PATH = (
    "/usr/lib/trino/etc/catalog/postgresql-2.properties"
)
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


def simulate_lifecycle_worker(harness):
    """Establish a relation between Trino worker and coordinator.

    Args:
        harness: ops.testing.Harness object used to simulate trino relation.

    Returns:
        container: the trino application container.
        event: the relation event.
    """
    # Simulate peer relation readiness.
    harness.add_relation("peer", "trino")

    # Simulate pebble readiness.
    harness.handle_exec("trino", ["keytool"], result=0)
    harness.handle_exec("trino", ["htpasswd"], result=0)
    harness.handle_exec(
        "trino", ["/bin/sh"], result="/usr/lib/jvm/java-21-openjdk-amd64/"
    )
    harness.update_config({"charm-function": "worker"})

    # Add catalog secrets
    bigquery_secret_id = harness.add_model_secret(
        "trino-k8s",
        {"service-accounts": BIGQUERY_SECRET},
    )

    postgresql_secret_id = harness.add_model_secret(
        "trino-k8s",
        {
            "replicas": POSTGRESQL_REPLICA_SECRET,
            "cert": POSTGRESQL_REPLICA_CERT,
        },
    )
    mysql_secret_id = harness.add_model_secret(
        "trino-k8s",
        {"replicas": MYSQL_REPLICA_SECRET},
    )
    redshift_secret_id = harness.add_model_secret(
        "trino-k8s",
        {"replicas": REDSHIFT_REPLICA_SECRET},
    )
    gsheets_secret_id = harness.add_model_secret(
        "trino-k8s",
        {"service-accounts": GSHEET_SECRET},
    )
    catalog_config = create_catalog_config(
        postgresql_secret_id,
        mysql_secret_id,
        redshift_secret_id,
        bigquery_secret_id,
        gsheets_secret_id,
    )

    container = harness.model.unit.get_container("trino")
    harness.charm.on.trino_pebble_ready.emit(container)

    rel_id = harness.add_relation("trino-worker", "trino-k8s")
    harness.add_relation_unit(rel_id, "trino-k8s-worker/0")

    data = {
        "trino-worker": {
            "discovery-uri": "http://trino-k8s:8080",
            "catalogs": catalog_config,
        }
    }
    event = make_relation_event("trino-worker", rel_id, data)
    harness.charm.trino_worker._on_relation_changed(event)
    return (
        container,
        event,
        postgresql_secret_id,
        mysql_secret_id,
        redshift_secret_id,
        bigquery_secret_id,
        gsheets_secret_id,
    )


def simulate_lifecycle_coordinator(harness):
    """Simulate a healthy charm life-cycle.

    Args:
        harness: ops.testing.Harness object used to simulate charm lifecycle.

    Returns:
        rel_id: the relation ID of the trino coordinator:worker relation.
    """
    # Simulate peer relation readiness.
    harness.add_relation("peer", "trino")

    # Simulate pebble readiness.
    container = harness.model.unit.get_container("trino")
    harness.handle_exec("trino", ["htpasswd"], result=0)
    harness.handle_exec(
        "trino", ["/bin/sh"], result="/usr/lib/jvm/java-21-openjdk-amd64/"
    )
    harness.charm.on.trino_pebble_ready.emit(container)

    secret_id = harness.add_model_secret(
        "trino-k8s",
        {"users": TEST_USERS},
    )

    harness.charm.on.trino_pebble_ready.emit(container)

    # Add catalog secrets
    bigquery_secret_id = harness.add_model_secret(
        "trino-k8s",
        {"service-accounts": BIGQUERY_SECRET},
    )

    postgresql_secret_id = harness.add_model_secret(
        "trino-k8s",
        {
            "replicas": POSTGRESQL_REPLICA_SECRET,
            "cert": POSTGRESQL_REPLICA_CERT,
        },
    )
    mysql_secret_id = harness.add_model_secret(
        "trino-k8s",
        {
            "replicas": MYSQL_REPLICA_SECRET,
        },
    )
    redshift_secret_id = harness.add_model_secret(
        "trino-k8s",
        {
            "replicas": REDSHIFT_REPLICA_SECRET,
        },
    )
    gsheets_secret_id = harness.add_model_secret(
        "trino-k8s",
        {"service-accounts": GSHEET_SECRET},
    )

    catalog_config = create_catalog_config(
        postgresql_secret_id,
        mysql_secret_id,
        redshift_secret_id,
        bigquery_secret_id,
        gsheets_secret_id,
    )

    # Add worker and coordinator relation
    harness.handle_exec("trino", ["keytool"], result=0)
    harness.update_config({"catalog-config": catalog_config})
    rel_id = harness.add_relation("trino-coordinator", "trino-k8s-worker")

    harness.update_config({"user-secret-id": secret_id})
    return (
        rel_id,
        postgresql_secret_id,
        mysql_secret_id,
        redshift_secret_id,
        bigquery_secret_id,
        gsheets_secret_id,
    )


def make_relation_event(app, rel_id, data):
    """Create and return a mock policy created event.

        The event is generated by the relation with postgresql_db

    Args:
        app: name of the application.
        rel_id: relation id.
        data: relation data.

    Returns:
        Event dict.
    """
    return type(
        "Event",
        (),
        {
            "app": app,
            "relation": type(
                "Relation",
                (),
                {
                    "data": data,
                    "id": rel_id,
                },
            ),
        },
    )


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
    """


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
    """
