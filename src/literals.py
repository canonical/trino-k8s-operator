#!/usr/bin/env python3
# Copyright 2023 Canonical Ltd.
# See LICENSE file for licensing details.

"""Literals used by the Trino K8s charm."""

APP_NAME = "trino-k8s"
PEER_RELATION_NAME = "peer"
POSTGRESQL_RELATION_NAME = "postgresql"
TRINO_COORDINATOR_RELATION_NAME = "trino-coordinator"
TRINO_WORKER_RELATION_NAME = "trino-worker"
TRINO_CATALOG_RELATION_NAME = "trino-catalog"
POLICY_RELATION_NAME = "policy"
OPENSEARCH_RELATION_NAME = "opensearch"
INGRESS_RELATION_NAME = "ingress"
TRINO_PORTS = {
    "HTTPS": 443,
    "HTTP": 8080,
}

# Observability literals
METRICS_PORT = 9090
JMX_PORT = 9081

# Configuration literals
TRINO_HOME = "/usr/lib/trino/etc"
# JAVA_HOME is baked into the rock image (see trino_rock/rockcraft.yaml); update
# this literal when the rock's bundled JDK version changes.
JAVA_HOME = "/usr/lib/jvm/java-25-openjdk-amd64"
CONFIG_FILES = {
    "config.jinja": "config.properties",
    "logging.jinja": "log.properties",
    "password-authenticator.jinja": "password-authenticator.properties",
    "access-control.jinja": "access-control.properties",
    "rules.jinja": "rules.json",
    "jvm.jinja": "jvm.config",
}

CONF_DIR = "conf"
CATALOG_DIR = "catalog"
RUN_TRINO_COMMAND = "./entrypoint.sh"
TRINO_PLUGIN_DIR = "/usr/lib/trino/plugin"

# Authentication literals
PASSWORD_DB = "password.db"  # nosec

# Ranger plugin literals
RANGER_PLUGIN_FILES = {
    "access-control.jinja": "access-control.properties",
    "ranger-trino-security.jinja": "ranger-trino-security.xml",
    "ranger-trino-audit.jinja": "ranger-trino-audit.xml",
}

SECRET_LABEL = "catalog-config"  # nosec


# OpenSearch literals
INDEX_NAME = "ranger_audits"
CERTIFICATE_NAME = "opensearch-ca"

DEFAULT_CREDENTIALS = {"trino": "trinoR0cks!"}

DEFAULT_JVM_OPTIONS = [
    "-Xmx2G",
    "-XX:InitialRAMPercentage=80",
    "-XX:+ExplicitGCInvokesConcurrent",
    "-XX:-OmitStackTraceInFastThrow",
    "-Djdk.attach.allowAttachSelf=true",
    "-Dfile.encoding=UTF-8",
    "-XX:+ExitOnOutOfMemoryError",
    "-XX:+HeapDumpOnOutOfMemoryError",
]
USER_SECRET_LABEL = "trino-user-management"  # nosec
TRINO_CATALOG_SECRET_PREFIX = "trino-catalog-user-"  # nosec
INT_COMMS_SECRET_LABEL = "trino-int-comms-secret"  # nosec
INT_COMMS_SECRET_RELATION_KEY = "int-comms-secret-id"  # nosec
POSTGRESQL_SECRET_LABEL = "trino-postgresql-secrets"  # nosec
POSTGRESQL_SECRET_RELATION_KEY = "postgresql-secrets-id"  # nosec
TRUSTSTORE_SECRET_LABEL = "trino-truststore-password"  # nosec

# Sidecar manifests tracking managed truststore aliases (under CONF_DIR).
TRUSTSTORE_MANIFEST = ".truststore-manifest.json"  # nosec
CACERTS_MANIFEST = ".cacerts-manifest.json"  # nosec
CACERTS_PATH = "lib/security/cacerts"  # nosec
CATALOG_SCHEMA = {
    "backend": {"type": "string"},
    "database": {"type": "string", "nullable": True},
    "project": {"type": "string", "nullable": True},
    "metasheet-id": {"type": "string", "nullable": True},
    "secret-id": {"type": "string"},
}

SQL_BACKEND_SCHEMA = {
    "connector": {"type": "string"},
    "url": {"type": "string"},
    "params": {"type": "string"},
    "config": {"type": "string", "nullable": True},
}

REPLICA_SCHEMA = {
    "user": {"type": "string"},
    "password": {"type": "string"},
    "suffix": {"type": "string", "nullable": True},
    "params": {"type": "string", "nullable": True},
}

BIGQUERY_BACKEND_SCHEMA = {
    "connector": {"type": "string"},
    "config": {"type": "string", "nullable": True},
}

GSHEETS_BACKEND_SCHEMA = {
    "connector": {"type": "string"},
    "config": {"type": "string", "nullable": True},
}
