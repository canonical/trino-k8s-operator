#!/usr/bin/env python3
# Copyright 2023 Canonical Ltd.
# See LICENSE file for licensing details.

"""Literals used by the Trino K8s charm."""

APP_NAME = "trino-k8s"
TRINO_PORTS = {
    "HTTPS": 8443,
    "HTTP": 8080,
}

# Observability literals
METRICS_PORT = 9090
JMX_PORT = 9081

# Configuration literals
TRINO_HOME = "/usr/lib/trino/etc"
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
}

BIGQUERY_BACKEND_SCHEMA = {
    "connector": {"type": "string"},
    "config": {"type": "string", "nullable": True},
}

GSHEETS_BACKEND_SCHEMA = {
    "connector": {"type": "string"},
    "config": {"type": "string", "nullable": True},
}
