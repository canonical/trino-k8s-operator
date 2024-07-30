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
LOG_FILES = [
    "data/trino/var/log/http-request.log",
    "data/trino/var/log/launcher.log",
    "data/trino/var/log/server.log",
]

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
RANGER_PLUGIN_VERSION = "2.4.0"
RANGER_PLUGIN_HOME = "/usr/lib/ranger"
RANGER_PLUGIN_FILES = {
    "ranger-plugin.jinja": "install.properties",
    "access-control.jinja": "access-control.properties",
}

JAVA_ENV = {
    "JAVA_HOME": "/usr/lib/jvm/java-21-openjdk-amd64",
}

SECRET_LABEL = "catalog-config"  # nosec

# Connector literal
CONNECTOR_FIELDS = {
    "elasticsearch": {
        "required": [
            "connector.name",
            "elasticsearch.host",
            "elasticsearch.port",
            "elasticsearch.default-schema-name",
        ],
        "optional": [],
    },
    "mysql": {
        "required": [
            "connector.name",
            "connection-url",
            "connection-user",
            "connection-password",
        ],
        "optional": [
            "case-insensitive-name-matching",
            "case-insensitive-name-matching.cache-ttl",
            "metadata.cache-ttl",
            "metadata.cache-missing",
            "metadata.cache-maximum-size",
            "write.batch-size",
            "dynamic-filtering.enabled",
            "dynamic-filtering.wait-timeout",
        ],
    },
    "postgresql": {
        "required": [
            "connector.name",
            "connection-url",
            "connection-user",
            "connection-password",
        ],
        "optional": [
            "case-insensitive-name-matching",
            "case-insensitive-name-matching.cache-ttl",
            "metadata.cache-ttl",
            "metadata.cache-missing",
            "metadata.cache-maximum-size",
            "write.batch-size",
            "dynamic-filtering.enabled",
            "dynamic-filtering.wait-timeout",
        ],
    },
    "redis": {
        "required": ["connector.name", "redis.table-names", "redis.nodes"],
        "optional": [],
    },
}

JAVA_HOME = "/usr/lib/jvm/java-21-openjdk-amd64"

# OpenSearch literals
INDEX_NAME = "ranger_audits"
CERTIFICATE_NAME = "opensearch-ca"
