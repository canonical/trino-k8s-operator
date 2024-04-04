#!/usr/bin/env python3
# Copyright 2023 Canonical Ltd.
# See LICENSE file for licensing details.

"""Literals used by the Trino K8s charm."""
import textwrap

APP_NAME = "trino-k8s"
TRINO_PORTS = {
    "HTTPS": 8443,
    "HTTP": 8080,
}

# Observability literals
PROMETHEUS_PORT = 12345
LOG_FILE = "data/trino/var/log/http-request.log"
JMX_PATH = "trino/jmx/config.yaml"
JMX_RULES = textwrap.dedent(
    """
    rules:
    - pattern: ".*"
    """
)

# Configuration literals
TRINO_HOME = "/usr/lib/trino/etc"
CONFIG_FILES = {
    "config.jinja": "config.properties",
    "logging.jinja": "log.properties",
    "password-authenticator.jinja": "password-authenticator.properties",
    "jvm.jinja": "jvm.config",
}

CONF_DIR = "conf"
CATALOG_DIR = "catalog"
RUN_TRINO_COMMAND = "./entrypoint.sh"

# Authentication literals
PASSWORD_DB = "password.db"  # nosec

# Ranger plugin literals
RANGER_PLUGIN_VERSION = "2.4.0"
RANGER_PLUGIN_HOME = "/usr/lib/ranger"
RANGER_PLUGIN_FILES = {
    "access-control.properties": "access-control.properties",
    "ranger-plugin.jinja": "install.properties",
}

JAVA_ENV = {"JAVA_HOME": "/usr/lib/jvm/java-21-openjdk-amd64"}


# UNIX literals
UNIX_TYPE_MAPPING = {
    "user": "passwd",
    "group": "group",
    "membership": "group",
}

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

SYSTEM_CONNECTORS = ["jmx", "memory", "tpcds", "tpch"]
