#!/usr/bin/env python3
# Copyright 2023 Canonical Ltd.
# See LICENSE file for licensing details.

"""Literals used by the Trino K8s charm."""

APP_NAME = "trino-k8s"
TRINO_PORTS = {
    "HTTPS": 8443,
    "HTTP": 8080,
}

# Configuration literals
TRINO_HOME = "/usr/lib/trino/etc"
CONFIG_FILES = {
    "config.jinja": "config.properties",
    "logging.jinja": "log.properties",
    "password-authenticator.jinja": "password-authenticator.properties",
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
    "accumlo": {
        "required": [
            "connector.name",
            "accumlo.instance",
            "accumlo.zookeepers",
            "accumlo.username",
            "accumlo.password",
        ],
        "optional": [],
    },
    "bigquery": {
        "required": ["connector.name", "bigquery.project-id"],
        "optional": [],
    },
    "cassandra": {
        "required": [
            "connector.name",
            "cassandra.contact-points",
            "cassandra.load-policy.dc-aware.local-dc",
        ],
        "optional": [],
    },
    "clickhouse": {
        "required": [
            "connector.name",
            "connection-url",
            "connection-user",
            "connection-password",
        ],
        "optional": [],
    },
    "delta_lake": {
        "required": ["connector.name", "hive.metastore.uri"],
        "optional": [],
    },
    "druid": {
        "required": ["connector.name", "connection-url"],
        "optional": [],
    },
    "elasticsearch": {
        "required": [
            "connector.name",
            "elasticsearch.host",
            "elasticsearch.port",
            "elasticsearch.default-schema-name",
        ],
        "optional": [],
    },
    "hive": {
        "required": ["connector.name", "hive.metastore.uri"],
        "optional": [],
    },
    "hudi": {
        "required": ["connector.name", "hive.metastore.uri"],
        "optional": [],
    },
    "ignite": {
        "required": [
            "connector.name",
            "connection-url",
            "connection-user",
            "connection-password",
        ],
        "optional": [],
    },
    "jmx": {
        "required": [
            "connector.name",
            "jmx.dump-tables",
            "jmx.dump-period",
            "jmx.max-entries",
        ],
        "optional": [],
    },
    "kinesis": {
        "required": [
            "connector.name",
            "kinesis.access-key",
            "kinesis.secret-key",
        ],
        "optional": [],
    },
    "mariadb": {
        "required": [
            "connector.name",
            "connection-url",
            "connection-user",
            "connection-password",
        ],
        "optional": [],
    },
    "mongodb": {
        "required": ["connector.name", "mongodb.connection-url"],
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
    "oracle": {
        "required": [
            "connector.name",
            "connection-url",
            "connection-user",
            "connection-password",
        ],
        "optional": [],
    },
    "pinot": {
        "required": ["connector.name", "pinot.controller-urls"],
        "optional": [],
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
    "redshift": {
        "required": [
            "connector.name",
            "connection-url",
            "connection-user",
            "connection-password",
        ],
        "optional": [],
    },
    "singlestore": {
        "required": [
            "connector.name",
            "connection-url",
            "connection-user",
            "connection-password",
        ],
        "optional": [],
    },
    "sqlserver": {
        "required": [
            "connector.name",
            "connection-url",
            "connection-user",
            "connection-password",
        ],
        "optional": [],
    },
    "trino_thrift": {
        "required": ["connector.name", "trino.thrift.client.addresses"],
        "optional": [],
    },
}

SYSTEM_CONNECTORS = ["jmx", "memory", "tpcds", "tpch"]
