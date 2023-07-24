#!/usr/bin/env python3
# Copyright 2023 Canonical Ltd.
# See LICENSE file for licensing details.

"""Literals used by the Trino K8s charm."""

RANGER_PLUGIN_PATH = "/root/ranger-2.3.0-trino-plugin.tar.gz"
INSTALL_PROPERTIES_PATH = "/root/install.properties"

TLS_RELATION = "certificates"
CONF_PATH = "/etc/trino/conf"
CATALOG_PATH = "/etc/trino/catalog"
CONFIG_JINJA = "config.jinja"
CONFIG_PATH = "/etc/trino/config.properties"
LOG_PATH = "/etc/trino/log.properties"
LOG_JINJA = "logging.jinja"
PASSWORD_DB_PATH = "/etc/trino/password.db"
AUTHENTICATOR_PATH = "/etc/trino/password-authenticator.properties"
AUTHENTICATOR_PROPERTIES = """password-authenticator.name=file
file.password-file=/etc/trino/password.db
file.refresh-period=1m
file.auth-token-cache.max-size=1000"""

TRINO_PORTS = {
    "HTTPS": 8443,
    "HTTP": 8080,
}

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
