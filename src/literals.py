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
            "accumlo.password"
        ],
        "optional": [
            "accumlo.zookeeper.metadata.root",
            "accumulo.cardinality.cache.expire.duration"
        ]
    },
    "atop": {
        "required": [
            "connector.name",
            "atop.executable-path"
        ],
        "optional": []
    },
    "bigquery": {
        "required": [
            "connector.name",
            "bigquery.project-id"
        ],
        "optional": []
    },
    "cassandra": {
        "required": [
            "connector.name",
            "cassandra.contact-points",
            "cassandra.load-policy.dc-aware.local-dc"
        ],
        "optional": []
    },
    "clickhouse": {
        "required": [
            "connector.name",
            "connection-url",
            "connection-user",
            "connection-password"
        ],
        "optional": []
    },
    "delta_lake": {
        "required": [
            "connector.name",
            "hive.metastore.uri"
        ],
        "optional": []
    },
    "druid": {
        "required": [
            "connector.name",
            "connection-url"
        ],
        "optional": []
    },
    "elasticsearch": {
        "required": [
            "connector.name",
            "elasticsearch.host",
            "elasticsearch.port",
            "elasticsearch.default-schema-name"
        ],
        "optional": []
    },
    "gsheets": {
        "required": [
            "connector.name",
            "gsheets.credentials-path",
            "gsheets.metadata-sheet-id"
        ],
        "optional": []
    },
    "hive": {
        "required": [
            "connector.name",
            "hive.metastore.uri"
        ],
        "optional": []
    },
    "hudi": {
        "required": [
            "connector.name",
            "hive.metastore.uri"
        ],
        "optional": []
    },
    "ignite": {
        "required": [
            "connector.name",
            "connection-url",
            "connection-user",
            "connection-password"
        ],
        "optional": []
    },
    "jmx": {
        "required": [
            "connector.name",
            "jmx.dump-tables",
            "jmx.dump-period",
            "jmx.max-entries"
        ],
        "optional": []
    },
    "kafka": {
        "required": [
            "connector.name",
            "kafka.table-names",
            "kafka.nodes",
            "kafka.config.resource"
        ],
        "optional": []
    },
    "kinesis": {
        "required": [
            "connector.name",
            "kinesis.access-key",
            "kinesis.secret-key"
        ],
        "optional": []
    },
    "mariadb": {
        "required": [
            "connector.name",
            "connection-url",
            "connection-user",
            "connection-password"
        ],
        "optional": []
    },
    "mongodb": {
        "required": [
            "connector.name",
            "mongodb.connection-url"
        ],
        "optional": []
    },
    "mysql": {
        "required": [
            "connector.name",
            "connection-url",
            "connection-user",
            "connection-password"
        ],
        "optional": []
    },
    "oracle": {
        "required": [
            "connector.name",
            "connection-url",
            "connection-user",
            "connection-password"
        ],
        "optional": []
    },
    "phoenix": {
        "required": [
            "connector.name",
            "phoenix.connection-url",
            "phoenix.config.resources"
        ],
        "optional": []
    },
    "pinot": {
        "required": [
            "connector.name",
            "pinot.controller-urls"
        ],
        "optional": []
    },
    "postgresql": {
        "required": [
            "connector.name",
            "connection-url",
            "connection-user",
            "connection-password"
        ],
        "optional": []
    },
    "prometheus": {
        "required": [
            "connector.name",
            "prometheus.uri",
            "prometheus.query.chunk.size.duration",
            "prometheus.max.query.range.duration",
            "prometheus.cache.ttl",
            "prometheus.bearer.token.file",
            "prometheus.read-timeout"
        ],
        "optional": []
    },
    "redis": {
        "required": [
            "connector.name",
            "redis.table-names",
            "redis.nodes"
        ],
        "optional": []
    },
    "redshift": {
        "required": [
            "connector.name",
            "connection-url",
            "connection-user",
            "connection-password"
        ],
        "optional": []
    },
    "singlestore": {
        "required": [
            "connector.name",
            "connection-url",
            "connection-user",
            "connection-password"
        ],
        "optional": []
    },
    "sqlserver": {
        "required": [
            "connector.name",
            "connection-url",
            "connection-user",
            "connection-password"
        ],
        "optional": []
    },
    "trino_thrift": {
        "required": [
            "connector.name",
            "trino.thrift.client.addresses"
        ],
        "optional": []
    }
}
