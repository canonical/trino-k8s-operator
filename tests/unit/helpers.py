#!/usr/bin/env python3
# Copyright 2023 Canonical Ltd.
# See LICENSE file for licensing details.

"""Literals used by the Trino K8s charm unit tests."""

SERVER_PORT = "8080"

BIGQUERY_SECRET = """\
project-12345: |
  base64encodedserviceaccountcredentials
"""
POSTGRESQL_REPLICA_SECRET = """\
rw:
  user: trino
  password: pwd1
  suffix: _developer
ro:
  user: trino_ro
  password: pwd2
"""
POSTGRESQL_REPLICA_CERT = """\
cert: |
  -----BEGIN CERTIFICATE-----
      CERTIFICATE CONTENT...
  -----END CERTIFICATE-----
"""

POSTGRESQL_1_CATALOG_PATH = (
    "/usr/lib/trino/etc/catalog/postgresql-1.properties"
)
POSTGRESQL_2_CATALOG_PATH = (
    "/usr/lib/trino/etc/catalog/postgresql-2.properties"
)
BIGQUERY_CATALOG_PATH = "/usr/lib/trino/etc/catalog/bigquery.properties"
RANGER_PROPERTIES_PATH = "/usr/lib/ranger/install.properties"
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
        "-Xxs10G",
    ]
)
