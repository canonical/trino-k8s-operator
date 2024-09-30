#!/usr/bin/env python3
# Copyright 2023 Canonical Ltd.
# See LICENSE file for licensing details.

"""Literals used by the Trino K8s charm unit tests."""

SERVER_PORT = "8080"

BIGQUERY_SECRET = """\
project-12345: |
    {
      "type": "service_account",
      "project_id": "example-project",
      "private_key_id": "key123",
      "private_key": "-----BEGIN PRIVATE KEY-----\\ndhQfcPA9zu\\n-----END PRIVATE KEY-----\\n",
      "client_email": "test-380@example-project.iam.gserviceaccount.com",
      "client_id": "12345",
      "auth_uri": "https://accounts.google.com/o/oauth2/auth",
      "token_uri": "https://oauth2.googleapis.com/token",
      "auth_provider_x509_cert_url": "https://www.googleapis.com/oauth2/v1/certs",
      "client_x509_cert_url": "https://www.googleapis.com/robot/v1/metadata/x509/test-380.project.iam.gserviceaccount.com",
      "universe_domain": "googleapis.com"
    }
"""  #nosec
POSTGRESQL_REPLICA_SECRET = """\
rw:
  user: trino
  password: pwd1
  suffix: _developer
ro:
  user: trino_ro
  password: pwd2
"""  #nosec
POSTGRESQL_REPLICA_CERT = """\
cert: |
  -----BEGIN CERTIFICATE-----
      CERTIFICATE CONTENT...
  -----END CERTIFICATE-----
"""  #nosec

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
