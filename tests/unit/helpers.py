#!/usr/bin/env python3
# Copyright 2023 Canonical Ltd.
# See LICENSE file for licensing details.

"""Literals used by the Trino K8s charm unit tests."""


SERVER_PORT = "8080"
TEST_CATALOG_CONFIG = """\
catalogs:
  example:
    backend: dwh
    database: example
  updated-db:
    backend: dwh
    database: updated-db
backends:
  dwh:
    connector: postgresql
    url: jdbc:postgresql://example.com:5432
    params: ssl=true&sslmode=require&sslrootcert={SSL_PATH}&sslrootcertpassword={SSL_PWD}
    replicas:
      rw:
        user: trino
        password: pwd1
        suffix: _developer
      ro:
        user: trino_ro
        password: pwd2
    config: |
      case-insensitive-name-matching=true
      decimal-mapping=allow_overflow
      decimal-rounding-mode=HALF_UP
certs:
    example-cert: |
        -----BEGIN CERTIFICATE-----
        CERTIFICATE CONTENT...
        -----END CERTIFICATE-----
"""
INCORRECT_CATALOG_CONFIG = """\
catalogs:
  example:
    database: example
  updated-db:
    backend: dwh
    database: updated-db
backends:
  dwh:
    connector: postgresql
    url: jdbc:postgresql://example.com:5432
    params: ssl=true&sslmode=require&sslrootcert={SSL_PATH}&sslrootcertpassword={SSL_PWD}
    replicas:
      rw:
        user: trino
        password: pwd1
        suffix: _developer
      ro:
        user: trino_ro
        password: pwd2
    config: |
      case-insensitive-name-matching=true
      decimal-mapping=allow_overflow
      decimal-rounding-mode=HALF_UP
certs:
    example-cert: |
        -----BEGIN CERTIFICATE-----
        CERTIFICATE CONTENT...
        -----END CERTIFICATE-----
"""

UPDATED_CATALOG_CONFIG = """\
catalogs:
  updated:
    backend: dwh
    database: updated-db
backends:
  dwh:
    connector: postgresql
    url: jdbc:postgresql://updated.com:5432
    params: ssl=true&sslmode=require&sslrootcert={SSL_PATH}&sslrootcertpassword={SSL_PWD}
    replicas:
      rw:
        user: trino
        password: pwd1
        suffix: _developer
      ro:
        user: trino_ro
        password: pwd2
    config: |
      case-insensitive-name-matching=true
      decimal-mapping=allow_overflow
      decimal-rounding-mode=HALF_UP
certs:
    example-cert: |
        -----BEGIN CERTIFICATE-----
        CERTIFICATE CONTENT...
        -----END CERTIFICATE-----
"""
TEST_CATALOG_PATH = "/usr/lib/trino/etc/catalog/example.properties"
UPDATED_CATALOG_PATH = "/usr/lib/trino/etc/catalog/updated.properties"
RANGER_PROPERTIES_PATH = "/usr/lib/ranger/install.properties"
POLICY_MGR_URL = "http://ranger-k8s:6080"

RANGER_LIB = "/usr/lib/ranger"

JAVA_HOME = "/usr/lib/jvm/java-21-openjdk-amd64"
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
