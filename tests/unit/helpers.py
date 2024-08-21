#!/usr/bin/env python3
# Copyright 2023 Canonical Ltd.
# See LICENSE file for licensing details.

"""Literals used by the Trino K8s charm unit tests."""


SERVER_PORT = "8080"
TEST_CATALOG_CONFIG = """\
catalogs:
    example-db: |
        connector.name=postgresql
        connection-url=jdbc:postgresql://host.com:5432/database?ssl=true&sslmode=require&sslrootcert={SSL_PATH}&sslrootcertpassword={SSL_PWD}
        connection-user=testing
        connection-password=pd3h@!}93*hdu
certs:
    example-cert: |
        -----BEGIN CERTIFICATE-----
        CERTIFICATE CONTENT...
        -----END CERTIFICATE-----
"""
UPDATED_CATALOG_CONFIG = """\
catalogs:
    updated-db: |
        connector.name=postgresql
        connection-url=jdbc:postgresql://host.com:5432/database?ssl=true&sslmode=require&sslrootcert={SSL_PATH}&sslrootcertpassword={SSL_PWD}
        connection-user=testing
        connection-password=pd3h@!}93*hdu
certs:
    updated-cert: |
        -----BEGIN CERTIFICATE-----
        CERTIFICATE CONTENT...
        -----END CERTIFICATE-----
"""
TEST_CATALOG_PATH = "/usr/lib/trino/etc/catalog/example-db.properties"
UPDATED_CATALOG_PATH = "/usr/lib/trino/etc/catalog/updated-db.properties"
RANGER_PROPERTIES_PATH = "/usr/lib/ranger/install.properties"
POLICY_MGR_URL = "http://ranger-k8s:6080"

RANGER_LIB = "/usr/lib/ranger"

JAVA_HOME = "/usr/lib/jvm/java-21-openjdk-amd64"
TEST_USERS = """\
    example_user: ubuntu123
    another_user: ubuntu345
"""

DEFAULT_JVM_OPTIONS = [
    "-XX:+ExplicitGCInvokesConcurrent",
    "-XX:+ExitOnOutOfMemoryError",
    "-XX:+HeapDumpOnOutOfMemoryError",
    "-XX:-OmitStackTraceInFastThrow",
    "-XX:PerMethodRecompilationCutoff=10000",
    "-XX:PerBytecodeRecompilationCutoff=10000",
    "-Djdk.attach.allowAttachSelf=true",
    "-Djdk.nio.maxCachedBufferSize=2000000",
    "-Dfile.encoding=UTF-8",
    "-XX:+UnlockDiagnosticVMOptions",
    "-XX:+EnableDynamicAgentLoading",
]

DEFAULT_JVM_STRING = " ".join(DEFAULT_JVM_OPTIONS)

CUSTOMIZED_JVM_STRING = (
    "-XX:+ExplicitGCInvokesConcurrent -XX:+ExitOnOutOfMemoryError"
)
