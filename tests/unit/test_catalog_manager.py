# Copyright 2023 Canonical Ltd.
# See LICENSE file for licensing details.

"""Unit tests for the pure static catalog and credential renderers."""

import copy
from pathlib import Path
from unittest import TestCase

from catalog_manager import (
    BigqueryCatalog,
    GsheetCatalog,
    HiveCatalog,
    PrometheusCatalog,
    RenderedCatalog,
    password_env_var,
)
from sql_catalog import RedshiftCatalog, SqlCatalog
from tests.unit.helpers import create_test_certificate

CREDENTIAL_DIR = Path("/usr/lib/trino/etc/credentials")
TRUSTSTORE_PATH = Path("/usr/lib/trino/etc/conf/truststore.jks")
AUTH_VALUE = "test-basic-value"


class FakeSecret:
    """Stand-in for the `ops.model.Secret` object returned by `get_secret`."""

    def __init__(self, content):
        """Construct.

        Args:
            content: the mapping returned by `get_content`.
        """
        self._content = content

    def get_content(self, refresh=False):
        """Return the canned secret content.

        Args:
            refresh: unused, present to match the `ops.model.Secret` API.

        Returns:
            The canned secret content mapping.
        """
        return self._content


class FakeModel:
    """Stand-in for `ops.charm.CharmBase.model` exposing only `get_secret`."""

    def __init__(self, secrets):
        """Construct.

        Args:
            secrets: mapping of secret id to `FakeSecret`.
        """
        self._secrets = secrets

    def get_secret(self, id):
        """Return the canned secret for the given id.

        Args:
            id: the secret id.

        Returns:
            The `FakeSecret` registered under that id.
        """
        return self._secrets[id]


class FakeCharm:
    """Minimal charm double exposing only what catalog rendering reads.

    Deliberately has no `unit`/`get_container` attribute: any accidental
    container access during rendering raises `AttributeError` and fails
    the test.
    """

    def __init__(self, secrets=None):
        """Construct.

        Args:
            secrets: mapping of secret id to `FakeSecret`, defaults to empty.
        """
        self.model = FakeModel(secrets or {})
        self.truststore_abs_path = TRUSTSTORE_PATH
        self.credential_abs_path = CREDENTIAL_DIR


class TestBigqueryCatalog(TestCase):
    """Tests for `BigqueryCatalog.render`."""

    def test_render_returns_properties_and_credentials(self):
        """Rendering returns the catalog properties and credential file content."""
        secret = FakeSecret({"service-accounts": 'my-project: \'{"key": "a"}\'\n'})
        charm = FakeCharm(secrets={"secret:1": secret})
        info = {"project": "my-project", "secret-id": "secret:1"}
        backend = {"connector": "bigquery", "config": "bigquery.extra=true\n"}

        instance = BigqueryCatalog(charm, "bq1", info, backend)
        rendered = instance.render()

        self.assertIsInstance(rendered, RenderedCatalog)
        self.assertEqual(set(rendered.properties), {"bq1"})
        properties = rendered.properties["bq1"]
        self.assertIn("connector.name=bigquery", properties)
        self.assertIn("bigquery.project-id=my-project", properties)
        self.assertIn(
            f"bigquery.credentials-file={CREDENTIAL_DIR}/bq1.json",
            properties,
        )
        self.assertIn("bigquery.extra=true", properties)
        self.assertEqual(rendered.credentials, {"bq1.json": '{"key": "a"}'})
        self.assertEqual(rendered.certs, {})

    def test_render_does_not_mutate_inputs(self):
        """Rendering leaves the info and backend mappings unchanged."""
        secret = FakeSecret({"service-accounts": 'my-project: \'{"key": "a"}\'\n'})
        charm = FakeCharm(secrets={"secret:1": secret})
        info = {"project": "my-project", "secret-id": "secret:1"}
        backend = {"connector": "bigquery", "config": ""}
        info_before, backend_before = copy.deepcopy(info), copy.deepcopy(backend)

        BigqueryCatalog(charm, "bq1", info, backend).render()

        self.assertEqual(info, info_before)
        self.assertEqual(backend, backend_before)

    def test_render_emits_no_info_log(self):
        """Rendering never logs the removed per-catalog success message."""
        secret = FakeSecret({"service-accounts": 'my-project: \'{"key": "a"}\'\n'})
        charm = FakeCharm(secrets={"secret:1": secret})
        info = {"project": "my-project", "secret-id": "secret:1"}
        backend = {"connector": "bigquery", "config": ""}

        with self.assertNoLogs("catalog_manager", level="INFO"):
            BigqueryCatalog(charm, "bq1", info, backend).render()

    def test_render_failure_keeps_secret_content_out_of_logs(self):
        """A malformed secret is reported without echoing its content."""
        secret = FakeSecret({"service-accounts": "{super-secret-token: [unterminated"})
        charm = FakeCharm(secrets={"secret:1": secret})
        info = {"project": "my-project", "secret-id": "secret:1"}
        backend = {"connector": "bigquery", "config": ""}

        with self.assertLogs("catalog_manager", level="ERROR") as logs:
            with self.assertRaises(Exception):
                BigqueryCatalog(charm, "bq1", info, backend).render()

        self.assertNotIn("super-secret-token", "\n".join(logs.output))


class TestGsheetCatalog(TestCase):
    """Tests for `GsheetCatalog.render`."""

    def test_render_returns_properties_and_credentials(self):
        """Rendering returns the catalog properties and credential file content."""
        secret = FakeSecret({"service-accounts": 'gsheets-1: \'{"key": "b"}\'\n'})
        charm = FakeCharm(secrets={"secret:2": secret})
        info = {"metasheet-id": "sheet-123", "secret-id": "secret:2"}
        backend = {"connector": "gsheets", "config": ""}

        instance = GsheetCatalog(charm, "gsheets-1", info, backend)
        rendered = instance.render()

        properties = rendered.properties["gsheets-1"]
        self.assertIn("connector.name=gsheets", properties)
        self.assertIn("gsheets.metadata-sheet-id=sheet-123", properties)
        self.assertIn(
            f"gsheets.credentials-path={CREDENTIAL_DIR}/gsheets-1.json",
            properties,
        )
        self.assertEqual(rendered.credentials, {"gsheets-1.json": '{"key": "b"}'})
        self.assertEqual(rendered.certs, {})


class TestPrometheusCatalog(TestCase):
    """Tests for `PrometheusCatalog.render`."""

    def test_render_without_authentication(self):
        """Optional settings render without a secret or credential file."""
        backend = {
            "connector": "prometheus",
            "url": "http://prometheus:9090",
            "config": "prometheus.read-timeout=20s\n",
        }

        rendered = PrometheusCatalog(FakeCharm(), "metrics", {}, backend).render()

        self.assertEqual(
            rendered,
            RenderedCatalog(
                properties={
                    "metrics": (
                        "connector.name=prometheus\n"
                        "prometheus.uri=http://prometheus:9090\n"
                        "prometheus.read-timeout=20s\n"
                    )
                },
                credentials={},
                certs={},
                env={},
            ),
        )

    def test_render_with_basic_authentication(self):
        """Basic authentication comes from a Juju secret, not backend config."""
        secret = FakeSecret({"user": "metrics-reader", "password": AUTH_VALUE})
        charm = FakeCharm(secrets={"secret:1": secret})
        backend = {"connector": "prometheus", "url": "https://prometheus.example.com"}

        rendered = PrometheusCatalog(charm, "metrics", {"secret-id": "secret:1"}, backend).render()

        self.assertIn("prometheus.auth.user=metrics-reader\n", rendered.properties["metrics"])
        self.assertIn(
            "prometheus.auth.password=${ENV:CATALOG_PASS_METRICS}\n",
            rendered.properties["metrics"],
        )
        self.assertNotIn(AUTH_VALUE, rendered.properties["metrics"])
        self.assertNotIn("prometheus.bearer.token.file", rendered.properties["metrics"])
        self.assertEqual(rendered.credentials, {})
        self.assertEqual(rendered.certs, {})
        self.assertEqual(rendered.env, {"CATALOG_PASS_METRICS": AUTH_VALUE})

    def test_render_with_bearer_authentication(self):
        """The catalog references a managed file instead of embedding the token."""
        secret = FakeSecret({"bearer-token": "test-bearer-value"})
        charm = FakeCharm(secrets={"secret:1": secret})
        backend = {"connector": "prometheus", "url": "https://prometheus.example.com"}

        rendered = PrometheusCatalog(charm, "metrics", {"secret-id": "secret:1"}, backend).render()

        self.assertIn(
            f"prometheus.bearer.token.file={CREDENTIAL_DIR}/metrics.token\n",
            rendered.properties["metrics"],
        )
        self.assertNotIn("test-bearer-value", rendered.properties["metrics"])
        self.assertNotIn("prometheus.auth.user", rendered.properties["metrics"])
        self.assertEqual(rendered.credentials, {"metrics.token": "test-bearer-value"})
        self.assertEqual(rendered.certs, {})
        self.assertEqual(rendered.env, {})

    def test_render_with_custom_ca(self):
        """An unauthenticated HTTPS catalog can supply a CA through its secret."""
        certificate = create_test_certificate()
        charm = FakeCharm(secrets={"secret:1": FakeSecret({"tls-ca": certificate})})
        backend = {"connector": "prometheus", "url": "https://prometheus.example.com"}

        rendered = PrometheusCatalog(charm, "metrics", {"secret-id": "secret:1"}, backend).render()

        self.assertEqual(rendered.certs, {"prometheus-ca-metrics": certificate})
        self.assertEqual(rendered.credentials, {})
        self.assertNotIn("prometheus.auth.", rendered.properties["metrics"])

    def test_invalid_authentication_is_rejected(self):
        """Secrets reject incomplete authentication and empty TLS CA values."""
        for content in (
            {"user": "reader"},
            {"user": "reader", "password": AUTH_VALUE, "bearer-token": "test-value"},
            {"tls-ca": ""},
        ):
            with self.subTest(content=content):
                charm = FakeCharm(secrets={"secret:1": FakeSecret(content)})
                with self.assertRaises(ValueError):
                    PrometheusCatalog(
                        charm,
                        "metrics",
                        {"secret-id": "secret:1"},
                        {"connector": "prometheus", "url": "http://prometheus:9090"},
                    ).render()


class TestHiveCatalog(TestCase):
    """Tests for `HiveCatalog.render`."""

    def test_render_returns_properties_with_no_credentials(self):
        """Rendering a Hive catalog produces no credential file or certs."""
        charm = FakeCharm()
        backend = {"connector": "hive", "url": "thrift://metastore:9083"}

        instance = HiveCatalog(charm, "hive1", {}, backend)
        rendered = instance.render()

        properties = rendered.properties["hive1"]
        self.assertIn("connector.name=hive", properties)
        self.assertIn("hive.metastore.uri=thrift://metastore:9083", properties)
        self.assertEqual(rendered.credentials, {})
        self.assertEqual(rendered.certs, {})


class TestSqlCatalog(TestCase):
    """Tests for `SqlCatalog.render`, covering replica suffix expansion."""

    def test_render_expands_replicas_and_collects_certs(self):
        """Each replica renders its own catalog file, suffixed as configured."""
        replicas = (
            "rw:\n"
            "  user: trino\n"
            "  password: pwd1\n"
            "  suffix: _developer\n"
            "ro:\n"
            "  user: trino_ro\n"
            "  password: pwd2\n"
        )
        cert = "cert: |\n  -----BEGIN CERTIFICATE-----\n  fake\n  -----END CERTIFICATE-----\n"
        secret = FakeSecret({"replicas": replicas, "cert": cert})
        charm = FakeCharm(secrets={"secret:3": secret})
        info = {"database": "mydb", "secret-id": "secret:3"}
        backend = {"connector": "postgresql", "url": "jdbc:postgresql://host:5432", "config": ""}

        instance = SqlCatalog(charm, "pg1", info, backend)
        rendered = instance.render()

        self.assertEqual(set(rendered.properties), {"pg1_developer", "pg1"})
        developer_properties = rendered.properties["pg1_developer"]
        self.assertIn("connection-url=jdbc:postgresql://host:5432/mydb", developer_properties)
        self.assertIn("connection-user=trino", developer_properties)
        self.assertIn(
            "connection-password=${ENV:CATALOG_PASS_PG1__DEVELOPER}\n", developer_properties
        )
        self.assertIn("connection-password=${ENV:CATALOG_PASS_PG1}\n", rendered.properties["pg1"])
        self.assertEqual(
            rendered.env,
            {"CATALOG_PASS_PG1__DEVELOPER": "pwd1", "CATALOG_PASS_PG1": "pwd2"},  # nosec B105
        )
        self.assertEqual(rendered.credentials, {})
        self.assertEqual(
            rendered.certs,
            {"cert": "-----BEGIN CERTIFICATE-----\nfake\n-----END CERTIFICATE-----\n"},
        )

    def test_render_keeps_truststore_password_out_of_properties(self):
        """`{SSL_PWD}` becomes a reference to the truststore password variable."""
        replicas = "ro:\n  user: trino_ro\n  password: pwd2\n"
        secret = FakeSecret({"replicas": replicas, "cert": ""})
        charm = FakeCharm(secrets={"secret:4": secret})
        info = {"database": "mydb", "secret-id": "secret:4"}
        backend = {
            "connector": "postgresql",
            "url": "jdbc:postgresql://host:5432",
            "params": "ssl=true&sslrootcert={SSL_PATH}&sslrootcertpassword={SSL_PWD}",
            "config": "",
        }

        rendered = SqlCatalog(charm, "pg1", info, backend).render()

        self.assertIn(
            "connection-url=jdbc:postgresql://host:5432/mydb?ssl=true"
            f"&sslrootcert={TRUSTSTORE_PATH}&sslrootcertpassword=${{ENV:JAVA_TRUSTSTORE_PWD}}\n",
            rendered.properties["pg1"],
        )

    def test_render_does_not_mutate_secret_content(self):
        """Rendering leaves the parsed secret content unchanged."""
        replicas = "ro:\n  user: trino_ro\n  password: pwd2\n"
        secret = FakeSecret({"replicas": replicas, "cert": ""})
        charm = FakeCharm(secrets={"secret:4": secret})
        info = {"database": "mydb", "secret-id": "secret:4"}
        backend = {"connector": "postgresql", "url": "jdbc:postgresql://host:5432", "config": ""}
        info_before, backend_before = copy.deepcopy(info), copy.deepcopy(backend)

        SqlCatalog(charm, "pg1", info, backend).render()

        self.assertEqual(info, info_before)
        self.assertEqual(backend, backend_before)


class TestRedshiftCatalog(TestCase):
    """Tests for `RedshiftCatalog.render`, which drops the database suffix."""

    def test_render_uses_backend_url_without_database(self):
        """The Redshift connection URL never appends a database segment."""
        replicas = "ro:\n  user: trino_ro\n  password: pwd4\n"
        secret = FakeSecret({"replicas": replicas, "cert": ""})
        charm = FakeCharm(secrets={"secret:5": secret})
        info = {"secret-id": "secret:5"}
        backend = {"connector": "redshift", "url": "jdbc:redshift://host:5439/db", "config": ""}

        instance = RedshiftCatalog(charm, "redshift", info, backend)
        rendered = instance.render()

        properties = rendered.properties["redshift"]
        self.assertIn("connection-url=jdbc:redshift://host:5439/db", properties)
        self.assertIn("connection-password=${ENV:CATALOG_PASS_REDSHIFT}\n", properties)
        self.assertEqual(rendered.env, {"CATALOG_PASS_REDSHIFT": "pwd4"})  # nosec B105
        self.assertEqual(rendered.credentials, {})


class TestPasswordEnvVar(TestCase):
    """Tests for `password_env_var`."""

    def test_names_are_distinct_and_valid(self):
        """Names that differ only in separators get distinct, valid variable names."""
        names = ["pg", "pg_ro", "pg-ro", "pg__ro", "pg_5Fro", "pg.ro", "PG", "pg_2Dro", "pgé"]

        variables = [password_env_var(name) for name in names]

        self.assertEqual(len(set(variables)), len(names))
        for variable in variables:
            self.assertRegex(variable, r"^CATALOG_PASS_[A-Z0-9_]+$")

    def test_common_names_stay_readable(self):
        """Lowercase names keep their letters, digits and underscores."""
        self.assertEqual(password_env_var("pgdata_developer"), "CATALOG_PASS_PGDATA__DEVELOPER")
        self.assertEqual(password_env_var("postgresql-1"), "CATALOG_PASS_POSTGRESQL_2D1")
