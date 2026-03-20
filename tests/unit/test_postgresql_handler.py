# Copyright 2023 Canonical Ltd.
# See LICENSE file for licensing details.

"""Unit tests for PostgreSQL relation handler pure/static methods."""

# pylint:disable=protected-access

from unittest import TestCase, mock

from relations.postgresql import (
    DYNAMIC_CATALOG_MARKER,
    PostgresqlRelationHandler,
    _env_var_name,
)


class TestEnvVarName(TestCase):
    """Tests for the _env_var_name helper."""

    def test_simple_name(self):
        self.assertEqual(_env_var_name("mydb"), "PG_PASS_MYDB")

    def test_hyphen_replaced(self):
        self.assertEqual(_env_var_name("my-db"), "PG_PASS_MY_DB")

    def test_already_uppercase(self):
        self.assertEqual(_env_var_name("MYDB"), "PG_PASS_MYDB")

    def test_mixed_case_with_hyphens(self):
        self.assertEqual(_env_var_name("My-Cool-Db"), "PG_PASS_MY_COOL_DB")


class TestParseProperties(TestCase):
    """Tests for _parse_properties static method."""

    def test_simple(self):
        raw = "key=value\nfoo=bar"
        result = PostgresqlRelationHandler._parse_properties(raw)
        self.assertEqual(result, {"key": "value", "foo": "bar"})

    def test_escaped_colon_and_equals(self):
        # Trino escapes : and = in property keys/values
        raw = r"connection\=url\:jdbc\:postgresql\://host\:5432/db"
        # After unescaping: connection=url:jdbc:postgresql://host:5432/db
        # Split on first =: key="connection", value="url:jdbc:postgresql://host:5432/db"
        result = PostgresqlRelationHandler._parse_properties(raw)
        self.assertEqual(
            result,
            {"connection": "url:jdbc:postgresql://host:5432/db"},
        )

    def test_comments_and_blanks_skipped(self):
        raw = "# comment\n\nkey=value\n  \n# another"
        result = PostgresqlRelationHandler._parse_properties(raw)
        self.assertEqual(result, {"key": "value"})

    def test_value_with_equals(self):
        raw = "url=jdbc:postgresql://host:5432/db?a=1&b=2"
        result = PostgresqlRelationHandler._parse_properties(raw)
        self.assertEqual(
            result, {"url": "jdbc:postgresql://host:5432/db?a=1&b=2"}
        )

    def test_whitespace_trimmed(self):
        raw = "  key  =  value  "
        result = PostgresqlRelationHandler._parse_properties(raw)
        self.assertEqual(result, {"key": "value"})

    def test_empty_string(self):
        result = PostgresqlRelationHandler._parse_properties("")
        self.assertEqual(result, {})


class TestHashProperties(TestCase):
    """Tests for _hash_properties static method."""

    def test_deterministic(self):
        props = {"b": "2", "a": "1"}
        h1 = PostgresqlRelationHandler._hash_properties(props)
        h2 = PostgresqlRelationHandler._hash_properties(props)
        self.assertEqual(h1, h2)

    def test_order_independent(self):
        h1 = PostgresqlRelationHandler._hash_properties({"a": "1", "b": "2"})
        h2 = PostgresqlRelationHandler._hash_properties({"b": "2", "a": "1"})
        self.assertEqual(h1, h2)

    def test_different_values_differ(self):
        h1 = PostgresqlRelationHandler._hash_properties({"a": "1"})
        h2 = PostgresqlRelationHandler._hash_properties({"a": "2"})
        self.assertNotEqual(h1, h2)


class TestBuildCatalogSql(TestCase):
    """Tests for _build_catalog_sql static method."""

    def test_simple(self):
        sql = PostgresqlRelationHandler._build_catalog_sql(
            "mycat", {"connection-url": "jdbc:postgresql://host/db"}
        )
        self.assertIn('CREATE CATALOG "mycat" USING postgresql', sql)
        self.assertIn("\"connection-url\" = 'jdbc:postgresql://host/db'", sql)

    def test_multiple_properties(self):
        props = {
            "connection-url": "jdbc:postgresql://host/db",
            "connection-user": "admin",
        }
        sql = PostgresqlRelationHandler._build_catalog_sql("cat", props)
        self.assertIn('"connection-url"', sql)
        self.assertIn('"connection-user"', sql)

    def test_marker_property_included(self):
        props = {
            "connection-url": "jdbc:postgresql://host/db",
            "query.comment-format": DYNAMIC_CATALOG_MARKER,
        }
        sql = PostgresqlRelationHandler._build_catalog_sql("cat", props)
        self.assertIn(f"'{DYNAMIC_CATALOG_MARKER}'", sql)


class TestBuildJdbcUrl(TestCase):
    """Tests for _build_jdbc_url (requires minimal mocking)."""

    def _make_pg(self, all_endpoints, tls=False, tls_ca=None):
        pg = mock.MagicMock()
        pg.all_endpoints = all_endpoints
        pg.tls = tls
        pg.tls_ca = tls_ca
        return pg

    def _make_handler(self):
        handler = mock.MagicMock()
        handler._build_jdbc_url = (
            PostgresqlRelationHandler._build_jdbc_url.__get__(handler)
        )
        handler.charm.truststore_abs_path = "/path/to/truststore"
        return handler

    def test_no_tls(self):
        handler = self._make_handler()
        pg = self._make_pg("host1,host2:5432")
        url = handler._build_jdbc_url(pg, "mydb", 1, "preferSecondary")
        self.assertEqual(
            url,
            "jdbc:postgresql://host1,host2:5432/mydb"
            "?targetServerType=preferSecondary&ssl=false",
        )

    def test_tls_without_ca(self):
        handler = self._make_handler()
        pg = self._make_pg("host:5432", tls=True)
        url = handler._build_jdbc_url(pg, "mydb", 1, "primary")
        self.assertIn("ssl=true", url)
        self.assertIn("sslmode=require", url)
        self.assertNotIn("sslrootcert", url)
        handler._import_tls_cert.assert_not_called()

    def test_tls_with_ca(self):
        handler = self._make_handler()
        pg = self._make_pg("host:5432", tls=True, tls_ca="BEGIN CERT...")
        url = handler._build_jdbc_url(pg, "mydb", 1, "primary")
        self.assertIn("ssl=true", url)
        self.assertIn("sslrootcert=/path/to/truststore", url)
        handler._import_tls_cert.assert_called_once_with(1, "BEGIN CERT...")
