# Copyright 2023 Canonical Ltd.
# See LICENSE file for licensing details.

"""Unit tests for PostgreSQL catalog relation handler pure/static methods."""

# pylint:disable=protected-access

from unittest import TestCase, mock

import yaml
from pydantic import ValidationError

from config import CharmConfig
from relations.postgresql_catalog import (
    DYNAMIC_CATALOG_MARKER,
    PostgresqlCatalogRelationHandler,
    _env_var_name,
)


class TestEnvVarName(TestCase):
    """Tests for the _env_var_name helper."""

    def test_simple_name(self):
        """Verify simple database name is uppercased with prefix."""
        self.assertEqual(_env_var_name("mydb"), "PG_PASS_MYDB")

    def test_hyphen_replaced(self):
        """Verify hyphens are replaced with underscores."""
        self.assertEqual(_env_var_name("my-db"), "PG_PASS_MY_DB")

    def test_already_uppercase(self):
        """Verify already uppercase name is unchanged."""
        self.assertEqual(_env_var_name("MYDB"), "PG_PASS_MYDB")

    def test_mixed_case_with_hyphens(self):
        """Verify mixed case with hyphens is normalized."""
        self.assertEqual(_env_var_name("My-Cool-Db"), "PG_PASS_MY_COOL_DB")


class TestParseProperties(TestCase):
    """Tests for _parse_properties static method."""

    def test_simple(self):
        """Verify simple key=value parsing."""
        raw = "key=value\nfoo=bar"
        result = PostgresqlCatalogRelationHandler._parse_properties(raw)
        self.assertEqual(result, {"key": "value", "foo": "bar"})

    def test_escaped_colon_and_equals(self):
        """Verify Trino-style escaped colons and equals are unescaped."""
        # Trino escapes : and = in property keys/values
        raw = r"connection\=url\:jdbc\:postgresql\://host\:5432/db"
        # After unescaping: connection=url:jdbc:postgresql://host:5432/db
        # Split on first =: key="connection", value="url:jdbc:postgresql://host:5432/db"
        result = PostgresqlCatalogRelationHandler._parse_properties(raw)
        self.assertEqual(
            result,
            {"connection": "url:jdbc:postgresql://host:5432/db"},
        )

    def test_comments_and_blanks_skipped(self):
        """Verify comments and blank lines are ignored."""
        raw = "# comment\n\nkey=value\n  \n# another"
        result = PostgresqlCatalogRelationHandler._parse_properties(raw)
        self.assertEqual(result, {"key": "value"})

    def test_value_with_equals(self):
        """Verify values containing equals signs are preserved."""
        raw = "url=jdbc:postgresql://host:5432/db?a=1&b=2"
        result = PostgresqlCatalogRelationHandler._parse_properties(raw)
        self.assertEqual(result, {"url": "jdbc:postgresql://host:5432/db?a=1&b=2"})

    def test_whitespace_trimmed(self):
        """Verify surrounding whitespace is trimmed."""
        raw = "  key  =  value  "
        result = PostgresqlCatalogRelationHandler._parse_properties(raw)
        self.assertEqual(result, {"key": "value"})

    def test_empty_string(self):
        """Verify empty input returns empty dict."""
        result = PostgresqlCatalogRelationHandler._parse_properties("")
        self.assertEqual(result, {})


class TestHashProperties(TestCase):
    """Tests for _hash_properties static method."""

    def test_deterministic(self):
        """Verify same input produces same hash."""
        props = {"b": "2", "a": "1"}
        h1 = PostgresqlCatalogRelationHandler._hash_properties(props)
        h2 = PostgresqlCatalogRelationHandler._hash_properties(props)
        self.assertEqual(h1, h2)

    def test_order_independent(self):
        """Verify key insertion order does not affect hash."""
        h1 = PostgresqlCatalogRelationHandler._hash_properties({"a": "1", "b": "2"})
        h2 = PostgresqlCatalogRelationHandler._hash_properties({"b": "2", "a": "1"})
        self.assertEqual(h1, h2)

    def test_different_values_differ(self):
        """Verify different values produce different hashes."""
        h1 = PostgresqlCatalogRelationHandler._hash_properties({"a": "1"})
        h2 = PostgresqlCatalogRelationHandler._hash_properties({"a": "2"})
        self.assertNotEqual(h1, h2)


class TestBuildCatalogSql(TestCase):
    """Tests for _build_catalog_sql static method."""

    def test_simple(self):
        """Verify basic CREATE CATALOG SQL generation."""
        sql = PostgresqlCatalogRelationHandler._build_catalog_sql(
            "mycat", {"connection-url": "jdbc:postgresql://host/db"}
        )
        self.assertIn('CREATE CATALOG "mycat" USING postgresql', sql)
        self.assertIn("\"connection-url\" = 'jdbc:postgresql://host/db'", sql)

    def test_multiple_properties(self):
        """Verify multiple properties are included in SQL."""
        props = {
            "connection-url": "jdbc:postgresql://host/db",
            "connection-user": "admin",
        }
        sql = PostgresqlCatalogRelationHandler._build_catalog_sql("cat", props)
        self.assertIn('"connection-url"', sql)
        self.assertIn('"connection-user"', sql)

    def test_marker_property_included(self):
        """Verify dynamic catalog marker is included in SQL."""
        props = {
            "connection-url": "jdbc:postgresql://host/db",
            "query.comment-format": DYNAMIC_CATALOG_MARKER,
        }
        sql = PostgresqlCatalogRelationHandler._build_catalog_sql("cat", props)
        self.assertIn(f"'{DYNAMIC_CATALOG_MARKER}'", sql)


class TestBuildJdbcUrl(TestCase):
    """Tests for _build_jdbc_url (requires minimal mocking)."""

    def _make_pg(self, all_endpoints, tls=False, tls_ca=None):
        """Create a mock PostgresqlRelationModel."""
        pg = mock.MagicMock()
        pg.all_endpoints = all_endpoints
        pg.tls = tls
        pg.tls_ca = tls_ca
        return pg

    def _make_handler(self):
        """Create a mock PostgresqlCatalogRelationHandler."""
        handler = mock.MagicMock()
        handler._build_jdbc_url = PostgresqlCatalogRelationHandler._build_jdbc_url.__get__(handler)
        handler.charm.truststore_abs_path = "/path/to/truststore"
        return handler

    def test_no_tls(self):
        """Verify JDBC URL without TLS includes ssl=false."""
        handler = self._make_handler()
        pg = self._make_pg("host1,host2:5432")
        url = handler._build_jdbc_url(pg, "mydb", 1, "preferSecondary")
        self.assertEqual(
            url,
            "jdbc:postgresql://host1,host2:5432/mydb?targetServerType=preferSecondary&ssl=false",
        )

    def test_tls_without_ca(self):
        """Verify TLS without CA cert does not set sslrootcert."""
        handler = self._make_handler()
        pg = self._make_pg("host:5432", tls=True)
        url = handler._build_jdbc_url(pg, "mydb", 1, "primary")
        self.assertIn("ssl=true", url)
        self.assertIn("sslmode=require", url)
        self.assertNotIn("sslrootcert", url)
        handler._import_tls_cert.assert_not_called()

    def test_tls_with_ca(self):
        """Verify TLS with CA cert imports cert and sets sslrootcert."""
        handler = self._make_handler()
        pg = self._make_pg("host:5432", tls=True, tls_ca="BEGIN CERT...")
        url = handler._build_jdbc_url(pg, "mydb", 1, "primary")
        self.assertIn("ssl=true", url)
        self.assertIn("sslrootcert=/path/to/truststore", url)
        handler._import_tls_cert.assert_called_once_with(1, "BEGIN CERT...")


def _pg_yaml(entries: dict) -> str:
    """Serialise a postgresql-catalog-config dict to a YAML string."""
    return yaml.dump(entries)


def _static_catalog_config(catalog_names: list) -> str:
    """Build a minimal valid catalog-config YAML string with the given catalog names."""
    catalogs = {name: {"backend": "pg"} for name in catalog_names}
    return yaml.dump({"catalogs": catalogs, "backends": {"pg": {"connector": "postgresql"}}})


def _make_config(pg_entries: dict, static_catalog_names: list | None = None) -> CharmConfig:
    """Instantiate CharmConfig with the given postgresql-catalog-config.

    Raises ValidationError if config is invalid.
    """
    kwargs = {"postgresql_catalog_config": _pg_yaml(pg_entries)}
    if static_catalog_names:
        kwargs["catalog_config"] = _static_catalog_config(static_catalog_names)
    return CharmConfig(**kwargs)


class TestPostgresqlCatalogConfigValidation(TestCase):
    """Tests for CharmConfig postgresql-catalog-config validation."""

    def test_no_conflicts(self):
        """Verify no error when all catalog names are unique."""
        _make_config(
            {
                "pg-app-a": {
                    "database_prefix": "db_a*",
                    "ro_catalog_name": "cat_a_ro",
                    "rw_catalog_name": "cat_a_rw",
                },
                "pg-app-b": {
                    "database_prefix": "db_b*",
                    "ro_catalog_name": "cat_b_ro",
                },
            }
        )  # should not raise

    def test_rw_only_no_conflicts(self):
        """Verify no error when entries only have rw_catalog_name."""
        _make_config(
            {
                "pg-app-a": {"database_prefix": "db_a*", "rw_catalog_name": "cat_a_rw"},
                "pg-app-b": {"database_prefix": "db_b*", "rw_catalog_name": "cat_b_rw"},
            }
        )  # should not raise

    def test_invalid_yaml_rejected(self):
        """Verify non-YAML postgresql-catalog-config is rejected."""
        with self.assertRaises(ValidationError) as ctx:
            CharmConfig(postgresql_catalog_config=": bad: yaml: [")
        self.assertIn("postgresql-catalog-config", str(ctx.exception))

    def test_not_a_mapping_rejected(self):
        """Verify non-mapping postgresql-catalog-config is rejected."""
        with self.assertRaises(ValidationError) as ctx:
            CharmConfig(postgresql_catalog_config="- list\n- item")
        self.assertIn("postgresql-catalog-config", str(ctx.exception))

    def test_entry_not_a_mapping_rejected(self):
        """Verify entry that is not a mapping is rejected."""
        with self.assertRaises(ValidationError) as ctx:
            CharmConfig(postgresql_catalog_config="pg-app: just-a-string")
        self.assertIn("pg-app", str(ctx.exception))

    def test_missing_database_prefix_rejected(self):
        """Verify missing database_prefix is rejected."""
        with self.assertRaises(ValidationError) as ctx:
            _make_config({"pg-app": {"ro_catalog_name": "cat"}})
        self.assertIn("database_prefix", str(ctx.exception))

    def test_database_prefix_without_star_rejected(self):
        """Verify database_prefix not ending with '*' is rejected."""
        with self.assertRaises(ValidationError) as ctx:
            _make_config({"pg-app": {"database_prefix": "mydb", "ro_catalog_name": "cat"}})
        self.assertIn("database_prefix", str(ctx.exception))

    def test_no_catalog_name_rejected(self):
        """Verify entry with neither ro_catalog_name nor rw_catalog_name is rejected."""
        with self.assertRaises(ValidationError) as ctx:
            _make_config({"pg-app": {"database_prefix": "db*"}})
        self.assertIn("ro_catalog_name", str(ctx.exception))

    def test_duplicate_ro_names(self):
        """Verify error when two entries share the same ro_catalog_name."""
        with self.assertRaises(ValidationError) as ctx:
            _make_config(
                {
                    "pg-app-a": {"database_prefix": "db_a*", "ro_catalog_name": "shared_name"},
                    "pg-app-b": {"database_prefix": "db_b*", "ro_catalog_name": "shared_name"},
                }
            )
        self.assertIn("Duplicate", str(ctx.exception))
        self.assertIn("shared_name", str(ctx.exception))

    def test_duplicate_rw_names(self):
        """Verify error when two entries share the same rw_catalog_name."""
        with self.assertRaises(ValidationError) as ctx:
            _make_config(
                {
                    "pg-app-a": {"database_prefix": "db_a*", "rw_catalog_name": "shared_rw"},
                    "pg-app-b": {"database_prefix": "db_b*", "rw_catalog_name": "shared_rw"},
                }
            )
        self.assertIn("Duplicate", str(ctx.exception))
        self.assertIn("shared_rw", str(ctx.exception))

    def test_ro_clashes_with_rw(self):
        """Verify error when ro_catalog_name matches another entry's rw_catalog_name."""
        with self.assertRaises(ValidationError) as ctx:
            _make_config(
                {
                    "pg-app-a": {"database_prefix": "db_a*", "ro_catalog_name": "clash"},
                    "pg-app-b": {
                        "database_prefix": "db_b*",
                        "ro_catalog_name": "unique",
                        "rw_catalog_name": "clash",
                    },
                }
            )
        self.assertIn("clash", str(ctx.exception))

    def test_clashes_with_static_catalog(self):
        """Verify error when a PG ro_catalog_name matches a static catalog name."""
        with self.assertRaises(ValidationError) as ctx:
            _make_config(
                {"pg-app": {"database_prefix": "db*", "ro_catalog_name": "static_cat"}},
                static_catalog_names=["static_cat"],
            )
        self.assertIn("clashes with catalog-config", str(ctx.exception))

    def test_rw_clashes_with_static_catalog(self):
        """Verify error when an rw_catalog_name matches a static catalog name."""
        with self.assertRaises(ValidationError) as ctx:
            _make_config(
                {
                    "pg-app": {
                        "database_prefix": "db*",
                        "ro_catalog_name": "unique_ro",
                        "rw_catalog_name": "static_cat",
                    }
                },
                static_catalog_names=["static_cat"],
            )
        self.assertIn("clashes with catalog-config", str(ctx.exception))
