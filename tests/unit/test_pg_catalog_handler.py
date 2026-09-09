# Copyright 2023 Canonical Ltd.
# See LICENSE file for licensing details.

"""Unit tests for PostgreSQL catalog relation handler pure/static methods."""

# pylint:disable=protected-access

from types import SimpleNamespace
from unittest import TestCase, mock

import requests
import yaml
from pydantic import ValidationError

from config import CharmConfig
from literals import POSTGRESQL_RELATION_NAME
from relations.postgresql_catalog import (
    DYNAMIC_CATALOG_MARKER,
    CatalogAlreadyExistsError,
    CatalogSQLError,
    PostgresqlCatalogRelationHandler,
    _env_var_name,
    _parse_properties,
    canonical_from_raw,
    canonical_properties,
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
    """Tests for the _parse_properties module-level function."""

    def test_simple(self):
        """Verify simple key=value parsing."""
        raw = "key=value\nfoo=bar"
        result = _parse_properties(raw)
        self.assertEqual(result, {"key": "value", "foo": "bar"})

    def test_escaped_colon_and_equals(self):
        """Verify Trino-style escaped colons and equals are unescaped."""
        # Trino escapes : and = in property keys/values
        raw = r"connection\=url\:jdbc\:postgresql\://host\:5432/db"
        # After unescaping: connection=url:jdbc:postgresql://host:5432/db
        # Split on first =: key="connection", value="url:jdbc:postgresql://host:5432/db"
        result = _parse_properties(raw)
        self.assertEqual(
            result,
            {"connection": "url:jdbc:postgresql://host:5432/db"},
        )

    def test_comments_and_blanks_skipped(self):
        """Verify comments and blank lines are ignored."""
        raw = "# comment\n\nkey=value\n  \n# another"
        result = _parse_properties(raw)
        self.assertEqual(result, {"key": "value"})

    def test_value_with_equals(self):
        """Verify values containing equals signs are preserved."""
        raw = "url=jdbc:postgresql://host:5432/db?a=1&b=2"
        result = _parse_properties(raw)
        self.assertEqual(result, {"url": "jdbc:postgresql://host:5432/db?a=1&b=2"})

    def test_whitespace_trimmed(self):
        """Verify surrounding whitespace is trimmed."""
        raw = "  key  =  value  "
        result = _parse_properties(raw)
        self.assertEqual(result, {"key": "value"})

    def test_empty_string(self):
        """Verify empty input returns empty dict."""
        result = _parse_properties("")
        self.assertEqual(result, {})


class TestCanonicalProperties(TestCase):
    """Tests for canonical_properties and canonical_from_raw."""

    def test_drops_connector_name_and_sorts(self):
        """Verify connector.name is dropped and keys are sorted."""
        result = canonical_properties({"connector.name": "postgresql", "b": "2", "a": "1"})
        self.assertEqual(result, "a=1\nb=2")

    def test_trino_written_file_matches_rendered_desired_state(self):
        """Verify canonical serializations of Trino- and charm-rendered forms match.

        A file written by Trino (escaped colons/equals, connector.name added,
        reordered keys) must canonicalise to the same string as the charm's
        own rendering of the same desired properties.
        """
        desired = {
            "connection-url": "jdbc:postgresql://host:5432/db",
            "connection-user": "admin",
            "connection-password": "${ENV:PG_PASS_DB}",
            "query.comment-format": DYNAMIC_CATALOG_MARKER,
        }
        # Trino persists the file with connector.name added, keys reordered,
        # and colons/equals escaped.
        raw = (
            "connector.name=postgresql\n"
            "query.comment-format=dynamic catalog\n"
            r"connection-user=admin"
            "\n"
            r"connection-password=${ENV\:PG_PASS_DB}"
            "\n"
            r"connection-url=jdbc\:postgresql\://host\:5432/db"
            "\n"
        )
        self.assertEqual(canonical_from_raw(raw), canonical_properties(desired))

    def test_property_value_change_differs(self):
        """Verify a changed property value produces a different canonical string."""
        original = canonical_properties({"connection-user": "admin"})
        changed = canonical_properties({"connection-user": "other"})
        self.assertNotEqual(original, changed)


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
        """Verify TLS with CA cert sets sslrootcert to the shared truststore."""
        handler = self._make_handler()
        pg = self._make_pg("host:5432", tls=True, tls_ca="BEGIN CERT...")
        url = handler._build_jdbc_url(pg, "mydb", 1, "primary")
        self.assertIn("ssl=true", url)
        self.assertIn("sslrootcert=/path/to/truststore", url)


class TestTrinoReadiness(TestCase):
    """Tests for is_trino_ready."""

    def _make_handler(self):
        """Create a mock handler bound to the real readiness method."""
        handler = mock.MagicMock()
        handler.is_trino_ready = PostgresqlCatalogRelationHandler.is_trino_ready.__get__(handler)
        return handler

    @mock.patch("relations.postgresql_catalog.requests.get")
    def test_reachable_when_started(self, mock_get):
        """Verify reachable when HTTP 200 and starting is False."""
        mock_get.return_value = mock.MagicMock(
            status_code=200, json=mock.MagicMock(return_value={"starting": False})
        )
        handler = self._make_handler()
        self.assertTrue(handler.is_trino_ready())

    @mock.patch("relations.postgresql_catalog.requests.get")
    def test_not_reachable_while_starting(self, mock_get):
        """Verify not reachable when server is still initializing."""
        mock_get.return_value = mock.MagicMock(
            status_code=200, json=mock.MagicMock(return_value={"starting": True})
        )
        handler = self._make_handler()
        self.assertFalse(handler.is_trino_ready())

    @mock.patch("relations.postgresql_catalog.requests.get")
    def test_not_reachable_on_non_200(self, mock_get):
        """Verify not reachable when the health endpoint returns non-200."""
        mock_get.return_value = mock.MagicMock(status_code=503)
        handler = self._make_handler()
        self.assertFalse(handler.is_trino_ready())

    @mock.patch("relations.postgresql_catalog.requests.get", side_effect=Exception("boom"))
    def test_not_reachable_on_exception(self, _mock_get):
        """Verify not reachable when the request raises."""
        handler = self._make_handler()
        self.assertFalse(handler.is_trino_ready())


class TestRenderDynamicCatalogs(TestCase):
    """Tests for render_dynamic_catalogs."""

    def _make_handler(
        self, relations, config_by_relation, pg_by_relation, charm_function="coordinator"
    ):
        """Bind render_dynamic_catalogs and its pure collaborators to a mock handler."""
        handler = mock.MagicMock()
        handler.relation_name = POSTGRESQL_RELATION_NAME
        handler.render_dynamic_catalogs = (
            PostgresqlCatalogRelationHandler.render_dynamic_catalogs.__get__(handler)
        )
        handler._compute_wanted_catalogs = (
            PostgresqlCatalogRelationHandler._compute_wanted_catalogs.__get__(handler)
        )
        handler._build_catalog_props = (
            PostgresqlCatalogRelationHandler._build_catalog_props.__get__(handler)
        )
        handler._build_jdbc_url = PostgresqlCatalogRelationHandler._build_jdbc_url.__get__(handler)
        handler.charm.config.charm_function = charm_function
        handler.charm.model.relations = {POSTGRESQL_RELATION_NAME: relations}
        handler.charm.truststore_abs_path = "/path/to/truststore"
        handler._find_config_for_relation = mock.Mock(
            side_effect=lambda r: config_by_relation.get(r)
        )
        handler._load_relation_data = mock.Mock(side_effect=lambda r: pg_by_relation.get(r))
        return handler

    @staticmethod
    def _make_pg(prefix_databases="mydb", password="pw", username="user"):  # nosec B107
        """Build a minimal stand-in for a PostgresqlRelationModel."""
        return SimpleNamespace(
            prefix_databases=prefix_databases,
            password=password,
            username=username,
            all_endpoints="host:5432",
            tls=False,
            tls_ca=None,
        )

    def test_valid_relation_returns_expected_catalogs(self):
        """Verify a fully configured relation yields the desired RO/RW catalogs."""
        relation = mock.MagicMock(id=1)
        relation.app.name = "pg-app"
        config_entry = {
            "database_prefix": "mydb*",
            "ro_catalog_name": "cat_ro",
            "rw_catalog_name": "cat_rw",
        }
        pg = self._make_pg()
        handler = self._make_handler([relation], {relation: config_entry}, {relation: pg})

        catalogs = handler.render_dynamic_catalogs()

        self.assertEqual(set(catalogs), {"cat_ro", "cat_rw"})
        self.assertIn("targetServerType=preferSecondary", catalogs["cat_ro"]["connection-url"])
        self.assertIn("targetServerType=primary", catalogs["cat_rw"]["connection-url"])

    def test_skips_relation_missing_config(self):
        """Verify a relation with no matching config entry is skipped."""
        relation = mock.MagicMock(id=1)
        relation.app.name = "pg-app"
        pg = self._make_pg()
        handler = self._make_handler([relation], {}, {relation: pg})

        self.assertEqual(handler.render_dynamic_catalogs(), {})

    def test_skips_relation_missing_data(self):
        """Verify a relation with no loadable data is skipped."""
        relation = mock.MagicMock(id=1)
        relation.app.name = "pg-app"
        config_entry = {"database_prefix": "mydb*", "ro_catalog_name": "cat_ro"}
        handler = self._make_handler([relation], {relation: config_entry}, {})

        self.assertEqual(handler.render_dynamic_catalogs(), {})

    def test_skips_relation_with_multiple_prefix_databases(self):
        """Verify a relation reporting more than one prefix database is skipped."""
        relation = mock.MagicMock(id=1)
        relation.app.name = "pg-app"
        config_entry = {"database_prefix": "mydb*", "ro_catalog_name": "cat_ro"}
        pg = self._make_pg(prefix_databases="db1,db2")
        handler = self._make_handler([relation], {relation: config_entry}, {relation: pg})

        self.assertEqual(handler.render_dynamic_catalogs(), {})

    def test_non_coordinator_returns_empty(self):
        """Verify a non-coordinator charm function yields no catalogs."""
        relation = mock.MagicMock(id=1)
        relation.app.name = "pg-app"
        config_entry = {"database_prefix": "mydb*", "ro_catalog_name": "cat_ro"}
        pg = self._make_pg()
        handler = self._make_handler(
            [relation], {relation: config_entry}, {relation: pg}, charm_function="worker"
        )

        self.assertEqual(handler.render_dynamic_catalogs(), {})


class TestCreateAndDropCatalog(TestCase):
    """Tests for create_catalog and drop_catalog error classification."""

    def _make_handler(self):
        """Bind the executor methods to a mock handler using a default user."""
        handler = mock.MagicMock()
        handler.create_catalog = PostgresqlCatalogRelationHandler.create_catalog.__get__(handler)
        handler.drop_catalog = PostgresqlCatalogRelationHandler.drop_catalog.__get__(handler)
        handler._execute_sql = PostgresqlCatalogRelationHandler._execute_sql.__get__(handler)
        handler._raise_for_trino_error = PostgresqlCatalogRelationHandler._raise_for_trino_error
        handler._build_catalog_sql = PostgresqlCatalogRelationHandler._build_catalog_sql
        handler._get_trino_user = PostgresqlCatalogRelationHandler._get_trino_user.__get__(handler)
        handler._cancel_statement = PostgresqlCatalogRelationHandler._cancel_statement
        handler.charm._effective_user_secret_id.return_value = None
        return handler

    @mock.patch("relations.postgresql_catalog.requests.post")
    def test_create_catalog_success(self, mock_post):
        """Verify create_catalog returns normally when Trino reports no error."""
        mock_post.return_value = mock.MagicMock(
            status_code=200, json=mock.MagicMock(return_value={})
        )
        handler = self._make_handler()
        handler.create_catalog("cat", {"connection-url": "jdbc:postgresql://host/db"})

    @mock.patch("relations.postgresql_catalog.requests.post")
    def test_create_catalog_already_exists(self, mock_post):
        """Verify an explicit already-exists report raises CatalogAlreadyExistsError."""
        mock_post.return_value = mock.MagicMock(
            status_code=200,
            json=mock.MagicMock(
                return_value={
                    "error": {
                        "errorName": "CATALOG_ALREADY_EXISTS",
                        "message": "Catalog 'cat' already exists",
                    }
                }
            ),
        )
        handler = self._make_handler()
        with self.assertRaises(CatalogAlreadyExistsError):
            handler.create_catalog("cat", {"connection-url": "jdbc:postgresql://host/db"})

    @mock.patch("relations.postgresql_catalog.requests.post")
    def test_create_catalog_other_error_in_first_response(self, mock_post):
        """Verify a non-already-exists error in the first response raises the base error."""
        mock_post.return_value = mock.MagicMock(
            status_code=200,
            json=mock.MagicMock(
                return_value={
                    "error": {
                        "errorName": "INVALID_CATALOG_PROPERTY",
                        "message": "invalid property",
                    }
                }
            ),
        )
        handler = self._make_handler()
        with self.assertRaises(CatalogSQLError) as ctx:
            handler.create_catalog("cat", {"connection-url": "jdbc:postgresql://host/db"})
        self.assertNotIsInstance(ctx.exception, CatalogAlreadyExistsError)

    @mock.patch(
        "relations.postgresql_catalog.requests.post",
        side_effect=requests.ConnectionError("boom"),
    )
    def test_create_catalog_wraps_request_exception(self, _mock_post):
        """Verify a requests exception is wrapped in CatalogSQLError."""
        handler = self._make_handler()
        with self.assertRaises(CatalogSQLError):
            handler.create_catalog("cat", {"connection-url": "jdbc:postgresql://host/db"})

    @mock.patch("relations.postgresql_catalog.requests.post")
    def test_drop_catalog_raises_on_error(self, mock_post):
        """Verify drop_catalog raises CatalogSQLError when Trino reports an error."""
        mock_post.return_value = mock.MagicMock(
            status_code=200,
            json=mock.MagicMock(
                return_value={"error": {"errorName": "GENERIC_INTERNAL_ERROR", "message": "boom"}}
            ),
        )
        handler = self._make_handler()
        with self.assertRaises(CatalogSQLError):
            handler.drop_catalog("cat")

    def test_quotes_are_escaped_in_generated_sql(self):
        """Verify names and values cannot break out of their SQL quoting."""
        sql = PostgresqlCatalogRelationHandler._build_catalog_sql(
            'ev"il',
            {"connection-user": "o'brien", 'we"ird': "value"},
        )

        self.assertIn('CREATE CATALOG "ev""il" USING postgresql', sql)
        self.assertIn("\"connection-user\" = 'o''brien'", sql)
        self.assertIn('"we""ird" = ', sql)

    @mock.patch("relations.postgresql_catalog.requests.delete")
    @mock.patch("relations.postgresql_catalog.requests.get")
    @mock.patch("relations.postgresql_catalog.requests.post")
    def test_repeated_next_uri_is_abandoned(self, mock_post, mock_get, mock_delete):
        """Verify a looping result walk ends instead of blocking the hook."""
        page = {"nextUri": "http://localhost:8080/v1/statement/q/1"}
        mock_post.return_value = mock.MagicMock(
            status_code=200, json=mock.MagicMock(return_value=page)
        )
        mock_get.return_value = mock.MagicMock(
            status_code=200, json=mock.MagicMock(return_value=page)
        )
        handler = self._make_handler()

        with self.assertRaises(CatalogSQLError):
            handler.drop_catalog("cat")

        mock_delete.assert_called_once()


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
