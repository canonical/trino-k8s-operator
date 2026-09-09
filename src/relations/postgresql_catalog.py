# Copyright 2023 Canonical Ltd.
# See LICENSE file for licensing details.

"""PostgreSQL catalog relation handler."""

import json
import logging
import time
from typing import Callable, Optional

import pydantic
import requests
import yaml
from ops import framework
from ops.model import SecretNotFoundError

from literals import DEFAULT_CREDENTIALS, POSTGRESQL_RELATION_NAME

REQUESTED_SECRETS = ["username", "password", "tls", "tls-ca"]
PASS_ENV_VAR_PREFIX = "PG_PASS_"  # nosec
DYNAMIC_CATALOG_MARKER = "dynamic catalog"

logger = logging.getLogger(__name__)

REQUEST_TIMEOUT = 30.0
STATEMENT_DEADLINE = 120.0


def _quote_identifier(value) -> str:
    """Quote a SQL identifier so that it cannot alter the statement.

    Args:
        value: The identifier to quote.

    Returns:
        The identifier wrapped in double quotes with embedded quotes doubled.
    """
    return '"' + str(value).replace('"', '""') + '"'


def _quote_literal(value) -> str:
    """Quote a SQL string literal so that it cannot alter the statement.

    Args:
        value: The value to quote.

    Returns:
        The value wrapped in single quotes with embedded quotes doubled.
    """
    return "'" + str(value).replace("'", "''") + "'"


class CatalogSQLError(Exception):
    """Raised when a CREATE/DROP CATALOG statement fails against Trino."""


class CatalogAlreadyExistsError(CatalogSQLError):
    """Raised when Trino explicitly reports that a catalog already exists."""


def _env_var_name(database: str) -> str:
    """Derive the pebble environment variable name for a database password.

    Args:
        database: The database name.

    Returns:
        Environment variable name, e.g. `PG_PASS_MYDB`.
    """
    return PASS_ENV_VAR_PREFIX + database.upper().replace("-", "_")


def _parse_properties(raw: str) -> dict:
    """Parse a Java `.properties` file into a dict.

    Handles backslash-escaped colons and equals signs that Trino writes
    when persisting dynamic catalogs.

    Args:
        raw: The raw file content.

    Returns:
        Dict of property key-value pairs.
    """
    props = {}
    for line in raw.splitlines():
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        # Java properties use first unescaped = or : as separator
        line = line.replace("\\:", ":").replace("\\=", "=")
        if "=" in line:
            k, v = line.split("=", 1)
            props[k.strip()] = v.strip()
    return props


def canonical_properties(properties: dict) -> str:
    """Render catalog properties into a canonical comparable form.

    Drops `connector.name`, which Trino adds on its own when persisting a
    catalog, and emits `key=value` lines sorted by key so that a
    Trino-rewritten file and the charm's own rendering of the same desired
    state compare equal.

    Args:
        properties: Catalog properties.

    Returns:
        Canonical "key=value" lines, one per property, joined by newlines.
    """
    filtered = {
        k.strip(): v.strip() for k, v in properties.items() if k.strip() != "connector.name"
    }
    return "\n".join(f"{k}={filtered[k]}" for k in sorted(filtered))


def canonical_from_raw(raw: str) -> str:
    """Parse and canonicalise a raw `.properties` file.

    Args:
        raw: Raw file content, potentially written by Trino.

    Returns:
        The canonical serialization; see `canonical_properties`.
    """
    return canonical_properties(_parse_properties(raw))


def is_dynamic_catalog(raw: str) -> bool:
    """Check whether a raw `.properties` file claims dynamic ownership.

    Args:
        raw: Raw file content.

    Returns:
        True when the file carries the ownership marker as the value of the
        `query.comment-format` property, rather than merely mentioning it.
    """
    return _parse_properties(raw).get("query.comment-format") == DYNAMIC_CATALOG_MARKER


class PostgresqlRelationModel(pydantic.BaseModel):
    """Typed representation of the provider's relation databag.

    Attributes:
        endpoints: Primary endpoint(s) as "host:port" comma-separated.
        read_only_endpoints: Read-only endpoint(s) as "host:port" comma-separated.
        prefix_databases: Comma-separated list of prefix-matched database names.
        secret_user: Dict with "username" and "password" keys.
        secret_tls: Dict with "tls" and "tls-ca" keys.
        all_endpoints: All available endpoints formatted as 'host1,host2,host3:port'.
        username: Username for the database.
        password: Password for the database.
        tls: Whether the database implements TLS.
        tls_ca: Certificate authority cert for TLS, if available.
    """

    endpoints: str
    read_only_endpoints: str = pydantic.Field(alias="read-only-endpoints")
    prefix_databases: str = pydantic.Field(alias="prefix-databases")
    secret_user: dict[str, str] = pydantic.Field(alias="secret-user")
    secret_tls: dict[str, str] = pydantic.Field(alias="secret-tls")

    @property
    def all_endpoints(self) -> str:
        """All available endpoints formatted as 'host1,host2,host3:port'."""
        hosts = set()
        port = ""
        for src in (self.endpoints, self.read_only_endpoints):
            for entry in src.split(","):
                entry = entry.strip()
                if not entry:
                    continue
                host, port = entry.rsplit(":", 1)
                hosts.add(host)
        return f"{','.join(sorted(hosts))}:{port}"

    @property
    def username(self) -> str:
        """Username for the database server."""
        return self.secret_user["username"]

    @property
    def password(self) -> str:
        """Password for the database server."""
        return self.secret_user["password"]

    @property
    def tls(self) -> bool:
        """Whether the database server implements TLS."""
        return self.secret_tls.get("tls", "false").lower() == "true"

    @property
    def tls_ca(self) -> Optional[str]:
        """Certificate authority cert for TLS, if available."""
        return self.secret_tls.get("tls-ca")

    @classmethod
    def decode(cls, charm) -> Callable[[str], str | dict[str, str]]:
        """Generate a decoder that normalizes JSON and fetches Juju secrets.

        Args:
            charm: The charm instance for secret resolution.

        Returns:
            A function that decodes relation databag values.
        """

        def wrapped(v: str) -> str | dict[str, str]:
            """Decode a single relation databag value.

            Args:
                v: Raw value from the databag.

            Returns:
                Decoded value (string or dict for secrets).
            """
            try:
                ret = json.loads(v)
            except json.JSONDecodeError:
                ret = v

            if not v.startswith("secret:"):
                return ret

            secret = charm.model.get_secret(id=v)
            content = secret.get_content(refresh=True)
            return content

        return wrapped


class PostgresqlCatalogRelationHandler(framework.Object):
    """Handler for PostgreSQL catalog relations via the postgresql_client interface."""

    def __init__(self, charm, relation_name=POSTGRESQL_RELATION_NAME):
        """Construct.

        Args:
            charm: The charm to attach the hooks to.
            relation_name: The name of the relation.
        """
        super().__init__(charm, relation_name)
        self.charm = charm
        self.relation_name = relation_name

    def get_desired_tls_certs(self) -> dict:
        """Return the desired TLS CA certificates for PostgreSQL relations.

        Returns:
            Mapping of truststore alias to CA certificate (PEM) for each
            TLS-enabled PostgreSQL relation.
        """
        if self.charm.config.charm_function not in ("coordinator", "all"):
            return {}

        certs = {}
        for relation in self.charm.model.relations[self.relation_name]:
            pg = self._load_relation_data(relation)
            if pg is None or not pg.tls or not pg.tls_ca:
                continue
            certs[f"pg-relation-{relation.id}"] = pg.tls_ca
        return certs

    def get_postgresql_env_vars(self) -> dict:
        """Return password env vars derived from PG relations.

        Returns:
            Dict mapping env var names to password values.
        """
        if self.charm.config.charm_function not in ("coordinator", "all"):
            return {}
        _, env_vars = self._compute_wanted_catalogs()
        return env_vars

    def render_dynamic_catalogs(self) -> dict[str, dict[str, str]]:
        """Return desired dynamic catalog properties from relations and config.

        Pure: reads only relation data and charm config, never touches the
        container or Trino.

        Returns:
            Mapping of catalog name to its rendered properties.
        """
        if self.charm.config.charm_function not in ("coordinator", "all"):
            return {}
        catalogs, _ = self._compute_wanted_catalogs()
        return catalogs

    def _write_databag(self):
        """Write database and requested-secrets for relations missing them."""
        for relation in self.charm.model.relations[self.relation_name]:
            app_databag = relation.data.get(self.charm.app, {})
            if app_databag.get("database"):
                logger.debug(
                    "PG relation %s already requests database prefix %r",
                    relation.app.name,
                    app_databag.get("database"),
                )
                continue

            config_entry = self._find_config_for_relation(relation)
            if not config_entry:
                logger.error(
                    "No postgresql-catalog-config entry for %s, databag not written",
                    relation.app.name,
                )
                continue

            app_databag = relation.data[self.charm.app]
            app_databag["database"] = config_entry["database_prefix"]
            app_databag["requested-secrets"] = json.dumps(REQUESTED_SECRETS)
            logger.info(
                "Requested PG database prefix %r on relation %s",
                config_entry["database_prefix"],
                relation.app.name,
            )

    def _find_config_for_relation(self, relation) -> Optional[dict]:
        """Find the postgresql-catalog-config entry matching a relation.

        Args:
            relation: The Juju relation.

        Returns:
            The config dict for this relation, or None.
        """
        raw = self.charm.config.postgresql_catalog_config
        if not raw:
            logger.warning(
                "postgresql-catalog-config is empty, cannot map relation %r",
                relation.app.name,
            )
            return None
        config = yaml.safe_load(raw)
        if not isinstance(config, dict):
            logger.error("postgresql-catalog-config is not a valid YAML")
            return None
        entry = config.get(relation.app.name)
        if entry is None:
            logger.warning(
                "No postgresql-catalog-config entry for app %r",
                relation.app.name,
            )
            return None
        return entry

    def _compute_wanted_catalogs(self) -> tuple[dict, dict]:
        """Compute the wanted catalog state from relations and config.

        Returns:
            Tuple of (catalogs, password_env_vars)
            catalogs = catalog_name: properties dict
            password_env_vars maps = env_var_name: password_value
        """
        catalogs = {}
        env_vars: dict[str, str] = {}
        for relation in self.charm.model.relations[self.relation_name]:
            config_entry = self._find_config_for_relation(relation)
            if not config_entry:
                logger.error(
                    "No postgresql-catalog-config entry for %s",
                    relation.app.name,
                )
                continue

            pg = self._load_relation_data(relation)
            if pg is None:
                logger.error(
                    "Relation data not available for %s",
                    relation.app.name,
                )
                continue

            if not pg.prefix_databases:
                logger.error("No prefix databases for %s", relation.app.name)
                continue

            databases = [d.strip() for d in pg.prefix_databases.split(",") if d.strip()]
            if len(databases) != 1:
                logger.error(
                    "Multiple prefix databases returned for %s: %s",
                    relation.app.name,
                    databases,
                )
                continue

            database = databases[0]
            env_vars[_env_var_name(database)] = pg.password

            # RO catalog
            ro_name = config_entry.get("ro_catalog_name")
            if ro_name:
                catalogs[ro_name] = self._build_catalog_props(
                    pg=pg,
                    database=database,
                    config_entry=config_entry,
                    relation_id=relation.id,
                    target_server_type="preferSecondary",
                )

            # RW catalog
            rw_name = config_entry.get("rw_catalog_name")
            if rw_name:
                catalogs[rw_name] = self._build_catalog_props(
                    pg=pg,
                    database=database,
                    config_entry=config_entry,
                    relation_id=relation.id,
                    target_server_type="primary",
                )

        return catalogs, env_vars

    def _load_relation_data(self, relation) -> Optional[PostgresqlRelationModel]:
        """Load and validate relation data from the provider's databag.

        Args:
            relation: The Juju relation to load data from.

        Returns:
            A PostgresqlRelationModel if data is available, None otherwise.
        """
        if relation.app is None:
            return None
        try:
            return relation.load(
                PostgresqlRelationModel,
                relation.app,
                decoder=PostgresqlRelationModel.decode(self.charm),
            )
        except pydantic.ValidationError as err:
            # Log which provider keys are present (names only, not secret
            # values) to distinguish an incomplete handshake from a missing
            # secret when diagnosing why no catalog is created.
            logger.warning(
                "PG relation %s databag incomplete; provider keys present=%s; error=%s",
                relation.app.name,
                sorted(relation.data[relation.app].keys()),
                err,
            )
            return None
        except SecretNotFoundError as err:
            logger.warning("PG relation %s secret not yet available: %s", relation.app.name, err)
            return None

    def _build_catalog_props(self, pg, database, config_entry, relation_id, target_server_type):
        """Build catalog properties dict for a single catalog.

        Args:
            pg: The PostgresqlRelationModel with connection data.
            database: The database name.
            config_entry: The config dict for this relation.
            relation_id: The relation ID.
            target_server_type: JDBC targetServerType (preferSecondary or primary).

        Returns:
            Dict of catalog properties.
        """
        url = self._build_jdbc_url(pg, database, relation_id, target_server_type)

        properties = {
            "connection-url": url,
            "connection-user": pg.username,
            "connection-password": f"${{ENV:{_env_var_name(database)}}}",
            "query.comment-format": DYNAMIC_CATALOG_MARKER,
        }

        # Parse extra config lines (key=value format)
        extra_config = config_entry.get("config", "")
        if extra_config:
            for line in extra_config.strip().splitlines():
                line = line.strip()
                if "=" in line:
                    k, v = line.split("=", 1)
                    properties[k.strip()] = v.strip()

        return properties

    def _build_jdbc_url(self, pg, database, relation_id, target_server_type):
        """Build JDBC URL with auto-deduced SSL params.

        Args:
            pg: The PostgresqlRelationModel with connection data.
            database: The database name.
            relation_id: The relation ID.
            target_server_type: JDBC targetServerType parameter value.

        Returns:
            The complete JDBC URL string.
        """
        params = [f"targetServerType={target_server_type}"]

        if pg.tls:
            params.append("ssl=true")
            params.append("sslmode=require")
            if pg.tls_ca:
                params.append(f"sslrootcert={self.charm.truststore_abs_path}")
                params.append("sslrootcertpassword=${ENV:JAVA_TRUSTSTORE_PWD}")
        else:
            params.append("ssl=false")

        url = f"jdbc:postgresql://{pg.all_endpoints}/{database}"
        return f"{url}?{'&'.join(params)}"

    def is_trino_ready(self) -> bool:
        """Check if Trino is reachable and finished initializing.

        Returns:
            True if Trino responds to a health check and is no longer starting.
        """
        try:
            resp = requests.get("http://localhost:8080/v1/info", timeout=5.0)
            if resp.status_code != 200:
                return False
            # /v1/info returns 200 while the server is still initializing, so
            # also require the "starting" flag to be false before treating
            # Trino as ready to accept CREATE/DROP CATALOG statements.
            return resp.json().get("starting") is False
        except Exception:
            return False

    def _get_trino_user(self) -> str:
        """Get the Trino user for HTTP API calls.

        Uses the same logic as _update_password_db: if user-secret-id is set,
        use the first user from the secret; otherwise use DEFAULT_CREDENTIALS.

        HTTP to localhost:8080 within the same container bypasses authentication
        (allow-insecure-over-http=true) but authorization is still needed:
        - Without Ranger: the file-based ACL set to "acl-mode-default=owner"
          grants catalog management to the catalog owner.
        - With Ranger: the user must have CREATE/DROP catalog
          permissions configured in Ranger policies.

        Returns:
            The Trino username.
        """
        secret_id = self.charm._effective_user_secret_id()
        if secret_id:
            try:
                credentials = yaml.safe_load(self.charm._get_secret_content(secret_id)["users"])
                return next(iter(credentials))
            except Exception:
                logger.error("Could not read user secret, using default creds")
        return next(iter(DEFAULT_CREDENTIALS))

    def _execute_sql(self, sql):
        """Execute SQL via the Trino HTTP API.

        Args:
            sql: The SQL statement to execute.

        Raises:
            CatalogAlreadyExistsError: Trino reported that the catalog
                already exists.
            CatalogSQLError: The request failed, or Trino reported any
                other error.
        """
        user = self._get_trino_user()
        headers = {"X-Trino-User": user}

        next_uri = None
        try:
            resp = requests.post(
                "http://localhost:8080/v1/statement",
                data=sql,
                headers=headers,
                timeout=REQUEST_TIMEOUT,
            )
            resp.raise_for_status()
            data = resp.json()
            self._raise_for_trino_error(data)

            # A statement is only complete once its result pages are drained.
            # The walk is bounded so a looping or stalled query cannot hold
            # the charm hook open indefinitely.
            deadline = time.monotonic() + STATEMENT_DEADLINE
            seen = set()
            while "nextUri" in data:
                next_uri = data["nextUri"]
                if next_uri in seen or time.monotonic() > deadline:
                    raise CatalogSQLError("Trino statement did not complete in time")
                seen.add(next_uri)
                resp = requests.get(next_uri, headers=headers, timeout=REQUEST_TIMEOUT)
                resp.raise_for_status()
                data = resp.json()
                self._raise_for_trino_error(data)
            next_uri = None
        except requests.RequestException as err:
            raise CatalogSQLError(f"Trino SQL request failed: {err}") from err
        finally:
            self._cancel_statement(next_uri, headers)

    @staticmethod
    def _cancel_statement(next_uri, headers) -> None:
        """Ask Trino to abandon a statement that was left partially consumed.

        Args:
            next_uri: The pending result URI, or None when there is nothing
                to cancel.
            headers: The request headers identifying the Trino user.
        """
        if not next_uri:
            return
        try:
            requests.delete(next_uri, headers=headers, timeout=REQUEST_TIMEOUT)
        except requests.RequestException:
            logger.debug("Could not cancel an abandoned Trino statement")

    @staticmethod
    def _raise_for_trino_error(data: dict) -> None:
        """Raise a typed error if a Trino statement response reports one.

        Only an explicit "catalog already exists" report is classified as
        `CatalogAlreadyExistsError`; every other error (unavailability,
        authorization, invalid properties) raises the base `CatalogSQLError`
        so callers cannot mistake it for a safe-to-drop condition.

        Args:
            data: A decoded Trino statement API response.

        Raises:
            CatalogAlreadyExistsError: Trino reported the catalog exists.
            CatalogSQLError: Trino reported any other error.
        """
        error = data.get("error")
        if not error:
            return
        error_name = error.get("errorName", "")
        message = str(error.get("message", ""))
        # The exception message intentionally omits the raw Trino message,
        # which may echo back invalid property values.
        if error_name == "CATALOG_ALREADY_EXISTS" or "already exists" in message.lower():
            raise CatalogAlreadyExistsError(f"Catalog already exists ({error_name or 'unknown'})")
        raise CatalogSQLError(f"Trino reported error {error_name or 'unknown'}")

    def create_catalog(self, name, properties):
        """Create a Trino catalog via SQL.

        Args:
            name: The catalog name.
            properties: Dict of catalog properties (may contain credentials).

        Raises:
            CatalogAlreadyExistsError: Trino reported that the catalog
                already exists.
            CatalogSQLError: SQL execution failed for another reason.
        """
        sql = self._build_catalog_sql(name, properties)
        self._execute_sql(sql)
        logger.info("Created catalog %r", name)

    @staticmethod
    def _build_catalog_sql(name, properties) -> str:
        """Build a CREATE CATALOG SQL statement.

        Args:
            name: The catalog name.
            properties: Dict of catalog properties.

        Returns:
            The SQL string.
        """
        props_sql = ",\n  ".join(
            f"{_quote_identifier(k)} = {_quote_literal(v)}" for k, v in properties.items()
        )
        return (
            f"CREATE CATALOG {_quote_identifier(name)} USING postgresql\nWITH (\n  {props_sql}\n)"
        )

    def drop_catalog(self, name):
        """Drop a Trino catalog via SQL.

        Args:
            name: The catalog name to drop.

        Raises:
            CatalogSQLError: SQL execution failed.
        """
        self._execute_sql(f"DROP CATALOG IF EXISTS {_quote_identifier(name)}")
        logger.info("Dropped catalog %r", name)
