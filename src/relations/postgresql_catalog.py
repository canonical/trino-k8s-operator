# Copyright 2023 Canonical Ltd.
# See LICENSE file for licensing details.

"""PostgreSQL catalog relation handler."""

import hashlib
import json
import logging
from typing import Callable, Optional

import pydantic
import requests
import yaml
from ops import framework, pebble
from ops.model import SecretNotFoundError

from literals import DEFAULT_CREDENTIALS, POSTGRESQL_RELATION_NAME
from log import log_event_handler
from utils import add_cert_to_truststore

REQUESTED_SECRETS = ["username", "password", "tls", "tls-ca"]
PASS_ENV_VAR_PREFIX = "PG_PASS_"  # nosec
DYNAMIC_CATALOG_MARKER = "dynamic catalog"

logger = logging.getLogger(__name__)


def _env_var_name(database: str) -> str:
    """Derive the pebble environment variable name for a database password.

    Args:
        database: The database name.

    Returns:
        Environment variable name, e.g. ``PG_PASS_MYDB``.
    """
    return PASS_ENV_VAR_PREFIX + database.upper().replace("-", "_")


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
        self._current_wanted_envs = None

        self.framework.observe(
            charm.on[self.relation_name].relation_created,
            self._on_relation_created,
        )
        self.framework.observe(
            charm.on[self.relation_name].relation_changed,
            self._on_relation_changed,
        )
        self.framework.observe(
            charm.on[self.relation_name].relation_broken,
            self._on_relation_broken,
        )

    @log_event_handler(logger)
    def _on_relation_created(self, event):
        """Handle relation created: trigger reconciliation.

        Args:
            event: The relation created event.
        """
        self.reconcile_postgresql_catalogs(event)

    @log_event_handler(logger)
    def _on_relation_changed(self, event):
        """Handle relation changed: trigger reconciliation.

        Args:
            event: The relation changed event.
        """
        self.reconcile_postgresql_catalogs(event)
        self.charm.trino_catalog.reconcile_trino_catalog_relations()

    @log_event_handler(logger)
    def _on_relation_broken(self, event):
        """Handle relation broken: trigger reconciliation.

        Args:
            event: The relation broken event.
        """
        self.reconcile_postgresql_catalogs(event)
        self.charm.trino_catalog.reconcile_trino_catalog_relations()

    def reconcile_postgresql_catalogs(self, event=None):
        """Reconcile wanted vs tracked catalog state via CREATE/DROP CATALOG.

        Compares what catalogs should exist (from relations and config) against
        what currently exists on disk (`.properties` files with the dynamic
        catalog marker), then issues CREATE/DROP CATALOG SQL to correct
        discrepancies.

        Before issuing SQL statements, ensures pebble env vars carry the
        current passwords so that ``${ENV:…}`` references in CREATE CATALOG
        resolve correctly.  Delegates replanning to ``self.charm._update()``.

        Args:
            event: The Juju event that triggered reconciliation (optional).
        """
        if self.charm.config["charm-function"] not in ("coordinator", "all"):
            return

        self._write_databag()

        wanted_catalogs, wanted_envs = self._compute_wanted_catalogs()
        self._current_wanted_envs = wanted_envs

        if not self._is_trino_reachable():
            logger.warning(
                "Trino not reachable, skipping catalog reconciliation"
            )
            self._current_wanted_envs = None
            return

        tracked_catalogs = self._read_tracked_catalogs()

        # DROP catalogs that should no longer exist while Trino is still
        # running with the current env vars (before any replan/restart).
        to_drop = set(tracked_catalogs) - set(wanted_catalogs)
        for name in to_drop:
            logger.info("Dropping relation catalog %r", name)
            self._drop_catalog(name)

        # Replan if password env vars changed so ${ENV:…} references resolve
        if self._env_vars_changed(wanted_envs):
            self.charm._update(event)
            self.charm.trino_coordinator.update_coordinator_relation_data()

        self._current_wanted_envs = None

        if not self._is_trino_reachable():
            logger.warning(
                "Trino not reachable after replan, skipping catalog creates"
            )
            return

        # CREATE new catalogs and UPDATE changed ones (drop + re-create)
        for name, props in wanted_catalogs.items():
            props_hash = self._hash_properties(props)
            tracked_hash = tracked_catalogs.get(name)

            # Already up-to-date
            if tracked_hash == props_hash:
                continue

            is_update = name in tracked_catalogs
            action = "Updating" if is_update else "Creating"
            logger.info("%s relation catalog %r", action, name)

            # Trino has no ALTER CATALOG; drop first then re-create
            if is_update:
                self._drop_catalog(name)
            self._create_catalog(name, props)

    def get_postgresql_relation_catalogs(self) -> list:
        """Return catalog names managed by this handler.

        Reads `.properties` files from the catalog directory and returns
        names of catalogs that contain the dynamic catalog marker.

        Returns:
            List of catalog name strings.
        """
        return list(self._read_tracked_catalogs().keys())

    def get_postgresql_env_vars(self) -> dict:
        """Return password env vars derived from PG relations.

        If called during a ``reconcile_postgresql_catalogs()`` run, returns the cached value
        to avoid recomputing.  Otherwise computes fresh from relations.

        Returns:
            Dict mapping env var names to password values.
        """
        if getattr(self, "_current_wanted_envs", None) is not None:
            return self._current_wanted_envs
        if self.charm.config["charm-function"] not in (
            "coordinator",
            "all",
        ):
            return {}
        _, env_vars = self._compute_wanted_catalogs()
        return env_vars

    def _read_tracked_catalogs(self) -> dict:
        """Read dynamic catalogs from `.properties` files on disk.

        Scans the catalog directory for `.properties` files that contain
        the ``query.comment-format=dynamic catalog`` marker and returns
        a dict of catalog names to property hashes.

        Returns:
            Dict mapping catalog name to SHA-256 hash of its properties.
        """
        container = self.charm.unit.get_container(self.charm.name)
        if not container.can_connect():
            logger.debug(
                "Container not connectable, cannot read tracked catalogs"
            )
            return {}

        catalog_dir = str(self.charm.catalog_abs_path)
        try:
            files = container.list_files(catalog_dir)
        except pebble.PathError:
            logger.debug(
                "Catalog directory %s does not exist yet", catalog_dir
            )
            return {}
        except pebble.Error:
            logger.warning(
                "Failed to list catalog directory %s",
                catalog_dir,
                exc_info=True,
            )
            return {}

        tracked = {}
        for f in files:
            if not f.name.endswith(".properties"):
                continue
            file_path = f"{catalog_dir}/{f.name}"
            try:
                raw = container.pull(file_path).read()
            except pebble.Error:
                logger.warning("Failed to read %s", file_path, exc_info=True)
                continue
            try:
                props = self._parse_properties(raw)
            except Exception:
                logger.warning("Failed to parse %s", file_path, exc_info=True)
                continue
            if props.get("query.comment-format") == DYNAMIC_CATALOG_MARKER:
                catalog_name = f.name[: -len(".properties")]
                props.pop("connector.name", None)
                tracked[catalog_name] = self._hash_properties(props)
        return tracked

    def _env_vars_changed(self, wanted_envs) -> bool:
        """Check if PG password env vars differ from the current pebble plan.

        Args:
            wanted_envs: Dict of wanted env var names to values.

        Returns:
            True if any PG password env var is missing or changed.
        """
        container = self.charm.unit.get_container(self.charm.name)
        if not container.can_connect():
            return bool(wanted_envs)

        try:
            plan = container.get_plan().to_dict()
            services = plan.get("services", {})
            current_env = services.get(self.charm.name, {}).get(
                "environment", {}
            )
        except Exception:
            return bool(wanted_envs)

        current_pg = {
            k: v
            for k, v in current_env.items()
            if k.startswith(PASS_ENV_VAR_PREFIX)
        }
        return current_pg != (wanted_envs or {})

    @staticmethod
    def _parse_properties(raw: str) -> dict:
        """Parse a Java `.properties` file into a dict.

        Handles backslash-escaped colons and equals signs that Trino
        writes when persisting dynamic catalogs.

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

    def _write_databag(self):
        """Write database and requested-secrets for relations missing them."""
        for relation in self.charm.model.relations[self.relation_name]:
            app_databag = relation.data.get(self.charm.app, {})
            if app_databag.get("database"):
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

    def _find_config_for_relation(self, relation) -> Optional[dict]:
        """Find the postgresql-catalog-config entry matching a relation.

        Args:
            relation: The Juju relation.

        Returns:
            The config dict for this relation, or None.
        """
        raw = self.charm.config.get("postgresql-catalog-config")
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
        if not self._validate_config_entry(entry, relation.app.name):
            return None
        return entry

    def _validate_config_entry(self, entry, app_name) -> bool:
        """Validate a postgresql-catalog-config entry.

        Args:
            entry: The config dict to validate.
            app_name: The app name (for error messages).

        Returns:
            True if the entry is valid.
        """
        prefix = entry.get("database_prefix")
        if not prefix or not prefix.endswith("*"):
            logger.error(
                "Invalid or missing database_prefix for %s: must end with '*'",
                app_name,
            )
            return False

        if not entry.get("ro_catalog_name") and not entry.get(
            "rw_catalog_name"
        ):
            logger.error(
                "At least one of ro_catalog_name or rw_catalog_name "
                "must be set for %s",
                app_name,
            )
            return False

        return True

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

            databases = [
                d.strip() for d in pg.prefix_databases.split(",") if d.strip()
            ]
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

    def _load_relation_data(
        self, relation
    ) -> Optional[PostgresqlRelationModel]:
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
        except (pydantic.ValidationError, SecretNotFoundError):
            return None

    def _build_catalog_props(
        self, pg, database, config_entry, relation_id, target_server_type
    ):
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
        url = self._build_jdbc_url(
            pg, database, relation_id, target_server_type
        )

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
                self._import_tls_cert(relation_id, pg.tls_ca)
                params.append(f"sslrootcert={self.charm.truststore_abs_path}")
                params.append("sslrootcertpassword=${ENV:JAVA_TRUSTSTORE_PWD}")
        else:
            params.append("ssl=false")

        url = f"jdbc:postgresql://{pg.all_endpoints}/{database}"
        return f"{url}?{'&'.join(params)}"

    def _import_tls_cert(self, relation_id, tls_ca):
        """Import TLS CA certificate into the Java truststore.

        Args:
            relation_id: The relation ID (used as cert alias).
            tls_ca: The CA certificate content.
        """
        container = self.charm.unit.get_container(self.charm.name)
        if not container.can_connect():
            return

        truststore_pwd = self.charm.state.java_truststore_pwd
        if not truststore_pwd:
            return

        alias = f"pg-relation-{relation_id}"
        try:
            add_cert_to_truststore(
                container,
                alias,
                tls_ca,
                truststore_pwd,
                str(self.charm.conf_abs_path),
            )
        except Exception as e:
            logger.error(
                "Failed to import TLS cert for relation %s: %s", relation_id, e
            )

    def _is_trino_reachable(self) -> bool:
        """Check if Trino is reachable via HTTP.

        Returns:
            True if Trino responds to a health check.
        """
        try:
            resp = requests.get("http://localhost:8080/v1/info", timeout=5.0)
            return resp.status_code == 200
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
        secret_id = self.charm.state.user_secret_id
        if secret_id:
            try:
                credentials = yaml.safe_load(
                    self.charm._get_secret_content(secret_id)["users"]
                )
                return next(iter(credentials))
            except Exception:
                logger.error("Could not read user secret, using default creds")
        return next(iter(DEFAULT_CREDENTIALS))

    def _execute_sql(self, sql):
        """Execute SQL via the Trino HTTP API.

        Args:
            sql: The SQL statement to execute.

        Raises:
            RuntimeError: If the SQL execution fails.
        """
        user = self._get_trino_user()
        headers = {"X-Trino-User": user}

        resp = requests.post(
            "http://localhost:8080/v1/statement",
            data=sql,
            headers=headers,
            timeout=30.0,
        )
        resp.raise_for_status()
        data = resp.json()

        # Follow nextUri until completion
        while "nextUri" in data:
            resp = requests.get(data["nextUri"], headers=headers, timeout=30.0)
            resp.raise_for_status()
            data = resp.json()
            if "error" in data:
                raise RuntimeError(
                    f"Trino SQL error: {data['error'].get('message', data['error'])}"
                )

    def _create_catalog(self, name, properties):
        """Create a Trino catalog via SQL.

        Args:
            name: The catalog name.
            properties: Dict of catalog properties.
        """
        sql = self._build_catalog_sql(name, properties)
        try:
            self._execute_sql(sql)
            logger.info("Created catalog %r", name)
        except Exception as e:
            logger.error("Failed to create catalog %r: %s", name, e)

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
            f"\"{k}\" = '{v}'" for k, v in properties.items()
        )
        return (
            f'CREATE CATALOG "{name}" USING postgresql\n'
            f"WITH (\n  {props_sql}\n)"
        )

    def _drop_catalog(self, name):
        """Drop a Trino catalog via SQL.

        Args:
            name: The catalog name to drop.
        """
        try:
            self._execute_sql(f'DROP CATALOG IF EXISTS "{name}"')
            logger.info("Dropped catalog %r", name)
        except Exception as e:
            logger.error("Failed to drop catalog %r: %s", name, e)

    @staticmethod
    def _hash_properties(properties) -> str:
        """Create a stable hash of catalog properties for change detection.

        Args:
            properties: Dict of catalog properties.

        Returns:
            Hex digest string.
        """
        serialized = json.dumps(properties, sort_keys=True)
        return hashlib.sha256(serialized.encode()).hexdigest()
