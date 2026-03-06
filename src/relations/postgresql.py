# Copyright 2025 Canonical Ltd.
# See LICENSE file for licensing details.

"""PostgreSQL relation handler."""

import hashlib
import json
import logging
from typing import Callable, Optional

import httpx
import pydantic
import yaml
from ops import framework
from ops.model import SecretNotFoundError

from literals import DEFAULT_CREDENTIALS, POSTGRESQL_RELATION_NAME
from log import log_event_handler
from utils import add_cert_to_truststore

REQUESTED_SECRETS = ["username", "password", "tls", "tls-ca"]
PASS_ENV_VAR_PREFIX = "PG_PASS_"

logger = logging.getLogger(__name__)


def _env_var_name(database: str) -> str:
    """Derive the pebble environment variable name for a database password.

    Args:
        database: The database name.

    Returns:
        Environment variable name, e.g. ``PG_PASS_MYDB``.
    """
    return PASS_ENV_VAR_PREFIX + database.upper().replace("-", "_")


class PostgresRelationModel(pydantic.BaseModel):
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


class PostgresqlRelationHandler(framework.Object):
    """Handler for PostgreSQL relations via the postgresql_client interface."""

    def __init__(self, charm, relation_name=POSTGRESQL_RELATION_NAME):
        """Construct.

        Args:
            charm: The charm to attach the hooks to.
            relation_name: The name of the relation.
        """
        super().__init__(charm, relation_name)
        self.charm = charm
        self.relation_name = relation_name

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
        self.reconcile()

    @log_event_handler(logger)
    def _on_relation_changed(self, event):
        """Handle relation changed: trigger reconciliation.

        Args:
            event: The relation changed event.
        """
        self.reconcile()

    @log_event_handler(logger)
    def _on_relation_broken(self, event):
        """Handle relation broken: trigger reconciliation.

        Args:
            event: The relation broken event.
        """
        self.reconcile()

    def reconcile(self):
        """Reconcile wanted vs tracked catalog state via CREATE/DROP CATALOG.

        Compares what catalogs should exist (from relations and config) against
        what was previously created (tracked in peer data), then issues
        CREATE/DROP CATALOG SQL to correct any discrepancies.

        Before issuing SQL statements, ensures pebble env vars carry the current
        passwords so that ``${ENV:…}`` references in CREATE CATALOG resolve
        correctly.  Only restarts Trino when an env var is missing or changed.
        """
        if not self.charm.unit.is_leader():
            return

        self._write_databag()

        wanted_catalogs, wanted_envs = self._compute_wanted_catalogs()
        tracked_catalogs = self.charm.state.relation_catalogs or {}

        # Ensure password env vars are set before any SQL that references them
        if wanted_envs and self._sync_password_env_vars(wanted_envs):
            logger.info(
                "Pebble replanned to update Postgres password env vars"
            )

        if not self._is_trino_reachable():
            logger.error("Trino not reachable, skipping reconciliation")
            return

        # DROP catalogs that should no longer exist
        for name in set(tracked_catalogs) - set(wanted_catalogs):
            logger.info("Dropping relation catalog %r", name)
            self._drop_catalog(name)

        # CREATE or UPDATE catalogs
        for name, props in wanted_catalogs.items():
            props_hash = self._hash_properties(props)
            tracked_hash = tracked_catalogs.get(name)
            if tracked_hash == props_hash:
                continue
            if name in tracked_catalogs:
                logger.info("Updating relation catalog %r", name)
                self._drop_catalog(name)
            else:
                logger.info("Creating relation catalog %r", name)
            self._create_catalog(name, props)

        # Update tracked state with property hashes
        self.charm.state.relation_catalogs = {
            name: self._hash_properties(props)
            for name, props in wanted_catalogs.items()
        }

    def get_postgresql_relation_catalogs(self) -> list:
        """Return tracked catalog names managed by this handler.

        Returns:
            List of catalog name strings.
        """
        tracked_catalogs = self.charm.state.relation_catalogs or {}
        return list(tracked_catalogs.keys())

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
            return None
        config = yaml.safe_load(raw)
        if not isinstance(config, dict):
            return None
        entry = config.get(relation.app.name)
        if entry is None:
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

        if not entry.get("ro_catalog_name"):
            logger.error("Missing ro_catalog_name for %s", app_name)
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

            # RO catalog (mandatory)
            ro_name = config_entry["ro_catalog_name"]
            catalogs[ro_name] = self._build_catalog_props(
                pg, database, config_entry, relation.id, "preferSecondary"
            )

            # RW catalog (optional)
            rw_name = config_entry.get("rw_catalog_name")
            if rw_name:
                catalogs[rw_name] = self._build_catalog_props(
                    pg, database, config_entry, relation.id, "primary"
                )

        return catalogs, env_vars

    def _load_relation_data(self, relation) -> Optional[PostgresRelationModel]:
        """Load and validate relation data from the provider's databag.

        Args:
            relation: The Juju relation to load data from.

        Returns:
            A PostgresRelationModel if data is available, None otherwise.
        """
        if relation.app is None:
            return None
        try:
            return relation.load(
                PostgresRelationModel,
                relation.app,
                decoder=PostgresRelationModel.decode(self.charm),
            )
        except (pydantic.ValidationError, SecretNotFoundError):
            return None

    def _build_catalog_props(
        self, pg, database, config_entry, relation_id, target_server_type
    ):
        """Build catalog properties dict for a single catalog.

        Args:
            pg: The PostgresRelationModel with connection data.
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
            pg: The PostgresRelationModel with connection data.
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

    def _sync_password_env_vars(self, needed: dict[str, str]) -> bool:
        """Ensure pebble env vars contain the required passwords.

        Reads the current pebble plan, checks for missing or changed env vars,
        and replans (restarts Trino) only when there is a difference.
        Stale env vars from removed catalogs are ignored (lazy cleanup).

        Args:
            needed: Mapping of env var name to password value.

        Returns:
            True if pebble was replanned.
        """
        container = self.charm.unit.get_container(self.charm.name)
        if not container.can_connect():
            return False

        plan = container.get_plan()
        services = plan.to_dict().get("services", {})
        current_env = services.get(self.charm.name, {}).get("environment", {})

        updates = {k: v for k, v in needed.items() if current_env.get(k) != v}
        if not updates:
            return False

        logger.info("Updating %d password env var(s) in pebble", len(updates))
        new_env = dict(current_env)
        new_env.update(updates)
        container.add_layer(
            "pg-passwords",
            {
                "services": {
                    self.charm.name: {
                        "override": "merge",
                        "environment": new_env,
                    }
                }
            },
            combine=True,
        )
        container.replan()
        return True

    def _is_trino_reachable(self) -> bool:
        """Check if Trino is reachable via HTTP.

        Returns:
            True if Trino responds to a health check.
        """
        try:
            resp = httpx.get("http://localhost:8080/v1/info", timeout=5.0)
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

        resp = httpx.post(
            "http://localhost:8080/v1/statement",
            content=sql,
            headers=headers,
            timeout=30.0,
        )
        resp.raise_for_status()
        data = resp.json()

        # Follow nextUri until completion
        while "nextUri" in data:
            resp = httpx.get(data["nextUri"], headers=headers, timeout=30.0)
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
        props_sql = ",\n  ".join(
            f"\"{k}\" = '{v}'" for k, v in properties.items()
        )
        sql = (
            f'CREATE CATALOG "{name}" USING postgresql\n'
            f"WITH (\n  {props_sql}\n)"
        )
        try:
            self._execute_sql(sql)
            logger.info("Created catalog %r", name)
        except Exception as e:
            logger.error("Failed to create catalog %r: %s", name, e)

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
