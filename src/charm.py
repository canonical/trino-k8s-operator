#!/usr/bin/env python3
# Copyright 2023 Canonical Ltd.
# See LICENSE file for licensing details.

"""Charm the service.

Refer to the following post for a quick-start guide that will help you
develop a new k8s charm using the Operator Framework:

https://discourse.charmhub.io/t/4208
"""

# pylint: disable=too-many-lines

import json
import logging
import socket
import subprocess  # nosec B404
from pathlib import Path

import yaml
from charms.comsys_libs.v0.kubernetes_statefulset_patch import (
    KubernetesStatefulsetPatch,
)
from charms.data_platform_libs.v0.data_interfaces import OpenSearchRequires
from charms.data_platform_libs.v0.data_models import TypedCharmBase
from charms.grafana_k8s.v0.grafana_dashboard import GrafanaDashboardProvider
from charms.loki_k8s.v1.loki_push_api import LogForwarder
from charms.prometheus_k8s.v0.prometheus_scrape import MetricsEndpointProvider
from charms.traefik_k8s.v2.ingress import IngressPerAppRequirer
from ops.charm import CollectStatusEvent, PebbleReadyEvent
from ops.main import main
from ops.model import (
    ActiveStatus,
    BlockedStatus,
    MaintenanceStatus,
    ModelError,
    SecretNotFoundError,
    WaitingStatus,
)
from ops.pebble import CheckStatus, ExecError, PathError
from pydantic import ValidationError

from catalog_manager import BigqueryCatalog, GsheetCatalog, HiveCatalog
from config import CharmConfig
from literals import (
    CACERTS_MANIFEST,
    CACERTS_PATH,
    CATALOG_DIR,
    CATALOG_SCHEMA,
    CERTIFICATE_NAME,
    CONF_DIR,
    CONFIG_FILES,
    DEFAULT_CREDENTIALS,
    DEFAULT_JVM_OPTIONS,
    INDEX_NAME,
    INGRESS_RELATION_NAME,
    INT_COMMS_SECRET_LABEL,
    JAVA_HOME,
    JMX_PORT,
    METRICS_PORT,
    OPENSEARCH_RELATION_NAME,
    PASSWORD_DB,
    PEER_RELATION_NAME,
    POLICY_RELATION_NAME,
    POSTGRESQL_RELATION_NAME,
    RUN_TRINO_COMMAND,
    TRINO_CATALOG_RELATION_NAME,
    TRINO_COORDINATOR_RELATION_NAME,
    TRINO_HOME,
    TRINO_PORTS,
    TRINO_WORKER_RELATION_NAME,
    TRUSTSTORE_MANIFEST,
    TRUSTSTORE_SECRET_LABEL,
)
from log import log_event_handler
from relations.opensearch import OpensearchRelationHandler
from relations.policy import PolicyRelationHandler
from relations.postgresql_catalog import PostgresqlCatalogRelationHandler
from relations.trino_catalog import TrinoCatalogRelationHandler
from relations.trino_coordinator import TrinoCoordinator
from relations.trino_worker import TrinoWorker
from sql_catalog import RedshiftCatalog, SqlCatalog
from state import State
from utils import (
    add_users_to_password_db,
    content_hash,
    generate_password,
    reconcile_truststore,
    render,
    truststore_manifest_hash,
    update_opts,
    validate_keys,
)

# Log messages can be retrieved using juju debug-log
logger = logging.getLogger(__name__)


def _format_config_error(err: ValidationError) -> str:
    """Return the first validation error message for use in BlockedStatus."""
    errors = err.errors()
    if not errors:
        return str(err)
    return errors[0]["msg"]


class TrinoK8SCharm(TypedCharmBase[CharmConfig]):
    """Charm the service.

    Attrs:
        state: Used to store data that is persisted across invocations.
        trino_abs_path: The absolute path for Trino home directory.
        catalog_abs_path: The absolute path for the catalog directory.
        conf_abs_path: The absolute path for the conf directory.
        truststore_abs_path: The absolute path for the truststore.
    """

    config_type = CharmConfig

    @property
    def _cluster_local_coordinator_uri(self):
        """Return the cluster-local service URI for the Trino coordinator."""
        host = self.app.name
        namespace = self.model.name
        port = TRINO_PORTS["HTTP"]
        return f"http://{host}.{namespace}.svc.cluster.local:{port}"

    @property
    def _coordinator_discovery_uri(self):
        """Return the effective discovery URI advertised to Trino workers.

        Uses the operator-supplied `discovery-uri` config value when set,
        so that deployments where workers cannot reach the coordinator's
        in-cluster service DNS (e.g. cross-cluster topologies) can supply a
        reachable override.  Falls back to the cluster-local Kubernetes service
        FQDN for same-cluster deployments, which requires no manual
        configuration.
        """
        return self.config.discovery_uri or self._cluster_local_coordinator_uri

    @property
    def trino_abs_path(self):
        """Return the catalog absolute path."""
        return Path(TRINO_HOME)

    @property
    def catalog_abs_path(self):
        """Return the catalog absolute path."""
        return self.trino_abs_path.joinpath(CATALOG_DIR)

    @property
    def conf_abs_path(self):
        """Return the catalog absolute path."""
        return self.trino_abs_path.joinpath(CONF_DIR)

    @property
    def truststore_abs_path(self):
        """Return the truststore absolute path."""
        return self.conf_abs_path.joinpath("truststore.jks")

    def __init__(self, *args):
        """Construct.

        Args:
            args: Ignore.
        """
        super().__init__(*args)
        self.name = "trino"
        self.state = State(self.app, lambda: self.model.get_relation(PEER_RELATION_NAME))
        self.policy = PolicyRelationHandler(self)
        self.trino_coordinator = TrinoCoordinator(self)
        self.trino_worker = TrinoWorker(self)
        self.trino_catalog = TrinoCatalogRelationHandler(self)

        # Every hook converges through a single idempotent reconciler; unit
        # status is derived separately by collect-unit-status.
        self.framework.observe(self.on.trino_pebble_ready, self._on_pebble_ready)
        self.framework.observe(self.on.config_changed, self._reconcile_hook)
        self.framework.observe(self.on.update_status, self._reconcile_hook)
        self.framework.observe(self.on.peer_relation_changed, self._reconcile_hook)
        self.framework.observe(self.on.secret_changed, self._reconcile_hook)
        self.framework.observe(self.on.collect_unit_status, self._on_collect_unit_status)

        # Actions
        self.framework.observe(self.on.restart_action, self._on_restart)
        self.framework.observe(self.on.list_system_users_action, self._on_list_system_users)

        # Relation events are centralized here; handlers expose logic methods
        # invoked by the reconciler rather than observing events themselves.
        for endpoint in (
            TRINO_COORDINATOR_RELATION_NAME,
            TRINO_WORKER_RELATION_NAME,
            POLICY_RELATION_NAME,
            POSTGRESQL_RELATION_NAME,
        ):
            self.framework.observe(self.on[endpoint].relation_created, self._reconcile_hook)
            self.framework.observe(self.on[endpoint].relation_changed, self._reconcile_hook)
            self.framework.observe(self.on[endpoint].relation_broken, self._reconcile_hook)
        self.framework.observe(
            self.on[TRINO_CATALOG_RELATION_NAME].relation_created, self._reconcile_hook
        )
        self.framework.observe(
            self.on[TRINO_CATALOG_RELATION_NAME].relation_broken,
            self._on_trino_catalog_relation_broken,
        )
        self.framework.observe(
            self.on[OPENSEARCH_RELATION_NAME].relation_broken, self._reconcile_hook
        )

        # Handle Ingress
        self.ingress = IngressPerAppRequirer(
            self,
            relation_name=INGRESS_RELATION_NAME,
            port=TRINO_PORTS["HTTP"],
            scheme="http",
            strip_prefix=True,
            redirect_https=True,
        )
        self.framework.observe(self.ingress.on.ready, self._reconcile_hook)
        self.framework.observe(self.ingress.on.revoked, self._reconcile_hook)

        # Prometheus
        self._prometheus_scraping = MetricsEndpointProvider(
            self,
            relation_name="metrics-endpoint",
            jobs=[
                {"static_configs": [{"targets": [f"{socket.getfqdn()}:{METRICS_PORT}"]}]},
            ],
            refresh_event=self.on.config_changed,
        )

        # Loki
        self.log_proxy = LogForwarder(self, relation_name="logging")

        # Grafana
        self._grafana_dashboards = GrafanaDashboardProvider(
            self, relation_name="grafana-dashboard"
        )

        self.opensearch_relation = OpenSearchRequires(
            self,
            relation_name=OPENSEARCH_RELATION_NAME,
            index=INDEX_NAME,
            extra_user_roles="admin",
        )
        self.opensearch_relation_handler = OpensearchRelationHandler(self)
        self.postgresql_catalog_handler = PostgresqlCatalogRelationHandler(self)
        self.framework.observe(self.opensearch_relation.on.index_created, self._reconcile_hook)
        self.framework.observe(self.opensearch_relation.on.endpoints_changed, self._reconcile_hook)
        self.framework.observe(
            self.opensearch_relation.on.authentication_updated, self._reconcile_hook
        )

        raw = self.model.config
        resources = {
            "memory": {
                "requests": raw.get("workload-memory-requests"),
                "limits": raw.get("workload-memory-limits"),
            },
            "cpu": {
                "requests": raw.get("workload-cpu-requests"),
                "limits": raw.get("workload-cpu-limits"),
            },
        }
        resources = {k: v for k, v in resources.items() if v is not None}

        self.k8s_resources = KubernetesStatefulsetPatch(
            self,
            resource_updates={self.name: resources},
            refresh_event=[self.on.trino_pebble_ready, self.on.config_changed],
        )

    def _reconcile_hook(self, event):
        """Route any observed hook through the single reconciler.

        Args:
            event: The triggering Juju event (unused; state is read from the model).
        """
        self._reconcile()

    def _warn_deprecated_config(self):
        """Log deprecation warnings if obsolete config options are still set."""
        cfg = self.model.config
        if cfg.get("external-hostname"):
            logger.warning(
                "Config option 'external-hostname' is deprecated and has no effect. "
                "The ingress hostname is now managed by the ingress provider charm."
            )
        if cfg.get("tls-secret-name"):
            logger.warning(
                "Config option 'tls-secret-name' is deprecated and has no effect. "
                "TLS is now managed by the ingress provider charm."
            )

    @log_event_handler(logger)
    def _on_pebble_ready(self, event: PebbleReadyEvent):
        """Record the workload version, then reconcile.

        Args:
            event: The pebble-ready event.
        """
        container = self.unit.get_container(self.name)
        if container.can_connect():
            try:
                meta_file = container.pull("/rockcraft.yaml")
                meta = yaml.safe_load(meta_file)
                if meta and "version" in meta:
                    self.unit.set_workload_version(meta["version"])
            except (PathError, yaml.YAMLError) as e:
                logger.debug("Could not get workload version: %s", str(e))

        self._reconcile()

    @log_event_handler(logger)
    def _on_trino_catalog_relation_broken(self, event):
        """Remove the departing relation's secret, then reconcile.

        Args:
            event: The trino-catalog relation-broken event.
        """
        self.trino_catalog.remove_relation_secret(event.relation.id)
        self._reconcile()

    def _on_collect_unit_status(self, event: CollectStatusEvent):  # noqa: C901
        """Derive terminal unit status from the current model and workload health.

        Args:
            event: The collect-unit-status event to add derived statuses to.
        """
        try:
            cfg = self.config
        except ValidationError as err:
            event.add_status(BlockedStatus(_format_config_error(err)))
            return

        if not self.state.is_ready():
            event.add_status(WaitingStatus("waiting for peer relation"))
            return

        container = self.unit.get_container(self.name)
        if not container.can_connect():
            event.add_status(WaitingStatus("waiting for container"))
            return

        try:
            self._validate_relations()
        except (RuntimeError, ValueError) as err:
            event.add_status(BlockedStatus(str(err)))
            return

        try:
            self._resolve_oidc_credentials()
        except ValueError as err:
            event.add_status(BlockedStatus(str(err)))
            return

        if cfg.charm_function in ("coordinator", "all"):
            if self._get_int_comms_secret_value() is None:
                event.add_status(
                    WaitingStatus("waiting for leader to create internal communication secret")
                )
                return

        if cfg.charm_function == "worker" and self.model.relations[TRINO_WORKER_RELATION_NAME]:
            if self._get_int_comms_secret_value() is None:
                event.add_status(
                    WaitingStatus(
                        "waiting for coordinator to publish internal communication secret"
                    )
                )
                return

        if cfg.charm_function in ("coordinator", "all"):
            try:
                check = container.get_check("up")
            except ModelError:
                event.add_status(MaintenanceStatus("waiting for workload"))
                return
            if check.status != CheckStatus.UP:
                event.add_status(MaintenanceStatus("Status check: DOWN"))
                return

        event.add_status(ActiveStatus("Status check: UP"))

    def _restart_trino(self):
        """Restart Trino."""
        container = self.unit.get_container(self.name)
        if not container.can_connect():
            return

        container.restart(self.name)

    @log_event_handler(logger)
    def _on_restart(self, event):
        """Restart Trino, action handler.

        Args:
            event:The event triggered by the restart action
        """
        self.unit.status = MaintenanceStatus("restarting trino")
        self._restart_trino()

        event.set_results({"result": "trino successfully restarted"})

    @log_event_handler(logger)
    def _on_list_system_users(self, event):
        """List all Trino users, action handler.

        Args:
            event: The event triggered by the list-system-users action.
        """
        secret_id = self.state.user_secret_id
        if secret_id:
            try:
                content = self._get_secret_content(secret_id)
                configured_users = list(yaml.safe_load(content["users"]).keys())
            except Exception:
                configured_users = ["(error reading secret)"]
        else:
            configured_users = list(DEFAULT_CREDENTIALS.keys())

        relation_creds = self.trino_catalog.get_relation_credentials()

        event.set_results(
            {
                "configured-users": ", ".join(configured_users),
                "relation-users": (
                    ", ".join(relation_creds.keys()) if relation_creds else "(none)"
                ),
            }
        )

    def _ensure_truststore_password(self):
        """Return the stable truststore password backed by an app Juju secret.

        The leader creates the secret once, migrating any legacy peer-stored
        value, so the password no longer rotates on every reconcile and the
        catalog property files that embed it stay stable.

        Returns:
            The truststore password, or None when a non-leader unit cannot yet
            read the leader-created secret.
        """
        try:
            secret = self.model.get_secret(label=TRUSTSTORE_SECRET_LABEL)
            return secret.get_content(refresh=True).get("password")
        except SecretNotFoundError:
            pass

        if not self.unit.is_leader():
            return None

        # Reuse any legacy peer-stored value so the truststore password stays
        # compatible across charm revisions during upgrade.
        password = self.state.java_truststore_pwd or generate_password()
        self.app.add_secret({"password": password}, label=TRUSTSTORE_SECRET_LABEL)
        return password

    def set_java_truststore_password(self, truststore_pwd):
        """Set the JVM `cacerts` password to the stable truststore password.

        Idempotent: re-running once the password is already applied fails with
        an "incorrect password" error from keytool, which is ignored.

        Args:
            truststore_pwd: The stable truststore password.
        """
        container = self.unit.get_container(self.name)
        if not container.can_connect():
            return

        command = [
            "keytool",
            "-storepass",
            "changeit",
            "-storepasswd",
            "-new",
            truststore_pwd,
            "-keystore",
            f"{JAVA_HOME}/{CACERTS_PATH}",
        ]
        try:
            container.exec(command).wait_output()
        except (subprocess.CalledProcessError, ExecError) as e:
            if e.stderr and "password was incorrect" in e.stderr:
                return
            if e.stderr and "Warning" in e.stderr:
                return
            logger.debug(f"Unable to update truststore password {e.stderr}")

    def _get_secret_content(self, secret_id):
        """Get the content of a Juju secret.

        Args:
            secret_id: the juju secret id.

        Returns:
            content: the content of the secret.

        Raises:
            SecretNotFoundError: in case the secret cannot be found.
        """
        try:
            secret = self.model.get_secret(id=secret_id)
            content = secret.get_content(refresh=True)
        except SecretNotFoundError:
            logger.error(f"secret {secret_id!r} not found.")
            raise
        return content

    def _resolve_oidc_credentials(self) -> tuple[str | None, str | None]:
        """Resolve Google OIDC credentials from the configured Juju secret.

        Returns:
            A (client_id, client_secret) tuple, or (None, None) when
            oidc-secret-id is unset (OAuth2 disabled).

        Raises:
            ValueError: if oidc-secret-id is set but the secret cannot be
                resolved or is missing the required keys.
        """
        secret_id = self.config.oidc_secret_id
        if not secret_id:
            return None, None
        try:
            content = self._get_secret_content(secret_id)
        except SecretNotFoundError:
            raise ValueError(
                f"oidc-secret-id {secret_id!r} could not be resolved; ensure the "
                "secret exists and is granted to this application"
            ) from None
        try:
            return content["google-client-id"], content["google-client-secret"]
        except KeyError:
            raise ValueError(
                "oidc secret must contain 'google-client-id' and 'google-client-secret' keys"
            ) from None

    def _compute_credentials(self):
        """Return the full set of authentication credentials for `password.db`.

        Merges the configured system users (or defaults) with per-relation
        trino-catalog users.

        Returns:
            Mapping of username to plaintext password.

        Raises:
            ValueError: if the configured user secret is malformed.
        """
        secret_id = self.state.user_secret_id
        if secret_id:
            try:
                credentials = yaml.safe_load(self._get_secret_content(secret_id)["users"])
            except (yaml.YAMLError, KeyError, SecretNotFoundError) as e:
                logger.error(f"Error reading secret {secret_id!r}: {e}")
                raise ValueError(f"invalid user secret {secret_id!r}") from e
        else:
            credentials = dict(DEFAULT_CREDENTIALS)

        credentials.update(self.trino_catalog.get_relation_credentials())
        return credentials

    def _write_password_db(self, container, credentials):
        """Write the authentication database file.

        Args:
            container: The Trino container.
            credentials: Mapping of username to plaintext password.
        """
        db_path = str(self.trino_abs_path.joinpath(PASSWORD_DB))
        add_users_to_password_db(container, credentials, db_path)

    def _configure_catalogs(self, container, truststore_pwd):
        """Render static catalog property files and collect their hashes and certs.

        Args:
            container: The Trino container.
            truststore_pwd: The stable truststore password embedded in catalog
                property files.

        Returns:
            A tuple of (per-file content hashes for the Pebble plan, desired
            truststore certificates keyed by alias).
        """
        catalog_index = yaml.safe_load(self.state.catalog_config or "")
        if catalog_index is None:
            catalog_index = {
                "catalogs": {},
                "backends": {},
            }
        catalogs, backends = (
            catalog_index["catalogs"],
            catalog_index["backends"],
        )

        file_hashes = {}
        desired_certs = {}
        upserted_catalogs = []
        for name, info in catalogs.items():
            validate_keys(info, CATALOG_SCHEMA)
            backend = backends[info["backend"]]
            catalog_instance = self._create_catalog_instance(truststore_pwd, name, info, backend)
            batch = catalog_instance.configure_catalogs()
            upserted_catalogs.extend(batch)
            for key, content in catalog_instance.rendered.items():
                file_hashes[self._hash_key(f"catalog_{key}.properties")] = content_hash(content)
            desired_certs.update(catalog_instance.desired_certs)

        # Remove obsolete catalog files that are neither config- nor relation-managed.
        if container.isdir(self.catalog_abs_path):
            pg_catalogs = self.postgresql_catalog_handler.get_postgresql_relation_catalogs()
            for file in container.list_files(self.catalog_abs_path, pattern="*.properties"):
                stem = Path(file.name).stem
                if stem in upserted_catalogs or stem in pg_catalogs:
                    continue
                try:
                    container.remove_path(file.path)
                except PathError as e:
                    logging.debug(
                        "Could not remove obsolete catalog file '%s': %s",
                        file.name,
                        str(e),
                    )

        return file_hashes, desired_certs

    def _create_catalog_instance(self, truststore_pwd, name, info, backend):
        """Create catalog instances based on connector type.

        Args:
            truststore_pwd: the truststore password.
            name: the catalog name.
            info: the catalog specific information.
            backend: the backend template for configuration.

        Returns:
            the appropriate connector class.

        Raises:
            ValueError: in case the backend type is not supported.
        """
        catalog_map = {
            "postgresql": SqlCatalog,
            "mysql": SqlCatalog,
            "redshift": RedshiftCatalog,
            "bigquery": BigqueryCatalog,
            "gsheets": GsheetCatalog,
            "hive": HiveCatalog,
        }
        catalog_cls = catalog_map.get(backend["connector"], None)

        if catalog_cls is not None:
            return catalog_cls(self, truststore_pwd, name, info, backend)

        raise ValueError(f"Unsupported backend: {backend}")

    def _hash_key(self, filename):
        """Return the Pebble-plan env var name that carries a file's content hash.

        Args:
            filename: The managed file name.

        Returns:
            The `HASH_<FILE>` env var name.
        """
        return f"HASH_{filename.upper().replace('.', '_').replace('-', '_')}"

    def _configure_trino(self, container, env):
        """Render and push the core Trino config files.

        Args:
            container: The Trino container.
            env: Environment variables for Jinja templating.

        Returns:
            Mapping of per-file content-hash env vars for the Pebble plan.
        """
        hashes = {}
        for template, file in CONFIG_FILES.items():
            path = self.trino_abs_path.joinpath(file)
            content = render(template, env)
            container.push(path, content, make_dirs=True, permissions=0o644)
            hashes[self._hash_key(file)] = content_hash(content)
        return hashes

    def _configure_file_based_manager(
        self,
        container,
        env,
        manager_config,
        template_name,
        properties_filename,
        config_filename,
    ):
        """Configure a file-based Trino manager from charm config.

        Args:
            container: The Trino container.
            env: Environment variables for Jinja templating.
            manager_config: The raw JSON configuration string (or None/empty).
            template_name: The template used to render the manager properties.
            properties_filename: The properties file written under
                `TRINO_HOME`.
            config_filename: The JSON config file written under `TRINO_HOME`.

        Returns:
            Mapping of per-file content-hash env vars for the Pebble plan.
        """
        properties_path = self.trino_abs_path.joinpath(properties_filename)
        config_path = self.trino_abs_path.joinpath(config_filename)

        if manager_config:
            properties_content = render(template_name, env)
            container.push(
                properties_path,
                properties_content,
                make_dirs=True,
                permissions=0o644,
            )
            container.push(
                config_path,
                manager_config,
                make_dirs=True,
                permissions=0o644,
            )
            logger.info("%s configuration applied", properties_filename)
            return {
                self._hash_key(properties_filename): content_hash(properties_content),
                self._hash_key(config_filename): content_hash(manager_config),
            }

        try:
            container.remove_path(properties_path)
            logger.info("%s properties removed", properties_filename)
        except PathError:
            pass

        try:
            container.remove_path(config_path)
            logger.info("%s configuration removed", config_filename)
        except PathError:
            pass

        return {}

    def _configure_resource_groups(self, container, env):
        """Configure resource groups if provided.

        Args:
            container: The Trino container.
            env: Environment variables containing resource groups config.

        Returns:
            Mapping of per-file content-hash env vars for the Pebble plan.
        """
        return self._configure_file_based_manager(
            container=container,
            env=env,
            manager_config=self.config.resource_groups_config,
            template_name="resource-groups.jinja",
            properties_filename="resource-groups.properties",
            config_filename="resource-groups.json",
        )

    def _configure_session_property_manager(self, container, env):
        """Configure the session property manager if provided.

        Args:
            container: The Trino container.
            env: Environment variables containing session property manager
                config.

        Returns:
            Mapping of per-file content-hash env vars for the Pebble plan.
        """
        return self._configure_file_based_manager(
            container=container,
            env=env,
            manager_config=self.config.session_property_manager_config,
            template_name="session-property-config.jinja",
            properties_filename="session-property-config.properties",
            config_filename="session-property-config.json",
        )

    def _validate_relations(self):
        """Validate that required relations are valid and ready.

        Raises:
            ValueError: in case of invalid configuration.
        """
        if not self.state.is_ready():
            raise ValueError("peer relation not ready")

        if self.config.charm_function == "worker":
            self.trino_worker._validate()

        if self.config.charm_function == "coordinator":
            self.trino_coordinator._validate()

    def _get_int_comms_secret_value(self) -> str | None:
        """Return the internal communication shared secret value.

        For coordinator/all: creates or retrieves the singleton app-owned Juju secret.
        For worker: resolves the secret by the ID stored in peer state (written by
        the worker gathering coordinator data after the secret has been published).

        Returns:
            The shared secret string, or None if not yet available.
        """
        cfg = self.config
        if cfg.charm_function in ("coordinator", "all"):
            if self.unit.is_leader():
                secret = self.trino_coordinator._get_or_create_int_comms_secret()
            else:
                try:
                    secret = self.model.get_secret(label=INT_COMMS_SECRET_LABEL)
                except SecretNotFoundError:
                    return None
            if secret is None:
                return None
            return secret.get_content(refresh=True).get("secret")
        elif cfg.charm_function == "worker":
            secret_id = self.state.int_comms_secret_id
            if not secret_id:
                return None
            return self.trino_worker._resolve_int_comms_secret(secret_id)
        return None

    def _build_base_environment(self, truststore_pwd, int_comms_secret):
        """Build the Trino service environment without per-file hash triggers.

        Args:
            truststore_pwd: The stable truststore password.
            int_comms_secret: The internal communication shared secret value.

        Returns:
            env: a dictionary of trino environment variables.
        """
        cfg = self.config
        db_path = self.trino_abs_path.joinpath(PASSWORD_DB)
        default_opts = " ".join(DEFAULT_JVM_OPTIONS)
        user_opts = cfg.additional_jvm_options

        jvm_opts = update_opts(default_opts, user_opts) if user_opts else default_opts

        oauth_client_id, oauth_client_secret = self._resolve_oidc_credentials()

        env = {
            "LOG_LEVEL": cfg.log_level,
            "OAUTH_CLIENT_ID": oauth_client_id,
            "OAUTH_CLIENT_SECRET": oauth_client_secret,
            "OAUTH_USER_MAPPING": cfg.oauth_user_mapping,
            "WEB_PROXY": cfg.web_proxy,
            "CHARM_FUNCTION": cfg.charm_function,
            "DISCOVERY_URI": self.state.discovery_uri or self._coordinator_discovery_uri,
            "APPLICATION_NAME": self.app.name,
            "PASSWORD_DB_PATH": str(db_path),
            "TRINO_HOME": str(self.trino_abs_path),
            "METRICS_PORT": METRICS_PORT,
            "JMX_PORT": JMX_PORT,
            "RANGER_RELATION": self.state.ranger_enabled or False,
            "ACL_ACCESS_MODE": cfg.acl_mode_default,
            "ACL_USER_PATTERN": cfg.acl_user_pattern,
            "ACL_CATALOG_PATTERN": cfg.acl_catalog_pattern,
            "JAVA_TRUSTSTORE_PWD": truststore_pwd,
            "INT_COMMS_SECRET": int_comms_secret,
            "USER_SECRET_ID": cfg.user_secret_id,
            "JVM_OPTIONS": jvm_opts,
            "COORDINATOR_REQUEST_TIMEOUT": cfg.coordinator_request_timeout,
            "COORDINATOR_CONNECT_TIMEOUT": cfg.coordinator_connect_timeout,
            "WORKER_REQUEST_TIMEOUT": cfg.worker_request_timeout,
            "MAX_CONCURRENT_QUERIES": cfg.max_concurrent_queries,
            "QUERY_MAX_CPU_TIME": cfg.query_max_cpu_time,
            "QUERY_MAX_RUN_TIME": cfg.query_max_run_time,
            "QUERY_MAX_MEMORY_PER_NODE": cfg.query_max_memory_per_node,
            "QUERY_MAX_MEMORY": cfg.query_max_memory,
            "QUERY_MAX_TOTAL_MEMORY": cfg.query_max_total_memory,
            "MEMORY_HEAP_HEADROOM_PER_NODE": cfg.memory_heap_headroom_per_node,
            "RESOURCE_GROUPS_CONFIG": cfg.resource_groups_config,
            "SESSION_PROPERTY_MANAGER_CONFIG": cfg.session_property_manager_config,
        }

        # Merge PostgreSQL password env vars (derived at runtime)
        if cfg.charm_function in ("coordinator", "all"):
            pg_secrets = self.postgresql_catalog_handler.get_postgresql_env_vars()
        elif cfg.charm_function == "worker":
            pg_secrets = self.trino_worker.get_postgresql_secrets_from_coordinator()
        else:
            pg_secrets = {}
        if pg_secrets:
            env.update(pg_secrets)

        return env

    def _reconcile_relation_state(self):
        """Persist Ranger and OpenSearch state derived from related applications."""
        if not self.unit.is_leader():
            return

        self.policy.publish_service_data()
        policy_url = self.policy.read_policy_manager_url()
        self.state.policy_manager_url = policy_url or ""
        self.state.ranger_enabled = bool(policy_url)

        conn = self.opensearch_relation_handler.gather_connection()
        if conn.get("is_enabled"):
            self.state.opensearch = conn
            self.state.opensearch_certificate = (
                self.opensearch_relation_handler.gather_certificate() or ""
            )
        else:
            self.state.opensearch = {"is_enabled": False}
            self.state.opensearch_certificate = ""

    def _reconcile_truststores(self, container, truststore_pwd, conf_certs):
        """Reconcile both truststores in place and return their plan hashes.

        Args:
            container: The Trino container.
            truststore_pwd: The stable truststore password.
            conf_certs: Desired {alias: PEM} certificates for `conf/truststore.jks`.

        Returns:
            Mapping of truststore manifest-hash env vars for the Pebble plan.
        """
        reconcile_truststore(
            container,
            str(self.truststore_abs_path),
            truststore_pwd,
            conf_certs,
            str(self.conf_abs_path),
            str(self.conf_abs_path.joinpath(TRUSTSTORE_MANIFEST)),
        )

        cacerts_certs = {}
        opensearch = self.state.opensearch or {}
        certificate = self.state.opensearch_certificate
        if opensearch.get("is_enabled") and certificate:
            cacerts_certs[CERTIFICATE_NAME] = certificate

        reconcile_truststore(
            container,
            f"{JAVA_HOME}/{CACERTS_PATH}",
            truststore_pwd,
            cacerts_certs,
            str(self.conf_abs_path),
            str(self.conf_abs_path.joinpath(CACERTS_MANIFEST)),
        )

        return {
            "HASH_TRUSTSTORE_JKS": truststore_manifest_hash(conf_certs),
            "HASH_CACERTS": truststore_manifest_hash(cacerts_certs),
        }

    def _clean_catalog_dir(self, container):
        """Remove the catalog directory when no worker relation is present.

        Args:
            container: The Trino container.
        """
        try:
            container.remove_path(self.catalog_abs_path, recursive=True)
        except PathError:
            pass

    def _pebble_layer(self, env, is_coordinator):
        """Build the Trino Pebble layer.

        Args:
            env: The service environment (including per-file hash triggers).
            is_coordinator: Whether this unit runs the coordinator role.

        Returns:
            The Pebble layer definition.
        """
        layer = {
            "summary": "trino layer",
            "description": "pebble config layer for trino",
            "services": {
                self.name: {
                    "override": "replace",
                    "summary": "trino server",
                    "command": RUN_TRINO_COMMAND,
                    "startup": "enabled",
                    "environment": env,
                    "on-check-failure": {"up": "restart"},
                }
            },
        }
        if is_coordinator:
            layer["checks"] = {
                "up": {
                    "override": "replace",
                    "period": "30s",
                    "http": {"url": "http://localhost:8080/"},
                }
            }
        return layer

    def _reconcile(self):  # noqa: C901
        """Converge the Trino workload to the desired state read from the model.

        Idempotent: applies configuration, reconciles truststores in place, and
        lets Pebble decide restarts via per-file content hashes in the plan.
        Guards return early without deferring; convergence resumes on the next
        hook and terminal status is reported by collect-unit-status.
        """
        container = self.unit.get_container(self.name)
        if not container.can_connect():
            return

        try:
            cfg = self.config
        except ValidationError:
            return

        if not self.state.is_ready():
            return

        function = cfg.charm_function
        is_coordinator = function in ("coordinator", "all")

        # A worker with no coordinator relation has nothing to serve; drop any
        # stale catalogs and stop. This runs before relation validation, which
        # treats a missing coordinator relation as a blocking error.
        if function == "worker" and not self.model.relations[TRINO_WORKER_RELATION_NAME]:
            self._clean_catalog_dir(container)
            return

        try:
            self._validate_relations()
        except (RuntimeError, ValueError):
            return

        self._warn_deprecated_config()

        # Gather desired state from the model.
        if is_coordinator:
            self.state.discovery_uri = self._coordinator_discovery_uri
            self.state.catalog_config = cfg.catalog_config or ""
            self.state.user_secret_id = cfg.user_secret_id or ""
        if function == "worker":
            self.trino_worker.gather_from_coordinator()

        int_comms_secret = self._get_int_comms_secret_value()
        if is_coordinator and int_comms_secret is None:
            return
        if (
            function == "worker"
            and self.model.relations[TRINO_WORKER_RELATION_NAME]
            and int_comms_secret is None
        ):
            return

        # A misconfigured OIDC secret must not converge with broken auth; the
        # blocking status is reported by collect-unit-status.
        try:
            self._resolve_oidc_credentials()
        except ValueError:
            return

        truststore_pwd = self._ensure_truststore_password()
        if truststore_pwd is None:
            return
        self.set_java_truststore_password(truststore_pwd)

        if is_coordinator:
            self._reconcile_relation_state()

        # Per-relation users must exist before password.db is rebuilt.
        self.trino_catalog.reconcile_trino_catalog_relations()

        # Static catalog files and truststore contents.
        file_hashes, conf_certs = self._configure_catalogs(container, truststore_pwd)
        conf_certs.update(self.postgresql_catalog_handler.get_desired_tls_certs())
        file_hashes.update(self._reconcile_truststores(container, truststore_pwd, conf_certs))

        # Render and push managed config files, hashing each for the plan.
        env = self._build_base_environment(truststore_pwd, int_comms_secret)
        file_hashes.update(self._configure_trino(container, env))
        file_hashes.update(self._configure_resource_groups(container, env))
        file_hashes.update(self._configure_session_property_manager(container, env))

        try:
            credentials = self._compute_credentials()
        except ValueError as err:
            logger.error(err)
            return
        self._write_password_db(container, credentials)
        file_hashes["HASH_PASSWORD_DB"] = content_hash(json.dumps(credentials, sort_keys=True))

        if is_coordinator and self.state.ranger_enabled:
            rendered = self.policy._configure_ranger_plugin(container)
            for name, content in (rendered or {}).items():
                file_hashes[self._hash_key(name)] = content_hash(content)

        env.update(file_hashes)

        if is_coordinator:
            self.model.unit.open_port(port=8080, protocol="tcp")
        else:
            self.model.unit.close_port(port=8080, protocol="tcp")

        container.add_layer(self.name, self._pebble_layer(env, is_coordinator), combine=True)
        container.replan()

        # Publishing coordinator relation data is a pure databag write and is
        # always safe. reconcile_postgresql_catalogs writes its request databag
        # unconditionally and self-guards the live CREATE/DROP CATALOG SQL with a
        # reachability check, so calling it during a workload restart still lets
        # the provider start provisioning; the live SQL converges on a later hook.
        if is_coordinator:
            if self.unit.is_leader():
                self.trino_coordinator.update_coordinator_relation_data()
            self.postgresql_catalog_handler.reconcile_postgresql_catalogs()


if __name__ == "__main__":
    main(TrinoK8SCharm)
