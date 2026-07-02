#!/usr/bin/env python3
# Copyright 2023 Canonical Ltd.
# See LICENSE file for licensing details.

"""Charm the service.

Refer to the following post for a quick-start guide that will help you
develop a new k8s charm using the Operator Framework:

https://discourse.charmhub.io/t/4208
"""

# pylint: disable=too-many-lines

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
from charms.nginx_ingress_integrator.v0.nginx_route import require_nginx_route
from charms.prometheus_k8s.v0.prometheus_scrape import MetricsEndpointProvider
from ops.charm import ConfigChangedEvent, PebbleReadyEvent
from ops.main import main
from ops.model import (
    ActiveStatus,
    BlockedStatus,
    MaintenanceStatus,
    SecretNotFoundError,
    WaitingStatus,
)
from ops.pebble import CheckStatus, ExecError, PathError
from pydantic import ValidationError

from catalog_manager import BigqueryCatalog, GsheetCatalog, HiveCatalog
from config import CharmConfig
from literals import (
    CATALOG_DIR,
    CATALOG_SCHEMA,
    CONF_DIR,
    CONFIG_FILES,
    DEFAULT_CREDENTIALS,
    DEFAULT_JVM_OPTIONS,
    INDEX_NAME,
    INT_COMMS_SECRET_LABEL,
    JMX_PORT,
    METRICS_PORT,
    PASSWORD_DB,
    RUN_TRINO_COMMAND,
    TRINO_HOME,
    TRINO_PORTS,
    USER_SECRET_LABEL,
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
    generate_password,
    render,
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
        external_hostname: DNS listing used for external connections.
        trino_abs_path: The absolute path for Trino home directory.
        catalog_abs_path: The absolute path for the catalog directory.
        conf_abs_path: The absolute path for the conf directory.
        truststore_abs_path: The absolute path for the truststore.
    """

    config_type = CharmConfig

    @property
    def external_hostname(self):
        """Return the DNS listing used for external connections."""
        return self.config.external_hostname or self.app.name

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
        self.state = State(self.app, lambda: self.model.get_relation("peer"))
        self.policy = PolicyRelationHandler(self)
        self.trino_coordinator = TrinoCoordinator(self)
        self.trino_worker = TrinoWorker(self)
        self.trino_catalog = TrinoCatalogRelationHandler(self)

        # Handle basic charm lifecycle
        self.framework.observe(self.on.install, self._on_install)
        self.framework.observe(self.on.trino_pebble_ready, self._on_pebble_ready)
        self.framework.observe(self.on.config_changed, self._on_config_changed)
        self.framework.observe(self.on.restart_action, self._on_restart)
        self.framework.observe(self.on.list_system_users_action, self._on_list_system_users)
        self.framework.observe(self.on.update_status, self._on_update_status)
        self.framework.observe(self.on.peer_relation_changed, self._on_peer_relation_changed)
        self.framework.observe(self.on.secret_changed, self._on_secret_changed)

        # Handle Ingress
        self._require_nginx_route()

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
            relation_name="opensearch",
            index=INDEX_NAME,
            extra_user_roles="admin",
        )
        self.opensearch_relation_handler = OpensearchRelationHandler(self)
        self.postgresql_catalog_handler = PostgresqlCatalogRelationHandler(self)

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

    def _require_nginx_route(self):
        """Require nginx-route relation based on current configuration."""
        # Use raw model config here for bootstrap-safety: this method is called
        # from __init__ before any hook handler runs, so a persisted invalid
        # config must not prevent the charm from starting.
        raw = self.model.config
        require_nginx_route(
            charm=self,
            service_hostname=raw.get("external-hostname") or self.app.name,
            service_name=self.app.name,
            service_port=TRINO_PORTS["HTTP"],
            tls_secret_name=raw.get("tls-secret-name"),
            backend_protocol="HTTP",
        )

    @log_event_handler(logger)
    def _on_install(self, event):
        """Install Trino.

        Args:
            event: The event triggered when the relation changed.
        """
        self.unit.status = MaintenanceStatus(f"{self.name} unit provisioned.")

    @log_event_handler(logger)
    def _on_pebble_ready(self, event: PebbleReadyEvent):
        """Define and start a workload using the Pebble API.

        Args:
            event: The event triggered when the relation changed.
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

        self._update(event)

    @log_event_handler(logger)
    def _on_peer_relation_changed(self, event):
        """Handle peer relation changes.

        Args:
            event: The event triggered when the peer relation changed.
        """
        if self.unit.is_leader():
            return

        self.unit.status = WaitingStatus("configuring trino")
        self._update(event)

    @log_event_handler(logger)
    def _on_config_changed(self, event: ConfigChangedEvent):
        """Handle changed configuration.

        Args:
            event: The event triggered when the relation changed.
        """
        self.unit.status = WaitingStatus("configuring trino")
        try:
            _ = self.config  # validate upfront before any side-effects
        except ValidationError as err:
            self.unit.status = BlockedStatus(_format_config_error(err))
            return
        self.trino_coordinator._update_coordinator_relation_data(event)
        self._update(event)
        self.postgresql_catalog_handler.reconcile_postgresql_catalogs(event)
        self.trino_catalog.reconcile_trino_catalog_relations()

    @log_event_handler(logger)
    def _on_update_status(self, event):
        """Handle `update-status` events.

        Args:
            event: The `update-status` event triggered at intervals
        """
        container = self.unit.get_container(self.name)

        if not self.state.is_ready():
            return

        if not container.can_connect():
            return

        valid_pebble_plan = self._validate_pebble_plan(container)
        if not valid_pebble_plan:
            self._update(event)
            return

        try:
            cfg = self.config
            self._validate_relations()
        except ValidationError as err:
            self.unit.status = BlockedStatus(_format_config_error(err))
            return
        except (RuntimeError, ValueError) as err:
            self.unit.status = BlockedStatus(str(err))
            return

        if cfg.charm_function in ("coordinator", "all"):
            check = container.get_check("up")
            if check.status != CheckStatus.UP:
                self.unit.status = MaintenanceStatus("Status check: DOWN")
                self._restart_trino()
                return

        self.postgresql_catalog_handler.reconcile_postgresql_catalogs(event)
        self.trino_catalog.reconcile_trino_catalog_relations()

        self.unit.status = ActiveStatus("Status check: UP")

    def _validate_pebble_plan(self, container):
        """Validate pebble plan.

        Args:
            container: application container

        Returns:
            bool of pebble plan validity
        """
        try:
            plan = container.get_plan().to_dict()
            return bool(plan and plan["services"].get(self.name, {}).get("on-check-failure"))
        except ConnectionError:
            return False

    @log_event_handler(logger)
    def _on_secret_changed(self, event):
        """Handle secret changed hook.

        Args:
            event: the secret changed event.
        """
        # Catalog credential changes would enter the branch
        if not event.secret.label == USER_SECRET_LABEL:
            self._configure_catalogs(event)
            self.postgresql_catalog_handler.reconcile_postgresql_catalogs(event)
            self.trino_catalog.reconcile_trino_catalog_relations()
            self._restart_trino()
            return

        try:
            self._update_password_db(event)
            self._restart_trino()
        except Exception:
            self.unit.status = BlockedStatus("Secret cannot be found or is incorrectly formatted.")

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

    def set_java_truststore_password(self, event):
        """Update the truststore password to the randomly generated one.

        Args:
            event: The event triggered on relation changed.
        """
        container = self.unit.get_container(self.name)
        if not container.can_connect():
            return

        if self.unit.is_leader():
            self.state.java_truststore_pwd = self.state.java_truststore_pwd or generate_password()

        out, _ = container.exec(["/bin/sh", "-c", "echo $JAVA_HOME"]).wait_output()
        java_home = out.strip()
        command = [
            "keytool",
            "-storepass",
            "changeit",
            "-storepasswd",
            "-new",
            self.state.java_truststore_pwd,
            "-keystore",
            f"{java_home}/lib/security/cacerts",
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

    def _update_password_db(self, event):
        """Create necessary db file for authentication.

        Args:
            event: The pebble ready or config changed event.

        Raises:
            ScannerError: In case the secret is incorrectly formatted.
        """
        container = self.unit.get_container(self.name)
        if not container.can_connect():
            return

        secret_id = self.state.user_secret_id
        db_path = str(self.trino_abs_path.joinpath(PASSWORD_DB))

        if secret_id:
            try:
                credentials = yaml.safe_load(self._get_secret_content(secret_id)["users"])
            except Exception as e:
                logger.error(f"Error reading secret {secret_id!r}: {e}")
                raise
        else:
            credentials = dict(DEFAULT_CREDENTIALS)

        # Merge per-relation users from trino-catalog relations
        relation_creds = self.trino_catalog.get_relation_credentials()
        credentials.update(relation_creds)

        add_users_to_password_db(container, credentials, db_path)

    def _update_password_db_and_restart(self):
        """Update password.db with current credentials and restart Trino.

        Called by the trino-catalog handler when per-relation users change.
        """
        container = self.unit.get_container(self.name)
        if not container.can_connect():
            return

        try:
            self._update_password_db(None)
        except Exception as err:
            logger.error("Failed to update password.db: %s", err)
            return

        self._restart_trino()

    def _configure_catalogs(self, event):
        """Manage catalog properties files.

        Args:
            event: The juju event.
        """
        container = self.unit.get_container(self.name)
        if not container.can_connect():
            event.defer()
            return

        truststore_pwd = generate_password()

        # Truststore password is changed with each invocation
        # so we need to delete it here and it will be regenerated
        # by the end of this method.
        try:
            container.remove_path(self.truststore_abs_path)
        except PathError as e:
            logging.debug("Could not remove truststore: %s", str(e))

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

        # Upsert current catalogs
        upserted_catalogs = []
        for name, info in catalogs.items():
            validate_keys(info, CATALOG_SCHEMA)
            backend = backends[info["backend"]]
            catalog_instance = self._create_catalog_instance(truststore_pwd, name, info, backend)
            batch = catalog_instance.configure_catalogs()
            upserted_catalogs.extend(batch)

        # Remove obsolete catalogs
        if not container.isdir(self.catalog_abs_path):
            return

        for file in container.list_files(self.catalog_abs_path, pattern="*.properties"):
            if (
                Path(file.name).stem in upserted_catalogs
                or Path(file.name).stem
                in self.postgresql_catalog_handler.get_postgresql_relation_catalogs()
            ):
                continue

            try:
                container.remove_path(file.path)
            except PathError as e:
                logging.debug(
                    "Could not remove obsolete catalog file '%s': %s",
                    file.name,
                    str(e),
                )

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

    def _configure_trino(self, container, env):
        """Add the files needed to configure Trino to the Trino home directory.

        Args:
            container: The Trino container.
            env: Environment variables for Jija templating.
        """
        for template, file in CONFIG_FILES.items():
            path = self.trino_abs_path.joinpath(file)
            content = render(template, env)
            container.push(path, content, make_dirs=True, permissions=0o644)

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
            return

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

    def _configure_resource_groups(self, container, env):
        """Configure resource groups if provided.

        Args:
            container: The Trino container.
            env: Environment variables containing resource groups config.
        """
        self._configure_file_based_manager(
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
        """
        self._configure_file_based_manager(
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
        _on_relation_changed after the coordinator has published it).

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

    def _create_environment(self):
        """Create application environment.

        Returns:
            env: a dictionary of trino environment variables.
        """
        cfg = self.config
        db_path = self.trino_abs_path.joinpath(PASSWORD_DB)
        default_opts = " ".join(DEFAULT_JVM_OPTIONS)
        user_opts = cfg.additional_jvm_options

        jvm_opts = update_opts(default_opts, user_opts) if user_opts else default_opts

        env = {
            "LOG_LEVEL": cfg.log_level,
            "OAUTH_CLIENT_ID": cfg.google_client_id,
            "OAUTH_CLIENT_SECRET": cfg.google_client_secret,
            "OAUTH_USER_MAPPING": cfg.oauth_user_mapping,
            "WEB_PROXY": cfg.web_proxy,
            "CHARM_FUNCTION": cfg.charm_function,
            "DISCOVERY_URI": self.state.discovery_uri or self._coordinator_discovery_uri,
            "APPLICATION_NAME": self.app.name,
            "PASSWORD_DB_PATH": str(db_path),
            "TRINO_HOME": str(self.trino_abs_path),
            "CATALOG_CONFIG": self.state.catalog_config or cfg.catalog_config,
            "METRICS_PORT": METRICS_PORT,
            "JMX_PORT": JMX_PORT,
            "RANGER_RELATION": self.state.ranger_enabled or False,
            "ACL_ACCESS_MODE": cfg.acl_mode_default,
            "ACL_USER_PATTERN": cfg.acl_user_pattern,
            "ACL_CATALOG_PATTERN": cfg.acl_catalog_pattern,
            "JAVA_TRUSTSTORE_PWD": self.state.java_truststore_pwd,
            "INT_COMMS_SECRET": self._get_int_comms_secret_value(),
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

    # TODO (mertalpt): Remove suppressing the too complex error in the upcoming refactor.
    def _update(self, event):  # noqa: C901
        """Update the Trino server configuration and replan its execution.

        Args:
            event: The event triggered when the relation changed.
        """
        container = self.unit.get_container(self.name)
        if not container.can_connect():
            event.defer()
            return

        try:
            cfg = self.config
        except ValidationError as err:
            self.unit.status = BlockedStatus(_format_config_error(err))
            return

        try:
            self._validate_relations()
        except (RuntimeError, ValueError) as err:
            self.unit.status = BlockedStatus(str(err))
            return

        if cfg.charm_function in ("coordinator", "all"):
            if self._get_int_comms_secret_value() is None:
                self.unit.status = WaitingStatus(
                    "waiting for leader to create internal communication secret"
                )
                return

        if cfg.charm_function == "worker" and self.model.relations["trino-worker"]:
            if self._get_int_comms_secret_value() is None:
                self.unit.status = WaitingStatus(
                    "waiting for coordinator to publish internal communication secret"
                )
                return

        logger.info("configuring trino")
        if cfg.charm_function in ("coordinator", "all"):
            self.state.discovery_uri = self._coordinator_discovery_uri
            self.state.catalog_config = cfg.catalog_config or ""
            self.state.user_secret_id = cfg.user_secret_id or ""

        self._configure_catalogs(event)

        self.set_java_truststore_password(event)
        env = self._create_environment()
        self._configure_trino(container, env)
        self._configure_resource_groups(container, env)
        self._configure_session_property_manager(container, env)

        try:
            self._update_password_db(event)
        except Exception as err:
            logger.error(err)
            self.unit.status = BlockedStatus("Secret cannot be found or is incorrectly formatted.")
            return

        logger.info("planning trino execution")
        pebble_layer = {
            "summary": "trino layer",
            "description": "pebble config layer for trino",
            "services": {
                self.name: {
                    "override": "replace",
                    "summary": "trino server",
                    "command": RUN_TRINO_COMMAND,
                    "startup": "enabled",
                    "environment": env,
                    "on-check-failure": {"up": "ignore"},
                }
            },
        }
        if cfg.charm_function in ("coordinator", "all"):
            pebble_layer.update(
                {
                    "checks": {
                        "up": {
                            "override": "replace",
                            "period": "30s",
                            "http": {"url": "http://localhost:8080/"},
                        }
                    }
                },
            )

            # Handle Ranger plugin
            if self.state.ranger_enabled:
                # No leadership check required as coordinator
                # and single node cannot scale.
                self.policy._configure_ranger_plugin(container)

            self.model.unit.open_port(port=8080, protocol="tcp")
        else:
            self.model.unit.close_port(port=8080, protocol="tcp")

        container.add_layer(self.name, pebble_layer, combine=True)
        container.replan()

        self.unit.status = MaintenanceStatus("replanning application")


if __name__ == "__main__":
    main(TrinoK8SCharm)
