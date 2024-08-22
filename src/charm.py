#!/usr/bin/env python3
# Copyright 2023 Canonical Ltd.
# See LICENSE file for licensing details.

"""Charm the service.

Refer to the following post for a quick-start guide that will help you
develop a new k8s charm using the Operator Framework:

https://discourse.charmhub.io/t/4208
"""

import logging
import subprocess  # nosec B404
from pathlib import Path

import yaml
from charms.data_platform_libs.v0.data_interfaces import OpenSearchRequires
from charms.grafana_k8s.v0.grafana_dashboard import GrafanaDashboardProvider
from charms.loki_k8s.v0.loki_push_api import LogProxyConsumer
from charms.nginx_ingress_integrator.v0.nginx_route import require_nginx_route
from charms.prometheus_k8s.v0.prometheus_scrape import MetricsEndpointProvider
from ops.charm import CharmBase, ConfigChangedEvent, PebbleReadyEvent
from ops.main import main
from ops.model import (
    ActiveStatus,
    BlockedStatus,
    MaintenanceStatus,
    SecretNotFoundError,
    WaitingStatus,
)
from ops.pebble import CheckStatus, ExecError

from literals import (
    CATALOG_DIR,
    CONF_DIR,
    CONFIG_FILES,
    DEFAULT_CREDENTIALS,
    INDEX_NAME,
    JAVA_HOME,
    JMX_PORT,
    LOG_FILES,
    METRICS_PORT,
    PASSWORD_DB,
    RUN_TRINO_COMMAND,
    TRINO_HOME,
    TRINO_PLUGIN_DIR,
    TRINO_PORTS,
)
from log import log_event_handler
from relations.opensearch import OpensearchRelationHandler
from relations.policy import PolicyRelationHandler
from relations.trino_coordinator import TrinoCoordinator
from relations.trino_worker import TrinoWorker
from state import State
from utils import (
    add_cert_to_truststore,
    add_users_to_password_db,
    create_cert_and_catalog_dicts,
    generate_password,
    render,
)

# Log messages can be retrieved using juju debug-log
logger = logging.getLogger(__name__)


class TrinoK8SCharm(CharmBase):
    """Charm the service.

    Attrs:
        state: Used to store data that is persisted across invocations.
        external_hostname: DNS listing used for external connections.
        trino_abs_path: The absolute path for Trino home directory.
        catalog_abs_path: The absolute path for the catalog directory.
        conf_abs_path: The absolute path for the conf directory.
        truststore_abs_path: The absolute path for the truststore.
    """

    @property
    def external_hostname(self):
        """Return the DNS listing used for external connections."""
        return self.config["external-hostname"] or self.app.name

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

        # Handle basic charm lifecycle
        self.framework.observe(self.on.install, self._on_install)
        self.framework.observe(
            self.on.trino_pebble_ready, self._on_pebble_ready
        )
        self.framework.observe(self.on.config_changed, self._on_config_changed)
        self.framework.observe(self.on.restart_action, self._on_restart)
        self.framework.observe(self.on.update_status, self._on_update_status)
        self.framework.observe(
            self.on.peer_relation_changed, self._on_peer_relation_changed
        )
        self.framework.observe(self.on.secret_changed, self._on_secret_changed)

        # Handle Ingress
        self._require_nginx_route()

        # Prometheus
        self._prometheus_scraping = MetricsEndpointProvider(
            self,
            relation_name="metrics-endpoint",
            jobs=[{"static_configs": [{"targets": [f"*:{METRICS_PORT}"]}]}],
            refresh_event=self.on.config_changed,
        )

        # Loki
        self.log_proxy = LogProxyConsumer(
            self, log_files=LOG_FILES, relation_name="log-proxy"
        )

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

    def _require_nginx_route(self):
        """Require nginx-route relation based on current configuration."""
        require_nginx_route(
            charm=self,
            service_hostname=self.external_hostname,
            service_name=self.app.name,
            service_port=TRINO_PORTS["HTTP"],
            tls_secret_name=self.config["tls-secret-name"],
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
        self._update(event)
        self.trino_coordinator._update_coordinator_relation_data(event)

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
            self._validate_relations()
        except ValueError as err:
            self.unit.status = BlockedStatus(str(err))
            return

        if self.config["charm-function"] in ["coordinator", "all"]:
            check = container.get_check("up")
            if check.status != CheckStatus.UP:
                self.unit.status = MaintenanceStatus("Status check: DOWN")
                self._restart_trino()
                return

        self.unit.status = ActiveStatus("Status check: UP")

    def _validate_pebble_plan(self, container):
        """Validate Superset pebble plan.

        Args:
            container: application container

        Returns:
            bool of pebble plan validity
        """
        try:
            plan = container.get_plan().to_dict()
            return bool(
                plan
                and plan["services"].get(self.name, {}).get("on-check-failure")
            )
        except ConnectionError:
            return False

    @log_event_handler(logger)
    def _on_secret_changed(self, event):
        """Handle secret changed hook.

        Args:
            event: the secret changed event.
        """
        try:
            self._update_password_db(event)
        except Exception:
            self.unit.status = BlockedStatus(
                "Secret cannot be found or is incorrectly formatted."
            )
            return
        self._restart_trino()

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

    def set_java_truststore_password(self, event):
        """Update the truststore password to the randomly generated one.

        Args:
            event: The event triggered on relation changed.
        """
        container = self.unit.get_container(self.name)
        if not container.can_connect():
            return

        if self.unit.is_leader():
            self.state.java_truststore_pwd = (
                self.state.java_truststore_pwd or generate_password()
            )

        command = [
            f"{JAVA_HOME}/bin/keytool",
            "-storepass",
            "changeit",
            "-storepasswd",
            "-new",
            self.state.java_truststore_pwd,
            "-keystore",
            f"{JAVA_HOME}/lib/security/cacerts",
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
            event.defer()
            return

        secret_id = self.config.get("user-secret-id")
        db_path = str(self.trino_abs_path.joinpath(PASSWORD_DB))

        if secret_id:
            try:
                credentials = yaml.safe_load(
                    self._get_secret_content(secret_id)["users"]
                )
            except Exception as e:
                logger.error(f"Error reading secret {secret_id!r}: {e}")
                raise
        else:
            credentials = DEFAULT_CREDENTIALS

        add_users_to_password_db(container, credentials, db_path)

    def _add_catalogs(self, container, catalogs, truststore_pwd):
        """Add catalogs to Trino.

        Args:
            container: The application container.
            catalogs: Dictionary of catalog configurations.
            truststore_pwd: The truststore password.
        """
        for name, config in catalogs.items():
            # Inject truststore path and password.
            config = config.replace(
                "{SSL_PATH}", str(self.truststore_abs_path)
            ).replace("{SSL_PWD}", truststore_pwd)

            # Add catalog.
            container.push(
                self.catalog_abs_path.joinpath(f"{name}.properties"),
                config,
                make_dirs=True,
            )

    def _add_certs(self, container, certs, truststore_pwd):
        """Prepare and add certificates to Trino truststore.

        Args:
            container: The application container.
            certs: Dictionary of certificate configurations.
            truststore_pwd: The truststore password.
        """
        for name, cert in certs.items():
            container.push(
                self.conf_abs_path.joinpath(f"{name}.crt"),
                cert,
                make_dirs=True,
            )
            try:
                add_cert_to_truststore(
                    container,
                    name,
                    cert,
                    truststore_pwd,
                    str(self.conf_abs_path),
                )
                container.remove_path(
                    self.conf_abs_path.joinpath(f"{name}.crt")
                )
            except Exception as e:
                logger.error(f"Failed to add {name} cert: {e}")

    def _configure_catalogs(self, container):
        """Manage catalog properties files.

        Args:
            container: The application container.
        """
        catalog_config = self.state.catalog_config
        truststore_pwd = generate_password()

        # Remove existing catalogs and certs.
        for path in [self.catalog_abs_path, self.conf_abs_path]:
            if container.exists(path):
                container.remove_path(path, recursive=True)

        # Add catalogs from state
        if not catalog_config:
            return
        certs, catalogs = create_cert_and_catalog_dicts(catalog_config)
        self._add_catalogs(container, catalogs, truststore_pwd)

        # Add certs from state
        if not certs:
            return
        self._add_certs(container, certs, truststore_pwd)

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

    def _validate_config_params(self):
        """Validate that configuration is valid.

        Raises:
            ValueError: in case of invalid log configuration.
                        in case of web-proxy as empty string.
                        in case of invalid acl-mode-default value.
            ScannerError: in case of incorrectly formatted catalog-config.
        """
        valid_log_levels = ["info", "debug", "warn", "error"]

        log_level = self.model.config["log-level"].lower()
        if log_level not in valid_log_levels:
            raise ValueError(f"config: invalid log level {log_level!r}")

        web_proxy = self.config.get("web-proxy")
        if web_proxy and not web_proxy.strip():
            raise ValueError("Web-proxy value cannot be an empty string")

        acl_mode_default = self.config.get("acl-mode-default")
        if acl_mode_default not in ["all", "none"]:
            raise ValueError(
                f"Invalid acl-mode-default value: {acl_mode_default!r}"
            )

        catalogs = self.config.get("catalog-config")
        if catalogs:
            try:
                yaml.safe_load(catalogs)
            except yaml.ScannerError as e:
                raise yaml.ScannerError(
                    f"Incorrectly formatted catalog-config: {e}"
                )

    def _validate_relations(self):
        """Validate that required relations are valid and ready.

        Raises:
            ValueError: in case of invalid configuration.
        """
        if not self.state.is_ready():
            raise ValueError("peer relation not ready")

        if self.config["charm-function"] == "worker":
            self.trino_worker._validate()

        if self.config["charm-function"] == "coordinator":
            self.trino_coordinator._validate()

    def _create_environment(self):
        """Create application environment.

        Returns:
            env: a dictionary of trino environment variables.
        """
        db_path = self.trino_abs_path.joinpath(PASSWORD_DB)
        env = {
            "LOG_LEVEL": self.config["log-level"],
            "OAUTH_CLIENT_ID": self.config.get("google-client-id"),
            "OAUTH_CLIENT_SECRET": self.config.get("google-client-secret"),
            "OAUTH_USER_MAPPING": self.config.get("oauth-user-mapping"),
            "WEB_PROXY": self.config.get("web-proxy"),
            "CHARM_FUNCTION": self.config["charm-function"],
            "DISCOVERY_URI": self.state.discovery_uri
            or self.config["discovery-uri"],
            "APPLICATION_NAME": self.app.name,
            "PASSWORD_DB_PATH": str(db_path),
            "TRINO_HOME": str(self.trino_abs_path),
            "CATALOG_CONFIG": self.state.catalog_config
            or self.config.get("catalog-config"),
            "METRICS_PORT": METRICS_PORT,
            "JMX_PORT": JMX_PORT,
            "RANGER_RELATION": self.state.ranger_enabled or False,
            "ACL_ACCESS_MODE": self.config["acl-mode-default"],
            "ACL_USER_PATTERN": self.config["acl-user-pattern"],
            "ACL_CATALOG_PATTERN": self.config["acl-catalog-pattern"],
            "JAVA_TRUSTSTORE_PWD": self.state.java_truststore_pwd,
            "USER_SECRET_ID": self.config.get("user-secret-id"),
        }
        return env

    def _update(self, event):
        """Update the Trino server configuration and replan its execution.

        Args:
            event: The event triggered when the relation changed.
        """
        container = self.unit.get_container(self.name)
        if not container.can_connect():
            event.defer()
            return

        try:
            self._validate_config_params()
            self._validate_relations()
        except (RuntimeError, ValueError) as err:
            self.unit.status = BlockedStatus(str(err))
            return

        logger.info("configuring trino")
        if self.config["charm-function"] in ["coordinator", "all"]:
            self.state.discovery_uri = self.config.get("discovery-uri", "")
            self.state.catalog_config = self.config.get("catalog-config", "")

        self._configure_catalogs(container)

        self.set_java_truststore_password(event)
        env = self._create_environment()
        self._configure_trino(container, env)

        try:
            self._update_password_db(event)
        except Exception as err:
            logger.error(err)
            self.unit.status = BlockedStatus(
                "Secret cannot be found or is incorrectly formatted."
            )
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
        if self.config["charm-function"] in ["coordinator", "all"]:
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
            if self.state.ranger_enabled and not container.exists(
                f"{TRINO_PLUGIN_DIR}/ranger"
            ):
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
