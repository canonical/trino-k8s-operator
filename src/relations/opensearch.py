# Copyright 2023 Canonical Ltd.
# See LICENSE file for licensing details.

"""Defines opensearch relation event handling methods."""

import logging
import re

from charms.data_platform_libs.v0.data_interfaces import IndexCreatedEvent
from ops import framework
from ops.model import BlockedStatus, WaitingStatus
from ops.pebble import ExecError

from literals import JAVA_HOME
from log import log_event_handler

logger = logging.getLogger(__name__)


class OpensearchRelationHandler(framework.Object):
    """Client for ranger:postgresql relations.

    Attributes:
        INDEX_NAME: the opensearch index name.
        CERTIFICATE_NAME: the name of the opensearch certificate
    """

    INDEX_NAME = "ranger_audits"
    CERTIFICATE_NAME = "opensearch-ca"

    def __init__(self, charm, relation_name="opensearch"):
        """Construct.

        Args:
            charm: The charm to attach the hooks to.
            relation_name: The name of the relation.
        """
        self.relation_name = relation_name
        self.charm = charm

        super().__init__(charm, self.relation_name)
        self.framework.observe(
            self.charm.opensearch_relation.on.index_created,
            self._on_index_created,
        )
        self.framework.observe(
            self.charm.on[relation_name].relation_broken,
            self._on_relation_broken,
        )

    @log_event_handler(logger)
    def _on_index_created(self, event: IndexCreatedEvent) -> None:
        """Handle openserach relation changed events.

        Args:
            event: The event triggered when the relation changed.
        """
        if not self.charm.unit.is_leader():
            return

        if not self.charm.state.ranger_enabled:
            self.charm.unit.status = BlockedStatus(
                "A Ranger relation is required to use OpenSearch."
            )
            return

        if not self.charm.config["charm-function"] == "coordinator":
            self.charm.unit.status = BlockedStatus(
                "Only Trino coordinator can relate to Opensearch"
            )
            return

        self.charm.unit.status = WaitingStatus(
            f"handling {self.relation_name} change"
        )
        self.update(event)

    @log_event_handler(logger)
    def _on_relation_broken(self, event) -> None:
        """Handle broken relations with opensearch.

        Args:
            event: The event triggered when the relation changed.
        """
        if not self.charm.config["charm-function"] == "coordinator":
            return

        if not self.charm.state.ranger_enabled:
            return

        if self.charm.unit.is_leader():
            self.update(event, True)

    def update_certificates(self, relation_broken=False) -> None:
        """Add/remove the Opensearch certificate in the Java truststore.

        Args:
            relation_broken: If the event is a relation broken event.
        """
        container = self.charm.unit.get_container(self.charm.name)
        if not container.can_connect():
            return

        certificate = self.charm.state.opensearch_certificate
        truststore_pwd = "changeit"

        if not relation_broken and certificate:
            container.push("/opensearch.crt", certificate)
            command = [
                f"{JAVA_HOME}/bin/keytool",
                "-importcert",
                "-keystore",
                f"{JAVA_HOME}/lib/security/cacerts",
                "-file",
                "/opensearch.crt",
                "-alias",
                self.CERTIFICATE_NAME,
                "-storepass",
                truststore_pwd,
                "--no-prompt",
            ]
        else:
            container.remove_path("/opensearch.crt")
            command = [
                f"{JAVA_HOME}/bin/keytool",
                "-delete",
                "-keystore",
                f"{JAVA_HOME}/lib/security/cacerts",
                "-alias",
                self.CERTIFICATE_NAME,
                "-storepass",
                truststore_pwd,
            ]
        try:
            container.exec(command).wait()
        except ExecError as e:
            if e.stdout and "already exists" in e.stdout:
                return
            logger.error(e.stdout)

    def get_secret_content(self, secret_id) -> dict:
        """Get the content of a juju secret by id.

        Args:
            secret_id: The Juju secret ID.

        Returns:
            content: The content of the secret.
        """
        secret = self.model.get_secret(id=secret_id)
        content = secret.get_content(refresh=True)
        return content

    def get_cert_value(self, event) -> None:
        """Get certificate from opensearch secret.

        Args:
            event: The index created event.
        """
        event_data = event.relation.data[event.app]
        secret_id = event_data.get("secret-tls")
        content = self.get_secret_content(secret_id)
        tls_ca = content["tls-ca"]
        pattern = r"-----BEGIN CERTIFICATE-----.*?-----END CERTIFICATE-----"
        certificates_list = re.findall(pattern, tls_ca, re.DOTALL)
        self.charm.state.opensearch_certificate = certificates_list[1]

    def get_conn_values(self, event) -> dict:
        """Get the connection values from the relation to Opensearch.

        Args:
            event: The event triggered by index created or relation broken events.

        Returns:
            A dictionary of connection values.
        """
        event_data = event.relation.data[event.app]
        secret_id = event_data.get("secret-user")
        user_credentials = self.get_secret_content(secret_id)

        host, port = event_data.get("endpoints").split(",", 1)[0].split(":")
        return {
            "index": OpensearchRelationHandler.INDEX_NAME,
            "host": host,
            "port": port,
            "password": user_credentials["password"],
            "username": user_credentials["username"],
            "is_enabled": True,
        }

    def update(self, event, relation_broken=False) -> None:
        """Assign nested value in peer relation.

        Args:
            event: The event triggered when the relation changed.
            relation_broken: true if opensearch connection is broken.
        """
        env = {"is_enabled": False}
        if not relation_broken:
            env = self.get_conn_values(event)
            self.get_cert_value(event)
        self.update_certificates(relation_broken)
        self.charm.state.opensearch = env

        self.charm.policy.restart_ranger_plugin(event)
