# Copyright 2023 Canonical Ltd.
# See LICENSE file for licensing details.

"""Manager for handling Trino TLS configuration."""

import logging

from charms.tls_certificates_interface.v2.tls_certificates import (
    TLSCertificatesRequiresV2, generate_csr, generate_private_key)
from ops.framework import Object
from ops.model import BlockedStatus, WaitingStatus

from literals import CONF_PATH, TLS_RELATION
from log import log_event_handler
from utils import generate_password, push

logger = logging.getLogger(__name__)


class TrinoTLS(Object):
    """Handler for managing the client and unit TLS keys/certs."""

    def __init__(self, charm):
        super().__init__(charm, "tls")
        self.charm = charm
        self.cert_subject = "trino-k8s"
        self.certificates = TLSCertificatesRequiresV2(self.charm, TLS_RELATION)

        self.framework.observe(
            self.charm.on[TLS_RELATION].relation_created,
            self._on_tls_relation_created,
        )
        self.framework.observe(
            self.charm.on[TLS_RELATION].relation_joined,
            self._on_tls_relation_joined,
        )
        self.framework.observe(
            self.certificates.on.certificate_available,
            self._on_certificate_available,
        )
        self.framework.observe(
            self.charm.on[TLS_RELATION].relation_broken,
            self._tls_relation_broken,
        )

    @log_event_handler(logger)
    def _on_tls_relation_created(self, event):
        """Handle `certificates_relation_created` event.

        Args:
            event: The event triggered when the relation is created.
        """
        if not self.charm.unit.is_leader():
            return

        if not self.charm._state.is_ready():
            event.defer()
            return

    @log_event_handler(logger)
    def _on_tls_relation_joined(self, _):
        """Handle `certificates_relation_joined` event."""
        private_key = generate_private_key()
        self.charm._state.private_key = private_key.decode("utf-8")

        self._request_certificate()

    def _request_certificate(self):
        """Generate and submit CSR to provider."""
        csr = generate_csr(
            private_key=self.charm._state.private_key.encode("utf-8"),
            subject=self.cert_subject,
            sans_dns=["trino-k8s"],
        )
        self.charm._state.csr = csr.decode("utf-8")
        self.certificates.request_certificate_creation(
            certificate_signing_request=csr
        )

    @log_event_handler(logger)
    def _on_certificate_available(self, event):
        """Handle `certificates_available` event.

        Args:
            event: The event triggered when the certificate is available
        """
        if not self.charm._state.is_ready():
            self.charm.model.unit.status = WaitingStatus(
                "Waiting for peer relation to be created"
            )
            event.defer()
            return

        container = self.charm.model.unit.get_container(self.charm.name)
        if not container.can_connect():
            event.defer()
            return

        if not event.certificate:
            self.charm._state.tls = "disabled"
            self.charm.status = BlockedStatus("TLS relation is blocked")
            return

        self.charm._state.tls = "enabled"
        self.charm._state.certificate = event.certificate
        self.charm._state.ca = event.ca

        push(
            container, self.charm._state.private_key, f"{CONF_PATH}/server.key"
        )
        push(container, self.charm._state.ca, f"{CONF_PATH}/ca.pem")
        push(
            container, self.charm._state.certificate, f"{CONF_PATH}/server.crt"
        )
        self.set_truststore(container)
        self.set_keystore(container)

        self.charm._update(event)

    def set_truststore(self, container):
        """Add CA to JKS truststore.

        Args:
            container: Trino container
        """
        if not self.charm._state.truststore_password:
            truststore_password = generate_password()
            self.charm._state.truststore_password = truststore_password

        if container.exists(f"{CONF_PATH}/truststore.jks"):
            logging.info("truststore.jks already exists")
            return

        try:
            container.exec(
                [
                    "keytool",
                    "-import",
                    "-v",
                    "-alias",
                    "cert",
                    "-file",
                    "server.crt",
                    "-keystore",
                    "truststore.jks",
                    "-storepass",
                    f"{self.charm._state.truststore_password}",
                    "-noprompt",
                ],
                working_dir=CONF_PATH,
            ).wait_output()
            container.exec(
                ["chown", "trino:trino", f"{CONF_PATH}/truststore.jks"]
            )
            container.exec(["chmod", "770", f"{CONF_PATH}/truststore.jks"])
            logging.info("truststore created")
        except RuntimeError as err:
            logger.exception(err)
            return

    def set_keystore(self, container):
        """Create and add cert and private-key to keystore.

        Args:
            container: Trino container
        """
        if not self.charm._state.keystore_password:
            keystore_password = generate_password()
            self.charm._state.keystore_password = keystore_password

        if container.exists(f"{CONF_PATH}/keystore.p12"):
            logging.info("keystore.p12 already exists")
            return

        try:
            container.exec(
                ["yum", "install", "openssl", "--assumeyes"]
            ).wait_output()
            container.exec(
                [
                    "openssl",
                    "pkcs12",
                    "-export",
                    "-in",
                    "server.crt",
                    "-inkey",
                    "server.key",
                    "-passin",
                    f"pass:{self.charm._state.keystore_password}",
                    "-certfile",
                    "server.crt",
                    "-out",
                    "keystore.p12",
                    "-password",
                    f"pass:{self.charm._state.keystore_password}",
                ],
                working_dir=CONF_PATH,
            ).wait_output()
            container.exec(
                ["chown", "trino:trino", f"{CONF_PATH}/keystore.p12"]
            )
            container.exec(["chmod", "770", f"{CONF_PATH}/keystore.p12"])
            logging.info("keystore created")
        except RuntimeError as err:
            logger.exception(err)
            return

    @log_event_handler(logger)
    def _tls_relation_broken(self, event):
        """Handle relation broken event.

        Args:
            event: The event triggered when the relation is broken
        """
        if not self.charm.unit.is_leader():
            return

        if not self.charm._state.is_ready():
            self.charm.model.unit.status = WaitingStatus(
                "Waiting for peer relation to be created"
            )
            event.defer()
            return

        container = self.charm.model.unit.get_container(self.charm.name)
        if not container.can_connect():
            event.defer()
            return

        for file in [
            "server.key",
            "ca.pem",
            "server.crt",
            "truststore.jks",
            "keystore.p12",
        ]:
            if container.exists(f"{CONF_PATH}/{file}"):
                container.remove_path(path=f"{CONF_PATH}/{file}")

        state_values = [
            "certificate",
            "ca",
            "truststore_password",
            "keystore_password",
        ]
        for value in state_values:
            setattr(self.charm._state, value, None)
        self.charm._state.tls = "disabled"
        self.charm._update(event)
