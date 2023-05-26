"""Manager for handling Trino TLS configuration."""

import logging
import os
import socket
from typing import Dict, List, Optional

from charms.tls_certificates_interface.v2.tls_certificates import (
    TLSCertificatesRequiresV2,
    generate_csr,
    generate_private_key,
)
from ops.charm import ActionEvent
from ops.framework import Object
from ops.model import Container, Relation
from ops.pebble import ExecError
from literals import TLS_RELATION

from literals import CONF_PATH
from ops.model import (ActiveStatus, BlockedStatus, MaintenanceStatus, WaitingStatus)
from utils import generate_password, parse_tls_file, push

logger = logging.getLogger(__name__)


class TrinoTLS(Object):
    """Handler for managing the client and unit TLS keys/certs."""

    def __init__(self, charm):
        super().__init__(charm, "tls")
        self.charm = charm
        self.cert_subject = "trino-k8s"
        self.certificates = TLSCertificatesRequiresV2(self.charm, TLS_RELATION)

        self.framework.observe(
            self.charm.on[TLS_RELATION].relation_created, self._tls_relation_created
        )
        self.framework.observe(
            self.charm.on[TLS_RELATION].relation_joined, self._tls_relation_joined
        )
        self.framework.observe(
            self.certificates.on.certificate_available, self._on_certificate_available
        )

    def _tls_relation_created(self, _) -> None:
        """Handler for `certificates_relation_created` event."""
        if not self.charm.unit.is_leader():
            return

        self.peer_relation.data[self.charm.app].update({"tls": "enabled"})

    def _tls_relation_joined(self, _) -> None:
        """Handler for `certificates_relation_joined` event."""
        # generate unit private key if not already created by action
        private_key =  generate_private_key()
        self.charm._state.private_key = private_key.decode("utf-8")

        self._request_certificate()

    def _request_certificate(self):
        """Generates and submits CSR to provider."""

        csr = generate_csr(
            private_key=self.charm._state.private_key.encode("utf-8"),
            subject=self.cert_subject,
            sans_dns=['trino-k8s'],
        )
        self.charm._state.csr = csr.decode("utf-8")

        self.certificates.request_certificate_creation(certificate_signing_request=csr)


    def _on_certificate_available(self, event) -> None:
        """Handler for `certificates_available` event after provider updates signed certs."""
        if not self.charm._state.is_ready():
            self.charm.model.unit.status = WaitingStatus("Waiting for peer relation to be created")
            event.defer()
            return

        container = self.charm.model.unit.get_container(self.charm.name)
        if not container.can_connect():
            event.defer()
            return

        self.charm._state.certificate = event.certificate
        self.charm._state.ca = event.ca

        push(container, self.charm._state.private_key, f"{CONF_PATH}/server.key")
        push(container, self.charm._state.ca, f"{CONF_PATH}/ca.pem")
        push(container, self.charm._state.certificate, f"{CONF_PATH}/server.crt")

        self.charm._update(event)
