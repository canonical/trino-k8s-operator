# Copyright 2023 Canonical Ltd.
# See LICENSE file for licensing details.

"""Defines opensearch relation event handling methods."""

import logging
import re

from ops import framework

from literals import INDEX_NAME

logger = logging.getLogger(__name__)


class OpensearchRelationHandler(framework.Object):
    """Client for ranger:opensearch relations.

    Event observation is centralized in the charm; this object exposes logic
    methods invoked by the charm reconciler to read connection and certificate
    data from the relation via the model.
    """

    def __init__(self, charm, relation_name="opensearch"):
        """Construct.

        Args:
            charm: The charm to attach the hooks to.
            relation_name: The name of the relation.
        """
        self.relation_name = relation_name
        self.charm = charm

        super().__init__(charm, self.relation_name)

    def _get_relation(self):
        """Return the opensearch relation, or None when absent.

        Returns:
            The opensearch relation or None.
        """
        relation = self.charm.model.get_relation(self.relation_name)
        if relation is None or relation.app is None:
            return None
        return relation

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

    def gather_certificate(self):
        """Read the OpenSearch CA certificate from the relation via the model.

        Returns:
            The CA certificate (PEM), or None when unavailable.
        """
        relation = self._get_relation()
        if relation is None:
            return None
        secret_id = relation.data[relation.app].get("secret-tls")
        if not secret_id:
            return None
        content = self.get_secret_content(secret_id)
        tls_ca = content["tls-ca"]
        pattern = r"-----BEGIN CERTIFICATE-----.*?-----END CERTIFICATE-----"
        certificates_list = re.findall(pattern, tls_ca, re.DOTALL)
        return certificates_list[1]

    def gather_connection(self) -> dict:
        """Read OpenSearch connection values from the relation via the model.

        Returns:
            A dictionary of connection values, disabled when unavailable.
        """
        relation = self._get_relation()
        if relation is None:
            return {"is_enabled": False}

        event_data = relation.data[relation.app]
        secret_id = event_data.get("secret-user")
        endpoints = event_data.get("endpoints")
        if not secret_id or not endpoints:
            return {"is_enabled": False}

        user_credentials = self.get_secret_content(secret_id)
        host, port = endpoints.split(",", 1)[0].split(":")
        return {
            "index": INDEX_NAME,
            "host": host,
            "port": port,
            "password": user_credentials["password"],
            "username": user_credentials["username"],
            "is_enabled": True,
        }
