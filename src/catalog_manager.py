# Copyright 2023 Canonical Ltd.
# See LICENSE file for licensing details.

"""Trino catalog classes."""

import dataclasses
import logging
import textwrap
from abc import ABC, abstractmethod

import yaml
from ops.model import SecretNotFoundError

from literals import BIGQUERY_BACKEND_SCHEMA, GSHEETS_BACKEND_SCHEMA
from utils import validate_keys

logger = logging.getLogger(__name__)


@dataclasses.dataclass(frozen=True)
class RenderedCatalog:
    """The desired state produced by rendering a single catalog.

    Attrs:
        properties: Mapping of catalog file stem to `.properties` text.
        credentials: Mapping of credential file name (including the `.json`
            suffix) to text content.
        certs: Certificates the catalog wants present in the truststore,
            keyed by alias, collected for central truststore reconciliation.
    """

    properties: dict
    credentials: dict
    certs: dict


class CatalogBase(ABC):
    """The base class for all catalog configurations."""

    def __init__(self, charm, truststore_pwd, name, info, backend):
        """Construct.

        Args:
            charm: the Trino charm.
            truststore_pwd: the truststore password.
            name: the catalog name.
            info: the catalog specific information.
            backend: the backend template for configuration.
        """
        self.charm = charm
        self.truststore_pwd = truststore_pwd
        self.name = name
        self.info = info
        self.backend = backend

    def _resolve_placeholders(self, catalogs):
        """Substitute truststore placeholders in rendered catalog properties.

        Args:
            catalogs: mapping of catalog file stem to `.properties` text.

        Returns:
            A new mapping with `{SSL_PATH}` and `{SSL_PWD}` substituted.
        """
        return {
            key: value.replace("{SSL_PATH}", str(self.charm.truststore_abs_path)).replace(
                "{SSL_PWD}", self.truststore_pwd
            )
            for key, value in catalogs.items()
        }

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
            secret = self.charm.model.get_secret(id=secret_id)
            content = secret.get_content(refresh=True)
        except SecretNotFoundError:
            logger.error(f"secret {secret_id!r} not found.")
            raise
        return content

    @abstractmethod
    def _get_credentials(self):
        """Handle connector-specific logic for retrieving credentials.

        Returns:
            A tuple of (data needed by `_create_properties`, truststore
            certificates keyed by alias, empty for connectors with none).
        """

    @abstractmethod
    def _create_properties(self, secret_content):
        """Handle connector-specific logic for creating the `.properties` file.

        Args:
            secret_content: the data returned by `_get_credentials`.

        Returns:
            A tuple of (catalog properties keyed by file stem, credential
            file contents keyed by file name, empty for connectors with
            none).
        """

    def render(self):
        """Render this catalog's desired properties, credentials and certs.

        Returns:
            The `RenderedCatalog` desired state.

        Raises:
            Exception: in case of error rendering the catalog.
        """
        try:
            secret_content, certs = self._get_credentials()
            properties, credentials = self._create_properties(secret_content)
            properties = self._resolve_placeholders(properties)
            return RenderedCatalog(properties=properties, credentials=credentials, certs=certs)
        except Exception as e:
            # Secret content can be echoed back in parser and lookup errors,
            # so only the failure type is recorded.
            logger.error("Unable to render catalog %r: %s", self.name, type(e).__name__)
            raise


class BigqueryCatalog(CatalogBase):
    """Class for handling the BigQuery connector."""

    def _get_credentials(self):
        """Handle BigQuery catalog configuration.

        Returns:
            A tuple of (the service account credentials as a string, an
            empty certs mapping).
        """
        validate_keys(self.backend, BIGQUERY_BACKEND_SCHEMA)

        secret = self._get_secret_content(self.info["secret-id"])
        service_accounts = secret["service-accounts"]
        sa_dict = yaml.safe_load(service_accounts)
        sa_string = sa_dict[self.info["project"]]
        return sa_string, {}

    def _create_properties(self, sa_string):
        """Create the BigQuery connector catalog files.

        Args:
            sa_string: the service account credentials as a string.

        Returns:
            A tuple of (catalog name to configuration, credential file name
            to content).
        """
        credential_name = f"{self.name}.json"
        sa_creds_path = self.charm.credential_abs_path.joinpath(credential_name)

        catalog_content = textwrap.dedent(
            f"""\
            connector.name={self.backend["connector"]}
            bigquery.project-id={self.info["project"]}
            bigquery.credentials-file={sa_creds_path}
            """
        )
        catalog_content += self.backend.get("config", "")
        return {self.name: catalog_content}, {credential_name: sa_string}


class GsheetCatalog(CatalogBase):
    """Class for handling the Google Sheets connector."""

    def _get_credentials(self):
        """Handle Google Sheets catalog configuration.

        Returns:
            A tuple of (the service account credentials as a string, an
            empty certs mapping).
        """
        validate_keys(self.backend, GSHEETS_BACKEND_SCHEMA)

        secret = self._get_secret_content(self.info["secret-id"])
        service_accounts = secret["service-accounts"]
        sa_dict = yaml.safe_load(service_accounts)
        sa_string = sa_dict[self.name]
        return sa_string, {}

    def _create_properties(self, sa_string):
        """Create the Google Sheets connector catalog files.

        Args:
            sa_string: the service account credentials as a string.

        Returns:
            A tuple of (catalog name to configuration, credential file name
            to content).
        """
        credential_name = f"{self.name}.json"
        sa_creds_path = self.charm.credential_abs_path.joinpath(credential_name)

        catalog_content = textwrap.dedent(
            f"""\
            connector.name={self.backend["connector"]}
            gsheets.metadata-sheet-id={self.info["metasheet-id"]}
            gsheets.credentials-path={sa_creds_path}
            """
        )
        catalog_content += self.backend.get("config", "")
        return {self.name: catalog_content}, {credential_name: sa_string}


class HiveCatalog(CatalogBase):
    """Class for handling the Hive connector."""

    def _get_credentials(self):
        """No-op fetch method for a connector with no credentials.

        Returns:
            A tuple of (None, an empty certs mapping).
        """
        return None, {}

    def _create_properties(self, secret_content):
        """Create the Hive connector catalog files.

        Args:
            secret_content: unused argument.

        Returns:
            A tuple of (catalog name to configuration, an empty credentials
            mapping).
        """
        catalog_content = textwrap.dedent(
            f"""\
            connector.name={self.backend["connector"]}
            hive.metastore.uri={self.backend["url"]}
            """
        )
        return {self.name: catalog_content}, {}
