# Copyright 2023 Canonical Ltd.
# See LICENSE file for licensing details.

"""Trino catalog classes."""

import logging
import textwrap
from abc import ABC, abstractmethod

import yaml
from ops.model import SecretNotFoundError

from literals import (
    BIGQUERY_BACKEND_SCHEMA,
    GSHEETS_BACKEND_SCHEMA,
    REPLICA_SCHEMA,
    SQL_BACKEND_SCHEMA,
)
from utils import add_cert_to_truststore, validate_keys

logger = logging.getLogger(__name__)


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

    def _add_catalog(self, catalogs):
        """Add catalogs to Trino.

        Args:
            catalogs: the catalogs to add.
        """
        container = self.charm.unit.get_container(self.charm.name)

        for key, value in catalogs.items():
            config = value.replace(
                "{SSL_PATH}", str(self.charm.truststore_abs_path)
            ).replace("{SSL_PWD}", self.truststore_pwd)

            container.push(
                self.charm.catalog_abs_path.joinpath(f"{key}.properties"),
                config,
                make_dirs=True,
            )

    def _add_certs(self, certs):
        """Prepare and add certificates to Trino truststore.

        Args:
            certs: the certificates to add.
        """
        if not certs:
            return

        container = self.charm.unit.get_container(self.charm.name)

        for name, cert in certs.items():
            container.push(
                self.charm.conf_abs_path.joinpath(f"{name}.crt"),
                cert,
                make_dirs=True,
            )
            try:
                add_cert_to_truststore(
                    container,
                    name,
                    cert,
                    self.truststore_pwd,
                    str(self.charm.conf_abs_path),
                )
                container.remove_path(
                    self.charm.conf_abs_path.joinpath(f"{name}.crt")
                )
            except Exception as e:
                logger.error(f"Failed to add {name} cert: {e}")

    def _add_service_account(self, sa_string, sa_creds_path):
        """Add service account credentials.

        Args:
            sa_string: the service account credentials as a string.
            sa_creds_path: the path to the service account file.
        """
        container = self.charm.unit.get_container(self.charm.name)
        container.push(sa_creds_path, sa_string, make_dirs=True)

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
        """Handle connector-specific logic for retrieving credentials."""

    @abstractmethod
    def _create_properties(self, secret_content):
        """Handle connector-specific logic for creating the `.properties` file.

        Args:
            secret_content: the content of the juju secret.
        """

    def configure_catalogs(self):
        """Manage catalog properties files and create the appropriate catalog instance.

        Raises:
            Exception: in case of error adding catalog.
        """
        try:
            secret_content = self._get_credentials()
            catalogs = self._create_properties(secret_content)
            self._add_catalog(catalogs)
            connector = self.backend["connector"]
            logger.info(
                f"{connector} catalog {self.name!r} added successfully"
            )
        except Exception as e:
            logger.error(f"Unable to add catalog {self.name!r}: {e}")
            raise


class SqlCatalog(CatalogBase):
    """Class for handling the PostgreSQL and MySQL connectors."""

    def _get_credentials(self):
        """Handle PostgreSQL/MySQL catalog configuration.

        Returns:
            replicas: the database replica configuration.
        """
        validate_keys(self.backend, SQL_BACKEND_SCHEMA)
        secret = self._get_secret_content(self.info["secret-id"])
        replicas = yaml.safe_load(secret["replicas"])
        certs = yaml.safe_load(secret.get("cert", ""))
        self._add_certs(certs)
        return replicas

    def _create_properties(self, replicas):
        """Create the PostgreSQL/MySQL connector catalog files.

        Args:
            replicas: the database replica configuration.

        Returns:
            catalogs: a dictionary of catalog name and configuration.
        """
        catalogs = {}
        for replica_info in replicas.values():
            validate_keys(replica_info, REPLICA_SCHEMA)
            user_name = replica_info.get("user")
            user_pwd = replica_info.get("password")
            suffix = replica_info.get("suffix", "")

            catalog_name = f"{self.name}{suffix}"
            url = f"{self.backend['url']}/{self.info.get('database','')}"
            if self.backend.get("params"):
                url = f"{url}?{self.backend['params']}"

            catalog_content = textwrap.dedent(
                f"""\
                connector.name={self.backend['connector']}
                connection-url={url}
                connection-user={user_name}
                connection-password={user_pwd}
                """
            )
            catalog_content += self.backend.get("config", "")
            catalogs[catalog_name] = catalog_content
        return catalogs


class BigqueryCatalog(CatalogBase):
    """Class for handling the BigQuery connector."""

    def _get_credentials(self):
        """Handle BigQuery catalog configuration.

        Returns:
            sa_creds_path: the path of the service account credentials.
        """
        validate_keys(self.backend, BIGQUERY_BACKEND_SCHEMA)

        secret = self._get_secret_content(self.info["secret-id"])
        service_accounts = secret["service-accounts"]
        sa_dict = yaml.safe_load(service_accounts)
        sa_string = sa_dict[self.info["project"]]

        sa_creds_path = self.charm.conf_abs_path.joinpath(f"{self.name}.json")
        self._add_service_account(sa_string, sa_creds_path)
        return sa_creds_path

    def _create_properties(self, sa_creds_path):
        """Create the BigQuery connector catalog files.

        Args:
            sa_creds_path: the path of the service account credentials.

        Returns:
            catalog: a dictionary of catalog name and configuration.
        """
        catalog = {}

        catalog_content = textwrap.dedent(
            f"""\
            connector.name={self.backend['connector']}
            bigquery.project-id={self.info['project']}
            bigquery.credentials-file={sa_creds_path}
            """
        )
        catalog_content += self.backend.get("config", "")
        catalog[self.name] = catalog_content
        return catalog


class GsheetCatalog(CatalogBase):
    """Class for handling the Google Sheets connector."""

    def _get_credentials(self):
        """Handle BigQuery catalog configuration.

        Returns:
            sa_creds_path: the path of the service account credentials.
        """
        validate_keys(self.backend, GSHEETS_BACKEND_SCHEMA)

        secret = self._get_secret_content(self.info["secret-id"])
        service_accounts = secret["service-accounts"]
        sa_dict = yaml.safe_load(service_accounts)
        sa_string = sa_dict[self.name]

        sa_creds_path = self.charm.conf_abs_path.joinpath(f"{self.name}.json")
        self._add_service_account(sa_string, sa_creds_path)

        return sa_creds_path

    def _create_properties(self, sa_creds_path):
        """Create the BigQuery connector catalog files.

        Args:
            sa_creds_path: the path of the service account credentials.

        Returns:
            catalog: a dictionary of catalog name and configuration.
        """
        catalog = {}

        catalog_content = textwrap.dedent(
            f"""\
            connector.name={self.backend['connector']}
            gsheets.metadata-sheet-id={self.info['metasheet-id']}
            gsheets.credentials-path={sa_creds_path}
            """
        )
        catalog_content += self.backend.get("config", "")
        catalog[self.name] = catalog_content
        return catalog
