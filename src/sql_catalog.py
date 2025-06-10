# Copyright 2023 Canonical Ltd.
# See LICENSE file for licensing details.

"""Trino catalog classes."""

import logging
import textwrap

import yaml

from catalog_manager import CatalogBase
from literals import REPLICA_SCHEMA, SQL_BACKEND_SCHEMA
from utils import validate_keys

logger = logging.getLogger(__name__)


class SqlCatalog(CatalogBase):
    """Class for handling the PostgreSQL and MySQL connectors."""

    def _get_credentials(self):
        """Handle PostgreSQL/MySQL/Redshift catalog configuration.

        Returns:
            replicas: the database replica configuration.
        """
        validate_keys(self.backend, SQL_BACKEND_SCHEMA)
        secret = self._get_secret_content(self.info["secret-id"])
        replicas = yaml.safe_load(secret["replicas"])
        certs = yaml.safe_load(secret.get("cert", ""))
        self._add_certs(certs)
        return replicas

    def _get_db_url(self):
        """Get database url for the connection."""
        return f"{self.backend['url']}/{self.info.get('database','')}"

    def _create_properties(self, replicas):
        """Create the PostgreSQL/MySQL/Redshift connector catalog files.

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
            url = self._get_db_url()
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


class RedshiftCatalog(SqlCatalog):
    """Class for handling the Redshift connector."""

    def _get_db_url(self):
        """Get database url for the connection."""
        return f"{self.backend['url']}"
