# Copyright 2023 Canonical Ltd.
# See LICENSE file for licensing details.

"""Trino catalog classes."""

import dataclasses
import logging
import string
import textwrap
from abc import ABC, abstractmethod

import yaml
from cryptography import x509
from ops.model import SecretNotFoundError

from config import (
    PrometheusConfigError,
    validate_prometheus_backend,
    validate_prometheus_headers,
)
from literals import BIGQUERY_BACKEND_SCHEMA, GSHEETS_BACKEND_SCHEMA, PROMETHEUS_BACKEND_SCHEMA
from utils import validate_keys

logger = logging.getLogger(__name__)

CATALOG_PASS_ENV_VAR_PREFIX = "CATALOG_PASS_"  # nosec
ENV_VAR_PLAIN_CHARS = frozenset(string.ascii_lowercase + string.digits)


def password_env_var(catalog_name):
    """Return the environment variable that carries a catalog's password.

    Lowercase letters and digits are uppercased, `_` is doubled, and every
    other byte of the UTF-8 name becomes `_` followed by its two hex digits,
    so distinct catalog names such as `a-b` and `a_b` never share a variable.

    Args:
        catalog_name: the name of the rendered catalog.

    Returns:
        The variable name, e.g. `CATALOG_PASS_PG_2D1__RO` for `pg-1_ro`.
    """
    encoded = "".join(_encode_env_var_byte(byte) for byte in catalog_name.encode())
    return CATALOG_PASS_ENV_VAR_PREFIX + encoded


def _encode_env_var_byte(byte):
    """Encode one byte of a catalog name for `password_env_var`.

    Args:
        byte: the byte value.

    Returns:
        The characters that stand for it in an environment variable name.
    """
    char = chr(byte)
    if char in ENV_VAR_PLAIN_CHARS:
        return char.upper()
    if char == "_":
        return "__"
    return f"_{byte:02X}"


def password_reference(catalog_name):
    """Return the property value that makes Trino read a catalog's password.

    Args:
        catalog_name: the name of the rendered catalog.

    Returns:
        The `${ENV:...}` reference to `password_env_var(catalog_name)`.
    """
    return f"${{ENV:{password_env_var(catalog_name)}}}"


@dataclasses.dataclass(frozen=True)
class RenderedCatalog:
    """The desired state produced by rendering a single catalog.

    With `catalog.management=dynamic` Trino copies catalog properties into
    every query plan, so passwords are never written into `properties`: they
    are `${ENV:...}` references resolved from `env`, which goes into the
    Trino service environment.

    Attrs:
        properties: Mapping of catalog file stem to `.properties` text.
        credentials: Mapping of credential file name (including its suffix)
            to text content.
        certs: Certificates the catalog wants present in the truststore,
            keyed by alias, collected for central truststore reconciliation.
        env: Mapping of environment variable name to the secret value that a
            `${ENV:...}` reference in `properties` resolves to.
    """

    properties: dict
    credentials: dict
    certs: dict
    env: dict


class CatalogBase(ABC):
    """The base class for all catalog configurations."""

    def __init__(self, charm, name, info, backend):
        """Construct.

        Args:
            charm: the Trino charm.
            name: the catalog name.
            info: the catalog specific information.
            backend: the backend template for configuration.
        """
        self.charm = charm
        self.name = name
        self.info = info
        self.backend = backend

    def _resolve_placeholders(self, catalogs):
        """Substitute truststore placeholders in rendered catalog properties.

        Args:
            catalogs: mapping of catalog file stem to `.properties` text.

        Returns:
            A new mapping with `{SSL_PATH}` replaced by the truststore path and
            `{SSL_PWD}` by a reference to the `JAVA_TRUSTSTORE_PWD` service
            environment variable, so that the password stays out of the file.
        """
        return {
            key: value.replace("{SSL_PATH}", str(self.charm.truststore_abs_path)).replace(
                "{SSL_PWD}", "${ENV:JAVA_TRUSTSTORE_PWD}"
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
            file contents keyed by file name, environment variables keyed by
            name), the last two empty for connectors without them.
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
            properties, credentials, env = self._create_properties(secret_content)
            properties = self._resolve_placeholders(properties)
            return RenderedCatalog(
                properties=properties, credentials=credentials, certs=certs, env=env
            )
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
            to content, an empty environment mapping).
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
        return {self.name: catalog_content}, {credential_name: sa_string}, {}


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
            to content, an empty environment mapping).
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
        return {self.name: catalog_content}, {credential_name: sa_string}, {}


class PrometheusCatalog(CatalogBase):
    """Class for handling the read-only Prometheus connector."""

    def _get_credentials(self):
        """Validate the backend and fetch optional credentials and a TLS CA.

        Returns:
            A tuple of (authentication fields, certificates keyed by alias).

        Raises:
            ValueError: If the backend, authentication or TLS CA fields are invalid.
        """
        validate_keys(self.backend, PROMETHEUS_BACKEND_SCHEMA)
        properties = validate_prometheus_backend(self.backend)
        if "secret-id" not in self.info:
            return {}, {}

        try:
            content = self._get_secret_content(self.info["secret-id"])
        except Exception:
            raise PrometheusConfigError(
                "Prometheus secret-id must reference a readable, granted secret"
            ) from None
        certs = {}
        if "tls-ca" in content:
            certificate = content["tls-ca"]
            if not isinstance(certificate, str) or not certificate.strip():
                raise PrometheusConfigError("Prometheus tls-ca must be a nonempty PEM certificate")
            try:
                x509.load_pem_x509_certificate(certificate.encode())
            except ValueError:
                raise PrometheusConfigError(
                    "Prometheus tls-ca must be a valid PEM certificate"
                ) from None
            certs[f"prometheus-ca-{self.name}"] = certificate

        auth_fields = set(content) & {"user", "password", "bearer-token"}
        if auth_fields not in (set(), {"user", "password"}, {"bearer-token"}) or (
            not auth_fields and not certs
        ):
            raise PrometheusConfigError(
                "Prometheus requires user/password, bearer-token or tls-ca"
            )
        if not all(
            isinstance(content[field], str)
            and content[field].strip()
            and "\n" not in content[field]
            and "\r" not in content[field]
            for field in auth_fields
        ):
            raise PrometheusConfigError(
                "Prometheus authentication fields must be nonempty single-line strings"
            )
        validate_prometheus_headers(properties, basic_auth="user" in auth_fields)
        return {field: content[field] for field in auth_fields}, certs

    def _create_properties(self, secret_content):
        """Create the Prometheus properties and optional managed token file.

        Args:
            secret_content: Validated basic or bearer authentication fields.

        Returns:
            A tuple of (catalog properties, credential file contents, the
            Basic authentication password keyed by its environment variable).
        """
        lines = ["connector.name=prometheus", f"prometheus.uri={self.backend['url']}"]
        credentials, env = {}, {}
        if "bearer-token" in secret_content:
            credential_name = f"{self.name}.token"
            token_path = self.charm.credential_abs_path.joinpath(credential_name)
            lines.append(f"prometheus.bearer.token.file={token_path}")
            credentials[credential_name] = secret_content["bearer-token"]
        elif "user" in secret_content:
            lines.append(f"prometheus.auth.user={secret_content['user']}")
            lines.append(f"prometheus.auth.password={password_reference(self.name)}")
            env[password_env_var(self.name)] = secret_content["password"]
        catalog_content = "\n".join(lines) + "\n" + (self.backend.get("config") or "")
        return {self.name: catalog_content}, credentials, env


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
            A tuple of (catalog name to configuration, empty credentials and
            environment mappings).
        """
        catalog_content = textwrap.dedent(
            f"""\
            connector.name={self.backend["connector"]}
            hive.metastore.uri={self.backend["url"]}
            """
        )
        return {self.name: catalog_content}, {}, {}
