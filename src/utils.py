#!/usr/bin/env python3
# Copyright 2023 Canonical Ltd.
# See LICENSE file for licensing details.

"""Collection of helper methods for Trino Charm."""

import hashlib
import json
import logging
import os
import re
import secrets
import string
import subprocess  # nosec B404
import textwrap
from dataclasses import dataclass
from urllib.parse import urlparse

from cerberus import Validator
from jinja2 import Environment, FileSystemLoader
from ops.pebble import Error as PebbleError
from ops.pebble import ExecError

from literals import REPLICA_SCHEMA

logger = logging.getLogger(__name__)


def generate_password() -> str:
    """Create randomized string for use as app passwords.

    Returns:
        String of 32 randomized letter+digit characters
    """
    return "".join([secrets.choice(string.ascii_letters + string.digits) for _ in range(32)])


def content_hash(content: str) -> str:
    """Return a stable SHA-256 hex digest of file content.

    Used to encode a managed file's desired content into the Pebble plan so
    that Pebble's own change detection triggers a restart only when the
    content actually changes.

    Args:
        content: The rendered desired file content.

    Returns:
        Hex-encoded SHA-256 digest of the content.
    """
    return hashlib.sha256(content.encode()).hexdigest()


def _certificate_fingerprint(cert: str) -> str:
    """Return a stable identity for a certificate for change detection.

    Hashes the PEM text directly: matching the exact bytes keytool records is
    unnecessary because reconciliation tracks its own manifest of what it
    installed rather than reading back the keystore.

    Args:
        cert: The certificate content (PEM).

    Returns:
        Hex-encoded SHA-256 digest of the certificate text.
    """
    return hashlib.sha256(cert.encode()).hexdigest()


def truststore_manifest_hash(certs: dict) -> str:
    """Return a deterministic hash of a desired truststore cert set.

    JKS files embed per-entry timestamps, so hashing the keystore bytes is not
    reproducible. Hashing a sorted `alias:fingerprint` manifest instead yields
    a stable value that changes only when the desired cert set changes, making
    it safe to place in the Pebble plan as a restart trigger.

    Args:
        certs: Mapping of alias to certificate content (PEM).

    Returns:
        Hex-encoded SHA-256 digest of the sorted manifest.
    """
    manifest = sorted(f"{alias}:{_certificate_fingerprint(cert)}" for alias, cert in certs.items())
    return hashlib.sha256("\n".join(manifest).encode()).hexdigest()


def _read_truststore_manifest(container, manifest_path) -> dict:
    """Read the tracked truststore manifest, or an empty dict if absent.

    Args:
        container: The Trino container.
        manifest_path: Path to the sidecar manifest file.

    Returns:
        Mapping of alias to fingerprint of the last-installed certs.
    """
    try:
        return json.loads(container.pull(manifest_path).read())
    except PebbleError:
        return {}
    except (json.JSONDecodeError, TypeError):
        logger.warning("Corrupt truststore manifest at %s, treating as empty", manifest_path)
        return {}


def _keytool_delete_alias(container, keystore_path, storepass, alias) -> None:
    """Delete an alias from a keystore, ignoring a missing entry.

    Args:
        container: The Trino container.
        keystore_path: Path to the JKS/PKCS12 keystore.
        storepass: The keystore password.
        alias: The alias to delete.
    """
    command = [
        "keytool",
        "-delete",
        "-alias",
        alias,
        "-keystore",
        keystore_path,
        "-storepass",
        storepass,
        "-noprompt",
    ]
    try:
        container.exec(command).wait_output()
    except ExecError as e:
        if e.stderr and "does not exist" in e.stderr:
            return
        logger.warning("Failed to delete alias %r from %s: %s", alias, keystore_path, e.stderr)


def _keytool_import_cert(container, keystore_path, storepass, alias, cert, working_dir) -> None:
    """Import a PEM certificate into a keystore under the given alias.

    Args:
        container: The Trino container.
        keystore_path: Path to the JKS/PKCS12 keystore.
        storepass: The keystore password.
        alias: The alias to import under.
        cert: The certificate content (PEM).
        working_dir: Directory used to stage the certificate file.
    """
    cert_file = f"{alias}.crt"
    container.push(os.path.join(working_dir, cert_file), cert, make_dirs=True)
    command = [
        "keytool",
        "-import",
        "-v",
        "-alias",
        alias,
        "-file",
        cert_file,
        "-keystore",
        keystore_path,
        "-storepass",
        storepass,
        "-noprompt",
    ]
    try:
        container.exec(command, working_dir=working_dir).wait_output()
    finally:
        try:
            container.remove_path(os.path.join(working_dir, cert_file))
        except PebbleError:
            pass


def reconcile_truststore(container, keystore_path, storepass, desired, working_dir, manifest_path):
    """Reconcile a keystore's managed certificates to the desired set in place.

    Adds missing or changed certificates, removes obsolete ones, and leaves
    unchanged entries untouched. Only aliases recorded in the sidecar manifest
    are considered managed, so default CA entries in a shared keystore (e.g.
    the JVM `cacerts`) are never removed. Avoids the churn of deleting and
    recreating the keystore on every reconcile.

    Args:
        container: The Trino container.
        keystore_path: Path to the JKS/PKCS12 keystore.
        storepass: The keystore password.
        desired: Mapping of alias to certificate content (PEM) that should exist.
        working_dir: Directory used to stage certificate files for import.
        manifest_path: Path to the sidecar manifest tracking managed aliases.
    """
    tracked = _read_truststore_manifest(container, manifest_path)
    desired_fps = {alias: _certificate_fingerprint(cert) for alias, cert in desired.items()}

    # Remove certs that are managed but no longer desired, or whose content changed.
    for alias, fingerprint in tracked.items():
        if desired_fps.get(alias) != fingerprint:
            _keytool_delete_alias(container, keystore_path, storepass, alias)

    # Import certs that are new or whose content changed.
    for alias, cert in desired.items():
        if tracked.get(alias) == desired_fps[alias]:
            continue
        _keytool_import_cert(container, keystore_path, storepass, alias, cert, working_dir)

    container.push(manifest_path, json.dumps(desired_fps), make_dirs=True)


def render(template_name, env=None):
    """Pushes configuration files to application.

    Args:
        template_name: template_file.
        env: (Optional) The subset of config values for the file.

    Returns:
        content: template content.
    """
    # get the absolute path of templates directory.
    charm_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), os.pardir))
    templates_path = os.path.join(charm_dir, "templates")

    # handle jinja files.
    if "jinja" in template_name:
        loader = FileSystemLoader(templates_path)
        content = (
            Environment(loader=loader, autoescape=True).get_template(template_name).render(**env)
        )

    # handle properties files.
    else:
        file_path = os.path.join(templates_path, template_name)
        with open(file_path, "r") as file:
            content = file.read()
    return content


def string_to_dict(string_value):
    """Convert a string to a dictionary with = delimiter.

    Args:
        string_value: The string to be converted

    Returns:
        dictionary: The converted dictionary
    """
    pairs = string_value.splitlines()
    dictionary = {}
    for pair in pairs:
        key, value = pair.split("=", maxsplit=1)
        dictionary[key] = value
    return dictionary


def validate_membership(connector_fields, conn_input):
    """Validate if user input fields match those allowed by Trino.

    Args:
        connector_fields: Allowed and required Trino fields by connector
        conn_input: User input connection fields

    Raises:
        ValueError: In the case where a required field is missing
                    In the case where a provided field is not accepted
    """
    required = connector_fields["required"]
    optional = connector_fields["optional"]

    missing_fields = set(required) - set(conn_input)
    if missing_fields:
        raise ValueError(f"field(s) {missing_fields!r} are required")

    illegal_fields = set(conn_input) - (set(required) | set(optional))
    if illegal_fields:
        raise ValueError(f"field(s) {illegal_fields!r} are not allowed")


def validate_jdbc_pattern(conn_input, conn_name):
    """Validate the format of postgresql jdbc string.

    Args:
        conn_input: user input connector dictionary
        conn_name: user input connector name

    Raises:
        ValueError: In the case the jdbc string is invalid
    """
    if not re.match("jdbc:[a-z0-9]+:(?s:.*)$", conn_input["connection-url"]):
        raise ValueError(f"{conn_name!r} has an invalid jdbc format")


def handle_exec_error(func):
    """Handle ExecError while executing command on application container.

    Args:
        func: The function to decorate.

    Returns:
        wrapper: A decorated function that raises an error on failure.
    """

    def wrapper(*args, **kwargs):
        """Execute wrapper for the decorated function and handle errors.

        Args:
            args: Positional arguments passed to the decorated function.
            kwargs: Keyword arguments passed to the decorated function.

        Returns:
            result: The result of the decorated function if successful.

        Raises:
            ExecError: In case the command fails to execute successfully.
        """
        try:
            result = func(*args, **kwargs)
            return result
        except ExecError:
            logger.exception(f"Failed to execute {func.__name__}:")
            raise

    return wrapper


def validate_keys(data, schema):
    """Validate the catalog schema.

    Args:
        data: the provided catalog data
        schema: the expected schema

    Raise:
        ValueError: if the catalog does not match the schema.
    """
    v = Validator(schema)
    if not v.validate(data):
        raise ValueError(f"Data does not conform to schema: {schema}")


def create_postgresql_properties(cat_name, cat_info, backend, replicas):
    """Create the postgresql connector catalog files.

    Args:
        cat_name: catalog name.
        cat_info: the templated configuration values.
        backend: the db configuration values.
        replicas: the ro and rw replicas.

    Returns:
        catalogs: the PostgreSQL catalogs.
    """
    catalogs = {}
    for replica_info in replicas.values():
        validate_keys(replica_info, REPLICA_SCHEMA)
        user_name = replica_info.get("user")
        user_pwd = replica_info.get("password")
        suffix = replica_info.get("suffix", "")

        catalog_name = f"{cat_name}{suffix}"
        url = f"{backend['url']}/{cat_info['database']}"
        if backend.get("params"):
            url = f"{url}?{backend['params']}"

        catalog_content = textwrap.dedent(
            f"""\
            connector.name={backend["connector"]}
            connection-url={url}
            connection-user={user_name}
            connection-password={user_pwd}
        """
        )
        catalog_content += backend.get("config", "")
        catalogs[catalog_name] = catalog_content
    return catalogs


def create_bigquery_properties(name, info, backend, sa_creds_path):
    """Create the bigquery connector catalog files.

    Args:
        name: the catalog name.
        info: the templated configuration values.
        backend: the db configuration values.
        sa_creds_path: the path of the service account credentials.

    Returns:
        catalog: the bigquery catalogs.
    """
    catalog = {}

    catalog_content = textwrap.dedent(
        f"""\
    connector.name={backend["connector"]}
    bigquery.project-id={info["project"]}
    bigquery.credentials-file={sa_creds_path}
    """
    )
    catalog_content += backend.get("config", "")
    catalog[name] = catalog_content
    return catalog


def add_cert_to_truststore(container, name, cert, storepass, conf_path):
    """Add CA to JKS truststore.

    Args:
        container: Trino container.
        name: Certificate file name.
        cert: Certificate content.
        storepass: Truststore password.
        conf_path: The conf directory.

    Raises:
        ExecError: In case of error during keytool certificate import
    """
    command = [
        "keytool",
        "-import",
        "-v",
        "-alias",
        name,
        "-file",
        f"{name}.crt",
        "-keystore",
        "truststore.jks",
        "-storepass",
        storepass,
        "-noprompt",
    ]
    try:
        process = container.exec(
            command,
            working_dir=conf_path,
        )
        stdout, _ = process.wait_output()
        logger.info(stdout)
    except ExecError as e:
        expected_error_string = f"alias <{name}> already exists"
        if expected_error_string in str(e.stdout):
            logger.debug(expected_error_string)
            return
        logger.error(e.stdout)
        raise


def add_users_to_password_db(container, credentials, db_path):
    """Create necessary db users for authentication.

    Args:
        container: The trino container.
        credentials: A dictionary of user/password.
        db_path: The path to the `password.db`.

    Raises:
        ExecError: in case the container exec is unsuccessful.
    """
    if container.exists(db_path):
        container.remove_path(db_path)

    db_exists = False
    for user, password in credentials.items():
        command = [
            "htpasswd",
            "-b",
            "-B",
            "-C",
            "10",
            db_path,
            user,
            password,
        ]
        if not db_exists:
            command.insert(2, "-c")
        try:
            container.exec(command).wait_output()
            db_exists = True
        except (subprocess.CalledProcessError, ExecError) as e:
            logger.error(f"unable to add user credentials {e.stderr}")
            raise


class ProxyConfigError(Exception):
    """Raised when a Juju model proxy URL or a JVM proxy override is invalid.

    Callers catch this to surface `BlockedStatus` rather than crash.
    The message names the offending setting and never includes credentials.
    """


@dataclass(frozen=True)
class ParsedProxy:
    """A parsed proxy URL, in the shape needed by both rendering paths.

    Attributes:
        host: The proxy hostname, without brackets even for IPv6.
        port: The explicit port, defaulted from the URL's own scheme.
        secure: Whether the proxy URL scheme was `https://`.
    """

    host: str
    port: int
    secure: bool


def parse_proxy_url(value, setting_name):
    """Parse and validate a Juju model proxy URL.

    Args:
        value: The raw proxy URL (e.g. `JUJU_CHARM_HTTPS_PROXY`), or None/empty.
        setting_name: The Juju setting name, used to identify the offending
            setting in a raised `ProxyConfigError` (e.g. `"juju-https-proxy"`).

    Returns:
        A `ParsedProxy`, or None if `value` is unset/empty.

    Raises:
        ProxyConfigError: If the URL has no explicit http(s) scheme, no host,
            a non-numeric port, a path/query/fragment, or embedded credentials.
            The message never includes the credential values.
    """
    if not value:
        return None

    parsed = urlparse(value)

    if parsed.scheme not in ("http", "https"):
        raise ProxyConfigError(f"{setting_name} must have an explicit http:// or https:// scheme")

    if not parsed.hostname:
        raise ProxyConfigError(f"{setting_name} is missing a proxy host")

    if parsed.path or parsed.query or parsed.fragment:
        raise ProxyConfigError(
            f"{setting_name} must be a bare host:port address, without a path, query or fragment"
        )

    if parsed.username or parsed.password:
        raise ProxyConfigError(f"{setting_name} must not contain credentials")

    try:
        port = parsed.port
    except ValueError:
        raise ProxyConfigError(f"{setting_name} has a non-numeric port") from None

    if port is None:
        port = 443 if parsed.scheme == "https" else 80

    return ParsedProxy(host=parsed.hostname, port=port, secure=parsed.scheme == "https")


def _convert_no_proxy(no_proxy_value):
    """Convert a comma-separated `juju-no-proxy` value to a pipe-separated list.

    CIDR entries cannot be expressed by `-Dhttp.nonProxyHosts`, which only
    supports literal hosts and `*` wildcards, so they are dropped with a
    single aggregated warning naming all of them, rather than one log
    line per entry.

    Args:
        no_proxy_value: The raw comma-separated value, or None/empty.

    Returns:
        The pipe-separated string of literal hosts (possibly empty).
    """
    if not no_proxy_value:
        return ""

    kept = []
    dropped = []
    for entry in no_proxy_value.split(","):
        entry = entry.strip()
        if not entry:
            continue
        if "/" in entry:
            dropped.append(entry)
            continue
        kept.append(entry)

    if dropped:
        logger.warning(
            "Dropping unsupported CIDR entries from juju-no-proxy (not expressible by "
            "-Dhttp.nonProxyHosts): %s",
            ", ".join(dropped),
        )

    return "|".join(kept)


def jvm_proxy_options(http_proxy_url, https_proxy_url, no_proxy_value):
    """Derive JVM proxy system property flags from Juju model proxy config.

    Ports are always explicit, derived from each proxy URL's own scheme:
    the JVM's own per-family defaults (`http.proxyPort` defaults to 80,
    `https.proxyPort` to 443) are never relied upon, since they are keyed by
    system-property family rather than by the proxy URL's transport.

    Args:
        http_proxy_url: The raw `JUJU_CHARM_HTTP_PROXY` value, or None/empty.
        https_proxy_url: The raw `JUJU_CHARM_HTTPS_PROXY` value, or None/empty.
        no_proxy_value: The raw `JUJU_CHARM_NO_PROXY` value, or None/empty.

    Returns:
        A space-separated string of `-D...` flags (possibly empty).

    Raises:
        ProxyConfigError: If either proxy URL is invalid.
    """
    flags = []

    http_proxy = parse_proxy_url(http_proxy_url, "juju-http-proxy")
    if http_proxy is not None:
        flags.append(f"-Dhttp.proxyHost={http_proxy.host}")
        flags.append(f"-Dhttp.proxyPort={http_proxy.port}")

    https_proxy = parse_proxy_url(https_proxy_url, "juju-https-proxy")
    if https_proxy is not None:
        flags.append(f"-Dhttps.proxyHost={https_proxy.host}")
        flags.append(f"-Dhttps.proxyPort={https_proxy.port}")

    # Emitted independently of whether a proxy host is set: the JVM only
    # consults it once a proxyHost is present, so a standalone value is inert
    # but still expected.
    non_proxy_hosts = _convert_no_proxy(no_proxy_value)
    if non_proxy_hosts:
        flags.append(f"-Dhttp.nonProxyHosts={non_proxy_hosts}")

    return " ".join(flags)


def oauth_proxy_properties(http_proxy_url, https_proxy_url):
    """Derive the OAuth JWKS proxy properties from model proxy config only.

    Built from the model proxy configuration alone: unlike
    `jvm_proxy_options`, `additional-jvm-options` has no influence here, since
    JVM proxy flags carry no scheme and cannot express `.secure`.

    `juju-https-proxy` is preferred, falling back to `juju-http-proxy`.
    Airlift's `http-proxy` property names the address of the
    proxy server (a CONNECT proxy), not "the proxy for http:// destinations".

    Args:
        http_proxy_url: The raw `JUJU_CHARM_HTTP_PROXY` value, or None/empty.
        https_proxy_url: The raw `JUJU_CHARM_HTTPS_PROXY` value, or None/empty.

    Returns:
        A dict with `"http_proxy"` (`host:port`, IPv6 hosts bracketed) and,
        only when the selected proxy URL's scheme is `https://`, `"secure":
        True`. Empty if no model proxy is configured.

    Raises:
        ProxyConfigError: If either proxy URL is invalid.
    """
    https_proxy = parse_proxy_url(https_proxy_url, "juju-https-proxy")
    http_proxy = parse_proxy_url(http_proxy_url, "juju-http-proxy")

    selected = https_proxy or http_proxy
    if selected is None:
        return {}

    host = f"[{selected.host}]" if ":" in selected.host else selected.host
    properties: dict[str, str | bool] = {"http_proxy": f"{host}:{selected.port}"}
    if selected.secure:
        properties["secure"] = True
    return properties


def validate_jvm_proxy_overrides(user_opts):
    """Validate that additional-jvm-options proxy overrides pair host with port.

    A `-D{http,https}.proxyHost` or `-D{http,https}.proxyPort` override without
    its matching counterpart is incomplete: the correct port cannot be
    inferred, so it is rejected rather than silently defaulted.

    Args:
        user_opts: The raw `additional-jvm-options` value, or None/empty.

    Raises:
        ProxyConfigError: If a proxyHost/proxyPort pair is incomplete, naming
            `additional-jvm-options`.
    """
    if not user_opts:
        return

    seen = {}
    for opt in user_opts.split():
        match = re.match(r"^-D(https?)\.proxy(Host|Port)=", opt)
        if match:
            family, kind = match.groups()
            seen.setdefault(family, set()).add(kind)

    for family, kinds in seen.items():
        if kinds != {"Host", "Port"}:
            present = "Host" if "Host" in kinds else "Port"
            missing = "Port" if present == "Host" else "Host"
            raise ProxyConfigError(
                f"additional-jvm-options: -D{family}.proxy{present} is missing its "
                f"matching -D{family}.proxy{missing}"
            )


def update_opts(default_opts, user_opts):
    """Update the default options with user-provided options.

    Args:
        default_opts: The jvm default options.
        user_opts: The user options that may override the defaults.

    Returns:
        Combined options, with user options overriding any matching default options.
    """

    def get_opt_name(opt: str):
        """Extract the name or key from a given option string.

        Args:
            opt: The option string from which to extract the key or name.

        Returns:
            opt: The extracted key or name if a match is found, otherwise the
            original option string.
        """
        patterns_index = [
            (r"^-X[a-z]+", 0),
            (r"^-XX:[+-](\w+)", 1),
            (r"^(.*?)=", 1),
        ]
        for pi in patterns_index:
            match = re.match(pi[0], opt)
            if match:
                return match.group(pi[1])
        return opt

    def_dict = {get_opt_name(opt): opt for opt in default_opts.split()}
    user_dict = {get_opt_name(opt): opt for opt in user_opts.split()}
    def_dict.update(user_dict)
    return " ".join(def_dict.values())
