#!/usr/bin/env python3
# Copyright 2023 Canonical Ltd.
# See LICENSE file for licensing details.

"""Collection of helper methods for Trino Charm."""

import base64
import hashlib
import hmac
import ipaddress
import json
import logging
import os
import re
import secrets
import string
import textwrap
from collections.abc import Mapping
from dataclasses import dataclass
from urllib.parse import urlparse

import bcrypt
from cerberus import Validator
from jinja2 import Environment, FileSystemLoader
from ops.pebble import Error as PebbleError
from ops.pebble import ExecError, PathError

from literals import BCRYPT_COST, BCRYPT_MAX_PASSWORD_BYTES, REPLICA_SCHEMA

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


def is_valid_username(username: object) -> bool:
    """Report whether a username can be written safely to `password.db`.

    The file holds one `user:hash` entry per line, so a name with a colon, whitespace or a
    control character could corrupt an entry or inject another one.

    Args:
        username: The username to check. YAML can produce non-string names, which are invalid.

    Returns:
        True if the username is a non-empty string with no colon, whitespace or control
        character.
    """
    return (
        isinstance(username, str)
        and bool(username)
        and not any(char == ":" or char.isspace() or not char.isprintable() for char in username)
    )


def has_valid_password_length(password: object) -> bool:
    """Report whether bcrypt can hash the password without truncating it.

    Args:
        password: The plaintext password. YAML can produce non-string passwords, which are
            invalid.

    Returns:
        True if the password is a string of at most 72 bytes in UTF-8.
    """
    return isinstance(password, str) and len(password.encode()) <= BCRYPT_MAX_PASSWORD_BYTES


def invalid_usernames(credentials: Mapping) -> list[str]:
    """Return the users that cannot be written to `password.db`.

    Args:
        credentials: A mapping of username to plaintext password.

    Returns:
        The offending usernames as strings, sorted. Passwords are never included.
    """
    return sorted(
        str(user)
        for user, password in credentials.items()
        if not is_valid_username(user) or not has_valid_password_length(password)
    )


# Standard base64 and bcrypt's base64 use the same 64 characters in a different order.
_BCRYPT_BASE64 = bytes.maketrans(
    b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/",
    b"./ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789",
)


def _derive_bcrypt_salt(key: str, username: str) -> bytes:
    """Derive the bcrypt salt for a user from a per-deployment key.

    The salt is deterministic so that rebuilding `password.db` from unchanged credentials gives
    the same file, which lets the charm detect real changes. This is safe because a salt only
    has to be unique, not secret, and the key makes it unique per user and per deployment.

    Args:
        key: A value unique to the deployment, such as the model UUID and application name.
        username: The user the salt is for.

    Returns:
        A bcrypt salt, as accepted by `bcrypt.hashpw`.
    """
    digest = hmac.new(key.encode(), username.encode(), hashlib.sha256).digest()
    encoded = base64.b64encode(digest[:16]).rstrip(b"=").translate(_BCRYPT_BASE64)
    return f"$2b${BCRYPT_COST:02d}$".encode() + encoded


@dataclass(frozen=True)
class PasswordDbWrite:
    """The outcome of `write_password_db`.

    Attributes:
        written: The usernames in the file.
        pushed: Whether the file content changed and was pushed.
    """

    written: list[str]
    pushed: bool


def write_password_db(container, credentials, db_path, salt_key, hashes=None) -> PasswordDbWrite:
    """Write the authentication database from the full set of credentials.

    The file is built in memory and pushed once, which replaces it atomically, and only if its
    content changed. Users whose name or password cannot be stored are skipped.

    Args:
        container: The trino container.
        credentials: A dictionary of user/password.
        db_path: The path to the `password.db`.
        salt_key: The per-deployment key the bcrypt salts are derived from.
        hashes: Users written with an already hashed password, as {username: hash}.

    Returns:
        The usernames written to the file, and whether the file was pushed.
    """
    lines = []
    written = []
    for user, password in credentials.items():
        if not is_valid_username(user):
            logger.warning("Skipping user with an invalid name: %r", user)
            continue
        if not has_valid_password_length(password):
            logger.warning("Skipping user %r: password not a string or longer than 72 bytes", user)
            continue
        password_hash = bcrypt.hashpw(password.encode(), _derive_bcrypt_salt(salt_key, user))
        lines.append(f"{user}:{password_hash.decode()}\n")
        written.append(user)
    for user, password_hash in (hashes or {}).items():
        lines.append(f"{user}:{password_hash}\n")
        written.append(user)
    content = "".join(lines)

    try:
        current = container.pull(db_path).read()
    except PathError:
        current = None
    if content == current:
        return PasswordDbWrite(written=written, pushed=False)

    container.push(db_path, content, make_dirs=True, permissions=0o644)
    return PasswordDbWrite(written=written, pushed=True)


class SecretReadError(Exception):
    """Raised when a relation secret exists but `secret-get` fails.

    Rendering without the secret would change the Pebble plan and restart
    Trino, so the reconcile keeps the values the running plan was built from.
    """

    def __init__(self, relation_name, error):
        """Build the message reported as the unit status.

        Args:
            relation_name: The relation whose secret could not be read.
            error: The `ModelError` raised by `secret-get`.
        """
        detail = str(error).strip().removeprefix("ERROR ").strip()
        super().__init__(f"cannot read {relation_name} relation secret: {detail}")


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
        ProxyConfigError: If the URL cannot be parsed at all (e.g. a malformed
            bracketed IPv6 host), has no explicit http(s) scheme, no host,
            a non-numeric port, a path/query/fragment, or embedded credentials.
            The message never includes the URL or its credential values.
    """
    if not value:
        return None

    try:
        parsed = urlparse(value)
    except ValueError:
        raise ProxyConfigError(f"{setting_name} is not a valid URL") from None

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


def _ipv4_network_to_patterns(network):
    """Express an IPv4 network exactly as `-Dhttp.nonProxyHosts` patterns.

    The JVM only supports leading/trailing `*` wildcards, so the prefix is
    rounded up to the next octet boundary and the network is expanded into
    one `a.b.*`-style pattern per resulting subnet (a literal IP for /32).

    Args:
        network: An `ipaddress.IPv4Network` with a non-zero prefix length.

    Returns:
        The list of patterns covering exactly `network`.
    """
    rounded = ((network.prefixlen + 7) // 8) * 8
    patterns = []
    for subnet in network.subnets(new_prefix=rounded):
        if rounded == 32:
            patterns.append(str(subnet.network_address))
        else:
            octets = str(subnet.network_address).split(".")[: rounded // 8]
            patterns.append(".".join(octets) + ".*")
    return patterns


def _parse_ipv4_cidr(entry):
    """Parse a `juju-no-proxy` CIDR entry as an IPv4 network.

    Host bits are allowed and masked off (`10.1.2.3/8` is `10.0.0.0/8`).

    Args:
        entry: A single `juju-no-proxy` entry containing a `/`.

    Returns:
        The parsed `ipaddress.IPv4Network`.

    Raises:
        ValueError: If `entry` is not a valid CIDR, or is an IPv6 CIDR.
    """
    network = ipaddress.ip_network(entry, strict=False)
    if not isinstance(network, ipaddress.IPv4Network):
        raise ValueError(f"{entry} is not an IPv4 network")
    return network


def _host_entry_to_patterns(entry):
    """Translate a non-CIDR `juju-no-proxy` entry to `-Dhttp.nonProxyHosts` patterns.

    Args:
        entry: A single, non-empty `juju-no-proxy` entry without a `/`.

    Returns:
        The list of patterns: a bare IPv6 address bracketed in its compressed
        form, a leading-dot domain as both the domain and its `*.` wildcard,
        nothing for a lone `.`, else `entry` itself.
    """
    try:
        address = ipaddress.ip_address(entry)
    except ValueError:
        pass
    else:
        if isinstance(address, ipaddress.IPv6Address):
            # URL hosts are usually written in compressed form, e.g. `[fd00::1]`.
            return [f"[{address}]"]
    if entry.startswith("."):
        domain = entry[1:]
        return [domain, f"*{entry}"] if domain else []
    return [entry]


def _convert_no_proxy(no_proxy_value):
    """Convert a comma-separated `juju-no-proxy` value to `-Dhttp.nonProxyHosts`.

    The JVM matches host strings as written in the URL, case-insensitively,
    with `*` wildcards only at the start and/or end of a pattern. Entries are
    translated as follows:

    - IPv4 CIDRs become octet wildcards, expanded exactly when the prefix is
      not octet-aligned (e.g. `172.16.0.0/12` yields 16 `172.N.*` patterns);
      a /32 becomes the literal IP.
    - IPv4 /0 CIDRs are dropped with a dedicated warning, since they would
      bypass the proxy for every destination.
    - IPv6 and invalid CIDRs are dropped with one aggregated warning.
    - Bare IPv6 addresses are bracketed in compressed form, as the JVM
      matches `[::1]`.
    - Leading-dot entries (`.example.com`) yield both `example.com` and
      `*.example.com`, matching the domain and its subdomains. A lone `.`
      is dropped like an empty segment.
    - Anything else is passed through verbatim.

    Duplicates are removed, keeping the first occurrence.

    Args:
        no_proxy_value: The raw comma-separated value, or None/empty.

    Returns:
        The pipe-separated string of patterns (possibly empty).
    """
    if not no_proxy_value:
        return ""

    patterns = []
    unconvertible = []
    match_all = []
    for entry in no_proxy_value.split(","):
        entry = entry.strip()
        if not entry:
            continue
        if "/" not in entry:
            patterns.extend(_host_entry_to_patterns(entry))
            continue
        try:
            network = _parse_ipv4_cidr(entry)
        except ValueError:
            unconvertible.append(entry)
            continue
        if network.prefixlen == 0:
            match_all.append(entry)
        else:
            patterns.extend(_ipv4_network_to_patterns(network))

    if unconvertible:
        logger.warning(
            "Dropping juju-no-proxy entries not expressible by -Dhttp.nonProxyHosts "
            "(IPv6 or invalid CIDR): %s",
            ", ".join(unconvertible),
        )
    if match_all:
        logger.warning(
            "Ignoring %s in juju-no-proxy: it would bypass the proxy for all destinations. "
            "Use '*' if that is intended.",
            ", ".join(match_all),
        )

    return "|".join(dict.fromkeys(patterns))


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
