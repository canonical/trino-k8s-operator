# Copyright 2023 Canonical Ltd.
# See LICENSE file for licensing details.

# Pydantic validators take class as first value
# which causes a false positive with ruff for N805.
# ruff: noqa: N805

"""Module for a Pydantic model that is used for the charm configuration."""

import json
import re
from typing import Optional

import yaml
from charms.data_platform_libs.v0.data_models import BaseConfigModel
from pydantic import root_validator, validator

# Trino duration: digits + optional decimal + unit (ms, s, m, h, d)
_DURATION_RE = re.compile(r"^\d+(\.\d+)?(ms|s|m|h|d)$", re.IGNORECASE)

# Trino memory quantity: digits + data-size suffix
_TRINO_MEMORY_RE = re.compile(r"^\d+(\.\d+)?(B|kB|MB|GB|TB|PB)$")

# Kubernetes resource quantity: plain number or number + binary/decimal suffix
_K8S_QUANTITY_RE = re.compile(r"^\d+(\.\d+)?(Ki|Mi|Gi|Ti|Pi|Ei|K|M|G|T|P|E|m)?$")


def _validate_duration(v: str, field_label: str) -> str:
    """Validate a Trino duration string (e.g. '10m', '30s', '1h').

    Args:
        v: The value to validate.
        field_label: Field name used in the error message.

    Returns:
        The original value if valid.

    Raises:
        ValueError: If the format is not a recognised Trino duration.
    """
    if not _DURATION_RE.match(v):
        raise ValueError(f"{field_label} must be a duration like '10s', '5m', '2h'; got {v!r}")
    return v


def _validate_trino_memory(v: str, field_label: str) -> str:
    """Validate a Trino memory quantity string (e.g. '2GB', '512MB').

    Args:
        v: The value to validate.
        field_label: Field name used in the error message.

    Returns:
        The original value if valid.

    Raises:
        ValueError: If the format is not a recognised Trino memory quantity.
    """
    if not _TRINO_MEMORY_RE.match(v):
        raise ValueError(f"{field_label} must be a memory quantity like '512MB', '2GB'; got {v!r}")
    return v


def _validate_k8s_quantity(v: str, field_label: str) -> str:
    """Validate a Kubernetes resource quantity (e.g. '1Gi', '100m', '2').

    Args:
        v: The value to validate.
        field_label: Field name used in the error message.

    Returns:
        The original value if valid.

    Raises:
        ValueError: If the format is not a recognised Kubernetes quantity.
    """
    if not _K8S_QUANTITY_RE.match(v):
        raise ValueError(
            f"{field_label} must be a Kubernetes quantity like '512Mi', '1Gi', '100m'; got {v!r}"
        )
    return v


class CharmConfig(BaseConfigModel):
    """Typed configuration for the charm."""

    log_level: str = "info"
    google_client_id: Optional[str] = None
    google_client_secret: Optional[str] = None
    oidc_secret_id: Optional[str] = None
    web_proxy: Optional[str] = None
    ranger_service_name: Optional[str] = None
    external_hostname: Optional[str] = None
    tls_secret_name: Optional[str] = None
    charm_function: str = "all"
    discovery_uri: Optional[str] = None
    catalog_config: Optional[str] = None
    postgresql_catalog_config: Optional[str] = None
    catalog_exclusions: Optional[str] = None
    resource_groups_config: Optional[str] = None
    session_property_manager_config: Optional[str] = None
    oauth_user_mapping: Optional[str] = None
    acl_mode_default: str = "owner"
    acl_user_pattern: str = ".*"
    acl_catalog_pattern: str = ".*"
    user_secret_id: Optional[str] = None
    additional_jvm_options: Optional[str] = None
    coordinator_request_timeout: str = "10m"
    coordinator_connect_timeout: str = "30s"
    worker_request_timeout: str = "30s"
    max_concurrent_queries: int = 50
    query_max_cpu_time: Optional[str] = None
    query_max_run_time: Optional[str] = None
    query_max_memory_per_node: Optional[str] = None
    query_max_memory: Optional[str] = None
    query_max_total_memory: Optional[str] = None
    memory_heap_headroom_per_node: Optional[str] = None
    workload_memory_requests: Optional[str] = None
    workload_memory_limits: Optional[str] = None
    workload_cpu_requests: Optional[str] = None
    workload_cpu_limits: Optional[str] = None

    # ── Enum / fixed-choice validators ────────────────────────────────────

    @validator("log_level", pre=True)
    def validate_log_level(cls, v):
        """Validate log_level is one of the accepted Trino log levels."""
        normalized = str(v).lower()
        if normalized not in {"info", "debug", "warn", "error"}:
            raise ValueError(f"config: invalid log level {str(v)!r}")
        return normalized

    @validator("charm_function")
    def validate_charm_function(cls, v):
        """Validate charm_function is coordinator, worker, or all."""
        if v not in {"coordinator", "worker", "all"}:
            raise ValueError(
                f"Invalid charm-function {v!r}; must be one of: coordinator, worker, all"
            )
        return v

    @validator("acl_mode_default")
    def validate_acl_mode_default(cls, v):
        """Validate acl_mode_default is all, none, or owner."""
        if v not in {"all", "none", "owner"}:
            raise ValueError(
                f"Invalid acl-mode-default value: {v!r}; must be one of: all, none, owner"
            )
        return v

    # ── String sanity validators ───────────────────────────────────────────

    @validator("google_client_id", "google_client_secret")
    def reject_deprecated_oidc_plaintext(cls, v, field):
        """Block plaintext OIDC credentials; require a Juju secret instead."""
        if v is not None:
            option = field.name.replace("_", "-")
            raise ValueError(
                f"{option} is deprecated; store the Google OIDC credentials in a "
                "Juju secret and set oidc-secret-id instead, then unset this option"
            )
        return v

    @validator("web_proxy")
    def validate_web_proxy(cls, v):
        """Reject whitespace-only web-proxy values."""
        if v is not None and not v.strip():
            raise ValueError("web-proxy value cannot be an empty string")
        return v

    @validator("max_concurrent_queries")
    def validate_max_concurrent_queries(cls, v):
        """Enforce max_concurrent_queries is at least 1."""
        if v < 1:
            raise ValueError(f"max-concurrent-queries must be at least 1, got {v!r}")
        return v

    # ── Regex validators ───────────────────────────────────────────────────

    @validator("acl_user_pattern", "acl_catalog_pattern")
    def validate_acl_regex(cls, v, field):
        """Validate that ACL pattern fields are compilable regular expressions."""
        try:
            re.compile(v)
        except re.error as e:
            raise ValueError(
                f"{field.name.replace('_', '-')} is not a valid regular expression: {e}"
            ) from None
        return v

    @validator("oauth_user_mapping")
    def validate_oauth_user_mapping(cls, v):
        """Validate oauth_user_mapping is a compilable regular expression."""
        if v is not None:
            try:
                re.compile(v)
            except re.error as e:
                raise ValueError(
                    f"oauth-user-mapping is not a valid regular expression: {e}"
                ) from None
        return v

    # ── URI / hostname validators ──────────────────────────────────────────

    @validator("discovery_uri")
    def validate_discovery_uri(cls, v):
        """Validate discovery_uri starts with http:// or https://."""
        if v is not None and not re.match(r"^https?://", v):
            raise ValueError(f"discovery-uri must start with http:// or https://, got {v!r}")
        return v

    # ── Duration validators ────────────────────────────────────────────────

    @validator(
        "coordinator_request_timeout",
        "coordinator_connect_timeout",
        "worker_request_timeout",
    )
    def validate_required_duration(cls, v, field):
        """Validate required timeout fields are valid Trino durations."""
        return _validate_duration(v, field.name.replace("_", "-"))

    @validator("query_max_cpu_time", "query_max_run_time")
    def validate_optional_duration(cls, v, field):
        """Validate optional duration fields when provided."""
        if v is not None:
            return _validate_duration(v, field.name.replace("_", "-"))
        return v

    # ── Trino memory validators ────────────────────────────────────────────

    @validator(
        "query_max_memory_per_node",
        "query_max_memory",
        "query_max_total_memory",
        "memory_heap_headroom_per_node",
    )
    def validate_trino_memory(cls, v, field):
        """Validate Trino memory quantity fields when provided."""
        if v is not None:
            return _validate_trino_memory(v, field.name.replace("_", "-"))
        return v

    # ── Kubernetes quantity validators ─────────────────────────────────────

    @validator(
        "workload_memory_requests",
        "workload_memory_limits",
        "workload_cpu_requests",
        "workload_cpu_limits",
    )
    def validate_k8s_quantity(cls, v, field):
        """Validate Kubernetes resource quantity fields when provided."""
        if v is not None:
            return _validate_k8s_quantity(v, field.name.replace("_", "-"))
        return v

    # ── YAML / JSON content validators ─────────────────────────────────────

    @validator("catalog_config")
    def validate_catalog_config(cls, v):
        """Validate catalog_config is valid YAML with required top-level keys."""
        if not v:
            return v
        try:
            parsed = yaml.safe_load(v)
        except yaml.YAMLError as e:
            raise ValueError(f"catalog-config is not valid YAML: {e}") from None
        if parsed is None:
            return v
        if not isinstance(parsed, dict):
            raise ValueError("catalog-config must be a YAML mapping")
        if "catalogs" not in parsed or "backends" not in parsed:
            raise ValueError("catalog-config must have top-level 'catalogs' and 'backends' keys")
        return v

    @validator("postgresql_catalog_config")
    def validate_postgresql_catalog_config(cls, v):
        """Validate postgresql_catalog_config is valid YAML and has well-formed entries."""
        if not v:
            return v
        try:
            parsed = yaml.safe_load(v)
        except yaml.YAMLError as e:
            raise ValueError(f"postgresql-catalog-config is not valid YAML: {e}") from None
        if parsed is None:
            return v
        if not isinstance(parsed, dict):
            raise ValueError("postgresql-catalog-config must be a YAML mapping")
        for app_name, entry in parsed.items():
            if not isinstance(entry, dict):
                raise ValueError(
                    f"postgresql-catalog-config entry for {app_name!r} must be a mapping"
                )
            prefix = entry.get("database_prefix")
            if not prefix or not prefix.endswith("*"):
                raise ValueError(
                    f"postgresql-catalog-config entry for {app_name!r}: "
                    f"database_prefix must be set and end with '*'"
                )
            if not entry.get("ro_catalog_name") and not entry.get("rw_catalog_name"):
                raise ValueError(
                    f"postgresql-catalog-config entry for {app_name!r}: "
                    f"at least one of ro_catalog_name or rw_catalog_name must be set"
                )
        return v

    @validator("catalog_exclusions")
    def validate_catalog_exclusions(cls, v):
        """Validate catalog_exclusions is valid YAML when provided."""
        if not v:
            return v
        try:
            yaml.safe_load(v)
        except yaml.YAMLError as e:
            raise ValueError(f"catalog-exclusions is not valid YAML: {e}") from None
        return v

    @validator("resource_groups_config")
    def validate_resource_groups_config(cls, v):
        """Validate resource_groups_config is valid JSON when provided."""
        if not v:
            return v
        try:
            json.loads(v)
        except json.JSONDecodeError as e:
            raise ValueError(str(e)) from None
        return v

    @validator("session_property_manager_config")
    def validate_session_property_manager_config(cls, v):
        """Validate session_property_manager_config is valid JSON when provided."""
        if not v:
            return v
        try:
            json.loads(v)
        except json.JSONDecodeError as e:
            raise ValueError(str(e)) from None
        return v

    # ── Cross-field validators ─────────────────────────────────────────────

    @root_validator(skip_on_failure=True)
    def validate_oauth_credentials_paired(cls, values):
        """Require google-client-id and google-client-secret to be set together."""
        client_id = values.get("google_client_id")
        client_secret = values.get("google_client_secret")
        if bool(client_id) != bool(client_secret):
            raise ValueError(
                "google-client-id and google-client-secret must both be set or both unset"
            )
        return values

    @root_validator(skip_on_failure=True)
    def validate_postgresql_catalog_name_conflicts(cls, values):  # noqa: C901
        """Detect duplicate names and clashes between postgresql-catalog-config and catalog-config.

        Ensures no two postgresql-catalog-config entries share a catalog name and
        no dynamic catalog name clashes with a static catalog from catalog-config.
        """
        pg_raw = values.get("postgresql_catalog_config")
        catalog_raw = values.get("catalog_config")
        if not pg_raw:
            return values

        pg_config = yaml.safe_load(pg_raw)
        if not isinstance(pg_config, dict):
            return values

        static_catalogs: set = set()
        if catalog_raw:
            parsed_catalog = yaml.safe_load(catalog_raw)
            if isinstance(parsed_catalog, dict):
                static_catalogs = set(parsed_catalog.get("catalogs", {}).keys())

        seen: set = set()
        for entry in pg_config.values():
            if not isinstance(entry, dict):
                continue
            for key in ("ro_catalog_name", "rw_catalog_name"):
                name = entry.get(key)
                if not name:
                    continue
                if name in seen:
                    raise ValueError(
                        f"Duplicate catalog name in postgresql-catalog-config: {name!r}"
                    )
                if name in static_catalogs:
                    raise ValueError(
                        f"postgresql-catalog-config catalog {name!r} clashes with catalog-config"
                    )
                seen.add(name)
        return values
