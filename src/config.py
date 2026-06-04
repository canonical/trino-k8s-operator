# Copyright 2023 Canonical Ltd.
# See LICENSE file for licensing details.

# Pydantic validators take class as first value
# which causes a false positive with ruff for N805.
# ruff: noqa: N805

"""Module for a Pydantic model that is used for the charm configuration."""

from typing import Optional

from charms.data_platform_libs.v0.data_models import BaseConfigModel


class CharmConfig(BaseConfigModel):
    """Typed configuration for the charm."""

    log_level: str = "info"
    google_client_id: Optional[str] = None
    google_client_secret: Optional[str] = None
    web_proxy: Optional[str] = None
    ranger_service_name: Optional[str] = None
    external_hostname: Optional[str] = None
    tls_secret_name: str = "trino-tls"
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
