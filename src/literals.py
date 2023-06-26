#!/usr/bin/env python3
# Copyright 2023 Canonical Ltd.
# See LICENSE file for licensing details.

"""Literals used by the Trino K8s charm."""

RANGER_PLUGIN_PATH = "/root/ranger-2.3.0-trino-plugin.tar.gz"
INSTALL_PROPERTIES_PATH = "/root/install.properties"

TLS_RELATION = "certificates"
CONF_PATH = "/etc/trino/conf"
CATALOG_PATH = "/etc/trino/catalog"
CONFIG_JINJA = "config.jinja"
CONFIG_PATH = "/etc/trino/config.properties"
LOG_PATH = "/etc/trino/log.properties"
LOG_JINJA = "logging.jinja"
