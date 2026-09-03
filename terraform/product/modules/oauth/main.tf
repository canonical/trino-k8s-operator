# Copyright 2023 Canonical Ltd.
# See LICENSE file for licensing details.

resource "juju_application" "this" {
  name       = var.app_name
  model_uuid = var.model_uuid

  charm {
    name     = "oauth-external-idp-integrator"
    channel  = var.channel
    revision = var.revision
    base     = var.base
  }

  config             = var.config
  constraints        = var.constraints
  endpoint_bindings  = var.endpoint_bindings
  resources          = var.resources
  storage_directives = var.storage_directives
  units              = var.units

  dynamic "expose" {
    for_each = local.expose_blocks
    content {
      cidrs     = expose.value.cidrs
      endpoints = expose.value.endpoints
      spaces    = expose.value.spaces
    }
  }
}

resource "juju_offer" "this" {
  for_each = toset(var.offered_endpoints)

  model_uuid       = var.model_uuid
  application_name = juju_application.this.name
  endpoints        = [each.value]
}

locals {
  # null means no expose block; {} means an empty (expose-all) block.
  expose_blocks = var.expose == null ? [] : [var.expose]
}
