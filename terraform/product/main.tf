# Copyright 2023 Canonical Ltd.
# See LICENSE file for licensing details.

resource "juju_model" "this" {
  name = var.model_name

  config = merge(
    {
      "logging-config" = var.logging_config
    },
    # Unset proxy fields are omitted rather than set to empty strings.
    {
      for key, value in {
        "juju-http-proxy"  = var.proxy.http
        "juju-https-proxy" = var.proxy.https
        "juju-no-proxy"    = var.proxy.no_proxy
      } : key => value if value != null
    },
  )
}

module "trino_standalone" {
  source = "../charm"
  count  = var.mode == "standalone" ? 1 : 0

  app_name   = "trino"
  model_uuid = juju_model.this.uuid
  units      = 1

  base               = var.trino.base
  channel            = local.trino_channel
  config             = local.standalone_config
  constraints        = var.trino.constraints
  endpoint_bindings  = var.trino.endpoint_bindings
  offered_endpoints  = ["trino-catalog"]
  resources          = var.trino.resources
  revision           = var.trino.revision
  storage_directives = var.trino.storage_directives
  trust              = true
}

module "trino_coordinator" {
  source = "../charm"
  count  = local.is_cluster ? 1 : 0

  app_name   = "trino"
  model_uuid = juju_model.this.uuid
  units      = 1

  base               = var.trino.base
  channel            = local.trino_channel
  config             = local.coordinator_config
  constraints        = var.trino.constraints
  endpoint_bindings  = var.trino.endpoint_bindings
  offered_endpoints  = ["trino-catalog"]
  resources          = var.trino.resources
  revision           = var.trino.revision
  storage_directives = var.trino.storage_directives
  trust              = true
}

module "trino_worker" {
  source = "../charm"
  count  = local.is_cluster ? 1 : 0

  app_name   = "trino-worker"
  model_uuid = juju_model.this.uuid
  units      = var.worker_units

  base               = var.trino.base
  channel            = local.trino_channel
  config             = local.worker_config
  constraints        = var.trino.constraints
  endpoint_bindings  = var.trino.endpoint_bindings
  offered_endpoints  = []
  resources          = var.trino.resources
  revision           = var.trino.revision
  storage_directives = var.trino.storage_directives
  trust              = true
}

module "traefik" {
  source = "./modules/traefik"

  app_name   = "traefik-k8s"
  model_uuid = juju_model.this.uuid

  base    = var.traefik.base
  channel = var.traefik.channel
  # Forced last so caller-supplied config cannot select path-based routing.
  config             = merge(var.traefik.config, { "routing_mode" = "subdomain" })
  constraints        = var.traefik.constraints
  endpoint_bindings  = var.traefik.endpoint_bindings
  expose             = var.traefik.expose
  offered_endpoints  = var.traefik.offered_endpoints
  resources          = var.traefik.resources
  revision           = var.traefik.revision
  storage_directives = var.traefik.storage_directives
  trust              = var.traefik.trust
  units              = var.traefik.units
}

module "tls" {
  source = "./modules/tls"

  app_name   = "self-signed-certificates"
  model_uuid = juju_model.this.uuid

  base               = var.self_signed_certificates.base
  channel            = var.self_signed_certificates.channel
  config             = var.self_signed_certificates.config
  constraints        = var.self_signed_certificates.constraints
  endpoint_bindings  = var.self_signed_certificates.endpoint_bindings
  expose             = var.self_signed_certificates.expose
  offered_endpoints  = var.self_signed_certificates.offered_endpoints
  resources          = var.self_signed_certificates.resources
  revision           = var.self_signed_certificates.revision
  storage_directives = var.self_signed_certificates.storage_directives
  units              = var.self_signed_certificates.units
}

module "oauth" {
  source = "./modules/oauth"
  count  = var.enable_oauth ? 1 : 0

  app_name   = "oauth-external-idp-integrator"
  model_uuid = juju_model.this.uuid

  base               = var.oauth.base
  channel            = var.oauth.channel
  config             = local.oauth_charm_config
  constraints        = var.oauth.constraints
  endpoint_bindings  = var.oauth.endpoint_bindings
  expose             = var.oauth.expose
  offered_endpoints  = var.oauth.offered_endpoints
  resources          = var.oauth.resources
  revision           = var.oauth.revision
  storage_directives = var.oauth.storage_directives
  units              = var.oauth.units
}

resource "juju_integration" "traefik_certificates" {
  model_uuid = juju_model.this.uuid

  application {
    name     = module.traefik.requires["certificates"].name
    endpoint = module.traefik.requires["certificates"].endpoint
  }

  application {
    name     = module.tls.provides["certificates"].name
    endpoint = module.tls.provides["certificates"].endpoint
  }
}

resource "juju_integration" "trino_cluster" {
  count = local.is_cluster ? 1 : 0

  model_uuid = juju_model.this.uuid

  application {
    name     = module.trino_coordinator[0].provides["trino_coordinator"].name
    endpoint = module.trino_coordinator[0].provides["trino_coordinator"].endpoint
  }

  application {
    name     = module.trino_worker[0].requires["trino_worker"].name
    endpoint = module.trino_worker[0].requires["trino_worker"].endpoint
  }
}

resource "juju_integration" "trino_ingress" {
  model_uuid = juju_model.this.uuid

  application {
    name     = local.frontend_requires["ingress"].name
    endpoint = local.frontend_requires["ingress"].endpoint
  }

  application {
    name     = module.traefik.provides["ingress"].name
    endpoint = module.traefik.provides["ingress"].endpoint
  }
}

resource "juju_integration" "trino_oauth" {
  count = var.enable_oauth ? 1 : 0

  model_uuid = juju_model.this.uuid

  application {
    name     = local.frontend_requires["oauth"].name
    endpoint = local.frontend_requires["oauth"].endpoint
  }

  application {
    name     = module.oauth[0].provides["oauth"].name
    endpoint = module.oauth[0].provides["oauth"].endpoint
  }
}

resource "time_static" "deployed_at" {}

resource "time_static" "updated_at" {
  triggers = {
    model_name               = var.model_name
    proxy                    = jsonencode(var.proxy)
    logging_config           = var.logging_config
    mode                     = var.mode
    trino_channel            = local.trino_channel
    trino_revision           = coalesce(var.trino.revision, -1)
    trino_config             = jsonencode(local.standalone_config)
    trino_worker_config      = jsonencode(local.worker_config)
    trino_coordinator_config = jsonencode(local.coordinator_config)
    trino_resources          = jsonencode(var.trino.resources)
    worker_units             = var.worker_units
    traefik                  = jsonencode(var.traefik)
    self_signed_certificates = jsonencode(var.self_signed_certificates)
    oauth                    = jsonencode(var.oauth)
    enable_oauth             = var.enable_oauth
    oauth_config_fingerprint = local.oauth_config_fingerprint
  }
}
