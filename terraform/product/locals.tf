# Copyright 2023 Canonical Ltd.
# See LICENSE file for licensing details.

locals {
  # Standalone/coordinator module (whichever is active) owns ingress, OAuth, and the catalog offer.
  components = merge(
    local.is_cluster ? {
      trino_coordinator = module.trino_coordinator[0].application
      trino_worker      = module.trino_worker[0].application
      } : {
      trino = module.trino_standalone[0].application
    },
    {
      self_signed_certificates = module.tls.application
      traefik                  = module.traefik.application
    },
    var.enable_oauth ? {
      oauth_external_idp_integrator = module.oauth[0].application
    } : {},
  )

  coordinator_config = merge(var.trino.config, var.trino.coordinator_config, { "charm-function" = "coordinator" })

  # tflint-ignore: terraform_unused_declarations
  frontend_application = local.is_cluster ? module.trino_coordinator[0].application : module.trino_standalone[0].application
  # Needed to wire ingress/OAuth integrations to whichever module is the active frontend.
  frontend_requires = local.is_cluster ? module.trino_coordinator[0].requires : module.trino_standalone[0].requires

  is_cluster = var.mode == "cluster"

  module_version = "1.0.0"

  # Built here (rather than inline) since it also feeds the update-trigger fingerprint below.
  oauth_charm_config = var.oauth_config == null ? {} : {
    authorization_endpoint = var.oauth_config.authorization_endpoint
    client_id              = sensitive(var.oauth_config.client_id)
    client_secret          = sensitive(var.oauth_config.client_secret)
    introspection_endpoint = var.oauth_config.introspection_endpoint
    issuer_url             = var.oauth_config.issuer_url
    jwks_endpoint          = var.oauth_config.jwks_endpoint
    jwt_access_token       = tostring(var.oauth_config.jwt_access_token)
    scope                  = var.oauth_config.scope
    token_endpoint         = var.oauth_config.token_endpoint
    userinfo_endpoint      = var.oauth_config.userinfo_endpoint
  }

  # Fingerprint change detection without leaking secrets into time_static outputs.
  oauth_config_fingerprint = var.oauth_config == null ? "" : nonsensitive(sha256(jsonencode(var.oauth_config)))

  standalone_config = merge(var.trino.config, var.trino.standalone_config, { "charm-function" = "all" })

  trino_catalog_offer_url = local.is_cluster ? module.trino_coordinator[0].offers["trino-catalog"].url : module.trino_standalone[0].offers["trino-catalog"].url

  trino_channel = "${var.trino_track}/${var.risk}"

  worker_config = merge(var.trino.config, var.trino.worker_config, { "charm-function" = "worker" })
}
