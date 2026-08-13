# Copyright 2023 Canonical Ltd.
# See LICENSE file for licensing details.

variable "enable_oauth" {
  description = "Whether to deploy the OAuth integrator and relate it to the Trino frontend."
  type        = bool
  default     = false
  nullable    = false
}

variable "logging_config" {
  description = "Model logging configuration, mapped to the Juju model's logging-config key."
  type        = string
  nullable    = false
}

variable "mode" {
  description = "Deployment topology: standalone (single trino application) or cluster (coordinator plus workers)."
  type        = string
  default     = "standalone"
  nullable    = false

  validation {
    condition     = contains(["standalone", "cluster"], var.mode)
    error_message = "mode must be one of: standalone, cluster."
  }
}

variable "model_name" {
  description = "Name for the product-owned Juju model."
  type        = string
  nullable    = false

  validation {
    condition     = length(var.model_name) > 0
    error_message = "model_name must not be empty."
  }
}

variable "oauth" {
  description = "Optional deployment overrides for the oauth-external-idp-integrator charm. Application name is fixed."
  type = object({
    base        = optional(string)
    channel     = optional(string)
    config      = optional(map(string), {})
    constraints = optional(string)
    endpoint_bindings = optional(set(object({
      space    = string
      endpoint = optional(string)
    })), [])
    expose = optional(object({
      cidrs     = optional(string)
      endpoints = optional(string)
      spaces    = optional(string)
    }))
    offered_endpoints  = optional(list(string), [])
    resources          = optional(map(string), {})
    revision           = optional(number)
    storage_directives = optional(map(string), {})
    units              = optional(number)
  })
  default  = {}
  nullable = false
}

variable "oauth_config" {
  description = "Sensitive OAuth identity-provider configuration for oauth-external-idp-integrator. Required when enable_oauth is true."
  type = object({
    authorization_endpoint = optional(string, "https://accounts.google.com/o/oauth2/auth")
    client_id              = string
    client_secret          = string
    introspection_endpoint = optional(string, "https://oauth2.googleapis.com/tokeninfo")
    issuer_url             = optional(string, "https://accounts.google.com")
    jwks_endpoint          = optional(string, "https://www.googleapis.com/oauth2/v3/certs")
    jwt_access_token       = optional(bool, false)
    scope                  = optional(string, "openid email profile")
    token_endpoint         = optional(string, "https://oauth2.googleapis.com/token")
    userinfo_endpoint      = optional(string, "https://www.googleapis.com/oauth2/v1/userinfo")
  })
  default   = null
  nullable  = true
  sensitive = true

  validation {
    condition     = var.enable_oauth == false || var.oauth_config != null
    error_message = "oauth_config must be set when enable_oauth is true."
  }
}

variable "proxy" {
  description = "Model proxy settings. Optional fields map to juju-http-proxy, juju-https-proxy, and juju-no-proxy; unset fields are omitted from model config."
  type = object({
    http     = optional(string)
    https    = optional(string)
    no_proxy = optional(string)
  })
  nullable = false
}

variable "risk" {
  description = "Charmhub risk applied to the Trino channel."
  type        = string
  nullable    = false

  validation {
    condition     = contains(["stable", "candidate", "beta", "edge"], var.risk)
    error_message = "risk must be one of: stable, candidate, beta, edge."
  }
}

variable "self_signed_certificates" {
  description = "Optional deployment overrides for the self-signed-certificates charm. Application name is fixed."
  type = object({
    base        = optional(string)
    channel     = optional(string)
    config      = optional(map(string), {})
    constraints = optional(string)
    endpoint_bindings = optional(set(object({
      space    = string
      endpoint = optional(string)
    })), [])
    expose = optional(object({
      cidrs     = optional(string)
      endpoints = optional(string)
      spaces    = optional(string)
    }))
    offered_endpoints  = optional(list(string), [])
    resources          = optional(map(string), {})
    revision           = optional(number)
    storage_directives = optional(map(string), {})
    units              = optional(number)
  })
  default  = {}
  nullable = false
}

variable "traefik" {
  description = "Optional deployment overrides for the traefik-k8s charm. Application name is fixed; routing_mode is forced to subdomain by the product."
  type = object({
    base        = optional(string)
    channel     = optional(string)
    config      = optional(map(string), {})
    constraints = optional(string)
    endpoint_bindings = optional(set(object({
      space    = string
      endpoint = optional(string)
    })), [])
    expose = optional(object({
      cidrs     = optional(string)
      endpoints = optional(string)
      spaces    = optional(string)
    }))
    offered_endpoints  = optional(list(string), [])
    resources          = optional(map(string), {})
    revision           = optional(number)
    storage_directives = optional(map(string), {})
    trust              = optional(bool)
    units              = optional(number)
  })
  default  = {}
  nullable = false
}

variable "trino" {
  description = "Trino deployment settings shared across roles, plus optional per-role config overrides. Excludes expose; Trino is only reachable through Traefik ingress."
  type = object({
    base               = optional(string)
    config             = optional(map(string), {})
    constraints        = optional(string)
    coordinator_config = optional(map(string), {})
    endpoint_bindings = optional(set(object({
      space    = string
      endpoint = optional(string)
    })), [])
    resources          = optional(map(string), {})
    revision           = optional(number)
    standalone_config  = optional(map(string), {})
    storage_directives = optional(map(string), {})
    worker_config      = optional(map(string), {})
  })
  default  = {}
  nullable = false

  validation {
    condition = alltrue([
      !contains(keys(var.trino.config), "charm-function"),
      !contains(keys(var.trino.coordinator_config), "charm-function"),
      !contains(keys(var.trino.standalone_config), "charm-function"),
      !contains(keys(var.trino.worker_config), "charm-function"),
    ])
    error_message = "trino.config, trino.coordinator_config, trino.standalone_config, and trino.worker_config must not set charm-function; the product derives it from mode."
  }
}

variable "trino_track" {
  description = "Charmhub track combined with risk to form the Trino channel."
  type        = string
  default     = "latest"
  nullable    = false

  validation {
    condition     = length(var.trino_track) > 0
    error_message = "trino_track must not be empty."
  }
}

variable "worker_units" {
  description = "Number of trino-worker units to deploy in cluster mode. Ignored in standalone mode."
  type        = number
  default     = 3
  nullable    = false

  validation {
    condition     = var.worker_units >= 1
    error_message = "worker_units must be greater than or equal to 1."
  }
}
