# Copyright 2023 Canonical Ltd.
# See LICENSE file for licensing details.

variable "app_name" {
  description = "Application name for the oauth-external-idp-integrator deployment."
  type        = string
  default     = "oauth-external-idp-integrator"
  nullable    = false

  validation {
    condition     = length(var.app_name) > 0
    error_message = "app_name must not be empty."
  }
}

variable "base" {
  description = "Operating system base passed to the charm block."
  type        = string
  default     = "ubuntu@22.04"
  nullable    = true
}

variable "channel" {
  description = "Charmhub channel to deploy oauth-external-idp-integrator from."
  type        = string
  default     = "latest/edge"
  nullable    = false
}

variable "config" {
  description = "Charm configuration options passed to the application unchanged."
  type        = map(string)
  default     = {}
  nullable    = false
}

variable "constraints" {
  description = "Juju deployment constraints for the application."
  type        = string
  default     = null
  nullable    = true
}

variable "endpoint_bindings" {
  description = "Network space bindings for the application's endpoints."
  type = set(object({
    space    = string
    endpoint = optional(string)
  }))
  default  = []
  nullable = false
}

variable "expose" {
  description = "Exposure configuration. Null omits exposure; an empty object exposes all endpoints."
  type = object({
    cidrs     = optional(string)
    endpoints = optional(string)
    spaces    = optional(string)
  })
  default  = null
  nullable = true
}

variable "model_uuid" {
  description = "UUID of the Juju model to deploy the application into."
  type        = string
  nullable    = false
}

variable "offered_endpoints" {
  description = "Provided endpoints to offer for cross-model integration."
  type        = list(string)
  default     = []
  nullable    = false

  validation {
    condition = alltrue([
      for endpoint in var.offered_endpoints : contains(["oauth"], endpoint)
    ])
    error_message = "offered_endpoints may only contain oauth."
  }
}

variable "resources" {
  description = "Charm resource revisions or OCI image overrides. Empty uses resources bundled with the selected charm revision."
  type        = map(string)
  default     = {}
  nullable    = false
}

variable "revision" {
  description = "Charm revision to deploy. Null selects the latest revision on channel."
  type        = number
  default     = null
  nullable    = true
}

variable "storage_directives" {
  description = "Juju storage directives for the application."
  type        = map(string)
  default     = {}
  nullable    = false
}

variable "units" {
  description = "Number of application units to deploy."
  type        = number
  default     = 1
  nullable    = false

  validation {
    condition     = var.units >= 1
    error_message = "units must be greater than or equal to 1."
  }
}
