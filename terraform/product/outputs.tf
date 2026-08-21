# Copyright 2023 Canonical Ltd.
# See LICENSE file for licensing details.

output "metadata" {
  description = "Product module metadata: schema version and stable deployment timestamps."
  value = {
    version     = local.module_version
    deployed_at = time_static.deployed_at.rfc3339
    updated_at  = time_static.updated_at.rfc3339
  }
}

output "models" {
  description = "Created Trino model UUID and the full application object for every deployed component, keyed by component name."
  # Sensitive because component application objects include the OAuth integrator's credential config.
  sensitive = true
  value = {
    trino = {
      model_uuid = juju_model.this.uuid
      components = local.components
    }
  }
}

output "offers" {
  description = "Cross-model offer URLs exported by the product."
  value = {
    trino_catalog = local.trino_catalog_offer_url
  }
}
