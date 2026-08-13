# Copyright 2023 Canonical Ltd.
# See LICENSE file for licensing details.

output "application" {
  description = "The full juju_application resource for the deployed self-signed-certificates charm."
  value       = juju_application.this
}

output "offers" {
  description = "Offer objects for each requested offered endpoint, keyed by endpoint name."
  value = {
    for endpoint, offer in juju_offer.this : endpoint => {
      kind = "offer"
      url  = offer.url
    }
  }
}

output "provides" {
  description = "Provided endpoint objects for cross-charm integration."
  value = {
    certificates = {
      kind     = "endpoint"
      name     = juju_application.this.name
      endpoint = "certificates"
    }
  }
}

output "requires" {
  description = "Required endpoint objects for cross-charm integration. self-signed-certificates requires nothing."
  value       = {}
}
