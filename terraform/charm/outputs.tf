output "application" {
  description = "The full juju_application resource for the deployed trino-k8s charm."
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
    for key, endpoint in local.provided_endpoints : key => {
      kind     = "endpoint"
      name     = juju_application.this.name
      endpoint = endpoint
    }
  }
}

output "requires" {
  description = "Required endpoint objects for cross-charm integration."
  value = {
    for key, endpoint in local.required_endpoints : key => {
      kind     = "endpoint"
      name     = juju_application.this.name
      endpoint = endpoint
    }
  }
}
