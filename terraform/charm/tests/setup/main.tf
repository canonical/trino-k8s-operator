terraform {
  required_version = ">= 1.14"
  required_providers {
    juju = {
      source  = "juju/juju"
      version = "~> 2.0"
    }
  }
}

provider "juju" {}

variable "k8s_cloud_name" {
  description = "Name of the Kubernetes cloud registered on the controller."
  type        = string
  default     = "tfk8s"
}

variable "k8s_credential_name" {
  description = "Name of the credential for the Kubernetes cloud."
  type        = string
  default     = "tfk8s"
}

resource "juju_model" "test_model" {
  name       = "tf-testing-trino-charm-${formatdate("YYYYMMDDhhmmss", timestamp())}"
  credential = var.k8s_credential_name

  cloud {
    name = var.k8s_cloud_name
  }
}

output "model_uuid" {
  value = juju_model.test_model.uuid
}
