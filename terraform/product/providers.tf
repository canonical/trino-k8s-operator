terraform {
  required_providers {
    juju = {
      source  = "juju/juju"
      version = "~> 2.0"
    }
    time = {
      source  = "hashicorp/time"
      version = "~> 0.12"
    }
  }
}
