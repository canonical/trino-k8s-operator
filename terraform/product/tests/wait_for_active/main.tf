terraform {
  required_version = ">= 1.14"
  required_providers {
    external = {
      source  = "hashicorp/external"
      version = "~> 2.3"
    }
  }
}

variable "model_uuid" {
  description = "UUID of the model containing the application to poll."
  type        = string
}

variable "app_name" {
  description = "Application name to poll for active status."
  type        = string
}

variable "timeout" {
  description = "Maximum seconds to wait for the application to reach active status."
  type        = number
}

# tflint-ignore: terraform_unused_declarations
data "external" "app_status" {
  program = ["sh", "${path.module}/wait_for_active.sh", var.model_uuid, var.app_name, tostring(var.timeout)]
}
