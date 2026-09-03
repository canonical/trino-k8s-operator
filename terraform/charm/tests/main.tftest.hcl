# Copyright 2023 Canonical Ltd.
# See LICENSE file for licensing details.

# tests/setup creates one ephemeral K8s model shared by every plan-only run below. Only
# "offer_requested_apply" applies real resources into it, since offer URLs are provider-computed
# and cannot be asserted at plan time.
run "setup" {
  module {
    source = "./tests/setup"
  }
}

run "basic_deploy" {
  command = plan

  variables {
    model_uuid = run.setup.model_uuid
    channel    = "latest/edge"
  }

  assert {
    condition     = output.application.name == "trino-k8s"
    error_message = "default app_name did not match expected trino-k8s"
  }

  assert {
    condition     = length(output.offers) == 0
    error_message = "no offers should be created when offered_endpoints is empty"
  }

  assert {
    condition = alltrue([
      output.provides["grafana_dashboard"].kind == "endpoint",
      output.provides["grafana_dashboard"].name == "trino-k8s",
      output.provides["grafana_dashboard"].endpoint == "grafana-dashboard",
      output.provides["metrics_endpoint"].kind == "endpoint",
      output.provides["metrics_endpoint"].endpoint == "metrics-endpoint",
      output.provides["trino_catalog"].kind == "endpoint",
      output.provides["trino_catalog"].endpoint == "trino-catalog",
      output.provides["trino_coordinator"].kind == "endpoint",
      output.provides["trino_coordinator"].endpoint == "trino-coordinator",
    ])
    error_message = "provides endpoint objects did not match the expected shape"
  }

  assert {
    condition = alltrue([
      output.requires["ingress"].kind == "endpoint",
      output.requires["ingress"].name == "trino-k8s",
      output.requires["ingress"].endpoint == "ingress",
      output.requires["logging"].endpoint == "logging",
      output.requires["oauth"].endpoint == "oauth",
      output.requires["opensearch"].endpoint == "opensearch",
      output.requires["policy"].endpoint == "policy",
      output.requires["postgresql"].endpoint == "postgresql",
      output.requires["trino_worker"].endpoint == "trino-worker",
    ])
    error_message = "requires endpoint objects did not match the expected shape"
  }
}

run "resources_default" {
  command = plan

  variables {
    model_uuid = run.setup.model_uuid
  }

  assert {
    condition     = length(output.application.resources) == 0
    error_message = "empty resources should use resources bundled with the charm revision"
  }
}

run "resources_override" {
  command = plan

  variables {
    model_uuid = run.setup.model_uuid
    resources  = { "trino-image" = "docker.io/example/trino-image:test" }
  }

  assert {
    condition     = output.application.resources["trino-image"] == "docker.io/example/trino-image:test"
    error_message = "resource override was not forwarded to the application"
  }
}

run "offer_requested_plan" {
  command = plan

  variables {
    model_uuid        = run.setup.model_uuid
    app_name          = "trino-k8s-offer"
    offered_endpoints = ["trino-catalog"]
  }

  assert {
    condition     = length(output.offers) == 1 && contains(keys(output.offers), "trino-catalog")
    error_message = "a trino-catalog offer was not created when requested"
  }
}

# CI-only: requires a live Juju/K8s controller because the offer URL is computed by the provider
# and is unknown at plan time.
run "offer_requested_apply" {
  command = apply

  variables {
    model_uuid        = run.setup.model_uuid
    app_name          = "trino-k8s-offer"
    offered_endpoints = ["trino-catalog"]
  }

  assert {
    condition     = output.offers["trino-catalog"].url != ""
    error_message = "the trino-catalog offer URL was empty after apply"
  }
}

run "invalid_offered_endpoint" {
  command = plan

  variables {
    model_uuid        = run.setup.model_uuid
    offered_endpoints = ["not-a-real-endpoint"]
  }

  expect_failures = [var.offered_endpoints]
}
