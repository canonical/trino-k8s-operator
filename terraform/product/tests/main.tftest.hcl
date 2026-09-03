# Copyright 2023 Canonical Ltd.
# See LICENSE file for licensing details.

# The product owns its own model per apply, so every scenario supplies a unique model_name instead
# of sharing a setup module. Plan-level runs cover structural/config assertions that are fully
# known before any provider round trip (module/resource counts, config maps, resource overrides,
# validation failures). Apply-level runs are marked CI-only: they need a live Juju/K8s controller
# because offer URLs and application/unit status are provider-computed.

run "standalone_plan" {
  command = plan

  variables {
    model_name     = "trino-tf-test-standalone"
    logging_config = "<root>=WARNING"
    risk           = "edge"

    proxy = {
      http     = "http://proxy.example:3128"
      https    = "https://proxy.example:3128"
      no_proxy = "localhost,127.0.0.1"
    }
  }

  assert {
    condition = alltrue([
      juju_model.this.config["juju-http-proxy"] == "http://proxy.example:3128",
      juju_model.this.config["juju-https-proxy"] == "https://proxy.example:3128",
      juju_model.this.config["juju-no-proxy"] == "localhost,127.0.0.1",
      juju_model.this.config["logging-config"] == "<root>=WARNING",
    ])
    error_message = "model config did not include all proxy fields and logging-config"
  }

  assert {
    condition     = length(module.trino_standalone) == 1 && length(module.trino_coordinator) == 0 && length(module.trino_worker) == 0
    error_message = "standalone mode must deploy trino and skip coordinator/worker modules"
  }

  assert {
    condition     = length(module.oauth) == 0
    error_message = "oauth must not deploy when enable_oauth is false"
  }

  assert {
    condition     = module.trino_standalone[0].application.config["charm-function"] == "all" && module.trino_standalone[0].application.units == 1
    error_message = "standalone trino must have charm-function=all and one unit"
  }

  assert {
    condition     = module.traefik.application.config["routing_mode"] == "subdomain"
    error_message = "traefik routing_mode must be forced to subdomain"
  }
}

# CI-only: requires a live Juju/K8s controller. Offer URLs and application status are computed by
# the provider and unknown at plan time.
run "standalone_apply" {
  command = apply

  variables {
    model_name     = "trino-tf-test-standalone-apply"
    logging_config = "<root>=WARNING"
    risk           = "edge"
    proxy          = {}
    traefik = {
      config = {
        external_hostname = "trino.test"
      }
    }
  }

  assert {
    condition = alltrue([
      contains(keys(output.models.trino.components), "trino"),
      contains(keys(output.models.trino.components), "traefik"),
      contains(keys(output.models.trino.components), "self_signed_certificates"),
      !contains(keys(output.models.trino.components), "trino_worker"),
      !contains(keys(output.models.trino.components), "trino_coordinator"),
      !contains(keys(output.models.trino.components), "oauth_external_idp_integrator"),
    ])
    error_message = "standalone models.trino.components did not contain exactly the expected applications"
  }

  assert {
    condition     = output.offers.trino_catalog != ""
    error_message = "trino_catalog offer URL was empty after standalone apply"
  }
}

# CI-only: waits for real workload status via the wait_for_active helper.
run "wait_for_standalone_trino_active" {
  module {
    source = "./tests/wait_for_active"
  }

  variables {
    model_uuid = run.standalone_apply.models.trino.model_uuid
    app_name   = "trino"
    timeout    = 1800
  }

  assert {
    condition     = data.external.app_status.result.status == "active"
    error_message = "trino did not reach active status in standalone mode"
  }
}

run "wait_for_standalone_traefik_active" {
  module {
    source = "./tests/wait_for_active"
  }

  variables {
    model_uuid = run.standalone_apply.models.trino.model_uuid
    app_name   = "traefik-k8s"
    timeout    = 1800
  }

  assert {
    condition     = data.external.app_status.result.status == "active"
    error_message = "traefik-k8s did not reach active status"
  }
}

run "wait_for_standalone_tls_active" {
  module {
    source = "./tests/wait_for_active"
  }

  variables {
    model_uuid = run.standalone_apply.models.trino.model_uuid
    app_name   = "self-signed-certificates"
    timeout    = 1800
  }

  assert {
    condition     = data.external.app_status.result.status == "active"
    error_message = "self-signed-certificates did not reach active status"
  }
}

run "cluster_plan" {
  command = plan

  variables {
    model_name     = "trino-tf-test-cluster"
    logging_config = "<root>=WARNING"
    risk           = "edge"
    mode           = "cluster"
    # Verifies proxy = {} omits every proxy key instead of setting empty-string model config.
    proxy = {}
  }

  assert {
    condition = alltrue([
      !contains(keys(juju_model.this.config), "juju-http-proxy"),
      !contains(keys(juju_model.this.config), "juju-https-proxy"),
      !contains(keys(juju_model.this.config), "juju-no-proxy"),
    ])
    error_message = "proxy = {} must omit all proxy keys from model config"
  }

  assert {
    condition     = length(module.trino_standalone) == 0 && length(module.trino_coordinator) == 1 && length(module.trino_worker) == 1
    error_message = "cluster mode must deploy a coordinator and a worker module and skip standalone"
  }

  assert {
    condition     = module.trino_coordinator[0].application.config["charm-function"] == "coordinator"
    error_message = "coordinator trino must have charm-function=coordinator"
  }

  assert {
    condition     = module.trino_worker[0].application.config["charm-function"] == "worker" && module.trino_worker[0].application.units == 3
    error_message = "worker trino must have charm-function=worker and the default worker_units"
  }

  assert {
    condition     = length(module.trino_worker[0].offers) == 0
    error_message = "the worker must not own the trino-catalog offer in cluster mode"
  }

  assert {
    condition     = length(juju_integration.trino_cluster) == 1
    error_message = "the coordinator/worker cluster integration must exist in cluster mode"
  }
}

# CI-only: requires a live Juju/K8s controller.
run "cluster_apply" {
  command = apply

  variables {
    model_name     = "trino-tf-test-cluster-apply"
    logging_config = "<root>=WARNING"
    risk           = "edge"
    mode           = "cluster"
    proxy          = {}
    traefik = {
      config = {
        external_hostname = "trino.test"
      }
    }
  }

  assert {
    condition = alltrue([
      contains(keys(output.models.trino.components), "trino_coordinator"),
      contains(keys(output.models.trino.components), "trino_worker"),
      !contains(keys(output.models.trino.components), "trino"),
      !contains(keys(output.models.trino.components), "oauth_external_idp_integrator"),
    ])
    error_message = "cluster models.trino.components did not contain exactly the expected applications"
  }

  assert {
    condition     = output.offers.trino_catalog != ""
    error_message = "trino_catalog offer URL was empty after cluster apply"
  }
}

# CI-only: waits for real workload status via the wait_for_active helper.
run "wait_for_cluster_coordinator_active" {
  module {
    source = "./tests/wait_for_active"
  }

  variables {
    model_uuid = run.cluster_apply.models.trino.model_uuid
    app_name   = "trino"
    timeout    = 1800
  }

  assert {
    condition     = data.external.app_status.result.status == "active"
    error_message = "the trino coordinator did not reach active status"
  }
}

run "wait_for_cluster_worker_active" {
  module {
    source = "./tests/wait_for_active"
  }

  variables {
    model_uuid = run.cluster_apply.models.trino.model_uuid
    app_name   = "trino-worker"
    timeout    = 1800
  }

  assert {
    condition     = data.external.app_status.result.status == "active"
    error_message = "trino-worker did not reach active status"
  }
}

run "oauth_plan" {
  command = plan

  variables {
    model_name     = "trino-tf-test-oauth"
    logging_config = "<root>=WARNING"
    risk           = "edge"
    proxy          = {}
    enable_oauth   = true

    oauth_config = {
      client_id     = "stub-client-id"
      client_secret = "stub-client-secret" # nosec B105 - non-secret stub used for structural tests only
    }
  }

  assert {
    condition     = length(module.oauth) == 1
    error_message = "the oauth module must deploy when enable_oauth is true"
  }

  assert {
    condition     = length(juju_integration.trino_oauth) == 1
    error_message = "the frontend/oauth integration must exist when enable_oauth is true"
  }

  assert {
    condition     = contains(keys(output.models.trino.components), "oauth_external_idp_integrator")
    error_message = "oauth_external_idp_integrator must appear in models.trino.components when enabled"
  }
}

# OAuth is intentionally validated at plan only. An apply-based scenario deploys and reaches
# active cleanly, but tearing down the trino:oauth relation hangs at the Juju layer
# ("integration deletion ... max duration exceeded"), and juju_integration exposes no delete
# timeout to control it. The oauth_plan run above already asserts the integrator application
# and the trino_oauth integration are planned when enable_oauth is true.

run "resource_overrides_plan" {
  command = plan

  variables {
    model_name     = "trino-tf-test-resources"
    logging_config = "<root>=WARNING"
    risk           = "edge"
    proxy          = {}
    enable_oauth   = true

    oauth_config = {
      client_id     = "stub-client-id"
      client_secret = "stub-client-secret" # nosec B105 - non-secret stub used for structural tests only
    }

    trino                    = { resources = { "trino-image" = "docker.io/example/trino-image:test" } }
    traefik                  = { resources = { "traefik-image" = "docker.io/example/traefik-image:test" } }
    self_signed_certificates = { resources = { "cert-image" = "docker.io/example/cert-image:test" } }
    oauth                    = { resources = { "oauth-image" = "docker.io/example/oauth-image:test" } }
  }

  assert {
    condition     = module.trino_standalone[0].application.resources["trino-image"] == "docker.io/example/trino-image:test"
    error_message = "trino resource override did not reach the juju_application plan"
  }

  assert {
    condition     = module.traefik.application.resources["traefik-image"] == "docker.io/example/traefik-image:test"
    error_message = "traefik resource override did not reach the juju_application plan"
  }

  assert {
    condition     = module.tls.application.resources["cert-image"] == "docker.io/example/cert-image:test"
    error_message = "self-signed-certificates resource override did not reach the juju_application plan"
  }

  assert {
    condition     = module.oauth[0].application.resources["oauth-image"] == "docker.io/example/oauth-image:test"
    error_message = "oauth resource override did not reach the juju_application plan"
  }
}

run "empty_model_name_fails" {
  command = plan

  variables {
    model_name     = ""
    logging_config = "<root>=WARNING"
    risk           = "edge"
    proxy          = {}
  }

  expect_failures = [var.model_name]
}

run "invalid_mode_fails" {
  command = plan

  variables {
    model_name     = "trino-tf-test-invalid-mode"
    logging_config = "<root>=WARNING"
    risk           = "edge"
    proxy          = {}
    mode           = "invalid"
  }

  expect_failures = [var.mode]
}

run "invalid_risk_fails" {
  command = plan

  variables {
    model_name     = "trino-tf-test-invalid-risk"
    logging_config = "<root>=WARNING"
    risk           = "invalid"
    proxy          = {}
  }

  expect_failures = [var.risk]
}

run "oauth_enabled_without_config_fails" {
  command = plan

  variables {
    model_name     = "trino-tf-test-oauth-no-config"
    logging_config = "<root>=WARNING"
    risk           = "edge"
    proxy          = {}
    enable_oauth   = true
  }

  expect_failures = [var.oauth_config]
}

run "charm_function_in_trino_config_fails" {
  command = plan

  variables {
    model_name     = "trino-tf-test-charm-function"
    logging_config = "<root>=WARNING"
    risk           = "edge"
    proxy          = {}

    trino = { config = { "charm-function" = "all" } }
  }

  expect_failures = [var.trino]
}

run "worker_units_below_minimum_fails" {
  command = plan

  variables {
    model_name     = "trino-tf-test-worker-units"
    logging_config = "<root>=WARNING"
    risk           = "edge"
    proxy          = {}
    mode           = "cluster"
    worker_units   = 0
  }

  expect_failures = [var.worker_units]
}
