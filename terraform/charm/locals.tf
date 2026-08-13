locals {
  # null means no expose block; {} means an empty (expose-all) block.
  expose_blocks = var.expose == null ? [] : [var.expose]

  provided_endpoints = {
    grafana_dashboard = "grafana-dashboard"
    metrics_endpoint  = "metrics-endpoint"
    trino_catalog     = "trino-catalog"
    trino_coordinator = "trino-coordinator"
  }

  required_endpoints = {
    ingress      = "ingress"
    logging      = "logging"
    oauth        = "oauth"
    opensearch   = "opensearch"
    policy       = "policy"
    postgresql   = "postgresql"
    trino_worker = "trino-worker"
  }
}
