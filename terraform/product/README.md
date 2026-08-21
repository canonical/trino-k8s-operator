# trino-k8s product module

Deploys a ready-to-use Trino product: the `trino-k8s` charm (standalone or as a coordinator plus
worker pool), Traefik ingress, self-signed TLS certificates, and an optional OAuth integrator. The
product owns and creates the Juju model it deploys into; charm deployment details live in
`terraform/charm` and the dependency wrappers in `modules/traefik`, `modules/tls`, and
`modules/oauth`.

## Prerequisites

The caller must configure an authenticated `juju` provider (controller and credentials) before
using this module, equivalent to being logged in for `juju add-model <name>`. This module does not
accept `juju_controller`, cloud, credential, owner, or region inputs; it uses the caller's provider
context to create the model.

**The product owns the created model's lifecycle.** Destroying this module destroys the model and
every application it contains.

Reaching Traefik's ingress from outside the cluster requires a Kubernetes LoadBalancer
provider. If using MicroK8s, enable one before expecting an external address, otherwise the
`traefik-k8s-lb` Kubernetes service stays `<pending>`:

    microk8s enable metallb:<start-ip>-<end-ip>

Without a LoadBalancer you can still reach ingress for testing via the service ClusterIP or
a NodePort using a `Host` header (see "Ingress and exposure"). The bundled self-signed
certificate is not trusted by clients, so use `curl -k` or add the CA to your trust store.

## Modes

- `mode = "standalone"` (default): deploys one `trino` application with `charm-function = all`,
  one unit, and the `trino-catalog` offer.
- `mode = "cluster"`: deploys `trino` as a one-unit coordinator (`charm-function = coordinator`,
  owns ingress, OAuth, and the `trino-catalog` offer) and `trino-worker` as a worker pool
  (`charm-function = worker`, `worker_units` units, related to the coordinator over
  `trino-coordinator`/`trino-worker`).

Traefik and self-signed certificates are always deployed. OAuth and its relation are created only
when `enable_oauth = true`.

## Model configuration

- `logging_config` is required and maps to the model's `logging-config` key.
- `proxy` is required. Its optional `http`, `https`, and `no_proxy` fields map to
  `juju-http-proxy`, `juju-https-proxy`, and `juju-no-proxy`; unset fields are omitted from model
  configuration rather than set to empty strings. Pass `proxy = {}` to explicitly request no
  proxy.

## Ingress and exposure

Trino is never exposed directly through `juju_application.expose`; Traefik is the public entry
point and terminates TLS with the self-signed certificates deployed by this module. Replace
`self_signed_certificates` with a production certificate provider integration where appropriate;
the bundled charm is suitable for development only.

The product forces Traefik's `routing_mode` to `subdomain`. Set an external hostname so ingress
produces usable URLs:

    traefik = {
      config = {
        external_hostname = "trino.test"
      }
    }

With subdomain routing the frontend (standalone or coordinator) is served at:

    https://<model_name>-<app>.<external_hostname>/

For example, `model_name = "trino-standalone"` and `external_hostname = "trino.test"` yields
`https://trino-standalone-trino.trino.test/`. Ask Traefik for the authoritative URL rather than
constructing it by hand:

    juju run traefik-k8s/0 show-proxied-endpoints --format yaml

To smoke-test without external DNS or a LoadBalancer, target the Traefik service ClusterIP (or a
node IP and the service NodePort) and override the host:

    curl -sk -H "Host: <model_name>-<app>.<external_hostname>" \
      https://<traefik-clusterip>/v1/info

## OAuth

Enabling `enable_oauth` requires a non-null `oauth_config`. `oauth_config` is marked sensitive and
its `client_id`/`client_secret` are wrapped with `sensitive()` before being passed to the charm
config. **OAuth credentials are still written to Terraform state in plain form for the deployed
charm to consume; use an encrypted remote backend.**

## Offers

`offers.trino_catalog` exports the `trino-catalog` offer URL from the standalone or coordinator
application. Terraform does not export any credentials: Juju creates per-relation credentials only
when a remote model consumes the offer.

## Inputs

| Name | Type | Default | Description |
| --- | --- | --- | --- |
| `enable_oauth` | `bool` | `false` | Deploy the OAuth integrator and relate it to the Trino frontend. |
| `logging_config` | `string` | None | Required. Maps to the model's `logging-config`. |
| `mode` | `string` | `"standalone"` | One of `standalone`, `cluster`. |
| `model_name` | `string` | None | Required, non-empty name for the created model. |
| `oauth` | object | `{}` | Optional deployment overrides for `oauth-external-idp-integrator` (base, channel, config, constraints, endpoint bindings, exposure, offered endpoints, resources, revision, storage directives, units). |
| `oauth_config` | sensitive object | `null` | Required when `enable_oauth`. Google-style OIDC defaults; `client_id` and `client_secret` are required. |
| `proxy` | object | None | Required. `http`, `https`, `no_proxy` are optional. |
| `risk` | `string` | None | Required; one of `stable`, `candidate`, `beta`, `edge`. Applies only to the Trino channel. |
| `self_signed_certificates` | object | `{}` | Optional deployment overrides for `self-signed-certificates`. |
| `traefik` | object | `{}` | Optional deployment overrides for `traefik-k8s`, including `trust`. `routing_mode` is always forced to `subdomain`. |
| `trino` | object | `{}` | Shared `base`, `constraints`, `endpoint_bindings`, `resources`, `revision`, `storage_directives`, plus `config`/`coordinator_config`/`standalone_config`/`worker_config` maps. None of the four config maps may set `charm-function`; the product derives it from `mode`. |
| `trino_track` | `string` | `"latest"` | Non-empty track combined with `risk` to form the Trino channel. |
| `worker_units` | `number` | `3` | At least `1`. Ignored in standalone mode. |

Every dependency deployment object (`oauth`, `self_signed_certificates`, `traefik`) includes
`resources = {}` by default; leaving it empty uses the resources bundled with the selected charm
revision, and setting it overrides specific charm resources or OCI images. `trino.resources`
behaves the same way and is forwarded unchanged to every Trino charm module call.

## Outputs

| Name | Description |
| --- | --- |
| `metadata` | `{ version, deployed_at, updated_at }`. Timestamps come from `time_static` resources; `updated_at` changes only when user-visible product state changes. |
| `models` | `{ trino = { model_uuid, components } }`. `components` holds the full `juju_application` object for every deployed application, keyed by `trino` (standalone) or `trino_coordinator`/`trino_worker` (cluster), plus `traefik`, `self_signed_certificates`, and `oauth_external_idp_integrator` when enabled. |
| `offers` | `{ trino_catalog = <offer URL> }`. |

## Examples

### Standalone

```hcl
module "trino_product" {
  source = "./terraform/product"

  model_name     = "trino-standalone"
  logging_config = "<root>=INFO"
  proxy          = {}
  risk           = "edge"
}
```

### Cluster

```hcl
module "trino_product" {
  source = "./terraform/product"

  model_name     = "trino-cluster"
  logging_config = "<root>=INFO"
  proxy          = {}
  risk           = "edge"
  mode           = "cluster"
  worker_units   = 3
}
```

### OAuth-enabled

```hcl
module "trino_product" {
  source = "./terraform/product"

  model_name     = "trino-oauth"
  logging_config = "<root>=INFO"
  proxy          = {}
  risk           = "edge"
  enable_oauth   = true

  oauth_config = {
    client_id     = var.oauth_client_id
    client_secret = var.oauth_client_secret
  }
}
```

## Testing

Run from this directory:

```shell
terraform init
terraform fmt -check
terraform validate
terraform test
```
