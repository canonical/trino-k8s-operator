# traefik-k8s dependency module

Deploys exactly one `traefik-k8s` charm application and, optionally, Juju offers for its
provided endpoints. This module owns only the charm deployment; the `routing_mode` config value
and all integrations belong to the product module in `terraform/product`.

## Prerequisites

The caller must configure an authenticated `juju` provider (controller and credentials) before
using this module. The module accepts a `model_uuid` and does not create, select, or authenticate
against a model or controller itself.

## Kubernetes-only exception

`traefik-k8s` is a Kubernetes charm. This module intentionally omits the `machines` input present
in machine-charm modules; there is no way to target specific machines for a Kubernetes
application.

## Inputs

| Name | Type | Default | Nullable | Description |
| --- | --- | --- | --- | --- |
| `app_name` | `string` | `"traefik-k8s"` | No | Application name for the deployment. |
| `base` | `string` | `null` | Yes | Operating system base passed to the charm block. |
| `channel` | `string` | `"latest/stable"` | No | Charmhub channel to deploy from. |
| `config` | `map(string)` | `{}` | No | Charm configuration options, passed unchanged. |
| `constraints` | `string` | `null` | Yes | Juju deployment constraints. |
| `endpoint_bindings` | `set(object({ space = string, endpoint = optional(string) }))` | `[]` | No | Network space bindings; omitted `endpoint` binds the application default. |
| `expose` | `object({ cidrs = optional(string), endpoints = optional(string), spaces = optional(string) })` | `null` | Yes | `null` omits exposure; `{}` exposes all endpoints. |
| `model_uuid` | `string` | None | No | Target Juju model UUID. Required. |
| `offered_endpoints` | `list(string)` | `[]` | No | Provided endpoints to offer. Must be `ingress`. |
| `resources` | `map(string)` | `{}` | No | Resource/image overrides. Empty uses the resources bundled with the selected charm revision. |
| `revision` | `number` | `null` | Yes | Charm revision. Null selects the latest revision on `channel`. |
| `storage_directives` | `map(string)` | `{}` | No | Juju storage directives for the application. |
| `trust` | `bool` | `true` | No | Grants the application trust to interact with Kubernetes. |
| `units` | `number` | `1` | No | Number of units. Must be at least `1`. |

## Outputs

| Name | Description |
| --- | --- |
| `application` | The full `juju_application` resource. |
| `offers` | Map keyed by offered endpoint: `{ kind = "offer", url = <offer URL> }`. Empty when `offered_endpoints` is empty. |
| `provides` | `ingress` as `{ kind = "endpoint", name = <application name>, endpoint = "ingress" }`. |
| `requires` | `certificates` as `{ kind = "endpoint", name = <application name>, endpoint = "certificates" }`. |

## Resource overrides

Set `resources = { "<resource-name>" = "<revision-or-oci-url>" }` to pin a specific resource.
Leaving `resources` empty (the default) uses whatever resource Juju resolves for the deployed
charm revision.

## Example

```hcl
module "traefik" {
  source = "./terraform/product/modules/traefik"

  model_uuid = juju_model.this.uuid
}
```

## Testing

Run from this directory:

```shell
terraform init
terraform fmt -check
terraform validate
```
