# oauth-external-idp-integrator dependency module

Deploys exactly one `oauth-external-idp-integrator` charm application and, optionally, Juju
offers for its provided endpoints. This module owns only the charm deployment; the OAuth
configuration object and all integrations belong to the product module in `terraform/product`.

## Prerequisites

The caller must configure an authenticated `juju` provider (controller and credentials) before
using this module. The module accepts a `model_uuid` and does not create, select, or authenticate
against a model or controller itself.

## Kubernetes-only exception

`oauth-external-idp-integrator` is deployed here as a Kubernetes charm. This module intentionally
omits the `machines` input present in machine-charm modules; there is no way to target specific
machines for a Kubernetes application.

## Credential storage warning

OAuth `config` values, including client secrets, are stored in Terraform state. Use an encrypted
remote backend when this module is used with real credentials.

## Inputs

| Name | Type | Default | Nullable | Description |
| --- | --- | --- | --- | --- |
| `app_name` | `string` | `"oauth-external-idp-integrator"` | No | Application name for the deployment. |
| `base` | `string` | `"ubuntu@22.04"` | Yes | Operating system base passed to the charm block. |
| `channel` | `string` | `"latest/edge"` | No | Charmhub channel to deploy from. |
| `config` | `map(string)` | `{}` | No | Charm configuration options, passed unchanged. |
| `constraints` | `string` | `null` | Yes | Juju deployment constraints. |
| `endpoint_bindings` | `set(object({ space = string, endpoint = optional(string) }))` | `[]` | No | Network space bindings; omitted `endpoint` binds the application default. |
| `expose` | `object({ cidrs = optional(string), endpoints = optional(string), spaces = optional(string) })` | `null` | Yes | `null` omits exposure; `{}` exposes all endpoints. |
| `model_uuid` | `string` | None | No | Target Juju model UUID. Required. |
| `offered_endpoints` | `list(string)` | `[]` | No | Provided endpoints to offer. Must be `oauth`. |
| `resources` | `map(string)` | `{}` | No | Resource/image overrides. Empty uses the resources bundled with the selected charm revision. |
| `revision` | `number` | `null` | Yes | Charm revision. Null selects the latest revision on `channel`. |
| `storage_directives` | `map(string)` | `{}` | No | Juju storage directives for the application. |
| `units` | `number` | `1` | No | Number of units. Must be at least `1`. |

## Outputs

| Name | Description |
| --- | --- |
| `application` | The full `juju_application` resource. |
| `offers` | Map keyed by offered endpoint: `{ kind = "offer", url = <offer URL> }`. Empty when `offered_endpoints` is empty. |
| `provides` | `oauth` as `{ kind = "endpoint", name = <application name>, endpoint = "oauth" }`. |
| `requires` | Empty map; `oauth-external-idp-integrator` requires nothing. |

## Resource overrides

Set `resources = { "<resource-name>" = "<revision-or-oci-url>" }` to pin a specific resource.
Leaving `resources` empty (the default) uses whatever resource Juju resolves for the deployed
charm revision.

## Example

```hcl
module "oauth" {
  source = "./terraform/product/modules/oauth"

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
