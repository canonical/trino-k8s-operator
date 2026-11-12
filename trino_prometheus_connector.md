
| Index | BD035 |  |  |
| :---- | :---- | :---- | :---- |
| Title | Trino Prometheus Connector |  |  |
| [**Type**](https://docs.google.com/document/d/1lStJjBGW7lyojgBhxGLUNnliUocYWjAZ1VEbbVduX54/edit?usp=sharing) | **Author(s)** | [**Status**](https://docs.google.com/document/d/1lStJjBGW7lyojgBhxGLUNnliUocYWjAZ1VEbbVduX54/edit?usp=sharing) | **Created** |
| Implementation | [Ceng Tünaydın](mailto:ceng.tunaydin@canonical.com) | Completed | 10 Nov 2026 |
|  | **Reviewer(s)** | **Status** | **Date** |
|  | [Mert Alp Taytak](mailto:mertalp.taytak@canonical.com) | Pending Review | Date |
|  | [Kristi Gjerko](mailto:kristi.gjerko@canonical.com) | Pending Review | Date |
|  | [Sergey Burkov](mailto:sergey.burkov@canonical.com) | Pending Review | Date |

# Abstract

Allow the Trino charm to query an existing Prometheus instance using Trino's read-only connector and the existing catalog-config backend/catalog format.

# Rationale

Currently, there is no connector configured for Prometheus. Existing SQL connectors render JDBC settings and replica credentials, not the HTTP configuration required by Prometheus. A dedicated renderer lets users add and maintain Prometheus catalogs through the same charm configuration workflow.

# Specification

## Requirements

The packaged Trino 479 already includes the plugin. Prometheus must be version 2.15.1 or later.

## Design

A catalog entry references a backend and, optionally, a Juju secret. The backend selects the Prometheus connector, supplies the HTTP(S) endpoint and carries extra connector properties. The Prometheus renderer implements the existing catalog interface: it validates the backend, reads the secret when supplied and returns the desired properties, credential files and certificates without writing to the workload container.

The renderer maps the backend URL to prometheus.uri, maps basic credentials to authentication properties or a bearer token to a managed token file, and appends the backend's extra configuration. The existing planner writes, updates and removes the managed files. The existing truststore reconciler imports supplied certificates, including into Java's default CA store for the Prometheus client, and includes CA changes in the restart hash.

| Workload file | Contents |
| :---- | :---- |
| /usr/lib/trino/etc/catalog/CATALOG\_NAME.properties | Connector name, endpoint, optional basic credentials and extra properties. |
| /usr/lib/trino/etc/credentials/CATALOG\_NAME.token | Bearer token, when selected. The properties file contains its path, not the token value. |

## Configuration

Example:

```yaml
catalogs:
  metrics:
    backend: monitoring
backends:
  monitoring:
    connector: prometheus
    url: http://prometheus.example.com:9090
    config: |
      prometheus.query.chunk.size.duration=10m
      prometheus.max.query.range.duration=1h
```

These durations are examples, not defaults. They can be amended for metric volume and Trino memory, and queries can be restricted by timestamp. The default 21-day range can be expensive; the maximum range must be at least the chunk size.

Select the authentication method required by the existing Prometheus endpoint:

| Method | Configuration | Details |
| :---- | :---- | :---- |
| None | Omit secret-id unless a custom CA is needed. | A CA-only secret is supported. |
| Basic | Set secret-id | The Juju secret contains user and password. |
| Bearer token | Set secret-id | The Juju secret contains bearer-token. |

As with PostgreSQL, credentials come from Juju secrets, given to the Trino applications that read them. Unlike PostgreSQL, this connector queries metrics over HTTP and has no read/write replicas, so it uses direct secret fields rather than the nested replica structure used by SQL connectors.

### Secret Schema

For basic authentication:

```yaml
user: <username>
password: <password>
```

For bearer-token authentication:

```yaml
bearer-token: <token>
```

Basic and bearer authentication cannot be combined. Authentication values must be nonempty, single-line strings. For an endpoint using a private CA or a self-signed certificate, add tls-ca to either secret above, or use a CA-only secret when authentication is not required:

```yaml
tls-ca: |
  -----BEGIN CERTIFICATE-----
  <certificate body>
  -----END CERTIFICATE-----
```

Supply the issuing CA certificate or the self-signed server certificate in PEM format. The existing reconciler imports it under a catalog-specific alias on the coordinator and workers, including into Java's default CA store used by the Prometheus client. Certificate changes contribute to the CA-set restart hash. This store is shared workload-wide, not isolated to one catalog; bundled CAs are retained and TLS verification remains enabled.

### **Property List**

"Should" means recommended tuning, not required input. Optional properties go in the backend's configuration text; omitted values use Trino's defaults, not charm-hard-coded values.

| Trino property | Requirement | Source / upstream default |
| :---- | :---- | :---- |
| connector.name | Hard-coded | prometheus, selected by backend.connector. |
| prometheus.uri | Must have | backend.url: the reachable Prometheus HTTP(S) endpoint. |
| prometheus.query.chunk.size.duration | Should tune | backend.config; default 1d. |
| prometheus.max.query.range.duration | Should tune | backend.config; default 21d; must be at least the chunk size. |
| prometheus.cache.ttl | Optional | backend.config; default 30s, controlling metric-discovery caching. |
| prometheus.read-timeout | Optional | backend.config; default 10s. |
| prometheus.case-insensitive-name-matching | Optional | backend.config; default false. |
| prometheus.auth.user | Must have for basic auth | Juju secret field user; no default. |
| prometheus.auth.password | Must have for basic auth | Juju secret field password; no default. |
| prometheus.bearer.token.file | Generated for bearer auth | Managed token-file path; content from Juju secret field bearer-token. |
| prometheus.auth.http.header.name | Optional | backend.config; default Authorization. Renames the bearer header, not the basic-auth header. |
| prometheus.http.additional-headers | Optional | backend.config; default none. Use non-sensitive, comma-separated name:value pairs with upstream escaping; do not duplicate the authentication header. |

Do not repeat generated connector, URI, authentication or token-path properties in the additional backend configuration. Trino validates connector-specific property constraints.

## Validation

- The charm validates backend field types and rejects unknown fields through the existing schema validator. Secret validation requires complete basic credentials or a bearer token, rejects mixed authentication modes, and checks that authentication values are nonempty, single-line strings. An optional tls-ca must be nonempty text; a CA-only secret is valid. The existing certificate import checks the certificate format.
- Trino validates connector properties when loading the catalog: query chunk size and maximum query range must each be at least 1ms, the maximum range must be at least the chunk size, and cache TTL and read timeout must each be at least 1s. Trino also rejects additional headers that duplicate the selected authentication header. These checks stay in Trino rather than duplicating its duration parser in the charm.
- Invalid secrets stop catalog rendering. Invalid connector properties can prevent Trino starting in static mode. Invalid certificate imports fail reconciliation instead of disabling TLS verification.

## Updates and Limitations

Properties are easy to change after deployment.

Property and credential changes use the charm’s existing reconciliation process and can restart Trino, briefly interrupting queries.

HTTPS uses the workload's JVM certificate trust, with optional custom CA import through tls-ca and the existing truststore reconciler. Certificate changes and removals follow the existing reconciliation and restart handling. No TLS-verification bypass is added.

# Further Information

The property list is supported by both documentation versions:

- [Trino 479 Prometheus connector](https://trino.io/docs/479/connector/prometheus.html), matching the packaged version.
- [Current Prometheus connector documentation](https://trino.io/docs/current/connector/prometheus.html).

# Spec History and Changelog

| Author(s) | Status | Date | Comment |
| :---- | :---- | :---- | :---- |
| [Ceng Tünaydın](mailto:ceng.tunaydin@canonical.com) | Drafting | 10 Nov 2026 | Initial draft |
| [Ceng Tünaydın](mailto:ceng.tunaydin@canonical.com) | Drafting | 11 Nov 2026 | Clarified the design and validation responsibilities, added secret schemas and enabled custom CA import following review. |
| Person | Approved | Date |  |
|  |  |  |  |
