# Trino K8s Operator
The Charmed Trino K8s Operator delivers automated management on [Trino](https://trino.io/) data virtualization software on top of a Kubernetes cluster. Trino is a distributed SQL query engine designed to query large data sets distributed over one or more heterogeneous data sources.

## Usage
Note: This operator requires the use of juju >= 3.3. More information on setting up your environment can be found [here](CONTRIBUTING.md).

### Single node deployment
To deploy a single node of Trino which acts as both the coordinator and the worker run the below command.
```
# deploy Trino operator:
juju deploy trino-k8s --config charm-function=all
```
### Scalable deployment
To deploy Trino in a production environment you will need to deploy the coordinator and worker separately, and then relate them. The relation serves the purpose of communicating the `discovery-uri` and `catalog-config` from the coordinator to the worker.
```
juju deploy trino-k8s --trust --channel=latest/edge --config charm-function=coordinator
juju deploy trino-k8s --trust --channel=latest/edge --config charm-function=worker trino-k8s-worker

# Relate the two applications
juju relate trino-k8s:trino-coordinator trino-k8s-worker:trino-worker
```

## HTTPS
The Trino Charm is configured to secure communications with relation to a load balancer or proxy server such as Nginx Ingress. Nginx must be configured with a valid, globally trusted TLS certificate.

The load balancer or proxy server accepts TLS connections and forwards them to the Trino coordinator, which runs with default HTTP configuration on the default port, 8080. Client tools can access Trino with the URL exposed by the load balancer.

![trino-communication](trino-tls.svg)

### Ingress
The Trino operator exposes its ports using the Nginx Ingress Integrator operator. You must first make sure to have an Nginx Ingress Controller deployed. To enable TLS connections, you must have a TLS certificate stored as a k8s secret (default name is "trino-tls"). A self-signed certificate for development purposes can be created as follows:

```
# Generate private key
openssl genrsa -out server.key 2048

# Generate a certificate signing request
openssl req -new -key server.key -out server.csr -subj "/CN=trino-k8s"

# Create self-signed certificate
openssl x509 -req -days 365 -in server.csr -signkey server.key -out server.crt -extfile <(printf "subjectAltName=DNS:trino-k8s")

# Create a k8s secret
kubectl create secret tls trino-tls --cert=server.crt --key=server.key
```
This operator can then be deployed and connected to the Trino operator using the Juju command line as follows:

```
# Deploy ingress controller.
microk8s enable ingress:default-ssl-certificate=trino-k8s/trino-tls

juju deploy nginx-ingress-integrator --channel edge --revision 71
juju relate trino-k8s nginx-ingress-integrator
```

Once deployed, the hostname will default to the name of the application (trino-k8s), and can be configured using the external-hostname configuration on the Trino operator.

## Trino catalogs
Adding a catalog to Trino requires user or service account credentials. For this we use Juju secrets.

### Juju secrets
Juju secrets are used to manage connector credentials. The format of these differ by connector type. Note: the same secret can be shared by multiple trino catalogs.

For PostgreSQL, MySQL or Redshift (`postgresql-user-creds.yaml`, `mysql-user-creds.yaml` or `redshift-user-creds.yaml`):
```
rw: 
  user: trino
  password: "pwd1" 
  suffix: _developer
ro:
  user: trino_ro
  password: "pwd2"
```

For PostgreSQL certificates (`certificates.yaml`):
```
postgresql-cert: |
  -----BEGIN CERTIFICATE-----
  YOUR CERTIFICATE CONTENT
  -----END CERTIFICATE-----
```

For BigQuery (`bigquery-service-accounts.yaml`):
```
<your-project-id>: |
    {
      "type": "service_account",
      "project_id": "example-project",
      "private_key_id": "key123",
      "private_key": "-----BEGIN PRIVATE KEY-----\YOUR PRIVATE KEY\n-----END PRIVATE KEY-----",
      "client_email": "test-380@example-project.iam.gserviceaccount.com",
      "client_id": "12345",
      "auth_uri": "https://accounts.google.com/o/oauth2/auth",
      "token_uri": "https://oauth2.googleapis.com/token",
      "auth_provider_x509_cert_url": "https://www.googleapis.com/oauth2/v1/certs",
      "client_x509_cert_url": "https://www.googleapis.com/robot/v1/metadata/x509/test-380.project.iam.gserviceaccount.com",
      "universe_domain": "googleapis.com"
    }
```
For Google sheets (`gsheets-service-accounts.yaml`):
```
<catalog-name>: |
    {
      "type": "service_account",
      "project_id": "example-project",
      "private_key_id": "key123",
      "private_key": "-----BEGIN PRIVATE KEY-----\YOUR PRIVATE KEY\n-----END PRIVATE KEY-----",
      "client_email": "test-380@example-project.iam.gserviceaccount.com",
      "client_id": "12345",
      "auth_uri": "https://accounts.google.com/o/oauth2/auth",
      "token_uri": "https://oauth2.googleapis.com/token",
      "auth_provider_x509_cert_url": "https://www.googleapis.com/oauth2/v1/certs",
      "client_x509_cert_url": "https://www.googleapis.com/robot/v1/metadata/x509/test-380.project.iam.gserviceaccount.com",
      "universe_domain": "googleapis.com"
    }
```
These secrets can be created by running the following:
```
juju add-secret postgresql-credentials replicas#file=postgresql-user-creds.yaml cert#file=certificates.yaml
juju add-secret mysql-credentials replicas#file=mysql-user-creds.yaml
juju add-secret redshift-credentials replicas#file=redshift-user-creds.yaml
juju add-secret bigquery-service-accounts service-accounts#file=bigquery-service-accounts.yaml
juju add-secret gsheets-service-accounts service-accounts#file=gsheets-service-accounts.yaml
```
And access granted to trino coordinator and worker with the following:
```
juju grant-secret <secret-id> trino-k8s
juju grant-secret <secret-id> trino-k8s-worker
```

### The config file
To add or remove catalogs the configuration parameter `catalog-config` should be updated.
The below is an example of the `catalog_config.yaml`. It lists the catalogs to add, and points to a juju secret in which the credentials are stored. Any commonality is included as part of the `backend` these configuration properties will be applied to all catalogs with the same backend.
```
catalogs:
  example: 
    backend: dwh
    database: example  
    secret-id: crt7gpnmp25c760ji150
  mysql_example: 
    backend: mysql
    secret-id: crt7gpnmp25c760ji150
  redshift_example: 
    backend: redshift
    secret-id: crt7gpnmp25c760ji150
  ge_bigquery:
    backend: bigquery
    project: <project-id>
    secret-id: crt7d1vmp25c760ji14g
  gsheet-1:
    backend: gsheets
    metasheet-id: 1Es4HhWALUQjoa-bQh4a8B5HROz7dpGMfq_HbfoaW5LM
    secret-id: csp2ccvmp25c77vadfcg
  metastore_example:
    backend: hive_metastore
backends: 
  dwh:
    connector: postgresql
    url: jdbc:postgresql://<database-host>:5432
    params: ssl=true&sslmode=require&sslrootcert={SSL_PATH}&sslrootcertpassword={SSL_PWD}
    config: |
      case-insensitive-name-matching=true
      decimal-mapping=allow_overflow
      decimal-rounding-mode=HALF_UP
  mysql:
    connector: mysql
    url: jdbc:mysql://<database-host>:3306
    params: sslMode=REQUIRED
    config: |
      case-insensitive-name-matching=true
      decimal-mapping=allow_overflow
      decimal-rounding-mode=HALF_UP
  redshift:
    connector: redshift
    url: jdbc:redshift://<database-host>:5439/<database-name>
    params: SSL=TRUE
    config: |
      case-insensitive-name-matching=true
  bigquery:
    connector: bigquery
    config: |
      bigquery.case-insensitive-name-matching=true
  gsheets:
    connector: gsheets
  hive_metastore:
    connector: hive
    url: thrift://<hive-metastore-host>:9083
```

Note: the allowed fields change significantly by connector, see the Trino documentation on this [here](https://trino.io/docs/current/connector.html).

The `{SSL_PATH}` and `{SSL PWD}` variables will be replaced with the truststore path and password by the charm.

The catalog-config can be applied with the following:

```
juju config trino-k8s catalog-config=@catalog_config.yaml
```

### Resource group manager

Trino's built-in file-based resource group manager can be enabled with the
`resource-groups-config` charm config. The value must be JSON matching the
resource group manager format documented [here](https://trino.io/docs/current/admin/resource-groups.html).

Example `resource_groups.json`:

```json
{
  "rootGroups": [
    {
      "name": "global",
      "softMemoryLimit": "80%",
      "maxQueued": 1000,
      "hardConcurrencyLimit": 100,
      "subGroups": [
        {
          "name": "interactive",
          "softMemoryLimit": "50%",
          "hardConcurrencyLimit": 50,
          "maxQueued": 500
        },
        {
          "name": "pipeline",
          "softMemoryLimit": "30%",
          "hardConcurrencyLimit": 20,
          "maxQueued": 100
        }
      ]
    }
  ],
  "selectors": [
    {
      "user": ".*",
      "group": "global.interactive"
    },
    {
      "clientTags": ["etl"],
      "group": "global.pipeline"
    }
  ]
}
```

Apply it with:

```bash
juju config trino-k8s resource-groups-config=@resource_groups.json
```

### Session property manager

Trino's built-in file-based session property manager can be enabled with the
`session-property-manager-config` charm config. The value must be a JSON array
of match rules, following the Trino documentation [here](https://trino.io/docs/current/admin/session-property-managers.html).

Example `session_property_manager.json`:

```json
[
  {
    "group": "global.*",
    "sessionProperties": {
      "query_max_execution_time": "8h"
    }
  },
  {
    "group": "global.pipeline.*",
    "clientTags": ["etl"],
    "sessionProperties": {
      "scale_writers": "true",
      "hive.insert_existing_partitions_behavior": "overwrite"
    }
  }
]
```

Apply it with:

```bash
juju config trino-k8s session-property-manager-config=@session_property_manager.json
```

### Additional information for Google sheets connector
For the google sheets connector it is worth noting that the sheet that is connected to Trino is not the sheet with the data, but rather a metadata sheet following [this format](https://docs.google.com/spreadsheets/d/1Es4HhWALUQjoa-bQh4a8B5HROz7dpGMfq_HbfoaW5LM/edit?gid=0#gid=0). This sheet serves the purpose of mapping other google sheets by id to Trino tables.

In order to add this connector, follow the documentation [here](https://trino.io/docs/current/connector/googlesheets.html) for setting up a Google service account and providing access to that service account to the metasheet and also any listed data sheets.

### Additional information for Hive connector
The Hive connector is currently intended to enable virtual views (i.e. `CREATE VIEW`) with the use of Hive Metastore. Intended use case for now is for a co-located Hive Metastore deployment to be used where only a URL is needed. Since the only data stored on Hive Metastore is encrypted view definitions, authentication is not necessary for the moment and no credentials are needed.

### Charm relation
The `trino-catalog` relation allows external applications to discover and connect to Trino catalogs programmatically. When a relation is established, Trino automatically creates a per-relation user and shares credentials via a Juju secret granted to the requirer. The server URL and available catalogs are shared via the relation databag.

```bash
# Deploy a requirer application and create the relation
juju deploy <requirer-app>
juju relate trino-k8s <requirer-app>
```

No manual secret granting is required. Each requirer gets a unique username in the format `app-<requirer-app-name>-<relation-id>` with an auto-generated password. This works across both same-model and cross-model relations.

## User management
By default password authentication is enabled for Charmed Trino. This being said, Trino supports implementing multiple forms of authentication mechanisms at the same time. Available with the charm are Google Oauth and user/password authentication. We recommend user/password for application users which do no support Oauth, and Oauth for everything else.

### Google Oauth
Configure Google Oauth by adding the following config values to the coordinator charm:
```
juju config trino-k8s google-client-id=<id>
juju config trino-k8s google-client-secret=<secret>

```

### User/password
Additionally user/password authentication can be enabled via a Juju secret.

```
# Create the secret and grant access to Trino.
juju add-secret trino-user-management users#file=/path/to/user-secrets.yaml
juju grant-secret trino-user-management trino-k8s
juju grant-secret trino-user-management trino-k8s-worker

# Get the secret id and pass this to the charm via the config.
juju show-secret trino-user-management
juju config trino-k8s user-secret-id=<juju-secret-id>
```

Where the `user-secrets.yaml` has the below format:
```
<user>:<password>
<another-user>:<another-password>
```

## Policy
### Ranger
Ranger acts as a fine-grained authorization manager for the Trino charm. It is an optional relation in order to provide access control on the data connected to Trino.

```
# deploy ranger-k8s charm
juju deploy ranger-k8s --channel beta

# deploy ranger charm metadata database
juju deploy postgresql-k8s

# relate ranger charm and postgresql charm
juju relate ranger-k8s postgresql-k8s

# relate trino-k8s ranger-k8s
juju relate trino-k8s ranger-k8s
```
By default Trino has an allow all access control policy. If you're using an alternative to Trino's built-in ACLs (ie Ranger) then you can configure the default Trino policy to default to `none`. This will deny all access in the case that Ranger is unavailable.

## Trino CLI

Trino provides an interactive shell for running queries. The full installation and user guide can be found [here](https://trino.io/docs/current/client/cli.html).

Depending on how the authentication is set up, the CLI can be run as follows:

```
# username and password authentication
./trino --server https://trino.example.com --user=exampleusername --password

# or SSO authentication
./trino --server https://trino.example.com --user=exampleusername --external-authentication
```

## Charmed OpenSearch relation
[Charmed OpenSearch](https://charmhub.io/opensearch) should be integrated with the Charmed Trino to enable auditing functionality for data access. 
Pre-requisites:
- A Charmed Ranger relation has been implemented
- Charmed OpenSearch is deployed and scaled to at least 2 units.
- The `opensearch_client` endpoint is offered and can be consumed by Charmed Trino.
- Charmed Ranger is related to Charmed OpenSearch

Instructions on implementing the above pre-requisites can be found [here](https://github.com/canonical/ranger-k8s-operator/blob/main/README.md). With additional details on the OpenSearch setup process can be found [here](https://charmhub.io/opensearch/docs/t-overview).

Consume opensearch offer:
```
juju consume lxd-controller:admin/opensearch.opensearch
```

Finally, relate the applications:
```
juju relate trino-k8s opensearch
```

## Observability

The Trino charm can be related to the
[Canonical Observability Stack](https://charmhub.io/topics/canonical-observability-stack)
in order to collect logs and telemetry.
To deploy cos-lite and expose its endpoints as offers, follow these steps:

```bash
# Deploy the cos-lite bundle:
juju add-model cos
juju deploy cos-lite --trust
```

```bash
# Expose the cos integration endpoints:
juju offer prometheus:metrics-endpoint
juju offer loki:logging
juju offer grafana:grafana-dashboard

# Relate trino to the cos-lite apps:
juju relate trino-k8s admin/cos.grafana
juju relate trino-k8s admin/cos.loki
juju relate trino-k8s admin/cos.prometheus
```

```bash
# Access grafana with username "admin" and password:
juju run grafana/0 -m cos get-admin-password --wait 1m
# Grafana is listening on port 3000 of the app ip address.
# Dashboard can be accessed under "Trino Server Metrics", make sure to select the juju model which contains your Trino charm.
```

## PostgreSQL Integration

This guide describes how to connect Trino to [PostgreSQL](https://charmhub.io/postgresql-k8s), enabling users to query PostgreSQL databases through Trino catalogs. The integration uses the `postgresql_client` relation interface, which automatically creates and maintains Trino catalogs backed by PostgreSQL. 

This integration depends on the PostgreSQL charm feature that allows to discover existing databases through prefix matching. The feature is available only on the `16/edge` track as of the writing of this doc.

On Trino's side, [dynamic catalog management](https://trino.io/docs/current/admin/properties-catalog.html) has been enabled to be able to execute CREATE/DROP CATALOG statements that update Trino's catalog list without having to restart the service. 

### Deploy the charms

```bash
juju deploy trino-k8s trino-coordinator --config charm-function=coordinator --trust
juju deploy trino-k8s trino-worker --config charm-function=worker --trust
juju relate trino-coordinator:trino-coordinator trino-worker:trino-worker

juju deploy postgresql-k8s --channel=16/edge --trust
```

### Create the database

The PostgreSQL relation uses prefix matching (`database_prefix: testdb*`) to discover databases. The database must be created beforehand, the simplest way is via `data-integrator`:

```bash
juju deploy data-integrator --channel=latest/edge --config database-name=testdb
juju relate postgresql-k8s data-integrator
```

### Configure the PostgreSQL catalog

Set the `postgresql-catalog-config` on the Trino coordinator. Each top-level key must match the name of a related PostgreSQL app:

```bash
juju config trino-coordinator postgresql-catalog-config="
postgresql-k8s:
  database_prefix: testdb*
  ro_catalog_name: mydb_ro
"
```

#### Config fields

| Field | Required | Description |
|-------|----------|-------------|
| `database_prefix` | Yes | Must end with `*`. Used for prefix matching against existing PG databases. |
| `ro_catalog_name` | Yes | Name of the read-only Trino catalog. Routes queries to replicas via `targetServerType=preferSecondary`. |
| `rw_catalog_name` | No | Name of an optional read-write catalog. Routes queries to the primary via `targetServerType=primary`. |
| `config` | No | Extra key=value lines added to the catalog properties (e.g., `case-insensitive-name-matching=true`). |

#### Example with all options

```bash
juju config trino-coordinator postgresql-catalog-config="
postgresql-k8s:
  database_prefix: testdb*
  ro_catalog_name: testcatalog
  rw_catalog_name: testcatalog_developer
  config: |
    case-insensitive-name-matching=true
    decimal-mapping=allow_overflow
"
```

### Establish the relation

```bash
juju relate trino-coordinator postgresql-k8s
```

After the relation is established and PostgreSQL responds with credentials and endpoints, Trino will automatically create the configured catalogs.

### Verify the integration

Check that the catalogs appear in Trino by running `SHOW CATALOGS`.

You can also inspect the catalog properties file:

```bash
juju ssh --container trino trino-coordinator/0 \
  cat /usr/lib/trino/etc/catalog/mydb_ro.properties
```

### Coexistence with static catalogs

The `postgresql-catalog-config` (SQL-managed catalogs via relations) and `catalog-config` (static `.properties` catalogs) coexist. Both types of catalogs appear in `SHOW CATALOGS` and are independently queryable.

Removing a static catalog from `catalog-config` does not affect relation-managed catalogs, and vice versa.

### Credential rotation

If PostgreSQL rotates credentials, Trino automatically detects the change and recreates the affected catalogs with the new credentials. No manual intervention is required.

### Authorization

The charm manages catalogs via Trino's HTTP API on `localhost:8080`. Authentication is bypassed over HTTP (`allow-insecure-over-http=true`), but authorization still applies:

- **Without Ranger**: the file-based ACL (`acl-mode-default=owner`) grants catalog management to the catalog owner. The charm uses the first user from `user-secret-id`, or falls back to the default credentials.
- **With Ranger**: the user must have `CREATE`/`DROP` catalog permissions configured in Ranger policies.

Note: The [file-based ACL documentation](https://trino.io/docs/current/security/file-system-access-control.html) does not provide any information on CREATE/DROP CATALOG permissions and the full scope of the "owner" mode is not properly documented either. This could be due to the dynamic catalog management currently being an experimental feature. However, this [issue](https://github.com/trinodb/trino/issues/22022) provides some details on it. 

### TLS

SSL parameters are auto-deduced from the PostgreSQL provider's TLS data:
- **TLS enabled with CA**: `ssl=true&sslmode=require` with CA cert imported into the Java truststore
- **TLS enabled without CA**: `ssl=true&sslmode=require`
- **TLS disabled**: `ssl=false`

No manual SSL configuration is needed.

### Troubleshooting

1. Verify the relation exists and both apps are active:
   ```bash
   juju status --relations
   ```

2. Check that the `postgresql-catalog-config` key matches the PG app name exactly:
   ```bash
   juju config trino-coordinator postgresql-catalog-config
   ```

3. Check Trino logs for errors:
   ```bash
   juju debug-log --include trino-coordinator
   ```

   Common errors:
   - `Invalid or missing database_prefix`: prefix must end with `*`
   - `Missing ro_catalog_name`: required field
   - `No postgresql-catalog-config entry for <app>`: config key doesn't match app name
   - `Multiple prefix databases returned`: only one database should match the prefix


## Contributing
Please see the [Juju SDK documentation](https://juju.is/docs/sdk) for more information about developing and improving charms and [Contributing](CONTRIBUTING.md) for developer guidance.

## License
The Charmed Trino K8s Operator is free software, distributed under the Apache Software License, version 2.0. See [License](LICENSE) for more details. 
