

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
```

Note: the allowed fields change significantly by connector, see the Trino documentation on this [here](https://trino.io/docs/current/connector.html).

The `{SSL_PATH}` and `{SSL PWD}` variables will be replaced with the truststore path and password by the charm.

The catalog-config can be applied with the following:

```
juju config trino-k8s catalog-config=@catalog_config.yaml
```
### Additional information for Google sheets connector
For the google sheets connector it is worth noting that the sheet that is connected to Trino is not the sheet with the data, but rather a metadata sheet following [this format](https://docs.google.com/spreadsheets/d/1Es4HhWALUQjoa-bQh4a8B5HROz7dpGMfq_HbfoaW5LM/edit?gid=0#gid=0). This sheet serves the purpose of mapping other google sheets by id to Trino tables.

In order to add this connector, follow the documentation [here](https://trino.io/docs/current/connector/googlesheets.html) for setting up a Google service account and providing access to that service account to the metasheet and also any listed data sheets.


### Charm relation
The `trino-catalog` relation allows external applications to discover and connect to Trino catalogs programmatically. The Trino charm shares connection details including the server URL, available catalogs, and user credentials via Juju secrets.

```bash
# Deploy a requirer application
juju deploy <requirer-app>
juju relate trino-k8s <requirer-app>

# Grant the credentials secret to the requirer
juju grant-secret trino-user-management <requirer-app>
```

**Note:** Credentials must be manually granted using `juju grant-secret`. The requirer application must have a user in the credentials secret matching the format `app-<requirer-charm-name>`.

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

### Charmed OpenSearch relation
[Charmed OpenSearch](https://charmhub.io/opensearch) should be integrated with the Charmed Trino to enable auditing functionality for data access. 
Pre-requisites:
- A Charmed Ranger relation has been implemented
- Charmed OpenSearch is deployed and scaled to at least 2 units.
- The `opensearch_client` endpoint is offered and can be consumed by Charmed Trino.
- Charmed Ranger is related to Charmed OpenSearch

Instructions on implementing the above pre-requisites can be found [here](https://github.com/canonical/ranger-k8s-operator/blob/main/README.md). With additional details on the OpenSearch setup process can be found [here](https://charmhub.io/opensearch/docs/t-overview).

# Consume opensearch offer
juju consume lxd-controller:admin/opensearch.opensearch

# Finally, relate the applications
juju relate trino-k8s opensearch
```

### Observability

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

## Contributing
Please see the [Juju SDK documentation](https://juju.is/docs/sdk) for more information about developing and improving charms and [Contributing](CONTRIBUTING.md) for developer guidance.

## License
The Charmed Trino K8s Operator is free software, distributed under the Apache Software License, version 2.0. See [License](LICENSE) for more details. 
