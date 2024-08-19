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
juju deploy trino-k8s --channel=edge --config charm-function=coordinator
juju deploy trino-k8s --channel=edge --config charm-function=worker trino-k8s-worker

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

## Trino connectors
Adding or removing a connector from Trino is done using the `juju config` parameter `catalog-config`, as there are likely to be a number of catalogs to connect this is recommended to be done through a `catalog_config.yaml` file.
The below is an example of the `catalog_config.yaml`, which connects 1 postgresql database without TLS and 1 with TLS.

### The config file
```
catalogs:
  production_db: |
    connector.name=postgresql
    connection-url=jdbc:postgresql://host:port/db?ssl=true&sslmode=require&sslrootcert={SSL_PATH}&sslrootcertpassword={SSL_PWD}
    connection-user=user
    connection-password=password
  staging_db: |
    connector.name=postgresql
    connection-url=jdbc:postgresql://host:port/staging_db
    connection-user=user
    connection-password=password
certs:
  production_cert: |
    -----BEGIN CERTIFICATE-----
    Certificate values...
    -----END CERTIFICATE-----
```
Note: the required fields change significantly by connector, see the Trino documentation on this [here](https://trino.io/docs/current/connector.html). Currently only Elasticsearch, PostgreSQL, Google sheets, MySQL, Prometheus and Redis connectors are supported by the charm. 

The user provided should have the maximum permissions you would want any user to have. Restictions to access can be made on this user but no further permissions can be granted.

`{SSL_PATH}` and `{SSL PWD}` variables will be replaced with the truststore path and password by the charm, as long as the certificte has been added to the `certs` key this will be added to the trustore automatically.

### Adding or removing a catalog
Once you have your `catalog_config.yaml` file you can configure the Trino charm with the below:
```
juju config trino-k8s catalog-config=@/path/to/file/trino_catalogs.yaml
```
To add or remove a connector simply update the file and run the above again.

### Connecting database clusters
In order to connect clustered database systems to Trino please connect the read-only and read-write endpoints with 2 separate `juju actions`. The read-only database should be appended with `_ro` to distinguish between the two. 
```
salesforce #read-write endpoint
salesforce_ro #read-only endpoint
```
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
juju add-secret trino-user-management --file /path/to/user-secrets.yaml
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
