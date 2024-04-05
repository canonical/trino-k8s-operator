# Trino K8s Operator
The Charmed Trino K8s Operator delivers automated management on [Trino](https://trino.io/) data virtualization software on top of a Kubernetes cluster. Trino is a distributed SQL query engine designed to query large data sets distributed over one or more heterogeneous data sources.

## Usage
Note: This operator requires the use of juju >= 3.1. More information on setting up your environment can be found [here](CONTRIBUTING.md).

```
# deploy Trino operator:
juju deploy trino-k8s
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
Adding or removing a connector from Trino is done using a `juju action` and configuration values passed as parameters to this action. This is best done with a `yaml` file.

```
# adding a connector
juju run trino-k8s/0 add-connector --params database.yaml

# removing a connector
juju run trino-k8s/0 remove-connector --params database.yaml
```
Details on the `database.yaml` file below.

### Without TLS
Connecting Trino to a database without TLS, requires a `<database>.yaml` structured as below (this example is for a PostgreSQL connector):
```
conn-name: example
conn-config: |
  connector.name=postgresql
  connection-url=jdbc:postgresql://host.com:5432/database
  connection-user=example-user
  connection-password=example-password
```
Note: the fields required for `conn-config` can change significantly by database type, see supported connectors and their properties files [here](https://trino.io/docs/current/connector.html). 

The user provided should have the maximum permissions you would want any user to have. Restictions to access can be made on this user but no further permissions can be granted.

### With TLS
Connecting Trino to a database with TLS, requires a `<database>.yaml` structured as below (this example is for a PostgreSQL connector):
```
conn-name: example
conn-config: |
  connector.name=postgresql
  connection-url=jdbc:postgresql://host.com:5432/database?ssl=true&sslmode=require&sslrootcert={{ SSL_PATH }}&sslrootcertpassword={{ SSL_PWD }}
  connection-user=example-user
  connection-password=example-password
conn-cert:
  -----BEGIN CERTIFICATE-----
  YOUR CERTIFICATE CONTENT
  -----END CERTIFICATE-----
```
Note: the `connection-url` parameters for TLS are specific to the connector.
`{{ SSL_PATH }}` and `{{ SSL PWD }}` variables should be used in place of the truststore path and password. These are environmental variables of the Trino application.

### Removing a database from Trino
To remove a database you must provide the full configuration of that database. The user and password must match those that the connection was established with. It is not enough for them to have permissions to the database. For this reason we recommend creating a distinct `trino` user for this connection.

### Connecting database clusters
In order to connect clustered database systems to Trino please connect the read-only and read-write endpoints with 2 separate `juju actions`. The read-only database should be appended with `_ro` to distinguish between the two. 
```
salesforce #read-write endpoint
salesforce_ro #read-only endpoint
```
## Relations
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

After relating the trino server charm to cos-lite services,
we need, for the time being, to attach the promtail-bin resource so that
Loki works with the non absolute symbolic link put in place for the `server.log` 
file by Trino. This has the added benefit of avoiding download of promtail from the web:

```bash
# Download promtail binary
curl -O -L "https://github.com/grafana/loki/releases/download/v2.7.5/promtail-linux-amd64.zip"

# Extract the binary
unzip "promtail-linux-amd64.zip"

# Make sure it is executable
chmod a+x "promtail-linux-amd64"

juju switch <TRINO_JUJU_MODEL>
juju attach-resource trino-k8s promtail-bin=<PATH_TO_PROMTAIL_BINARY>/promtail-linux-amd64
```


```bash
# Access grafana with username "admin" and password:
juju run grafana/0 -m cos get-admin-password --wait 1m
# Grafana is listening on port 3000 of the app ip address.
# Dashboard can be accessed under "Trino Server Metrics", make sure to select the juju model which contains your Tino charm.
```

## Contributing
Please see the [Juju SDK documentation](https://juju.is/docs/sdk) for more information about developing and improving charms and [Contributing](CONTRIBUTING.md) for developer guidance.

## License
The Charmed Trino K8s Operator is free software, distributed under the Apache Software License, version 2.0. See [License](LICENSE) for more details. 
