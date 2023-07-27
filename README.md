# Trino K8s Operator
The Charmed Trino K8s Operator delivers automated management on [Trino](https://trino.io/) data virtualization software on top of a Kubernetes cluster. Trino is a distributed SQL query engine designed to query large data sets distributed over one or more heterogeneous data sources.

## Usage
Note: This operator requires the use of juju >= 3.1. More information on setting up your environment can be found [here](CONTRIBUTING.md).

```
# deploy Trino operator:
juju deploy trino-k8s
```
By default the Trino K8s operator requires a TLS relation, details found below.

### tls-certificates
The `tls-certificates` interface is used with the `tls-certificates-operator` charm. 

Note: The TLS settings here are for self-signed-certificates which are not recommended for production clusters, the tls-certificates-operator charm offers a variety of configurations, read more on the TLS charm [here](https://charmhub.io/tls-certificates-operator).

To enable TLS: 
```
# deploy the TLS charm:
juju deploy tls-certificates-operator --channel=edge

# add necessary configurations for TLS:
juju config tls-certificates-operator generate-self-signed-certificates="true" ca-common-name="trino-server"

# provide Google credentials (optional):
juju config trino-k8s google-client-id=<id> google-client-secret=<secret>

# relate with the Trino charm:
juju relate tls-certificates-operator trino-k8s
```
Note: currently only Google Oauth authentication and password authentication are supported.
For information on how to set this up on Google see [here](https://developers.google.com/identity/protocols/oauth2).

To disable TLS:
```
# remove relation:
juju remove-relation trino-k8s tls-certificates-operator
```

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

Note: for HTTPS on port 8443 (default) the Trino operator must have a certificates relation.

### Connecting a database to Trino
This is done using a `juju action` and configuration values passed as parameters to this action. 
```
# Add a database:
juju run trino-k8s/0 add-connector conn-name=name conn-config="connector.name=postgresql
connection-url=jdbc:postgresql://host:port/database
connection-user=user
connection-password=password"

# Remove a database:
juju run trino-k8s/0 remove-connector conn-name=name conn-config="connector.name=postgresql
connection-url=jdbc:postgresql://host:port/database
connection-user=user
connection-password=password"

```
Note: the fields required can change sigificantly by database type, see supported connectors and their properties files [here](https://trino.io/docs/current/connector.html). 

The user provided should have the maximum permissions you would want any user to have. Restictions to access can be made on this user but no further permissions can be granted.

### Removing a database from Trino
To remove a database you must provide the full configuration of that database. The user and password must match those that the connection was established with. It is not enough for them to have permissions to the database. For this reason we recommend creating a distinct `trino` user for this connection.

### Connecting database clusters
In order to connect clustered database systems to Trino please connect the read-only and read-write endpoints with 2 separate `juju actions`. The read-only database should be appended with `_ro` to distinguish between the two. 
```
salesforce #read-write endpoint
salesforce_ro #read-only endpoint
```

## Contributing
Please see the [Juju SDK documentation](https://juju.is/docs/sdk) for more information about developing and improving charms and [Contributing](CONTRIBUTING.md) for developer guidance.

## License
The Charmed Trino K8s Operator is free software, distributed under the Apache Software License, version 2.0. See [License](LICENSE) for more details. 
