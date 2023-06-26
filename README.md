# trino-k8s-charm
The Charmed Trino K8s Operator delivers automated management on Trino data virtualization software on top of a Kubernetes cluster. Trino is a distributed SQL query engine designed to query large data sets distributed over one or more heterogeneous data sources.

The operator charm comes with features such as:
- Access control management supported with Ranger-provided ACLs

## Relations
### tls-certificates
The `tls-certificates` interface is used with the `tls-certificates-operator` charm. 

Note: The TLS settings here are for self-signed-certificates which are not recommended for production clusters, the tls-certificates-operator charm offers a variety of configurations, read more on the TLS charm [here](https://charmhub.io/tls-certificates-operator).

To enable TLS: 
```
# deploy the TLS charm:
juju deploy tls-certificates-operator --channel=edge

# add necessary configurations for TLS:
juju config tls-certificates-operator generate-self-signed-certificates="true" ca-common-name="trino-server"

# provide google credentials:
juju config trino-k8s google-client-id=<id> google-client-secret=<secret>

# relate with the Trino charm:
juju relate tls-certificates-operator trino-k8s
```
Note: currently only Google Oauth authentication is supported.
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

## Contributing
Please see the [Juju SDK documentation](https://juju.is/docs/sdk) for more information about developing and improving charms and [Contributing](CONTRIBUTING.md) for developer guidance.

## License
The Charmed Trino K8s Operator is free software, distributed under the Apache Software License, version 2.0. See [License](LICENSE) for more details. 
