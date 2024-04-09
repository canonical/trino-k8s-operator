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

The key value is important as for certificates this must end in `_cert` to be automatically imported to the truststore. For all other entries this will be the name of the catalog you can access through Trino. [More information on catalog terminology found here](https://trino.io/docs/current/overview/concepts.html).

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

## Contributing
Please see the [Juju SDK documentation](https://juju.is/docs/sdk) for more information about developing and improving charms and [Contributing](CONTRIBUTING.md) for developer guidance.

## License
The Charmed Trino K8s Operator is free software, distributed under the Apache Software License, version 2.0. See [License](LICENSE) for more details. 
