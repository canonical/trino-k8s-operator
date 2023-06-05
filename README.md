# trino-k8s-charm
The Charmed Trino K8s Operator delivers automated management on Trino data virtualization software on top of a Kubernetes cluster. Trino is a distributed SQL query engine designed to query large data sets distributed over one or more heterogeneous data sources.

The operator charm comes with features such as:
- Access control management supported with Ranger-provided ACLs

## Relations
### tls-certificates
The `tls-certificates` interface is used with the `tls-certificates-operator` charm. 

Note: The TLS settings here are for self-signed-certificates which are not recommended for production clusters, the tls-certificates-operator charm offers a variety of configurations, read more on the TLS charm [here](https://charmhub.io/tls-certificates-operator)

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
## Contributing
Please see the [Juju SDK documentation](https://juju.is/docs/sdk) for more information about developing and improving charms and [Contributing](CONTRIBUTING.md) for developer guidance.

## License
The Charmed Trino K8s Operator is free software, distributed under the Apache Software License, version 2.0. See [License](LICENSE) for more details. 
