# trino-k8s-charm
The Charmed Trino K8s Operator delivers automated management on Trino data vitulization software on top of a Kubernetes cluter. Trino is a distributed SQL query engine designed to query large data sets distributed over one or more heterogeneous data sources.

The operator charm comes with features such as:
- Access control management supported with Ranger provided ACLs

## Relations
### tls-certificates
The `tls-certificates` interface is used wth the `tls-certificates-operator` charm. 

To enable TLS: 
```
# deploy the TLS charm:
juju deploy tls-certificates-operator --channel=edge

# add necessary configurations for TLS:
juju config tls-certificates-operator generate-self-signed-certificate="true" ca-common-name="trino-server"

# relate with the Trino charm:
juju relate tls-certificates-operator trino-k8s

```

## Contributing
Please see the [Juju SDK documentation](https://juju.is/docs/sdk) for more information about developing and improving charms and [Contributing](CONTRIBUTING.md) for developer guidence.

## License
The Charmed Trino K8s Operator is free software, distributed under the Apache Software License, version 2.0. See [License](LICENSE) for more details. 
