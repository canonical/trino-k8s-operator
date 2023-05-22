# Testing

This project uses `tox` for managing test environments (4.4.x). There are some pre-configured environments
that can be used for linting and formatting code when you're preparing contributions to the charm:

```shell
tox run -e format        # update your code according to linting rules
tox run -e lint          # code style
tox run -e unit          # unit tests
tox run -e integration   # integration tests
tox                      # runs 'format', 'lint', and 'unit' environments
```

# Deploy Trino

This charm is used to deploy Trino Server in a k8s cluster. For local deployment, follow the following steps:

## Set up your development environment
### Install Microk8s
```
# Install Microk8s from snap:
sudo snap install microk8s --classic --channel=1.25

# Add the 'ubuntu' user to the Microk8s group:
sudo usermod -a -G microk8s ubuntu

# Give the 'ubuntu' user permissions to read the ~/.kube directory:
sudo chown -f -R ubuntu ~/.kube

# Create the 'microk8s' group:
newgrp microk8s

# Enable the necessary Microk8s addons:
microk8s enable hostpath-storage dns
```
### Install Charmcraft
```
# Install lxd from snap:
sudo snap install lxd --classic --channel=5.12/stable

# Install charmcraft from snap:
sudo snap install charmcraft --classic --channel=2.2/stable

# Charmcraft relies on LXD. Configure LXD:
lxd init --auto
```
### Set up the Juju OLM
```
# Install the Juju CLI client, juju:
sudo snap install juju --channel=3.1/stable

# Install a "juju" controller into your "microk8s" cloud:
juju bootstrap microk8s trino-controller

# Create a 'model' on this controller:
juju add-model trino-k8s

# Enable DEBUG logging:
juju model-config logging-config="<root>=INFO;unit=DEBUG"

# Check progress:
juju status
juju debug-log
```
### Deploy charm
```
# Pack the charm:
charmcraft pack

# Deploy the charm:
juju deploy ./trino-k8s_ubuntu-22.04-amd64.charm --resource trino-image=trinodb/trino:418

# Check deployment was successful:
kubectl get pods -n trino-k8s
```
## Trino configuration
```
# Enable DEBUG logging
juju config trino-k8s log-level=debug

#TODO: add details on configuring Oauth

```

## Trino actions
```
# Add a database:
juju run trino-k8s/0 connect-database \
db-name=<your-db-name> \
db-type=<your-db-type> \
db-conn-string=<your-conn-string> \
db-user=<your-user> \
db-pwd=<your-db-password>

# Remove a database:
juju run trino-k8s/0 remove-database \
db-name=<your-db-name> \
db-user=<your-user> \
db-pwd=<your-db-password>

# Restart Trino Server:
juju run trino-k8s/0 restart
```
## Accessing Trino
```
# Port forward
kubectl port-forward pod/trino-k8s-0 8080:8080

# Connect to Trino server
./cli/trino --server http://localhost:8080 --user dev

# View databases
SHOW CATALOGS;
```
## Enabling HTTPS
```
#TODO: Use TLS to enable trino HTTPS
```

## Cleanup
```
# Remove the application before retrying
juju remove-application trino-k8s --force
```