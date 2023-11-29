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
sudo snap install microk8s --channel 1.25-strict/stable

# Add your user to the Microk8s group:
sudo usermod -a -G snap_microk8s $USER

# Switch to microk8s group
newgrp snap_microk8s

# Create the ~/.kube/ directory and load microk8s configuration
mkdir -p ~/.kube/ && microk8s config > ~/.kube/config

# Enable the necessary Microk8s addons:
sudo microk8s enable hostpath-storage dns

# Set up a short alias for Kubernetes CLI:
sudo snap alias microk8s.kubectl kubectl
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

# Deploy the coordinator:
juju deploy ./trino-k8s_ubuntu-22.04-amd64.charm --resource trino-image=trinodb/trino:418

# Deploy the worker:
juju deploy ./trino-k8s_ubuntu-22.04-amd64.charm --resource trino-image=trinodb/trino:418 --config charm-function=worker trino-k8s-worker

# Check deployment was successful:
kubectl get pods -n trino-k8s
```

For development of the `policy` relation please use the `trino-image=ghcr.io/canonical/trino:418` instead.

Note: due to the requirements of the `discovery_uri`, when using a separate coordinator and worker, the default `discovery_uri` value of `http://trino-k8s:8080` will only work if the trino coordinator deployment is named `trino-k8s`. If using another alias please update the worker and coordinator charm configurations accordingly.

## Trino configuration
```
# Enable DEBUG logging
juju config trino-k8s log-level=debug

```

## Trino actions
```
# Add a database:
juju run trino-k8s/0 add-connector --params connector.yaml

# Remove a database:
juju run trino-k8s/0 remove-connector --params connector.yaml

# Restart Trino Server:
juju run trino-k8s/0 restart
```

Note: the example of actions above are for postgres connector, however any connector listed [here](https://trino.io/docs/current/connector.html) are permissible.

## Accessing Trino
```
# Port forward (http)
kubectl port-forward pod/trino-k8s-0 8080:8080

# Connect to Trino server (http):
./cli/trino --server http://localhost:8080 --user dev

# View databases
SHOW CATALOGS;
```

## Cleanup
```
# Remove TLS relation: 
juju remove-relation tls-certificates-operator trino-k8s

# Remove the application before retrying
juju remove-application trino-k8s --force
```
