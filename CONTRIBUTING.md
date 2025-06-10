# Testing

This project uses `tox` for managing test environments (4.4.x). There are some pre-configured environments
that can be used for linting and formatting code when you're preparing contributions to the charm:

```shell
tox run -e fmt           # update your code according to linting rules
tox run -e lint          # code style
tox run -e unit          # unit tests
tox run -e integration   # integration tests
tox                      # runs 'fmt', 'lint', and 'unit' environments
```

# Deploy Trino

This charm is used to deploy Trino Server in a k8s cluster. For local deployment, follow the following steps:

## Set up your development environment
### Install Microk8s
```bash
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
sudo microk8s enable registry

# Set up a short alias for Kubernetes CLI:
sudo snap alias microk8s.kubectl kubectl
```

### Install Charmcraft
```bash
# Install lxd from snap:
sudo snap install lxd --classic --channel=5.12/stable

# Install charmcraft from snap:
sudo snap install charmcraft --classic --channel=2.2/stable

# Charmcraft relies on LXD. Configure LXD:
lxd init --auto
```

### Set up the Juju OLM
```bash
# Install the Juju CLI client, juju:
sudo snap install juju --channel=3.4/stable

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

### Set up Rockcraft
```bash
sudo snap install rockcraft --edge --classic
sudo snap install skopeo --edge --devmode

# Note: Docker must be installed after LXD is initialized due to firewall rules incompatibility.
sudo snap install docker
sudo groupadd docker
sudo usermod -aG docker $USER
newgrp docker

# Note: disabling and enabling docker snap is required to avoid sudo requirement. 
# As described in https://github.com/docker-snap/docker-snap.
sudo snap disable docker
sudo snap enable docker
```

### Pack the rock
```bash
cd trino_rock

# Note: to build trino rock, you need to have at least 30GB of free disk space.
# The first run may take 45 to 60 minutes.
rockcraft pack
rockcraft.skopeo --insecure-policy copy --dest-tls-verify=false oci-archive:trino_468-24.04-edge_amd64.rock docker://localhost:32000/trino-rock:468
```

### Deploy charm
```bash
# Pack the charm:
charmcraft pack

# Deploy the coordinator:
juju deploy ./trino-k8s_ubuntu-22.04-amd64.charm --resource trino-image=localhost:32000/trino-rock:468 --config charm-function=coordinator trino-k8s

# Deploy the worker:
juju deploy ./trino-k8s_ubuntu-22.04-amd64.charm --resource trino-image=localhost:32000/trino-rock:468 --config charm-function=worker trino-k8s-worker

# Check deployment was successful:
kubectl get pods -n trino-k8s
```

For development of the `policy` relation please use the `trino-image=ghcr.io/canonical/trino:418` instead.

Note: due to the requirements of the `discovery_uri`, when using a separate coordinator and worker, the default `discovery_uri` value of `http://trino-k8s:8080` will only work if the trino coordinator deployment is named `trino-k8s`. If using another alias please update the worker and coordinator charm configurations accordingly.

### Refresh charm
```bash
# Refresh the coordinator:
juju refresh --path="./trino-k8s_ubuntu-22.04-amd64.charm" trino-k8s --resource trino-image=localhost:32000/trino-rock:468 

# Refresh the worker:
juju refresh --path="./trino-k8s_ubuntu-22.04-amd64.charm" trino-k8s-worker --resource trino-image=localhost:32000/trino-rock:468 
```


## Trino configuration
```
# Enable DEBUG logging
juju config trino-k8s log-level=debug

```

## Trino actions
```
# Restart Trino Server:
juju run trino-k8s/0 restart
```

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
```bash
# Remove TLS relation: 
juju remove-relation nginx-ingress-integrator trino-k8s

# Remove the application before retrying
juju remove-application trino-k8s --force
juju remove-application trino-k8s-worker --force
```

```bash
# Remove LXD container for rock
lxc project switch rockcraft

# Remove the LXD container. This will save nearly 30GB of disk space. 
lxc delete rockcraft-trino-amd64-4
```