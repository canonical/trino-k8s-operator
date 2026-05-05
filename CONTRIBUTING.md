# Contributing

## Testing

This project uses `tox` for managing test environments. The `Makefile` provides convenience targets that wrap common `tox` commands:

```shell
make fmt              # update your code according to linting rules
make lint             # check code style
make test-unit        # run unit tests
make test-static      # run static type checks
make test             # run unit and static tests
make test-integration # run integration tests
make checks           # run fmt, lint, and test
```

You can also invoke `tox` directly:

```shell
tox run -e fmt
tox run -e lint
tox run -e unit
tox run -e static
tox run -e integration
```

Run `make help` to see all available targets.

# Deploy Trino

This charm is used to deploy Trino Server in a k8s cluster. For local deployment, follow the steps below.

## Install dependencies

### Build dependencies

Install tools needed to build the charm and rock:

```bash
make install-build-deps
```

This installs: `yq`, `uv`, `charmcraft`, `rockcraft`, and `tox`.

To verify all build dependencies are present:

```bash
make check-build-deps
```

### Deploy dependencies

Install tools needed to deploy locally:

```bash
make install-deploy-deps
```

This installs: `juju`, `microk8s`, and `docker`.

To verify all deploy dependencies are present:

```bash
make check-deploy-deps
```

## Set up your development environment

### Configure MicroK8s

```bash
# Add your user to the Microk8s group:
sudo usermod -a -G snap_microk8s $USER

# Switch to microk8s group
newgrp snap_microk8s

# Create the ~/.kube/ directory and load microk8s configuration
mkdir -p ~/.kube/ && microk8s config > ~/.kube/config

# Enable the necessary Microk8s addons:
sudo microk8s enable hostpath-storage
sudo microk8s enable dns
sudo microk8s enable registry

# Set up a short alias for Kubernetes CLI:
sudo snap alias microk8s.kubectl kubectl
```

### Configure LXD

```bash
# Install lxd from snap:
sudo snap install lxd --classic --channel=5.21/stable

# Charmcraft and Rockcraft rely on LXD. Configure LXD:
lxd init --auto
```

### Configure Docker

```bash
sudo groupadd docker
sudo usermod -aG docker $USER
newgrp docker

# Disabling and enabling the docker snap is required to avoid the sudo requirement.
# See https://github.com/docker-snap/docker-snap.
sudo snap disable docker
sudo snap enable docker
```

### Set up the Juju OLM

```bash
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

## Build and import the rock

> **Note:** Building the Trino rock requires at least 30 GB of free disk space. The first run may take 45–60 minutes.

```bash
# Build the rock and import it into MicroK8s in one step:
make import-rock

# Or build and import separately:
make build-rock
./scripts/import_rock.sh trino_rock/trino_<version>_amd64.rock trino <version> --latest
```

## Build and deploy the charm

```bash
# Build the charm:
make build-charm

# Deploy the coordinator with the local rock:
make deploy-local-coordinator

# Deploy the worker with the local rock:
make deploy-local-worker

# Relate the coordinator and worker:
juju relate trino-k8s:trino-coordinator trino-k8s-worker:trino-worker

# Check deployment was successful:
juju status
```

> **Note:** For development of the `policy` relation, use `trino-image=ghcr.io/canonical/trino:418` instead of the local rock.

> **Note:** Due to the requirements of `discovery_uri`, when using a separate coordinator and worker, the default value of `http://trino-k8s:8080` will only work if the coordinator is deployed as `trino-k8s`. If using a different alias, update the `discovery_uri` configuration on both the coordinator and worker accordingly.

### Refresh a running deployment

```bash
# Refresh the coordinator:
juju refresh --path="./trino-k8s_ubuntu-22.04-amd64.charm" trino-k8s --resource trino-image=localhost:32000/trino:<version>

# Refresh the worker:
juju refresh --path="./trino-k8s_ubuntu-22.04-amd64.charm" trino-k8s-worker --resource trino-image=localhost:32000/trino:<version>
```

## Trino configuration

```bash
# Enable DEBUG logging
juju config trino-k8s log-level=debug
```

## Trino actions

```bash
# Restart Trino Server:
juju run trino-k8s/0 restart
```

## Accessing Trino

```bash
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
# Remove built charm and rock files:
make clean

# Clean charmcraft or rockcraft LXD environments:
make clean-charmcraft
make clean-rockcraft

# To reclaim ~30GB of disk space, remove the LXD container used by rockcraft:
lxc project switch rockcraft
lxc delete rockcraft-trino-amd64-4
```