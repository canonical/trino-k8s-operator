# Copyright 2024 Canonical Ltd.
# See LICENSE file for licensing details.

"""# KubernetesStatefulsetPatch Library.

This library is designed to enable developers to patch the Kubernetes Statefulset created
by Juju during charm deployment. When charms are deployed, Juju creates a
statefulset named after the application in the namespace (named after the Juju model).

It is possible to add constraints to the charm which Juju will set for all containers
within a stateful set. However, there is a need for resources to be allocated differently
across containers.

When initialised, this library binds a handler to the parent charm's `install` and `update_status`
events which applies the patch to the cluster. This should ensure that the statefulset resources
are consistent across the charms lifecycle.

For information regarding the `lightkube` model, please visit the
`lightkube` [docs](https://gtsystem.github.io/lightkube-models/1.23/models/core_v1).


## Getting Started

To get started using the library, you just need to fetch the library using `charmcraft`. **Note
that you also need to add `lightkube` and `lightkube-models` to your charm's `requirements.txt`.**

```shell
cd some-charm
charmcraft fetch-lib charms.comsys_libs.v0.kubernetes_statefulset_patch
cat << EOF >> requirements.txt
lightkube==0.15.4
lightkube-models==1.28.1.4
EOF
```

Then, to initialise the library:

The memory and cpu can either come from a configuration parameter or be set directly.

```python
# ...
from charms.comsys_libs.v0.kubernetes_statefulset_patch import KubernetesStatefulsetPatch

class SomeCharm(CharmBase):
  def __init__(self, *args):
    # ...
    self.k8s_resources = KubernetesStatefulsetPatch(
        self,
        resource_updates = {
            "charm": {
                "memory": {
                    "limits": "2Gi",
                    "requests": "1Gi",
                },
                "cpu": {
                    "limits": 1,
                    "requests": 1,
                },
            },
            "self.name": {
                "memory": {
                    "limits": self.config.get(workload-memory-limits),
                    "requests": self.config.get(workload-memory-requests),
                    },
                "cpu": {
                    "limits": self.config.get(workload-cpu-limits),
                    "requests": self.config.get(workload-cpu-requests),
                    },
                }
            },
        }

    )
    # ...
```

Observe with additional or custom events by providing `refresh_event` argument:
For example, if you would like to have the resource values as configuration values you will need
to provide the on.config_changed event as a refresh_event.

```python
from charms.comsys_libs.v0.kubernetes_statefulset_patch import KubernetesStatefulsetPatch

class SomeCharm(CharmBase):
  def __init__(self, *args):
    # ...
    self.k8s_resources = KubernetesStatefulsetPatch(
        self,
        resource_updates={
            "charm": {"memory": "1Gi", "cpu": 1},
            self.name: {"memory": self.config["workload-memory"], "cpu": self.config["workload-cpu"]},
        },
        refresh_event=[self.on.config_changed],
    )
    # ...
```

Additionally, you may wish to use mocks in your charm's unit testing to ensure that the library
does not try to make any API calls, or open any files during testing that are unlikely to be
present, and could break your tests. The easiest way to do this is during your test `setUp`:

```python
    # ...
    @mock.patch("charm.KubernetesStatefulsetPatch")
    def setUp(self, _):
    # ...
```
"""

import logging
from typing import List, Optional, Union

from lightkube import ApiError, Client  # pyright: ignore
from lightkube.core import exceptions
from lightkube.models.core_v1 import ResourceRequirements
from lightkube.resources.apps_v1 import StatefulSet
from lightkube.types import PatchType
from ops.charm import CharmBase
from ops.framework import BoundEvent, Object

logger = logging.getLogger(__name__)

# The unique Charmhub library identifier, never change it
LIBID = "88aaabd870504630983b878637115533"

# Increment this major API version when introducing breaking changes
LIBAPI = 0

# Increment this PATCH version before using `charmcraft publish-lib` or reset
# to 0 if you are raising the major API version
LIBPATCH = 3


class KubernetesStatefulsetPatch(Object):
    """A utility for patching the Kubernetes statefulset set up by Juju."""

    def __init__(
        self,
        charm: CharmBase,
        resource_updates: dict = {},
        refresh_event: Optional[Union[BoundEvent, List[BoundEvent]]] = None,
    ):
        """Constructor for KubernetesServicePatch.

        Args:
            charm: the charm that is instantiating the library.
            resource_updates: the resources being requested.
            refresh_event: an optional bound event or list of bound events which
                will be observed to re-apply the patch (e.g. on port change).
                The `install` and `upgrade-charm` events would be observed regardless.
        """
        super().__init__(charm, "kubernetes-service-patch")
        self.charm = charm
        self.statefulset_name = self._app
        self.resource_updates = resource_updates

        self.framework.observe(charm.on.install, self._patch_statefulset)
        self.framework.observe(charm.on.update_status, self._patch_statefulset)

        # apply user defined events
        if refresh_event:
            if not isinstance(refresh_event, list):
                refresh_event = [refresh_event]

            for evt in refresh_event:
                self.framework.observe(evt, self._patch_statefulset)

    def _build_resource_requirements(self, resources: dict) -> ResourceRequirements:
        """Builds a ResourceRequirements object from the given resources dictionary.

        Args:
            resources (dict): Dictionary with resource limits/requests (see above).

        Returns:
            ResourceRequirements: The constructed ResourceRequirements object.
        """
        limits = {}
        requests = {}

        for resource_type in ["memory", "cpu"]:
            if resource_type in resources:
                key = resources[resource_type]
                if key.get("limits"):
                    limits[resource_type] = str(key["limits"])
                if key.get("requests"):
                    requests[resource_type] = str(key["requests"])

        return ResourceRequirements(limits=limits, requests=requests)

    def _patch_statefulset(self, event) -> None:
        """Patches the StatefulSet to update resource limits/requests for specified containers."""
        try:
            client = Client()
            statefulset = client.get(
                StatefulSet,
                name=self.statefulset_name,
                namespace=self._namespace,
            )
            needs_patching = False

            # Loop through containers in StatefulSet and update resources if they match keys in the input dictionary
            for container in statefulset.spec.template.spec.containers:
                if container.name in self.resource_updates:
                    resources = self.resource_updates[container.name]
                    if self._is_patched(container, resources):
                        continue
                    container.resources = self._build_resource_requirements(resources)
                    needs_patching = True

            # Apply the patch if any updates were made
            if needs_patching:
                client.patch(
                    StatefulSet,
                    name=self.statefulset_name,
                    namespace=self._namespace,
                    obj=statefulset,
                    patch_type=PatchType.MERGE,
                )
                logger.info(
                    f"Successfully patched StatefulSet {self.statefulset_name!r} with new resource requests."
                )
            else:
                logger.debug(f"No updates needed for StatefulSet {self.statefulset_name!r}.")

        except (exceptions.ConfigError, ApiError) as e:
            logger.error("Failed to patch StatefulSet: %s", e)
            raise

    def _is_patched(self, container, desired_resources) -> bool:
        """Checks if the container's resources match the desired specifications.

        Args:
            container: The container object from the StatefulSet.
            desired_resources (dict): The desired resource specifications for the container.

        Returns:
            bool: True if the container's resources match the desired resources, otherwise False.
        """
        # Extract current resources from the container
        current_limits = container.resources.limits or {}
        current_requests = container.resources.requests or {}

        # Check memory limits and requests
        if current_limits.get("memory") != desired_resources.get("memory", {}).get(
            "limits"
        ) or current_requests.get("memory") != desired_resources.get("memory", {}).get("requests"):
            return False

        # Check CPU limits and requests
        if current_limits.get("cpu") != str(
            desired_resources.get("cpu", {}).get("limits")
        ) or current_requests.get("cpu") != str(desired_resources.get("cpu", {}).get("requests")):
            return False

        # All checks passed, container is patched
        return True

    @property
    def _app(self) -> str:
        """Name of the current Juju application.

        Returns:
            str: A string containing the name of the current Juju application.
        """
        return self.charm.app.name

    @property
    def _namespace(self) -> str:
        """The Kubernetes namespace we're running in.

        Returns:
            str: A string containing the name of the current Kubernetes namespace.
        """
        with open("/var/run/secrets/kubernetes.io/serviceaccount/namespace", "r") as f:
            return f.read().strip()
