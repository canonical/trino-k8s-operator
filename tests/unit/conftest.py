# Copyright 2023 Canonical Ltd.
# See LICENSE file for licensing details.

"""Shared fixtures for the Scenario-based unit tests."""

import pathlib
from unittest import mock

import pytest
import yaml
from ops.testing import Context

from charm import TrinoK8SCharm

_CHARM_ROOT = pathlib.Path(__file__).parents[2]
_CHARMCRAFT = yaml.safe_load((_CHARM_ROOT / "charmcraft.yaml").read_text())

# Keys from the unified `charmcraft.yaml` that make up the charm metadata.
_META_KEYS = (
    "name",
    "summary",
    "description",
    "assumes",
    "containers",
    "resources",
    "storage",
    "requires",
    "provides",
    "peers",
)


def _charm_meta() -> dict:
    """Build the charm metadata mapping from `charmcraft.yaml`."""
    return {key: _CHARMCRAFT[key] for key in _META_KEYS if key in _CHARMCRAFT}


@pytest.fixture
def ctx():
    """Return a Scenario `Context` for the Trino charm.

    The Kubernetes statefulset patch is mocked for the duration of the test so
    that charm initialisation does not reach out to a real cluster.
    """
    with mock.patch("charm.KubernetesStatefulsetPatch"):
        yield Context(
            TrinoK8SCharm,
            meta=_charm_meta(),
            config=_CHARMCRAFT["config"],
            actions=_CHARMCRAFT["actions"],
        )
