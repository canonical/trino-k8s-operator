# Copyright 2023 Canonical Ltd.
# See LICENSE file for licensing details.

"""Trino charm upgrades integration tests."""

import logging

import jubilant
import pytest
import requests
from helpers import APP_NAME, get_unit, get_unit_url, wait_for_apps

logger = logging.getLogger(__name__)


@pytest.fixture(name="deploy-upgrade", scope="module")
def deploy(juju: jubilant.Juju):
    """Deploy the app."""
    # Deploy trino charm
    trino_config = {
        "acl-mode-default": "none",
        "charm-function": "all",
    }
    juju.deploy(APP_NAME, channel="edge", config=trino_config, trust=True)

    wait_for_apps(juju, [APP_NAME], status="active", timeout=900)
    assert get_unit(juju, APP_NAME).workload_status.current == "active"


@pytest.mark.incremental
@pytest.mark.usefixtures("deploy-upgrade")
class TestUpgrade:
    """Integration test for Trino charm upgrade from previous release."""

    def test_upgrade(self, juju: jubilant.Juju, charm: str, charm_image: str):
        """Builds the current charm and refreshes the current deployment."""
        juju.refresh(APP_NAME, path=str(charm), resources={"trino-image": charm_image})

        wait_for_apps(juju, [APP_NAME], status="active", timeout=600)
        assert get_unit(juju, APP_NAME).workload_status.current == "active"

    def test_ui_relation(self, juju: jubilant.Juju):
        """Perform GET request on the Trino UI host."""
        url = get_unit_url(juju, application=APP_NAME, unit=0, port=8080)
        logger.info("curling app address: %s", url)

        response = requests.get(url, timeout=300)
        assert response.status_code == 200

    def test_config_unchanged(self, juju: jubilant.Juju):
        """Validate config remains unchanged."""
        config = juju.config(APP_NAME)
        acl_mode_default = config["acl-mode-default"]

        # Default is `owner` but we deploy with `none`.
        assert acl_mode_default == "none"
