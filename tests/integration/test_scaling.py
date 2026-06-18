# Copyright 2023 Canonical Ltd.
# See LICENSE file for licensing details.

"""Trino charm scaling integration tests."""

import logging

import jubilant
import pytest
from helpers import WORKER_NAME, get_active_workers, get_status, scale

logger = logging.getLogger(__name__)


@pytest.mark.abort_on_fail
@pytest.mark.usefixtures("deploy")
class TestScaling:
    """Integration tests for Trino worker scaling."""

    def test_scaling_up(self, juju: jubilant.Juju):
        """Scale Trino worker up to 2 units."""
        scale(juju, app=WORKER_NAME, units=2)
        assert len(get_status(juju).apps[WORKER_NAME].units) == 2

        active_workers = get_active_workers(juju)
        assert len(active_workers) == 2

    def test_scaling_down(self, juju: jubilant.Juju):
        """Scale Trino worker down to 1 unit."""
        scale(juju, app=WORKER_NAME, units=1)
        assert len(get_status(juju).apps[WORKER_NAME].units) == 1

        active_workers = get_active_workers(juju)
        assert len(active_workers) == 1
