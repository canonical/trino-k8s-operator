# Copyright 2023 Canonical Ltd.
# See LICENSE file for licensing details.

"""Trino charm scaling integration tests."""

import logging
import pytest
from helpers import WORKER_NAME, get_active_workers, scale
from pytest_operator.plugin import OpsTest

logger = logging.getLogger(__name__)


@pytest.mark.abort_on_fail
@pytest.mark.usefixtures("deploy")
class TestScaling:
    """Integration tests for Trino worker scaling."""

    async def test_scaling_up(self, ops_test: OpsTest):
        """Scale Trino worker up to 2 units."""
        await scale(ops_test, app=WORKER_NAME, units=2)
        assert len(ops_test.model.applications[WORKER_NAME].units) == 2

        active_workers = await get_active_workers(ops_test)
        assert len(active_workers) == 2

    async def test_scaling_down(self, ops_test: OpsTest):
        """Scale Trino worker down to 1 unit."""
        await scale(ops_test, app=WORKER_NAME, units=1)
        assert len(ops_test.model.applications[WORKER_NAME].units) == 1

        active_workers = await get_active_workers(ops_test)
        assert len(active_workers) == 1
