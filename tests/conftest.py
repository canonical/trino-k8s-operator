# Copyright 2023 Canonical Ltd.
# See LICENSE file for licensing details.

"""Fixtures for charm tests."""

import pytest


def pytest_configure(config: pytest.Config):
    """Register custom markers used by the test suite."""
    config.addinivalue_line(
        "markers",
        "abort_on_fail: xfail the remaining tests in the same module after a failure",
    )


def pytest_addoption(parser: pytest.Parser):
    """Parse additional pytest options.

    Args:
        parser: pytest command line parser.
    """
    # The prebuilt charm file.
    parser.addoption("--charm-file", action="append", default=[])
    # The charm image name:tag.
    parser.addoption("--trino-image", action="store", default="")
    parser.addoption(
        "--keep-models",
        action="store_true",
        default=False,
    )
    parser.addoption(
        "--model",
        action="store",
    )


@pytest.hookimpl(hookwrapper=True)
def pytest_runtest_makereport(item: pytest.Item, call: pytest.CallInfo[None]):
    """Expose per-phase reports on each collected test item."""
    outcome = yield
    report = outcome.get_result()
    setattr(item, f"rep_{report.when}", report)


@pytest.fixture(autouse=True)
def abort_on_fail(request: pytest.FixtureRequest):
    """Preserve pytest-operator abort-on-fail semantics at module scope."""
    module = request.module
    if module is not None and getattr(module, "_abort_on_fail_triggered", False):
        pytest.xfail("aborted")

    yield

    marker = request.node.get_closest_marker("abort_on_fail")
    report = getattr(request.node, "rep_call", None)
    if module is not None and marker and report and report.failed:
        setattr(module, "_abort_on_fail_triggered", True)
