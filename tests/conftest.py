# Copyright 2023 Canonical Ltd.
# See LICENSE file for licensing details.

"""Fixtures for charm tests."""

import pytest

# Incremental test support: once a test in a class marked @pytest.mark.incremental
# fails, the remaining tests in that class are xfailed. This is the recipe from
# the pytest docs.
#
# IMPORTANT: incremental tests MUST live in a class (item.cls must not be None).
# Bare module-level functions with @pytest.mark.incremental will all share the
# same str(item.cls) == "None" key and collide across modules, causing spurious
# xfails. Wrap them in a class instead.
_test_failed_incremental: dict[str, dict[tuple[int, ...], str]] = {}


def pytest_configure(config: pytest.Config):
    """Register custom markers used by the test suite."""
    config.addinivalue_line(
        "markers",
        "incremental: mark a test class so a failure aborts the remaining tests in the class.",
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

    # Passed by integration_test.yaml.
    parser.addoption("--model", action="store", default=None)
    parser.addoption("--keep-models", action="store_true", default=False)
    parser.addoption("--series", action="store", default=None)


@pytest.hookimpl(hookwrapper=True)
def pytest_runtest_makereport(item: pytest.Item, call: pytest.CallInfo[None]):
    """Expose per-phase reports on each collected test item and record incremental failures."""
    outcome = yield
    report = outcome.get_result()
    setattr(item, f"rep_{report.when}", report)

    if "incremental" in item.keywords and call.excinfo is not None:
        cls_name = str(item.cls)
        parametrize_index = (
            tuple(item.callspec.indices.values()) if hasattr(item, "callspec") else ()
        )
        test_name = item.originalname or item.name
        _test_failed_incremental.setdefault(cls_name, {}).setdefault(parametrize_index, test_name)


def pytest_runtest_setup(item: pytest.Item) -> None:
    """Xfail a test if an earlier test in its incremental-marked class failed."""
    if "incremental" in item.keywords:
        cls_name = str(item.cls)
        if cls_name in _test_failed_incremental:
            parametrize_index = (
                tuple(item.callspec.indices.values()) if hasattr(item, "callspec") else ()
            )
            test_name = _test_failed_incremental[cls_name].get(parametrize_index)
            if test_name is not None:
                pytest.xfail(f"previous test failed ({test_name})")
