# Copyright 2023 Canonical Ltd.
# See LICENSE file for licensing details.

"""Trino charm integration test config."""

import subprocess  # nosec B404
from pathlib import Path

import jubilant
import pytest
from helpers import (
    APP_NAME,
    COORDINATOR_CONFIG,
    NGINX_NAME,
    WORKER_CONFIG,
    WORKER_NAME,
    get_unit,
    wait_for_apps,
)
from pytest import FixtureRequest


def pack_charm(source_dir: Path) -> Path:
    """Build a charm and return the newest resulting artifact path."""
    existing = {path.resolve() for path in source_dir.glob("*.charm")}
    subprocess.run(
        ["/snap/bin/charmcraft", "pack", "--verbose"],
        cwd=source_dir,
        check=True,
        text=True,
    )  # nosec B603
    artifacts = sorted(source_dir.glob("*.charm"), key=lambda path: path.stat().st_mtime)
    assert artifacts, f"No charm artifact found in {source_dir} after packing"

    for artifact in reversed(artifacts):
        if artifact.resolve() not in existing:
            return artifact
    return artifacts[-1]


@pytest.fixture(scope="module", name="charm_image")
def charm_image_fixture(request: FixtureRequest) -> str:
    """Get the OCI image for charm."""
    charm_image = request.config.getoption("--trino-image")
    assert charm_image, (
        "--trino-image argument is required which should contain the name of the OCI image."
    )
    return charm_image


@pytest.fixture(scope="module", name="charm")
def charm_fixture(request: FixtureRequest) -> str | Path:
    """Fetch the path to charm."""
    charms = request.config.getoption("--charm-file")
    if charms:
        charm = charms[0]
    else:
        charm_dir = Path(__file__).resolve().parents[2]
        charms = list(charm_dir.glob("*.charm"))
        assert charms, f"No charms were found in {charm_dir.resolve()}"
        assert len(charms) == 1, f"Found more than one charm {charms}"
        charm = charms[0]

    path = Path(charm).resolve()
    assert path.is_file(), f"{path} is not a file"
    return path


@pytest.fixture(name="deploy", scope="module")
def deploy(juju: jubilant.Juju, charm: str, charm_image: str):
    """Deploy the app."""
    # Deploy trino and nginx charms
    juju.deploy(
        charm,
        APP_NAME,
        resources={"trino-image": charm_image},
        config=COORDINATOR_CONFIG,
        num_units=1,
        trust=True,
    )
    juju.deploy(
        charm,
        WORKER_NAME,
        resources={"trino-image": charm_image},
        config=WORKER_CONFIG,
        num_units=1,
        trust=True,
    )
    juju.deploy(NGINX_NAME, trust=True)

    # Integrate immediately so relation processing overlaps with workload startup.
    # This avoids the deploy->wait(blocked)->integrate->wait(active) sequencing
    # that depends on a deferred relation-changed to clear stale blocked status.
    juju.integrate(f"{APP_NAME}:trino-coordinator", f"{WORKER_NAME}:trino-worker")
    juju.integrate(APP_NAME, NGINX_NAME)

    wait_for_apps(
        juju,
        [APP_NAME, WORKER_NAME],
        status="active",
        idle_period=30,
        timeout=1200,  # Extend timeout as we do not wait for apps to go `active` first anymore.
    )
    assert get_unit(juju, APP_NAME).workload_status.current == "active"
