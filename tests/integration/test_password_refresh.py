# Copyright 2023 Canonical Ltd.
# See LICENSE file for licensing details.

"""Integration tests for Trino reloading `password.db` without a restart.

The Trino Gateway integration relies on Trino's file password authenticator rebuilding its
password store every `file.refresh-period`, so users can change without a Pebble restart. The
charm's user-secret path uses the same `password.db` writer, so it is used to exercise that
behavior without deploying a Trino Gateway.
"""

import logging
import re
import time

import jubilant
import pytest
import requests
from helpers import APP_NAME, get_unit, wait_for_apps

logger = logging.getLogger(__name__)

USERS_SECRET_NAME = "trino-users"  # nosec B105
ORIGINAL_USER = ("alice", "alice-original-password")  # nosec B105
ROTATED_USER = ("alice", "alice-rotated-password")  # nosec B105
ADDED_USER = ("bob", "bob-added-password")  # nosec B105

# `file.refresh-period` is one minute; leave room for the secret-changed hook and Trino's schedule.
REFRESH_TIMEOUT = 240
POLL_INTERVAL = 5
# Trino reports uptime rounded to two decimals of its largest unit (for example `2.35m`), and
# each request adds latency. A restart sets uptime back to about zero, far below this slack.
UPTIME_TOLERANCE = 30

_DURATION_UNIT_SECONDS = {
    "ns": 1e-9,
    "us": 1e-6,
    "ms": 1e-3,
    "s": 1,
    "m": 60,
    "h": 3600,
    "d": 86400,
}


def _users_content(*users: tuple[str, str]) -> dict[str, str]:
    """Build the user secret content in the format the charm expects."""
    return {"users": "\n".join(f"{name}: {password}" for name, password in users)}


def _address(juju: jubilant.Juju) -> str:
    return get_unit(juju, APP_NAME).address


def _can_authenticate(juju: jubilant.Juju, user: tuple[str, str]) -> bool:
    """Report whether Trino accepts the user's password.

    Trino skips authentication on plain HTTP. `X-Forwarded-Proto` makes it treat the request as
    secure, as it does behind the ingress, so the password is checked.
    """
    response = requests.post(
        f"http://{_address(juju)}:8080/v1/statement",
        data="SELECT 1",
        auth=user,
        headers={"X-Forwarded-Proto": "https"},
        timeout=30,
    )
    assert response.status_code in (200, 401), response.text
    return response.status_code == 200


def _uptime_seconds(juju: jubilant.Juju) -> float:
    """Return the Trino server uptime, which resets to zero whenever Pebble restarts Trino."""
    info = requests.get(f"http://{_address(juju)}:8080/v1/info", timeout=30).json()
    match = re.fullmatch(r"([\d.]+)(ns|us|ms|s|m|h|d)", info["uptime"])
    assert match, f"unexpected uptime {info['uptime']!r}"
    return float(match.group(1)) * _DURATION_UNIT_SECONDS[match.group(2)]


def _wait_for_authentication(
    juju: jubilant.Juju,
    user: tuple[str, str],
    expected: bool,
    timeout: float = REFRESH_TIMEOUT,
    tolerate_restart: bool = False,
) -> float:
    """Wait until Trino accepts (or rejects) the user and return the seconds it took.

    Args:
        juju: Jubilant Juju object.
        user: The username and password to try.
        expected: Whether Trino should accept the user.
        timeout: Seconds to wait.
        tolerate_restart: Keep waiting while Trino is unreachable or starting up. Only for
            steps that are expected to restart Trino.
    """
    start = time.monotonic()
    while time.monotonic() - start < timeout:
        try:
            if _can_authenticate(juju, user) == expected:
                return time.monotonic() - start
        except (requests.RequestException, AssertionError):
            if not tolerate_restart:
                raise
        time.sleep(POLL_INTERVAL)
    raise AssertionError(
        f"Trino {'rejected' if expected else 'still accepted'} user {user[0]!r} "
        f"after {timeout}s without a restart"
    )


def _assert_not_restarted(juju: jubilant.Juju, uptime_before: float, started: float) -> None:
    """Assert that Trino kept running: its uptime kept growing instead of resetting.

    Args:
        juju: Jubilant Juju object.
        uptime_before: The uptime read at `started`, in seconds.
        started: The `time.monotonic()` value when `uptime_before` was read.
    """
    expected = uptime_before + (time.monotonic() - started)
    assert _uptime_seconds(juju) >= expected - UPTIME_TOLERANCE


@pytest.fixture(name="deploy-trino", scope="module")
def deploy_trino(juju: jubilant.Juju, charm: str, charm_image: str):
    """Deploy a single `all` Trino unit once for the module."""
    juju.deploy(
        charm,
        APP_NAME,
        resources={"trino-image": charm_image},
        config={"charm-function": "all"},
        trust=True,
    )
    wait_for_apps(juju, [APP_NAME], status="active", timeout=1000)


@pytest.mark.incremental
@pytest.mark.usefixtures("deploy-trino")
class TestPasswordRefresh:
    """Credential changes reach Trino through `password.db` alone."""

    def test_01_configured_user_authenticates(self, juju: jubilant.Juju):
        """A user from `user-secret-id` can authenticate."""
        secret = juju.add_secret(USERS_SECRET_NAME, _users_content(ORIGINAL_USER))
        juju.grant_secret(USERS_SECRET_NAME, APP_NAME)
        juju.config(APP_NAME, {"user-secret-id": str(secret)})
        wait_for_apps(juju, [APP_NAME], status="active", timeout=1000)

        # Configuring the secret changes the Pebble plan once, so Trino may still be restarting.
        _wait_for_authentication(
            juju, ORIGINAL_USER, expected=True, timeout=600, tolerate_restart=True
        )
        assert not _can_authenticate(juju, (ORIGINAL_USER[0], "wrong-password"))  # nosec B106

    def test_02_added_and_rotated_users_apply_without_restart(self, juju: jubilant.Juju):
        """Adding a user and rotating a password take effect within the refresh period."""
        uptime_before = _uptime_seconds(juju)
        assert not _can_authenticate(juju, ADDED_USER)
        started = time.monotonic()

        juju.update_secret(USERS_SECRET_NAME, _users_content(ROTATED_USER, ADDED_USER))

        added_after = _wait_for_authentication(juju, ADDED_USER, expected=True)
        rotated_after = _wait_for_authentication(juju, ROTATED_USER, expected=True)
        old_rejected_after = _wait_for_authentication(juju, ORIGINAL_USER, expected=False)
        logger.info(
            "added user accepted after %.0fs, rotated password after %.0fs, old password "
            "rejected after %.0fs",
            added_after,
            rotated_after,
            old_rejected_after,
        )

        _assert_not_restarted(juju, uptime_before, started)

    def test_03_removed_user_is_rejected_without_restart(self, juju: jubilant.Juju):
        """Removing a user from the secret revokes it within the refresh period."""
        uptime_before = _uptime_seconds(juju)
        started = time.monotonic()

        juju.update_secret(USERS_SECRET_NAME, _users_content(ROTATED_USER))

        _wait_for_authentication(juju, ADDED_USER, expected=False)
        assert _can_authenticate(juju, ROTATED_USER)
        _assert_not_restarted(juju, uptime_before, started)
