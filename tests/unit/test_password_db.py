# Copyright 2026 Canonical Ltd.
# See LICENSE file for licensing details.

"""Unit tests for the atomic `password.db` writer."""

from unittest import mock

import pytest
from ops.pebble import ExecError

from utils import add_users_to_password_db

DB_PATH = "/usr/lib/trino/etc/password.db"
TMP_PATH = f"{DB_PATH}.tmp"


def _commands(container):
    return [call.args[0] for call in container.exec.call_args_list]


def test_builds_a_temporary_file_then_renames_it():
    """The database is built next to the target and moved onto it in one step."""
    container = mock.MagicMock()
    container.exists.return_value = False

    add_users_to_password_db(container, {"alice": "pw-a", "bob": "pw-b"}, DB_PATH)

    assert _commands(container) == [
        ["htpasswd", "-b", "-c", "-B", "-C", "10", TMP_PATH, "alice", "pw-a"],
        ["htpasswd", "-b", "-B", "-C", "10", TMP_PATH, "bob", "pw-b"],
        ["mv", "-f", TMP_PATH, DB_PATH],
    ]
    container.remove_path.assert_not_called()


def test_stale_temporary_file_is_removed_first_and_target_is_kept():
    """A leftover temporary file never leaks into the new database."""
    container = mock.MagicMock()
    container.exists.side_effect = lambda path: path == TMP_PATH

    add_users_to_password_db(container, {"alice": "pw-a"}, DB_PATH)

    container.remove_path.assert_called_once_with(TMP_PATH)
    assert _commands(container)[0][:3] == ["htpasswd", "-b", "-c"]


def test_failed_build_leaves_the_target_untouched():
    """The live database is only replaced once every user was added."""
    container = mock.MagicMock()
    container.exists.return_value = False
    container.exec.return_value.wait_output.side_effect = ExecError(["htpasswd"], 1, "", "boom")

    with pytest.raises(ExecError):
        add_users_to_password_db(container, {"alice": "pw-a"}, DB_PATH)

    assert all(command[0] != "mv" for command in _commands(container))


def test_no_credentials_removes_the_database():
    """With no users there is nothing to rename."""
    container = mock.MagicMock()
    container.exists.return_value = True

    add_users_to_password_db(container, {}, DB_PATH)

    container.exec.assert_not_called()
    container.remove_path.assert_called_with(DB_PATH)
