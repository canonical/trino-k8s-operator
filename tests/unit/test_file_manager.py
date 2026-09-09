# Copyright 2023 Canonical Ltd.
# See LICENSE file for licensing details.

"""Unit tests for the change-aware workload file manager."""

from unittest import TestCase

from ops.pebble import Error as PebbleError
from ops.pebble import ExecError, PathError

from file_manager import Inventory, inventory, read_files, reconcile_files
from utils import content_hash


class _Process:
    """Stand-in for the object returned by `Container.exec`."""

    def __init__(self, stdout: str = "", stderr: str = ""):
        self._stdout = stdout
        self._stderr = stderr

    def wait_output(self):
        """Return the canned (stdout, stderr) pair for this call."""
        return self._stdout, self._stderr


class FakeContainer:
    """Minimal in-memory workload container double.

    Attrs:
        files: Mapping of path to text content, standing in for the
            workload filesystem.
        exec_results: Mapping of directory to either a `_Process` (success)
            or an `ExecError` instance (raised on `exec`).
        pushes: Recorded (path, content) pairs from `push` calls, in order.
        removed: Recorded paths passed to `remove_path`, in order.
        exec_calls: Recorded (argv, environment) pairs from `exec` calls.
        fail_push_path: When set, `push` raises for this path.
        fail_remove_path: When set, `remove_path` raises for this path.
        fail_pull_path: When set, `pull` raises `fail_pull_error` for this path.
    """

    def __init__(self, files=None, exec_results=None):
        self.files = dict(files or {})
        self.exec_results = dict(exec_results or {})
        self.pushes = []
        self.removed = []
        self.exec_calls = []
        self.fail_push_path = None
        self.fail_remove_path = None
        self.fail_pull_path = None
        self.fail_pull_error = None

    def exec(self, command, environment=None):
        """Return or raise the canned result for the scanned directory.

        Args:
            command: The `find ... -exec sha256sum {} +` argv list.
            environment: The environment the command runs with.

        Returns:
            The `_Process` configured for the scanned directory.

        Raises:
            ExecError: When the directory was configured to fail.
        """
        self.exec_calls.append((list(command), dict(environment or {})))
        directory = command[1]
        result = self.exec_results.get(directory, _Process())
        if isinstance(result, Exception):
            raise result
        return result

    def pull(self, path):
        """Return a file-like object over the recorded content for `path`.

        Args:
            path: The absolute path to read.

        Returns:
            An object with a `read()` method yielding the text content.

        Raises:
            PathError: When `path` is not present in `self.files` or matches
                `fail_pull_path`.
        """
        if path == self.fail_pull_path:
            raise self.fail_pull_error
        if path not in self.files:
            raise PathError("not-found", f"stat {path}: no such file or directory")

        class _Reader:
            def __init__(self, text):
                self._text = text

            def read(self):
                return self._text

        return _Reader(self.files[path])

    def push(self, path, content, make_dirs=True, permissions=0o644):
        """Record a push, or raise if `path` is configured to fail.

        Args:
            path: The absolute destination path.
            content: The text content to write.
            make_dirs: Unused; accepted for interface compatibility.
            permissions: Unused; accepted for interface compatibility.

        Raises:
            PathError: When `path` matches `fail_push_path`.
        """
        if path == self.fail_push_path:
            raise PathError("generic-file-error", f"push {path}: disk full")
        self.pushes.append((path, content))
        self.files[path] = content

    def remove_path(self, path):
        """Record a removal, or raise if `path` is configured to fail.

        Args:
            path: The absolute path to remove.

        Raises:
            PathError: When `path` matches `fail_remove_path`.
        """
        if path == self.fail_remove_path:
            raise PathError("generic-file-error", f"remove {path}: permission denied")
        self.removed.append(path)
        self.files.pop(path, None)


DIGEST_A = "a" * 64
DIGEST_B = "b" * 64
DIGEST_C = "c" * 64


class TestInventory(TestCase):
    """Tests for `inventory`."""

    def test_parses_multi_file_batch_two_directories(self):
        """Two directories each get exactly one `exec` and are parsed."""
        container = FakeContainer(
            exec_results={
                "/etc/catalog": _Process(
                    stdout=(
                        f"{DIGEST_A}  /etc/catalog/one.properties\n"
                        f"{DIGEST_B}  /etc/catalog/two.properties\n"
                    )
                ),
                "/etc/credentials": _Process(stdout=f"{DIGEST_C}  /etc/credentials/db.json\n"),
            }
        )

        result = inventory(container, ["/etc/catalog", "/etc/credentials"])

        self.assertFalse(result.failed)
        self.assertEqual(
            result.files,
            {
                "/etc/catalog/one.properties": DIGEST_A,
                "/etc/catalog/two.properties": DIGEST_B,
                "/etc/credentials/db.json": DIGEST_C,
            },
        )

    def test_missing_directory_is_empty_not_failed(self):
        """A missing scope root yields an empty, non-failed inventory."""
        container = FakeContainer(
            exec_results={
                "/etc/catalog": ExecError(
                    command=["find"],
                    exit_code=1,
                    stdout="",
                    stderr="/usr/bin/find: '/etc/catalog': No such file or directory",
                )
            }
        )

        result = inventory(container, ["/etc/catalog"])

        self.assertEqual(result.files, {})
        self.assertFalse(result.failed)

    def test_exec_error_with_blank_stderr_marks_failed(self):
        """An `ExecError` with no parsable stderr is treated as a real failure."""
        container = FakeContainer(
            exec_results={
                "/etc/catalog": ExecError(
                    command=["find"],
                    exit_code=1,
                    stdout="",
                    stderr="",
                )
            }
        )

        result = inventory(container, ["/etc/catalog"])

        self.assertTrue(result.failed)
        self.assertEqual(result.files, {})

    def test_partial_hash_failure_marks_failed(self):
        """Output plus an unrelated error line marks the inventory failed."""
        container = FakeContainer(
            exec_results={
                "/etc/catalog": ExecError(
                    command=["find"],
                    exit_code=1,
                    stdout=f"{DIGEST_A}  /etc/catalog/one.properties\n",
                    stderr="/usr/bin/sha256sum: /etc/catalog/two.properties: Permission denied",
                )
            }
        )

        result = inventory(container, ["/etc/catalog"])

        self.assertTrue(result.failed)
        self.assertEqual(result.files, {"/etc/catalog/one.properties": DIGEST_A})

    def test_exec_error_marks_failed(self):
        """An `ExecError` unrelated to a missing directory marks it failed."""
        container = FakeContainer(
            exec_results={
                "/etc/catalog": ExecError(
                    command=["find"],
                    exit_code=2,
                    stdout="",
                    stderr="/usr/bin/find: invalid option",
                )
            }
        )

        result = inventory(container, ["/etc/catalog"])

        self.assertTrue(result.failed)
        self.assertEqual(result.files, {})

    def test_missing_child_is_not_treated_as_missing_root(self):
        """A file vanishing mid-scan is a failure, not an empty directory."""
        container = FakeContainer(
            exec_results={
                "/etc/catalog": ExecError(
                    command=["find"],
                    exit_code=1,
                    stdout="",
                    stderr=(
                        "/usr/bin/find: '/etc/catalog/gone.properties': No such file or directory"
                    ),
                )
            }
        )

        result = inventory(container, ["/etc/catalog"])

        self.assertTrue(result.failed)

    def test_pebble_error_marks_failed(self):
        """A `PebbleError` unrelated to `ExecError` marks the scope failed."""
        container = FakeContainer(
            exec_results={"/etc/catalog": PebbleError("socket connection refused")}
        )

        result = inventory(container, ["/etc/catalog"])

        self.assertTrue(result.failed)
        self.assertEqual(result.files, {})

    def test_successful_exit_with_stderr_marks_failed(self):
        """A batch that exits cleanly but still writes to stderr is untrusted."""
        container = FakeContainer(
            exec_results={
                "/etc/catalog": _Process(
                    stdout=f"{DIGEST_A}  /etc/catalog/one.properties\n",
                    stderr="sha256sum: /etc/catalog/one.properties: stat changed mid-read",
                )
            }
        )

        result = inventory(container, ["/etc/catalog"])

        self.assertTrue(result.failed)
        self.assertEqual(result.files, {"/etc/catalog/one.properties": DIGEST_A})

    def test_binary_mode_and_spaced_paths_are_parsed(self):
        """Binary-mode records and paths containing spaces are parsed."""
        container = FakeContainer(
            exec_results={
                "/etc/catalog": _Process(
                    stdout=(
                        f"{DIGEST_A} */etc/catalog/one.properties\n"
                        f"{DIGEST_B}  /etc/catalog/two words.properties\n"
                        "\n"
                    )
                )
            }
        )

        result = inventory(container, ["/etc/catalog"])

        self.assertFalse(result.failed)
        self.assertEqual(
            result.files,
            {
                "/etc/catalog/one.properties": DIGEST_A,
                "/etc/catalog/two words.properties": DIGEST_B,
            },
        )

    def test_malformed_record_marks_failed(self):
        """A record without a valid digest marks the inventory failed."""
        container = FakeContainer(
            exec_results={
                "/etc/catalog": _Process(stdout="not-a-hash  /etc/catalog/one.properties\n")
            }
        )

        result = inventory(container, ["/etc/catalog"])

        self.assertTrue(result.failed)
        self.assertEqual(result.files, {})

    def test_scan_uses_one_exec_per_directory_in_the_c_locale(self):
        """Each directory is scanned once with locale-stable diagnostics."""
        container = FakeContainer()

        inventory(container, ["/etc/catalog/", "/etc/credentials"])

        self.assertEqual(len(container.exec_calls), 2)
        scanned = [argv[1] for argv, _ in container.exec_calls]
        self.assertEqual(scanned, ["/etc/catalog", "/etc/credentials"])
        for _, environment in container.exec_calls:
            self.assertEqual(environment.get("LC_ALL"), "C")


class TestReadFiles(TestCase):
    """Tests for `read_files`."""

    def test_missing_file_is_skipped(self):
        """A missing file is omitted without failing the batch."""
        container = FakeContainer(files={"/etc/catalog/one.properties": "content"})

        contents, failed = read_files(
            container, ["/etc/catalog/one.properties", "/etc/catalog/missing.properties"]
        )

        self.assertEqual(contents, {"/etc/catalog/one.properties": "content"})
        self.assertFalse(failed)

    def test_non_missing_path_error_marks_failed(self):
        """A `PathError` other than a missing file marks the batch failed."""
        container = FakeContainer(files={"/etc/catalog/one.properties": "content"})
        container.fail_pull_path = "/etc/catalog/one.properties"
        container.fail_pull_error = PathError(
            "generic-file-error", "stat /etc/catalog/one.properties: permission denied"
        )

        contents, failed = read_files(container, ["/etc/catalog/one.properties"])

        self.assertEqual(contents, {})
        self.assertTrue(failed)

    def test_pebble_error_marks_failed(self):
        """A `PebbleError` while pulling a file marks the batch failed."""
        container = FakeContainer(files={"/etc/catalog/one.properties": "content"})
        container.fail_pull_path = "/etc/catalog/one.properties"
        container.fail_pull_error = PebbleError("socket connection refused")

        contents, failed = read_files(container, ["/etc/catalog/one.properties"])

        self.assertEqual(contents, {})
        self.assertTrue(failed)


class TestReconcileFiles(TestCase):
    """Tests for `reconcile_files`."""

    def test_unchanged_content_produces_no_push(self):
        """Content already matching the desired hash is never pushed."""
        content = "connector.name=postgresql\n"
        container = FakeContainer()
        current = Inventory(
            files={"/etc/catalog/pg.properties": content_hash(content)}, failed=False
        )

        result = reconcile_files(container, {"/etc/catalog": {"pg.properties": content}}, current)

        self.assertEqual(container.pushes, [])
        self.assertFalse(result.changed)
        self.assertFalse(result.failed)
        self.assertEqual(
            result.desired_hashes, {"/etc/catalog/pg.properties": content_hash(content)}
        )

    def test_changed_and_new_content_are_each_pushed_once(self):
        """A changed file and a brand-new file each get exactly one push."""
        current = Inventory(
            files={"/etc/catalog/pg.properties": content_hash("old content\n")},
            failed=False,
        )
        container = FakeContainer(files={"/etc/catalog/pg.properties": "old content\n"})

        result = reconcile_files(
            container,
            {
                "/etc/catalog": {
                    "pg.properties": "new content\n",
                    "mysql.properties": "mysql content\n",
                }
            },
            current,
        )

        self.assertTrue(result.changed)
        self.assertFalse(result.failed)
        self.assertEqual(len(container.pushes), 2)
        self.assertEqual(
            {path for path, _ in container.pushes},
            {"/etc/catalog/pg.properties", "/etc/catalog/mysql.properties"},
        )

    def test_unmanaged_files_deleted_and_protected_kept(self):
        """Unmanaged files are removed; protected and managed ones are kept."""
        current = Inventory(
            files={
                "/etc/catalog/pg.properties": content_hash("pg\n"),
                "/etc/catalog/stale.properties": "stale-hash",
                "/etc/catalog/dynamic.properties": "dynamic-hash",
            },
            failed=False,
        )
        container = FakeContainer(
            files={
                "/etc/catalog/pg.properties": "pg\n",
                "/etc/catalog/stale.properties": "stale",
                "/etc/catalog/dynamic.properties": "dynamic",
            }
        )

        result = reconcile_files(
            container,
            {"/etc/catalog": {"pg.properties": "pg\n"}},
            current,
            protect={"/etc/catalog/dynamic.properties"},
        )

        self.assertTrue(result.changed)
        self.assertFalse(result.failed)
        self.assertEqual(container.removed, ["/etc/catalog/stale.properties"])
        self.assertIn("/etc/catalog/dynamic.properties", container.files)
        self.assertIn("/etc/catalog/pg.properties", container.files)

    def test_files_outside_group_directories_are_never_deleted(self):
        """A file outside every managed directory is left untouched."""
        current = Inventory(
            files={
                "/etc/catalog/pg.properties": content_hash("pg\n"),
                "/etc/other/unrelated.properties": "unrelated-hash",
            },
            failed=False,
        )
        container = FakeContainer(
            files={
                "/etc/catalog/pg.properties": "pg\n",
                "/etc/other/unrelated.properties": "unrelated",
            }
        )

        result = reconcile_files(container, {"/etc/catalog": {"pg.properties": "pg\n"}}, current)

        self.assertFalse(result.changed)
        self.assertEqual(container.removed, [])
        self.assertIn("/etc/other/unrelated.properties", container.files)

    def test_remove_unmanaged_false_skips_deletion(self):
        """`remove_unmanaged=False` leaves stale managed-directory files in place."""
        current = Inventory(
            files={
                "/etc/catalog/pg.properties": content_hash("pg\n"),
                "/etc/catalog/stale.properties": "stale-hash",
            },
            failed=False,
        )
        container = FakeContainer(
            files={
                "/etc/catalog/pg.properties": "pg\n",
                "/etc/catalog/stale.properties": "stale",
            }
        )

        result = reconcile_files(
            container,
            {"/etc/catalog": {"pg.properties": "pg\n"}},
            current,
            remove_unmanaged=False,
        )

        self.assertFalse(result.changed)
        self.assertFalse(result.failed)
        self.assertEqual(container.removed, [])
        self.assertIn("/etc/catalog/stale.properties", container.files)

    def test_remove_error_marks_failed(self):
        """A deletion failure stops further removals and marks the result failed."""
        current = Inventory(
            files={
                "/etc/catalog/pg.properties": content_hash("pg\n"),
                "/etc/catalog/stale.properties": "stale-hash",
            },
            failed=False,
        )
        container = FakeContainer(
            files={
                "/etc/catalog/pg.properties": "pg\n",
                "/etc/catalog/stale.properties": "stale",
            }
        )
        container.fail_remove_path = "/etc/catalog/stale.properties"

        result = reconcile_files(container, {"/etc/catalog": {"pg.properties": "pg\n"}}, current)

        self.assertTrue(result.failed)
        self.assertEqual(container.removed, [])

    def test_push_error_stops_further_writes(self):
        """A push failure stops processing and marks the result failed."""
        current = Inventory(files={}, failed=False)
        container = FakeContainer()
        container.fail_push_path = "/etc/catalog/pg.properties"

        result = reconcile_files(
            container,
            {"/etc/catalog": {"pg.properties": "pg\n", "mysql.properties": "mysql\n"}},
            current,
        )

        self.assertTrue(result.failed)
        self.assertEqual(container.pushes, [])

    def test_desired_hashes_cover_every_managed_path(self):
        """`desired_hashes` covers every managed path, written or not."""
        content = "pg\n"
        current = Inventory(
            files={"/etc/catalog/pg.properties": content_hash(content)}, failed=False
        )
        container = FakeContainer(files={"/etc/catalog/pg.properties": content})

        result = reconcile_files(
            container,
            {
                "/etc/catalog": {
                    "pg.properties": content,
                    "mysql.properties": "mysql\n",
                }
            },
            current,
        )

        self.assertEqual(
            set(result.desired_hashes),
            {"/etc/catalog/pg.properties", "/etc/catalog/mysql.properties"},
        )

    def test_failed_inventory_blocks_all_mutation(self):
        """An untrustworthy snapshot yields no write and no deletion."""
        current = Inventory(files={"/etc/catalog/stale.properties": DIGEST_A}, failed=True)
        container = FakeContainer(files={"/etc/catalog/stale.properties": "stale"})

        result = reconcile_files(container, {"/etc/catalog": {"pg.properties": "pg\n"}}, current)

        self.assertTrue(result.failed)
        self.assertFalse(result.changed)
        self.assertEqual(container.pushes, [])
        self.assertEqual(container.removed, [])
        self.assertEqual(
            result.desired_hashes, {"/etc/catalog/pg.properties": content_hash("pg\n")}
        )

    def test_unsafe_file_names_block_all_mutation(self):
        """A name that escapes its directory blocks the whole batch."""
        current = Inventory(files={}, failed=False)
        container = FakeContainer()

        result = reconcile_files(
            container,
            {
                "/etc/catalog": {
                    "pg.properties": "pg\n",
                    "../../etc/passwd": "escaped\n",
                    "/etc/absolute.properties": "absolute\n",
                }
            },
            current,
        )

        self.assertTrue(result.failed)
        self.assertFalse(result.changed)
        self.assertEqual(container.pushes, [])

    def test_dot_and_empty_names_are_unsafe(self):
        """`.`, `..` and empty names are rejected, not treated as real files."""
        current = Inventory(files={}, failed=False)
        container = FakeContainer()

        result = reconcile_files(
            container,
            {"/etc/catalog": {".": "self\n", "..": "parent\n", "": "empty\n"}},
            current,
        )

        self.assertTrue(result.failed)
        self.assertFalse(result.changed)
        self.assertEqual(container.pushes, [])

    def test_push_error_still_reports_every_desired_hash(self):
        """A failed batch still reports the hashes of all managed paths."""
        current = Inventory(files={}, failed=False)
        container = FakeContainer()
        container.fail_push_path = "/etc/catalog/pg.properties"

        result = reconcile_files(
            container,
            {"/etc/catalog": {"pg.properties": "pg\n", "mysql.properties": "mysql\n"}},
            current,
        )

        self.assertTrue(result.failed)
        self.assertEqual(
            result.desired_hashes,
            {
                "/etc/catalog/pg.properties": content_hash("pg\n"),
                "/etc/catalog/mysql.properties": content_hash("mysql\n"),
            },
        )

    def test_equivalent_path_spellings_compare_equal(self):
        """Trailing separators never cause a spurious write or deletion."""
        content = "pg\n"
        current = Inventory(
            files={"/etc/catalog//pg.properties": content_hash(content)}, failed=False
        )
        container = FakeContainer(files={"/etc/catalog/pg.properties": content})

        result = reconcile_files(
            container,
            {"/etc/catalog/": {"pg.properties": content}},
            current,
            protect={"/etc/catalog/"},
        )

        self.assertFalse(result.changed)
        self.assertEqual(container.pushes, [])
        self.assertEqual(container.removed, [])
