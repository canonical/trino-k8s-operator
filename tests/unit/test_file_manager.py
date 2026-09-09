# Copyright 2023 Canonical Ltd.
# See LICENSE file for licensing details.

"""Unit tests for the change-aware workload file manager."""

from unittest import TestCase

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
        fail_push_path: When set, `push` raises for this path.
        fail_remove_path: When set, `remove_path` raises for this path.
    """

    def __init__(self, files=None, exec_results=None):
        self.files = dict(files or {})
        self.exec_results = dict(exec_results or {})
        self.pushes = []
        self.removed = []
        self.fail_push_path = None
        self.fail_remove_path = None

    def exec(self, command):
        """Return or raise the canned result for the scanned directory.

        Args:
            command: The `find ... -exec sha256sum {} +` argv list.

        Returns:
            The `_Process` configured for the scanned directory.

        Raises:
            ExecError: When the directory was configured to fail.
        """
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
            PathError: When `path` is not present in `self.files`.
        """
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


class TestInventory(TestCase):
    """Tests for `inventory`."""

    def test_parses_multi_file_batch_two_directories(self):
        """Two directories each get exactly one `exec` and are parsed."""
        container = FakeContainer(
            exec_results={
                "/etc/catalog": _Process(
                    stdout=("aaa  /etc/catalog/one.properties\nbbb  /etc/catalog/two.properties\n")
                ),
                "/etc/credentials": _Process(stdout="ccc  /etc/credentials/db.json\n"),
            }
        )

        result = inventory(container, ["/etc/catalog", "/etc/credentials"])

        self.assertFalse(result.failed)
        self.assertEqual(
            result.files,
            {
                "/etc/catalog/one.properties": "aaa",
                "/etc/catalog/two.properties": "bbb",
                "/etc/credentials/db.json": "ccc",
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

    def test_partial_hash_failure_marks_failed(self):
        """Output plus an unrelated error line marks the inventory failed."""
        container = FakeContainer(
            exec_results={
                "/etc/catalog": ExecError(
                    command=["find"],
                    exit_code=1,
                    stdout="aaa  /etc/catalog/one.properties\n",
                    stderr="/usr/bin/sha256sum: /etc/catalog/two.properties: Permission denied",
                )
            }
        )

        result = inventory(container, ["/etc/catalog"])

        self.assertTrue(result.failed)
        self.assertEqual(result.files, {"/etc/catalog/one.properties": "aaa"})

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
