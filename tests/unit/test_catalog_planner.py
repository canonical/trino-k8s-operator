# Copyright 2023 Canonical Ltd.
# See LICENSE file for licensing details.

"""Unit tests for the stateless catalog reconciliation planner."""

import logging
from unittest import TestCase

from ops.pebble import PathError

from catalog_planner import (
    DesiredCatalogs,
    aggregate_hash,
    find_duplicate_names,
    reconcile_catalogs,
)
from relations.postgresql_catalog import CatalogAlreadyExistsError, CatalogSQLError
from utils import content_hash

CATALOG_DIR = "/etc/catalog"
CREDENTIAL_DIR = "/etc/credentials"
ROOT = "/etc"


class _Process:
    """Stand-in for the object returned by `Container.exec`."""

    def __init__(self, stdout: str = ""):
        self._stdout = stdout

    def wait_output(self):
        """Return the canned (stdout, stderr) pair for this call."""
        return self._stdout, ""


class FakeContainer:
    """Minimal in-memory workload container double.

    Attrs:
        files: Mapping of path to text content, standing in for the
            workload filesystem.
        pushes: Recorded (path, content) pairs from `push` calls, in order.
        removed: Recorded paths passed to `remove_path`, in order.
        fail_push_path: When set, `push` raises for this path.
    """

    def __init__(self, files=None):
        self.files = dict(files or {})
        self.pushes = []
        self.removed = []
        self.fail_push_path = None
        self.fail_pull_path = None
        self.fail_pull_error = None

    def _process_for(self, directory):
        lines = "".join(
            f"{content_hash(content)}  {path}\n"
            for path, content in self.files.items()
            if path.rsplit("/", 1)[0] == directory
        )
        return _Process(stdout=lines)

    def exec(self, command, environment=None):
        """Return the sha256sum-style batch for the scanned directory.

        Args:
            command: The `find ... -exec sha256sum {} +` argv list.
            environment: Unused; accepted for interface compatibility.

        Returns:
            The `_Process` reflecting the current contents of the scanned
            directory.
        """
        return self._process_for(command[1])

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
        """Record a removal.

        Args:
            path: The absolute path to remove.
        """
        self.removed.append(path)
        self.files.pop(path, None)


class FakeExecutor:
    """Minimal dynamic catalog SQL executor double.

    Attrs:
        ready: The value returned by `is_trino_ready`.
        calls: Recorded ("create" | "drop", name) pairs, in order.
        fail_create: Names whose `create_catalog` raises `CatalogSQLError`.
        fail_drop: Names whose `drop_catalog` raises `CatalogSQLError`.
        already_exists: Names whose first `create_catalog` call raises
            `CatalogAlreadyExistsError`.
    """

    def __init__(self, ready=True, fail_create=None, fail_drop=None, already_exists=None):
        self.ready = ready
        self.calls = []
        self.fail_create = set(fail_create or [])
        self.fail_drop = set(fail_drop or [])
        self.already_exists = set(already_exists or [])

    def is_trino_ready(self):
        """Report the canned readiness state.

        Returns:
            The configured `ready` flag.
        """
        return self.ready

    def create_catalog(self, name, properties):
        """Record a create call, or raise per the configured failure sets.

        Args:
            name: The catalog name.
            properties: Unused; accepted for interface compatibility.

        Raises:
            CatalogAlreadyExistsError: When `name` is in `already_exists`.
            CatalogSQLError: When `name` is in `fail_create`.
        """
        self.calls.append(("create", name))
        if name in self.already_exists:
            self.already_exists.discard(name)
            raise CatalogAlreadyExistsError(name)
        if name in self.fail_create:
            raise CatalogSQLError(name)

    def drop_catalog(self, name):
        """Record a drop call, or raise per the configured failure set.

        Args:
            name: The catalog name.

        Raises:
            CatalogSQLError: When `name` is in `fail_drop`.
        """
        self.calls.append(("drop", name))
        if name in self.fail_drop:
            raise CatalogSQLError(name)


def _dynamic_raw(properties: dict) -> str:
    """Render a dynamic catalog `.properties` file as Trino would persist it.

    Args:
        properties: The catalog properties, including `connector.name` and
            the dynamic catalog marker.

    Returns:
        The rendered file content.
    """
    return "".join(f"{k}={v}\n" for k, v in properties.items())


def _dynamic_properties(**extra) -> dict:
    """Build a dynamic catalog properties mapping with the marker set.

    Args:
        **extra: Extra property key-value pairs.

    Returns:
        A properties mapping including `connector.name` and the dynamic
        catalog marker.
    """
    return {"connector.name": "postgresql", "query.comment-format": "dynamic catalog", **extra}


def _reconcile(container, executor, desired, dynamic_enabled=True):
    """Call `reconcile_catalogs` with the fixed test directories.

    Args:
        container: The fake workload container.
        executor: The fake dynamic catalog SQL executor.
        desired: The `DesiredCatalogs` to reconcile.
        dynamic_enabled: Whether dynamic catalog SQL may run.

    Returns:
        The resulting `CatalogReconcileResult`.
    """
    return reconcile_catalogs(
        container, executor, desired, CATALOG_DIR, CREDENTIAL_DIR, ROOT, dynamic_enabled
    )


class TestFindDuplicateNames(TestCase):
    """Tests for `find_duplicate_names`."""

    def test_no_overlap(self):
        """Disjoint static and dynamic names produce no duplicates."""
        desired = DesiredCatalogs(static={"pg": "x"}, credentials={}, dynamic={"mysql": {}})
        self.assertEqual(find_duplicate_names(desired), set())

    def test_overlap(self):
        """A shared name is reported as a duplicate."""
        desired = DesiredCatalogs(static={"pg": "x"}, credentials={}, dynamic={"pg": {}})
        self.assertEqual(find_duplicate_names(desired), {"pg"})


class TestAggregateHash(TestCase):
    """Tests for `aggregate_hash`."""

    def test_stable_and_order_independent(self):
        """The hash is stable and independent of dict insertion order."""
        hashes_a = {f"{CATALOG_DIR}/pg.properties": "h1", f"{CREDENTIAL_DIR}/db.json": "h2"}
        hashes_b = {f"{CREDENTIAL_DIR}/db.json": "h2", f"{CATALOG_DIR}/pg.properties": "h1"}
        self.assertEqual(aggregate_hash(hashes_a, ROOT), aggregate_hash(hashes_b, ROOT))

    def test_content_change_changes_hash(self):
        """A changed digest for a path changes the aggregate."""
        base = {f"{CATALOG_DIR}/pg.properties": "h1"}
        changed = {f"{CATALOG_DIR}/pg.properties": "h2"}
        self.assertNotEqual(aggregate_hash(base, ROOT), aggregate_hash(changed, ROOT))


class TestReconcileCatalogs(TestCase):
    """Tests for `reconcile_catalogs` covering the full ownership lifecycle."""

    def test_duplicate_name_blocks_everything(self):
        """A name claimed by both static and dynamic state blocks all mutation."""
        desired = DesiredCatalogs(static={"pg": "content\n"}, credentials={}, dynamic={"pg": {}})
        container = FakeContainer()
        executor = FakeExecutor()

        result = _reconcile(container, executor, desired)

        self.assertEqual(result.duplicates, {"pg"})
        self.assertFalse(result.failed)
        self.assertIsNone(result.state_hash)
        self.assertEqual(executor.calls, [])
        self.assertEqual(container.pushes, [])
        self.assertEqual(container.removed, [])

    def test_failed_inventory_blocks_everything(self):
        """An untrustworthy inventory snapshot performs no SQL or writes."""
        desired = DesiredCatalogs(static={"pg": "content\n"}, credentials={}, dynamic={})
        container = FakeContainer()

        def _broken_exec(command, environment=None):
            return _Process(stdout="not-a-hash  /etc/catalog/pg.properties\n")

        container.exec = _broken_exec
        executor = FakeExecutor()

        result = _reconcile(container, executor, desired)

        self.assertTrue(result.failed)
        self.assertIsNone(result.state_hash)
        self.assertEqual(executor.calls, [])
        self.assertEqual(container.pushes, [])

    def test_failed_read_during_classification_blocks_everything(self):
        """A pull failure while classifying an ambiguous catalog file blocks all mutation."""
        content = "connector.name=mysql\nfoo=bar\n"
        path = f"{CATALOG_DIR}/mysql.properties"
        container = FakeContainer(files={path: content})
        # Digest differs from the desired one, forcing classify_actual to read it.
        container.fail_pull_path = path
        container.fail_pull_error = PathError("generic-file-error", "stat: permission denied")
        desired = DesiredCatalogs(
            static={"mysql": "connector.name=mysql\n"}, credentials={}, dynamic={}
        )
        executor = FakeExecutor()

        result = _reconcile(container, executor, desired)

        self.assertTrue(result.failed)
        self.assertIsNone(result.state_hash)
        self.assertEqual(executor.calls, [])
        self.assertEqual(container.pushes, [])
        self.assertEqual(container.removed, [])

    def test_file_vanishing_between_snapshot_and_read_is_dropped_quietly(self):
        """A catalog file that disappears mid-cycle is neither claimed nor blocking."""
        path = f"{CATALOG_DIR}/gone.properties"

        def _exec_with_ghost_file(command, environment=None):
            directory = command[1]
            if directory != CATALOG_DIR:
                return FakeContainer()._process_for(directory)
            return _Process(stdout=f"{'0' * 64}  {path}\n")

        container = FakeContainer()
        container.exec = _exec_with_ghost_file
        desired = DesiredCatalogs(static={}, credentials={}, dynamic={})
        executor = FakeExecutor()

        result = _reconcile(container, executor, desired)

        self.assertFalse(result.failed)
        self.assertEqual(executor.calls, [])
        self.assertEqual(container.pushes, [])
        self.assertEqual(container.removed, [path])

    def test_steady_state_logs_unchanged_once_with_no_info(self):
        """Matching static state produces a single unchanged DEBUG message."""
        content = "connector.name=postgresql\nfoo=bar\n"
        container = FakeContainer(files={f"{CATALOG_DIR}/pg.properties": content})
        desired = DesiredCatalogs(static={"pg": content}, credentials={}, dynamic={})
        executor = FakeExecutor()

        with self.assertLogs(level="DEBUG") as log_ctx:
            result = _reconcile(container, executor, desired)

        self.assertFalse(result.changed)
        self.assertFalse(result.failed)
        self.assertIsNotNone(result.state_hash)
        self.assertEqual(container.pushes, [])
        self.assertEqual(container.removed, [])
        self.assertTrue(any("catalogs unchanged" in message for message in log_ctx.output))
        self.assertFalse(any(record.levelno == logging.INFO for record in log_ctx.records))

    def test_changed_static_catalog_writes_once_and_changes_hash(self):
        """A changed static catalog is written once and shifts the hash."""
        old_content = "connector.name=postgresql\nfoo=old\n"
        new_content = "connector.name=postgresql\nfoo=new\n"
        container = FakeContainer(files={f"{CATALOG_DIR}/pg.properties": old_content})
        desired = DesiredCatalogs(static={"pg": new_content}, credentials={}, dynamic={})
        executor = FakeExecutor()

        unchanged_result = _reconcile(
            FakeContainer(files={f"{CATALOG_DIR}/pg.properties": old_content}),
            FakeExecutor(),
            DesiredCatalogs(static={"pg": old_content}, credentials={}, dynamic={}),
        )
        result = _reconcile(container, executor, desired)

        self.assertTrue(result.changed)
        self.assertEqual(container.pushes, [(f"{CATALOG_DIR}/pg.properties", new_content)])
        self.assertNotEqual(result.state_hash, unchanged_result.state_hash)

    def test_changed_credential_file_writes_and_changes_hash(self):
        """A changed credential file is written and shifts the hash."""
        container = FakeContainer(files={f"{CREDENTIAL_DIR}/db.json": "old\n"})
        desired = DesiredCatalogs(static={}, credentials={"db.json": "new\n"}, dynamic={})
        executor = FakeExecutor()

        unchanged_result = _reconcile(
            FakeContainer(files={f"{CREDENTIAL_DIR}/db.json": "old\n"}),
            FakeExecutor(),
            DesiredCatalogs(static={}, credentials={"db.json": "old\n"}, dynamic={}),
        )
        result = _reconcile(container, executor, desired)

        self.assertTrue(result.changed)
        self.assertEqual(container.pushes, [(f"{CREDENTIAL_DIR}/db.json", "new\n")])
        self.assertNotEqual(result.state_hash, unchanged_result.state_hash)

    def test_removed_static_catalog_is_deleted_and_hash_changes(self):
        """A static catalog no longer desired is deleted and the hash shifts."""
        content = "connector.name=postgresql\n"
        container = FakeContainer(files={f"{CATALOG_DIR}/pg.properties": content})
        desired = DesiredCatalogs(static={}, credentials={}, dynamic={})
        executor = FakeExecutor()

        present_result = _reconcile(
            FakeContainer(files={f"{CATALOG_DIR}/pg.properties": content}),
            FakeExecutor(),
            DesiredCatalogs(static={"pg": content}, credentials={}, dynamic={}),
        )
        result = _reconcile(container, executor, desired)

        self.assertTrue(result.changed)
        self.assertEqual(container.removed, [f"{CATALOG_DIR}/pg.properties"])
        self.assertNotEqual(result.state_hash, present_result.state_hash)

    def test_dynamic_unchanged_skips_sql_even_with_unrelated_static_change(self):
        """An unchanged dynamic definition triggers no SQL despite other churn."""
        props = _dynamic_properties(foo="bar")
        container = FakeContainer(
            files={
                f"{CATALOG_DIR}/pg.properties": _dynamic_raw(props),
                f"{CATALOG_DIR}/mysql.properties": "connector.name=mysql\nold=1\n",
            }
        )
        desired = DesiredCatalogs(
            static={"mysql": "connector.name=mysql\nnew=1\n"},
            credentials={},
            dynamic={"pg": props},
        )
        executor = FakeExecutor()

        result = _reconcile(container, executor, desired)

        self.assertEqual(executor.calls, [])
        self.assertTrue(result.changed)  # the unrelated static file still changed

    def test_dynamic_definition_change_drops_then_creates(self):
        """A changed dynamic definition is dropped, then recreated."""
        old_props = _dynamic_properties(foo="old")
        new_props = _dynamic_properties(foo="new")
        container = FakeContainer(files={f"{CATALOG_DIR}/pg.properties": _dynamic_raw(old_props)})
        desired = DesiredCatalogs(static={}, credentials={}, dynamic={"pg": new_props})
        executor = FakeExecutor()

        _reconcile(container, executor, desired)

        self.assertEqual(executor.calls, [("drop", "pg"), ("create", "pg")])

    def test_missing_dynamic_already_exists_drops_then_creates(self):
        """`CatalogAlreadyExistsError` triggers a drop and a retried create."""
        props = _dynamic_properties()
        container = FakeContainer()
        desired = DesiredCatalogs(static={}, credentials={}, dynamic={"pg": props})
        executor = FakeExecutor(already_exists={"pg"})

        _reconcile(container, executor, desired)

        self.assertEqual(executor.calls, [("create", "pg"), ("drop", "pg"), ("create", "pg")])

    def test_missing_dynamic_other_sql_error_does_not_drop(self):
        """A non-already-exists create failure never triggers a drop."""
        props = _dynamic_properties()
        container = FakeContainer()
        desired = DesiredCatalogs(static={}, credentials={}, dynamic={"pg": props})
        executor = FakeExecutor(fail_create={"pg"})

        _reconcile(container, executor, desired)

        self.assertEqual(executor.calls, [("create", "pg")])

    def test_dynamic_drop_failure_blocks_only_that_name(self):
        """A drop failure for one dynamic catalog does not stop the others."""
        pg_props = _dynamic_properties(foo="old")
        pg_new = _dynamic_properties(foo="new")
        mysql_props = _dynamic_properties(foo="1")
        container = FakeContainer(
            files={
                f"{CATALOG_DIR}/pg.properties": _dynamic_raw(pg_props),
            }
        )
        desired = DesiredCatalogs(
            static={}, credentials={}, dynamic={"pg": pg_new, "mysql": mysql_props}
        )
        executor = FakeExecutor(fail_drop={"pg"})

        _reconcile(container, executor, desired)

        self.assertIn(("drop", "pg"), executor.calls)
        self.assertNotIn(("create", "pg"), executor.calls)
        self.assertIn(("create", "mysql"), executor.calls)

    def test_static_to_dynamic_handover(self):
        """A stale static file is deleted this cycle; create follows next cycle."""
        container = FakeContainer(files={f"{CATALOG_DIR}/pg.properties": "connector.name=x\n"})
        props = _dynamic_properties()
        desired = DesiredCatalogs(static={}, credentials={}, dynamic={"pg": props})
        executor = FakeExecutor()

        first = _reconcile(container, executor, desired)

        self.assertEqual(executor.calls, [])
        self.assertEqual(container.removed, [f"{CATALOG_DIR}/pg.properties"])
        self.assertTrue(first.changed)

        second_executor = FakeExecutor()
        _reconcile(container, second_executor, desired)

        self.assertEqual(second_executor.calls, [("create", "pg")])

    def test_dynamic_to_static_handover_writes_after_drop(self):
        """A successful drop is followed by the static file write, same cycle."""
        old_props = _dynamic_properties()
        container = FakeContainer(files={f"{CATALOG_DIR}/pg.properties": _dynamic_raw(old_props)})
        desired = DesiredCatalogs(
            static={"pg": "connector.name=postgresql\nstatic=1\n"}, credentials={}, dynamic={}
        )
        executor = FakeExecutor()

        result = _reconcile(container, executor, desired)

        self.assertEqual(executor.calls, [("drop", "pg")])
        self.assertEqual(
            container.pushes,
            [(f"{CATALOG_DIR}/pg.properties", "connector.name=postgresql\nstatic=1\n")],
        )
        self.assertTrue(result.changed)

    def test_dynamic_to_static_handover_skips_write_when_drop_fails(self):
        """A failed drop leaves the static file unwritten this cycle."""
        old_props = _dynamic_properties()
        container = FakeContainer(files={f"{CATALOG_DIR}/pg.properties": _dynamic_raw(old_props)})
        desired = DesiredCatalogs(
            static={"pg": "connector.name=postgresql\nstatic=1\n"}, credentials={}, dynamic={}
        )
        executor = FakeExecutor(fail_drop={"pg"})

        _reconcile(container, executor, desired)

        self.assertEqual(executor.calls, [("drop", "pg")])
        self.assertEqual(container.pushes, [])

    def test_trino_not_ready_skips_sql_but_static_still_applies(self):
        """Static work proceeds for non-conflicting names when Trino is not ready."""
        container = FakeContainer(files={f"{CATALOG_DIR}/mysql.properties": "old\n"})
        desired = DesiredCatalogs(
            static={"mysql": "new\n"}, credentials={}, dynamic={"pg": _dynamic_properties()}
        )
        executor = FakeExecutor(ready=False)

        result = _reconcile(container, executor, desired)

        self.assertEqual(executor.calls, [])
        self.assertTrue(result.changed)
        self.assertEqual(container.pushes, [(f"{CATALOG_DIR}/mysql.properties", "new\n")])

    def test_dynamic_marked_files_are_never_deleted_by_static_pass(self):
        """A file carrying the dynamic marker is never a deletion candidate."""
        props = _dynamic_properties()
        container = FakeContainer(files={f"{CATALOG_DIR}/pg.properties": _dynamic_raw(props)})
        desired = DesiredCatalogs(static={}, credentials={}, dynamic={})
        executor = FakeExecutor()

        _reconcile(container, executor, desired)

        self.assertEqual(container.removed, [])

    def test_marker_text_outside_the_ownership_property_is_not_dynamic(self):
        """Mentioning the marker elsewhere does not claim dynamic ownership."""
        container = FakeContainer(
            files={
                f"{CATALOG_DIR}/pg.properties": (
                    "# dynamic catalog\n"
                    "connector.name=postgresql\n"
                    "query.comment-format=static reporting\n"
                )
            }
        )
        desired = DesiredCatalogs(static={}, credentials={}, dynamic={})
        executor = FakeExecutor()

        _reconcile(container, executor, desired)

        self.assertEqual(container.removed, [f"{CATALOG_DIR}/pg.properties"])
        self.assertEqual(executor.calls, [])

    def test_non_properties_files_are_ignored_in_the_catalog_directory(self):
        """Only `.properties` files take part in catalog classification."""
        props = _dynamic_properties()
        container = FakeContainer(files={f"{CATALOG_DIR}/pg.properties.bak": _dynamic_raw(props)})
        desired = DesiredCatalogs(static={}, credentials={}, dynamic={})
        executor = FakeExecutor()

        _reconcile(container, executor, desired)

        self.assertEqual(container.removed, [f"{CATALOG_DIR}/pg.properties.bak"])
        self.assertEqual(executor.calls, [])

    def test_partial_static_batch_failure_reports_no_state_hash(self):
        """A push failure partway through the batch fails the whole pass."""
        container = FakeContainer()
        container.fail_push_path = f"{CATALOG_DIR}/pg.properties"
        desired = DesiredCatalogs(
            static={"pg": "connector.name=postgresql\n", "mysql": "connector.name=mysql\n"},
            credentials={},
            dynamic={},
        )
        executor = FakeExecutor()

        result = _reconcile(container, executor, desired)

        self.assertTrue(result.failed)
        self.assertIsNone(result.state_hash)
