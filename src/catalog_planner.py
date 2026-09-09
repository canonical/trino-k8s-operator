# Copyright 2023 Canonical Ltd.
# See LICENSE file for licensing details.

"""Stateless catalog reconciliation planner.

This module is the top-level coordinator for catalog reconciliation. It
validates ownership claims between static and dynamic desired state,
classifies what actually exists on the workload filesystem, executes
dynamic catalog SQL against Trino, applies static catalog and credential
files through `file_manager`, and computes the aggregate restart hash. It
holds no persistent state of its own: actual state is derived solely from
the filesystem snapshot taken at the start of each call.
"""

import dataclasses
import hashlib
import logging
import posixpath

from file_manager import Inventory, inventory, read_files, reconcile_files
from relations.postgresql_catalog import (
    DYNAMIC_CATALOG_MARKER,
    CatalogAlreadyExistsError,
    CatalogSQLError,
    canonical_from_raw,
    canonical_properties,
)
from utils import content_hash

logger = logging.getLogger(__name__)


@dataclasses.dataclass(frozen=True)
class DesiredCatalogs:
    """The rendered desired state for every catalog source.

    Attrs:
        static: Mapping of static catalog name to `.properties` text.
        credentials: Mapping of credential file name to text content.
        dynamic: Mapping of dynamic catalog name to its properties mapping.
    """

    static: dict
    credentials: dict
    dynamic: dict


@dataclasses.dataclass(frozen=True)
class ActualCatalogs:
    """The classified actual state of the catalog directory.

    Attrs:
        static: Mapping of catalog name to the content hash of its on-disk
            `.properties` file, for files that are not dynamically owned.
        dynamic: Mapping of catalog name to the canonical hash of its
            on-disk `.properties` file, for files carrying the dynamic
            catalog marker.
        failed: True when the classification could not be trusted, in which
            case no mutation may be made.
    """

    static: dict
    dynamic: dict
    failed: bool


@dataclasses.dataclass(frozen=True)
class CatalogReconcileResult:
    """The outcome of a full catalog reconciliation pass.

    Attrs:
        duplicates: Catalog names claimed by both static and dynamic
            desired state. Non-empty only when reconciliation was blocked
            before any mutation.
        failed: True when the pass could not be completed and no restart
            should follow.
        changed: True when at least one static file was written or deleted.
        state_hash: The aggregate restart hash, or None when the pass
            failed or was blocked.
    """

    duplicates: set
    failed: bool
    changed: bool
    state_hash: str | None


def find_duplicate_names(desired: DesiredCatalogs) -> set:
    """Find catalog names claimed by both static and dynamic desired state.

    Args:
        desired: The rendered desired state.

    Returns:
        The set of catalog names present in both `desired.static` and
        `desired.dynamic`.
    """
    return set(desired.static) & set(desired.dynamic)


def _catalog_name(path: str) -> str:
    """Derive a catalog name from a `.properties` file path.

    Args:
        path: The absolute file path.

    Returns:
        The file stem with a trailing `.properties` suffix removed.
    """
    stem = posixpath.basename(path)
    suffix = ".properties"
    return stem[: -len(suffix)] if stem.endswith(suffix) else stem


def classify_actual(container, catalog_dir: str, files: dict, desired_static: dict):
    """Classify the catalog directory files from an inventory snapshot.

    A file whose hash equals a desired static catalog hash is treated as
    static without being read. Every other catalog file is pulled once so
    that the dynamic marker can be detected.

    Args:
        container: The workload container to read ambiguous files from.
        catalog_dir: The normalized catalog directory path.
        files: Mapping of absolute path to content hash, from an inventory
            snapshot covering at least `catalog_dir`.
        desired_static: Mapping of desired static catalog name to
            `.properties` text.

    Returns:
        A tuple of the `ActualCatalogs` classification and the set of
        absolute paths that carried the dynamic marker at snapshot time.
    """
    desired_hashes = {content_hash(text) for text in desired_static.values()}
    catalog_files = {
        path: digest for path, digest in files.items() if posixpath.dirname(path) == catalog_dir
    }
    to_read = [path for path, digest in catalog_files.items() if digest not in desired_hashes]
    contents, failed = read_files(container, to_read)

    static, dynamic, protected = {}, {}, set()
    for path, digest in catalog_files.items():
        name = _catalog_name(path)
        if digest in desired_hashes and path not in contents:
            static[name] = digest
            continue
        raw = contents.get(path)
        if raw is None:
            # Deleted between the inventory snapshot and the read: it is no
            # longer a candidate for anything this cycle.
            continue
        if DYNAMIC_CATALOG_MARKER in raw:
            dynamic[name] = canonical_from_raw(raw)
            protected.add(path)
        else:
            static[name] = digest

    return ActualCatalogs(static=static, dynamic=dynamic, failed=failed), protected


def _reconcile_one_dynamic(executor, name: str, props: dict, actual_hash: str | None) -> None:
    """Apply the SQL needed to bring one dynamic catalog to its desired state.

    Args:
        executor: Object exposing `create_catalog` and `drop_catalog`.
        name: The catalog name.
        props: The desired properties for `name`.
        actual_hash: The canonical hash currently on disk for `name`, or
            None when no dynamic file exists for it.

    Raises:
        CatalogSQLError: A SQL operation failed. `CatalogAlreadyExistsError`
            is handled internally and never propagates.
    """
    if actual_hash is None:
        try:
            executor.create_catalog(name, props)
        except CatalogAlreadyExistsError:
            executor.drop_catalog(name)
            executor.create_catalog(name, props)
        return
    if actual_hash != canonical_properties(props):
        executor.drop_catalog(name)
        executor.create_catalog(name, props)


def apply_dynamic(executor, desired: dict, actual: ActualCatalogs, enabled: bool) -> set:
    """Bring dynamic catalogs to their desired state via SQL.

    Args:
        executor: Object exposing `is_trino_ready`, `create_catalog` and
            `drop_catalog`.
        desired: Mapping of desired dynamic catalog name to properties.
        actual: The classified actual state of the catalog directory.
        enabled: Whether this unit may execute dynamic catalog SQL (the
            coordinator role).

    Returns:
        The set of catalog names to skip in the static pass this cycle:
        desired dynamic names still held by a static file, and names whose
        SQL reconciliation failed.
    """
    if not enabled or not executor.is_trino_ready():
        return set(desired)

    blocked = set()
    for name in sorted(set(desired) | set(actual.dynamic)):
        if name in desired and name in actual.static:
            blocked.add(name)
            continue
        try:
            if name not in desired:
                executor.drop_catalog(name)
            else:
                _reconcile_one_dynamic(executor, name, desired[name], actual.dynamic.get(name))
        except CatalogSQLError:
            logger.error("Failed to reconcile dynamic catalog %r", name)
            blocked.add(name)
    return blocked


def apply_static(
    container,
    desired: DesiredCatalogs,
    current: Inventory,
    catalog_dir: str,
    credential_dir: str,
    skip: set,
    protected: set,
):
    """Apply static catalog and credential files through the file manager.

    Args:
        container: The workload container to write to and delete from.
        desired: The rendered desired state.
        current: The inventory snapshot covering `catalog_dir` and
            `credential_dir`, taken before any dynamic SQL was executed.
        catalog_dir: The absolute catalog directory path.
        credential_dir: The absolute credential directory path.
        skip: Catalog names to omit from the static catalog group, because
            they are owned dynamically this cycle or their handover is
            still pending.
        protected: Absolute paths that must never be deleted, because they
            carried the dynamic marker at snapshot time.

    Returns:
        The `file_manager.FileReconcileResult` describing what changed.
    """
    static_files = {
        f"{name}.properties": text for name, text in desired.static.items() if name not in skip
    }
    groups = {catalog_dir: static_files, credential_dir: desired.credentials}
    return reconcile_files(container, groups, current, remove_unmanaged=True, protect=protected)


def aggregate_hash(desired_hashes: dict, root: str) -> str:
    """Compute the single restart-relevant hash for the Pebble plan.

    Args:
        desired_hashes: Mapping of absolute path to content hash, covering
            only restart-relevant static catalog and credential files.
        root: The Trino home directory that paths are made relative to.

    Returns:
        A SHA-256 hex digest of the sorted `"<relative path>:<hash>"`
        entries.
    """
    entries = sorted(
        f"{posixpath.relpath(path, root)}:{digest}" for path, digest in desired_hashes.items()
    )
    return hashlib.sha256("\n".join(entries).encode()).hexdigest()


def _blocked_result(duplicates=frozenset(), changed=False) -> CatalogReconcileResult:
    """Build a `CatalogReconcileResult` for a pass that made no restart.

    Args:
        duplicates: Catalog names claimed by both static and dynamic
            desired state, if that is why the pass was blocked.
        changed: Whether a partially applied static batch changed anything.

    Returns:
        A failed or duplicate-blocked `CatalogReconcileResult` with no
        state hash.
    """
    return CatalogReconcileResult(
        duplicates=set(duplicates), failed=not duplicates, changed=changed, state_hash=None
    )


def reconcile_catalogs(
    container,
    executor,
    desired: DesiredCatalogs,
    catalog_dir: str,
    credential_dir: str,
    root: str,
    dynamic_enabled: bool,
) -> CatalogReconcileResult:
    """Reconcile static and dynamic catalogs against the workload filesystem.

    Args:
        container: The workload container to inventory, read, write to and
            delete from.
        executor: Object exposing `is_trino_ready`, `create_catalog` and
            `drop_catalog` for dynamic catalog SQL.
        desired: The rendered desired state for every catalog source.
        catalog_dir: The absolute catalog directory path.
        credential_dir: The absolute credential directory path.
        root: The Trino home directory that restart-hash paths are made
            relative to.
        dynamic_enabled: Whether this unit may execute dynamic catalog SQL.

    Returns:
        The `CatalogReconcileResult` describing what happened. No Pebble
        work is done here: the caller applies the aggregate hash and calls
        `replan`.
    """
    duplicates = find_duplicate_names(desired)
    if duplicates:
        logger.error("Catalog names claimed by both static and dynamic state: %s", duplicates)
        return _blocked_result(duplicates=duplicates)

    catalog_dir = posixpath.normpath(catalog_dir)
    credential_dir = posixpath.normpath(credential_dir)
    snapshot = inventory(container, [catalog_dir, credential_dir])
    if snapshot.failed:
        return _blocked_result()

    actual, protected = classify_actual(container, catalog_dir, snapshot.files, desired.static)
    if actual.failed:
        return _blocked_result()

    blocked = apply_dynamic(executor, desired.dynamic, actual, dynamic_enabled)
    result = apply_static(
        container, desired, snapshot, catalog_dir, credential_dir, blocked, protected
    )
    if result.failed:
        return _blocked_result(changed=result.changed)

    logger.debug("catalogs changed" if result.changed else "catalogs unchanged")
    state_hash = aggregate_hash(result.desired_hashes, root)
    return CatalogReconcileResult(
        duplicates=set(), failed=False, changed=result.changed, state_hash=state_hash
    )
