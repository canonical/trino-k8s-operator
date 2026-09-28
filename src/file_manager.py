# Copyright 2023 Canonical Ltd.
# See LICENSE file for licensing details.

"""Generic, change-aware workload filesystem reconciliation.

This module inventories files under a set of directories, writes managed
files only when their desired content differs from what is on disk, and
deletes files that are no longer managed. It knows nothing about catalogs,
Pebble plans or service restarts: callers decide what "managed" means and
what to do with the results.
"""

import dataclasses
import logging
import posixpath
import re

from ops.pebble import Error as PebbleError
from ops.pebble import ExecError, PathError

from utils import content_hash

logger = logging.getLogger(__name__)

FIND_BIN = "/usr/bin/find"
SHA256SUM_BIN = "/usr/bin/sha256sum"


@dataclasses.dataclass(frozen=True)
class Inventory:
    """A snapshot of file content hashes taken from one or more directories.

    Attrs:
        files: Mapping of absolute path to SHA-256 hex digest.
        failed: True when the snapshot could not be trusted, for example
            because a directory listing or a hash computation failed for a
            reason other than the directory being absent.
    """

    files: dict[str, str]
    failed: bool


@dataclasses.dataclass(frozen=True)
class FileReconcileResult:
    """The outcome of reconciling one or more managed file groups.

    Attrs:
        changed: True when at least one write or deletion was applied.
        failed: True when a write or deletion failed partway through.
        desired_hashes: Mapping of absolute path to the SHA-256 hex digest
            of its desired content, covering every managed path regardless
            of whether it was actually written.
    """

    changed: bool
    failed: bool
    desired_hashes: dict[str, str]


_SHA256SUM_LINE = re.compile(r"^(?P<digest>[0-9a-f]{64}) [ *](?P<path>\S.*)$")
_MISSING_PATH_LINE = re.compile(
    r"^[^:]*find: (?:cannot (?:search|access|open) )?(?P<operand>.+?): No such file or directory$"
)
_QUOTES = "'\"`\u2018\u2019"


def _normalize(path) -> str:
    """Return a canonical string form of a workload path.

    Args:
        path: The path to normalize, as a string or `Path`.

    Returns:
        The path with redundant separators, trailing separators and relative
        components collapsed, so that equivalent spellings compare equal.
    """
    return posixpath.normpath(str(path))


def _is_safe_name(name) -> bool:
    """Check that a managed file name stays inside its group directory.

    Args:
        name: The file name supplied by the caller.

    Returns:
        True when the name is a plain file name with no separators, no
        absolute prefix and no relative components.
    """
    text = str(name)
    if not text or text in (".", ".."):
        return False
    return "/" not in text and not posixpath.isabs(text)


def _is_missing_root(directory: str, stderr: str) -> bool:
    """Check whether a `find` failure is solely the scope root being absent.

    Args:
        directory: The normalized directory that was scanned.
        stderr: The standard error text produced by the `find` invocation.

    Returns:
        True when every error line reports that the scanned directory itself
        does not exist, tolerating the quoting styles GNU `find` may use.
    """
    lines = [line.strip() for line in stderr.splitlines() if line.strip()]
    if not lines:
        return False
    for line in lines:
        match = _MISSING_PATH_LINE.match(line)
        if match is None:
            return False
        operand = match.group("operand").strip().strip(_QUOTES)
        if _normalize(operand) != directory:
            return False
    return True


def _parse_hashes(directory: str, stdout: str) -> tuple[dict[str, str], bool]:
    """Parse the output of a `sha256sum` batch.

    Args:
        directory: The normalized directory the output belongs to, used for
            logging only.
        stdout: The standard output text produced by the `sha256sum` batch.

    Returns:
        A tuple of a mapping from absolute path to hex digest and a flag that
        is True when any line could not be parsed.
    """
    files: dict[str, str] = {}
    failed = False
    for line in stdout.splitlines():
        if not line.strip():
            continue
        match = _SHA256SUM_LINE.match(line)
        if match is None:
            logger.warning("Unparsable checksum line for %s: %r", directory, line)
            failed = True
            continue
        files[_normalize(match.group("path"))] = match.group("digest")
    return files, failed


def inventory(container, directories) -> Inventory:
    """Take a hash snapshot of files directly inside a set of directories.

    Runs one `find | sha256sum` batch per directory so that the cost is
    independent of how many files it contains.

    Args:
        container: The workload container to run commands against.
        directories: The directories to scan (non-recursive).

    Returns:
        The combined `Inventory` across all directories.
    """
    files: dict[str, str] = {}
    failed = False
    for raw_directory in directories:
        directory = _normalize(raw_directory)
        command = [
            FIND_BIN,
            directory,
            "-maxdepth",
            "1",
            "-type",
            "f",
            "-exec",
            SHA256SUM_BIN,
            "{}",
            "+",
        ]
        # A non-zero exit (missing directory, a bad -exec) surfaces as
        # ExecError; recover whatever partial output it still carries. The C
        # locale keeps the diagnostics of `find` parsable.
        try:
            process = container.exec(command, environment={"LC_ALL": "C"})
            stdout, stderr = process.wait_output()
        except ExecError as e:
            stdout, stderr = e.stdout or "", e.stderr or ""
            if not stdout.strip() and _is_missing_root(directory, stderr):
                continue
            logger.warning("Failed to inventory %s: %s", directory, stderr)
            failed = True
        except PebbleError as e:
            logger.warning("Failed to inventory %s: %s", directory, e)
            failed = True
            continue
        else:
            if stderr.strip():
                logger.warning("Partial failure inventorying %s: %s", directory, stderr)
                failed = True

        parsed, parse_failed = _parse_hashes(directory, stdout)
        files.update(parsed)
        failed = failed or parse_failed

    return Inventory(files=files, failed=failed)


def read_files(container, paths) -> tuple[dict[str, str], bool]:
    """Pull the text content of a set of files from the workload container.

    Args:
        container: The workload container to pull files from.
        paths: The absolute paths to read.

    Returns:
        A tuple of a mapping from path to text content (missing files are
        omitted) and a flag that is True when any non-missing-file pull
        failed.
    """
    contents: dict[str, str] = {}
    failed = False
    for path in paths:
        try:
            contents[path] = container.pull(path).read()
        except PathError as e:
            if e.kind == "not-found":
                continue
            logger.warning("Failed to read %s: %s", path, e)
            failed = True
        except PebbleError as e:
            logger.warning("Failed to read %s: %s", path, e)
            failed = True
    return contents, failed


def _plan_desired(groups) -> tuple[dict[str, str], set[str], list[str]]:
    """Resolve the desired managed paths, their hashes and any unsafe names.

    Args:
        groups: Mapping of directory to a mapping of file name to desired
            text content.

    Returns:
        A tuple of the desired hashes keyed by absolute path, the set of
        normalized group directories, and the rejected file names.
    """
    desired_hashes: dict[str, str] = {}
    group_dirs: set[str] = set()
    rejected: list[str] = []
    for directory, files in groups.items():
        group_dir = _normalize(directory)
        group_dirs.add(group_dir)
        for name, content in files.items():
            if not _is_safe_name(name):
                rejected.append(str(name))
                continue
            desired_hashes[posixpath.join(group_dir, str(name))] = content_hash(content)
    return desired_hashes, group_dirs, rejected


def _write_changed(container, groups, actual, desired_hashes) -> tuple[bool, bool]:
    """Push every managed file whose content differs from the snapshot.

    Args:
        container: The workload container to write to.
        groups: Mapping of directory to a mapping of file name to content.
        actual: Mapping of absolute path to the hash currently on disk.
        desired_hashes: Mapping of absolute path to the desired hash.

    Returns:
        A tuple of whether anything was written and whether a write failed.
    """
    changed = False
    for directory, files in groups.items():
        group_dir = _normalize(directory)
        for name, content in files.items():
            path = posixpath.join(group_dir, str(name))
            if actual.get(path) == desired_hashes[path]:
                continue
            try:
                container.push(path, content, make_dirs=True, permissions=0o644)
            except PebbleError as e:
                logger.warning("Failed to write %s: %s", path, e)
                return changed, True
            changed = True
    return changed, False


def _delete_unmanaged(container, actual, desired_hashes, group_dirs, protected):
    """Delete snapshot files that are neither managed nor protected.

    Args:
        container: The workload container to delete from.
        actual: Mapping of absolute path to the hash currently on disk.
        desired_hashes: Mapping of absolute path to the desired hash.
        group_dirs: The normalized directories under charm ownership.
        protected: Absolute paths that must never be deleted.

    Returns:
        A tuple of whether anything was deleted and whether a deletion failed.
    """
    changed = False
    for path in sorted(actual):
        if path in desired_hashes or path in protected:
            continue
        if posixpath.dirname(path) not in group_dirs:
            continue
        try:
            container.remove_path(path)
        except PebbleError as e:
            logger.warning("Failed to remove %s: %s", path, e)
            return changed, True
        changed = True
    return changed, False


def reconcile_files(
    container,
    groups,
    current: Inventory,
    remove_unmanaged: bool = True,
    protect=None,
) -> FileReconcileResult:
    """Write changed managed files and delete unmanaged ones.

    Args:
        container: The workload container to write to and delete from.
        groups: Mapping of directory (str or `Path`) to a mapping of file
            name to desired text content.
        current: The `Inventory` snapshot taken before this reconciliation.
        remove_unmanaged: When True, delete files found in `current` that
            live under a group directory but are not managed and not
            protected.
        protect: Absolute paths that must never be deleted. Defaults to an
            empty set.

    Returns:
        The `FileReconcileResult` describing what changed. Nothing is written
        or deleted when the snapshot is untrustworthy or a managed file name
        is rejected.
    """
    protected = {_normalize(path) for path in (protect or set())}
    desired_hashes, group_dirs, rejected = _plan_desired(groups)

    if rejected:
        logger.error("Rejected unsafe managed file names: %s", sorted(rejected))
        return FileReconcileResult(changed=False, failed=True, desired_hashes=desired_hashes)

    if current.failed:
        return FileReconcileResult(changed=False, failed=True, desired_hashes=desired_hashes)

    actual = {_normalize(path): digest for path, digest in current.files.items()}
    changed, failed = _write_changed(container, groups, actual, desired_hashes)
    if failed:
        return FileReconcileResult(changed=changed, failed=True, desired_hashes=desired_hashes)

    if remove_unmanaged:
        deleted, failed = _delete_unmanaged(
            container, actual, desired_hashes, group_dirs, protected
        )
        changed = changed or deleted
        if failed:
            return FileReconcileResult(changed=changed, failed=True, desired_hashes=desired_hashes)

    return FileReconcileResult(changed=changed, failed=False, desired_hashes=desired_hashes)
