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
import re
from pathlib import Path

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


def _missing_root_only(directory: str, stderr: str) -> bool:
    """Check whether a `find` failure is solely the scope root being absent.

    Args:
        directory: The directory that was scanned.
        stderr: The standard error text produced by the `find` invocation.

    Returns:
        True when every error line refers to the given directory missing,
        tolerating both the quoted and unquoted forms GNU `find` may use.
    """
    lines = [line for line in stderr.splitlines() if line.strip()]
    if not lines:
        return False
    pattern = re.compile(
        rf"^{re.escape(FIND_BIN)}: .*{re.escape(directory)}.*No such file or directory"
    )
    return all(pattern.search(line) for line in lines)


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
    for directory in directories:
        directory = str(directory)
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
        # ExecError; recover whatever partial output it still carries.
        try:
            process = container.exec(command)
            stdout, stderr = process.wait_output()
        except ExecError as e:
            stdout, stderr = e.stdout or "", e.stderr or ""
            lines = [line for line in stdout.splitlines() if line.strip()]
            if not lines and _missing_root_only(directory, stderr):
                continue
            logger.warning("Failed to inventory %s: %s", directory, stderr)
            failed = True
        except PebbleError as e:
            logger.warning("Failed to inventory %s: %s", directory, e)
            failed = True
            continue
        else:
            if stderr and not _missing_root_only(directory, stderr):
                logger.warning("Partial failure inventorying %s: %s", directory, stderr)
                failed = True

        for line in stdout.splitlines():
            if not line.strip():
                continue
            try:
                digest, path = line.split("  ", 1)
            except ValueError:
                logger.warning("Unparsable sha256sum line for %s: %r", directory, line)
                failed = True
                continue
            files[path] = digest

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
        The `FileReconcileResult` describing what changed.
    """
    protect = protect or set()
    desired_hashes: dict[str, str] = {}
    managed_paths: set[str] = set()
    changed = False

    for directory, files in groups.items():
        directory = Path(directory)
        for name, content in files.items():
            path = str(directory / name)
            managed_paths.add(path)
            digest = content_hash(content)
            desired_hashes[path] = digest
            if current.files.get(path) == digest:
                continue
            try:
                container.push(path, content, make_dirs=True, permissions=0o644)
            except PebbleError as e:
                logger.warning("Failed to write %s: %s", path, e)
                return FileReconcileResult(
                    changed=changed, failed=True, desired_hashes=desired_hashes
                )
            changed = True

    if remove_unmanaged:
        group_dirs = {Path(directory) for directory in groups}
        for path in current.files:
            if path in managed_paths or path in protect:
                continue
            if Path(path).parent not in group_dirs:
                continue
            try:
                container.remove_path(path)
            except PebbleError as e:
                logger.warning("Failed to remove %s: %s", path, e)
                return FileReconcileResult(
                    changed=changed, failed=True, desired_hashes=desired_hashes
                )
            changed = True

    return FileReconcileResult(changed=changed, failed=False, desired_hashes=desired_hashes)
