#!/usr/bin/env python3
# Copyright 2023 Canonical Ltd.
# See LICENSE file for licensing details.

"""Patch Elasticsearch client 7.17.29 to work with OpenSearch.

Problem
-------
Ranger 2.8.0 bundles elasticsearch-rest-high-level-client 7.17.29, which
rejects OpenSearch because it returns a different tagline than Elasticsearch.
Every audit write fails with:

    ElasticsearchException[Invalid or missing tagline
    [The OpenSearch Project: https://opensearch.org/]]

This was introduced in ES client 7.14 as a deliberate compatibility gate.
Ranger 2.5.0 used an older client that didn't have this check.
See: https://github.com/opensearch-project/OpenSearch/issues/1005

What the patch does
-------------------
In RestHighLevelClient.performClientRequest(), there is a validation check:

    Optional<String> error = getVersionValidationFuture().get();
    if (error.isPresent()) {            // gate
        throw new ElasticsearchException(error.get());
    }
    return client.performRequest(req);  // actual request

The patch changes the bytecode so the isPresent() result is discarded
instead of branching to the exception. The validation still runs, its
result is just ignored. 3 bytes change per patch site:

    Before: 9A 00 0C  (ifne = branch to throw if validation failed)
    After:  57 00 00  (pop nop nop = discard result, continue)

Updating for a new ES client version
-------------------------------------
If the ES client version changes, this script will find no jars and fail.
Update ES_CLIENT_VERSION below, then extract the new jar and run:

    javap -c -p org/elasticsearch/client/RestHighLevelClient.class | grep isPresent

If the constant pool index for Optional.isPresent() changed from #526,
update PATCH_OLD and PATCH_NEW with the new bytes. The index appears as
the two bytes after B6 (invokevirtual) in the pattern.

Usage
-----
    # Trino (only the ranger plugin, NOT the elasticsearch connector):
    python3 elasticsearch_patch.py /usr/lib/trino/plugin/ranger

    # Ranger Admin (both classpath locations):
    python3 elasticsearch_patch.py \\
        /usr/lib/ranger/admin/ews/lib \\
        /usr/lib/ranger/admin/ews/webapp/WEB-INF/lib
"""

import glob
import os
import subprocess
import sys

ES_CLIENT_VERSION = "7.17.29"

# invokevirtual #526 (Optional.isPresent) + ifne +12
PATCH_OLD = bytes([0xB6, 0x02, 0x0E, 0x9A, 0x00, 0x0C])
# invokevirtual #526 (Optional.isPresent) + pop + nop + nop
PATCH_NEW = bytes([0xB6, 0x02, 0x0E, 0x57, 0x00, 0x00])

CLASS_ENTRY = "org/elasticsearch/client/RestHighLevelClient.class"


def patch_jar(jar_path):
    """Extract, patch, and repack a single jar."""
    print(f"Patching: {jar_path}")

    subprocess.run(
        ["jar", "xf", jar_path, CLASS_ENTRY], cwd="/tmp", check=True
    )

    class_file = f"/tmp/{CLASS_ENTRY}"
    with open(class_file, "rb") as f:
        data = bytearray(f.read())

    count = 0
    i = 0
    while i < len(data) - len(PATCH_OLD):
        if data[i : i + len(PATCH_OLD)] == PATCH_OLD:
            data[i : i + len(PATCH_NEW)] = PATCH_NEW
            count += 1
            print(f"  Patched at byte offset {i}")
        i += 1

    assert count > 0, (
        f"No patch sites found. The ES client version or bytecode may have "
        f"changed from {ES_CLIENT_VERSION}. See patch script header for update steps."
    )

    with open(class_file, "wb") as f:
        f.write(data)

    subprocess.run(
        ["jar", "uf", jar_path, CLASS_ENTRY], cwd="/tmp", check=True
    )
    print(f"  Done: {count} site(s) patched")


def main():
    directories = sys.argv[1:]
    if not directories:
        print(f"Usage: {sys.argv[0]} <directory> [<directory> ...]")
        sys.exit(1)

    jars = []
    for d in directories:
        jars.extend(
            glob.glob(
                os.path.join(d, f"*high-level-client-{ES_CLIENT_VERSION}.jar")
            )
        )

    assert jars, (
        f"No elasticsearch-rest-high-level-client-{ES_CLIENT_VERSION}.jar found in: "
        + ", ".join(directories)
    )

    for jar in jars:
        patch_jar(jar)

    print(f"\nSuccess: {len(jars)} jar(s) patched.")


if __name__ == "__main__":
    main()
