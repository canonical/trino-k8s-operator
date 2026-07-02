# Copyright 2023 Canonical Ltd.
# See LICENSE file for licensing details.


"""Trino client activity."""

import logging
import time

import trino.exceptions
from trino.dbapi import connect

logger = logging.getLogger(__name__)


def query_trino(host, user, query, max_wait=120, retry_interval=10) -> str:
    """Trino catalogs.

    Retries transient `SERVER_STARTING_UP` errors, which occur when the Trino
    server process is up but still initializing (e.g. shortly after a restart
    triggered by a config change). All other errors are raised immediately.

    Args:
        host: trino server address.
        user: the user with which to access Trino.
        query: query to execute.
        max_wait: maximum total seconds to keep retrying while the server is
            still initializing.
        retry_interval: seconds to wait between retries.

    Returns:
        result: list of Trino catalogs
    """
    conn = connect(
        host=host,
        port=8080,
        user=user,
        http_scheme="http",
        verify=False,
    )
    cur = conn.cursor()

    deadline = time.time() + max_wait
    while True:
        try:
            cur.execute(query)
            result = cur.fetchall()
            return result
        except trino.exceptions.TrinoQueryError as err:
            if err.error_name != "SERVER_STARTING_UP" or time.time() >= deadline:
                raise
            logger.info("Trino server is still initializing, retrying in %ss", retry_interval)
            time.sleep(retry_interval)
