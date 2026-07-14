# Copyright 2023 Canonical Ltd.
# See LICENSE file for licensing details.


"""Trino client activity."""

import logging
import time

import trino.exceptions
from trino.dbapi import connect

logger = logging.getLogger(__name__)


def query_trino(host, user, query, max_wait=300, retry_interval=10) -> str:
    """Trino catalogs.

    Retries while the coordinator is unavailable after a config-triggered
    restart: connection-refused errors (process down, port 8080 not yet bound)
    and transient `SERVER_STARTING_UP` errors (process up but initializing).
    A cold JVM restart can take minutes, so the deadline is generous. All other
    errors are raised immediately.

    Args:
        host: trino server address.
        user: the user with which to access Trino.
        query: query to execute.
        max_wait: maximum total seconds to keep retrying while the server is
            unreachable or still initializing.
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

    deadline = time.monotonic() + max_wait
    while True:
        try:
            cur.execute(query)
            result = cur.fetchall()
            return result
        except trino.exceptions.TrinoConnectionError:
            # Connection refused while Pebble restarts the service to apply a
            # config change and port 8080 is not yet bound. Retry until it binds.
            if time.monotonic() >= deadline:
                raise
            logger.info("Trino server is unreachable, retrying in %ss", retry_interval)
            time.sleep(retry_interval)
        except trino.exceptions.TrinoQueryError as err:
            # While we are not past the deadline
            # retry after server initializing errors.
            retry = time.monotonic() < deadline and (
                err.error_name == "SERVER_STARTING_UP"  # Coordinator initializing
                or (
                    err.error_name == "PAGE_TRANSPORT_ERROR"
                    and "server is still initializing" in err.message
                )  # Worker initializing
            )
            if not retry:
                raise
            logger.info("Trino server is still initializing, retrying in %ss", retry_interval)
            time.sleep(retry_interval)
