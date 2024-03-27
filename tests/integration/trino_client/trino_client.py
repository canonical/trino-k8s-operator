# Copyright 2023 Canonical Ltd.
# See LICENSE file for licensing details.


"""Trino client activity."""

from trino.dbapi import connect


async def query_trino(host, user, query) -> str:
    """Trino catalogs.

    Args:
        host: trino server address.
        user: the user with which to access Trino.
        query: the query to execute.

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
    cur.execute(query)
    result = cur.fetchall()
    return result
