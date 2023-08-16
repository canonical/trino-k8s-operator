# Copyright 2023 Canonical Ltd.
# See LICENSE file for licensing details.


"""Trino client activity."""

from trino.dbapi import connect


async def show_catalogs(host) -> str:
    """Trino catalogs.

    Args:
        host: trino server address.

    Returns:
        result: list of Trino catalogs
    """
    conn = connect(
        host=host,
        port=8080,
        user="trino",
        http_scheme="http",
        verify=False,
    )
    cur = conn.cursor()
    cur.execute("SHOW CATALOGS")
    result = cur.fetchall()
    return result
