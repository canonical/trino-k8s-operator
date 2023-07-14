# Copyright 2023 Canonical Ltd.
# See LICENSE file for licensing details.


"""Trino client activity."""

from trino.auth import BasicAuthentication
from trino.dbapi import connect


async def show_catalogs(host, password) -> str:
    """Trino catalogs.

    Returns:
        List of Trino catalogs.
    """
    conn = connect(
        host=host,
        port=8443,
        user="trino",
        auth=BasicAuthentication("trino", password),
        http_scheme="https",
        verify=False,
    )
    cur = conn.cursor()
    cur.execute("SHOW CATALOGS")
    result = cur.fetchall()
    return result
