# Copyright 2023 Canonical Ltd.
# See LICENSE file for licensing details.


"""Trino client activity."""

from trino.dbapi import connect
from trino.auth import BasicAuthentication

async def show_catalogs(host) -> str:
    """Trino catalogs.

    Returns:
        List of Trino catalogs.
    """
    conn = connect(
        host=host,
        port=8443,
        user="trino",
        auth=BasicAuthentication("trino", "dummycreds123"),
        http_scheme="https",
        verify=False,
    )
    cur = conn.cursor()
    cur.execute('SHOW CATALOGS')
    result = cur.fetchall()
    return result
