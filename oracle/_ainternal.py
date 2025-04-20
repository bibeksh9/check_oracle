"""Shared async utility functions for the Postgres checkpoint & storage classes."""

from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from typing import Union

from oracledb import AsyncConnection, AsyncConnectionPool

Conn = Union[AsyncConnection, AsyncConnectionPool]


@asynccontextmanager
async def get_connection(
    conn: Conn,
) -> AsyncIterator[AsyncConnection]:
    """Acquire an async Oracle DB connection from a connection or pool."""
    if isinstance(conn, AsyncConnection):
        yield conn
    elif isinstance(conn, AsyncConnectionPool):
        async with conn.acquire() as conn2:
            yield conn2
    else:
        raise TypeError(f"Invalid connection type: {type(conn)}")
