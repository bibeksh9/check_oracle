"""Shared utility functions for the Postgres checkpoint & storage classes."""

from collections.abc import Iterator
from contextlib import contextmanager
from typing import Union

from oracledb import Connection, SessionPool

Conn = Union[Connection, SessionPool]


@contextmanager
def get_connection(conn: Conn) -> Iterator[Connection]:
    """Acquire an Oracle DB connection from a connection or session pool."""
    if isinstance(conn, Connection):
        yield conn
    elif isinstance(conn, SessionPool):
        with conn.acquire() as session_conn:
            yield session_conn
    else:
        raise TypeError(f"Invalid connection type: {type(conn)}")
