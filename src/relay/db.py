"""Database connection pool using psycopg 3."""

from __future__ import annotations

import logging
from collections.abc import Generator
from contextlib import contextmanager

import psycopg
from psycopg.rows import dict_row
from psycopg_pool import ConnectionPool

from relay.config import settings

logger = logging.getLogger(__name__)

_pool: ConnectionPool | None = None


def get_pool() -> ConnectionPool:
    """Return the global connection pool, creating it on first call."""
    global _pool
    if _pool is None:
        _pool = ConnectionPool(
            conninfo=settings.database_url,
            min_size=2,
            max_size=10,
            open=True,
            kwargs={"row_factory": dict_row, "autocommit": False},
        )
        _pool.wait()
        logger.info("Database connection pool created")
    return _pool


@contextmanager
def get_conn() -> Generator[psycopg.Connection, None, None]:
    """Get a connection from the pool as a context manager."""
    pool = get_pool()
    with pool.connection() as conn:
        yield conn


def close_pool() -> None:
    """Close the global connection pool."""
    global _pool
    if _pool is not None:
        _pool.close()
        _pool = None
        logger.info("Database connection pool closed")
