"""Shared test fixtures — uses a real PostgreSQL database.

No mocking the database. Tests expect a PostgreSQL instance reachable
at DATABASE_URL (defaults to postgresql://relay:relay@localhost:5432/relay).
"""

from __future__ import annotations

import psycopg
import pytest
from psycopg.rows import dict_row

from relay.config import settings
from relay.migrate import run_migrations


@pytest.fixture(scope="session", autouse=True)
def _run_migrations() -> None:
    """Run migrations once at test session start."""
    run_migrations()


@pytest.fixture(autouse=True)
def _clean_tables() -> None:
    """Clean all job-related tables before each test."""
    with psycopg.connect(settings.database_url, autocommit=True) as conn:
        conn.execute("DELETE FROM effects;")
        conn.execute("DELETE FROM llm_usage;")
        conn.execute("DELETE FROM job_attempts;")
        conn.execute("DELETE FROM job_dependencies;")
        conn.execute("DELETE FROM jobs;")
        conn.execute("DELETE FROM workers;")
        conn.execute("DELETE FROM queue_config;")


@pytest.fixture
def conn():
    """Provide a database connection with dict rows."""
    with psycopg.connect(
        settings.database_url, row_factory=dict_row, autocommit=False
    ) as connection:
        yield connection


@pytest.fixture
def make_conn():
    """Factory fixture: creates fresh connections (for concurrent tests)."""
    connections: list[psycopg.Connection] = []

    def _factory():
        c = psycopg.connect(
            settings.database_url, row_factory=dict_row, autocommit=False
        )
        connections.append(c)
        return c

    yield _factory

    for c in connections:
        try:
            c.close()
        except Exception:
            pass
