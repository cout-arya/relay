"""Migration runner: applies numbered .sql files from the migrations/ directory."""

from __future__ import annotations

import logging
import os
from pathlib import Path

import psycopg
from psycopg.rows import dict_row

from relay.config import settings

logger = logging.getLogger(__name__)

MIGRATIONS_DIR = Path(__file__).resolve().parent.parent.parent / "migrations"


def run_migrations(conninfo: str | None = None) -> None:
    """Apply all pending migrations in order.

    Uses a `schema_migrations` table to track which files have been applied.
    Each migration runs in its own transaction.
    """
    conninfo = conninfo or settings.database_url

    with psycopg.connect(conninfo, row_factory=dict_row, autocommit=True) as conn:
        # Ensure the tracking table exists
        conn.execute("""
            CREATE TABLE IF NOT EXISTS schema_migrations (
                filename TEXT PRIMARY KEY,
                applied_at TIMESTAMPTZ NOT NULL DEFAULT now()
            );
        """)

        # Get already-applied migrations
        rows = conn.execute("SELECT filename FROM schema_migrations ORDER BY filename;")
        applied = {row["filename"] for row in rows}

        # Find and sort migration files
        if not MIGRATIONS_DIR.exists():
            logger.warning("Migrations directory not found: %s", MIGRATIONS_DIR)
            return

        sql_files = sorted(f for f in os.listdir(MIGRATIONS_DIR) if f.endswith(".sql"))

        for filename in sql_files:
            if filename in applied:
                logger.debug("Migration already applied: %s", filename)
                continue

            filepath = MIGRATIONS_DIR / filename
            sql = filepath.read_text(encoding="utf-8")

            logger.info("Applying migration: %s", filename)
            # Run the migration in a transaction
            with conn.transaction():
                conn.execute(sql)
                conn.execute(
                    "INSERT INTO schema_migrations (filename) VALUES (%s);",
                    (filename,),
                )
            logger.info("Migration applied: %s", filename)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    run_migrations()
