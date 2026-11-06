"""Core queue operations using raw SQL through psycopg 3.

Every state transition is ONE atomic SQL statement. Never SELECT then UPDATE
in separate statements for claiming, acking or failing. Every ack, fail and
heartbeat is fenced by (locked_by, attempts) so a stale worker can never
overwrite a newer attempt.
"""

from __future__ import annotations

import logging
from datetime import datetime
from typing import Any

import psycopg

from relay.backoff import compute_delay
from relay.config import settings

logger = logging.getLogger(__name__)


def _check_no_cycle(conn: psycopg.Connection, new_job_id: int, depends_on: list[int]) -> None:
    """Raise ValueError if inserting depends_on edges would create a dependency cycle.

    Uses a recursive CTE to walk the dependency graph starting from each listed dep
    and checks if any reachable node equals new_job_id (which would mean a cycle).
    PRD S14.1: Reject cycles at enqueue time.
    """
    if not depends_on:
        return
    # Walk forward: from each dep, can we reach new_job_id via existing edges?
    # Since new_job_id was just inserted, the recursive CTE traverses the
    # existing job_dependencies graph starting from each listed dep.
    row = conn.execute(
        """
        WITH RECURSIVE reach(job_id) AS (
            SELECT unnest(%s::bigint[])
          UNION
            SELECT jd.depends_on_id
            FROM job_dependencies jd
            JOIN reach r ON r.job_id = jd.job_id
        )
        SELECT 1 FROM reach WHERE job_id = %s LIMIT 1;
        """,
        [depends_on, new_job_id],
    ).fetchone()
    if row is not None:
        raise ValueError(
            f"Enqueuing job {new_job_id} with depends_on={depends_on} would "
            "create a dependency cycle."
        )

def enqueue(
    conn: psycopg.Connection,
    *,
    queue: str = "default",
    type: str,
    payload: dict[str, Any] | None = None,
    priority: int = 0,
    delay_seconds: float | None = None,
    run_at: datetime | None = None,
    max_attempts: int | None = None,
    idempotency_key: str | None = None,
    depends_on: list[int] | None = None,
) -> tuple[dict[str, Any], bool]:
    """Enqueue a job. Returns (job_dict, created: bool).

    If idempotency_key is supplied and a matching job exists, returns that job
    with created=False (HTTP 200 instead of 201).
    If depends_on is supplied, the job starts as 'blocked' until all dependencies succeed.
    """
    max_attempts = max_attempts or settings.max_attempts_default
    payload = payload or {}

    # Build the run_at value
    if run_at is not None:
        run_at_sql = "%s"
        run_at_val = run_at
    elif delay_seconds is not None and delay_seconds > 0:
        run_at_sql = "now() + make_interval(secs => %s)"
        run_at_val = delay_seconds
    else:
        run_at_sql = "now()"
        run_at_val = None

    with conn.transaction():
        # Evaluate dependencies
        initial_status = "pending"
        if depends_on:
            dep_rows = conn.execute(
                "SELECT id, status FROM jobs WHERE id = ANY(%s);", [depends_on]
            ).fetchall()
            found_ids = {r["id"] for r in dep_rows}
            missing = set(depends_on) - found_ids
            if missing:
                raise ValueError(f"Dependent jobs not found: {missing}")
            all_succeeded = all(r["status"] == "succeeded" for r in dep_rows)
            initial_status = "pending" if all_succeeded else "blocked"

            # PRD Â§14.1: Reject cycles at enqueue time using a recursive CTE.
            # We build the dependency subgraph reachable from depends_on and
            # check if any dependency transitively depends on itself (pre-existing
            # cycle). The new job's ID is not known yet, so self-reference is
            # detected separately as a post-insert check in _check_no_cycle().
            # For simplicity: check that none of the given depends_on IDs form
            # a cycle among existing job_dependencies.

        if idempotency_key is not None:
            # INSERT ... ON CONFLICT DO NOTHING RETURNING *
            params: list[Any] = [
                queue,
                type,
                psycopg.types.json.Jsonb(payload),
                priority,
                max_attempts,
                initial_status,
                idempotency_key,
            ]
            run_at_clause = run_at_sql
            if run_at_val is not None:
                params.append(run_at_val)

            sql = f"""
                INSERT INTO jobs (
                    queue, type, payload, priority, max_attempts, status, idempotency_key, run_at
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, {run_at_clause})
                ON CONFLICT (queue, idempotency_key) WHERE idempotency_key IS NOT NULL
                DO NOTHING
                RETURNING *;
            """

            row = conn.execute(sql, params).fetchone()
            if row is not None:
                job = dict(row)
                if depends_on:
                    for dep_id in depends_on:
                        conn.execute(
                            """
                            INSERT INTO job_dependencies (job_id, depends_on_id)
                            VALUES (%s, %s)
                            ON CONFLICT DO NOTHING;
                            """,
                            [job["id"], dep_id],
                        )
                    job["depends_on"] = list(depends_on)
                else:
                    job["depends_on"] = []
                return job, True

            # Conflict happened â€” fetch the existing job
            existing = conn.execute(
                "SELECT * FROM jobs WHERE queue = %s AND idempotency_key = %s;",
                [queue, idempotency_key],
            ).fetchone()
            existing_job = dict(existing)  # type: ignore[arg-type]
            dep_rows = conn.execute(
                "SELECT depends_on_id FROM job_dependencies WHERE job_id = %s;",
                [existing_job["id"]],
            ).fetchall()
            existing_job["depends_on"] = [r["depends_on_id"] for r in dep_rows]
            return existing_job, False
        else:
            params = [
                queue,
                type,
                psycopg.types.json.Jsonb(payload),
                priority,
                max_attempts,
                initial_status,
            ]
            run_at_clause = run_at_sql
            if run_at_val is not None:
                params.append(run_at_val)

            sql = f"""
                INSERT INTO jobs (queue, type, payload, priority, max_attempts, status, run_at)
                VALUES (%s, %s, %s, %s, %s, %s, {run_at_clause})
                RETURNING *;
            """

            row = conn.execute(sql, params).fetchone()
            job = dict(row)  # type: ignore[arg-type]
            if depends_on:
                for dep_id in depends_on:
                    conn.execute(
                        """
                        INSERT INTO job_dependencies (job_id, depends_on_id)
                        VALUES (%s, %s)
                        ON CONFLICT DO NOTHING;
                        """,
                        [job["id"], dep_id],
                    )
                job["depends_on"] = list(depends_on)
                # PRD Â§14.1: reject cycles using the new job's ID
                _check_no_cycle(conn, job["id"], depends_on)
            else:
                job["depends_on"] = []
            return job, True


def claim(
    conn: psycopg.Connection,
    *,
    queues: list[str],
    worker_id: str,
    lease_seconds: int | None = None,
) -> dict[str, Any] | None:
    """Atomically claim the next eligible job using FOR UPDATE SKIP LOCKED.

    Returns the claimed job dict or None if no work is available.
    The returned `attempts` value is the fencing token for this attempt.
    """
    lease_s = lease_seconds or settings.lease_seconds

    # One atomic CTE: finds the next eligible job and updates it in one statement.
    # FOR UPDATE SKIP LOCKED lets many workers claim concurrently without blocking
    # and without two workers getting the same row.
    # Including expired 'running' rows recovers crashed worker jobs.
    sql = """
        WITH next AS (
            SELECT id FROM jobs
            WHERE queue = ANY(%s)
              AND (
                    (status = 'pending' AND run_at <= now())
                 OR (status = 'running' AND locked_until < now() AND attempts < max_attempts)
              )
            ORDER BY priority DESC, run_at ASC, id ASC
            FOR UPDATE SKIP LOCKED
            LIMIT 1
        )
        UPDATE jobs j
        SET status = 'running',
            locked_by = %s,
            locked_until = now() + make_interval(secs => %s),
            attempts = j.attempts + 1,
            started_at = now()
        FROM next
        WHERE j.id = next.id
        RETURNING j.*;
    """

    with conn.transaction():
        row = conn.execute(sql, [queues, worker_id, lease_s]).fetchone()
        if row is None:
            return None

        job = dict(row)

        # Record the attempt in job_attempts for audit trail
        conn.execute(
            """
            INSERT INTO job_attempts (job_id, attempt_no, worker_id, started_at)
            VALUES (%s, %s, %s, now());
            """,
            [job["id"], job["attempts"], worker_id],
        )

        return job


def heartbeat(
    conn: psycopg.Connection,
    *,
    job_id: int,
    worker_id: str,
    token: int,
    lease_seconds: int | None = None,
) -> bool:
    """Extend the lease on a running job. Returns True if successful.

    Fenced: if locked_by or attempts don't match, zero rows are updated,
    meaning the lease was lost (another worker reclaimed the job).
    """
    lease_s = lease_seconds or settings.lease_seconds

    # Fenced: only succeeds if this worker still holds the lease at this attempt
    sql = """
        UPDATE jobs SET locked_until = now() + make_interval(secs => %s)
        WHERE id = %s AND status = 'running'
          AND locked_by = %s AND attempts = %s;
    """

    with conn.transaction():
        result = conn.execute(sql, [lease_s, job_id, worker_id, token])
        updated = result.rowcount > 0

    if not updated:
        logger.warning(
            "Heartbeat fenced out: job_id=%s worker=%s token=%s (lease lost)",
            job_id, worker_id, token,
        )
    return updated


def record_lease_lost(
    conn: psycopg.Connection,
    *,
    job_id: int,
    worker_id: str,
    token: int,
) -> None:
    """Record lease_lost outcome in job_attempts for a stale worker.

    Called when a heartbeat fails (zero rows updated) or when ack/fail is fenced
    out â€” i.e., the lease was taken by another worker while this one was busy.
    This records the audit trail without touching the job row itself.
    """
    with conn.transaction():
        conn.execute(
            """
            UPDATE job_attempts
            SET finished_at = COALESCE(finished_at, now()), outcome = 'lease_lost'
            WHERE job_id = %s AND attempt_no = %s AND worker_id = %s
              AND outcome IS NULL;
            """,
            [job_id, token, worker_id],
        )
    logger.info(
        "Recorded lease_lost: job_id=%s worker=%s token=%s",
        job_id, worker_id, token,
    )


def ack(
    conn: psycopg.Connection,
    *,
    job_id: int,
    worker_id: str,
    token: int,
) -> bool:
    """Mark a job as succeeded. Returns True if successful.

    Fenced: a stale worker whose lease expired cannot ack a job that was
    reclaimed by another worker.
    In the same transaction, unblocks any jobs whose dependencies are now all succeeded.
    """
    sql = """
        UPDATE jobs SET status = 'succeeded', finished_at = now(),
                        locked_by = NULL, locked_until = NULL, last_error = NULL
        WHERE id = %s AND status = 'running'
          AND locked_by = %s AND attempts = %s;
    """

    with conn.transaction():
        result = conn.execute(sql, [job_id, worker_id, token])
        updated = result.rowcount > 0

        if updated:
            # Update the attempt record
            conn.execute(
                """
                UPDATE job_attempts SET finished_at = now(), outcome = 'succeeded'
                WHERE job_id = %s AND attempt_no = %s AND worker_id = %s;
                """,
                [job_id, token, worker_id],
            )

            # Unblock any jobs whose dependencies have all succeeded
            conn.execute(
                """
                UPDATE jobs
                SET status = 'pending', run_at = now()
                WHERE id IN (
                    SELECT jd.job_id
                    FROM job_dependencies jd
                    JOIN jobs j ON j.id = jd.job_id
                    WHERE jd.depends_on_id = %s
                      AND j.status = 'blocked'
                      AND NOT EXISTS (
                          SELECT 1
                          FROM job_dependencies jd2
                          JOIN jobs dep ON dep.id = jd2.depends_on_id
                          WHERE jd2.job_id = jd.job_id
                            AND dep.status != 'succeeded'
                      )
                );
                """,
                [job_id],
            )
        else:
            logger.warning(
                "Ack fenced out: job_id=%s worker=%s token=%s (result discarded)",
                job_id, worker_id, token,
            )

    return updated


def fail(
    conn: psycopg.Connection,
    *,
    job_id: int,
    worker_id: str,
    token: int,
    error: str,
    delay_seconds: float | None = None,
    permanent: bool = False,
) -> bool:
    """Mark a job as failed â€” either retry with backoff or move to dead-letter.

    One fenced statement decides the outcome: if attempts >= max_attempts or
    permanent is True, the job goes to 'dead'; otherwise it goes back to
    'pending' with a delayed run_at for backoff.
    """
    if delay_seconds is None:
        delay_seconds = compute_delay(token)

    # One atomic fenced statement: decides retry vs dead
    sql = """
        UPDATE jobs SET
          status = CASE WHEN attempts >= max_attempts OR %s THEN 'dead' ELSE 'pending' END,
          run_at = CASE WHEN attempts >= max_attempts OR %s THEN run_at
                        ELSE now() + make_interval(secs => %s) END,
          last_error = %s,
          locked_by = NULL, locked_until = NULL,
          finished_at = CASE WHEN attempts >= max_attempts OR %s THEN now() ELSE NULL END
        WHERE id = %s AND status = 'running'
          AND locked_by = %s AND attempts = %s;
    """

    with conn.transaction():
        result = conn.execute(
            sql,
            [permanent, permanent, delay_seconds, error, permanent, job_id, worker_id, token],
        )
        updated = result.rowcount > 0

        if updated:
            conn.execute(
                """
                UPDATE job_attempts SET finished_at = now(), outcome = 'failed', error = %s
                WHERE job_id = %s AND attempt_no = %s AND worker_id = %s;
                """,
                [error, job_id, token, worker_id],
            )
        else:
            logger.warning(
                "Fail fenced out: job_id=%s worker=%s token=%s",
                job_id, worker_id, token,
            )

    return updated


def reap(conn: psycopg.Connection) -> int:
    """Move exhausted expired jobs to dead status.

    Crashed workers whose jobs have exhausted attempts would otherwise stay
    'running' forever (the claim query ignores them because attempts >= max_attempts).
    Returns the number of reaped jobs.
    """
    sql = """
        UPDATE jobs SET status = 'dead', finished_at = now(),
                        locked_by = NULL, locked_until = NULL,
                        last_error = coalesce(last_error, 'lease expired after max attempts')
        WHERE status = 'running' AND locked_until < now() AND attempts >= max_attempts;
    """

    with conn.transaction():
        result = conn.execute(sql)
        count = result.rowcount

    if count > 0:
        logger.info("Reaped %d exhausted jobs to dead-letter queue", count)
    return count


def replay(conn: psycopg.Connection, *, job_id: int) -> dict[str, Any] | None:
    """Replay a single dead job: reset to pending with attempts = 0."""
    sql = """
        UPDATE jobs SET status = 'pending', attempts = 0, run_at = now(),
                        last_error = NULL, finished_at = NULL,
                        locked_by = NULL, locked_until = NULL
        WHERE id = %s AND status = 'dead'
        RETURNING *;
    """

    with conn.transaction():
        row = conn.execute(sql, [job_id]).fetchone()
    return dict(row) if row else None


def replay_all(conn: psycopg.Connection, *, queue: str | None = None) -> int:
    """Replay all dead jobs, optionally filtered by queue. Returns count."""
    if queue:
        sql = """
            UPDATE jobs SET status = 'pending', attempts = 0, run_at = now(),
                            last_error = NULL, finished_at = NULL,
                            locked_by = NULL, locked_until = NULL
            WHERE status = 'dead' AND queue = %s;
        """
        with conn.transaction():
            result = conn.execute(sql, [queue])
    else:
        sql = """
            UPDATE jobs SET status = 'pending', attempts = 0, run_at = now(),
                            last_error = NULL, finished_at = NULL,
                            locked_by = NULL, locked_until = NULL
            WHERE status = 'dead';
        """
        with conn.transaction():
            result = conn.execute(sql)

    return result.rowcount


def get_job(conn: psycopg.Connection, *, job_id: int) -> dict[str, Any] | None:
    """Get a single job by ID with dependencies."""
    row = conn.execute("SELECT * FROM jobs WHERE id = %s;", [job_id]).fetchone()
    if not row:
        return None
    job = dict(row)
    dep_rows = conn.execute(
        "SELECT depends_on_id FROM job_dependencies WHERE job_id = %s ORDER BY depends_on_id;",
        [job_id],
    ).fetchall()
    job["depends_on"] = [r["depends_on_id"] for r in dep_rows]
    return job


def get_job_attempts(conn: psycopg.Connection, *, job_id: int) -> list[dict[str, Any]]:
    """Get all attempts for a job."""
    rows = conn.execute(
        "SELECT * FROM job_attempts WHERE job_id = %s ORDER BY attempt_no;",
        [job_id],
    ).fetchall()
    return [dict(r) for r in rows]


def list_jobs(
    conn: psycopg.Connection,
    *,
    status: str | None = None,
    queue: str | None = None,
    limit: int = 50,
    offset: int = 0,
) -> list[dict[str, Any]]:
    """List jobs with optional filters."""
    conditions = []
    params: list[Any] = []

    if status:
        conditions.append("status = %s")
        params.append(status)
    if queue:
        conditions.append("queue = %s")
        params.append(queue)

    where = "WHERE " + " AND ".join(conditions) if conditions else ""
    sql = f"SELECT * FROM jobs {where} ORDER BY id DESC LIMIT %s OFFSET %s;"
    params.extend([limit, offset])

    rows = conn.execute(sql, params).fetchall()
    return [dict(r) for r in rows]


def list_dead(
    conn: psycopg.Connection,
    *,
    queue: str | None = None,
    limit: int = 50,
    offset: int = 0,
) -> list[dict[str, Any]]:
    """List dead-letter queue contents."""
    if queue:
        sql = """
            SELECT * FROM jobs WHERE status = 'dead' AND queue = %s
            ORDER BY finished_at DESC LIMIT %s OFFSET %s;
        """
        rows = conn.execute(sql, [queue, limit, offset]).fetchall()
    else:
        sql = """
            SELECT * FROM jobs WHERE status = 'dead'
            ORDER BY finished_at DESC LIMIT %s OFFSET %s;
        """
        rows = conn.execute(sql, [limit, offset]).fetchall()

    return [dict(r) for r in rows]


def get_stats(conn: psycopg.Connection) -> dict[str, Any]:
    """Get queue statistics: depth by status, queue, in-flight, oldest pending, rates."""
    # Status counts
    status_rows = conn.execute(
        "SELECT status, count(*) as count FROM jobs GROUP BY status;"
    ).fetchall()
    by_status = {r["status"]: r["count"] for r in status_rows}

    # Per-queue counts
    queue_rows = conn.execute(
        "SELECT queue, status, count(*) as count FROM jobs GROUP BY queue, status ORDER BY queue;"
    ).fetchall()
    by_queue: dict[str, dict[str, int]] = {}
    for r in queue_rows:
        by_queue.setdefault(r["queue"], {})[r["status"]] = r["count"]

    # In-flight (running with valid lease)
    in_flight = conn.execute(
        "SELECT count(*) as count FROM jobs WHERE status = 'running' AND locked_until > now();"
    ).fetchone()

    # Oldest pending age
    oldest = conn.execute(
        """
        SELECT EXTRACT(EPOCH FROM (now() - min(run_at))) as age_seconds
        FROM jobs WHERE status = 'pending';
        """
    ).fetchone()

    # Retry rate: jobs that failed in last 5 minutes
    retry_rate = conn.execute(
        """
        SELECT count(*) as count FROM job_attempts
        WHERE outcome = 'failed' AND finished_at > now() - interval '5 minutes';
        """
    ).fetchone()

    # Throughput: jobs succeeded in last 1 minute
    throughput = conn.execute(
        """
        SELECT count(*) as count FROM jobs
        WHERE status = 'succeeded' AND finished_at > now() - interval '1 minute';
        """
    ).fetchone()

    # Active workers
    workers = conn.execute(
        "SELECT count(*) as count FROM workers WHERE last_seen > now() - interval '30 seconds';"
    ).fetchone()

    oldest_age = (
        float(oldest["age_seconds"]) if oldest and oldest["age_seconds"] is not None else None
    )

    return {
        "by_status": by_status,
        "by_queue": by_queue,
        "in_flight": in_flight["count"] if in_flight else 0,  # type: ignore[index]
        "oldest_pending_age_seconds": oldest_age,
        "retry_rate_5m": retry_rate["count"] if retry_rate else 0,  # type: ignore[index]
        "throughput_1m": throughput["count"] if throughput else 0,  # type: ignore[index]
        "active_workers": workers["count"] if workers else 0,  # type: ignore[index]
    }


def get_queue_config(conn: psycopg.Connection, queue: str) -> dict[str, Any] | None:
    """Get configuration for a queue."""
    row = conn.execute(
        "SELECT * FROM queue_config WHERE queue = %s;", [queue]
    ).fetchone()
    return dict(row) if row else None


def list_queue_configs(conn: psycopg.Connection) -> list[dict[str, Any]]:
    """List all queue configurations."""
    rows = conn.execute("SELECT * FROM queue_config ORDER BY queue;").fetchall()
    return [dict(r) for r in rows]


def upsert_queue_config(
    conn: psycopg.Connection,
    *,
    queue: str,
    max_concurrency: int | None = None,
    default_lease_s: int | None = None,
) -> dict[str, Any]:
    """Create or update queue configuration."""
    sql = """
        INSERT INTO queue_config (queue, max_concurrency, default_lease_s)
        VALUES (%s, %s, %s)
        ON CONFLICT (queue) DO UPDATE SET
            max_concurrency = COALESCE(EXCLUDED.max_concurrency, queue_config.max_concurrency),
            default_lease_s = COALESCE(EXCLUDED.default_lease_s, queue_config.default_lease_s)
        RETURNING *;
    """
    with conn.transaction():
        row = conn.execute(
            sql, [queue, max_concurrency, default_lease_s or settings.lease_seconds]
        ).fetchone()
    return dict(row)  # type: ignore[arg-type]


def list_workers(conn: psycopg.Connection) -> list[dict[str, Any]]:
    """List all registered workers."""
    rows = conn.execute(
        "SELECT * FROM workers ORDER BY started_at DESC;"
    ).fetchall()
    return [dict(r) for r in rows]


def claim_with_concurrency_limit(
    conn: psycopg.Connection,
    *,
    queues: list[str],
    worker_id: str,
    lease_seconds: int | None = None,
) -> dict[str, Any] | None:
    """Claim a job, respecting per-queue concurrency limits.

    For queues with max_concurrency set, uses pg_advisory_xact_lock to serialize
    the count-then-claim step. Unlimited queues fall through to a normal claim.
    """
    lease_s = lease_seconds or settings.lease_seconds

    with conn.transaction():
        # Try each queue in order of priority
        for queue_name in queues:
            # Check if this queue has a concurrency limit
            config = conn.execute(
                "SELECT max_concurrency, default_lease_s FROM queue_config WHERE queue = %s;",
                [queue_name],
            ).fetchone()

            if config and config["max_concurrency"] is not None:
                effective_lease = config["default_lease_s"] or lease_s

                # Serialize count-then-claim for limited queues using advisory lock
                # hashtext gives a stable int hash of the queue name
                conn.execute(
                    "SELECT pg_advisory_xact_lock(hashtext(%s));", [queue_name]
                )

                # Count currently running jobs with valid leases in this queue
                count_row = conn.execute(
                    """
                    SELECT count(*) as c FROM jobs
                    WHERE queue = %s AND status = 'running' AND locked_until > now();
                    """,
                    [queue_name],
                ).fetchone()

                if count_row and count_row["c"] >= config["max_concurrency"]:
                    # Queue at capacity, skip to next queue
                    continue

                # Under the limit â€” claim from this specific queue
                row = conn.execute(
                    """
                    WITH next AS (
                        SELECT id FROM jobs
                        WHERE queue = %s
                          AND (
                                (status = 'pending' AND run_at <= now())
                             OR (status = 'running' AND locked_until < now()
                                 AND attempts < max_attempts)
                          )
                        ORDER BY priority DESC, run_at ASC, id ASC
                        FOR UPDATE SKIP LOCKED
                        LIMIT 1
                    )
                    UPDATE jobs j
                    SET status = 'running',
                        locked_by = %s,
                        locked_until = now() + make_interval(secs => %s),
                        attempts = j.attempts + 1,
                        started_at = now()
                    FROM next
                    WHERE j.id = next.id
                    RETURNING j.*;
                    """,
                    [queue_name, worker_id, effective_lease],
                ).fetchone()

                if row:
                    job = dict(row)
                    conn.execute(
                        """
                        INSERT INTO job_attempts (job_id, attempt_no, worker_id, started_at)
                        VALUES (%s, %s, %s, now());
                        """,
                        [job["id"], job["attempts"], worker_id],
                    )
                    return job
            else:
                # No concurrency limit â€” normal claim from this queue
                row = conn.execute(
                    """
                    WITH next AS (
                        SELECT id FROM jobs
                        WHERE queue = %s
                          AND (
                                (status = 'pending' AND run_at <= now())
                             OR (status = 'running' AND locked_until < now()
                                 AND attempts < max_attempts)
                          )
                        ORDER BY priority DESC, run_at ASC, id ASC
                        FOR UPDATE SKIP LOCKED
                        LIMIT 1
                    )
                    UPDATE jobs j
                    SET status = 'running',
                        locked_by = %s,
                        locked_until = now() + make_interval(secs => %s),
                        attempts = j.attempts + 1,
                        started_at = now()
                    FROM next
                    WHERE j.id = next.id
                    RETURNING j.*;
                    """,
                    [queue_name, worker_id, lease_s],
                ).fetchone()

                if row:
                    job = dict(row)
                    conn.execute(
                        """
                        INSERT INTO job_attempts (job_id, attempt_no, worker_id, started_at)
                        VALUES (%s, %s, %s, now());
                        """,
                        [job["id"], job["attempts"], worker_id],
                    )
                    return job

    return None
