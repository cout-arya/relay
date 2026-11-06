"""Relay worker process: claims and executes jobs with heartbeat-based leases.

Design:
- One job at a time per process. Scale by running more processes.
- Startup: generates worker_id (hostname-pid-random), registers in workers table, then loops.
- Loop: reap exhausted jobs, claim a job, execute with heartbeat thread, ack or fail.
- SIGTERM: graceful shutdown (finish current job, then exit).
- SIGKILL: safe by design (the lease handles recovery).
"""

from __future__ import annotations

import logging
import os
import platform
import random
import signal
import threading
import time
import uuid
from typing import Any

import relay.handlers.llm  # noqa: F401

# Import handlers to trigger registration
import relay.handlers.noop  # noqa: F401
import relay.handlers.transfer  # noqa: F401
from relay.config import settings
from relay.db import close_pool, get_conn
from relay.errors import PermanentError, RetryLater
from relay.logging_config import configure_logging
from relay.migrate import run_migrations
from relay.queue import (
    ack,
    claim,
    claim_with_concurrency_limit,
    fail,
    heartbeat,
    reap,
    record_lease_lost,
)
from relay.registry import get_handler

logger = logging.getLogger(__name__)

# Global shutdown flag
_shutdown = threading.Event()


def _generate_worker_id() -> str:
    """Generate a unique worker ID: hostname-pid-random."""
    host = platform.node()
    pid = os.getpid()
    rand = uuid.uuid4().hex[:8]
    return f"{host}-{pid}-{rand}"


def _register_worker(worker_id: str) -> None:
    """Register this worker in the workers table."""
    with get_conn() as conn:
        with conn.transaction():
            conn.execute(
                """
                INSERT INTO workers (id, pid, host, started_at, last_seen)
                VALUES (%s, %s, %s, now(), now())
                ON CONFLICT (id) DO UPDATE SET last_seen = now();
                """,
                [worker_id, os.getpid(), platform.node()],
            )
    logger.info("Worker registered: %s", worker_id)


def _update_last_seen(worker_id: str) -> None:
    """Update the worker's last_seen timestamp."""
    try:
        with get_conn() as conn:
            with conn.transaction():
                conn.execute(
                    "UPDATE workers SET last_seen = now() WHERE id = %s;",
                    [worker_id],
                )
    except Exception:
        logger.debug("Failed to update last_seen for %s", worker_id, exc_info=True)


def _heartbeat_loop(
    job_id: int,
    worker_id: str,
    token: int,
    lease_seconds: int,
    cancel_event: threading.Event,
    lease_lost_event: threading.Event,
) -> None:
    """Background thread that extends the lease every lease_s / 3 seconds.

    Sets cancel_event if the heartbeat fails (lease lost), signaling the
    handler to stop work. Also sets lease_lost_event so the main thread knows
    NOT to attempt ack/fail — it must instead record outcome = 'lease_lost'.
    """
    interval = lease_seconds / 3.0

    while not cancel_event.is_set() and not _shutdown.is_set():
        time.sleep(interval)
        if cancel_event.is_set() or _shutdown.is_set():
            break

        try:
            with get_conn() as conn:
                ok = heartbeat(
                    conn,
                    job_id=job_id,
                    worker_id=worker_id,
                    token=token,
                    lease_seconds=lease_seconds,
                )
            if not ok:
                logger.warning(
                    "Heartbeat failed (lease lost): job_id=%s worker=%s",
                    job_id, worker_id,
                )
                # Record lease_lost in job_attempts before signalling cancellation.
                # PRD §8.3: "records outcome lease_lost. It must not ack or fail."
                try:
                    with get_conn() as conn:
                        record_lease_lost(
                            conn, job_id=job_id, worker_id=worker_id, token=token
                        )
                except Exception:
                    logger.debug(
                        "Could not record lease_lost: job_id=%s", job_id, exc_info=True
                    )
                lease_lost_event.set()
                cancel_event.set()  # Signal the handler to stop
                return
        except Exception:
            logger.error(
                "Heartbeat error: job_id=%s", job_id, exc_info=True
            )
            cancel_event.set()
            return


def _execute_job(job: dict[str, Any], worker_id: str) -> None:
    """Execute a single job: look up handler, run with heartbeat thread, ack or fail."""
    job_id = job["id"]
    job_type = job["type"]
    token = job["attempts"]  # fencing token
    lease_s = settings.lease_seconds
    payload = job.get("payload", {})

    logger.info(
        "Executing job: id=%s type=%s attempt=%s",
        job_id, job_type, token,
    )

    handler = get_handler(job_type)
    if handler is None:
        # Unknown job type is a permanent failure
        logger.error("Unknown job type: %s (job_id=%s)", job_type, job_id)
        with get_conn() as conn:
            fail(
                conn,
                job_id=job_id,
                worker_id=worker_id,
                token=token,
                error=f"Unknown job type: {job_type}",
                permanent=True,
            )
        return

    # Two events:
    # - cancel_event: signals handler to stop (set by heartbeat loss or us on success)
    # - lease_lost_event: set ONLY when heartbeat detected lease loss from another worker
    cancel_event = threading.Event()
    lease_lost_event = threading.Event()

    hb_thread = threading.Thread(
        target=_heartbeat_loop,
        args=(job_id, worker_id, token, lease_s, cancel_event, lease_lost_event),
        daemon=True,
        name=f"heartbeat-{job_id}",
    )
    hb_thread.start()

    try:
        # Build context for the handler
        ctx = {
            "job_id": job_id,
            "worker_id": worker_id,
            "token": token,
            "attempt": token,
            "cancel_event": cancel_event,
        }

        # Run the handler
        handler(job_id, payload, ctx)

        # Stop heartbeating
        cancel_event.set()
        hb_thread.join(timeout=5)

        # PRD §8.3: if lease was lost during handler execution, do NOT ack/fail.
        # The lease_lost_event is set only by the heartbeat thread when it detects
        # the fencing token is no longer valid — the job was reclaimed by another worker.
        if lease_lost_event.is_set():
            logger.warning(
                "Lease lost during handler: job_id=%s type=%s — skipping ack",
                job_id, job_type,
            )
            return

        # Ack the job (fenced — if lease was lost, this affects zero rows)
        with get_conn() as conn:
            acked = ack(conn, job_id=job_id, worker_id=worker_id, token=token)

        if acked:
            logger.info("Job succeeded: id=%s type=%s", job_id, job_type)
        else:
            # Ack was fenced out: another worker reclaimed the job. Record it.
            logger.warning(
                "Ack fenced out (lease lost): id=%s type=%s", job_id, job_type
            )
            try:
                with get_conn() as conn:
                    record_lease_lost(
                        conn, job_id=job_id, worker_id=worker_id, token=token
                    )
            except Exception:
                logger.debug(
                    "Could not record fenced ack lease_lost: job_id=%s",
                    job_id, exc_info=True,
                )

    except PermanentError as e:
        cancel_event.set()
        hb_thread.join(timeout=5)
        if lease_lost_event.is_set():
            logger.warning(
                "Lease lost before PermanentError recorded: job_id=%s", job_id
            )
            return
        logger.warning("Permanent error: job_id=%s: %s", job_id, e)
        with get_conn() as conn:
            fail(
                conn,
                job_id=job_id,
                worker_id=worker_id,
                token=token,
                error=str(e),
                permanent=True,
            )

    except RetryLater as e:
        cancel_event.set()
        hb_thread.join(timeout=5)
        if lease_lost_event.is_set():
            logger.warning(
                "Lease lost before RetryLater recorded: job_id=%s", job_id
            )
            return
        logger.info("RetryLater: job_id=%s delay=%ss: %s", job_id, e.seconds, e)
        with get_conn() as conn:
            fail(
                conn,
                job_id=job_id,
                worker_id=worker_id,
                token=token,
                error=str(e),
                delay_seconds=e.seconds,
            )

    except Exception as e:
        cancel_event.set()
        hb_thread.join(timeout=5)
        if lease_lost_event.is_set():
            logger.warning(
                "Lease lost before error recorded: job_id=%s", job_id
            )
            return
        logger.error("Job failed: id=%s error=%s", job_id, e, exc_info=True)
        with get_conn() as conn:
            fail(
                conn,
                job_id=job_id,
                worker_id=worker_id,
                token=token,
                error=str(e),
            )


def _has_concurrency_limits() -> bool:
    """Check if any queue has concurrency limits configured."""
    try:
        with get_conn() as conn:
            row = conn.execute(
                "SELECT count(*) as c FROM queue_config WHERE max_concurrency IS NOT NULL;"
            ).fetchone()
            return row is not None and row["c"] > 0
    except Exception:
        return False


def run_worker() -> None:
    """Main worker entry point."""
    # Configure logging (supports LOG_FORMAT=json for structured output)
    configure_logging(level=settings.log_level, log_format=settings.log_format)

    # Run migrations
    logger.info("Running migrations...")
    run_migrations()

    worker_id = _generate_worker_id()
    queues = settings.queue_list
    poll_interval = settings.poll_interval

    logger.info(
        "Starting worker: id=%s queues=%s poll_interval=%s",
        worker_id, queues, poll_interval,
    )

    # Register worker
    _register_worker(worker_id)

    # Set up signal handlers for graceful shutdown
    def handle_sigterm(signum: int, frame: Any) -> None:
        logger.info("Received SIGTERM, shutting down gracefully...")
        _shutdown.set()

    signal.signal(signal.SIGTERM, handle_sigterm)
    # On Windows, SIGINT (Ctrl+C) is the equivalent
    signal.signal(signal.SIGINT, handle_sigterm)

    reap_counter = 0
    use_concurrency_limits = _has_concurrency_limits()

    while not _shutdown.is_set():
        try:
            # Run reaper occasionally (every ~10 iterations)
            reap_counter += 1
            if reap_counter >= 10:
                reap_counter = 0
                with get_conn() as conn:
                    reap(conn)

            # Update last_seen
            _update_last_seen(worker_id)

            # Claim a job
            with get_conn() as conn:
                if use_concurrency_limits:
                    job = claim_with_concurrency_limit(
                        conn, queues=queues, worker_id=worker_id
                    )
                else:
                    job = claim(conn, queues=queues, worker_id=worker_id)

            if job is None:
                # No work available — sleep with jitter to avoid thundering herd
                jitter = random.uniform(0, poll_interval * 0.5)
                _shutdown.wait(poll_interval + jitter)
                continue

            # Execute the job
            _execute_job(job, worker_id)

        except Exception:
            logger.error("Worker loop error", exc_info=True)
            _shutdown.wait(poll_interval)

    logger.info("Worker %s shutting down", worker_id)
    close_pool()


if __name__ == "__main__":
    run_worker()
