"""Tests: per-queue concurrency limits.

Verifies that with max_concurrency set, never more than N jobs run
concurrently in that queue while other queues are unaffected.
"""

from __future__ import annotations

import threading
import time

import pytest

from relay.queue import (
    ack,
    claim_with_concurrency_limit,
    enqueue,
    upsert_queue_config,
)


@pytest.mark.timeout(15)
def test_concurrency_limit_respected(make_conn) -> None:
    """With limit 2, never more than 2 running at once in the limited queue."""
    conn = make_conn()

    # Set concurrency limit = 2 for 'limited' queue
    upsert_queue_config(conn, queue="limited", max_concurrency=2, default_lease_s=30)

    # Enqueue 6 jobs in the limited queue
    for i in range(6):
        enqueue(conn, type="sleep", payload={"seconds": 0.5}, queue="limited")
    conn.close()

    max_running = 0
    current_running = 0
    lock = threading.Lock()

    def worker_fn(worker_id: str) -> None:
        nonlocal max_running, current_running
        c = make_conn()
        while True:
            job = claim_with_concurrency_limit(
                c, queues=["limited"], worker_id=worker_id
            )
            if job is None:
                break

            with lock:
                current_running += 1
                max_running = max(max_running, current_running)

            time.sleep(0.2)  # Simulate work

            with lock:
                current_running -= 1

            ack(c, job_id=job["id"], worker_id=worker_id, token=job["attempts"])
        c.close()

    threads = []
    for i in range(5):  # More workers than the limit
        t = threading.Thread(target=worker_fn, args=(f"w-{i}",))
        threads.append(t)
        t.start()

    for t in threads:
        t.join()

    assert max_running <= 2, f"Max running was {max_running}, expected <= 2"


@pytest.mark.timeout(10)
def test_unlimited_queue_not_affected(make_conn) -> None:
    """An unlimited queue is not affected by limits on other queues."""
    conn = make_conn()

    # Limited queue
    upsert_queue_config(conn, queue="limited", max_concurrency=1, default_lease_s=30)

    # Enqueue in unlimited queue
    for i in range(3):
        enqueue(conn, type="noop", queue="unlimited")

    # All 3 should be claimable
    jobs = []
    for i in range(3):
        job = claim_with_concurrency_limit(
            conn, queues=["unlimited"], worker_id=f"w{i}"
        )
        if job:
            jobs.append(job)

    assert len(jobs) == 3, "Unlimited queue should allow all claims"
    conn.close()
