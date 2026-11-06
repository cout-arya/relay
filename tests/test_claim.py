"""Tests: concurrent claim — every job claimed exactly once.

50 jobs, 10 concurrent claimers: no job processed twice.
This is Day 1's definition of done test.
"""

from __future__ import annotations

import threading
import time

import pytest

from relay.queue import claim, enqueue


@pytest.mark.timeout(30)
def test_concurrent_claim_no_duplicate(make_conn) -> None:
    """50 jobs, 10 concurrent claimers — every job claimed exactly once."""
    num_jobs = 50
    num_workers = 10

    # Enqueue 50 jobs
    conn = make_conn()
    for i in range(num_jobs):
        enqueue(conn, type="noop", payload={"index": i})
    conn.close()

    # Concurrent claim
    claimed: list[int] = []
    lock = threading.Lock()
    errors: list[str] = []

    def worker_fn(worker_id: str) -> None:
        try:
            c = make_conn()
            empty_count = 0
            while True:
                job = claim(c, queues=["default"], worker_id=worker_id)
                if job is None:
                    empty_count += 1
                    if empty_count >= 5 or len(claimed) >= num_jobs:
                        break
                    time.sleep(0.01)
                    continue
                empty_count = 0
                with lock:
                    claimed.append(job["id"])
            c.close()
        except Exception as e:
            with lock:
                errors.append(f"{worker_id}: {e}")

    threads = []
    for i in range(num_workers):
        t = threading.Thread(target=worker_fn, args=(f"worker-{i}",))
        threads.append(t)
        t.start()

    for t in threads:
        t.join()

    assert not errors, f"Errors: {errors}"
    assert len(claimed) == num_jobs, f"Expected {num_jobs}, got {len(claimed)}"
    assert len(set(claimed)) == num_jobs, "Duplicate claims detected!"


@pytest.mark.timeout(10)
def test_claim_returns_none_when_empty(conn) -> None:
    """Claim returns None when no jobs are available."""
    job = claim(conn, queues=["default"], worker_id="w1")
    assert job is None


@pytest.mark.timeout(10)
def test_claim_sets_running_status(conn) -> None:
    """Claimed job transitions to 'running' status."""
    enqueue(conn, type="noop")
    job = claim(conn, queues=["default"], worker_id="w1")
    assert job is not None
    assert job["status"] == "running"
    assert job["locked_by"] == "w1"
    assert job["attempts"] == 1
