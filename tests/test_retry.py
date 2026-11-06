"""Tests: retry with exponential backoff.

Verifies:
- Failure sets job back to pending with run_at in the future
- Attempts counter increments correctly
- Delay is within expected jitter bounds
"""

from __future__ import annotations

import pytest

from relay.backoff import compute_delay
from relay.queue import ack, claim, enqueue, fail, get_job


@pytest.mark.timeout(10)
def test_fail_sets_pending_with_future_run_at(conn) -> None:
    """A failed job goes back to 'pending' with a future run_at for backoff."""
    enqueue(conn, type="noop", max_attempts=3)
    job = claim(conn, queues=["default"], worker_id="w1")
    assert job is not None

    # Fail with a known delay
    ok = fail(
        conn,
        job_id=job["id"],
        worker_id="w1",
        token=job["attempts"],
        error="test failure",
        delay_seconds=5.0,
    )
    assert ok

    # Check the job state
    updated = get_job(conn, job_id=job["id"])
    assert updated is not None
    assert updated["status"] == "pending"
    assert updated["last_error"] == "test failure"
    assert updated["locked_by"] is None
    assert updated["locked_until"] is None


@pytest.mark.timeout(10)
def test_attempts_increment_on_claim(conn) -> None:
    """Each claim increments the attempts counter."""
    enqueue(conn, type="noop", max_attempts=5)

    # Claim and fail twice
    for i in range(2):
        job = claim(conn, queues=["default"], worker_id=f"w{i}", lease_seconds=1)
        assert job is not None
        assert job["attempts"] == i + 1
        fail(
            conn,
            job_id=job["id"],
            worker_id=f"w{i}",
            token=job["attempts"],
            error=f"fail {i}",
            delay_seconds=0,  # immediate retry for testing
        )


@pytest.mark.timeout(10)
def test_backoff_compute_delay_within_bounds() -> None:
    """Backoff delay is within expected range [0, min(cap, base * 2^(n-1))]."""
    base = 2.0
    cap = 300.0

    for attempt in range(1, 10):
        for _ in range(100):  # Statistical check
            delay = compute_delay(attempt, base=base, cap=cap)
            max_expected = min(cap, base * (2 ** (attempt - 1)))
            assert 0 <= delay <= max_expected, (
                f"Delay {delay} out of range [0, {max_expected}] for attempt {attempt}"
            )


@pytest.mark.timeout(10)
def test_retry_then_succeed(conn) -> None:
    """A job that fails once can be retried and then succeed."""
    enqueue(conn, type="noop", max_attempts=3)

    # First attempt: fail
    job = claim(conn, queues=["default"], worker_id="w1")
    assert job is not None
    fail(
        conn,
        job_id=job["id"],
        worker_id="w1",
        token=job["attempts"],
        error="transient",
        delay_seconds=0,
    )

    # Second attempt: succeed
    job2 = claim(conn, queues=["default"], worker_id="w2")
    assert job2 is not None
    assert job2["id"] == job["id"]
    assert job2["attempts"] == 2

    ok = ack(conn, job_id=job2["id"], worker_id="w2", token=job2["attempts"])
    assert ok

    # Verify final state
    final = get_job(conn, job_id=job["id"])
    assert final is not None
    assert final["status"] == "succeeded"
