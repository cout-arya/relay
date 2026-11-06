"""Tests: lease reclaim, fencing, and heartbeat.

Verifies that:
- After lease expiry, another worker can reclaim the job
- The original worker's ack, fail, and heartbeat are fenced out (zero rows)
"""

from __future__ import annotations

import time

import pytest

from relay.queue import ack, claim, enqueue, fail, heartbeat


@pytest.mark.timeout(15)
def test_lease_reclaim_after_expiry(conn) -> None:
    """A job whose lease expired is reclaimed by another worker."""
    # Enqueue with a very short lease
    enqueue(conn, type="sleep", payload={"seconds": 10})

    # Worker 1 claims with 1-second lease
    job1 = claim(conn, queues=["default"], worker_id="w1", lease_seconds=1)
    assert job1 is not None
    assert job1["locked_by"] == "w1"
    assert job1["attempts"] == 1

    # Wait for lease to expire
    time.sleep(1.5)

    # Worker 2 reclaims the expired job
    job2 = claim(conn, queues=["default"], worker_id="w2", lease_seconds=30)
    assert job2 is not None
    assert job2["id"] == job1["id"], "Should reclaim the same job"
    assert job2["locked_by"] == "w2"
    assert job2["attempts"] == 2  # Incremented


@pytest.mark.timeout(15)
def test_fenced_ack_after_reclaim(conn) -> None:
    """After reclaim, original worker's ack affects zero rows (fenced out)."""
    enqueue(conn, type="noop")

    # Worker 1 claims
    job1 = claim(conn, queues=["default"], worker_id="w1", lease_seconds=1)
    assert job1 is not None
    token1 = job1["attempts"]

    # Wait for lease to expire
    time.sleep(1.5)

    # Worker 2 reclaims
    job2 = claim(conn, queues=["default"], worker_id="w2", lease_seconds=30)
    assert job2 is not None

    # Worker 1 tries to ack — should be fenced out
    acked = ack(conn, job_id=job1["id"], worker_id="w1", token=token1)
    assert not acked, "Stale worker should be fenced out"


@pytest.mark.timeout(15)
def test_fenced_fail_after_reclaim(conn) -> None:
    """After reclaim, original worker's fail affects zero rows."""
    enqueue(conn, type="noop")

    job1 = claim(conn, queues=["default"], worker_id="w1", lease_seconds=1)
    assert job1 is not None
    token1 = job1["attempts"]

    time.sleep(1.5)

    job2 = claim(conn, queues=["default"], worker_id="w2", lease_seconds=30)
    assert job2 is not None

    # Worker 1 tries to fail — fenced out
    failed = fail(conn, job_id=job1["id"], worker_id="w1", token=token1, error="stale")
    assert not failed


@pytest.mark.timeout(15)
def test_fenced_heartbeat_after_reclaim(conn) -> None:
    """After reclaim, original worker's heartbeat affects zero rows."""
    enqueue(conn, type="noop")

    job1 = claim(conn, queues=["default"], worker_id="w1", lease_seconds=1)
    assert job1 is not None
    token1 = job1["attempts"]

    time.sleep(1.5)

    job2 = claim(conn, queues=["default"], worker_id="w2", lease_seconds=30)
    assert job2 is not None

    # Worker 1 tries to heartbeat — fenced out
    ok = heartbeat(conn, job_id=job1["id"], worker_id="w1", token=token1)
    assert not ok


@pytest.mark.timeout(10)
def test_heartbeat_extends_lease(conn) -> None:
    """A valid heartbeat extends the lease."""
    enqueue(conn, type="noop")
    job = claim(conn, queues=["default"], worker_id="w1", lease_seconds=5)
    assert job is not None

    original_until = job["locked_until"]

    # Heartbeat should extend the lease
    ok = heartbeat(conn, job_id=job["id"], worker_id="w1", token=job["attempts"], lease_seconds=10)
    assert ok

    # Verify the lease was extended
    from relay.queue import get_job
    updated = get_job(conn, job_id=job["id"])
    assert updated is not None
    assert updated["locked_until"] > original_until


@pytest.mark.timeout(15)
def test_lease_lost_outcome_recorded(conn) -> None:
    """After reclaim, the original worker attempt is marked outcome=lease_lost."""
    from relay.queue import record_lease_lost
    enqueue(conn, type="noop")

    # Worker 1 claims
    job1 = claim(conn, queues=["default"], worker_id="w1", lease_seconds=1)
    assert job1 is not None
    token1 = job1["attempts"]

    # Wait for lease to expire
    time.sleep(1.5)

    # Worker 2 reclaims
    job2 = claim(conn, queues=["default"], worker_id="w2", lease_seconds=30)
    assert job2 is not None

    # Simulate worker 1 discovering lease loss and recording it
    record_lease_lost(conn, job_id=job1["id"], worker_id="w1", token=token1)

    # Verify the attempt outcome was recorded
    from relay.queue import get_job_attempts
    attempts = get_job_attempts(conn, job_id=job1["id"])
    w1_attempt = next((a for a in attempts if a["worker_id"] == "w1"), None)
    assert w1_attempt is not None
    assert w1_attempt["outcome"] == "lease_lost", (
        f"Expected lease_lost, got {w1_attempt['outcome']}"
    )

    # Worker 2's attempt should be in-flight (no outcome yet)
    w2_attempt = next((a for a in attempts if a["worker_id"] == "w2"), None)
    assert w2_attempt is not None
    assert w2_attempt["outcome"] is None  # still running
