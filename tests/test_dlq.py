"""Tests: dead-letter queue, reaper, and replay.

Verifies:
- Jobs that exhaust max_attempts end up dead
- Reaper catches expired running jobs at max_attempts
- Replay resets dead jobs to pending
"""

from __future__ import annotations

import time

import pytest

from relay.queue import claim, enqueue, fail, get_job, list_dead, reap, replay


@pytest.mark.timeout(10)
def test_max_attempts_to_dead(conn) -> None:
    """A job that always fails goes to 'dead' after max_attempts."""
    enqueue(conn, type="noop", max_attempts=2)

    # Attempt 1: fail
    job = claim(conn, queues=["default"], worker_id="w1")
    assert job is not None
    fail(
        conn,
        job_id=job["id"],
        worker_id="w1",
        token=job["attempts"],
        error="fail 1",
        delay_seconds=0,
    )

    # Attempt 2: fail → should go to dead
    job2 = claim(conn, queues=["default"], worker_id="w2")
    assert job2 is not None
    assert job2["attempts"] == 2
    fail(
        conn,
        job_id=job2["id"],
        worker_id="w2",
        token=job2["attempts"],
        error="fail 2",
        delay_seconds=0,
    )

    # Verify dead
    final = get_job(conn, job_id=job["id"])
    assert final is not None
    assert final["status"] == "dead"
    assert final["last_error"] == "fail 2"
    assert final["finished_at"] is not None


@pytest.mark.timeout(10)
def test_permanent_error_goes_dead(conn) -> None:
    """A permanent failure goes straight to dead regardless of attempts."""
    enqueue(conn, type="noop", max_attempts=5)
    job = claim(conn, queues=["default"], worker_id="w1")
    assert job is not None

    fail(
        conn,
        job_id=job["id"],
        worker_id="w1",
        token=job["attempts"],
        error="bad input",
        permanent=True,
    )

    final = get_job(conn, job_id=job["id"])
    assert final is not None
    assert final["status"] == "dead"
    assert final["attempts"] == 1  # Only one attempt


@pytest.mark.timeout(15)
def test_reaper_catches_exhausted_expired(conn) -> None:
    """Reaper moves expired running jobs at max_attempts to dead."""
    enqueue(conn, type="noop", max_attempts=1)

    # Claim with a short lease
    job = claim(conn, queues=["default"], worker_id="w1", lease_seconds=1)
    assert job is not None
    assert job["attempts"] == 1  # At max_attempts

    # Wait for lease to expire
    time.sleep(1.5)

    # Run reaper
    reaped = reap(conn)
    assert reaped == 1

    # Verify dead
    final = get_job(conn, job_id=job["id"])
    assert final is not None
    assert final["status"] == "dead"


@pytest.mark.timeout(10)
def test_replay_resets_dead_job(conn) -> None:
    """Replay resets a dead job to pending with attempts = 0."""
    enqueue(conn, type="noop", max_attempts=1)

    # Make it fail → dead
    job = claim(conn, queues=["default"], worker_id="w1")
    assert job is not None
    fail(
        conn,
        job_id=job["id"],
        worker_id="w1",
        token=job["attempts"],
        error="oops",
        permanent=True,
    )

    # Verify it's dead
    dead = get_job(conn, job_id=job["id"])
    assert dead is not None
    assert dead["status"] == "dead"

    # Replay
    replayed = replay(conn, job_id=job["id"])
    assert replayed is not None
    assert replayed["status"] == "pending"
    assert replayed["attempts"] == 0
    assert replayed["last_error"] is None
    assert replayed["finished_at"] is None


@pytest.mark.timeout(10)
def test_list_dead_returns_dead_jobs(conn) -> None:
    """list_dead returns only dead jobs."""
    enqueue(conn, type="noop", max_attempts=1)
    job = claim(conn, queues=["default"], worker_id="w1")
    assert job is not None
    fail(conn, job_id=job["id"], worker_id="w1", token=job["attempts"],
         error="dead", permanent=True)

    dead_list = list_dead(conn)
    assert len(dead_list) == 1
    assert dead_list[0]["id"] == job["id"]
