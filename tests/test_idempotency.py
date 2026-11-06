"""Tests: idempotent enqueue and idempotent effects.

Verifies:
- Same idempotency_key returns the same job (no duplicate enqueue)
- multi_step_transfer produces exactly one effect per step despite crashes
"""

from __future__ import annotations

import pytest

from relay.queue import ack, claim, enqueue, fail


@pytest.mark.timeout(10)
def test_idempotent_enqueue(conn) -> None:
    """Same idempotency_key returns the same job, created=False."""
    job1, created1 = enqueue(
        conn, type="noop", payload={"x": 1}, idempotency_key="unique-1"
    )
    assert created1 is True

    job2, created2 = enqueue(
        conn, type="noop", payload={"x": 2}, idempotency_key="unique-1"
    )
    assert created2 is False
    assert job2["id"] == job1["id"]


@pytest.mark.timeout(10)
def test_different_idempotency_keys_create_different_jobs(conn) -> None:
    """Different keys create different jobs."""
    job1, _ = enqueue(conn, type="noop", idempotency_key="key-a")
    job2, _ = enqueue(conn, type="noop", idempotency_key="key-b")
    assert job1["id"] != job2["id"]


@pytest.mark.timeout(10)
def test_idempotent_effects_one_per_step(conn) -> None:
    """After crash + retry, effects table has exactly one row per step.

    Simulates the multi_step_transfer pattern: the first attempt crashes
    after step 2, the second attempt completes all steps, but effects
    for steps 1 and 2 already exist and are not duplicated.
    """
    from psycopg.types.json import Jsonb

    # Enqueue a job
    job, _ = enqueue(conn, type="multi_step_transfer", max_attempts=3)

    # Claim as worker 1
    claimed = claim(conn, queues=["default"], worker_id="w1")
    assert claimed is not None
    job_id = claimed["id"]
    token1 = claimed["attempts"]

    # Simulate: step "reserve" and "charge" succeed, then crash
    steps = ["reserve", "charge"]
    for step in steps:
        with conn.transaction():
            conn.execute(
                """
                INSERT INTO effects (job_id, step, result)
                VALUES (%s, %s, %s)
                ON CONFLICT (job_id, step) DO NOTHING;
                """,
                [job_id, step, Jsonb({"attempt": token1, "step": step})],
            )

    # Crash! Fail the job
    fail(conn, job_id=job_id, worker_id="w1", token=token1,
         error="crash after charge", delay_seconds=0)

    # Claim as worker 2
    claimed2 = claim(conn, queues=["default"], worker_id="w2")
    assert claimed2 is not None
    token2 = claimed2["attempts"]

    # Redo all steps — "reserve" and "charge" should be no-ops
    all_steps = ["reserve", "charge", "notify"]
    for step in all_steps:
        with conn.transaction():
            conn.execute(
                """
                INSERT INTO effects (job_id, step, result)
                VALUES (%s, %s, %s)
                ON CONFLICT (job_id, step) DO NOTHING;
                """,
                [job_id, step, Jsonb({"attempt": token2, "step": step})],
            )

    # Ack
    ack(conn, job_id=job_id, worker_id="w2", token=token2)

    # Verify: exactly 3 effects, one per step
    rows = conn.execute(
        "SELECT * FROM effects WHERE job_id = %s ORDER BY step;",
        [job_id],
    ).fetchall()
    assert len(rows) == 3
    step_names = {r["step"] for r in rows}
    assert step_names == {"reserve", "charge", "notify"}

    # The first two effects should have token1 (from the first attempt)
    for r in rows:
        if r["step"] in ("reserve", "charge"):
            assert r["result"]["attempt"] == token1, \
                f"Effect for {r['step']} should preserve original attempt"
        else:
            assert r["result"]["attempt"] == token2


@pytest.mark.timeout(10)
def test_priority_order(conn) -> None:
    """Higher priority jobs are claimed first."""
    enqueue(conn, type="noop", priority=1, payload={"order": "low"})
    enqueue(conn, type="noop", priority=10, payload={"order": "high"})
    enqueue(conn, type="noop", priority=5, payload={"order": "mid"})

    # Claims should return in priority DESC order
    j1 = claim(conn, queues=["default"], worker_id="w1")
    assert j1 is not None
    assert j1["payload"]["order"] == "high"

    j2 = claim(conn, queues=["default"], worker_id="w2")
    assert j2 is not None
    assert j2["payload"]["order"] == "mid"

    j3 = claim(conn, queues=["default"], worker_id="w3")
    assert j3 is not None
    assert j3["payload"]["order"] == "low"


@pytest.mark.timeout(10)
def test_delayed_job_not_claimable_before_run_at(conn) -> None:
    """A job with delay is not claimable before run_at."""
    enqueue(conn, type="noop", delay_seconds=300)  # 5 minutes in the future

    # Should not be claimable
    job = claim(conn, queues=["default"], worker_id="w1")
    assert job is None
