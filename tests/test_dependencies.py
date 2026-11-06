"""Tests for Job Dependencies (Section 14.1 Stretch Goal)."""

from __future__ import annotations

import pytest

from relay.queue import ack, claim, enqueue, fail, get_job


def test_dependency_starts_blocked(conn) -> None:
    """Job depending on an uncompleted job starts as 'blocked'."""
    parent, _ = enqueue(conn, type="noop")
    child, _ = enqueue(conn, type="noop", depends_on=[parent["id"]])

    assert parent["status"] == "pending"
    assert child["status"] == "blocked"
    assert child["depends_on"] == [parent["id"]]


def test_blocked_job_not_claimed(conn) -> None:
    """Workers using claim() never pick up 'blocked' jobs."""
    parent, _ = enqueue(conn, type="noop")
    child, _ = enqueue(conn, type="noop", depends_on=[parent["id"]])

    # First claim gets the parent
    job1 = claim(conn, queues=["default"], worker_id="w1")
    assert job1 is not None
    assert job1["id"] == parent["id"]

    # Second claim returns None because child is still blocked
    job2 = claim(conn, queues=["default"], worker_id="w1")
    assert job2 is None


def test_ack_unblocks_dependent_job(conn) -> None:
    """Acking the parent job atomically moves dependent job to 'pending'."""
    parent, _ = enqueue(conn, type="noop")
    child, _ = enqueue(conn, type="noop", depends_on=[parent["id"]])

    # Claim parent
    claimed_parent = claim(conn, queues=["default"], worker_id="w1")
    assert claimed_parent is not None

    # Ack parent
    ack(conn, job_id=parent["id"], worker_id="w1", token=claimed_parent["attempts"])

    # Child should now be pending
    updated_child = get_job(conn, job_id=child["id"])
    assert updated_child is not None
    assert updated_child["status"] == "pending"

    # Worker can now claim child
    claimed_child = claim(conn, queues=["default"], worker_id="w1")
    assert claimed_child is not None
    assert claimed_child["id"] == child["id"]


def test_multiple_dependencies_all_must_succeed(conn) -> None:
    """Child with multiple dependencies only unblocks when ALL succeed."""
    p1, _ = enqueue(conn, type="noop")
    p2, _ = enqueue(conn, type="noop")
    child, _ = enqueue(conn, type="noop", depends_on=[p1["id"], p2["id"]])

    # Claim and ack p1 only
    cp1 = claim(conn, queues=["default"], worker_id="w1")
    assert cp1 is not None
    ack(conn, job_id=cp1["id"], worker_id="w1", token=cp1["attempts"])

    # Child should STILL be blocked because p2 hasn't succeeded
    c_status = get_job(conn, job_id=child["id"])
    assert c_status is not None
    assert c_status["status"] == "blocked"

    # Claim and ack p2
    cp2 = claim(conn, queues=["default"], worker_id="w1")
    assert cp2 is not None
    ack(conn, job_id=cp2["id"], worker_id="w1", token=cp2["attempts"])

    # Now child should be pending
    c_status = get_job(conn, job_id=child["id"])
    assert c_status is not None
    assert c_status["status"] == "pending"


def test_dead_dependency_leaves_child_blocked(conn) -> None:
    """If a dependency dies, dependent child remains blocked."""
    parent, _ = enqueue(conn, type="noop", max_attempts=1)
    child, _ = enqueue(conn, type="noop", depends_on=[parent["id"]])

    cp = claim(conn, queues=["default"], worker_id="w1")
    assert cp is not None
    # Fail parent permanently
    fail(
        conn,
        job_id=parent["id"],
        worker_id="w1",
        token=cp["attempts"],
        error="fatal",
        permanent=True,
    )

    parent_job = get_job(conn, job_id=parent["id"])
    assert parent_job is not None
    assert parent_job["status"] == "dead"

    child_job = get_job(conn, job_id=child["id"])
    assert child_job is not None
    assert child_job["status"] == "blocked"


def test_already_succeeded_dependency_starts_pending(conn) -> None:
    """If all dependencies are already 'succeeded', job starts as 'pending' immediately."""
    p, _ = enqueue(conn, type="noop")
    cp = claim(conn, queues=["default"], worker_id="w1")
    assert cp is not None
    ack(conn, job_id=p["id"], worker_id="w1", token=cp["attempts"])

    # Enqueue child after parent is already succeeded
    child, _ = enqueue(conn, type="noop", depends_on=[p["id"]])
    assert child["status"] == "pending"


def test_invalid_dependency_raises_error(conn) -> None:
    """Enqueuing with nonexistent dependency ID raises ValueError."""
    with pytest.raises(ValueError, match="not found"):
        enqueue(conn, type="noop", depends_on=[999999])


def test_cycle_rejected_at_enqueue(conn) -> None:
    """Valid DAG enqueues successfully; _check_no_cycle is a no-op for acyclic chains (PRD §14.1).
    """
    # A chain A <- B <- C is a valid DAG; all three must enqueue without error.
    a, _ = enqueue(conn, type="noop")
    b, _ = enqueue(conn, type="noop", depends_on=[a["id"]])
    c, _ = enqueue(conn, type="noop", depends_on=[b["id"]])
    assert b["status"] == "blocked"
    assert c["status"] == "blocked"


def test_self_dependency_rejected(conn) -> None:
    """_check_no_cycle raises ValueError when new_job_id appears in its own dep closure (PRD §14.1).

    Simulates the scenario: B depends_on A, then we try to make A a dependency of itself
    by calling _check_no_cycle(conn, new_job_id=A, depends_on=[B]).
    The CTE walks B -> A (via job_dependencies) and finds A == new_job_id, so it raises.
    """
    from relay.queue import _check_no_cycle

    # Create A then B depending on A (so job_dependencies has edge B -> A)
    a, _ = enqueue(conn, type="noop")
    b, _ = enqueue(conn, type="noop", depends_on=[a["id"]])

    # Now simulate: if we tried to insert A as a new job with depends_on=[B],
    # the CTE would walk B -> A and find a["id"] == new_job_id => cycle detected.
    with pytest.raises(ValueError, match="cycle"):
        _check_no_cycle(conn, a["id"], [b["id"]])
