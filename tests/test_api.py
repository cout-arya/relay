"""Tests: API smoke tests for all endpoints."""

from __future__ import annotations

import pytest
from fastapi.testclient import TestClient

from relay.api.main import app
from relay.db import get_conn
from relay.queue import claim, enqueue, fail


@pytest.fixture
def client():
    """FastAPI test client."""
    with TestClient(app) as c:
        yield c


@pytest.mark.timeout(10)
def test_health(client) -> None:
    r = client.get("/health")
    assert r.status_code == 200
    assert r.json()["status"] == "ok"


@pytest.mark.timeout(10)
def test_enqueue_job(client) -> None:
    r = client.post("/jobs", json={"type": "noop", "payload": {"key": "value"}})
    assert r.status_code == 201
    data = r.json()
    assert data["type"] == "noop"
    assert data["status"] == "pending"


@pytest.mark.timeout(10)
def test_enqueue_idempotent(client) -> None:
    body = {"type": "noop", "idempotency_key": "test-idem"}
    r1 = client.post("/jobs", json=body)
    assert r1.status_code == 201

    r2 = client.post("/jobs", json=body)
    assert r2.status_code == 200
    assert r2.json()["id"] == r1.json()["id"]


@pytest.mark.timeout(10)
def test_get_job_detail(client) -> None:
    r = client.post("/jobs", json={"type": "noop"})
    job_id = r.json()["id"]

    r2 = client.get(f"/jobs/{job_id}")
    assert r2.status_code == 200
    assert r2.json()["job"]["id"] == job_id


@pytest.mark.timeout(10)
def test_get_job_not_found(client) -> None:
    r = client.get("/jobs/999999")
    assert r.status_code == 404


@pytest.mark.timeout(10)
def test_list_jobs(client) -> None:
    client.post("/jobs", json={"type": "noop"})
    r = client.get("/jobs")
    assert r.status_code == 200
    assert len(r.json()) >= 1


@pytest.mark.timeout(10)
def test_list_jobs_with_filter(client) -> None:
    client.post("/jobs", json={"type": "noop"})
    r = client.get("/jobs?status=pending")
    assert r.status_code == 200
    for j in r.json():
        assert j["status"] == "pending"


@pytest.mark.timeout(10)
def test_stats(client) -> None:
    client.post("/jobs", json={"type": "noop"})
    r = client.get("/stats")
    assert r.status_code == 200
    data = r.json()
    assert "by_status" in data
    assert "in_flight" in data


@pytest.mark.timeout(10)
def test_dead_empty(client) -> None:
    r = client.get("/dead")
    assert r.status_code == 200
    assert r.json() == []


@pytest.mark.timeout(10)
def test_dead_and_replay(client) -> None:
    # Create a dead job
    with get_conn() as conn:
        job, _ = enqueue(conn, type="noop", max_attempts=1)
        claimed = claim(conn, queues=["default"], worker_id="test")
        assert claimed is not None
        fail(conn, job_id=claimed["id"], worker_id="test",
             token=claimed["attempts"], error="test", permanent=True)

    r = client.get("/dead")
    assert r.status_code == 200
    dead_jobs = r.json()
    assert len(dead_jobs) == 1

    # Replay
    r2 = client.post(f"/dead/{dead_jobs[0]['id']}/replay")
    assert r2.status_code == 200
    assert r2.json()["status"] == "pending"


@pytest.mark.timeout(10)
def test_queues(client) -> None:
    r = client.get("/queues")
    assert r.status_code == 200


@pytest.mark.timeout(10)
def test_put_queue_config(client) -> None:
    r = client.put("/queues/test-q", json={"max_concurrency": 5, "default_lease_s": 60})
    assert r.status_code == 200
    data = r.json()
    assert data["queue"] == "test-q"
    assert data["max_concurrency"] == 5


@pytest.mark.timeout(10)
def test_workers(client) -> None:
    r = client.get("/workers")
    assert r.status_code == 200


@pytest.mark.timeout(10)
def test_llm_usage(client) -> None:
    r = client.get("/llm/usage")
    assert r.status_code == 200


@pytest.mark.timeout(10)
def test_demo_kill_disabled(client) -> None:
    """Kill-worker should be disabled when DEMO_MODE is off."""
    # settings.demo_mode defaults to False in test env
    r = client.post("/demo/kill-worker")
    assert r.status_code == 403


@pytest.mark.timeout(10)
def test_dashboard(client) -> None:
    r = client.get("/dashboard")
    assert r.status_code == 200
    assert "Relay" in r.text


@pytest.mark.timeout(10)
def test_payload_too_large(client) -> None:
    """Payloads exceeding 256 KB are rejected with HTTP 413."""
    big_payload = {"data": "x" * (256 * 1024 + 1)}
    r = client.post("/jobs", json={"type": "noop", "payload": big_payload})
    assert r.status_code == 413
    assert "256 KB" in r.json()["detail"]


@pytest.mark.timeout(10)
def test_replay_all_dead(client) -> None:
    """POST /dead/replay-all replays all dead jobs."""
    from relay.db import get_conn
    from relay.queue import claim, enqueue, fail

    with get_conn() as conn:
        for _ in range(3):
            job, _ = enqueue(conn, type="noop", max_attempts=1)
            claimed = claim(conn, queues=["default"], worker_id="test-ra")
            assert claimed is not None
            fail(conn, job_id=claimed["id"], worker_id="test-ra",
                 token=claimed["attempts"], error="test", permanent=True)

    r = client.post("/dead/replay-all")
    assert r.status_code == 200
    assert r.json()["replayed"] == 3


@pytest.mark.timeout(10)
def test_replay_all_dead_with_queue(client) -> None:
    """POST /dead/replay-all?queue=q replays only dead jobs in that queue."""
    from relay.db import get_conn
    from relay.queue import claim, enqueue, fail

    with get_conn() as conn:
        # 2 dead jobs in "q-test"
        for _ in range(2):
            job, _ = enqueue(conn, type="noop", max_attempts=1, queue="q-test")
            claimed = claim(conn, queues=["q-test"], worker_id="test-raq")
            assert claimed is not None
            fail(conn, job_id=claimed["id"], worker_id="test-raq",
                 token=claimed["attempts"], error="x", permanent=True)
        # 1 dead job in "q-other"
        job, _ = enqueue(conn, type="noop", max_attempts=1, queue="q-other")
        claimed = claim(conn, queues=["q-other"], worker_id="test-raq")
        assert claimed is not None
        fail(conn, job_id=claimed["id"], worker_id="test-raq",
             token=claimed["attempts"], error="x", permanent=True)

    r = client.post("/dead/replay-all?queue=q-test")
    assert r.status_code == 200
    assert r.json()["replayed"] == 2
