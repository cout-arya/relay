"""Smoke tests for the Relay Typer CLI."""

from __future__ import annotations

import json

from typer.testing import CliRunner

from relay.cli import app
from relay.queue import enqueue

runner = CliRunner()


def test_cli_enqueue(conn) -> None:
    """relay enqueue creates a new job."""
    result = runner.invoke(app, ["enqueue", "noop", "--payload", '{"k": "v"}'])
    assert result.exit_code == 0
    assert "Job" in result.output
    assert "created" in result.output


def test_cli_enqueue_json(conn) -> None:
    """relay enqueue --json outputs valid JSON."""
    result = runner.invoke(app, ["enqueue", "noop", "--json"])
    assert result.exit_code == 0
    data = json.loads(result.output)
    assert data["type"] == "noop"
    assert data["status"] == "pending"


def test_cli_stats(conn) -> None:
    """relay stats prints queue statistics."""
    result = runner.invoke(app, ["stats"])
    assert result.exit_code == 0
    assert "Queue Statistics" in result.output


def test_cli_stats_json(conn) -> None:
    """relay stats --json prints JSON stats."""
    result = runner.invoke(app, ["stats", "--json"])
    assert result.exit_code == 0
    data = json.loads(result.output)
    assert "by_status" in data


def test_cli_jobs(conn) -> None:
    """relay jobs lists jobs."""
    enqueue(conn, type="noop")
    result = runner.invoke(app, ["jobs"])
    assert result.exit_code == 0
    assert "noop" in result.output


def test_cli_show(conn) -> None:
    """relay show displays details for a specific job."""
    job, _ = enqueue(conn, type="noop")
    result = runner.invoke(app, ["show", str(job["id"])])
    assert result.exit_code == 0
    assert f"Job {job['id']}" in result.output


def test_cli_dead_and_replay(conn) -> None:
    """relay dead lists DLQ, relay replay replays dead job."""
    job, _ = enqueue(conn, type="noop", max_attempts=1)
    # Move to dead
    conn.execute("UPDATE jobs SET status = 'dead' WHERE id = %s;", [job["id"]])
    conn.commit()

    # List dead
    dead_res = runner.invoke(app, ["dead"])
    assert dead_res.exit_code == 0
    assert str(job["id"]) in dead_res.output

    # Replay
    replay_res = runner.invoke(app, ["replay", str(job["id"])])
    assert replay_res.exit_code == 0
    assert "replayed" in replay_res.output


def test_cli_workers(conn) -> None:
    """relay workers lists registered workers."""
    result = runner.invoke(app, ["workers"])
    assert result.exit_code == 0
    assert "Workers" in result.output


def test_cli_replay_all(conn) -> None:
    """relay replay-all replays all dead jobs."""
    job, _ = enqueue(conn, type="noop", max_attempts=1)
    conn.execute("UPDATE jobs SET status = 'dead' WHERE id = %s;", [job["id"]])
    conn.commit()

    result = runner.invoke(app, ["replay-all"])
    assert result.exit_code == 0
    assert "Replayed" in result.output


def test_cli_jobs_status_filter(conn) -> None:
    """relay jobs --status pending filters by status."""
    enqueue(conn, type="noop")
    result = runner.invoke(app, ["jobs", "--status", "pending"])
    assert result.exit_code == 0
    assert "noop" in result.output


def test_cli_jobs_json_output(conn) -> None:
    """relay jobs --json returns valid JSON array."""
    import json
    enqueue(conn, type="noop")
    result = runner.invoke(app, ["jobs", "--json"])
    assert result.exit_code == 0
    data = json.loads(result.output)
    assert isinstance(data, list)
    assert any(j["type"] == "noop" for j in data)
