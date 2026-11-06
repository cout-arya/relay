"""Relay CLI — Typer-based command-line interface."""

from __future__ import annotations

import json
from typing import Any

import typer
from rich.console import Console
from rich.table import Table

from relay.config import settings
from relay.db import get_conn
from relay.migrate import run_migrations
from relay.queue import (
    enqueue,
    get_job,
    get_job_attempts,
    get_stats,
    list_dead,
    list_jobs,
    list_workers,
    replay,
    replay_all,
)

app = typer.Typer(
    name="relay",
    help="Relay: A reliable PostgreSQL-backed job queue with dead-letter recovery.",
)

console = Console()


def _ensure_migrations() -> None:
    """Run migrations before any CLI command that touches the DB."""
    run_migrations()


def _format_job(job: dict[str, Any]) -> dict[str, Any]:
    """Make a job dict JSON-serializable."""
    return {k: v.isoformat() if hasattr(v, "isoformat") else v for k, v in job.items()}


# ─── Commands ────────────────────────────────────────────────────────────────


@app.command()
def migrate() -> None:
    """Run database migrations."""
    run_migrations()
    console.print("[green]Migrations applied successfully.[/green]")


@app.command(name="enqueue")
def enqueue_cmd(
    type: str = typer.Argument(..., help="Job type name"),
    queue: str = typer.Option("default", help="Queue name"),
    payload: str = typer.Option("{}", help="JSON payload"),
    priority: int = typer.Option(0, help="Priority (higher runs first)"),
    delay: float = typer.Option(0, help="Delay in seconds"),
    max_attempts: int = typer.Option(0, help="Max attempts (0 = default)"),
    idempotency_key: str = typer.Option(None, help="Idempotency key"),
    output_json: bool = typer.Option(False, "--json", help="Output as JSON"),
) -> None:
    """Enqueue a new job."""
    _ensure_migrations()

    try:
        payload_dict = json.loads(payload)
    except json.JSONDecodeError as e:
        console.print(f"[red]Invalid JSON payload: {e}[/red]")
        raise typer.Exit(1)

    with get_conn() as conn:
        job, created = enqueue(
            conn,
            queue=queue,
            type=type,
            payload=payload_dict,
            priority=priority,
            delay_seconds=delay if delay > 0 else None,
            max_attempts=max_attempts if max_attempts > 0 else None,
            idempotency_key=idempotency_key,
        )

    if output_json:
        console.print(json.dumps(_format_job(job), indent=2))
    else:
        status = "created" if created else "already exists (idempotent)"
        console.print(f"[green]Job {job['id']} {status}[/green]")
        console.print(f"  Type: {job['type']}")
        console.print(f"  Queue: {job['queue']}")
        console.print(f"  Priority: {job['priority']}")
        console.print(f"  Status: {job['status']}")


@app.command()
def stats(
    output_json: bool = typer.Option(False, "--json", help="Output as JSON"),
) -> None:
    """Show queue statistics."""
    _ensure_migrations()

    with get_conn() as conn:
        s = get_stats(conn)

    if output_json:
        console.print(json.dumps(s, indent=2))
    else:
        console.print("[bold]Queue Statistics[/bold]")
        table = Table()
        table.add_column("Status", style="cyan")
        table.add_column("Count", justify="right")
        for status_name, count in s.get("by_status", {}).items():
            table.add_row(status_name, str(count))
        console.print(table)

        console.print(f"\nIn-flight: {s['in_flight']}")
        console.print(f"Active workers: {s['active_workers']}")
        if s["oldest_pending_age_seconds"]:
            console.print(f"Oldest pending: {s['oldest_pending_age_seconds']:.1f}s")
        console.print(f"Retry rate (5m): {s['retry_rate_5m']}")
        console.print(f"Throughput (1m): {s['throughput_1m']}")


@app.command()
def jobs(
    status: str = typer.Option(None, help="Filter by status"),
    queue: str = typer.Option(None, help="Filter by queue"),
    limit: int = typer.Option(20, help="Max results"),
    output_json: bool = typer.Option(False, "--json", help="Output as JSON"),
) -> None:
    """List jobs with optional filters."""
    _ensure_migrations()

    with get_conn() as conn:
        job_list = list_jobs(conn, status=status, queue=queue, limit=limit)

    if output_json:
        console.print(json.dumps([_format_job(j) for j in job_list], indent=2))
    else:
        table = Table(title=f"Jobs (showing {len(job_list)})")
        table.add_column("ID", justify="right")
        table.add_column("Queue")
        table.add_column("Type")
        table.add_column("Status")
        table.add_column("Priority", justify="right")
        table.add_column("Attempts")
        table.add_column("Error")

        for j in job_list:
            error = (j.get("last_error") or "")[:40]
            table.add_row(
                str(j["id"]),
                j["queue"],
                j["type"],
                j["status"],
                str(j["priority"]),
                f"{j['attempts']}/{j['max_attempts']}",
                error,
            )
        console.print(table)


@app.command()
def show(
    job_id: int = typer.Argument(..., help="Job ID"),
    output_json: bool = typer.Option(False, "--json", help="Output as JSON"),
) -> None:
    """Show details for a specific job."""
    _ensure_migrations()

    with get_conn() as conn:
        job = get_job(conn, job_id=job_id)
        if not job:
            console.print(f"[red]Job {job_id} not found[/red]")
            raise typer.Exit(1)
        attempts = get_job_attempts(conn, job_id=job_id)

    if output_json:
        data = {
            "job": _format_job(job),
            "attempts": [_format_job(a) for a in attempts],
        }
        console.print(json.dumps(data, indent=2))
    else:
        console.print(f"[bold]Job {job_id}[/bold]")
        for k, v in _format_job(job).items():
            console.print(f"  {k}: {v}")

        if attempts:
            console.print(f"\n[bold]Attempts ({len(attempts)})[/bold]")
            table = Table()
            table.add_column("Attempt")
            table.add_column("Worker")
            table.add_column("Outcome")
            table.add_column("Error")
            for a in attempts:
                table.add_row(
                    str(a["attempt_no"]),
                    a["worker_id"],
                    a.get("outcome", "?"),
                    (a.get("error") or "")[:60],
                )
            console.print(table)


@app.command()
def dead(
    queue: str = typer.Option(None, help="Filter by queue"),
    limit: int = typer.Option(20, help="Max results"),
    output_json: bool = typer.Option(False, "--json", help="Output as JSON"),
) -> None:
    """List dead-letter queue contents."""
    _ensure_migrations()

    with get_conn() as conn:
        dead_list = list_dead(conn, queue=queue, limit=limit)

    if output_json:
        console.print(json.dumps([_format_job(j) for j in dead_list], indent=2))
    else:
        table = Table(title=f"Dead Letters ({len(dead_list)})")
        table.add_column("ID", justify="right")
        table.add_column("Queue")
        table.add_column("Type")
        table.add_column("Attempts")
        table.add_column("Error")
        table.add_column("Finished")

        for j in dead_list:
            finished = j.get("finished_at")
            table.add_row(
                str(j["id"]),
                j["queue"],
                j["type"],
                str(j["attempts"]),
                (j.get("last_error") or "")[:40],
                finished.isoformat() if hasattr(finished, "isoformat") else str(finished or ""),
            )
        console.print(table)


@app.command(name="replay")
def replay_cmd(
    job_id: int = typer.Argument(..., help="Job ID to replay"),
    output_json: bool = typer.Option(False, "--json", help="Output as JSON"),
) -> None:
    """Replay a dead job."""
    _ensure_migrations()

    with get_conn() as conn:
        job = replay(conn, job_id=job_id)

    if not job:
        console.print(f"[red]Job {job_id} not found or not dead[/red]")
        raise typer.Exit(1)

    if output_json:
        console.print(json.dumps(_format_job(job), indent=2))
    else:
        console.print(f"[green]Job {job_id} replayed → pending[/green]")


@app.command(name="replay-all")
def replay_all_cmd(
    queue: str = typer.Option(None, "--queue", help="Queue to replay"),
    output_json: bool = typer.Option(False, "--json", help="Output as JSON"),
) -> None:
    """Replay all dead jobs in a queue."""
    _ensure_migrations()

    with get_conn() as conn:
        count = replay_all(conn, queue=queue)

    if output_json:
        console.print(json.dumps({"replayed": count}))
    else:
        console.print(f"[green]Replayed {count} jobs[/green]")


@app.command()
def worker(
    queues: str = typer.Option(
        settings.worker_queues, help="Comma-separated queue names"
    ),
) -> None:
    """Start a worker process."""
    import os

    os.environ["WORKER_QUEUES"] = queues
    # Re-import to pick up the env change
    from relay.worker import run_worker

    run_worker()


@app.command(name="workers")
def workers_cmd(
    output_json: bool = typer.Option(False, "--json", help="Output as JSON"),
) -> None:
    """List registered workers."""
    _ensure_migrations()

    with get_conn() as conn:
        worker_list = list_workers(conn)

    if output_json:
        console.print(json.dumps([_format_job(w) for w in worker_list], indent=2))
    else:
        table = Table(title="Workers")
        table.add_column("ID")
        table.add_column("PID", justify="right")
        table.add_column("Host")
        table.add_column("Last Seen")

        for w in worker_list:
            last_seen = w.get("last_seen")
            table.add_row(
                w["id"],
                str(w["pid"]),
                w["host"],
                last_seen.isoformat() if hasattr(last_seen, "isoformat") else str(last_seen or ""),
            )
        console.print(table)


@app.command(name="chaos")
def chaos_cmd() -> None:
    """Run the chaos test (Section 13.1)."""
    _ensure_migrations()
    import pytest
    console.print("[bold]Running chaos test...[/bold]")
    ret = pytest.main(["tests/chaos/test_chaos.py", "-v", "-s"])
    if ret != 0:
        raise typer.Exit(code=int(ret))


@app.command(name="bench")
def bench_cmd() -> None:
    """Run throughput and latency benchmarks (Section 13.4)."""
    _ensure_migrations()
    from bench.bench import main as run_bench
    console.print("[bold]Running benchmarks...[/bold]")
    run_bench()


if __name__ == "__main__":
    app()
