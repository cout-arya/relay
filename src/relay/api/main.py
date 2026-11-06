"""FastAPI application — all HTTP endpoints for Relay."""

from __future__ import annotations

import logging
import os
import random
import signal
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Any

from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import HTMLResponse, JSONResponse

from relay.api.schemas import (
    EnqueueRequest,
    JobResponse,
    QueueConfigRequest,
)
from relay.config import settings
from relay.db import close_pool, get_conn
from relay.logging_config import configure_logging
from relay.migrate import run_migrations
from relay.queue import (
    enqueue,
    get_job,
    get_job_attempts,
    get_stats,
    list_dead,
    list_jobs,
    list_queue_configs,
    list_workers,
    replay,
    replay_all,
    upsert_queue_config,
)

logger = logging.getLogger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI):  # type: ignore[no-untyped-def]
    """Run migrations on startup, close pool on shutdown."""
    configure_logging(level=settings.log_level, log_format=settings.log_format)
    logger.info("Running migrations...")
    run_migrations()
    logger.info("API ready")
    yield
    close_pool()


app = FastAPI(
    title="Relay",
    description="A reliable PostgreSQL-backed job queue with dead-letter recovery",
    version="0.1.0",
    lifespan=lifespan,
)


# ─── Jobs ────────────────────────────────────────────────────────────────────


@app.post("/jobs", response_model=JobResponse, status_code=201)
def create_job(req: EnqueueRequest) -> Any:
    """Enqueue a new job. Returns 201 for new jobs, 200 for idempotent matches."""
    # Payload size limit: 256 KB
    import json  # noqa: PLC0415

    payload_str = json.dumps(req.payload)
    if len(payload_str) > 256 * 1024:
        raise HTTPException(status_code=413, detail="Payload exceeds 256 KB limit")

    try:
        with get_conn() as conn:
            job, created = enqueue(
                conn,
                queue=req.queue,
                type=req.type,
                payload=req.payload,
                priority=req.priority,
                delay_seconds=req.delay_seconds,
                run_at=req.run_at,
                max_attempts=req.max_attempts,
                idempotency_key=req.idempotency_key,
                depends_on=req.depends_on,
            )
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))

    if not created:
        return JSONResponse(content=_serialize_job(job), status_code=200)
    return JSONResponse(content=_serialize_job(job), status_code=201)


@app.get("/jobs/{job_id}")
def get_job_detail(job_id: int) -> Any:
    """Get job details plus its attempt history."""
    with get_conn() as conn:
        job = get_job(conn, job_id=job_id)
        if not job:
            raise HTTPException(status_code=404, detail="Job not found")
        attempts = get_job_attempts(conn, job_id=job_id)

    return {
        "job": _serialize_job(job),
        "attempts": [_serialize_dict(a) for a in attempts],
    }


@app.get("/jobs")
def list_jobs_endpoint(
    status: str | None = None,
    queue: str | None = None,
    limit: int = Query(default=50, le=200),
    offset: int = Query(default=0, ge=0),
) -> Any:
    """List jobs with optional filters."""
    with get_conn() as conn:
        jobs = list_jobs(conn, status=status, queue=queue, limit=limit, offset=offset)
    return [_serialize_job(j) for j in jobs]


# ─── Stats ───────────────────────────────────────────────────────────────────


@app.get("/stats")
def stats_endpoint() -> Any:
    """Queue statistics: depth by status/queue, in-flight, rates, workers."""
    with get_conn() as conn:
        return get_stats(conn)


# ─── Dead-Letter Queue ──────────────────────────────────────────────────────


@app.get("/dead")
def dead_endpoint(
    queue: str | None = None,
    limit: int = Query(default=50, le=200),
    offset: int = Query(default=0, ge=0),
) -> Any:
    """List dead-letter queue contents."""
    with get_conn() as conn:
        dead = list_dead(conn, queue=queue, limit=limit, offset=offset)
    return [_serialize_job(j) for j in dead]


@app.post("/dead/{job_id}/replay")
def replay_job(job_id: int) -> Any:
    """Replay one dead job."""
    with get_conn() as conn:
        job = replay(conn, job_id=job_id)
    if not job:
        raise HTTPException(status_code=404, detail="Job not found or not dead")
    return _serialize_job(job)


@app.post("/dead/replay-all")
def replay_all_endpoint(queue: str | None = None) -> Any:
    """Replay all dead jobs, optionally filtered by queue."""
    with get_conn() as conn:
        count = replay_all(conn, queue=queue)
    return {"replayed": count}


# ─── Queue Config ────────────────────────────────────────────────────────────


@app.get("/queues")
def list_queues() -> Any:
    """List all queue configurations."""
    with get_conn() as conn:
        return list_queue_configs(conn)


@app.put("/queues/{name}")
def update_queue(name: str, req: QueueConfigRequest) -> Any:
    """Set concurrency limit and lease duration for a queue."""
    with get_conn() as conn:
        config = upsert_queue_config(
            conn,
            queue=name,
            max_concurrency=req.max_concurrency,
            default_lease_s=req.default_lease_s,
        )
    return config


# ─── Workers ─────────────────────────────────────────────────────────────────


@app.get("/workers")
def workers_endpoint() -> Any:
    """List registered workers and last seen timestamps."""
    with get_conn() as conn:
        workers = list_workers(conn)
    return [_serialize_dict(w) for w in workers]


# ─── LLM Usage ───────────────────────────────────────────────────────────────


@app.get("/llm/usage")
def llm_usage_endpoint() -> Any:
    """Token and cost totals by provider and job type."""
    with get_conn() as conn:
        # Totals
        totals = conn.execute(
            """
            SELECT coalesce(sum(input_tokens), 0) as total_input,
                   coalesce(sum(output_tokens), 0) as total_output,
                   coalesce(sum(cost_usd), 0) as total_cost
            FROM llm_usage;
            """
        ).fetchone()

        # By provider
        by_provider = conn.execute(
            """
            SELECT provider, sum(input_tokens) as input_tokens,
                   sum(output_tokens) as output_tokens,
                   sum(cost_usd) as cost_usd, count(*) as calls
            FROM llm_usage GROUP BY provider ORDER BY cost_usd DESC;
            """
        ).fetchall()

        # By job type (join with jobs table)
        by_type = conn.execute(
            """
            SELECT j.type, sum(u.input_tokens) as input_tokens,
                   sum(u.output_tokens) as output_tokens,
                   sum(u.cost_usd) as cost_usd, count(*) as calls
            FROM llm_usage u JOIN jobs j ON u.job_id = j.id
            GROUP BY j.type ORDER BY cost_usd DESC;
            """
        ).fetchall()

    return {
        "total_input_tokens": totals["total_input"],  # type: ignore[index]
        "total_output_tokens": totals["total_output"],  # type: ignore[index]
        "total_cost_usd": float(totals["total_cost"]),  # type: ignore[index]
        "by_provider": [_serialize_dict(dict(r)) for r in by_provider],
        "by_job_type": [_serialize_dict(dict(r)) for r in by_type],
    }


# ─── Health ──────────────────────────────────────────────────────────────────


@app.get("/health")
def health() -> Any:
    """Liveness and DB connectivity check."""
    try:
        with get_conn() as conn:
            conn.execute("SELECT 1;")
        return {"status": "ok", "database": "connected"}
    except Exception as e:
        return JSONResponse(
            status_code=503,
            content={"status": "error", "database": str(e)},
        )


# ─── Demo Mode ───────────────────────────────────────────────────────────────


@app.post("/demo/kill-worker")
def kill_worker() -> Any:
    """SIGKILL a random registered worker on the same host. Only when DEMO_MODE=1."""
    if not settings.demo_mode:
        raise HTTPException(status_code=403, detail="Demo mode is not enabled")

    with get_conn() as conn:
        workers = list_workers(conn)

    if not workers:
        raise HTTPException(status_code=404, detail="No workers registered")

    # Pick a random worker
    worker = random.choice(workers)
    pid = worker["pid"]

    try:
        os.kill(pid, signal.SIGKILL)
        return {"killed": worker["id"], "pid": pid}
    except ProcessLookupError:
        return {"error": f"Worker {worker['id']} (pid {pid}) not found"}
    except PermissionError:
        return {"error": f"Permission denied to kill pid {pid}"}


# ─── Dashboard ───────────────────────────────────────────────────────────────


@app.get("/dashboard", response_class=HTMLResponse)
def dashboard() -> Any:
    """Serve the live dashboard HTML page."""
    html_path = Path(__file__).parent / "dashboard.html"
    if html_path.exists():
        return HTMLResponse(content=html_path.read_text(encoding="utf-8"))
    raise HTTPException(status_code=404, detail="Dashboard not found")


# ─── Helpers ─────────────────────────────────────────────────────────────────


def _serialize_job(job: dict[str, Any]) -> dict[str, Any]:
    """Convert a job dict to JSON-serializable format."""
    return _serialize_dict(job)


def _serialize_dict(d: dict[str, Any]) -> dict[str, Any]:
    """Convert all datetime values to ISO format strings."""
    result = {}
    for k, v in d.items():
        if hasattr(v, "isoformat"):
            result[k] = v.isoformat()
        else:
            result[k] = v
    return result


# ─── Entry point ─────────────────────────────────────────────────────────────

if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=8000)
