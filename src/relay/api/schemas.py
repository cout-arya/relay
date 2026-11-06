"""Pydantic request/response schemas for the Relay API."""

from __future__ import annotations

from datetime import datetime
from typing import Any

from pydantic import BaseModel, Field

# ─── Request Schemas ────────────────────────────────────────────────────────


class EnqueueRequest(BaseModel):
    """Request body for POST /jobs."""

    queue: str = "default"
    type: str
    payload: dict[str, Any] = Field(default_factory=dict)
    priority: int = 0
    delay_seconds: float | None = None
    run_at: datetime | None = None
    max_attempts: int | None = None
    idempotency_key: str | None = None
    depends_on: list[int] | None = None


class QueueConfigRequest(BaseModel):
    """Request body for PUT /queues/{name}."""

    max_concurrency: int | None = None
    default_lease_s: int | None = None


# ─── Response Schemas ───────────────────────────────────────────────────────


class JobResponse(BaseModel):
    """Response for a single job."""

    id: int
    queue: str
    type: str
    payload: dict[str, Any]
    priority: int
    status: str
    run_at: datetime | None = None
    attempts: int
    max_attempts: int
    locked_by: str | None = None
    locked_until: datetime | None = None
    last_error: str | None = None
    idempotency_key: str | None = None
    created_at: datetime | None = None
    started_at: datetime | None = None
    finished_at: datetime | None = None
    depends_on: list[int] = Field(default_factory=list)

    model_config = {"from_attributes": True}


class JobDetailResponse(BaseModel):
    """Response for GET /jobs/{id} — job plus attempt history."""

    job: JobResponse
    attempts: list[dict[str, Any]] = Field(default_factory=list)


class StatsResponse(BaseModel):
    """Response for GET /stats."""

    by_status: dict[str, int] = Field(default_factory=dict)
    by_queue: dict[str, dict[str, int]] = Field(default_factory=dict)
    in_flight: int = 0
    oldest_pending_age_seconds: float | None = None
    retry_rate_5m: int = 0
    throughput_1m: int = 0
    active_workers: int = 0


class HealthResponse(BaseModel):
    """Response for GET /health."""

    status: str
    database: str
