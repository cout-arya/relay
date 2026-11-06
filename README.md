# Relay ⚡

**A reliable PostgreSQL-backed job queue with dead-letter recovery.**

Relay is a standalone background-job engine: the thing Celery and Sidekiq hide from you. Applications enqueue jobs through an HTTP API. Separate worker processes pull jobs from a durable PostgreSQL-backed queue, run them, retry failures with exponential backoff, and move jobs that keep failing into a dead-letter queue (DLQ) that humans can inspect and replay.

**The central promise: no job is lost, and no job runs concurrently on two workers, even when workers crash mid-task.**

## Quick Start

```bash
# Start everything: PostgreSQL, API server, and 2 workers
docker compose up --build

# Enqueue a job
curl -X POST http://localhost:8000/jobs \
  -H "Content-Type: application/json" \
  -d '{"type": "noop", "payload": {}}'

# Check stats
curl http://localhost:8000/stats

# View the live dashboard
open http://localhost:8000/dashboard
```

## Architecture

```
  Producers (apps, curl, CLI)
            |
            v
   +-----------------+        +----------------------+
   |  FastAPI (API)  | -----> |     PostgreSQL       |
   |  enqueue/admin  |        |  jobs, job_attempts, |
   +-----------------+        |  effects, workers,   |
                              |  queue_config, ...   |
   +-----------------+        +----------------------+
   | Worker process  | <----- claim (SKIP LOCKED)
   |  claim loop     | -----> heartbeat / ack / fail
   |  heartbeat thr. |
   |  handler runner |
   +-----------------+   (N independent worker processes)

   Reaper: moves exhausted expired jobs to dead.
```

**Design principle:** PostgreSQL is the single source of truth and the only coordination mechanism. Workers do not talk to each other.

## Delivery Guarantees

- **At-least-once delivery.** A job that is claimed but not acknowledged becomes eligible again after its lease expires.
- **Mutual exclusion by lease.** At any moment, at most one worker holds a valid lease on a job.
- **Fencing.** A worker whose lease expired and was reassigned cannot successfully ack, fail, or heartbeat the job (its UPDATE matches zero rows).
- **Caveat.** A stalled worker may still finish its side effects after losing the lease. That is why handlers MUST be idempotent. Relay provides the effects-table pattern to make this easy.
- **Ordering.** Best effort by priority then run time. Strict FIFO is not guaranteed once retries occur.

## Features

| Feature | Description |
|---------|-------------|
| **Durable queue** | All state in PostgreSQL — survives crashes and restarts |
| **Priorities** | Higher priority jobs run first |
| **Scheduled execution** | `delay_seconds` or `run_at` for deferred jobs |
| **Exponential backoff** | Full jitter prevents retry storms |
| **Dead-letter queue** | Failed jobs are inspectable and replayable |
| **Lease-based recovery** | Crashed worker's job is automatically reclaimed |
| **Fenced operations** | Stale workers can never overwrite newer attempts |
| **Idempotency** | `idempotency_key` prevents duplicate enqueues; `effects` table prevents duplicate side effects |
| **Per-queue concurrency** | Configurable max concurrent jobs per queue |
| **LLM job handler** | Rate-limit-aware retries, token/cost tracking |
| **Live dashboard** | Real-time stats, DLQ management, kill-a-worker demo |

## Chaos Test Verification 🐒

The litmus test for Relay is surviving random `SIGKILL` signals mid-job without dropping any job or double-applying side effects.

Run the chaos test with:
```bash
pytest tests/chaos/ -v -s
# or via CLI:
relay chaos
```

Actual verified execution results:
```
+------------------------------------------------------+
|                 CHAOS TEST REPORT                    |
+------------------------------------------------------+
| Total jobs:                 200                      |
| Succeeded:                  194                      |
| Dead (incl. poison):          6                      |
| Poison jobs:                  6                      |
| Workers spawned:              6                      |
| Workers killed:               1                      |
| Total attempts:             232                      |
| Total effects:              582                      |
| Expected effects:           582                      |
| Duplicate effects:            0                      |
| Elapsed time:               3.2s                     |
| Jobs lost:                    0                      |
+------------------------------------------------------+
```
- **Zero jobs lost**: All jobs accounted for.
- **Zero duplicate side effects**: 582 side effects recorded across 194 succeeded jobs (exactly 3 per job via the `effects` table), even with worker crashes and 232 claim attempts.

## Benchmarks 📊

Run the benchmarks using `bench/bench.py` or `relay bench`:

```bash
python bench/bench.py
```

### Benchmark Results (500 jobs per run)

| Workers | Jobs/s | Claim p50 (ms) | Claim p99 (ms) | Completion p50 (ms) | Completion p99 (ms) |
|:-------:|:------:|:--------------:|:--------------:|:-------------------:|:-------------------:|
|    1    | 333.3  |      0.0       |      16.0      |         0.0         |        16.0         |
|    5    | 666.7  |      0.0       |      16.0      |         0.0         |        16.0         |
|   10    | 582.1  |     15.0       |      16.0      |        16.0         |        31.0         |

![Relay Benchmark Chart](bench/benchmark.svg)

## API Endpoints

| Method | Path | Purpose |
|--------|------|---------|
| `POST` | `/jobs` | Enqueue a job |
| `GET` | `/jobs/{id}` | Job details + attempts |
| `GET` | `/jobs` | List with filters |
| `GET` | `/stats` | Queue depth, rates, workers |
| `GET` | `/dead` | Dead-letter contents |
| `POST` | `/dead/{id}/replay` | Replay one dead job |
| `POST` | `/dead/replay-all` | Replay all dead jobs |
| `GET` | `/queues` | Queue configurations |
| `PUT` | `/queues/{name}` | Set concurrency/lease |
| `GET` | `/workers` | Registered workers |
| `GET` | `/llm/usage` | LLM token/cost totals |
| `GET` | `/health` | Liveness check |
| `GET` | `/dashboard` | Live dashboard |
| `POST` | `/demo/kill-worker` | Kill a worker (demo mode) |

## CLI

```bash
relay enqueue noop                          # Enqueue a noop job
relay enqueue sleep --payload '{"seconds":5}'  # Enqueue a sleep job
relay stats                                 # View queue statistics
relay jobs --status running                 # List running jobs
relay show 42                               # Job details
relay dead                                  # View dead-letter queue
relay replay 42                             # Replay a dead job
relay replay-all --queue default            # Replay all dead jobs
relay workers                               # List active workers
relay worker --queues default,llm           # Start a worker
relay chaos                                 # Run the chaos test
relay bench                                 # Run benchmarks
```

## Running Tests

```bash
# Start PostgreSQL
docker compose up db -d

# Set DATABASE_URL
export DATABASE_URL=postgresql://relay:relay@localhost:5432/relay

# Install dev dependencies
pip install -e ".[dev]"

# Run all tests
pytest tests/ -v --timeout=60

# Run chaos test
pytest tests/chaos/ -v --timeout=120 -s

# Run benchmarks
python bench/bench.py
```

## Configuration

All settings are read from environment variables (see `src/relay/config.py`):

| Variable | Default | Description |
|----------|---------|-------------|
| `DATABASE_URL` | *(required)* | PostgreSQL connection string |
| `MAX_ATTEMPTS_DEFAULT` | `5` | Default max retry attempts per job |
| `LEASE_SECONDS_DEFAULT` | `30` | Default worker lease duration in seconds |
| `MAX_PAYLOAD_BYTES` | `262144` | Maximum job payload size (256 KB) |
| `LOG_LEVEL` | `INFO` | Logging level (`DEBUG`, `INFO`, `WARNING`, `ERROR`) |
| `LOG_FORMAT` | `text` | Log format: `text` for human-readable, `json` for structured JSON lines |

### Structured JSON Logging

Set `LOG_FORMAT=json` to emit newline-delimited JSON log records — useful for log aggregation pipelines (Datadog, Cloud Logging, Loki, etc.):

```bash
LOG_FORMAT=json relay worker --queues default
# {"timestamp": "2026-10-29T10:00:00.123Z", "level": "INFO", "logger": "relay.worker",
#  "message": "claimed job", "job_id": 42, "worker_id": "w-abc", "attempt": 1}
```

Each log record includes `timestamp` (ISO 8601 UTC), `level`, `logger`, `message`, and optional structured fields `job_id`, `worker_id`, `attempt`, and `event`.

## Design Decisions

### Why PostgreSQL?
A Postgres-backed queue is simpler to operate than a dedicated message broker. For moderate scale (hundreds of jobs/second), it provides durability, ACID transactions, and familiar tooling without adding another infrastructure dependency.

### Why `SKIP LOCKED`?
`FOR UPDATE SKIP LOCKED` lets many workers claim concurrently without blocking. Without it, workers would serialize on the same row lock, destroying throughput.

### Why leases?
A worker might crash silently (SIGKILL, OOM, network partition). Without leases, its job would be stuck as "running" forever. With a time-bounded lease, the job automatically becomes eligible for reclaim after the lease expires.

### Why fencing tokens?
When a lease expires and the job is reclaimed, the original worker might still be alive and finish its work. The fencing token (`attempts` counter) ensures the stale worker's ack/fail/heartbeat affects zero rows — the database rejects it because the attempt number no longer matches.

### What changes at 100× scale?
- Redis Streams or Kafka for higher throughput
- Partitioned claims by queue to reduce index contention
- Separate claim index tuning and autovacuum configuration
- LISTEN/NOTIFY to replace polling

## Tech Stack

| Concern | Choice |
|---------|--------|
| Language | Python 3.12 |
| API | FastAPI + uvicorn |
| DB driver | psycopg 3 + psycopg_pool |
| Database | PostgreSQL 16 |
| CLI | Typer |
| Validation | pydantic v2 |
| Tests | pytest, pytest-timeout |
| Lint | ruff |
| Packaging | Docker Compose |
| CI | GitHub Actions |

## License

MIT
