# PRD: Relay, a Reliable Job Queue with Dead-Letter Recovery

*Product Requirements Document for an AI coding agent. Stack: Python, FastAPI, PostgreSQL, Docker Compose. Target build time: 7 days.*

## 0. Instructions for the AI coding agent (read first)

1. Build milestone by milestone (Section 18). Do not start the next milestone until the current Definition of Done passes its tests.
2. All queue state lives in PostgreSQL. Never use an in-memory list or dict as the queue.
3. Every state transition must be ONE atomic SQL statement. Never SELECT and then UPDATE in separate statements for claiming, acking or failing.
4. Every ack, fail and heartbeat must be fenced (Section 8.3), so a stale worker can never overwrite a newer attempt.
5. Use raw SQL through psycopg 3. No ORM. The SQL is the point of this project.
6. Python 3.12, full type hints, ruff for lint, pytest for tests. Keep dependencies to the list in Section 5. Ask before adding any other dependency.
7. Never describe the system as exactly-once. The honest claim is at-least-once delivery with idempotent effects.
8. Write tests alongside code. Reliability tests matter more than UI.
9. Keep code simple and readable. Add short comments explaining WHY on every concurrency-sensitive query.

## 1. Overview

**Relay** is a standalone background-job engine: the thing Celery and Sidekiq hide from you. Applications enqueue jobs through an HTTP API. Separate worker processes pull jobs from a durable PostgreSQL-backed queue, run them, retry failures with exponential backoff, and move jobs that keep failing into a dead-letter queue (DLQ) that humans can inspect and replay.

The central promise: **no job is lost, and no job runs concurrently on two workers, even when workers crash mid-task.**

### Why this project exists

Using a queue library teaches an API. Building one teaches the mechanics: leases, crash recovery, retry storms, idempotency, and what reliable really means. The project also doubles as a portfolio piece for backend and AI engineering roles, because real AI products depend on queues for batch embedding, document ingestion, agent runs and flaky LLM API calls.

## 2. Goals and non-goals

### Goals

- Durable queue that survives process crashes and database restarts.
- Priorities and delayed or scheduled execution.
- Automatic retries with exponential backoff and jitter, capped at a max attempt count.
- Dead-letter queue with inspect and replay.
- Lease (visibility timeout) pattern so a dead worker's job is safely reclaimed.
- Inspection through both HTTP endpoints and a CLI.
- Demonstrated idempotency: re-running a half-finished job does not double-apply side effects.
- Proof through automated chaos tests and published benchmarks.

### Non-goals

- Exactly-once delivery (impossible in general; we document this honestly).
- Multi-region or multi-database clustering.
- Authentication and multi-tenancy (optional later).
- Replacing Kafka or RabbitMQ. This is a Postgres-backed queue for moderate scale.

## 3. Users and use cases

| User | Need |
| --- | --- |
| Application developer | Enqueue work (email, PDF, LLM call) and return to the user immediately |
| Operator | See queue depth, in-flight jobs, failures; replay dead jobs after fixing a bug |
| Recruiter or interviewer | Watch the live demo: kill a worker mid-job and see zero loss |

Example jobs: send welcome email, resize image, generate invoice PDF, summarize a document with an LLM, process a payment webhook.

## 4. Delivery guarantees (state these precisely in the README)

- **At-least-once delivery.** A job that is claimed but not acknowledged becomes eligible again after its lease expires.
- **Mutual exclusion by lease.** At any moment, at most one worker holds a valid lease on a job.
- **Fencing.** A worker whose lease expired and was reassigned cannot successfully ack, fail or heartbeat the job (its UPDATE matches zero rows).
- **Caveat.** A stalled worker may still finish its side effects after losing the lease. That is why handlers MUST be idempotent. Relay provides the effects-table pattern (Section 12) to make this easy.
- **Ordering.** Best effort by priority then run time. Strict FIFO is not guaranteed once retries occur.

## 5. Tech stack

| Concern | Choice |
| --- | --- |
| Language | Python 3.12 |
| API | FastAPI plus uvicorn |
| DB driver | psycopg 3 with psycopg\_pool |
| Database | PostgreSQL 16 |
| CLI | Typer |
| Validation | pydantic v2 |
| Tests | pytest, pytest-timeout |
| Lint | ruff |
| Packaging | Docker Compose (db, api, worker services) |
| CI | GitHub Actions running pytest |

Migrations are plain numbered .sql files applied at startup by a tiny migration runner. No Alembic.

## 6. Architecture

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

   Reaper (inside each worker loop): moves exhausted expired jobs to dead.
```

Design principle: **PostgreSQL is the single source of truth and the only coordination mechanism.** Workers do not talk to each other.

## 7. Data model

```sql
CREATE TABLE jobs (
  id               BIGSERIAL PRIMARY KEY,
  queue            TEXT NOT NULL DEFAULT 'default',
  type             TEXT NOT NULL,
  payload          JSONB NOT NULL DEFAULT '{}',
  priority         INT NOT NULL DEFAULT 0,          -- higher runs first
  status           TEXT NOT NULL DEFAULT 'pending'
                   CHECK (status IN ('pending','running','succeeded','dead','blocked')),
  run_at           TIMESTAMPTZ NOT NULL DEFAULT now(), -- scheduled time and retry time
  attempts         INT NOT NULL DEFAULT 0,          -- also the fencing token
  max_attempts     INT NOT NULL DEFAULT 5,
  locked_by        TEXT,                            -- worker id holding the lease
  locked_until     TIMESTAMPTZ,                     -- lease expiry
  last_error       TEXT,
  idempotency_key  TEXT,
  created_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
  started_at       TIMESTAMPTZ,
  finished_at      TIMESTAMPTZ
);

CREATE INDEX jobs_claim_idx ON jobs (queue, priority DESC, run_at) WHERE status = 'pending';
CREATE INDEX jobs_lease_idx ON jobs (locked_until) WHERE status = 'running';
CREATE UNIQUE INDEX jobs_idem_idx ON jobs (queue, idempotency_key) WHERE idempotency_key IS NOT NULL;

-- One row per attempt: audit trail and chaos-test evidence
CREATE TABLE job_attempts (
  id           BIGSERIAL PRIMARY KEY,
  job_id       BIGINT NOT NULL REFERENCES jobs(id),
  attempt_no   INT NOT NULL,
  worker_id    TEXT NOT NULL,
  started_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
  finished_at  TIMESTAMPTZ,
  outcome      TEXT,   -- succeeded | failed | lease_lost | abandoned
  error        TEXT
);

-- Idempotent side effects: the demo of requirement 07
CREATE TABLE effects (
  job_id      BIGINT NOT NULL,
  step        TEXT NOT NULL,
  result      JSONB,
  created_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
  PRIMARY KEY (job_id, step)
);

CREATE TABLE queue_config (
  queue            TEXT PRIMARY KEY,
  max_concurrency  INT,            -- NULL means unlimited
  default_lease_s  INT NOT NULL DEFAULT 30
);

CREATE TABLE workers (
  id         TEXT PRIMARY KEY,
  pid        INT NOT NULL,
  host       TEXT NOT NULL,
  started_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  last_seen  TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE llm_usage (
  id             BIGSERIAL PRIMARY KEY,
  job_id         BIGINT NOT NULL,
  attempt_no     INT NOT NULL,
  provider       TEXT NOT NULL,
  model          TEXT NOT NULL,
  input_tokens   INT NOT NULL,
  output_tokens  INT NOT NULL,
  cost_usd       NUMERIC(12,6) NOT NULL,
  created_at     TIMESTAMPTZ NOT NULL DEFAULT now()
);
```

## 8. Core algorithms (the heart of the project)

### 8.1 Enqueue

Insert a row with the given queue, type, payload, priority, `run_at` (now plus optional delay) and `max_attempts`. If `idempotency_key` is supplied, use `INSERT ... ON CONFLICT DO NOTHING RETURNING *`; if nothing is returned, fetch and return the existing job (HTTP 200 instead of 201). This prevents duplicate enqueues from client retries.

### 8.2 Claim (atomic, one statement)

```sql
WITH next AS (
  SELECT id FROM jobs
  WHERE queue = ANY(%(queues)s)
    AND (
          (status = 'pending' AND run_at <= now())
       OR (status = 'running' AND locked_until < now() AND attempts < max_attempts)
    )
  ORDER BY priority DESC, run_at ASC, id ASC
  FOR UPDATE SKIP LOCKED
  LIMIT 1
)
UPDATE jobs j
SET status = 'running',
    locked_by = %(worker_id)s,
    locked_until = now() + make_interval(secs => %(lease_s)s),
    attempts = j.attempts + 1,
    started_at = now()
FROM next
WHERE j.id = next.id
RETURNING j.*;
```

Why: `FOR UPDATE SKIP LOCKED` lets many workers claim concurrently without blocking and without two workers getting the same row. Including expired `running` rows is what recovers a crashed worker's job. The returned `attempts` value is the worker's **fencing token** for this attempt. After a successful claim, insert a `job_attempts` row.

### 8.3 Heartbeat and fencing

While a handler runs, a background thread extends the lease every `lease_s / 3` seconds:

```sql
UPDATE jobs SET locked_until = now() + make_interval(secs => %(lease_s)s)
WHERE id = %(id)s AND status = 'running'
  AND locked_by = %(worker_id)s AND attempts = %(token)s;
```

If zero rows are updated, the lease was lost. The worker sets a cancellation flag, stops heartbeating, and records outcome `lease_lost`. It must not ack or fail.

### 8.4 Ack (success)

```sql
UPDATE jobs SET status = 'succeeded', finished_at = now(),
                locked_by = NULL, locked_until = NULL, last_error = NULL
WHERE id = %(id)s AND status = 'running'
  AND locked_by = %(worker_id)s AND attempts = %(token)s;
```

Zero rows means the result is discarded (fenced out). Log it.

### 8.5 Fail (retry or dead-letter)

One fenced statement decides the outcome:

```sql
UPDATE jobs SET
  status = CASE WHEN attempts >= max_attempts OR %(permanent)s THEN 'dead' ELSE 'pending' END,
  run_at = CASE WHEN attempts >= max_attempts OR %(permanent)s THEN run_at
                ELSE now() + make_interval(secs => %(delay_s)s) END,
  last_error = %(error)s,
  locked_by = NULL, locked_until = NULL,
  finished_at = CASE WHEN attempts >= max_attempts OR %(permanent)s THEN now() END
WHERE id = %(id)s AND status = 'running'
  AND locked_by = %(worker_id)s AND attempts = %(token)s;
```

Backoff is computed in Python: `delay = random.uniform(0, min(cap, base * 2 ** (attempts - 1)))` (full jitter). Defaults: base 2 seconds, cap 300 seconds. Jitter prevents retry storms where many failed jobs all retry at the same instant. A handler may raise `RetryLater(seconds)` to override the delay (used for rate limits) and `PermanentError` to skip retries.

### 8.6 Reaper

Crashed workers whose jobs have exhausted attempts would otherwise stay `running` forever (the claim query ignores them). Each worker loop periodically runs:

```sql
UPDATE jobs SET status = 'dead', finished_at = now(), locked_by = NULL, locked_until = NULL,
                last_error = coalesce(last_error, 'lease expired after max attempts')
WHERE status = 'running' AND locked_until < now() AND attempts >= max_attempts;
```

### 8.7 Replay from the DLQ

```sql
UPDATE jobs SET status = 'pending', attempts = 0, run_at = now(),
                last_error = NULL, finished_at = NULL
WHERE id = %(id)s AND status = 'dead';
```

### 8.8 Per-queue concurrency limit (signature feature)

When `queue_config.max_concurrency` is set, the claim must refuse if the queue already has that many `running` jobs with unexpired leases. Implement inside the claim transaction using `pg_advisory_xact_lock(hashtext(queue))` to serialize the count-then-claim step for limited queues only. Document the throughput trade-off.

## 9. Worker specification

- One job at a time per process. Scale by running more processes (simple, and ideal for SIGKILL chaos testing).
- Startup: generate `worker_id` (hostname-pid-random), register in `workers`, then loop.
- Loop: run reaper occasionally, claim a job, execute, ack or fail. If no job, sleep `poll_interval` (default 0.5 s) with small random jitter.
- Execution: look up handler by `job.type` in a registry (`@register('type_name')`). Run the handler while a heartbeat thread extends the lease.
- Exceptions: any exception triggers fail with backoff. `PermanentError` goes straight to dead. `RetryLater` uses the supplied delay. Unknown job type is a permanent failure.
- Graceful shutdown on SIGTERM: stop claiming, finish the current job, exit. SIGKILL must be safe by design (the lease handles it).
- Config through environment variables (Section 17).

## 10. HTTP API

| Method and path | Purpose |
| --- | --- |
| `POST /jobs` | Enqueue. Body: `queue`, `type`, `payload`, `priority`, `delay_seconds` or `run_at`, `max_attempts`, `idempotency_key`. Returns 201, or 200 with the existing job on idempotency match. |
| `GET /jobs/{id}` | Job details plus its attempt history |
| `GET /jobs` | List with filters `status`, `queue`, `limit`, `offset` |
| `GET /stats` | Depth by status and queue, in-flight jobs, oldest pending age (seconds), retry rate over the last 5 minutes, throughput over the last 1 minute, active workers |
| `GET /dead` | Dead-letter contents with error and attempts |
| `POST /dead/{id}/replay` | Replay one dead job |
| `POST /dead/replay-all?queue=` | Replay all dead jobs in a queue |
| `GET /queues`, `PUT /queues/{name}` | View and set concurrency limit and lease |
| `GET /workers` | Registered workers and last seen |
| `GET /llm/usage` | Token and cost totals by provider and job type |
| `GET /health` | Liveness and DB connectivity |
| `POST /demo/kill-worker` | **Only when `DEMO_MODE=1`.** SIGKILL a random registered worker on the same host, used by the dashboard demo |

All responses are JSON. Validation errors return 422 through pydantic. FastAPI auto docs at `/docs`.

## 11. CLI (Typer, command name `relay`)

`relay enqueue`, `relay stats`, `relay jobs --status running`, `relay show <id>`, `relay dead`, `relay replay <id>`, `relay replay-all --queue q`, `relay worker --queues default,llm`, `relay chaos` (runs the chaos test, Section 13.1), `relay bench` (benchmarks, Section 13.4). Output should be readable tables, with `--json` for scripting.

## 12. Built-in handlers

### 12.1 `noop` and `sleep`

For benchmarks and basic tests. `sleep` takes `payload.seconds`.

### 12.2 `multi_step_transfer` (idempotency demo, requirement 07)

A three-step job (for example reserve, charge, notify). Each step does:

```sql
INSERT INTO effects (job_id, step, result) VALUES (%s, %s, %s)
ON CONFLICT (job_id, step) DO NOTHING;
```

If the step already exists, it is skipped. A `fail_after_step` payload option and a random failure-injection rate let tests crash the job between steps. After any number of retries and crashes, `effects` contains exactly one row per (job, step). Count `job_attempts` to show how many redeliveries happened while duplicate effects stayed at zero.

### 12.3 `llm_task` (AI engineering angle)

Details in Section 13.2.

## 13. Signature features (what makes this resume-worthy)

### 13.1 Chaos test (MOST IMPORTANT)

A script and pytest test (`relay chaos`) that:

1. Starts the database and 5 worker processes.
2. Enqueues 10,000 `multi_step_transfer` jobs with random priorities and some delayed `run_at`.
3. Runs a chaos monkey that every 1 to 3 seconds sends SIGKILL to a random worker and starts a replacement.
4. Waits until every job is `succeeded` or intentionally `dead`.
5. Asserts: no job lost; `effects` has exactly 3 rows per succeeded job; no job ever had two simultaneous valid leases (check `job_attempts` overlap for `succeeded` outcomes); intentionally poisoned jobs ended in `dead`.
6. Prints a report: total jobs, workers killed, total attempts, redeliveries, duplicate effects prevented, total time.

Paste the report into the README. This single artifact is the proof of the project.

### 13.2 AI job handler (`llm_task`)

- Provider interface with a **MockProvider** (default, no API key needed: deterministic output, configurable error rate, simulated 429 responses with a retry-after value, fake token counts) and optional real providers selected by environment variable and API key.
- Handler behavior: call provider, on 429 raise `RetryLater(retry_after)`, on 5xx raise a normal retryable error, on invalid input raise `PermanentError`.
- Per-provider concurrency cap enforced through the per-queue limit (for example an `llm` queue limited to 4).
- Record every call in `llm_usage` with tokens and estimated cost. Expose totals at `GET /llm/usage` and on the dashboard.
- Demo: enqueue 200 summarization jobs against the mock with a 20 percent 429 rate and show all complete within the cap, with cost totals.

### 13.3 Per-queue concurrency limits

Implemented as in Section 8.8. Test: with limit 2 and 10 slow jobs, never more than 2 are running at once; other queues remain unaffected.

### 13.4 Benchmarks

`relay bench` measures, for 1, 5 and 10 workers running `noop` jobs: jobs per second, claim latency p50 and p99, and completion latency. Output a CSV and a PNG chart for the README. Record real numbers and be honest about the hardware. Indicative targets, not guarantees: aggregate throughput of several hundred jobs per second on a laptop and claim p99 under 50 ms at 10 workers.

## 14. Stretch goals

### 14.1 Job dependencies

Table `job_dependencies(job_id, depends_on_id)`. Jobs with unmet dependencies start as `blocked`. On ack of a job, in the same transaction, move any job whose dependencies are now all `succeeded` from `blocked` to `pending`. If a dependency goes `dead`, dependents stay blocked and are shown as such in stats. Reject cycles at enqueue time.

### 14.2 Live dashboard

A single static HTML page served by FastAPI that polls `/stats` every second: counters for pending, running, succeeded and dead; a progress bar; a workers list; a live dead-letter table with Replay buttons; LLM cost totals; and, in demo mode, a **Kill a worker** button calling `/demo/kill-worker`. No frontend framework needed. Watching a killed worker's job get rescued is the demo that non-technical viewers understand.

### 14.3 Other ideas

LISTEN/NOTIFY to cut idle polling, Prometheus `/metrics`, job timeouts, rate limiting per queue.

## 15. Testing strategy

Use a real PostgreSQL (docker or a CI service container). No mocking the database.

| Test | Asserts |
| --- | --- |
| Concurrent claim | 50 jobs, 10 concurrent claimers: every job claimed exactly once |
| Priority order | Higher priority claimed first; equal priority by run\_at |
| Delayed job | Not claimable before run\_at, claimable after |
| Lease reclaim | Claim, let lease expire without ack, another worker reclaims it |
| Fencing | After reclaim, the original worker's ack, fail and heartbeat each affect zero rows |
| Retry with backoff | Failure sets pending with run\_at in the future; attempts increments; delay within jitter bounds |
| Max attempts to DLQ | Always-failing job ends dead after exactly max\_attempts |
| Reaper | Expired running job at max attempts becomes dead |
| Replay | Dead job returns to pending with attempts reset and then succeeds when fixed |
| Idempotent enqueue | Same idempotency\_key returns the same job |
| Idempotent effects | Crash between steps, retry, exactly one effect per step |
| Concurrency limit | Never more than N running in a limited queue |
| API and CLI | Smoke tests for each endpoint and command |
| Chaos | Section 13.1 |

Use short lease times in tests (1 to 2 seconds) and pytest-timeout so hangs fail fast.

## 16. Non-functional requirements

- **Durability:** an acknowledged enqueue is committed to PostgreSQL before the API returns.
- **Recoverability:** killing any worker, the API, or restarting the database loses no committed job.
- **Observability:** structured JSON logs including job\_id, worker\_id, attempt and event; `/stats` reflects reality within one second.
- **Simplicity:** one command (`docker compose up`) runs db, api and two workers.
- **Security basics:** parameterized queries only; payload size limit (for example 256 KB); demo endpoint disabled unless `DEMO_MODE=1`.

## 17. Repository structure and configuration

```
relay/
  docker-compose.yml
  Dockerfile
  pyproject.toml
  README.md
  docs/design.md            # trade-offs and limitations
  docs/architecture.png
  migrations/001_init.sql
  src/relay/
    config.py  db.py  migrate.py
    queue.py                 # enqueue, claim, heartbeat, ack, fail, reaper, replay
    worker.py  backoff.py  registry.py  errors.py
    handlers/ noop.py  transfer.py  llm.py
    providers/ mock.py  base.py
    api/ main.py  schemas.py  dashboard.html
    cli.py
  tests/
    conftest.py  test_claim.py  test_lease.py  test_retry.py
    test_dlq.py  test_idempotency.py  test_limits.py  test_api.py
    chaos/ test_chaos.py
  bench/ bench.py
  .github/workflows/ci.yml
```

Environment variables: `DATABASE_URL`, `LEASE_SECONDS` (default 30), `POLL_INTERVAL` (0.5), `BACKOFF_BASE` (2), `BACKOFF_CAP` (300), `MAX_ATTEMPTS_DEFAULT` (5), `WORKER_QUEUES` (default), `DEMO_MODE` (0), `LLM_PROVIDER` (mock), `LOG_LEVEL`.

## 18. Milestones (7 days) with Definition of Done

**Day 1, Foundations.** Repo, Docker Compose, migration runner, `jobs` table, enqueue (priority, delay, idempotency key), basic worker using the claim query. *Done when:* 3 workers process 100 jobs with no job processed twice (concurrent-claim test passes).

**Day 2, Reliability core.** Lease, heartbeat thread, expired-lease reclaim, fenced ack and fail, `job_attempts`. *Done when:* lease reclaim and fencing tests pass, and a manually SIGKILLed worker's job is picked up by another worker.

**Day 3, Retries and DLQ.** Backoff with full jitter, max attempts, dead status, reaper, replay, `RetryLater` and `PermanentError`. *Done when:* retry, max-attempts and replay tests pass.

**Day 4, Priorities, scheduling, idempotency.** Priority and delayed-run tests, `effects` table and `multi_step_transfer` handler with failure injection. *Done when:* the idempotent-effects test shows one effect per step despite injected crashes.

**Day 5, API, CLI and observability.** All endpoints in Section 10, the CLI in Section 11, structured logs, `/stats`. *Done when:* API and CLI smoke tests pass and `/stats` matches database truth.

**Day 6, Signature features.** Chaos test, `llm_task` with mock provider and usage tracking, per-queue concurrency limit, dashboard page with Kill a worker (demo mode). *Done when:* the chaos test passes on 10,000 jobs and the LLM demo completes under a rate-limited mock.

**Day 7, Proof and polish.** Benchmarks with chart, README, design doc, architecture diagram, GitHub Actions CI, a short GIF of the kill-a-worker demo. *Done when:* a fresh clone runs with `docker compose up` and CI is green.

If time runs short, cut in this order: dashboard, per-queue limits, benchmarks. Never cut the chaos test.

## 19. Requirements traceability (acceptance)

| # | Requirement | How it is satisfied | Proof |
| --- | --- | --- | --- |
| 01 | Durable queue, jobs survive crashes | PostgreSQL `jobs` table; leases | Chaos test, kill and restart tests |
| 02 | Priorities and scheduled execution | `priority`, `run_at`, claim ordering | Priority and delay tests |
| 03 | Retries with exponential backoff, capped | Section 8.5, `max_attempts` | Retry test |
| 04 | Dead-letter queue, inspect and replay | `dead` status, `/dead`, replay endpoints and CLI | DLQ and replay tests |
| 05 | Never two workers at once, even on crash | SKIP LOCKED, lease, fencing token | Concurrent-claim, lease and fencing tests |
| 06 | Inspect depth, in-flight, DLQ | `/stats`, `/dead`, `/jobs`, CLI | API and CLI tests |
| 07 | Idempotency demonstrated | `effects` table with composite primary key | Idempotent-effects test and chaos report |
| Stretch | Dependencies, live UI | Sections 14.1 and 14.2 | Optional tests, dashboard demo |

## 20. Deliverables, README and resume material

### README must contain

1. One-paragraph pitch and the 30-second GIF at the top.
2. Quick start: `docker compose up`, then enqueue a job with curl.
3. Architecture diagram and the delivery-guarantees section (Section 4), including the honest at-least-once explanation.
4. Chaos test report and benchmark chart with real numbers.
5. Design decisions and limitations: why Postgres, why SKIP LOCKED, why leases, what changes at 100x scale (Redis Streams, Kafka, partitioning, separate claim index tuning).
6. How to run tests.

### Resume bullets (replace with your real numbers)

- Built a durable job queue (Python, FastAPI, PostgreSQL) with lease-based delivery, fenced acknowledgements, exponential backoff with jitter and dead-letter replay.
- Verified zero lost or duplicated side effects across 10,000 jobs while randomly SIGKILLing workers; published chaos-test and benchmark results.
- Added an LLM job mode with rate-limit-aware retries, per-queue concurrency caps and token and cost tracking.

### Interview questions this project prepares you for

Why SKIP LOCKED and what breaks without it. What happens when a lease expires but the worker is still running. Why exactly-once is a myth and how idempotency compensates. How retry storms happen and why jitter helps. What would change at 100x scale.

## 21. Risks and mitigations

| Risk | Mitigation |
| --- | --- |
| Race conditions from split SELECT and UPDATE | Single-statement claim and fenced transitions only |
| Stale worker overwriting newer attempt | Fencing token (`attempts`) on every write |
| Retry storms | Full jitter and capped backoff |
| Hot polling load on Postgres | Poll interval with jitter; LISTEN/NOTIFY as stretch |
| Index bloat from many updates | Partial indexes; note autovacuum tuning in design doc |
| Scope creep into UI | Dashboard is stretch; reliability tests come first |
