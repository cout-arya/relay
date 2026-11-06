-- Relay Job Queue: Initial Schema
-- All queue state lives in PostgreSQL. This is the single source of truth.

CREATE TABLE IF NOT EXISTS jobs (
    id               BIGSERIAL PRIMARY KEY,
    queue            TEXT NOT NULL DEFAULT 'default',
    type             TEXT NOT NULL,
    payload          JSONB NOT NULL DEFAULT '{}',
    priority         INT NOT NULL DEFAULT 0,          -- higher runs first
    status           TEXT NOT NULL DEFAULT 'pending'
                     CHECK (status IN ('pending','running','succeeded','dead','blocked')),
    run_at           TIMESTAMPTZ NOT NULL DEFAULT now(), -- scheduled / retry time
    attempts         INT NOT NULL DEFAULT 0,          -- doubles as fencing token
    max_attempts     INT NOT NULL DEFAULT 5,
    locked_by        TEXT,                            -- worker_id holding the lease
    locked_until     TIMESTAMPTZ,                     -- lease expiry
    last_error       TEXT,
    idempotency_key  TEXT,
    created_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
    started_at       TIMESTAMPTZ,
    finished_at      TIMESTAMPTZ
);

-- Claim index: find pending jobs ready to run, ordered by priority then run_at
CREATE INDEX IF NOT EXISTS jobs_claim_idx
    ON jobs (queue, priority DESC, run_at)
    WHERE status = 'pending';

-- Lease index: find running jobs with expired leases
CREATE INDEX IF NOT EXISTS jobs_lease_idx
    ON jobs (locked_until)
    WHERE status = 'running';

-- Idempotency: unique per queue when key is provided
CREATE UNIQUE INDEX IF NOT EXISTS jobs_idem_idx
    ON jobs (queue, idempotency_key)
    WHERE idempotency_key IS NOT NULL;

-- One row per attempt: audit trail and chaos-test evidence
CREATE TABLE IF NOT EXISTS job_attempts (
    id           BIGSERIAL PRIMARY KEY,
    job_id       BIGINT NOT NULL REFERENCES jobs(id),
    attempt_no   INT NOT NULL,
    worker_id    TEXT NOT NULL,
    started_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    finished_at  TIMESTAMPTZ,
    outcome      TEXT,   -- succeeded | failed | lease_lost | abandoned
    error        TEXT
);

-- Idempotent side effects: proves requirement 07
CREATE TABLE IF NOT EXISTS effects (
    job_id      BIGINT NOT NULL,
    step        TEXT NOT NULL,
    result      JSONB,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (job_id, step)
);

-- Per-queue configuration
CREATE TABLE IF NOT EXISTS queue_config (
    queue            TEXT PRIMARY KEY,
    max_concurrency  INT,            -- NULL means unlimited
    default_lease_s  INT NOT NULL DEFAULT 30
);

-- Worker registry
CREATE TABLE IF NOT EXISTS workers (
    id         TEXT PRIMARY KEY,
    pid        INT NOT NULL,
    host       TEXT NOT NULL,
    started_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    last_seen  TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- LLM usage tracking
CREATE TABLE IF NOT EXISTS llm_usage (
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
