# Relay Design Document

## Trade-offs and Limitations

### At-least-once, not exactly-once

Exactly-once delivery is impossible in a distributed system (a worker can crash after performing side effects but before acknowledging). Relay provides **at-least-once delivery** and makes idempotency easy through the `effects` table pattern: `INSERT ON CONFLICT DO NOTHING` ensures each side effect is recorded exactly once regardless of how many times the handler runs.

### Polling vs. LISTEN/NOTIFY

Workers currently poll the database every 0.5 seconds with jitter. This is simple and reliable but adds a small latency floor. `LISTEN/NOTIFY` would enable instant wakeup on enqueue, but adds complexity (connection management, missed notifications on reconnect). It remains a stretch goal.

### Single-row claim

Each worker claims one job at a time. This is intentionally simple: it makes SIGKILL testing straightforward (one job at risk per worker) and avoids complex batch-claim logic. The trade-off is higher claim overhead per job. At scale, batch claiming would improve throughput.

### Advisory locks for concurrency limits

Per-queue concurrency limits use `pg_advisory_xact_lock(hashtext(queue))` to serialize the count-then-claim step. This adds contention for limited queues but keeps unlimited queues completely unaffected. At extreme scale, this could be replaced with a separate counter table or Redis-based rate limiting.

### No ORM

All SQL is raw, deliberate, and visible. This is the point: understanding exactly what the database does on each state transition. The trade-off is more verbose code, but every query is easily auditable and optimizable.

### Index strategy

- `jobs_claim_idx`: Partial index on `(queue, priority DESC, run_at) WHERE status = 'pending'` — only indexes claimable rows, keeping the index small even with millions of completed jobs.
- `jobs_lease_idx`: Partial index on `(locked_until) WHERE status = 'running'` — enables fast lease reclaim without scanning all jobs.
- Autovacuum tuning is important in production: frequent status updates generate dead tuples.

### Payload size limit

Payloads are capped at 256 KB (enforced at the API level) to prevent JSONB bloat. For larger data, store it externally and pass a reference in the payload.

## What would change at 100× scale

1. **Replace polling with LISTEN/NOTIFY or Redis Streams** for sub-millisecond claim latency.
2. **Partition the jobs table** by queue or time range to reduce index contention.
3. **Batch claiming**: claim N jobs in one statement, process them concurrently within a worker process.
4. **Separate completed jobs** into an archive table to keep the active jobs table small.
5. **Connection pooling** with PgBouncer in transaction mode.
6. **Dedicated vacuum tuning** for the jobs table given its high UPDATE rate.
7. Consider **Kafka or Redis Streams** for truly high-throughput use cases where PostgreSQL becomes the bottleneck.
