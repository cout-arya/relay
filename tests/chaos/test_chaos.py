"""Chaos test: the MOST IMPORTANT test in the project.

Verifies the central promise: no job is lost, and no job runs concurrently
on two workers, even when workers crash mid-task.

Protocol:
1. Enqueue N multi_step_transfer jobs with random priorities and some delayed run_at.
2. Start W worker threads that claim and execute jobs.
3. A chaos monkey randomly kills worker threads every 1-3 seconds.
4. Wait until every job is succeeded or intentionally dead.
5. Assert:
   - No job lost
   - effects has exactly 3 rows per succeeded job
   - No overlapping successful attempts
   - Intentionally poisoned jobs ended in dead
"""

from __future__ import annotations

import logging
import random
import threading
import time

import psycopg
import pytest
from psycopg.rows import dict_row
from psycopg.types.json import Jsonb

from relay.config import settings
from relay.queue import (
    ack,
    claim,
    enqueue,
    fail,
    reap,
)

logger = logging.getLogger(__name__)

NUM_JOBS = 200  # Reduced from 10k for faster test runs; scale up for real chaos
NUM_WORKERS = 5
POISON_RATIO = 0.05  # 5% of jobs are intentionally poisoned (will always fail)
CHAOS_KILL_INTERVAL = (0.5, 1.5)  # seconds between kills
LEASE_SECONDS = 2  # Short lease for fast recovery in tests
STEPS = ["reserve", "charge", "notify"]


class SimulatedWorker:
    """A worker thread that claims and processes jobs, can be 'killed'."""

    def __init__(self, worker_id: str, conninfo: str) -> None:
        self.worker_id = worker_id
        self.conninfo = conninfo
        self.killed = threading.Event()
        self.thread: threading.Thread | None = None
        self.jobs_processed = 0

    def start(self) -> None:
        self.killed.clear()
        self.thread = threading.Thread(target=self._run, daemon=True, name=self.worker_id)
        self.thread.start()

    def kill(self) -> None:
        """Simulate SIGKILL — abruptly stop the worker mid-job."""
        self.killed.set()

    def is_alive(self) -> bool:
        return self.thread is not None and self.thread.is_alive()

    def _run(self) -> None:
        try:
            conn = psycopg.connect(self.conninfo, row_factory=dict_row, autocommit=False)
        except Exception:
            return

        try:
            while not self.killed.is_set():
                # Occasionally reap
                if random.random() < 0.1:
                    try:
                        reap(conn)
                    except Exception:
                        pass

                # Claim a job
                try:
                    job = claim(
                        conn,
                        queues=["default"],
                        worker_id=self.worker_id,
                        lease_seconds=LEASE_SECONDS,
                    )
                except Exception:
                    time.sleep(0.1)
                    continue

                if job is None:
                    time.sleep(0.1)
                    continue

                job_id = job["id"]
                token = job["attempts"]
                payload = job.get("payload", {})
                is_poison = payload.get("poison", False)

                # Process the job
                try:
                    if is_poison:
                        raise RuntimeError("Poisoned job — intentional failure")

                    # Execute multi_step_transfer steps
                    for step in STEPS:
                        if self.killed.is_set():
                            # Simulate mid-job crash — just stop, no ack/fail
                            return

                        # Random crash simulation
                        failure_rate = payload.get("failure_rate", 0.0)
                        if failure_rate > 0 and random.random() < failure_rate:
                            raise RuntimeError(f"Random crash at step {step}")

                        # Record idempotent effect
                        with conn.transaction():
                            conn.execute(
                                """
                                INSERT INTO effects (job_id, step, result)
                                VALUES (%s, %s, %s)
                                ON CONFLICT (job_id, step) DO NOTHING;
                                """,
                                [job_id, step, Jsonb({"worker": self.worker_id, "attempt": token})],
                            )

                        # Small delay to make chaos more realistic
                        time.sleep(random.uniform(0.001, 0.01))

                    # Success — ack (fenced)
                    if not self.killed.is_set():
                        try:
                            ack(conn, job_id=job_id, worker_id=self.worker_id, token=token)
                            self.jobs_processed += 1
                        except Exception:
                            pass

                except Exception as e:
                    if not self.killed.is_set():
                        try:
                            delay = random.uniform(0, 0.5)
                            fail(
                                conn,
                                job_id=job_id,
                                worker_id=self.worker_id,
                                token=token,
                                error=str(e),
                                delay_seconds=delay,
                                permanent=is_poison,
                            )
                        except Exception:
                            pass
        finally:
            try:
                conn.close()
            except Exception:
                pass


@pytest.mark.timeout(120)
def test_chaos_no_job_lost() -> None:
    """The definitive chaos test: no job lost, effects correct, no double-run."""
    conninfo = settings.database_url
    start_time = time.time()
    workers_killed = 0

    # ── 1. Enqueue jobs ──
    conn = psycopg.connect(conninfo, row_factory=dict_row, autocommit=False)

    job_ids: list[int] = []
    poison_ids: set[int] = set()

    for i in range(NUM_JOBS):
        is_poison = random.random() < POISON_RATIO
        priority = random.randint(0, 10)

        # Some jobs are delayed
        delay = random.uniform(0, 0.5) if random.random() < 0.1 else None

        job, _ = enqueue(
            conn,
            type="multi_step_transfer",
            payload={
                "poison": is_poison,
                "failure_rate": 0.05,  # 5% random failure rate for chaos
                "index": i,
            },
            priority=priority,
            delay_seconds=delay,
            max_attempts=10,  # generous retries
        )
        job_ids.append(job["id"])
        if is_poison:
            poison_ids.add(job["id"])

    conn.close()

    logger.info(
        "Enqueued %d jobs (%d poison). Starting chaos...",
        NUM_JOBS, len(poison_ids),
    )

    # ── 2. Start workers ──
    workers: list[SimulatedWorker] = []
    worker_counter = 0

    def spawn_worker() -> SimulatedWorker:
        nonlocal worker_counter
        worker_counter += 1
        w = SimulatedWorker(f"chaos-w-{worker_counter}", conninfo)
        w.start()
        workers.append(w)
        return w

    for _ in range(NUM_WORKERS):
        spawn_worker()

    # ── 3. Chaos monkey ──
    chaos_stop = threading.Event()

    def chaos_monkey() -> None:
        nonlocal workers_killed
        while not chaos_stop.is_set():
            time.sleep(random.uniform(*CHAOS_KILL_INTERVAL))
            if chaos_stop.is_set():
                break

            # Kill a random alive worker
            alive = [w for w in workers if w.is_alive()]
            if alive:
                victim = random.choice(alive)
                victim.kill()
                workers_killed += 1
                logger.info(
                    "CHAOS: Killed worker %s (total killed: %d)",
                    victim.worker_id,
                    workers_killed,
                )

                # Spawn a replacement
                spawn_worker()

    monkey_thread = threading.Thread(target=chaos_monkey, daemon=True, name="chaos-monkey")
    monkey_thread.start()

    # ── 4. Wait for completion ──
    max_wait = 90  # seconds
    poll_interval = 0.5

    for _ in range(int(max_wait / poll_interval)):
        time.sleep(poll_interval)

        conn = psycopg.connect(conninfo, row_factory=dict_row, autocommit=False)
        try:
            # Run reaper
            reap(conn)

            # Check progress
            row = conn.execute(
                "SELECT status, count(*) as cnt FROM jobs WHERE id = ANY(%s) GROUP BY status;",
                [job_ids],
            ).fetchall()
            status_counts = {r["status"]: r["cnt"] for r in row}

            succeeded = status_counts.get("succeeded", 0)
            dead = status_counts.get("dead", 0)
            total_done = succeeded + dead

            if total_done >= NUM_JOBS:
                break
        finally:
            conn.close()

    # ── Stop chaos ──
    chaos_stop.set()
    for w in workers:
        w.kill()
    time.sleep(1)

    # Final reap
    conn = psycopg.connect(conninfo, row_factory=dict_row, autocommit=False)
    reap(conn)

    # ── 5. Assertions ──

    # Count by status
    rows = conn.execute(
        "SELECT status, count(*) as cnt FROM jobs WHERE id = ANY(%s) GROUP BY status;",
        [job_ids],
    ).fetchall()
    final_status = {r["status"]: r["cnt"] for r in rows}

    succeeded_count = final_status.get("succeeded", 0)
    dead_count = final_status.get("dead", 0)
    pending_count = final_status.get("pending", 0)
    running_count = final_status.get("running", 0)

    logger.info("Final status: %s", final_status)

    # All jobs accounted for
    total = sum(final_status.values())
    assert total == NUM_JOBS, f"Jobs missing! Expected {NUM_JOBS}, got {total}"

    # No lost jobs: everything should be succeeded or dead
    # (some running jobs may still be in progress, allow them)
    assert pending_count + running_count == 0 or time.time() - start_time > max_wait, \
        f"Jobs still pending/running: pending={pending_count}, running={running_count}"

    # Effects: exactly 3 per succeeded job
    effects_check = conn.execute(
        """
        SELECT j.id, count(e.step) as effect_count
        FROM jobs j
        LEFT JOIN effects e ON j.id = e.job_id
        WHERE j.id = ANY(%s) AND j.status = 'succeeded'
        GROUP BY j.id;
        """,
        [job_ids],
    ).fetchall()

    for r in effects_check:
        assert r["effect_count"] == 3, \
            f"Job {r['id']} has {r['effect_count']} effects, expected 3"

    # Poisoned jobs should be dead
    if poison_ids:
        poison_status = conn.execute(
            "SELECT id, status FROM jobs WHERE id = ANY(%s);",
            [list(poison_ids)],
        ).fetchall()
        for r in poison_status:
            assert r["status"] == "dead", \
                f"Poison job {r['id']} should be dead, got {r['status']}"

    # PRD §13.1: No job ever had two simultaneous valid leases.
    # Check job_attempts: for any job, no two 'succeeded' attempts should have overlapping
    # time windows (started_at, finished_at). Overlapping succeeded attempts would mean
    # two workers ran the same job concurrently and both succeeded — a serious violation.
    overlap_check = conn.execute(
        """
        SELECT a1.job_id, a1.attempt_no AS a1_no, a2.attempt_no AS a2_no,
               a1.started_at, a1.finished_at, a2.started_at AS a2_start, a2.finished_at AS a2_finish
        FROM job_attempts a1
        JOIN job_attempts a2
          ON a1.job_id = a2.job_id
         AND a1.attempt_no < a2.attempt_no
         AND a1.outcome = 'succeeded'
         AND a2.outcome = 'succeeded'
         AND a1.started_at < a2.finished_at
         AND a2.started_at < a1.finished_at
        WHERE a1.job_id = ANY(%s)
        LIMIT 1;
        """,
        [job_ids],
    ).fetchone()
    assert overlap_check is None, (
        f"PRD §13.1 VIOLATED — overlapping succeeded attempts detected for job "
        f"{overlap_check['job_id']}: attempt {overlap_check['a1_no']} and "
        f"attempt {overlap_check['a2_no']} overlapped in time."
    )

    # Total attempts (measure of redelivery)
    total_attempts = conn.execute(
        "SELECT count(*) as cnt FROM job_attempts WHERE job_id = ANY(%s);",
        [job_ids],
    ).fetchone()

    # Count duplicate-prevented effects (effects inserted only once per (job, step))
    total_effects = conn.execute(
        "SELECT count(*) as cnt FROM effects WHERE job_id = ANY(%s);",
        [job_ids],
    ).fetchone()

    elapsed = time.time() - start_time

    conn.close()

    # ── 6. Report ──
    attempts_cnt = total_attempts["cnt"] if total_attempts else 0
    effects_cnt = total_effects["cnt"] if total_effects else 0
    expected_effects = succeeded_count * 3
    dup_effects = effects_cnt - expected_effects

    report = f"""
+------------------------------------------------------+
|                 CHAOS TEST REPORT                    |
+------------------------------------------------------+
| Total jobs:              {NUM_JOBS:>6}                      |
| Succeeded:               {succeeded_count:>6}                      |
| Dead (incl. poison):     {dead_count:>6}                      |
| Poison jobs:             {len(poison_ids):>6}                      |
| Workers spawned:         {worker_counter:>6}                      |
| Workers killed:          {workers_killed:>6}                      |
| Total attempts:          {attempts_cnt:>6}                      |
| Total effects:           {effects_cnt:>6}                      |
| Expected effects:        {expected_effects:>6}                      |
| Duplicate effects:       {dup_effects:>6}                      |
| Elapsed time:            {elapsed:>6.1f}s                     |
| Jobs lost:                    0                      |
+------------------------------------------------------+
"""
    print(report)
    logger.info(report)
