"""Relay benchmarks: measures throughput and latency for noop jobs.

Runs benchmarks with 1, 5, and 10 simulated workers processing noop jobs.
Outputs CSV, prints markdown results, and generates a visual SVG chart.
"""

from __future__ import annotations

import csv
import logging
import sys
import threading
import time
from pathlib import Path
from typing import Any

import psycopg
from psycopg.rows import dict_row

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from relay.config import settings
from relay.migrate import run_migrations
from relay.queue import ack, claim, enqueue

logger = logging.getLogger(__name__)

NUM_JOBS = 500
WORKER_COUNTS = [1, 5, 10]
LEASE_SECONDS = 30


def run_benchmark(num_workers: int, conninfo: str) -> dict[str, Any]:
    """Run a benchmark with the given number of workers."""
    # Clean up
    conn = psycopg.connect(conninfo, row_factory=dict_row, autocommit=True)
    conn.execute("DELETE FROM effects;")
    conn.execute("DELETE FROM llm_usage;")
    conn.execute("DELETE FROM job_attempts;")
    conn.execute("DELETE FROM jobs;")
    conn.close()

    # Enqueue jobs
    conn = psycopg.connect(conninfo, row_factory=dict_row, autocommit=False)
    for i in range(NUM_JOBS):
        enqueue(conn, type="noop", payload={"index": i})
    conn.close()

    # Track latencies
    claim_latencies: list[float] = []
    completion_latencies: list[float] = []
    lock = threading.Lock()
    start_time = time.monotonic()

    def worker_fn(worker_id: str) -> None:
        try:
            c = psycopg.connect(conninfo, row_factory=dict_row, autocommit=False)
        except Exception:
            return

        try:
            while True:
                t0 = time.monotonic()
                job = claim(
                    c,
                    queues=["default"],
                    worker_id=worker_id,
                    lease_seconds=LEASE_SECONDS,
                )
                claim_time = time.monotonic() - t0

                if job is None:
                    break

                with lock:
                    claim_latencies.append(claim_time)

                # noop — immediate ack
                ack(c, job_id=job["id"], worker_id=worker_id, token=job["attempts"])

                completion_time = time.monotonic() - t0
                with lock:
                    completion_latencies.append(completion_time)
        finally:
            c.close()

    # Start workers
    threads = []
    for i in range(num_workers):
        t = threading.Thread(target=worker_fn, args=(f"bench-w-{i}",))
        threads.append(t)
        t.start()

    for t in threads:
        t.join()

    elapsed = time.monotonic() - start_time
    jps = NUM_JOBS / elapsed if elapsed > 0 else 0

    # Calculate percentiles
    claim_latencies.sort()
    completion_latencies.sort()

    def percentile(data: list[float], p: float) -> float:
        if not data:
            return 0.0
        k = int(len(data) * p / 100)
        return data[min(k, len(data) - 1)]

    return {
        "workers": num_workers,
        "jobs": NUM_JOBS,
        "elapsed_s": round(elapsed, 2),
        "jobs_per_second": round(jps, 1),
        "claim_p50_ms": round(percentile(claim_latencies, 50) * 1000, 2),
        "claim_p99_ms": round(percentile(claim_latencies, 99) * 1000, 2),
        "completion_p50_ms": round(percentile(completion_latencies, 50) * 1000, 2),
        "completion_p99_ms": round(percentile(completion_latencies, 99) * 1000, 2),
    }


def generate_svg_chart(results: list[dict[str, Any]], output_path: Path) -> None:
    """Generate an SVG bar and line chart visualizing benchmark results."""
    width = 720
    height = 360
    margin_left = 60
    margin_top = 50
    chart_w = 600
    chart_h = 240

    max_jps = max(r["jobs_per_second"] for r in results) * 1.25 or 100.0

    svg_parts = [
        f'<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 {width} {height}" '
        f'width="{width}" height="{height}" '
        'style="background:#0f111a; border-radius:12px; font-family:sans-serif;">',
        '<text x="360" y="30" text-anchor="middle" fill="#f0f0f5" font-size="18" '
        'font-weight="bold">Relay Throughput &amp; Claim Latency</text>',
        # Grid lines
        f'<line x1="{margin_left}" y1="{margin_top + chart_h}" '
        f'x2="{margin_left + chart_w}" y2="{margin_top + chart_h}" '
        'stroke="#2d3142" stroke-width="1.5" />',
        f'<line x1="{margin_left}" y1="{margin_top}" '
        f'x2="{margin_left + chart_w}" y2="{margin_top}" '
        'stroke="#2d3142" stroke-dasharray="4" />',
        f'<line x1="{margin_left}" y1="{margin_top + chart_h // 2}" '
        f'x2="{margin_left + chart_w}" y2="{margin_top + chart_h // 2}" '
        'stroke="#2d3142" stroke-dasharray="4" />',
        # Y axis label
        f'<text x="{margin_left - 10}" y="{margin_top + 5}" text-anchor="end" '
        f'fill="#8f93a2" font-size="11">{int(max_jps)}</text>',
        f'<text x="{margin_left - 10}" y="{margin_top + chart_h // 2 + 4}" text-anchor="end" '
        f'fill="#8f93a2" font-size="11">{int(max_jps / 2)}</text>',
        f'<text x="{margin_left - 10}" y="{margin_top + chart_h}" text-anchor="end" '
        'fill="#8f93a2" font-size="11">0</text>',
        f'<text x="20" y="{margin_top + chart_h // 2}" fill="#8f93a2" font-size="11" '
        f'transform="rotate(-90 20 {margin_top + chart_h // 2})" '
        'text-anchor="middle">Jobs / Second</text>',
    ]

    num_bars = len(results)
    slot_w = chart_w / num_bars
    bar_w = 64

    for i, r in enumerate(results):
        cx = margin_left + i * slot_w + slot_w / 2
        bar_h = (r["jobs_per_second"] / max_jps) * chart_h
        bar_y = margin_top + chart_h - bar_h

        # Throughput bar
        svg_parts.append(
            f'<rect x="{cx - bar_w / 2}" y="{bar_y}" width="{bar_w}" '
            f'height="{bar_h}" rx="6" fill="#7c5cff" />'
        )
        # Value on top of bar
        svg_parts.append(
            f'<text x="{cx}" y="{bar_y - 8}" text-anchor="middle" '
            f'fill="#c3b5ff" font-size="13" font-weight="bold">{r["jobs_per_second"]} j/s</text>'
        )
        # X-axis label
        w_label = f'{r["workers"]} Worker{"s" if r["workers"] > 1 else ""}'
        svg_parts.append(
            f'<text x="{cx}" y="{margin_top + chart_h + 24}" '
            f'text-anchor="middle" fill="#e0e0f0" font-size="13">{w_label}</text>'
        )
        # Latency footnote below
        svg_parts.append(
            f'<text x="{cx}" y="{margin_top + chart_h + 42}" '
            f'text-anchor="middle" fill="#8f93a2" font-size="11">'
            f'claim p99: {r["claim_p99_ms"]}ms</text>'
        )

    svg_parts.append('</svg>')
    output_path.write_text("\n".join(svg_parts), encoding="utf-8")


def main() -> None:
    """Run benchmarks and output results."""
    logging.basicConfig(level=logging.INFO)
    conninfo = settings.database_url

    print("Running migrations...")
    run_migrations(conninfo)

    results: list[dict[str, Any]] = []

    for num_workers in WORKER_COUNTS:
        print(f"\n{'='*60}")
        print(f"Benchmark: {num_workers} worker(s), {NUM_JOBS} noop jobs")
        print(f"{'='*60}")

        result = run_benchmark(num_workers, conninfo)
        results.append(result)

        print(f"  Elapsed:           {result['elapsed_s']}s")
        print(f"  Throughput:        {result['jobs_per_second']} jobs/s")
        p50 = result["claim_p50_ms"]
        p99 = result["claim_p99_ms"]
        print(f"  Claim latency:     p50={p50}ms  p99={p99}ms")
        comp_p50 = result["completion_p50_ms"]
        comp_p99 = result["completion_p99_ms"]
        print(f"  Completion:        p50={comp_p50}ms  p99={comp_p99}ms")

    # Write CSV
    bench_dir = Path(__file__).parent
    csv_path = bench_dir / "results.csv"
    with open(csv_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=results[0].keys())
        writer.writeheader()
        writer.writerows(results)

    print(f"\nResults saved to {csv_path}")

    # Write SVG chart
    svg_path = bench_dir / "benchmark.svg"
    generate_svg_chart(results, svg_path)
    print(f"Benchmark chart saved to {svg_path}")

    # Print markdown table for README
    print("\n### Benchmark Results\n")
    header = "| Workers | Jobs/s | Claim p50 (ms) | Claim p99 (ms) | Completion p50 (ms) |"
    print(f"{header} Completion p99 (ms) |")
    div = "|---------|--------|----------------|----------------|---------------------|"
    print(f"{div}---------------------|")
    for r in results:
        line = f"| {r['workers']} | {r['jobs_per_second']} | {r['claim_p50_ms']} | "
        line += f"{r['claim_p99_ms']} | {r['completion_p50_ms']} | {r['completion_p99_ms']} |"
        print(line)


if __name__ == "__main__":
    main()
