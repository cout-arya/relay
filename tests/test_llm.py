"""Tests for LLM job handler, MockProvider, and usage tracking."""

from __future__ import annotations

import pytest

from relay.errors import PermanentError, RetryLater
from relay.handlers.llm import handle_llm_task
from relay.providers.base import LLMResponse, get_provider
from relay.providers.mock import MockProvider
from relay.queue import enqueue


def test_mock_provider_deterministic_output() -> None:
    """MockProvider produces consistent output for the same prompt."""
    provider = MockProvider()
    resp1 = provider.complete(prompt="summarize this article about queues")
    resp2 = provider.complete(prompt="summarize this article about queues")

    assert resp1.status_code == 200
    assert resp1.text == resp2.text
    assert resp1.input_tokens > 0
    assert resp1.output_tokens > 0
    assert resp1.cost_usd > 0.0


def test_mock_provider_error_simulation() -> None:
    """MockProvider simulates 500 internal errors when configured."""
    provider = MockProvider(error_rate=1.0)
    resp = provider.complete(prompt="hello")
    assert resp.status_code == 500
    assert resp.error is not None


def test_mock_provider_rate_limit_simulation() -> None:
    """MockProvider simulates 429 rate limit with retry_after value."""
    provider = MockProvider(rate_limit_rate=1.0, retry_after=3.5)
    resp = provider.complete(prompt="hello")
    assert resp.status_code == 429
    assert resp.retry_after == 3.5
    assert "Rate limit" in (resp.error or "")


def test_llm_handler_missing_prompt_raises_permanent_error() -> None:
    """Missing prompt in payload is a permanent failure (no retries)."""
    with pytest.raises(PermanentError, match="Missing 'prompt'"):
        handle_llm_task(job_id=1, payload={}, ctx={"attempt": 1})


def test_llm_handler_rate_limit_raises_retry_later(monkeypatch) -> None:
    """429 response from provider triggers RetryLater with retry_after delay."""
    class RateLimitedProvider(MockProvider):
        def complete(self, *, prompt: str, model: str = "default") -> LLMResponse:
            return LLMResponse(status_code=429, error="rate limited", retry_after=7.0)

    monkeypatch.setattr("relay.handlers.llm.get_provider", lambda _: RateLimitedProvider())

    with pytest.raises(RetryLater) as exc_info:
        handle_llm_task(
            job_id=1,
            payload={"prompt": "test prompt"},
            ctx={"attempt": 1},
        )
    assert exc_info.value.seconds == 7.0


def test_llm_handler_server_error_raises_retryable_error(monkeypatch) -> None:
    """5xx response triggers standard retryable RuntimeError."""
    class FailingProvider(MockProvider):
        def complete(self, *, prompt: str, model: str = "default") -> LLMResponse:
            return LLMResponse(status_code=503, error="service unavailable")

    monkeypatch.setattr("relay.handlers.llm.get_provider", lambda _: FailingProvider())

    with pytest.raises(RuntimeError, match="503"):
        handle_llm_task(
            job_id=1,
            payload={"prompt": "test prompt"},
            ctx={"attempt": 1},
        )


def test_llm_handler_success_records_usage(conn) -> None:
    """Successful LLM completion records tokens and cost in llm_usage table."""
    job, _ = enqueue(conn, type="llm_task", payload={"prompt": "Explain leases in job queues"})

    ctx = {"attempt": 1, "worker_id": "test-w", "job_id": job["id"]}
    handle_llm_task(
        job_id=job["id"],
        payload={"prompt": "Explain leases in job queues", "model": "mock-test"},
        ctx=ctx,
    )

    row = conn.execute(
        "SELECT * FROM llm_usage WHERE job_id = %s;", [job["id"]]
    ).fetchone()

    assert row is not None
    assert row["provider"] == "mock"
    assert row["model"] == "mock-test"
    assert row["input_tokens"] > 0
    assert row["output_tokens"] > 0
    assert row["cost_usd"] > 0.0


def test_get_provider_lookup() -> None:
    """get_provider correctly retrieves registered mock provider."""
    provider = get_provider("mock")
    assert isinstance(provider, MockProvider)
    with pytest.raises(ValueError, match="Unknown LLM provider"):
        get_provider("nonexistent-provider")



@pytest.mark.timeout(60)
def test_llm_batch_with_rate_limiting(conn) -> None:
    """Enqueue 20 LLM jobs; MockProvider with 20% 429 rate; all eventually succeed (PRD §11).

    Verifies:
    - Rate-limited jobs are retried (via fail() with delay) until they succeed.
    - llm_usage rows are written for each successful attempt.
    - Total cost tracked is > 0.
    """
    from relay.queue import ack, claim, fail

    num_jobs = 20
    rate_limit_rate = 0.2

    # Enqueue jobs
    job_ids = []
    for i in range(num_jobs):
        job, _ = enqueue(
            conn,
            type="llm_task",
            queue="llm",
            payload={"prompt": f"Summarize document {i}"},
            max_attempts=20,  # enough retries to survive 20% rate limiting
        )
        job_ids.append(job["id"])

    # Process all jobs with a rate-limited mock provider
    provider = MockProvider(rate_limit_rate=rate_limit_rate, retry_after=0.0)
    succeeded = 0
    max_iterations = num_jobs * 30  # safety limit

    for _ in range(max_iterations):
        job = claim(conn, queues=["llm"], worker_id="llm-batch-w", lease_seconds=30)
        if job is None:
            break  # No more claimable jobs
        resp = provider.complete(prompt=job["payload"]["prompt"])
        if resp.status_code == 429:
            # Rate limited — re-queue with zero delay for test speed
            fail(
                conn,
                job_id=job["id"],
                worker_id="llm-batch-w",
                token=job["attempts"],
                error="rate limited",
                delay_seconds=0.0,
            )
        elif resp.status_code == 200:
            conn.execute(
                """
                INSERT INTO llm_usage (job_id, attempt_no, provider, model,
                    input_tokens, output_tokens, cost_usd)
                VALUES (%s, %s, 'mock', 'mock-test', %s, %s, %s);
                """,
                [
                    job["id"],
                    job["attempts"],
                    resp.input_tokens,
                    resp.output_tokens,
                    resp.cost_usd,
                ],
            )
            ack(conn, job_id=job["id"], worker_id="llm-batch-w", token=job["attempts"])
            succeeded += 1
        else:
            fail(
                conn,
                job_id=job["id"],
                worker_id="llm-batch-w",
                token=job["attempts"],
                error=f"provider error {resp.status_code}",
            )

    # All jobs should have succeeded
    assert succeeded == num_jobs, (
        f"Only {succeeded}/{num_jobs} jobs succeeded despite sufficient retries"
    )

    # llm_usage must have exactly one row per job (one successful attempt each)
    row = conn.execute(
        "SELECT count(*) AS cnt, sum(cost_usd) AS total FROM llm_usage WHERE job_id = ANY(%s);",
        [job_ids],
    ).fetchone()
    assert row["cnt"] == num_jobs, f"Expected {num_jobs} llm_usage rows, got {row['cnt']}"
    assert row["total"] > 0.0, "Total LLM cost should be > 0"
