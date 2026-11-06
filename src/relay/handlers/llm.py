"""LLM task handler — AI engineering angle with rate-limit-aware retries.

Uses a provider interface with MockProvider (default, no API key needed).
Records every call in llm_usage with tokens and estimated cost.
"""

from __future__ import annotations

import logging
from typing import Any

from relay.config import settings
from relay.db import get_conn
from relay.errors import PermanentError, RetryLater
from relay.providers.base import LLMProvider, LLMResponse, get_provider
from relay.registry import register

logger = logging.getLogger(__name__)


@register("llm_task")
def handle_llm_task(
    job_id: int, payload: dict[str, Any], ctx: dict[str, Any]
) -> None:
    """Call an LLM provider, handle rate limits, and record usage.

    Payload:
        prompt: str — the prompt to send
        model: str — optional model name (default: provider default)
        provider: str — optional provider override

    Behavior:
        - On 429: raise RetryLater(retry_after)
        - On 5xx: raise a normal retryable error
        - On invalid input: raise PermanentError
    """
    prompt = payload.get("prompt")
    if not prompt:
        raise PermanentError("Missing 'prompt' in payload")

    model = payload.get("model", "mock-default")
    provider_name = payload.get("provider", settings.llm_provider)
    cancel_event = ctx.get("cancel_event")

    # Check for cancellation
    if cancel_event and cancel_event.is_set():
        logger.info("llm_task: job_id=%s cancelled before LLM call", job_id)
        return

    # Get the provider
    provider: LLMProvider = get_provider(provider_name)

    # Call the provider
    response: LLMResponse = provider.complete(prompt=prompt, model=model)

    # Handle error responses
    if response.error:
        if response.status_code == 429:
            retry_after = response.retry_after or 5.0
            logger.info(
                "llm_task: job_id=%s rate limited, retry after %ss",
                job_id, retry_after,
            )
            raise RetryLater(retry_after, f"Rate limited: {response.error}")
        elif response.status_code and response.status_code >= 500:
            raise RuntimeError(f"Provider error ({response.status_code}): {response.error}")
        else:
            raise PermanentError(f"Invalid request: {response.error}")

    # Record usage in llm_usage table
    with get_conn() as conn:
        with conn.transaction():
            conn.execute(
                """
                INSERT INTO llm_usage (job_id, attempt_no, provider, model,
                                       input_tokens, output_tokens, cost_usd)
                VALUES (%s, %s, %s, %s, %s, %s, %s);
                """,
                [
                    job_id,
                    ctx.get("attempt", 1),
                    provider_name,
                    model,
                    response.input_tokens,
                    response.output_tokens,
                    response.cost_usd,
                ],
            )

    logger.info(
        "llm_task: job_id=%s completed, tokens=%d+%d cost=$%.6f",
        job_id, response.input_tokens, response.output_tokens, response.cost_usd,
    )
