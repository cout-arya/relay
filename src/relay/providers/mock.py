"""Mock LLM provider — deterministic output, configurable errors, no API key needed.

Simulates:
- Deterministic text output
- Configurable error rate
- Simulated 429 responses with retry-after value
- Fake token counts and cost estimation
"""

from __future__ import annotations

import hashlib
import random

from relay.providers.base import LLMProvider, LLMResponse, register_provider


@register_provider("mock")
class MockProvider(LLMProvider):
    """Mock LLM provider for testing and demos."""

    def __init__(
        self,
        *,
        error_rate: float = 0.0,
        rate_limit_rate: float = 0.0,
        retry_after: float = 2.0,
        cost_per_1k_input: float = 0.001,
        cost_per_1k_output: float = 0.002,
    ) -> None:
        self.error_rate = error_rate
        self.rate_limit_rate = rate_limit_rate
        self.retry_after = retry_after
        self.cost_per_1k_input = cost_per_1k_input
        self.cost_per_1k_output = cost_per_1k_output

    def complete(self, *, prompt: str, model: str = "mock-default") -> LLMResponse:
        """Generate a deterministic mock response.

        Uses hash of prompt for reproducible "random" behavior in tests.
        """
        # Simulate rate limiting (429)
        if self.rate_limit_rate > 0 and random.random() < self.rate_limit_rate:
            return LLMResponse(
                status_code=429,
                error="Rate limit exceeded",
                retry_after=self.retry_after,
            )

        # Simulate server errors (5xx)
        if self.error_rate > 0 and random.random() < self.error_rate:
            return LLMResponse(
                status_code=500,
                error="Internal server error (mock)",
            )

        # Deterministic output based on prompt hash
        prompt_hash = hashlib.sha256(prompt.encode()).hexdigest()[:16]
        input_tokens = max(10, len(prompt.split()))
        output_tokens = max(10, input_tokens // 2)

        text = (
            f"Mock response for prompt hash {prompt_hash}. "
            f"This is a deterministic mock output for testing. "
            f"Model: {model}. Input length: {len(prompt)} chars."
        )

        cost = (
            (input_tokens / 1000) * self.cost_per_1k_input
            + (output_tokens / 1000) * self.cost_per_1k_output
        )

        return LLMResponse(
            text=text,
            input_tokens=input_tokens,
            output_tokens=output_tokens,
            cost_usd=round(cost, 6),
            status_code=200,
        )
