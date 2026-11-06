"""Exponential backoff with full jitter.

Full jitter prevents retry storms where many failed jobs all retry at the same instant.
Formula: delay = random.uniform(0, min(cap, base * 2^(attempts - 1)))
"""

from __future__ import annotations

import random

from relay.config import settings


def compute_delay(
    attempts: int,
    base: float | None = None,
    cap: float | None = None,
) -> float:
    """Compute retry delay using full jitter exponential backoff.

    Args:
        attempts: Current attempt number (1-based).
        base: Base delay in seconds. Defaults to config.
        cap: Maximum delay in seconds. Defaults to config.

    Returns:
        Delay in seconds with full jitter applied.
    """
    base = base if base is not None else settings.backoff_base
    cap = cap if cap is not None else settings.backoff_cap

    # Full jitter: uniform random between 0 and the exponential ceiling
    max_delay = min(cap, base * (2 ** (attempts - 1)))
    return random.uniform(0, max_delay)
