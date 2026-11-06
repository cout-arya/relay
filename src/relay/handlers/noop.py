"""Noop and sleep handlers — for benchmarks and basic tests."""

from __future__ import annotations

import logging
import time
from typing import Any

from relay.registry import register

logger = logging.getLogger(__name__)


@register("noop")
def handle_noop(job_id: int, payload: dict[str, Any], ctx: dict[str, Any]) -> None:
    """Does nothing. Used for benchmarks and basic claim/ack flow tests."""
    logger.debug("noop: job_id=%s", job_id)


@register("sleep")
def handle_sleep(job_id: int, payload: dict[str, Any], ctx: dict[str, Any]) -> None:
    """Sleeps for payload.seconds. Respects cancellation from lease loss."""
    seconds = payload.get("seconds", 1)
    cancel_event = ctx.get("cancel_event")

    logger.debug("sleep: job_id=%s seconds=%s", job_id, seconds)

    # Sleep in small chunks to allow cancellation
    elapsed = 0.0
    chunk = 0.1
    while elapsed < seconds:
        if cancel_event and cancel_event.is_set():
            logger.info("sleep: job_id=%s cancelled (lease lost)", job_id)
            return
        time.sleep(min(chunk, seconds - elapsed))
        elapsed += chunk
