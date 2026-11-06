"""Multi-step transfer handler — demonstrates idempotency using the effects table.

A three-step job where each step is recorded in the effects table using
INSERT ON CONFLICT DO NOTHING. After any number of retries and crashes,
effects contains exactly one row per (job, step).
"""

from __future__ import annotations

import logging
import random
import time
from typing import Any

from psycopg.types.json import Jsonb

from relay.db import get_conn
from relay.registry import register

logger = logging.getLogger(__name__)

STEPS = ["reserve", "charge", "notify"]


@register("multi_step_transfer")
def handle_multi_step_transfer(
    job_id: int, payload: dict[str, Any], ctx: dict[str, Any]
) -> None:
    """Execute a three-step transfer with idempotent effects.

    Payload options:
        fail_after_step: str — crash after this step name (for testing)
        failure_rate: float — random failure probability at each step (0.0-1.0)
    """
    fail_after_step = payload.get("fail_after_step")
    failure_rate = payload.get("failure_rate", 0.0)
    cancel_event = ctx.get("cancel_event")

    for step in STEPS:
        # Check for cancellation (lease lost)
        if cancel_event and cancel_event.is_set():
            logger.info(
                "multi_step_transfer: job_id=%s cancelled at step=%s",
                job_id, step,
            )
            return

        # Execute the step idempotently: INSERT ON CONFLICT DO NOTHING
        # If the step already exists (from a previous attempt), it is skipped
        result_data = {
            "step": step,
            "job_id": job_id,
            "attempt": ctx.get("attempt"),
            "timestamp": time.time(),
        }

        with get_conn() as conn:
            with conn.transaction():
                conn.execute(
                    """
                    INSERT INTO effects (job_id, step, result)
                    VALUES (%s, %s, %s)
                    ON CONFLICT (job_id, step) DO NOTHING;
                    """,
                    [job_id, step, Jsonb(result_data)],
                )

        logger.debug(
            "multi_step_transfer: job_id=%s step=%s completed", job_id, step
        )

        # Simulate potential crash after a specific step (for testing)
        if fail_after_step and step == fail_after_step:
            raise RuntimeError(
                f"Intentional crash after step '{step}' (fail_after_step)"
            )

        # Random failure injection for chaos testing
        if failure_rate > 0 and random.random() < failure_rate:
            raise RuntimeError(
                f"Random failure at step '{step}' (rate={failure_rate})"
            )

    logger.info("multi_step_transfer: job_id=%s all steps completed", job_id)
