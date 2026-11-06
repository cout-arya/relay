"""Custom exceptions for Relay job handlers."""

from __future__ import annotations


class RetryLater(Exception):
    """Raise to retry the job after a specific delay, overriding the backoff calculation.

    Useful for rate limits (e.g. HTTP 429 with Retry-After header).
    """

    def __init__(self, seconds: float, message: str = "retry requested") -> None:
        self.seconds = seconds
        super().__init__(message)


class PermanentError(Exception):
    """Raise to send the job straight to dead-letter queue, skipping remaining retries.

    Use for errors that will never succeed (e.g. invalid input, bad API key).
    """
