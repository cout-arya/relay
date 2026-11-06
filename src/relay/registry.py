"""Handler registry: maps job type names to handler functions."""

from __future__ import annotations

import logging
from collections.abc import Callable
from typing import Any

logger = logging.getLogger(__name__)

# Type for handler functions: they receive (job_id, payload, context) and return Any
HandlerFunc = Callable[..., Any]

_registry: dict[str, HandlerFunc] = {}


def register(type_name: str) -> Callable[[HandlerFunc], HandlerFunc]:
    """Decorator to register a handler for a job type.

    Usage:
        @register('send_email')
        def handle_send_email(job_id: int, payload: dict, ctx: dict) -> Any:
            ...
    """

    def decorator(func: HandlerFunc) -> HandlerFunc:
        if type_name in _registry:
            logger.warning("Overwriting handler for type: %s", type_name)
        _registry[type_name] = func
        logger.debug("Registered handler: %s -> %s", type_name, func.__name__)
        return func

    return decorator


def get_handler(type_name: str) -> HandlerFunc | None:
    """Look up a handler by job type name. Returns None if not found."""
    return _registry.get(type_name)


def registered_types() -> list[str]:
    """Return all registered job type names."""
    return list(_registry.keys())
