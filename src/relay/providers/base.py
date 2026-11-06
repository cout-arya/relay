"""Base LLM provider interface and provider factory."""

from __future__ import annotations

import importlib
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Any


@dataclass
class LLMResponse:
    """Response from an LLM provider."""

    text: str = ""
    input_tokens: int = 0
    output_tokens: int = 0
    cost_usd: float = 0.0
    status_code: int | None = None
    error: str | None = None
    retry_after: float | None = None
    metadata: dict[str, Any] = field(default_factory=dict)


class LLMProvider(ABC):
    """Abstract base for LLM providers."""

    @abstractmethod
    def complete(self, *, prompt: str, model: str = "default") -> LLMResponse:
        """Send a completion request and return the response."""
        ...


# Provider registry
_providers: dict[str, type[LLMProvider]] = {}


def register_provider(name: str) -> Any:
    """Decorator to register a provider class."""

    def decorator(cls: type[LLMProvider]) -> type[LLMProvider]:
        _providers[name] = cls
        return cls

    return decorator


def get_provider(name: str) -> LLMProvider:
    """Get a provider instance by name."""
    if name not in _providers:
        # Try to import the module to trigger registration
        try:
            importlib.import_module(f"relay.providers.{name}")
        except ImportError:
            pass

    if name not in _providers:
        raise ValueError(f"Unknown LLM provider: {name}")

    return _providers[name]()
