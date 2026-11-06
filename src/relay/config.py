"""Relay configuration from environment variables."""

from __future__ import annotations

from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    """All relay configuration, read from environment variables."""

    database_url: str = "postgresql://relay:relay@localhost:5432/relay"
    lease_seconds: int = 30
    poll_interval: float = 0.5
    backoff_base: float = 2.0
    backoff_cap: float = 300.0
    max_attempts_default: int = 5
    worker_queues: str = "default"  # comma-separated
    demo_mode: bool = False
    llm_provider: str = "mock"
    log_level: str = "info"
    log_format: str = "text"  # "text" or "json" for structured JSON logging

    @property
    def queue_list(self) -> list[str]:
        return [q.strip() for q in self.worker_queues.split(",") if q.strip()]

    model_config = {"env_prefix": "", "case_sensitive": False}


# Singleton — import this everywhere
settings = Settings()
