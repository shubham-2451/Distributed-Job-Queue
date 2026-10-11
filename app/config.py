from functools import lru_cache

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8")

    app_name: str = "Distributed Task Queue"
    redis_url: str = Field(default="redis://localhost:6379/0", alias="REDIS_URL")
    worker_concurrency: int = Field(default=4, alias="WORKER_CONCURRENCY", ge=1)
    worker_poll_interval_seconds: float = Field(
        default=0.5,
        alias="WORKER_POLL_INTERVAL_SECONDS",
        ge=0.1,
    )
    retry_backoff_base_seconds: int = Field(default=2, alias="RETRY_BACKOFF_BASE_SECONDS", ge=1)
    worker_lease_timeout_seconds: float = Field(
        default=30.0,
        alias="WORKER_LEASE_TIMEOUT_SECONDS",
        ge=0.1,
    )
    worker_heartbeat_interval_seconds: float = Field(
        default=10.0,
        alias="WORKER_HEARTBEAT_INTERVAL_SECONDS",
        ge=0.05,
    )
    recent_jobs_limit: int = Field(default=100, alias="RECENT_JOBS_LIMIT", ge=1)
    event_stream_limit: int = Field(default=200, alias="EVENT_STREAM_LIMIT", ge=1)
    log_level: str = Field(default="INFO", alias="LOG_LEVEL")


@lru_cache
def get_settings() -> Settings:
    return Settings()
