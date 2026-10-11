from __future__ import annotations

from datetime import UTC, datetime
from enum import StrEnum
from typing import Any
from uuid import uuid4

from pydantic import BaseModel, ConfigDict, Field, field_validator


class Priority(StrEnum):
    HIGH = "high"
    MEDIUM = "medium"
    LOW = "low"


PRIORITY_ORDER = (Priority.HIGH, Priority.MEDIUM, Priority.LOW)


class JobStatus(StrEnum):
    QUEUED = "queued"
    DELAYED = "delayed"
    RUNNING = "running"
    SUCCEEDED = "succeeded"
    FAILED = "failed"
    RETRYING = "retrying"
    DEAD = "dead"


def utc_now() -> datetime:
    return datetime.now(UTC)


class JobCreate(BaseModel):
    task_name: str = Field(min_length=1, max_length=120)
    payload: dict[str, Any] = Field(default_factory=dict)
    priority: Priority = Priority.MEDIUM
    delay_seconds: int = Field(default=0, ge=0, le=86_400)
    max_retries: int = Field(default=3, ge=0, le=20)


class Job(BaseModel):
    model_config = ConfigDict(use_enum_values=True)

    job_id: str = Field(default_factory=lambda: str(uuid4()))
    task_name: str
    payload: dict[str, Any]
    priority: Priority
    status: JobStatus
    attempts: int = 0
    max_retries: int
    created_at: datetime = Field(default_factory=utc_now)
    updated_at: datetime = Field(default_factory=utc_now)
    started_at: datetime | None = None
    finished_at: datetime | None = None
    error_message: str | None = None

    @field_validator("created_at", "updated_at", "started_at", "finished_at", mode="before")
    @classmethod
    def parse_datetime(cls, value: object) -> object:
        if isinstance(value, str):
            return datetime.fromisoformat(value)
        return value


class JobCreated(BaseModel):
    job_id: str
    status: JobStatus


class QueueStats(BaseModel):
    total_jobs: int
    queued_jobs: int
    delayed_jobs: int
    in_flight_jobs: int = 0
    running_jobs: int
    succeeded_jobs: int
    failed_jobs: int
    dead_letter_jobs: int
    queues: dict[str, int]


class HealthResponse(BaseModel):
    status: str
    redis: str


class JobEvent(BaseModel):
    event_id: str
    job_id: str
    status: JobStatus
    message: str
    timestamp: datetime = Field(default_factory=utc_now)
