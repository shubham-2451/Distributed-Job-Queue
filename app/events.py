from __future__ import annotations

import asyncio
import json
from collections.abc import AsyncIterator
from datetime import datetime
from typing import Any
from uuid import uuid4

from redis import Redis

from app.config import get_settings
from app.models import JobEvent, JobStatus

EVENTS_KEY = "dtq:events"
EVENTS_CHANNEL = "dtq:job-events"


class EventPublisher:
    def __init__(self, redis: Redis):
        self.redis = redis
        self.settings = get_settings()

    def publish(self, job_id: str, status: JobStatus, message: str) -> JobEvent:
        event = JobEvent(
            event_id=str(uuid4()),
            job_id=job_id,
            status=status,
            message=message,
        )
        payload = event.model_dump_json()
        pipe = self.redis.pipeline()
        pipe.lpush(EVENTS_KEY, payload)
        pipe.ltrim(EVENTS_KEY, 0, self.settings.event_stream_limit - 1)
        pipe.publish(EVENTS_CHANNEL, payload)
        pipe.execute()
        return event

    def recent(self, limit: int = 50) -> list[JobEvent]:
        values = self.redis.lrange(EVENTS_KEY, 0, limit - 1)
        return [JobEvent.model_validate_json(value) for value in values]


class RedisEventSubscriber:
    def __init__(self, redis: Redis):
        self.redis = redis

    async def listen(self) -> AsyncIterator[dict[str, Any]]:
        pubsub = self.redis.pubsub()
        pubsub.subscribe(EVENTS_CHANNEL)
        try:
            while True:
                message = await asyncio.to_thread(
                    pubsub.get_message,
                    ignore_subscribe_messages=True,
                    timeout=1.0,
                )
                if not message:
                    await asyncio.sleep(0.05)
                    continue
                data = message.get("data")
                if isinstance(data, bytes):
                    data = data.decode("utf-8")
                yield json.loads(data)
        finally:
            await asyncio.to_thread(pubsub.close)


def json_safe(value: Any) -> Any:
    if isinstance(value, datetime):
        return value.isoformat()
    return value
