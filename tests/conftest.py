from collections.abc import AsyncGenerator

import fakeredis
import httpx
import pytest

from app.main import app, get_redis
from app.queue import JobQueue


@pytest.fixture
def redis_client() -> fakeredis.FakeRedis:
    return fakeredis.FakeRedis(decode_responses=True)


@pytest.fixture
def queue(redis_client: fakeredis.FakeRedis) -> JobQueue:
    return JobQueue(redis_client)


@pytest.fixture
async def client(redis_client: fakeredis.FakeRedis) -> AsyncGenerator[httpx.AsyncClient, None]:
    async def override_get_redis() -> fakeredis.FakeRedis:
        return redis_client

    app.dependency_overrides[get_redis] = override_get_redis
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://testserver") as test_client:
        yield test_client
    app.dependency_overrides.clear()
