from __future__ import annotations

from typing import Annotated

from fastapi import (
    Depends,
    FastAPI,
    HTTPException,
    Query,
    Response,
    WebSocket,
    WebSocketDisconnect,
    status,
)
from fastapi.responses import HTMLResponse
from redis import Redis
from redis.exceptions import RedisError

from app.config import get_settings
from app.dashboard import dashboard_html
from app.events import RedisEventSubscriber
from app.logging_config import configure_logging, logger
from app.models import HealthResponse, Job, JobCreate, JobCreated, QueueStats
from app.queue import JobNotDeadLetteredError, JobNotFoundError, JobQueue
from app.redis_client import get_redis as build_redis

settings = get_settings()
configure_logging(settings.log_level)
app = FastAPI(title=settings.app_name, version="1.0.0")


async def get_redis() -> Redis:
    return build_redis()


async def get_queue(redis: Annotated[Redis, Depends(get_redis)]) -> JobQueue:
    return JobQueue(redis)


@app.post("/jobs", response_model=JobCreated, status_code=status.HTTP_201_CREATED)
async def create_job(
    request: JobCreate,
    queue: Annotated[JobQueue, Depends(get_queue)],
) -> JobCreated:
    job = queue.submit(request)
    logger.info("job_created", job_id=job.job_id, priority=job.priority, status=job.status)
    return JobCreated(job_id=job.job_id, status=job.status)


@app.get("/queues/dead-letter", response_model=list[Job])
@app.get("/jobs/dead-letter", response_model=list[Job])
async def list_dead_letter_jobs(
    queue: Annotated[JobQueue, Depends(get_queue)],
    limit: Annotated[int, Query(ge=1, le=100)] = 50,
    offset: Annotated[int, Query(ge=0)] = 0,
) -> list[Job]:
    return queue.list_dead_letter(limit=limit, offset=offset)


@app.get("/queues/dead-letter/{job_id}", response_model=Job)
@app.get("/jobs/dead-letter/{job_id}", response_model=Job)
async def get_dead_letter_job(
    job_id: str,
    queue: Annotated[JobQueue, Depends(get_queue)],
) -> Job:
    try:
        return queue.get_dead_letter(job_id)
    except JobNotFoundError as exc:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Job not found") from exc
    except JobNotDeadLetteredError as exc:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Job is not dead-lettered",
        ) from exc


@app.post("/jobs/{job_id}/replay", response_model=Job)
@app.post("/queues/dead-letter/{job_id}/replay", response_model=Job)
@app.post("/jobs/dead-letter/{job_id}/replay", response_model=Job)
async def replay_job(
    job_id: str,
    queue: Annotated[JobQueue, Depends(get_queue)],
) -> Job:
    try:
        job = queue.replay(job_id)
        logger.info("job_replayed", job_id=job.job_id, priority=job.priority)
        return job
    except JobNotFoundError as exc:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Job not found") from exc
    except JobNotDeadLetteredError as exc:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail="Job is not dead-lettered",
        ) from exc


@app.get("/jobs/{job_id}", response_model=Job)
async def get_job(job_id: str, queue: Annotated[JobQueue, Depends(get_queue)]) -> Job:
    try:
        return queue.get(job_id)
    except JobNotFoundError as exc:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Job not found") from exc


@app.get("/jobs", response_model=list[Job])
async def list_jobs(
    queue: Annotated[JobQueue, Depends(get_queue)],
    limit: Annotated[int, Query(ge=1, le=100)] = 50,
) -> list[Job]:
    return queue.list_recent(limit)


@app.get("/queues/stats", response_model=QueueStats)
async def queue_stats(queue: Annotated[JobQueue, Depends(get_queue)]) -> QueueStats:
    return queue.stats()


@app.get("/health", response_model=HealthResponse)
async def health(
    redis: Annotated[Redis, Depends(get_redis)],
    response: Response,
) -> HealthResponse:
    try:
        redis.ping()
    except RedisError:
        logger.exception("redis_health_check_failed")
        response.status_code = status.HTTP_503_SERVICE_UNAVAILABLE
        return HealthResponse(status="degraded", redis="unavailable")
    return HealthResponse(status="ok", redis="ok")


@app.get("/dashboard", response_class=HTMLResponse)
async def dashboard() -> HTMLResponse:
    return dashboard_html()


@app.websocket("/ws/jobs")
async def job_events(websocket: WebSocket) -> None:
    await websocket.accept()
    redis = build_redis()
    subscriber = RedisEventSubscriber(redis)
    try:
        for event in JobQueue(redis).events.recent(limit=20):
            await websocket.send_json(event.model_dump(mode="json"))
        async for event in subscriber.listen():
            await websocket.send_json(event)
    except WebSocketDisconnect:
        logger.info("websocket_disconnected")
