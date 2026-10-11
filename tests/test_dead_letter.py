import asyncio

import httpx
import pytest

from app.models import JobCreate, JobStatus
from app.queue import JobNotDeadLetteredError, JobQueue
from app.worker import SimulatedTaskExecutor, Worker


def make_worker(queue: JobQueue) -> Worker:
    return Worker(
        queue=queue,
        executor=SimulatedTaskExecutor(),
        concurrency=1,
        poll_interval_seconds=0.01,
        retry_backoff_base_seconds=1,
    )


@pytest.mark.asyncio
async def test_list_dead_letter_jobs_empty(client: httpx.AsyncClient) -> None:
    response = await client.get("/jobs/dead-letter")
    assert response.status_code == 200
    assert response.json() == []

    alt_response = await client.get("/queues/dead-letter")
    assert alt_response.status_code == 200
    assert alt_response.json() == []


@pytest.mark.asyncio
async def test_list_dead_letter_jobs_with_pagination(
    client: httpx.AsyncClient,
    redis_client,
) -> None:
    queue = JobQueue(redis_client)
    worker = make_worker(queue)

    job_ids = []
    for index in range(5):
        job = queue.submit(
            JobCreate(task_name="always_fail", payload={"idx": index}, max_retries=0)
        )
        job_ids.append(job.job_id)
        await worker.process_next()

    # List page 1 (limit 3, offset 0)
    res_page1 = await client.get("/jobs/dead-letter?limit=3&offset=0")
    assert res_page1.status_code == 200
    page1 = res_page1.json()
    assert len(page1) == 3

    # List page 2 (limit 3, offset 3)
    res_page2 = await client.get("/queues/dead-letter?limit=3&offset=3")
    assert res_page2.status_code == 200
    page2 = res_page2.json()
    assert len(page2) == 2

    # All returned jobs have DEAD status
    assert all(job["status"] == "dead" for job in page1 + page2)


@pytest.mark.asyncio
async def test_get_dead_letter_job_success(
    client: httpx.AsyncClient,
    redis_client,
) -> None:
    queue = JobQueue(redis_client)
    job = queue.submit(JobCreate(task_name="always_fail", payload={}, max_retries=0))
    worker = make_worker(queue)
    await worker.process_next()

    response = await client.get(f"/jobs/dead-letter/{job.job_id}")
    assert response.status_code == 200
    data = response.json()
    assert data["job_id"] == job.job_id
    assert data["status"] == "dead"
    assert data["error_message"] is not None

    alt_response = await client.get(f"/queues/dead-letter/{job.job_id}")
    assert alt_response.status_code == 200
    assert alt_response.json()["job_id"] == job.job_id


@pytest.mark.asyncio
async def test_get_dead_letter_job_not_found(client: httpx.AsyncClient) -> None:
    response = await client.get("/jobs/dead-letter/00000000-0000-0000-0000-000000000000")
    assert response.status_code == 404
    assert response.json()["detail"] == "Job not found"


@pytest.mark.asyncio
async def test_get_dead_letter_job_not_dead_lettered(client: httpx.AsyncClient) -> None:
    create_res = await client.post(
        "/jobs",
        json={"task_name": "send_email", "payload": {}},
    )
    job_id = create_res.json()["job_id"]

    response = await client.get(f"/jobs/dead-letter/{job_id}")
    assert response.status_code == 400
    assert response.json()["detail"] == "Job is not dead-lettered"


@pytest.mark.asyncio
async def test_replay_dead_letter_job_success(
    client: httpx.AsyncClient,
    redis_client,
) -> None:
    queue = JobQueue(redis_client)
    job = queue.submit(
        JobCreate(
            task_name="replay_me",
            payload={"key": "value", "duration_seconds": 0},
            priority="high",
            max_retries=1,
        )
    )

    # Force job to dead-letter state
    queue.mark_dead(job, "Simulated failure")
    assert queue.stats().dead_letter_jobs == 1
    assert queue.stats().queued_jobs == 0

    # Replay the dead-lettered job
    response = await client.post(f"/jobs/{job.job_id}/replay")
    assert response.status_code == 200
    replayed = response.json()

    assert replayed["job_id"] == job.job_id
    assert replayed["task_name"] == "replay_me"
    assert replayed["payload"] == {"key": "value", "duration_seconds": 0}
    assert replayed["priority"] == "high"
    assert replayed["status"] == "queued"
    assert replayed["attempts"] == 0
    assert replayed["error_message"] is None
    assert replayed["started_at"] is None
    assert replayed["finished_at"] is None

    # Check queue states
    stats = queue.stats()
    assert stats.dead_letter_jobs == 0
    assert stats.queued_jobs == 1
    assert stats.queues["high"] == 1
    assert stats.in_flight_jobs == 0
    assert redis_client.lrange("dtq:dead-letter", 0, -1) == []

    # Worker can now pick up the replayed job and successfully process it
    worker = make_worker(queue)
    processed = await worker.process_next()
    assert processed is True

    finished_job = queue.get(job.job_id)
    assert finished_job.status == JobStatus.SUCCEEDED
    assert finished_job.attempts == 1


@pytest.mark.asyncio
async def test_replay_missing_job_returns_404(client: httpx.AsyncClient) -> None:
    response = await client.post("/jobs/11111111-1111-1111-1111-111111111111/replay")
    assert response.status_code == 404
    assert response.json()["detail"] == "Job not found"


@pytest.mark.asyncio
async def test_replay_non_dead_lettered_job_returns_409(client: httpx.AsyncClient) -> None:
    create_res = await client.post(
        "/jobs",
        json={"task_name": "send_email", "payload": {}},
    )
    job_id = create_res.json()["job_id"]

    response = await client.post(f"/jobs/{job_id}/replay")
    assert response.status_code == 409
    assert response.json()["detail"] == "Job is not dead-lettered"


@pytest.mark.asyncio
async def test_repeated_replay_attempt_returns_409(
    client: httpx.AsyncClient,
    redis_client,
) -> None:
    queue = JobQueue(redis_client)
    job = queue.submit(JobCreate(task_name="send_email", payload={}, max_retries=0))
    queue.mark_dead(job, "Failed")

    first_res = await client.post(f"/jobs/{job.job_id}/replay")
    assert first_res.status_code == 200

    second_res = await client.post(f"/jobs/{job.job_id}/replay")
    assert second_res.status_code == 409
    assert second_res.json()["detail"] == "Job is not dead-lettered"


@pytest.mark.asyncio
async def test_concurrent_replay_requests_only_one_succeeds(
    client: httpx.AsyncClient,
    redis_client,
) -> None:
    queue = JobQueue(redis_client)
    job = queue.submit(JobCreate(task_name="race_task", payload={}, priority="medium"))
    queue.mark_dead(job, "Fatal")

    # Launch two replay requests concurrently
    res1, res2 = await asyncio.gather(
        client.post(f"/jobs/{job.job_id}/replay"),
        client.post(f"/jobs/{job.job_id}/replay"),
    )

    statuses = {res1.status_code, res2.status_code}
    assert statuses == {200, 409}

    # Job is only in ready queue once
    queued_ids = redis_client.lrange("dtq:queue:medium", 0, -1)
    assert queued_ids == [job.job_id]
    assert redis_client.lrange("dtq:dead-letter", 0, -1) == []


def test_queue_direct_replay_error_handling(redis_client) -> None:
    queue = JobQueue(redis_client)
    job = queue.submit(JobCreate(task_name="active_task", payload={}))

    # Direct call on non-dead job raises JobNotDeadLetteredError
    with pytest.raises(JobNotDeadLetteredError):
        queue.replay(job.job_id)

    # Calling get_dead_letter on non-dead job raises JobNotDeadLetteredError
    with pytest.raises(JobNotDeadLetteredError):
        queue.get_dead_letter(job.job_id)
