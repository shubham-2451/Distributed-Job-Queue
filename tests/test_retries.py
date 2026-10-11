import pytest

from app.models import JobCreate
from app.queue import JobQueue
from app.worker import SimulatedTaskExecutor, Worker


def make_worker(queue: JobQueue) -> Worker:
    return Worker(
        queue=queue,
        executor=SimulatedTaskExecutor(),
        concurrency=1,
        poll_interval_seconds=0.01,
        retry_backoff_base_seconds=2,
    )


@pytest.mark.asyncio
async def test_retry_behavior_schedules_failed_job(queue: JobQueue) -> None:
    job = queue.submit(JobCreate(task_name="always_fail", payload={}, max_retries=2))
    worker = make_worker(queue)

    await worker.process_next()

    stored = queue.get(job.job_id)
    assert stored.status == "retrying"
    assert stored.attempts == 1
    assert queue.stats().delayed_jobs == 1


@pytest.mark.asyncio
async def test_exponential_backoff_scores_increase(redis_client) -> None:
    queue = JobQueue(redis_client)
    job = queue.submit(JobCreate(task_name="always_fail", payload={}, max_retries=3))
    worker = make_worker(queue)

    await worker.process_next()
    first_score = redis_client.zscore("dtq:delayed", job.job_id)
    redis_client.zadd("dtq:delayed", {job.job_id: 0})
    await worker.process_next()
    second_score = redis_client.zscore("dtq:delayed", job.job_id)

    assert first_score is not None
    assert second_score is not None
    assert second_score > first_score


@pytest.mark.asyncio
async def test_transient_failure_eventually_succeeds(redis_client) -> None:
    queue = JobQueue(redis_client)
    job = queue.submit(
        JobCreate(
            task_name="flaky",
            payload={"fail_until_attempt": 1, "duration_seconds": 0},
            max_retries=2,
        )
    )
    worker = make_worker(queue)

    await worker.process_next()
    redis_client.zadd("dtq:delayed", {job.job_id: 0})
    await worker.process_next()

    stored = queue.get(job.job_id)
    assert stored.status == "succeeded"
    assert stored.attempts == 2


@pytest.mark.asyncio
async def test_dead_letter_after_retries_exhausted(redis_client) -> None:
    queue = JobQueue(redis_client)
    job = queue.submit(JobCreate(task_name="always_fail", payload={}, max_retries=1))
    worker = make_worker(queue)

    await worker.process_next()
    redis_client.zadd("dtq:delayed", {job.job_id: 0})
    await worker.process_next()

    stored = queue.get(job.job_id)
    assert stored.status == "dead"
    assert stored.attempts == 2
    assert redis_client.lrange("dtq:dead-letter", 0, -1) == [job.job_id]
