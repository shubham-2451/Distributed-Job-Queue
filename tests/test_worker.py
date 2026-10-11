import pytest

from app.models import JobCreate
from app.queue import JobQueue
from app.worker import SimulatedTaskExecutor, Worker


def build_test_worker(queue: JobQueue, retry_backoff_base_seconds: int = 1) -> Worker:
    return Worker(
        queue=queue,
        executor=SimulatedTaskExecutor(),
        concurrency=1,
        poll_interval_seconds=0.01,
        retry_backoff_base_seconds=retry_backoff_base_seconds,
    )


@pytest.mark.asyncio
async def test_worker_success_path(queue: JobQueue) -> None:
    job = queue.submit(JobCreate(task_name="send_email", payload={"duration_seconds": 0}))
    worker = build_test_worker(queue)

    assert await worker.process_next() is True

    stored = queue.get(job.job_id)
    assert stored.status == "succeeded"
    assert stored.attempts == 1
    assert stored.finished_at is not None


@pytest.mark.asyncio
async def test_worker_failure_moves_to_dead_after_no_retries(queue: JobQueue) -> None:
    job = queue.submit(JobCreate(task_name="always_fail", payload={}, max_retries=0))
    worker = build_test_worker(queue)

    assert await worker.process_next() is True

    stored = queue.get(job.job_id)
    assert stored.status == "dead"
    assert stored.attempts == 1
    assert queue.stats().dead_letter_jobs == 1

