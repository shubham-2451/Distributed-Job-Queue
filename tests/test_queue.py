from app.models import JobCreate
from app.queue import JobQueue


def test_priority_ordering_dequeues_high_before_medium_before_low(queue: JobQueue) -> None:
    low = queue.submit(JobCreate(task_name="low", payload={}, priority="low"))
    high = queue.submit(JobCreate(task_name="high", payload={}, priority="high"))
    medium = queue.submit(JobCreate(task_name="medium", payload={}, priority="medium"))

    assert queue.dequeue().job_id == high.job_id
    assert queue.dequeue().job_id == medium.job_id
    assert queue.dequeue().job_id == low.job_id


def test_fifo_order_within_same_priority(queue: JobQueue) -> None:
    first = queue.submit(JobCreate(task_name="first", payload={}, priority="medium"))
    second = queue.submit(JobCreate(task_name="second", payload={}, priority="medium"))

    assert queue.dequeue().job_id == first.job_id
    assert queue.dequeue().job_id == second.job_id


def test_delayed_job_is_not_dequeued_before_due(queue: JobQueue) -> None:
    queue.submit(JobCreate(task_name="later", payload={}, priority="high", delay_seconds=60))

    assert queue.dequeue() is None
    assert queue.stats().delayed_jobs == 1


def test_promote_due_delayed_job(redis_client) -> None:
    queue = JobQueue(redis_client)
    job = queue.submit(JobCreate(task_name="soon", payload={}, priority="high", delay_seconds=60))
    redis_client.zadd("dtq:delayed", {job.job_id: 0})

    promoted = queue.promote_due_jobs()

    assert promoted == 1
    assert queue.dequeue().job_id == job.job_id

