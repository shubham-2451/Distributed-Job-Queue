from app.models import JobCreate
from app.queue import JobQueue


def test_high_priority_is_processed_first_even_when_enqueued_last(queue: JobQueue) -> None:
    queue.submit(JobCreate(task_name="low-1", payload={}, priority="low"))
    queue.submit(JobCreate(task_name="medium-1", payload={}, priority="medium"))
    high = queue.submit(JobCreate(task_name="high-1", payload={}, priority="high"))

    assert queue.dequeue().job_id == high.job_id

