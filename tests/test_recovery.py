import pytest

from app.models import JobCreate, JobStatus
from app.queue import JobQueue
from app.worker import SimulatedTaskExecutor, Worker


def make_test_worker(queue: JobQueue, worker_id: str = "w1") -> Worker:
    return Worker(
        queue=queue,
        executor=SimulatedTaskExecutor(),
        concurrency=1,
        poll_interval_seconds=0.01,
        retry_backoff_base_seconds=1,
        worker_id=worker_id,
        lease_timeout_seconds=30.0,
        heartbeat_interval_seconds=10.0,
    )


@pytest.mark.asyncio
async def test_normal_completion_cleans_up_in_flight(redis_client) -> None:
    queue = JobQueue(redis_client, lease_timeout_seconds=30.0)
    job = queue.submit(JobCreate(task_name="send_email", payload={"duration_seconds": 0}))

    worker = make_test_worker(queue, worker_id="worker-1")

    # Before dequeue
    assert redis_client.zcard("dtq:in-flight") == 0

    # Process next
    processed = await worker.process_next()
    assert processed is True

    # After successful completion
    stored = queue.get(job.job_id)
    assert stored.status == JobStatus.SUCCEEDED
    assert stored.attempts == 1
    assert redis_client.zscore("dtq:in-flight", job.job_id) is None
    assert redis_client.hget("dtq:in-flight:owners", job.job_id) is None
    assert queue.stats().in_flight_jobs == 0


@pytest.mark.asyncio
async def test_retry_cleans_up_in_flight_and_schedules_delayed(redis_client) -> None:
    queue = JobQueue(redis_client, lease_timeout_seconds=30.0)
    job = queue.submit(JobCreate(task_name="always_fail", payload={}, max_retries=2))

    worker = make_test_worker(queue, worker_id="worker-retry")
    processed = await worker.process_next()
    assert processed is True

    stored = queue.get(job.job_id)
    assert stored.status == JobStatus.RETRYING
    assert stored.attempts == 1
    assert redis_client.zscore("dtq:in-flight", job.job_id) is None
    assert redis_client.hget("dtq:in-flight:owners", job.job_id) is None
    assert redis_client.zscore("dtq:delayed", job.job_id) is not None
    assert queue.stats().in_flight_jobs == 0


@pytest.mark.asyncio
async def test_dead_letter_cleans_up_in_flight(redis_client) -> None:
    queue = JobQueue(redis_client, lease_timeout_seconds=30.0)
    job = queue.submit(JobCreate(task_name="always_fail", payload={}, max_retries=0))

    worker = make_test_worker(queue, worker_id="worker-dead")
    processed = await worker.process_next()
    assert processed is True

    stored = queue.get(job.job_id)
    assert stored.status == JobStatus.DEAD
    assert redis_client.zscore("dtq:in-flight", job.job_id) is None
    assert redis_client.hget("dtq:in-flight:owners", job.job_id) is None
    assert redis_client.lrange("dtq:dead-letter", 0, -1) == [job.job_id]
    assert queue.stats().in_flight_jobs == 0


@pytest.mark.asyncio
async def test_crashed_worker_recovery_and_resumption(redis_client) -> None:
    queue = JobQueue(redis_client, lease_timeout_seconds=30.0)
    job = queue.submit(JobCreate(task_name="recoverable_task", payload={"duration_seconds": 0}))

    # Worker 1 dequeues and marks running, then crashes
    dequeued = queue.dequeue(worker_id="crashed-worker")
    assert dequeued is not None
    assert dequeued.job_id == job.job_id
    queue.mark_running(dequeued, worker_id="crashed-worker")

    # In-flight record exists
    assert redis_client.zscore("dtq:in-flight", job.job_id) is not None
    assert redis_client.hget("dtq:in-flight:owners", job.job_id) == "crashed-worker"
    assert queue.stats().in_flight_jobs == 1

    # Before lease expires, recovery must not touch the job
    assert queue.recover_abandoned_jobs() == 0

    # Simulate lease expiration (e.g. worker process crashed and lease timed out)
    redis_client.zadd("dtq:in-flight", {job.job_id: 0})

    # Worker 2 recovers the abandoned job
    recovered_count = queue.recover_abandoned_jobs()
    assert recovered_count == 1

    # Job is back in ready queue as QUEUED, and removed from in-flight
    stored = queue.get(job.job_id)
    assert stored.status == JobStatus.QUEUED
    assert redis_client.zscore("dtq:in-flight", job.job_id) is None
    assert redis_client.hget("dtq:in-flight:owners", job.job_id) is None

    # Worker 2 processes the job to successful completion
    worker2 = make_test_worker(queue, worker_id="surviving-worker")
    processed = await worker2.process_next()
    assert processed is True

    final_job = queue.get(job.job_id)
    assert final_job.status == JobStatus.SUCCEEDED
    assert final_job.attempts == 2


def test_prevent_simultaneous_recovery_by_two_workers(redis_client) -> None:
    queue = JobQueue(redis_client, lease_timeout_seconds=30.0)
    job = queue.submit(JobCreate(task_name="concurrent_test", payload={}))

    # Dequeue job and mark in-flight
    dequeued = queue.dequeue(worker_id="w-initial")
    assert dequeued is not None

    # Expire lease
    redis_client.zadd("dtq:in-flight", {job.job_id: 0})

    # Worker A recovers
    recovered_a = queue.recover_abandoned_jobs()
    assert recovered_a == 1

    # Worker B tries to recover at the same time (lease already resolved)
    recovered_b = queue.recover_abandoned_jobs()
    assert recovered_b == 0

    # Ensure queue only has exactly 1 copy of the job ID
    queue_items = redis_client.lrange("dtq:queue:medium", 0, -1)
    assert queue_items == [job.job_id]


@pytest.mark.asyncio
async def test_repeated_worker_crashes_move_to_dead_letter(redis_client) -> None:
    # max_retries=1: 1 initial attempt + 1 retry allowed
    queue = JobQueue(redis_client, lease_timeout_seconds=30.0)
    job = queue.submit(JobCreate(task_name="poison_pill", payload={}, max_retries=1))

    # Crash 1: Worker dequeues, marks running (attempt 1), crashes
    j1 = queue.dequeue(worker_id="crash-1")
    assert j1 is not None
    queue.mark_running(j1, worker_id="crash-1")
    redis_client.zadd("dtq:in-flight", {job.job_id: 0})
    assert queue.recover_abandoned_jobs() == 1

    # Crash 2: Worker dequeues, marks running (attempt 2), crashes
    j2 = queue.dequeue(worker_id="crash-2")
    assert j2 is not None
    queue.mark_running(j2, worker_id="crash-2")
    redis_client.zadd("dtq:in-flight", {job.job_id: 0})

    # Recovery 2: attempts (2) > max_retries (1) -> moved to DEAD letter
    assert queue.recover_abandoned_jobs() == 1

    stored = queue.get(job.job_id)
    assert stored.status == JobStatus.DEAD
    assert "exceeded max retries" in stored.error_message
    assert redis_client.lrange("dtq:dead-letter", 0, -1) == [job.job_id]
    assert redis_client.zscore("dtq:in-flight", job.job_id) is None


def test_lease_renewal_prevents_premature_recovery(redis_client) -> None:
    queue = JobQueue(redis_client, lease_timeout_seconds=10.0)
    job = queue.submit(JobCreate(task_name="long_task", payload={}))

    dequeued = queue.dequeue(worker_id="w-long")
    assert dequeued is not None

    initial_score = redis_client.zscore("dtq:in-flight", job.job_id)
    assert initial_score is not None

    # Renew lease
    renewed = queue.renew_lease(job.job_id, worker_id="w-long")
    assert renewed is True

    new_score = redis_client.zscore("dtq:in-flight", job.job_id)
    assert new_score is not None
    assert new_score >= initial_score

    # Cannot renew with wrong worker ID
    assert queue.renew_lease(job.job_id, worker_id="wrong-worker") is False


def test_recovery_cleans_up_orphaned_in_flight_when_job_metadata_deleted(redis_client) -> None:
    queue = JobQueue(redis_client, lease_timeout_seconds=30.0)
    job = queue.submit(JobCreate(task_name="deleted_metadata", payload={}))

    dequeued = queue.dequeue(worker_id="w1")
    assert dequeued is not None

    # Artificially delete job metadata in Redis
    redis_client.delete(f"dtq:job:{job.job_id}")

    # Expire lease
    redis_client.zadd("dtq:in-flight", {job.job_id: 0})

    # Recovery should clean up the orphan without error and without re-queueing
    assert queue.recover_abandoned_jobs() == 0
    assert redis_client.zscore("dtq:in-flight", job.job_id) is None
    assert redis_client.hget("dtq:in-flight:owners", job.job_id) is None


def test_recovery_cleans_up_already_terminal_job_without_requeueing(redis_client) -> None:
    queue = JobQueue(redis_client, lease_timeout_seconds=30.0)
    job = queue.submit(JobCreate(task_name="already_done", payload={}))

    dequeued = queue.dequeue(worker_id="w1")
    assert dequeued is not None

    # Simulate job reached SUCCEEDED in metadata but was left in in-flight
    job.status = JobStatus.SUCCEEDED
    redis_client.set(f"dtq:job:{job.job_id}", job.model_dump_json())
    redis_client.zadd("dtq:in-flight", {job.job_id: 0})

    # Recovery should clean up from in-flight and not push to ready queue
    assert queue.recover_abandoned_jobs() == 0
    assert redis_client.zscore("dtq:in-flight", job.job_id) is None
    assert redis_client.llen("dtq:queue:medium") == 0


def test_crash_before_mark_running_recovered_with_zero_attempts(redis_client) -> None:
    queue = JobQueue(redis_client, lease_timeout_seconds=30.0)
    job = queue.submit(JobCreate(task_name="crash_early", payload={}))

    # Worker crashes immediately after dequeue before mark_running is called
    dequeued = queue.dequeue(worker_id="w-early")
    assert dequeued is not None
    # attempts is 0
    assert dequeued.attempts == 0

    # Expire lease
    redis_client.zadd("dtq:in-flight", {job.job_id: 0})

    # Recovered
    assert queue.recover_abandoned_jobs() == 1
    recovered_job = queue.get(job.job_id)
    assert recovered_job.status == JobStatus.QUEUED
    assert recovered_job.attempts == 0
