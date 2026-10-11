from __future__ import annotations

import asyncio
import signal
from dataclasses import dataclass, field
from typing import Protocol
from uuid import uuid4

from redis import Redis

from app.config import get_settings
from app.logging_config import configure_logging, logger
from app.models import Job
from app.queue import JobQueue
from app.redis_client import get_redis


class TaskExecutor(Protocol):
    async def execute(self, job: Job) -> None:
        ...


class SimulatedTaskExecutor:
    async def execute(self, job: Job) -> None:
        duration = float(job.payload.get("duration_seconds", 0.05))
        await asyncio.sleep(max(duration, 0))

        fail_until_attempt = int(job.payload.get("fail_until_attempt", 0))
        if job.payload.get("fail") is True or job.task_name == "always_fail":
            raise RuntimeError("Simulated task failure")
        if fail_until_attempt >= job.attempts:
            raise RuntimeError(f"Simulated transient failure through attempt {fail_until_attempt}")


@dataclass
class Worker:
    queue: JobQueue
    executor: TaskExecutor
    concurrency: int
    poll_interval_seconds: float
    retry_backoff_base_seconds: int
    worker_id: str = field(default_factory=lambda: f"worker-{uuid4().hex[:8]}")
    lease_timeout_seconds: float = 30.0
    heartbeat_interval_seconds: float = 10.0
    _stopping: bool = False

    async def process_next(self, worker_id: str | None = None) -> bool:
        wid = worker_id or self.worker_id
        job = self.queue.dequeue(worker_id=wid)
        if job is None:
            return False

        job = self.queue.mark_running(job, worker_id=wid)
        heartbeat_task = asyncio.create_task(self._heartbeat_loop(job.job_id, wid))
        try:
            await self.executor.execute(job)
        except Exception as exc:  # noqa: BLE001 - task failures must be captured.
            heartbeat_task.cancel()
            error = str(exc)
            if job.attempts <= job.max_retries:
                delay = self.retry_backoff_base_seconds * (2 ** (job.attempts - 1))
                self.queue.mark_failed_for_retry(job, error, delay)
            else:
                self.queue.mark_dead(job, error)
            logger.warning("job_failed", job_id=job.job_id, attempts=job.attempts, error=error)
            return True
        else:
            heartbeat_task.cancel()

        self.queue.mark_succeeded(job)
        logger.info("job_succeeded", job_id=job.job_id, attempts=job.attempts)
        return True

    async def _heartbeat_loop(self, job_id: str, worker_id: str) -> None:
        try:
            while True:
                await asyncio.sleep(self.heartbeat_interval_seconds)
                self.queue.renew_lease(job_id, worker_id=worker_id)
        except asyncio.CancelledError:
            pass

    async def run(self) -> None:
        workers = [asyncio.create_task(self._run_loop(index)) for index in range(self.concurrency)]
        await asyncio.gather(*workers)

    async def _run_loop(self, index: int) -> None:
        wid = f"{self.worker_id}:{index}"
        logger.info("worker_loop_started", worker_index=index, worker_id=wid)
        while not self._stopping:
            processed = await self.process_next(worker_id=wid)
            if not processed:
                await asyncio.sleep(self.poll_interval_seconds)
        logger.info("worker_loop_stopped", worker_index=index, worker_id=wid)

    def stop(self) -> None:
        self._stopping = True


def build_worker(redis: Redis | None = None) -> Worker:
    settings = get_settings()
    redis_client = redis or get_redis()
    return Worker(
        queue=JobQueue(redis_client, lease_timeout_seconds=settings.worker_lease_timeout_seconds),
        executor=SimulatedTaskExecutor(),
        concurrency=settings.worker_concurrency,
        poll_interval_seconds=settings.worker_poll_interval_seconds,
        retry_backoff_base_seconds=settings.retry_backoff_base_seconds,
        lease_timeout_seconds=settings.worker_lease_timeout_seconds,
        heartbeat_interval_seconds=settings.worker_heartbeat_interval_seconds,
    )


async def main() -> None:
    settings = get_settings()
    configure_logging(settings.log_level)
    worker = build_worker()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, worker.stop)
        except (NotImplementedError, AttributeError):
            signal.signal(sig, lambda *_: worker.stop())
    logger.info("worker_started", concurrency=settings.worker_concurrency)
    await worker.run()


if __name__ == "__main__":
    asyncio.run(main())

