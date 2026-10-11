from __future__ import annotations

from datetime import timedelta

from redis import Redis
from redis.exceptions import WatchError

from app.config import get_settings
from app.events import EventPublisher
from app.models import PRIORITY_ORDER, Job, JobCreate, JobStatus, Priority, QueueStats, utc_now

IN_FLIGHT_KEY = "dtq:in-flight"
IN_FLIGHT_OWNERS_KEY = "dtq:in-flight:owners"


class JobNotFoundError(Exception):
    pass


class JobNotDeadLetteredError(Exception):
    pass


class JobQueue:
    def __init__(self, redis: Redis, lease_timeout_seconds: float | None = None):
        self.redis = redis
        self.settings = get_settings()
        self.lease_timeout_seconds = (
            lease_timeout_seconds
            if lease_timeout_seconds is not None
            else getattr(self.settings, "worker_lease_timeout_seconds", 30.0)
        )
        self.events = EventPublisher(redis)

    def submit(self, request: JobCreate) -> Job:
        now = utc_now()
        status = JobStatus.DELAYED if request.delay_seconds > 0 else JobStatus.QUEUED
        job = Job(
            task_name=request.task_name,
            payload=request.payload,
            priority=request.priority,
            status=status,
            max_retries=request.max_retries,
            created_at=now,
            updated_at=now,
        )
        self._save_job(job)
        self._remember_job(job.job_id)
        if request.delay_seconds > 0:
            run_at = now + timedelta(seconds=request.delay_seconds)
            self.redis.zadd("dtq:delayed", {job.job_id: run_at.timestamp()})
            self.events.publish(
                job.job_id,
                JobStatus.DELAYED,
                "Job scheduled for delayed execution",
            )
        else:
            self._push_queue(job.priority, job.job_id)
            self.events.publish(job.job_id, JobStatus.QUEUED, "Job queued")
        return job

    def get(self, job_id: str) -> Job:
        raw = self.redis.get(self._job_key(job_id))
        if raw is None:
            raise JobNotFoundError(job_id)
        return Job.model_validate_json(raw)

    def list_recent(self, limit: int = 50) -> list[Job]:
        ids = self.redis.lrange("dtq:recent-jobs", 0, limit - 1)
        jobs: list[Job] = []
        for job_id in ids:
            try:
                jobs.append(self.get(job_id))
            except JobNotFoundError:
                continue
        return jobs

    def list_dead_letter(self, limit: int = 50, offset: int = 0) -> list[Job]:
        ids = self.redis.lrange("dtq:dead-letter", offset, offset + limit - 1)
        jobs: list[Job] = []
        seen: set[str] = set()
        for job_id in ids:
            if job_id in seen:
                continue
            seen.add(job_id)
            try:
                job = self.get(job_id)
                if job.status == JobStatus.DEAD:
                    jobs.append(job)
            except JobNotFoundError:
                continue
        return jobs

    def get_dead_letter(self, job_id: str) -> Job:
        job = self.get(job_id)
        if job.status != JobStatus.DEAD:
            raise JobNotDeadLetteredError(job_id)
        return job

    def replay(self, job_id: str) -> Job:
        job_key = self._job_key(job_id)
        pipe = self.redis.pipeline()
        while True:
            try:
                pipe.watch(job_key, "dtq:dead-letter")
                raw = pipe.get(job_key)
                if raw is None:
                    pipe.unwatch()
                    raise JobNotFoundError(job_id)

                job = Job.model_validate_json(raw)
                if job.status != JobStatus.DEAD:
                    pipe.unwatch()
                    raise JobNotDeadLetteredError(job_id)

                now = utc_now()
                job.status = JobStatus.QUEUED
                job.attempts = 0
                job.error_message = None
                job.started_at = None
                job.finished_at = None
                job.updated_at = now

                pipe.multi()
                pipe.set(job_key, job.model_dump_json())
                pipe.lrem("dtq:dead-letter", 0, job_id)
                pipe.zrem(IN_FLIGHT_KEY, job_id)
                pipe.hdel(IN_FLIGHT_OWNERS_KEY, job_id)
                pipe.lrem(self._queue_key(job.priority), 0, job.job_id)
                pipe.lpush(self._queue_key(job.priority), job.job_id)
                pipe.lpush("dtq:recent-jobs", job_id)
                pipe.ltrim("dtq:recent-jobs", 0, self.settings.recent_jobs_limit - 1)
                pipe.execute()

                self.events.publish(
                    job.job_id,
                    JobStatus.QUEUED,
                    f"Dead-lettered job replayed and re-queued with priority {job.priority}",
                )
                return job
            except WatchError:
                continue
            finally:
                pipe.reset()

    def stats(self) -> QueueStats:
        jobs = self.list_recent(self.settings.recent_jobs_limit)
        counts = {status: 0 for status in JobStatus}
        for job in jobs:
            counts[JobStatus(job.status)] += 1
        queues = {
            priority.value: self.redis.llen(self._queue_key(priority))
            for priority in PRIORITY_ORDER
        }
        queued_jobs = sum(queues.values())
        delayed_jobs = self.redis.zcard("dtq:delayed")
        in_flight_jobs = self.redis.zcard(IN_FLIGHT_KEY)
        return QueueStats(
            total_jobs=len(jobs),
            queued_jobs=queued_jobs,
            delayed_jobs=delayed_jobs,
            in_flight_jobs=in_flight_jobs,
            running_jobs=counts[JobStatus.RUNNING],
            succeeded_jobs=counts[JobStatus.SUCCEEDED],
            failed_jobs=counts[JobStatus.FAILED] + counts[JobStatus.RETRYING],
            dead_letter_jobs=self.redis.llen("dtq:dead-letter"),
            queues=queues,
        )

    def promote_due_jobs(self) -> int:
        now_score = utc_now().timestamp()
        job_ids = self.redis.zrangebyscore("dtq:delayed", "-inf", now_score)
        promoted = 0
        for job_id in job_ids:
            if not self.redis.zrem("dtq:delayed", job_id):
                continue
            try:
                job = self.get(job_id)
            except JobNotFoundError:
                continue
            job.status = JobStatus.QUEUED
            job.updated_at = utc_now()
            self._save_job(job)
            self._push_queue(job.priority, job.job_id)
            self.events.publish(job.job_id, JobStatus.QUEUED, "Delayed job moved to ready queue")
            promoted += 1
        return promoted

    def _dequeue_priority(self, priority: Priority, worker_id: str | None = None) -> str | None:
        queue_key = self._queue_key(priority)
        pipe = self.redis.pipeline()
        while True:
            try:
                pipe.watch(queue_key)
                job_id = pipe.lindex(queue_key, -1)
                if not job_id:
                    pipe.unwatch()
                    return None

                now = utc_now().timestamp()
                expires_at = now + self.lease_timeout_seconds

                pipe.multi()
                pipe.rpop(queue_key)
                pipe.zadd(IN_FLIGHT_KEY, {job_id: expires_at})
                if worker_id:
                    pipe.hset(IN_FLIGHT_OWNERS_KEY, job_id, worker_id)
                pipe.execute()
                return job_id
            except WatchError:
                continue
            finally:
                pipe.reset()

    def dequeue(self, worker_id: str | None = None) -> Job | None:
        self.promote_due_jobs()
        self.recover_abandoned_jobs()
        for priority in PRIORITY_ORDER:
            job_id = self._dequeue_priority(priority, worker_id=worker_id)
            if not job_id:
                continue
            try:
                return self.get(job_id)
            except JobNotFoundError:
                self._clear_in_flight(job_id)
                continue
        return None

    def renew_lease(self, job_id: str, worker_id: str | None = None) -> bool:
        if worker_id:
            owner = self.redis.hget(IN_FLIGHT_OWNERS_KEY, job_id)
            if owner is not None and owner != worker_id:
                return False
        if not self.redis.zscore(IN_FLIGHT_KEY, job_id):
            return False
        new_expires_at = utc_now().timestamp() + self.lease_timeout_seconds
        self.redis.zadd(IN_FLIGHT_KEY, {job_id: new_expires_at})
        return True

    def recover_abandoned_jobs(self, max_recoveries: int = 50) -> int:
        now_score = utc_now().timestamp()
        expired_job_ids = self.redis.zrangebyscore(
            IN_FLIGHT_KEY, "-inf", now_score, start=0, num=max_recoveries
        )
        if not expired_job_ids:
            return 0

        recovered_count = 0
        for job_id in expired_job_ids:
            pipe = self.redis.pipeline()
            while True:
                try:
                    pipe.watch(IN_FLIGHT_KEY)
                    score = pipe.zscore(IN_FLIGHT_KEY, job_id)
                    if score is None or score > now_score:
                        pipe.unwatch()
                        break

                    try:
                        job = self.get(job_id)
                    except JobNotFoundError:
                        pipe.multi()
                        pipe.zrem(IN_FLIGHT_KEY, job_id)
                        pipe.hdel(IN_FLIGHT_OWNERS_KEY, job_id)
                        pipe.execute()
                        break

                    if job.status in (JobStatus.SUCCEEDED, JobStatus.DEAD, JobStatus.FAILED):
                        pipe.multi()
                        pipe.zrem(IN_FLIGHT_KEY, job_id)
                        pipe.hdel(IN_FLIGHT_OWNERS_KEY, job_id)
                        pipe.execute()
                        break

                    previous_owner = self.redis.hget(IN_FLIGHT_OWNERS_KEY, job_id)

                    if job.attempts > job.max_retries:
                        pipe.multi()
                        pipe.zrem(IN_FLIGHT_KEY, job_id)
                        pipe.hdel(IN_FLIGHT_OWNERS_KEY, job_id)
                        pipe.lpush("dtq:dead-letter", job.job_id)
                        pipe.execute()

                        job.status = JobStatus.DEAD
                        job.error_message = (
                            f"Job abandoned by worker '{previous_owner or 'unknown'}' "
                            f"and exceeded max retries"
                        )
                        job.finished_at = utc_now()
                        job.updated_at = job.finished_at
                        self._save_job(job)
                        self.events.publish(
                            job.job_id,
                            JobStatus.DEAD,
                            "Abandoned job moved to dead-letter queue",
                        )
                        recovered_count += 1
                        break

                    pipe.multi()
                    pipe.zrem(IN_FLIGHT_KEY, job_id)
                    pipe.hdel(IN_FLIGHT_OWNERS_KEY, job_id)
                    pipe.lpush(self._queue_key(job.priority), job.job_id)
                    pipe.execute()

                    job.status = JobStatus.QUEUED
                    job.updated_at = utc_now()
                    self._save_job(job)
                    owner_info = previous_owner or "unknown"
                    self.events.publish(
                        job.job_id,
                        JobStatus.QUEUED,
                        f"Abandoned job recovered from worker '{owner_info}' and re-queued",
                    )
                    recovered_count += 1
                    break
                except WatchError:
                    continue
                finally:
                    pipe.reset()

        return recovered_count

    def mark_running(self, job: Job, worker_id: str | None = None) -> Job:
        job.status = JobStatus.RUNNING
        job.attempts += 1
        job.started_at = utc_now()
        job.updated_at = job.started_at
        job.error_message = None
        self._save_job(job)
        if worker_id:
            self.redis.hset(IN_FLIGHT_OWNERS_KEY, job.job_id, worker_id)
        self.events.publish(job.job_id, JobStatus.RUNNING, "Job started")
        return job

    def mark_succeeded(self, job: Job) -> Job:
        job.status = JobStatus.SUCCEEDED
        job.finished_at = utc_now()
        job.updated_at = job.finished_at
        job.error_message = None
        self._save_job(job)
        self._clear_in_flight(job.job_id)
        self.events.publish(job.job_id, JobStatus.SUCCEEDED, "Job completed successfully")
        return job

    def mark_failed_for_retry(self, job: Job, error: str, delay_seconds: int) -> Job:
        job.status = JobStatus.RETRYING
        job.error_message = error
        job.updated_at = utc_now()
        self._save_job(job)
        self._clear_in_flight(job.job_id)
        run_at = job.updated_at + timedelta(seconds=delay_seconds)
        self.redis.zadd("dtq:delayed", {job.job_id: run_at.timestamp()})
        self.events.publish(
            job.job_id,
            JobStatus.RETRYING,
            f"Job failed and will retry in {delay_seconds} seconds",
        )
        return job

    def mark_dead(self, job: Job, error: str) -> Job:
        job.status = JobStatus.DEAD
        job.error_message = error
        job.finished_at = utc_now()
        job.updated_at = job.finished_at
        self._save_job(job)
        self._clear_in_flight(job.job_id)
        pipe = self.redis.pipeline()
        pipe.lrem(self._queue_key(job.priority), 0, job.job_id)
        pipe.lpush("dtq:dead-letter", job.job_id)
        pipe.execute()
        self.events.publish(job.job_id, JobStatus.DEAD, "Job moved to dead-letter queue")
        return job

    def mark_failed(self, job: Job, error: str) -> Job:
        job.status = JobStatus.FAILED
        job.error_message = error
        job.finished_at = utc_now()
        job.updated_at = job.finished_at
        self._save_job(job)
        self._clear_in_flight(job.job_id)
        self.events.publish(job.job_id, JobStatus.FAILED, "Job failed")
        return job

    def _clear_in_flight(self, job_id: str) -> None:
        pipe = self.redis.pipeline()
        pipe.zrem(IN_FLIGHT_KEY, job_id)
        pipe.hdel(IN_FLIGHT_OWNERS_KEY, job_id)
        pipe.execute()

    def _save_job(self, job: Job) -> None:
        self.redis.set(self._job_key(job.job_id), job.model_dump_json())

    def _remember_job(self, job_id: str) -> None:
        pipe = self.redis.pipeline()
        pipe.lpush("dtq:recent-jobs", job_id)
        pipe.ltrim("dtq:recent-jobs", 0, self.settings.recent_jobs_limit - 1)
        pipe.execute()

    def _push_queue(self, priority: Priority | str, job_id: str) -> None:
        self.redis.lpush(self._queue_key(Priority(priority)), job_id)

    @staticmethod
    def _job_key(job_id: str) -> str:
        return f"dtq:job:{job_id}"

    @staticmethod
    def _queue_key(priority: Priority | str) -> str:
        value = priority.value if isinstance(priority, Priority) else str(priority)
        return f"dtq:queue:{value}"
