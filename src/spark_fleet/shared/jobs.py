"""Job runner: queue, worker pool, lifecycle locks, cancellation, recovery.

Jobs are persisted in the SQLite store, executed by a fixed worker pool, and
serialized by named lifecycle locks (for example ``deployment:<name>`` or
``node:<id>``). Every transition is written as a job event and published on the
event bus. A restart marks jobs left pending/running by the previous process as
failed.
"""

from __future__ import annotations

import asyncio
import time
import uuid
from collections.abc import Awaitable, Callable, Sequence
from contextlib import asynccontextmanager
from dataclasses import dataclass
from typing import Any

from spark_fleet.shared.db import Database
from spark_fleet.shared.events import Event, EventBus

JobHandler = Callable[["Job"], Awaitable[Any]]


@dataclass(frozen=True)
class Job:
    id: str
    kind: str
    payload: Any
    deployment_id: str | None


class LockManager:
    """Named asyncio locks acquired in sorted order to avoid deadlocks."""

    def __init__(self) -> None:
        self._locks: dict[str, asyncio.Lock] = {}

    def _lock(self, key: str) -> asyncio.Lock:
        lock = self._locks.get(key)
        if lock is None:
            lock = asyncio.Lock()
            self._locks[key] = lock
        return lock

    @asynccontextmanager
    async def acquire(self, keys: Sequence[str]):
        locks = [self._lock(key) for key in sorted(set(keys))]
        acquired: list[asyncio.Lock] = []
        try:
            for lock in locks:
                await lock.acquire()
                acquired.append(lock)
            yield
        finally:
            for lock in reversed(acquired):
                lock.release()


class JobRunner:
    def __init__(
        self,
        db: Database,
        bus: EventBus | None = None,
        *,
        pool_size: int = 1,
        queue_size: int = 0,
    ):
        if pool_size < 1:
            raise ValueError("pool_size must be positive")
        self._db = db
        self._bus = bus
        self._pool_size = pool_size
        self._queue: asyncio.Queue[Any] = asyncio.Queue(maxsize=queue_size)
        self._handlers: dict[str, JobHandler] = {}
        self._locks = LockManager()
        self._workers: list[asyncio.Task] = []
        self._current: dict[str, asyncio.Task] = {}
        self._cancelled: set[str] = set()
        self._started = False
        self._stopping = False

    def register(self, kind: str, handler: JobHandler) -> None:
        self._handlers[kind] = handler

    async def start(self) -> None:
        if self._started:
            return
        interrupted = self._db.list_jobs_by_status(("pending", "running"))
        self._db.mark_interrupted_jobs_failed()
        self._cancelled.clear()
        self._started = True
        self._stopping = False
        for row in interrupted:
            self._db.append_job_event(row["id"], "failed", level="error", phase="failed")
            await self._publish(
                Job(row["id"], row["kind"], row["payload"], row["deployment_id"]),
                "failed",
                "interrupted by restart",
            )
        self._workers = [
            asyncio.create_task(self._worker(), name=f"job-worker-{index}")
            for index in range(self._pool_size)
        ]

    async def submit(
        self,
        kind: str,
        payload: Any = None,
        *,
        job_id: str | None = None,
        deployment_id: str | None = None,
        locks: Sequence[str] = (),
    ) -> str:
        if not self._started or self._stopping:
            raise RuntimeError("job runner is not running")

        job_id = job_id or uuid.uuid4().hex
        if self._queue.full():
            raise RuntimeError("job queue is full")

        self._db.create_job(job_id, kind, deployment_id=deployment_id, payload=payload)
        self._db.append_job_event(job_id, "queued", phase="queued")
        await self._publish(
            Job(job_id, kind, payload, deployment_id),
            "pending",
            "queued",
        )
        self._queue.put_nowait((job_id, tuple(locks)))
        return job_id

    async def cancel(self, job_id: str) -> bool:
        row = self._db.get_job(job_id)
        if row is None or row["status"] not in ("pending", "running"):
            return False

        self._cancelled.add(job_id)
        task = self._current.get(job_id)
        if task is not None:
            task.cancel()
            return True

        job = Job(job_id, row["kind"], row["payload"], row["deployment_id"])
        await self._finish(job, "cancelled", error="cancelled")
        return True

    async def stop(self) -> None:
        if not self._started:
            return
        self._stopping = True

        while not self._queue.empty():
            item = self._queue.get_nowait()
            if item is None:
                continue
            job_id, _ = item
            row = self._db.get_job(job_id)
            if row is not None and row["status"] == "pending":
                job = Job(job_id, row["kind"], row["payload"], row["deployment_id"])
                await self._finish(job, "cancelled", error="runner stopped")

        for worker in self._workers:
            worker.cancel()
        await asyncio.gather(*self._workers, return_exceptions=True)

        self._workers.clear()
        self._started = False

    async def _worker(self) -> None:
        while True:
            item = await self._queue.get()
            if item is None:
                return
            job_id, locks = item
            self._current[job_id] = asyncio.current_task()
            try:
                await self._run(job_id, locks)
            except asyncio.CancelledError:
                if self._stopping:
                    raise
            finally:
                self._current.pop(job_id, None)
            if self._stopping:
                return

    async def _run(self, job_id: str, locks: Sequence[str]) -> None:
        row = self._db.get_job(job_id)
        if row is None or row["status"] != "pending":
            return

        job = Job(job_id, row["kind"], row["payload"], row["deployment_id"])
        self._db.update_job(job_id, status="running", started_at=time.time())
        self._db.append_job_event(job_id, "started", phase="running")
        await self._publish(job, "running", "started")

        try:
            async with self._locks.acquire(locks):
                handler = self._handlers.get(job.kind)
                if handler is None:
                    raise ValueError(f"unknown job kind: {job.kind}")
                result = await handler(job)
        except asyncio.CancelledError:
            if job_id in self._cancelled:
                await self._finish(job, "cancelled", error="cancelled")
                if self._stopping:
                    raise
                return
            await self._finish(job, "failed", error="runner stopped")
            raise
        except Exception as exc:
            await self._finish(job, "failed", error=str(exc))
        else:
            if job_id in self._cancelled:
                await self._finish(job, "cancelled", error="cancelled")
            elif self._stopping:
                await self._finish(job, "failed", error="runner stopped")
            else:
                await self._finish(job, "succeeded", result=result)

    async def _finish(
        self,
        job: Job,
        status: str,
        *,
        result: Any = None,
        error: str | None = None,
    ) -> None:
        values: dict[str, Any] = {"status": status, "finished_at": time.time()}
        if status == "succeeded":
            values["result"] = result
        if error is not None:
            values["error"] = error
        self._db.update_job(job.id, **values)

        level = "error" if status in ("failed", "cancelled") else "info"
        self._db.append_job_event(job.id, status, level=level, phase=status)
        await self._publish(job, status, status)
        self._cancelled.discard(job.id)

    async def _publish(self, job: Job, status: str, message: str) -> None:
        if self._bus is None:
            return
        await self._bus.publish(
            Event(
                "job",
                {
                    "id": job.id,
                    "kind": job.kind,
                    "status": status,
                    "message": message,
                },
            )
        )
