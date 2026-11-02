"""Tests for the job runner, lifecycle locks, cancellation, and recovery."""

import asyncio
import time

import pytest

from spark_fleet.shared.db import Database
from spark_fleet.shared.events import EventBus
from spark_fleet.shared.jobs import JobRunner


@pytest.fixture
def db(tmp_path):
    database = Database(tmp_path / "fleet.sqlite")
    yield database
    database.close()


@pytest.fixture
def bus():
    return EventBus()


async def wait_status(db, job_id, status, timeout=2.0):
    deadline = asyncio.get_running_loop().time() + timeout
    while asyncio.get_running_loop().time() < deadline:
        job = db.get_job(job_id)
        if job is not None and job["status"] == status:
            return job
        await asyncio.sleep(0.005)
    raise AssertionError(f"job {job_id} did not reach {status}: {db.get_job(job_id)}")


async def test_submit_runs_handler(db, bus):
    runner = JobRunner(db, bus, pool_size=1)
    seen = []

    async def handler(job):
        seen.append((job.kind, job.payload))
        return {"ok": True}

    runner.register("demo", handler)
    await runner.start()

    job_id = await runner.submit("demo", {"x": 1})
    job = await wait_status(db, job_id, "succeeded")

    assert job["result"] == {"ok": True}
    assert seen == [("demo", {"x": 1})]
    messages = [event["message"] for event in db.list_job_events(job_id)]
    assert messages == ["queued", "started", "succeeded"]

    await runner.stop()


async def test_handler_failure_marks_job_failed(db, bus):
    runner = JobRunner(db, bus, pool_size=1)

    async def handler(job):
        raise RuntimeError("boom")

    runner.register("fail", handler)
    await runner.start()

    job_id = await runner.submit("fail")
    job = await wait_status(db, job_id, "failed")

    assert job["error"] == "boom"
    events = db.list_job_events(job_id)
    assert events[-1]["message"] == "failed"
    assert events[-1]["level"] == "error"

    await runner.stop()


async def test_unknown_job_kind_fails(db, bus):
    runner = JobRunner(db, bus, pool_size=1)
    await runner.start()

    job_id = await runner.submit("missing")
    job = await wait_status(db, job_id, "failed")

    assert "unknown job kind" in job["error"]

    await runner.stop()


async def test_pool_runs_jobs_concurrently(db, bus):
    runner = JobRunner(db, bus, pool_size=2)
    started = asyncio.Event()
    release = asyncio.Event()
    active = 0

    async def handler(job):
        nonlocal active
        active += 1
        if active == 2:
            started.set()
        await release.wait()

    runner.register("wait", handler)
    await runner.start()

    job_ids = [await runner.submit("wait") for _ in range(2)]
    await asyncio.wait_for(started.wait(), timeout=1)
    assert active == 2

    release.set()
    for job_id in job_ids:
        await wait_status(db, job_id, "succeeded")

    await runner.stop()


async def test_single_worker_serializes_jobs(db, bus):
    runner = JobRunner(db, bus, pool_size=1)
    first_started = asyncio.Event()
    release = asyncio.Event()
    active = 0
    peak = 0

    async def handler(job):
        nonlocal active, peak
        active += 1
        peak = max(peak, active)
        first_started.set()
        await release.wait()
        active -= 1

    runner.register("wait", handler)
    await runner.start()

    job_ids = [await runner.submit("wait") for _ in range(2)]
    await asyncio.wait_for(first_started.wait(), timeout=1)
    await asyncio.sleep(0.05)

    assert peak == 1

    release.set()
    for job_id in job_ids:
        await wait_status(db, job_id, "succeeded")

    await runner.stop()


async def test_shared_locks_serialize_jobs(db, bus):
    runner = JobRunner(db, bus, pool_size=2)
    started = asyncio.Event()
    release = asyncio.Event()
    active = 0
    peak = 0

    async def handler(job):
        nonlocal active, peak
        active += 1
        peak = max(peak, active)
        started.set()
        await release.wait()
        active -= 1

    runner.register("wait", handler)
    await runner.start()

    job_ids = [
        await runner.submit("wait", locks=["deployment:demo-glm"]),
        await runner.submit("wait", locks=["deployment:demo-glm"]),
    ]
    await asyncio.wait_for(started.wait(), timeout=1)
    await asyncio.sleep(0.05)

    assert peak == 1

    release.set()
    for job_id in job_ids:
        await wait_status(db, job_id, "succeeded")

    await runner.stop()


async def test_different_locks_run_concurrently(db, bus):
    runner = JobRunner(db, bus, pool_size=2)
    both = asyncio.Event()
    release = asyncio.Event()
    active = 0
    peak = 0

    async def handler(job):
        nonlocal active, peak
        active += 1
        peak = max(peak, active)
        if active == 2:
            both.set()
        await release.wait()
        active -= 1

    runner.register("wait", handler)
    await runner.start()

    job_ids = [
        await runner.submit("wait", locks=["node:spark-01"]),
        await runner.submit("wait", locks=["node:spark-02"]),
    ]
    await asyncio.wait_for(both.wait(), timeout=1)
    assert peak == 2

    release.set()
    for job_id in job_ids:
        await wait_status(db, job_id, "succeeded")

    await runner.stop()


async def test_cancel_queued_job(db, bus):
    runner = JobRunner(db, bus, pool_size=1)
    started = asyncio.Event()
    release = asyncio.Event()

    async def handler(job):
        started.set()
        await release.wait()

    runner.register("wait", handler)
    await runner.start()

    first = await runner.submit("wait")
    await asyncio.wait_for(started.wait(), timeout=1)
    second = await runner.submit("wait")

    assert await runner.cancel(second) is True
    assert db.get_job(second)["status"] == "cancelled"

    release.set()
    await wait_status(db, first, "succeeded")
    assert db.get_job(second)["status"] == "cancelled"

    await runner.stop()


async def test_cancel_running_job(db, bus):
    runner = JobRunner(db, bus, pool_size=1)
    interrupted = asyncio.Event()

    async def handler(job):
        try:
            await asyncio.sleep(30)
        except asyncio.CancelledError:
            interrupted.set()
            raise

    runner.register("sleep", handler)
    await runner.start()

    job_id = await runner.submit("sleep")
    await wait_status(db, job_id, "running")

    assert await runner.cancel(job_id) is True
    await asyncio.wait_for(interrupted.wait(), timeout=1)
    job = await wait_status(db, job_id, "cancelled")
    assert job["error"] == "cancelled"

    await runner.stop()


async def test_cancel_then_stop_does_not_hang(db, bus):
    runner = JobRunner(db, bus, pool_size=1)
    started = asyncio.Event()

    async def handler(job):
        started.set()
        await asyncio.sleep(30)

    runner.register("sleep", handler)
    await runner.start()

    job_id = await runner.submit("sleep")
    await asyncio.wait_for(started.wait(), timeout=1)

    assert await runner.cancel(job_id) is True
    await asyncio.wait_for(runner.stop(), timeout=1)

    assert db.get_job(job_id)["status"] == "cancelled"


async def test_cancel_wins_when_handler_swallows_cancellation(db, bus):
    runner = JobRunner(db, bus, pool_size=1)
    started = asyncio.Event()

    async def handler(job):
        started.set()
        try:
            await asyncio.sleep(30)
        except asyncio.CancelledError:
            return {"ignored": True}

    runner.register("stubborn", handler)
    await runner.start()

    job_id = await runner.submit("stubborn")
    await asyncio.wait_for(started.wait(), timeout=1)

    assert await runner.cancel(job_id) is True
    job = await wait_status(db, job_id, "cancelled")
    assert job["error"] == "cancelled"
    assert job["result"] is None

    await asyncio.wait_for(runner.stop(), timeout=1)


async def test_stop_wins_when_handler_swallows_cancellation(db, bus):
    runner = JobRunner(db, bus, pool_size=1)
    started = asyncio.Event()

    async def handler(job):
        started.set()
        try:
            await asyncio.sleep(30)
        except asyncio.CancelledError:
            return {"ignored": True}

    runner.register("stubborn", handler)
    await runner.start()

    job_id = await runner.submit("stubborn")
    await asyncio.wait_for(started.wait(), timeout=1)

    await asyncio.wait_for(runner.stop(), timeout=1)

    job = db.get_job(job_id)
    assert job["status"] == "failed"
    assert job["error"] == "runner stopped"


async def test_cancel_terminal_job_returns_false(db, bus):
    runner = JobRunner(db, bus, pool_size=1)

    async def handler(job):
        return None

    runner.register("demo", handler)
    await runner.start()

    job_id = await runner.submit("demo")
    await wait_status(db, job_id, "succeeded")

    assert await runner.cancel(job_id) is False

    await runner.stop()


async def test_start_recovers_interrupted_jobs(db, bus):
    db.create_job("pending", "load")
    db.create_job("running", "load")
    db.update_job("running", status="running")
    db.create_job("done", "load")
    db.update_job("done", status="succeeded")

    events = []

    async def collect():
        async for event in bus.subscribe():
            events.append(event)

    collector = asyncio.create_task(collect())
    await asyncio.sleep(0)

    runner = JobRunner(db, bus, pool_size=1)
    await runner.start()
    await asyncio.sleep(0)
    await runner.stop()

    for job_id in ("pending", "running"):
        job = db.get_job(job_id)
        assert job["status"] == "failed"
        assert job["error"] == "interrupted by restart"
        assert db.list_job_events(job_id)[-1]["message"] == "failed"
    assert db.get_job("done")["status"] == "succeeded"

    recovered = {
        event.data["id"]
        for event in events
        if event.type == "job" and event.data["status"] == "failed"
    }
    assert recovered == {"pending", "running"}

    collector.cancel()
    with pytest.raises(asyncio.CancelledError):
        await collector


async def test_recovery_reaches_jobs_beyond_list_limit(db, bus):
    now = time.time()
    with db._conn:
        db._conn.executemany(
            "INSERT INTO jobs (id, kind, status, created_at) VALUES (?, ?, ?, ?)",
            [(f"bulk-{index}", "load", "succeeded", now + index) for index in range(10_001)],
        )
    db.create_job("old-pending", "load", now=now - 1)

    events = []

    async def collect():
        async for event in bus.subscribe():
            events.append(event)

    collector = asyncio.create_task(collect())
    await asyncio.sleep(0)

    runner = JobRunner(db, bus, pool_size=1)
    await runner.start()
    await asyncio.sleep(0)
    await runner.stop()

    assert db.get_job("old-pending")["status"] == "failed"
    assert any(event.type == "job" and event.data["id"] == "old-pending" for event in events)

    collector.cancel()
    with pytest.raises(asyncio.CancelledError):
        await collector


async def test_stop_marks_running_job_failed(db, bus):
    runner = JobRunner(db, bus, pool_size=1)
    started = asyncio.Event()
    interrupted = asyncio.Event()

    async def handler(job):
        started.set()
        try:
            await asyncio.sleep(30)
        except asyncio.CancelledError:
            interrupted.set()
            raise

    runner.register("sleep", handler)
    await runner.start()

    job_id = await runner.submit("sleep")
    await asyncio.wait_for(started.wait(), timeout=1)

    await runner.stop()

    assert interrupted.is_set()
    job = db.get_job(job_id)
    assert job["status"] == "failed"
    assert job["error"] == "runner stopped"


async def test_submit_rejects_when_queue_full(db, bus):
    runner = JobRunner(db, bus, pool_size=1, queue_size=1)
    started = asyncio.Event()
    release = asyncio.Event()

    async def handler(job):
        started.set()
        await release.wait()

    runner.register("wait", handler)
    await runner.start()

    first = await runner.submit("wait")
    await asyncio.wait_for(started.wait(), timeout=1)
    second = await runner.submit("wait")

    with pytest.raises(RuntimeError, match="queue is full"):
        await runner.submit("wait")

    release.set()
    await wait_status(db, first, "succeeded")
    await wait_status(db, second, "succeeded")

    await runner.stop()
    runner = JobRunner(db, bus, pool_size=1)
    await runner.start()
    await runner.stop()

    with pytest.raises(RuntimeError, match="not running"):
        await runner.submit("demo")


async def test_job_transitions_are_published(db, bus):
    events = []

    async def collect():
        async for event in bus.subscribe():
            events.append(event)

    collector = asyncio.create_task(collect())
    await asyncio.sleep(0)

    runner = JobRunner(db, bus, pool_size=1)

    async def handler(job):
        return {"ok": True}

    runner.register("demo", handler)
    await runner.start()

    job_id = await runner.submit("demo")
    await wait_status(db, job_id, "succeeded")
    await asyncio.sleep(0)

    statuses = [event.data["status"] for event in events if event.type == "job"]
    assert "running" in statuses
    assert "succeeded" in statuses

    collector.cancel()
    with pytest.raises(asyncio.CancelledError):
        await collector

    await runner.stop()
