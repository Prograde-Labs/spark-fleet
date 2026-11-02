"""Tests for the event bus and SSE formatting."""

import asyncio
from types import MappingProxyType

import pytest

from spark_fleet.shared.events import Event, EventBus, format_sse


async def collect(bus, count):
    events = []
    async for event in bus.subscribe():
        events.append(event)
        if len(events) == count:
            break
    return events


async def test_publish_fans_out_to_all_subscribers():
    bus = EventBus()
    first = asyncio.create_task(collect(bus, 1))
    second = asyncio.create_task(collect(bus, 1))
    await asyncio.sleep(0)

    assert bus.subscriber_count == 2

    event = Event("memory", {"node": "spark-01"})
    await bus.publish(event)

    assert await first == [event]
    assert await second == [event]


async def test_subscriber_removed_when_stream_closes():
    bus = EventBus()
    stream = bus.subscribe()
    task = asyncio.create_task(anext(stream))
    await asyncio.sleep(0)
    assert bus.subscriber_count == 1

    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    await asyncio.sleep(0)

    assert bus.subscriber_count == 0


async def test_close_terminates_streams():
    bus = EventBus()

    async def drain():
        return [event async for event in bus.subscribe()]

    task = asyncio.create_task(drain())
    await asyncio.sleep(0)

    event = Event("job", {"id": "job-1"})
    await bus.publish(event)
    await bus.close()

    assert await task == [event]


async def test_close_is_idempotent():
    bus = EventBus()

    await bus.close()
    await bus.close()

    assert bus.subscriber_count == 0


async def test_publish_after_close_is_ignored():
    bus = EventBus()
    await bus.close()

    await bus.publish(Event("memory", {"node": "spark-01"}))

    assert bus.dropped == 0


async def test_keepalive_is_emitted_when_idle():
    bus = EventBus()
    stream = bus.sse_stream(keepalive_seconds=0.01)

    assert await anext(stream) == ": keepalive\n\n"

    await stream.aclose()
    assert bus.subscriber_count == 0


async def test_sse_stream_formats_events():
    bus = EventBus()
    stream = bus.sse_stream(keepalive_seconds=10)
    task = asyncio.create_task(anext(stream))
    await asyncio.sleep(0)

    await bus.publish(Event("load", {"name": "demo-glm"}, id="7"))
    chunk = await task

    assert chunk == 'id: 7\nevent: load\ndata: {"name":"demo-glm"}\n\n'

    await stream.aclose()


async def test_overflow_drops_oldest_event():
    bus = EventBus(queue_size=1)
    stream = bus.subscribe()
    task = asyncio.create_task(anext(stream))
    await asyncio.sleep(0)

    await bus.publish(Event("first", {}))
    await bus.publish(Event("second", {}))

    assert bus.dropped == 1
    assert (await task).type == "second"

    await stream.aclose()


def test_format_sse_without_id():
    assert format_sse(Event("memory", {"a": 1})) == 'event: memory\ndata: {"a":1}\n\n'


def test_event_has_default_timestamp():
    assert Event("memory", {}).ts > 0


def test_event_is_immutable():
    event = Event("memory", {})

    with pytest.raises(AttributeError):
        event.type = "other"


def test_zero_queue_size_rejected():
    with pytest.raises(ValueError, match="queue_size"):
        EventBus(queue_size=0)


async def test_nonpositive_keepalive_rejected():
    bus = EventBus()

    with pytest.raises(ValueError, match="keepalive"):
        await anext(bus.sse_stream(keepalive_seconds=0))


def test_event_rejects_newline_in_type():
    with pytest.raises(ValueError, match="invalid event type"):
        Event("bad\ntype", {})


def test_event_rejects_newline_in_id():
    with pytest.raises(ValueError, match="invalid event id"):
        Event("memory", {}, id="bad\nid")


def test_event_rejects_non_json_data():
    with pytest.raises(ValueError, match="JSON"):
        Event("memory", {"when": object()})


def test_event_normalizes_mapping_to_dict():
    event = Event("memory", MappingProxyType({"a": 1}))

    assert isinstance(event.data, dict)
    assert format_sse(event) == 'event: memory\ndata: {"a":1}\n\n'


def test_event_rejects_non_finite_numbers():
    with pytest.raises(ValueError, match="JSON"):
        Event("memory", {"nan": float("nan")})


async def test_publish_after_close_is_ignored_with_subscriber():
    bus = EventBus(queue_size=1)
    stream = bus.subscribe()
    task = asyncio.create_task(anext(stream))
    await asyncio.sleep(0)

    await bus.publish(Event("first", {}))
    await bus.close()
    await bus.publish(Event("after-close", {}))

    with pytest.raises(StopAsyncIteration):
        await asyncio.wait_for(task, timeout=1)
    assert bus.dropped == 1


async def test_close_is_idempotent_with_active_subscriber():
    bus = EventBus()

    async def drain():
        return [event async for event in bus.subscribe()]

    task = asyncio.create_task(drain())
    await asyncio.sleep(0)

    await bus.close()
    await bus.close()

    assert await task == []


async def test_sse_stream_terminates_on_close():
    bus = EventBus()
    stream = bus.sse_stream(keepalive_seconds=10)
    task = asyncio.create_task(anext(stream))
    await asyncio.sleep(0)

    await bus.close()

    with pytest.raises(StopAsyncIteration):
        await task


async def test_close_counts_dropped_queued_event():
    bus = EventBus(queue_size=1)
    stream = bus.subscribe()
    task = asyncio.create_task(anext(stream))
    await asyncio.sleep(0)

    await bus.publish(Event("first", {}))
    await bus.close()

    with pytest.raises(StopAsyncIteration):
        await task
    assert bus.dropped == 1


async def test_stalled_subscriber_does_not_block_others():
    bus = EventBus(queue_size=1)
    stalled = bus.subscribe()
    stalled_task = asyncio.create_task(anext(stalled))
    await asyncio.sleep(0)
    await bus.publish(Event("warm", {}))
    assert (await stalled_task).type == "warm"

    active = asyncio.create_task(collect(bus, 3))
    await asyncio.sleep(0)
    for index in range(5):
        await bus.publish(Event(f"e{index}", {}))
        await asyncio.sleep(0)

    events = await active

    assert [event.type for event in events] == ["e0", "e1", "e2"]
    assert bus.dropped > 0

    await stalled.aclose()
