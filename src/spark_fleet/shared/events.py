"""Typed event bus and Server-Sent Events formatting.

The controller publishes state changes (node health, jobs, deployments,
metrics) to any number of SSE subscribers. Each subscriber has a bounded queue:
when it falls behind, the oldest queued event is dropped and counted rather than
blocking the publisher. Closing the bus terminates every stream.
"""

from __future__ import annotations

import asyncio
import json
import time
from collections.abc import AsyncIterator, Mapping
from dataclasses import dataclass, field
from typing import Any

_CLOSED = object()


@dataclass(frozen=True)
class Event:
    type: str
    data: Mapping[str, Any]
    ts: float = field(default_factory=time.time)
    id: str | None = None

    def __post_init__(self) -> None:
        if not self.type:
            raise ValueError("event type must not be empty")
        if "\n" in self.type or "\r" in self.type:
            raise ValueError(f"invalid event type: {self.type!r}")
        if self.id is not None and ("\n" in self.id or "\r" in self.id):
            raise ValueError(f"invalid event id: {self.id!r}")
        if not isinstance(self.data, Mapping):
            raise ValueError("event data must be a mapping")
        data = dict(self.data)
        try:
            json.dumps(data, sort_keys=True, separators=(",", ":"), allow_nan=False)
        except (TypeError, ValueError) as exc:
            raise ValueError(f"event data is not JSON-serializable: {exc}") from exc
        object.__setattr__(self, "data", data)


def format_sse(event: Event) -> str:
    """Format an event as one SSE frame."""

    if "\n" in event.type or "\r" in event.type:
        raise ValueError(f"invalid event type: {event.type!r}")
    if event.id is not None and ("\n" in event.id or "\r" in event.id):
        raise ValueError(f"invalid event id: {event.id!r}")

    lines: list[str] = []
    if event.id is not None:
        lines.append(f"id: {event.id}")
    lines.append(f"event: {event.type}")
    payload = json.dumps(dict(event.data), sort_keys=True, separators=(",", ":"), allow_nan=False)
    lines.append(f"data: {payload}")
    return "\n".join(lines) + "\n\n"


class EventBus:
    """Fan-out bus with bounded per-subscriber queues."""

    def __init__(self, queue_size: int = 256):
        if queue_size < 1:
            raise ValueError("queue_size must be positive")
        self._queue_size = queue_size
        self._subscribers: set[asyncio.Queue[Any]] = set()
        self._closed = False
        self._dropped = 0

    @property
    def subscriber_count(self) -> int:
        return len(self._subscribers)

    @property
    def dropped(self) -> int:
        return self._dropped

    async def publish(self, event: Event) -> None:
        if self._closed:
            return
        for queue in list(self._subscribers):
            try:
                queue.put_nowait(event)
            except asyncio.QueueFull:
                try:
                    queue.get_nowait()
                except asyncio.QueueEmpty:
                    pass
                self._dropped += 1
                queue.put_nowait(event)

    async def subscribe(self) -> AsyncIterator[Event]:
        """Yield events until the bus closes or the consumer stops iterating."""

        if self._closed:
            return

        queue: asyncio.Queue[Any] = asyncio.Queue(maxsize=self._queue_size)
        self._subscribers.add(queue)
        try:
            while True:
                item = await queue.get()
                if item is _CLOSED:
                    return
                yield item
        finally:
            self._subscribers.discard(queue)

    async def sse_stream(self, keepalive_seconds: float = 15.0) -> AsyncIterator[str]:
        """Yield formatted SSE frames, with comment keepalives while idle."""

        if keepalive_seconds <= 0:
            raise ValueError("keepalive_seconds must be positive")
        if self._closed:
            return

        queue: asyncio.Queue[Any] = asyncio.Queue(maxsize=self._queue_size)
        self._subscribers.add(queue)
        try:
            while True:
                try:
                    item = await asyncio.wait_for(queue.get(), timeout=keepalive_seconds)
                except TimeoutError:
                    yield ": keepalive\n\n"
                    continue
                if item is _CLOSED:
                    return
                yield format_sse(item)
        finally:
            self._subscribers.discard(queue)

    async def close(self) -> None:
        """Stop accepting events and terminate every subscriber stream."""

        if self._closed:
            return
        self._closed = True
        for queue in list(self._subscribers):
            while True:
                try:
                    queue.put_nowait(_CLOSED)
                    break
                except asyncio.QueueFull:
                    try:
                        queue.get_nowait()
                        self._dropped += 1
                    except asyncio.QueueEmpty:
                        break
