"""Controller metrics store: ingest agent snapshots and gateway counters.

Agent snapshots are flattened into ``node_metrics`` series (host memory,
container memory, cache usage and capacity, and per-GPU values). GPU residency is
stored as ``0``/``1`` and omitted when the agent cannot determine it. Gateway
counters are written as ``inference_samples`` per deployment. The store exposes
latest values, bounded range queries, aggregate inference totals, retention
pruning, and incremental vacuum for periodic maintenance.
"""

from __future__ import annotations

import time
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from typing import Any

from spark_fleet.shared.db import Database

HOST_TOTAL = "host_total_bytes"
HOST_AVAILABLE = "host_available_bytes"
CONTAINER_MEMORY = "container_memory_bytes"
CACHE_BYTES = "cache_bytes"
CACHE_TOTAL = "cache_total_bytes"
CACHE_FREE = "cache_free_bytes"
BASE_METRICS = (
    HOST_TOTAL,
    HOST_AVAILABLE,
    CONTAINER_MEMORY,
    CACHE_BYTES,
    CACHE_TOTAL,
    CACHE_FREE,
)
GPU_FIELDS = (
    "memory_total_bytes",
    "memory_used_bytes",
    "utilization_percent",
    "temperature_celsius",
    "compute_processes",
    "compute_memory_bytes",
    "resident",
)


@dataclass(frozen=True)
class InferenceTotals:
    requests: int
    errors: int
    prompt_tokens: int
    completion_tokens: int
    average_latency_ms: float | None
    average_ttft_ms: float | None


class MetricsStore:
    """Persist and query node and inference metric series."""

    def __init__(self, db: Database):
        self._db = db

    def record_node_snapshot(
        self, node_id: str, snapshot: Mapping[str, Any], *, ts: float | None = None
    ) -> None:
        if not node_id:
            raise ValueError("node id must not be empty")
        ts = time.time() if ts is None else ts
        rows: list[tuple[str, str, float, float]] = []
        cache_total = snapshot.get(CACHE_TOTAL)
        cache_free = snapshot.get(CACHE_FREE)
        capacity_ok = _is_number(cache_total) and _is_number(cache_free)
        for metric in BASE_METRICS:
            value = snapshot.get(metric)
            if metric in (CACHE_TOTAL, CACHE_FREE) and not capacity_ok:
                continue
            if _is_number(value):
                rows.append((node_id, metric, float(value), ts))
        gpus = snapshot.get("gpus")
        if isinstance(gpus, Sequence) and not isinstance(gpus, (str, bytes)):
            for gpu in gpus:
                if not isinstance(gpu, Mapping):
                    continue
                index = gpu.get("index")
                if isinstance(index, bool) or not isinstance(index, int):
                    continue
                for field in GPU_FIELDS:
                    value = gpu.get(field)
                    if field == "resident" and isinstance(value, bool):
                        value = int(value)
                    if _is_number(value):
                        rows.append((node_id, f"gpu.{index}.{field}", float(value), ts))
        self._db.record_metrics(rows)

    def record_gateway_sample(
        self,
        deployment_id: str,
        *,
        requests: int = 0,
        errors: int = 0,
        latency_ms: float | None = None,
        ttft_ms: float | None = None,
        prompt_tokens: int = 0,
        completion_tokens: int = 0,
        ts: float | None = None,
    ) -> None:
        if not deployment_id:
            raise ValueError("deployment id must not be empty")
        for name, value in (
            ("requests", requests),
            ("errors", errors),
            ("prompt_tokens", prompt_tokens),
            ("completion_tokens", completion_tokens),
        ):
            if isinstance(value, bool) or not isinstance(value, int) or value < 0:
                raise ValueError(f"{name} must be a non-negative integer")
        self._db.record_inference_sample(
            deployment_id,
            requests=requests,
            errors=errors,
            latency_ms=latency_ms,
            ttft_ms=ttft_ms,
            prompt_tokens=prompt_tokens,
            completion_tokens=completion_tokens,
            ts=ts,
        )

    def latest_node_metrics(self, node_id: str) -> dict[str, float]:
        latest: dict[str, float] = {}
        for metric in self._db.list_metric_names(node_id):
            row = self._db.latest_metric(node_id, metric)
            if row is not None:
                latest[metric] = float(row["value"])
        return latest

    def node_series(
        self,
        node_id: str,
        metric: str,
        *,
        since: float | None = None,
        until: float | None = None,
        limit: int = 1000,
    ) -> list[dict[str, Any]]:
        return self._db.metric_series(node_id, metric, since=since, until=until, limit=limit)

    def inference_series(
        self,
        deployment_id: str,
        *,
        since: float | None = None,
        until: float | None = None,
        limit: int = 1000,
    ) -> list[dict[str, Any]]:
        return self._db.inference_series(deployment_id, since=since, until=until, limit=limit)

    def inference_totals(
        self,
        deployment_id: str,
        *,
        since: float | None = None,
        until: float | None = None,
    ) -> InferenceTotals:
        rows = self.inference_series(deployment_id, since=since, until=until, limit=100_000)
        latencies = [float(row["latency_ms"]) for row in rows if row["latency_ms"] is not None]
        ttfts = [float(row["ttft_ms"]) for row in rows if row["ttft_ms"] is not None]
        return InferenceTotals(
            requests=sum(int(row["requests"]) for row in rows),
            errors=sum(int(row["errors"]) for row in rows),
            prompt_tokens=sum(int(row["prompt_tokens"]) for row in rows),
            completion_tokens=sum(int(row["completion_tokens"]) for row in rows),
            average_latency_ms=(sum(latencies) / len(latencies)) if latencies else None,
            average_ttft_ms=(sum(ttfts) / len(ttfts)) if ttfts else None,
        )

    def prune(self, retention_seconds: float, *, now: float | None = None) -> dict[str, int]:
        return self._db.prune(retention_seconds, now=now)

    def vacuum(self) -> None:
        self._db.incremental_vacuum()

    def maintenance(self, retention_seconds: float, *, now: float | None = None) -> dict[str, int]:
        pruned = self.prune(retention_seconds, now=now)
        if any(pruned.values()):
            self.vacuum()
        return pruned


def _is_number(value: Any) -> bool:
    return isinstance(value, (int, float)) and not isinstance(value, bool)
