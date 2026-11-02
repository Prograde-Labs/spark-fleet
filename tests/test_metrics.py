"""Tests for the controller metrics store and rollup."""

import pytest

from spark_fleet.controller.metrics import (
    CACHE_BYTES,
    CONTAINER_MEMORY,
    HOST_AVAILABLE,
    HOST_TOTAL,
    InferenceTotals,
    MetricsStore,
)
from spark_fleet.shared.db import Database

SNAPSHOT = {
    "sampled_at": 1000.0,
    "host_total_bytes": 131_072_000 * 1024,
    "host_available_bytes": 65_536_000 * 1024,
    "container_memory_bytes": 1024,
    "cache_bytes": 2048,
    "containers": [],
    "gpus": [
        {
            "index": 0,
            "name": "NVIDIA GB10",
            "memory_total_bytes": 1000,
            "memory_used_bytes": 500,
            "utilization_percent": 5,
            "temperature_celsius": 40,
        },
        {
            "index": 1,
            "name": "NVIDIA GB10",
            "memory_total_bytes": 1000,
            "memory_used_bytes": None,
            "utilization_percent": None,
            "temperature_celsius": None,
        },
    ],
    "errors": {},
}


@pytest.fixture
def db(tmp_path):
    return Database(tmp_path / "fleet.db")


@pytest.fixture
def store(db):
    db.upsert_node(
        "spark-01",
        display_name="Spark 01",
        lan_ip="192.0.2.239",
        fabric_ip="198.51.100.10",
        agent_url="http://192.0.2.239:8092",
    )
    db.create_deployment("dep-1", "dep-1", "recipe", "deployment", recipe={})
    return MetricsStore(db)


def test_record_node_snapshot_writes_base_and_gpu_series(store):
    store.record_node_snapshot("spark-01", SNAPSHOT, ts=1234.0)

    assert store.latest_node_metrics("spark-01") == {
        HOST_TOTAL: float(131_072_000 * 1024),
        HOST_AVAILABLE: float(65_536_000 * 1024),
        CONTAINER_MEMORY: 1024.0,
        CACHE_BYTES: 2048.0,
        "gpu.0.memory_total_bytes": 1000.0,
        "gpu.0.memory_used_bytes": 500.0,
        "gpu.0.utilization_percent": 5.0,
        "gpu.0.temperature_celsius": 40.0,
        "gpu.1.memory_total_bytes": 1000.0,
    }


def test_record_node_snapshot_skips_missing_and_invalid_values(store):
    store.record_node_snapshot(
        "spark-01",
        {
            "host_total_bytes": None,
            "host_available_bytes": "bad",
            "container_memory_bytes": True,
            "cache_bytes": 10,
            "gpus": [{"index": "zero", "memory_total_bytes": 1}],
        },
        ts=1234.0,
    )

    assert store.latest_node_metrics("spark-01") == {CACHE_BYTES: 10.0}


def test_record_node_snapshot_requires_node_id(store):
    with pytest.raises(ValueError, match="node id"):
        store.record_node_snapshot("", SNAPSHOT)


def test_node_series_range_and_limit(store):
    for index in range(5):
        store.record_node_snapshot("spark-01", {CACHE_BYTES: index}, ts=100.0 + index)

    series = store.node_series("spark-01", CACHE_BYTES, since=101.0, until=103.0, limit=2)

    assert [row["ts"] for row in series] == [102.0, 103.0]
    assert [row["value"] for row in series] == [2.0, 3.0]


def test_record_gateway_sample(store):
    store.record_gateway_sample(
        "dep-1",
        requests=10,
        errors=1,
        latency_ms=12.5,
        ttft_ms=3.0,
        prompt_tokens=100,
        completion_tokens=20,
        ts=1234.0,
    )

    rows = store.inference_series("dep-1")

    assert len(rows) == 1
    assert rows[0]["requests"] == 10
    assert rows[0]["errors"] == 1
    assert rows[0]["prompt_tokens"] == 100
    assert rows[0]["completion_tokens"] == 20
    assert rows[0]["ts"] == 1234.0


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("requests", -1),
        ("errors", True),
        ("prompt_tokens", "10"),
        ("completion_tokens", 1.5),
    ],
)
def test_record_gateway_sample_rejects_invalid_counters(store, field, value):
    with pytest.raises(ValueError, match=field):
        store.record_gateway_sample("dep-1", **{field: value})


def test_record_gateway_sample_requires_deployment_id(store):
    with pytest.raises(ValueError, match="deployment id"):
        store.record_gateway_sample("")


def test_inference_totals(store):
    store.record_gateway_sample(
        "dep-1",
        requests=10,
        errors=1,
        latency_ms=10.0,
        ttft_ms=2.0,
        prompt_tokens=100,
        completion_tokens=20,
        ts=100.0,
    )
    store.record_gateway_sample(
        "dep-1",
        requests=5,
        errors=0,
        latency_ms=20.0,
        prompt_tokens=50,
        completion_tokens=10,
        ts=110.0,
    )

    totals = store.inference_totals("dep-1")

    assert totals == InferenceTotals(
        requests=15,
        errors=1,
        prompt_tokens=150,
        completion_tokens=30,
        average_latency_ms=15.0,
        average_ttft_ms=2.0,
    )


def test_inference_totals_with_no_samples(store):
    totals = store.inference_totals("dep-1")

    assert totals == InferenceTotals(
        requests=0,
        errors=0,
        prompt_tokens=0,
        completion_tokens=0,
        average_latency_ms=None,
        average_ttft_ms=None,
    )


def test_prune_removes_old_series(store, db):
    store.record_node_snapshot("spark-01", {CACHE_BYTES: 1}, ts=100.0)
    store.record_node_snapshot("spark-01", {CACHE_BYTES: 2}, ts=200.0)
    store.record_gateway_sample("dep-1", requests=1, ts=100.0)
    store.record_gateway_sample("dep-1", requests=2, ts=200.0)

    pruned = store.prune(50.0, now=200.0)

    assert pruned["metrics"] == 1
    assert pruned["inference"] == 1
    assert [row["value"] for row in store.node_series("spark-01", CACHE_BYTES)] == [2.0]
    assert [row["requests"] for row in store.inference_series("dep-1")] == [2]


def test_maintenance_vacuums_after_pruning(store, monkeypatch):
    calls = []
    monkeypatch.setattr(store._db, "incremental_vacuum", lambda: calls.append("vacuum"))
    store.record_node_snapshot("spark-01", {CACHE_BYTES: 1}, ts=100.0)

    pruned = store.maintenance(50.0, now=200.0)

    assert pruned["metrics"] == 1
    assert calls == ["vacuum"]


def test_maintenance_skips_vacuum_when_nothing_pruned(store, monkeypatch):
    calls = []
    monkeypatch.setattr(store._db, "incremental_vacuum", lambda: calls.append("vacuum"))
    store.record_node_snapshot("spark-01", {CACHE_BYTES: 1}, ts=200.0)

    pruned = store.maintenance(50.0, now=200.0)

    assert pruned == {"metrics": 0, "inference": 0, "job_events": 0}
    assert calls == []
