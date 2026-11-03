"""Tests for fleet gateway routing, proxying, streaming, and counters."""

import json

import httpx
import pytest

from spark_fleet.controller.gateway import (
    FleetGateway,
    GatewayError,
    GatewayUnavailable,
    InferenceCounters,
    ModelLoading,
    ModelNotFound,
    extract_usage,
    filter_response_headers,
    parse_sse_usage,
    usage_from_payload,
)
from spark_fleet.controller.metrics import MetricsStore
from spark_fleet.shared.db import Database


def recipe(name, node, served_name, aliases=(), port=8888, context_length=None):
    model = {"repo": "org/model", "servedName": served_name}
    if context_length is not None:
        model["contextLength"] = context_length
    return {
        "apiVersion": "spark-fleet/v1",
        "kind": "Recipe",
        "metadata": {"name": name},
        "spec": {
            "mode": "deployment",
            "runtime": "custom",
            "model": model,
            "gateway": {"aliases": list(aliases)},
            "roles": [
                {
                    "name": "head",
                    "node": node,
                    "class": "heavy",
                    "inferencePort": port,
                    "service": {"image": "img"},
                }
            ],
        },
    }


@pytest.fixture
def db(tmp_path):
    return Database(tmp_path / "fleet.db")


def add_node(db, node_id, lan_ip):
    db.upsert_node(
        node_id,
        display_name=node_id,
        lan_ip=lan_ip,
        fabric_ip="198.51.100.10",
        agent_url=f"http://{lan_ip}:8092",
    )


def add_deployment(
    db,
    deployment_id="dep-1",
    name="demo-glm",
    node="spark-01",
    lan_ip="192.0.2.239",
    served_name="Demo-Model-B",
    aliases=("demo-glm",),
    status="loaded",
    primary=False,
    port=8888,
    context_length=None,
):
    add_node(db, node, lan_ip)
    db.create_deployment(
        deployment_id,
        name,
        name,
        "deployment",
        recipe(name, node, served_name, aliases, port, context_length),
        status=status,
    )
    if primary:
        db.set_primary(deployment_id)
    return deployment_id


def test_routing_table_routes_aliases_and_served_name(db):
    add_deployment(db)
    gateway = FleetGateway(db)

    for model in ("demo-glm", "Demo-Model-B"):
        route = gateway.resolve(model)
        assert route.deployment_id == "dep-1"
        assert route.served_name == "Demo-Model-B"
        assert route.upstream_url == "http://192.0.2.239:8888"


def test_spark_active_routes_to_primary(db):
    add_deployment(
        db, "dep-1", "primary", served_name="PRIMARY", aliases=("primary",), primary=True
    )
    gateway = FleetGateway(db)

    route = gateway.resolve("spark-active")

    assert route.deployment_id == "dep-1"
    assert route.served_name == "PRIMARY"


def test_spark_active_without_primary_is_503(db):
    add_deployment(db)
    gateway = FleetGateway(db)

    with pytest.raises(GatewayUnavailable) as excinfo:
        gateway.resolve("spark-active")

    assert excinfo.value.status_code == 503


def test_unknown_model_is_404(db):
    add_deployment(db)
    gateway = FleetGateway(db)

    with pytest.raises(ModelNotFound) as excinfo:
        gateway.resolve("unknown")

    assert excinfo.value.status_code == 404


def test_loading_deployment_is_503_with_retry_after(db):
    add_deployment(db, status="loading")
    gateway = FleetGateway(db)

    with pytest.raises(ModelLoading) as excinfo:
        gateway.resolve("demo-glm")

    assert excinfo.value.status_code == 503
    assert excinfo.value.retry_after == 5


def test_duplicate_alias_is_rejected(db):
    add_deployment(db, "dep-1", "one", aliases=("shared",))
    add_deployment(db, "dep-2", "two", node="spark-02", lan_ip="192.0.2.79", aliases=("shared",))
    gateway = FleetGateway(db)

    with pytest.raises(GatewayError, match="duplicate alias"):
        gateway.routing_table()


def test_reserved_spark_active_alias_is_rejected(db):
    add_deployment(db, aliases=("spark-active",))
    gateway = FleetGateway(db)

    with pytest.raises(GatewayError, match="reserved"):
        gateway.routing_table()


async def test_models_lists_loaded_models_and_spark_active(db):
    add_deployment(
        db, "dep-1", "one", served_name="ONE", aliases=("one",), primary=True, context_length=1024
    )
    add_deployment(
        db,
        "dep-2",
        "two",
        node="spark-02",
        lan_ip="192.0.2.79",
        served_name="TWO",
        aliases=("two",),
        context_length=2048,
    )
    add_deployment(db, "dep-loading", "loading", served_name="LOADING", status="loading")
    calls = []

    def handler(request):
        calls.append(str(request.url))
        return httpx.Response(200, json={"object": "list", "data": []})

    async with FleetGateway(db, transport=httpx.MockTransport(handler)) as gateway:
        body = await gateway.models()

    assert calls == []
    assert body["object"] == "list"
    assert [item["id"] for item in body["data"]] == [
        "ONE",
        "TWO",
        "one",
        "spark-active",
        "two",
    ]


async def test_models_reports_explicit_recipe_context_length(db):
    add_deployment(
        db,
        "dep-1",
        "one",
        served_name="ONE",
        aliases=("one",),
        primary=True,
        context_length=524288,
    )

    async with FleetGateway(db) as gateway:
        body = await gateway.models()

    assert [item["id"] for item in body["data"]] == ["ONE", "one", "spark-active"]
    for item in body["data"]:
        assert item["max_model_len"] == 524288
        assert item["context_length"] == 524288


async def test_models_prefers_recipe_context_over_upstream(db):
    add_deployment(db, context_length=524288)
    calls = []

    def handler(request):
        calls.append(str(request.url))
        return httpx.Response(
            200,
            json={"object": "list", "data": [{"id": "Demo-Model-B", "max_model_len": 262144}]},
        )

    async with FleetGateway(db, transport=httpx.MockTransport(handler)) as gateway:
        body = await gateway.models()

    assert calls == []
    assert {item["max_model_len"] for item in body["data"]} == {524288}


async def test_models_falls_back_to_upstream_max_model_len(db):
    add_deployment(db, served_name="Demo-Model-B", aliases=("demo-glm",))
    seen = {}

    def handler(request):
        seen["url"] = str(request.url)
        return httpx.Response(
            200,
            json={"object": "list", "data": [{"id": "Demo-Model-B", "max_model_len": 262144}]},
        )

    async with FleetGateway(db, transport=httpx.MockTransport(handler)) as gateway:
        body = await gateway.models()

    assert seen["url"] == "http://192.0.2.239:8888/v1/models"
    assert {item["id"]: item["max_model_len"] for item in body["data"]} == {
        "Demo-Model-B": 262144,
        "demo-glm": 262144,
    }
    assert {item["context_length"] for item in body["data"]} == {262144}


async def test_models_falls_back_to_upstream_context_length(db):
    add_deployment(db)

    def handler(request):
        return httpx.Response(
            200,
            json={"object": "list", "data": [{"id": "Demo-Model-B", "context_length": 850000}]},
        )

    async with FleetGateway(db, transport=httpx.MockTransport(handler)) as gateway:
        body = await gateway.models()

    assert {item["max_model_len"] for item in body["data"]} == {850000}


async def test_models_falls_back_to_single_upstream_entry(db):
    add_deployment(db, served_name="Demo-Model-B")

    def handler(request):
        return httpx.Response(
            200,
            json={"object": "list", "data": [{"id": "org/model", "max_model_len": 4096}]},
        )

    async with FleetGateway(db, transport=httpx.MockTransport(handler)) as gateway:
        body = await gateway.models()

    assert {item["max_model_len"] for item in body["data"]} == {4096}


async def test_models_matches_upstream_entry_by_served_name(db):
    add_deployment(db, served_name="Demo-Model-B")
    add_deployment(
        db,
        "dep-2",
        "other",
        node="spark-02",
        lan_ip="192.0.2.79",
        served_name="OTHER",
        aliases=("other",),
        port=8889,
    )

    def handler(request):
        if request.url.port == 8888:
            entries = [
                {"id": "OTHER", "max_model_len": 111111},
                {"id": "Demo-Model-B", "max_model_len": 262144},
            ]
        else:
            entries = [
                {"id": "Demo-Model-B", "max_model_len": 222222},
                {"id": "OTHER", "max_model_len": 850000},
            ]
        return httpx.Response(200, json={"object": "list", "data": entries})

    async with FleetGateway(db, transport=httpx.MockTransport(handler)) as gateway:
        body = await gateway.models()

    contexts = {item["id"]: item["max_model_len"] for item in body["data"]}
    assert contexts == {
        "Demo-Model-B": 262144,
        "OTHER": 850000,
        "demo-glm": 262144,
        "other": 850000,
    }


async def test_models_does_not_guess_context_for_multi_model_upstream(db):
    add_deployment(db, served_name="Demo-Model-B")

    def handler(request):
        return httpx.Response(
            200,
            json={
                "object": "list",
                "data": [
                    {"id": "A", "max_model_len": 111111},
                    {"id": "B", "max_model_len": 222222},
                ],
            },
        )

    async with FleetGateway(db, transport=httpx.MockTransport(handler)) as gateway:
        body = await gateway.models()

    assert [item["id"] for item in body["data"]] == ["Demo-Model-B", "demo-glm"]
    for item in body["data"]:
        assert "max_model_len" not in item
        assert "context_length" not in item


async def test_models_omits_context_when_upstream_fails(db):
    add_deployment(db)

    def handler(request):
        raise httpx.ConnectError("upstream down", request=request)

    async with FleetGateway(db, transport=httpx.MockTransport(handler)) as gateway:
        body = await gateway.models()

    assert [item["id"] for item in body["data"]] == ["Demo-Model-B", "demo-glm"]
    for item in body["data"]:
        assert "max_model_len" not in item
        assert "context_length" not in item


@pytest.mark.parametrize(
    "response",
    [
        httpx.Response(500, text="boom"),
        httpx.Response(200, text="not json"),
        httpx.Response(200, json=["not", "a", "mapping"]),
        httpx.Response(200, json={"object": "list", "data": "not a list"}),
        httpx.Response(200, json={"object": "list", "data": []}),
        httpx.Response(200, json={"object": "list", "data": [{"id": "Demo-Model-B"}]}),
        httpx.Response(
            200, json={"object": "list", "data": [{"id": "Demo-Model-B", "max_model_len": True}]}
        ),
        httpx.Response(
            200, json={"object": "list", "data": [{"id": "Demo-Model-B", "max_model_len": 0}]}
        ),
        httpx.Response(
            200, json={"object": "list", "data": [{"id": "Demo-Model-B", "context_length": True}]}
        ),
        httpx.Response(
            200, json={"object": "list", "data": [{"id": "Demo-Model-B", "context_length": -1}]}
        ),
    ],
)
async def test_models_omits_context_for_bad_upstream_payload(db, response):
    add_deployment(db)

    def handler(request):
        return response

    async with FleetGateway(db, transport=httpx.MockTransport(handler)) as gateway:
        body = await gateway.models()

    assert [item["id"] for item in body["data"]] == ["Demo-Model-B", "demo-glm"]
    for item in body["data"]:
        assert "max_model_len" not in item
        assert "context_length" not in item


async def test_models_ignores_non_mapping_upstream_entries(db):
    add_deployment(db)

    def handler(request):
        return httpx.Response(
            200,
            json={
                "object": "list",
                "data": [1, "x", {"id": "Demo-Model-B", "max_model_len": 4096}],
            },
        )

    async with FleetGateway(db, transport=httpx.MockTransport(handler)) as gateway:
        body = await gateway.models()

    assert {item["max_model_len"] for item in body["data"]} == {4096}


async def test_models_caches_upstream_context_lookups(db, monkeypatch):
    add_deployment(db)
    calls = []

    def handler(request):
        calls.append(str(request.url))
        return httpx.Response(
            200,
            json={"object": "list", "data": [{"id": "Demo-Model-B", "max_model_len": 262144}]},
        )

    async with FleetGateway(db, transport=httpx.MockTransport(handler)) as gateway:
        first = await gateway.models()
        second = await gateway.models()

    assert len(calls) == 1
    assert {item["max_model_len"] for item in first["data"]} == {262144}
    assert {item["max_model_len"] for item in second["data"]} == {262144}


async def test_models_cache_expires(db, monkeypatch):
    add_deployment(db)
    calls = []

    def handler(request):
        calls.append(str(request.url))
        return httpx.Response(
            200,
            json={"object": "list", "data": [{"id": "Demo-Model-B", "max_model_len": 262144}]},
        )

    import spark_fleet.controller.gateway as gateway_module

    monkeypatch.setattr(gateway_module, "UPSTREAM_CONTEXT_CACHE_TTL_SECONDS", 0.0)

    async with FleetGateway(db, transport=httpx.MockTransport(handler)) as gateway:
        first = await gateway.models()
        second = await gateway.models()

    assert len(calls) == 2
    assert {item["max_model_len"] for item in first["data"]} == {262144}
    assert {item["max_model_len"] for item in second["data"]} == {262144}


async def test_proxy_rewrites_model_and_records_usage(db):
    add_deployment(db)
    calls = []

    def handler(request):
        calls.append(request)
        return httpx.Response(
            200,
            json={
                "model": "Demo-Model-B",
                "usage": {"prompt_tokens": 11, "completion_tokens": 7},
            },
        )

    gateway = FleetGateway(db, transport=httpx.MockTransport(handler))

    response = await gateway.proxy(
        "demo-glm",
        "POST",
        "/v1/chat/completions",
        {"Content-Type": "application/json", "Host": "fleet", "X-Test": "1"},
        json.dumps({"model": "demo-glm", "messages": []}).encode(),
    )

    assert response.status_code == 200
    request = calls[0]
    assert request.url == httpx.URL("http://192.0.2.239:8888/v1/chat/completions")
    assert json.loads(request.content)["model"] == "Demo-Model-B"
    assert request.headers["x-test"] == "1"
    assert request.headers["host"] == "192.0.2.239:8888"
    counters = gateway.counters.snapshot()
    assert counters["dep-1"].requests == 1
    assert counters["dep-1"].prompt_tokens == 11
    assert counters["dep-1"].completion_tokens == 7


async def test_proxy_forwards_non_json_body_unchanged(db):
    add_deployment(db)
    calls = []

    def handler(request):
        calls.append(request)
        return httpx.Response(200, content=b"ok")

    gateway = FleetGateway(db, transport=httpx.MockTransport(handler))

    await gateway.proxy(
        "demo-glm",
        "POST",
        "/v1/raw",
        {"Content-Type": "text/plain"},
        b"raw body",
    )

    assert calls[0].content == b"raw body"


async def test_proxy_records_upstream_errors(db):
    add_deployment(db)

    def handler(request):
        return httpx.Response(500, text="boom")

    gateway = FleetGateway(db, transport=httpx.MockTransport(handler))

    response = await gateway.proxy("demo-glm", "POST", "/v1/chat/completions", {}, b"{}")

    assert response.status_code == 500
    counters = gateway.counters.snapshot()
    assert counters["dep-1"].requests == 1
    assert counters["dep-1"].errors == 1


async def test_proxy_reports_transport_error(db):
    add_deployment(db)

    def handler(request):
        raise httpx.ConnectError("refused")

    gateway = FleetGateway(db, transport=httpx.MockTransport(handler))

    with pytest.raises(GatewayError, match="upstream request failed") as excinfo:
        await gateway.proxy("demo-glm", "POST", "/v1/chat/completions", {}, b"{}")

    assert excinfo.value.status_code == 502


async def test_stream_proxy_passes_chunks_and_usage(db):
    add_deployment(db)
    body = (
        b'data: {"choices":[{"delta":{"content":"hi"}}]}\n\n'
        b'data: {"usage":{"prompt_tokens":3,"completion_tokens":4}}\n\n'
        b"data: [DONE]\n\n"
    )

    def handler(request):
        return httpx.Response(200, content=body, headers={"Content-Type": "text/event-stream"})

    gateway = FleetGateway(db, transport=httpx.MockTransport(handler))

    stream = await gateway.stream("demo-glm", "POST", "/v1/chat/completions", {}, b"{}")
    chunks = [chunk async for chunk in stream.aiter_bytes()]

    assert b"".join(chunks) == body
    assert stream.status_code == 200
    counters = gateway.counters.snapshot()
    assert counters["dep-1"].prompt_tokens == 3
    assert counters["dep-1"].completion_tokens == 4
    assert counters["dep-1"].ttft_count == 1


def test_counters_average_latency_and_ttft(db):
    add_deployment(db)
    counters = InferenceCounters()

    counters.record("dep-1", status_code=200, latency_ms=10.0, ttft_ms=2.0, prompt_tokens=1)
    counters.record("dep-1", status_code=200, latency_ms=30.0, ttft_ms=6.0, completion_tokens=2)

    snapshot = counters.snapshot()["dep-1"]

    assert snapshot.requests == 2
    assert snapshot.average_latency_ms() == 20.0
    assert snapshot.average_ttft_ms() == 4.0
    assert snapshot.prompt_tokens == 1
    assert snapshot.completion_tokens == 2


def test_counters_flush_to_metrics_store(db):
    add_deployment(db)
    store = MetricsStore(db)
    counters = InferenceCounters()
    counters.record(
        "dep-1",
        status_code=200,
        latency_ms=20.0,
        ttft_ms=5.0,
        prompt_tokens=10,
        completion_tokens=3,
    )

    counters.flush(store, ts=1234.0)

    rows = store.inference_series("dep-1")
    assert len(rows) == 1
    assert rows[0]["requests"] == 1
    assert rows[0]["latency_ms"] == 20.0
    assert rows[0]["ttft_ms"] == 5.0
    assert rows[0]["prompt_tokens"] == 10
    assert rows[0]["completion_tokens"] == 3
    assert counters.snapshot() == {}


def test_usage_parsing():
    assert usage_from_payload({"usage": {"prompt_tokens": 1, "completion_tokens": 2}}) == (1, 2)
    assert usage_from_payload({"usage": {"input_tokens": 3, "output_tokens": 4}}) == (3, 4)
    assert usage_from_payload({"usage": {"prompt_tokens": "x"}}) == (0, 0)
    assert usage_from_payload({"usage": None}) == (0, 0)
    assert usage_from_payload({}) == (0, 0)

    assert parse_sse_usage(
        'data: {"usage":{"prompt_tokens":5,"completion_tokens":6}}\ndata: [DONE]\n'
    ) == (5, 6)
    assert parse_sse_usage("not sse") == (0, 0)
    assert parse_sse_usage('data: {"usage":{}}\n') == (0, 0)


def test_extract_usage_from_response():
    response = httpx.Response(200, json={"usage": {"prompt_tokens": 1, "completion_tokens": 2}})

    assert extract_usage(response) == (1, 2)
    assert extract_usage(httpx.Response(200, text="not json")) == (0, 0)


async def test_gateway_close(db):
    gateway = FleetGateway(db)

    assert gateway._client.is_closed is False
    await gateway.aclose()
    assert gateway._client.is_closed is True


def test_routing_prefers_head_role_by_start_order(db):
    add_node(db, "spark-01", "192.0.2.239")
    add_node(db, "spark-02", "192.0.2.79")
    recipe_doc = {
        "apiVersion": "spark-fleet/v1",
        "kind": "Recipe",
        "metadata": {"name": "multi"},
        "spec": {
            "mode": "deployment",
            "runtime": "custom",
            "model": {"repo": "org/model", "servedName": "MULTI"},
            "gateway": {"aliases": ["multi"]},
            "roles": [
                {
                    "name": "worker",
                    "node": "spark-02",
                    "class": "heavy",
                    "startOrder": 10,
                    "service": {"image": "img"},
                },
                {
                    "name": "head",
                    "node": "spark-01",
                    "class": "heavy",
                    "startOrder": 20,
                    "inferencePort": 8888,
                    "service": {"image": "img"},
                },
            ],
        },
    }
    db.create_deployment("dep-1", "multi", "multi", "deployment", recipe_doc, status="loaded")
    gateway = FleetGateway(db)

    route = gateway.resolve("multi")

    assert route.upstream_url == "http://192.0.2.239:8888"


def test_flush_drops_samples_for_deleted_deployments(db):
    add_deployment(db)
    store = MetricsStore(db)
    counters = InferenceCounters()
    counters.record("dep-1", status_code=200, latency_ms=1.0)
    db.delete_deployment("dep-1")

    counters.flush(store, ts=1.0)

    assert counters.snapshot() == {}
    assert store.inference_series("dep-1") == []


async def test_proxy_strips_credentials_from_upstream(db):
    add_deployment(db)
    calls = []

    def handler(request):
        calls.append(request)
        return httpx.Response(200, json={"ok": True})

    gateway = FleetGateway(db, transport=httpx.MockTransport(handler))

    await gateway.proxy(
        "demo-glm",
        "POST",
        "/v1/chat/completions",
        {
            "Content-Type": "application/json",
            "Authorization": "Bearer secret",
            "Cookie": "spark_fleet_session=abc",
            "X-CSRF-Token": "csrf",
        },
        b"{}",
    )

    assert "authorization" not in calls[0].headers
    assert "cookie" not in calls[0].headers
    assert "x-csrf-token" not in calls[0].headers


def test_filter_response_headers_drops_stale_encoding_headers():
    assert filter_response_headers(
        {
            "content-length": "10",
            "content-encoding": "gzip",
            "connection": "keep-alive",
            "content-type": "application/json",
            "x-upstream": "1",
        }
    ) == {"content-type": "application/json", "x-upstream": "1"}


def test_routing_prefers_named_head_even_with_lower_start_order(db):
    add_node(db, "spark-01", "192.0.2.239")
    add_node(db, "spark-02", "192.0.2.79")
    recipe_doc = {
        "apiVersion": "spark-fleet/v1",
        "kind": "Recipe",
        "metadata": {"name": "multi"},
        "spec": {
            "mode": "deployment",
            "runtime": "custom",
            "model": {"repo": "org/model", "servedName": "MULTI"},
            "gateway": {"aliases": ["multi"]},
            "roles": [
                {
                    "name": "head",
                    "node": "spark-01",
                    "class": "heavy",
                    "startOrder": 10,
                    "inferencePort": 8888,
                    "service": {"image": "img"},
                },
                {
                    "name": "worker",
                    "node": "spark-02",
                    "class": "heavy",
                    "startOrder": 20,
                    "service": {"image": "img"},
                },
            ],
        },
    }
    db.create_deployment("dep-1", "multi", "multi", "deployment", recipe_doc, status="loaded")
    gateway = FleetGateway(db)

    route = gateway.resolve("multi")

    assert route.upstream_url == "http://192.0.2.239:8888"
