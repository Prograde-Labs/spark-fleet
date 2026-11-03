"""Tests for the controller agent HTTP client."""

import json

import httpx
import pytest

from spark_fleet.controller.agent_client import (
    COMPOSE_TIMEOUT_SECONDS,
    STREAM_TIMEOUT,
    AgentClient,
    AgentError,
    AgentUnreachableError,
)

TOKEN = "agent-token-0123456789"

# agent client -------------------------------------------------------------


class Recorder:
    def __init__(self, responses):
        self.responses = list(responses)
        self.requests = []

    def __call__(self, request):
        self.requests.append(request)
        return self.responses.pop(0)


def json_response(payload, status_code=200):
    return httpx.Response(status_code, json=payload)


async def test_health_is_public():
    recorder = Recorder([json_response({"status": "ok", "node_id": "spark-01"})])
    async with AgentClient(
        "http://agent:8092", TOKEN, transport=httpx.MockTransport(recorder)
    ) as client:
        result = await client.health()

    assert result == {"status": "ok", "node_id": "spark-01"}
    request = recorder.requests[0]
    assert request.method == "GET"
    assert request.url.path == "/health"
    assert "authorization" not in request.headers


async def test_info_uses_bearer_token():
    recorder = Recorder([json_response({"node_id": "spark-01"})])
    async with AgentClient(
        "http://agent:8092", TOKEN, transport=httpx.MockTransport(recorder)
    ) as client:
        assert await client.info() == {"node_id": "spark-01"}

    request = recorder.requests[0]
    assert request.method == "GET"
    assert request.url.path == "/info"
    assert request.headers["authorization"] == f"Bearer {TOKEN}"


async def test_validate_compose_posts_project_and_document():
    compose = {"services": {"workload": {"image": "alpine"}}}
    recorder = Recorder([json_response({"project": "p", "config_hash": "abc"})])
    async with AgentClient(
        "http://agent:8092", TOKEN, transport=httpx.MockTransport(recorder)
    ) as client:
        result = await client.validate_compose("p", compose)

    assert result == {"project": "p", "config_hash": "abc"}
    request = recorder.requests[0]
    assert request.method == "POST"
    assert request.url.path == "/compose/validate"
    assert json.loads(request.content) == {"project": "p", "compose": compose}
    assert request.headers["authorization"] == f"Bearer {TOKEN}"


async def test_up_down_and_status_compose():
    containers = [{"id": "abc", "state": "running"}]
    recorder = Recorder(
        [
            json_response({"containers": containers}),
            json_response({"ok": True}),
            json_response({"containers": containers}),
        ]
    )
    compose = {"services": {"workload": {"image": "alpine"}}}
    async with AgentClient(
        "http://agent:8092", TOKEN, transport=httpx.MockTransport(recorder)
    ) as client:
        assert await client.up_compose("p", compose) == containers
        assert await client.down_compose("p") is None
        assert await client.compose_status("p") == containers

    assert recorder.requests[0].url.path == "/compose/up"
    assert json.loads(recorder.requests[0].content) == {"project": "p", "compose": compose}
    assert recorder.requests[1].url.path == "/compose/down"
    assert json.loads(recorder.requests[1].content) == {"project": "p"}
    assert recorder.requests[2].url.path == "/compose/status"
    assert recorder.requests[2].url.params["project"] == "p"


async def test_compose_lifecycle_uses_long_timeout():
    recorder = Recorder(
        [
            json_response({"containers": []}),
            json_response({"ok": True}),
        ]
    )
    compose = {"services": {"workload": {"image": "alpine"}}}
    async with AgentClient(
        "http://agent:8092", TOKEN, transport=httpx.MockTransport(recorder)
    ) as client:
        await client.up_compose("p", compose)
        await client.down_compose("p")

    for request in recorder.requests:
        assert request.extensions["timeout"]["read"] == COMPOSE_TIMEOUT_SECONDS
    assert COMPOSE_TIMEOUT_SECONDS > 10


async def test_container_logs_returns_text():
    recorder = Recorder([json_response({"logs": "hello\nworld\n"})])
    async with AgentClient(
        "http://agent:8092", TOKEN, transport=httpx.MockTransport(recorder)
    ) as client:
        logs = await client.container_logs("abc123", tail=50)

    assert logs == "hello\nworld\n"
    request = recorder.requests[0]
    assert request.url.path == "/containers/logs"
    assert request.url.params["container_id"] == "abc123"
    assert request.url.params["tail"] == "50"


async def test_follow_container_logs_streams_lines():
    recorder = Recorder([httpx.Response(200, content=b"one\ntwo\n")])
    async with AgentClient(
        "http://agent:8092", TOKEN, transport=httpx.MockTransport(recorder)
    ) as client:
        lines = [line async for line in client.follow_container_logs("abc123", tail=10)]

    assert lines == ["one", "two"]
    request = recorder.requests[0]
    assert request.url.path == "/containers/logs/stream"
    assert request.url.params["container_id"] == "abc123"
    assert request.url.params["tail"] == "10"
    assert request.headers["authorization"] == f"Bearer {TOKEN}"


async def test_follow_container_logs_reports_http_error():
    recorder = Recorder([httpx.Response(401, json={"detail": "bad token"})])
    async with AgentClient(
        "http://agent:8092", TOKEN, transport=httpx.MockTransport(recorder)
    ) as client:
        with pytest.raises(AgentError, match="bad token") as excinfo:
            async for _line in client.follow_container_logs("abc123"):
                pass

    assert excinfo.value.status_code == 401


async def test_follow_container_logs_reports_transport_error():
    def handler(request):
        raise httpx.ConnectError("connection refused")

    async with AgentClient(
        "http://agent:8092", TOKEN, transport=httpx.MockTransport(handler)
    ) as client:
        with pytest.raises(AgentUnreachableError):
            async for _line in client.follow_container_logs("abc123"):
                pass


async def test_follow_container_logs_disables_read_timeout(monkeypatch):
    calls = {}

    class FakeStream:
        async def __aenter__(self):
            return httpx.Response(200, content=b"one\n")

        async def __aexit__(self, *exc_info):
            return False

    def stream(method, path, **kwargs):
        calls.update(kwargs)
        return FakeStream()

    client = AgentClient("http://agent:8092", TOKEN)
    monkeypatch.setattr(client._client, "stream", stream)

    lines = [line async for line in client.follow_container_logs("abc123")]

    assert lines == ["one"]
    assert calls["timeout"] == STREAM_TIMEOUT
    assert calls["timeout"].read is None
    await client.aclose()


async def test_models_and_download_jobs():
    recorder = Recorder(
        [
            json_response({"roots": [], "models": []}),
            json_response({"job_id": "job-1"}),
            json_response({"id": "job-1", "state": "running"}),
            json_response({"cancelled": True}),
            json_response({"ok": True}),
        ]
    )
    async with AgentClient(
        "http://agent:8092", TOKEN, transport=httpx.MockTransport(recorder)
    ) as client:
        assert await client.models() == {"roots": [], "models": []}
        assert (
            await client.start_model_download("org/name", revision="v1", allow_patterns=["*.json"])
            == "job-1"
        )
        assert await client.model_job("job-1") == {"id": "job-1", "state": "running"}
        assert await client.cancel_model_job("job-1") is True
        assert await client.remove_model("org/name", confirm=True, in_use=["org/other"]) is None

    assert recorder.requests[0].url.path == "/models"
    assert recorder.requests[1].url.path == "/models/download"
    assert json.loads(recorder.requests[1].content) == {
        "repo": "org/name",
        "root": None,
        "revision": "v1",
        "allow_patterns": ["*.json"],
    }
    assert recorder.requests[2].url.path == "/models/jobs/job-1"
    assert recorder.requests[3].url.path == "/models/jobs/job-1/cancel"
    assert recorder.requests[4].url.path == "/models/remove"
    assert json.loads(recorder.requests[4].content) == {
        "repo": "org/name",
        "root": None,
        "confirm": True,
        "in_use": ["org/other"],
    }


async def test_metrics():
    recorder = Recorder([json_response({"host_total_bytes": 1000})])
    async with AgentClient(
        "http://agent:8092", TOKEN, transport=httpx.MockTransport(recorder)
    ) as client:
        assert await client.metrics() == {"host_total_bytes": 1000}

    assert recorder.requests[0].url.path == "/metrics"


async def test_jobs():
    recorder = Recorder(
        [
            json_response({"job_id": "job-1"}),
            json_response({"id": "job-1", "state": "running"}),
            json_response({"cancelled": True}),
            json_response({"jobs": [{"id": "job-1"}]}),
        ]
    )
    async with AgentClient(
        "http://agent:8092", TOKEN, transport=httpx.MockTransport(recorder)
    ) as client:
        assert (
            await client.start_job(
                name="stage",
                image="alpine:latest",
                command=["echo", "hi"],
                labels={"spark.fleet.deployment": "d", "spark.fleet.recipe": "r"},
                volumes=["/host:/stage"],
                environment={"A": "1"},
            )
            == "job-1"
        )
        assert await client.job_status("job-1") == {"id": "job-1", "state": "running"}
        assert await client.cancel_job("job-1") is True
        assert await client.list_jobs() == [{"id": "job-1"}]

    assert recorder.requests[0].url.path == "/jobs"
    assert json.loads(recorder.requests[0].content) == {
        "name": "stage",
        "image": "alpine:latest",
        "command": ["echo", "hi"],
        "labels": {"spark.fleet.deployment": "d", "spark.fleet.recipe": "r"},
        "volumes": ["/host:/stage"],
        "environment": {"A": "1"},
    }
    assert recorder.requests[1].url.path == "/jobs/job-1"
    assert recorder.requests[2].url.path == "/jobs/job-1/cancel"
    assert recorder.requests[3].url.path == "/jobs"


async def test_http_error_includes_detail_and_status():
    recorder = Recorder([json_response({"detail": "missing or invalid agent token"}, 401)])
    async with AgentClient(
        "http://agent:8092", TOKEN, transport=httpx.MockTransport(recorder)
    ) as client:
        with pytest.raises(AgentError, match="missing or invalid agent token") as excinfo:
            await client.info()

    assert excinfo.value.status_code == 401


async def test_http_error_without_json_uses_body():
    recorder = Recorder([httpx.Response(500, text="boom")])
    async with AgentClient(
        "http://agent:8092", TOKEN, transport=httpx.MockTransport(recorder)
    ) as client:
        with pytest.raises(AgentError, match="boom") as excinfo:
            await client.info()

    assert excinfo.value.status_code == 500


async def test_invalid_json_response_raises():
    recorder = Recorder([httpx.Response(200, text="not json")])
    async with AgentClient(
        "http://agent:8092", TOKEN, transport=httpx.MockTransport(recorder)
    ) as client:
        with pytest.raises(AgentError, match="invalid JSON"):
            await client.info()


@pytest.mark.parametrize(
    "error",
    [
        httpx.ConnectError("connection refused"),
        httpx.ReadTimeout("timed out"),
    ],
)
async def test_transport_errors_raise_unreachable(error):
    def handler(request):
        raise error

    async with AgentClient(
        "http://agent:8092", TOKEN, transport=httpx.MockTransport(handler)
    ) as client:
        with pytest.raises(AgentUnreachableError):
            await client.info()


@pytest.mark.parametrize(
    ("url", "token"),
    [
        ("ftp://agent:8092", TOKEN),
        ("http://agent:8092/path", TOKEN),
        ("http://agent:8092?x=1", TOKEN),
        ("http://agent:8092", ""),
    ],
)
def test_client_rejects_invalid_configuration(url, token):
    with pytest.raises(ValueError):
        AgentClient(url, token)


async def test_client_close():
    client = AgentClient("http://agent:8092", TOKEN)

    assert client.is_closed is False
    await client.aclose()
    assert client.is_closed is True
