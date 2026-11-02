"""Tests for the agent FastAPI skeleton, auth, and entrypoint."""

from pathlib import Path

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

import spark_fleet
from spark_fleet.agent.api import build_app
from spark_fleet.agent.main import main
from spark_fleet.shared.config import AgentConfig

TOKEN = "agent-token-0123456789"

CONFIG = AgentConfig(
    host="127.0.0.1",
    port=8092,
    node_id="spark-01",
    data_dir=Path("/var/lib/spark-fleet-agent"),
    compose_projects_dir=Path("/var/lib/spark-fleet-agent/projects"),
    model_cache_roots=(Path("/srv/spark-fleet/hub"), Path("/mnt/models")),
    agent_token=TOKEN,
    log_level="INFO",
)


@pytest.fixture
def client():
    return TestClient(build_app(CONFIG))


def auth(token=TOKEN):
    return {"Authorization": f"Bearer {token}"}


def test_health_is_public(client):
    response = client.get("/health")

    assert response.status_code == 200
    assert response.json() == {"status": "ok", "node_id": "spark-01"}


def test_health_reports_unset_node_id():
    config = AgentConfig(
        host="127.0.0.1",
        port=8092,
        node_id=None,
        data_dir=Path("/tmp/agent"),
        compose_projects_dir=Path("/tmp/agent/projects"),
        model_cache_roots=(Path("/tmp/models"),),
        agent_token=TOKEN,
    )

    response = TestClient(build_app(config)).get("/health")

    assert response.json() == {"status": "ok", "node_id": None}


def test_info_requires_token(client):
    response = client.get("/info")

    assert response.status_code == 401
    assert response.headers["www-authenticate"] == "Bearer"


def test_info_rejects_wrong_token(client):
    assert client.get("/info", headers=auth("wrong-token")).status_code == 401


def test_info_returns_node_metadata(client):
    response = client.get("/info", headers=auth())

    assert response.status_code == 200
    body = response.json()
    assert body["node_id"] == "spark-01"
    assert body["version"] == spark_fleet.__version__
    assert body["hostname"]
    assert body["platform"]
    assert body["python_version"]
    assert body["data_dir"] == "/var/lib/spark-fleet-agent"
    assert body["compose_projects_dir"] == "/var/lib/spark-fleet-agent/projects"
    assert body["model_cache_roots"] == ["/srv/spark-fleet/hub", "/mnt/models"]


def test_app_uses_configured_token():
    config = AgentConfig(
        host="127.0.0.1",
        port=8092,
        node_id="spark-02",
        data_dir=Path("/tmp/agent"),
        compose_projects_dir=Path("/tmp/agent/projects"),
        model_cache_roots=(Path("/tmp/models"),),
        agent_token="another-token-0123456789",
    )
    client = TestClient(build_app(config))

    assert client.get("/info", headers=auth(TOKEN)).status_code == 401
    assert client.get("/info", headers=auth("another-token-0123456789")).status_code == 200


def test_main_runs_uvicorn_with_config(monkeypatch):
    calls = []
    monkeypatch.setattr("spark_fleet.agent.main.load_agent_config", lambda: CONFIG)
    monkeypatch.setattr(
        "spark_fleet.agent.main.uvicorn.run",
        lambda app, **kwargs: calls.append((app, kwargs)),
    )

    main()

    assert len(calls) == 1
    app, kwargs = calls[0]
    assert isinstance(app, FastAPI)
    assert kwargs == {"host": "127.0.0.1", "port": 8092, "log_level": "info"}
