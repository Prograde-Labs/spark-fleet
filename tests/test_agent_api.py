"""Tests for the agent FastAPI routes, auth, and entrypoint."""

from pathlib import Path
from types import SimpleNamespace

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

import spark_fleet
import spark_fleet.agent.api as api_module
from spark_fleet.agent.api import build_app
from spark_fleet.agent.compose import ContainerStatus, ProjectSpec
from spark_fleet.agent.main import main
from spark_fleet.agent.models import CacheRoot, Catalog, ModelEntry, ModelJobStatus
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


# Operational routes -------------------------------------------------------


@pytest.fixture
def agent(tmp_path):
    models = tmp_path / "models"
    models.mkdir()
    config = AgentConfig(
        host="127.0.0.1",
        port=8092,
        node_id="spark-01",
        data_dir=tmp_path / "agent",
        compose_projects_dir=tmp_path / "agent" / "projects",
        model_cache_roots=(models,),
        agent_token=TOKEN,
        log_level="INFO",
    )
    app = build_app(config)
    return SimpleNamespace(app=app, client=TestClient(app), config=config, models=models)


def compose_spec(tmp_path, project="spark-fleet-glm-head"):
    return ProjectSpec(
        project=project,
        directory=tmp_path / "agent" / "projects" / project,
        deployment="glm",
        recipe="glm",
        role="head",
        config_hash="abc123",
    )


def container_status():
    return ContainerStatus(
        id="c1",
        name="spark-fleet-glm-head",
        service="workload",
        state="running",
        status="Up 2 minutes",
        labels={"com.docker.compose.project": "spark-fleet-glm-head"},
    )


def test_compose_validate_requires_auth(agent):
    response = agent.client.post("/compose/validate", json={"project": "p", "compose": {}})

    assert response.status_code == 401


def test_compose_validate_returns_hash(agent, monkeypatch, tmp_path):
    spec = compose_spec(tmp_path)
    monkeypatch.setattr(api_module, "validate_project", lambda directory, compose, project: spec)

    response = agent.client.post(
        "/compose/validate",
        json={"project": "spark-fleet-glm-head", "compose": {"services": {}}},
        headers=auth(),
    )

    assert response.status_code == 200
    assert response.json()["config_hash"] == "abc123"
    assert response.json()["directory"] == str(spec.directory)


def test_compose_validate_rejects_bad_body(agent):
    response = agent.client.post("/compose/validate", json={"project": "p"}, headers=auth())

    assert response.status_code == 400
    assert response.json()["detail"] == "compose must be a mapping"


def test_compose_up_returns_containers(agent, monkeypatch, tmp_path):
    spec = compose_spec(tmp_path)
    monkeypatch.setattr(api_module, "validate_project", lambda directory, compose, project: spec)
    monkeypatch.setattr(api_module, "up_project", lambda spec: [container_status()])

    response = agent.client.post(
        "/compose/up",
        json={"project": "spark-fleet-glm-head", "compose": {}},
        headers=auth(),
    )

    assert response.status_code == 200
    assert response.json()["containers"][0]["id"] == "c1"


def test_compose_down_and_status(agent, monkeypatch, tmp_path):
    compose_spec(tmp_path)
    monkeypatch.setattr(api_module, "down_project", lambda spec: None)
    monkeypatch.setattr(api_module, "status_project", lambda spec: [container_status()])

    down = agent.client.post(
        "/compose/down", json={"project": "spark-fleet-glm-head"}, headers=auth()
    )
    status = agent.client.get(
        "/compose/status", params={"project": "spark-fleet-glm-head"}, headers=auth()
    )

    assert down.json() == {"stopped": "spark-fleet-glm-head"}
    assert status.json()["containers"][0]["state"] == "running"


def test_compose_status_rejects_invalid_project(agent):
    response = agent.client.get(
        "/compose/status", params={"project": "Bad Project"}, headers=auth()
    )

    assert response.status_code == 400


def test_containers_list_logs_and_stream(agent, monkeypatch):
    monkeypatch.setattr(
        api_module, "managed_containers", lambda deployment, role: [container_status()]
    )
    monkeypatch.setattr(api_module, "container_logs", lambda container_id, tail: "hello")
    monkeypatch.setattr(
        api_module, "follow_container_logs", lambda container_id, tail: iter(["a\n", "b\n"])
    )

    listed = agent.client.get("/containers", headers=auth())
    logs = agent.client.get(
        "/containers/logs", params={"container_id": "c1", "tail": 10}, headers=auth()
    )
    stream = agent.client.get(
        "/containers/logs/stream", params={"container_id": "c1"}, headers=auth()
    )

    assert listed.json()["containers"][0]["name"] == "spark-fleet-glm-head"
    assert logs.json() == {"logs": "hello"}
    assert stream.text == "a\nb\n"


def test_containers_errors_are_400(agent, monkeypatch):
    from spark_fleet.agent.containers import ContainerError

    def fail(*args, **kwargs):
        raise ContainerError("unknown container")

    monkeypatch.setattr(api_module, "container_logs", fail)

    response = agent.client.get(
        "/containers/logs", params={"container_id": "missing"}, headers=auth()
    )

    assert response.status_code == 400
    assert response.json()["detail"] == "unknown container"


def test_models_lists_cache_roots(agent):
    response = agent.client.get("/models", headers=auth())

    assert response.status_code == 200
    body = response.json()
    assert body["roots"][0]["path"] == str(agent.models)
    assert body["roots"][0]["exists"] is True
    assert body["models"] == []


def test_models_scan_serializes_entries(agent, monkeypatch):
    catalog = Catalog(
        roots=(CacheRoot(path=agent.models, exists=True, error=None),),
        models=(
            ModelEntry(
                repo="org/model",
                root=agent.models,
                path=agent.models / "hub" / "models--org--model",
                cached=True,
                partial=False,
                size_bytes=123,
                error=None,
            ),
        ),
    )
    monkeypatch.setattr(agent.app.state.catalog, "scan", lambda: catalog)

    body = agent.client.get("/models", headers=auth()).json()

    assert body["models"][0]["repo"] == "org/model"
    assert body["models"][0]["size_bytes"] == 123
    assert body["models"][0]["root"] == str(agent.models)


def test_model_download_returns_job(agent, monkeypatch):
    calls = []

    def start(repo, *, root=None, revision=None, allow_patterns=None):
        calls.append((repo, root, revision, allow_patterns))
        return "job-1"

    monkeypatch.setattr(agent.app.state.model_jobs, "start", start)

    response = agent.client.post(
        "/models/download",
        json={"repo": "org/model", "root": str(agent.models), "allow_patterns": ["*.safetensors"]},
        headers=auth(),
    )

    assert response.json() == {"job_id": "job-1"}
    assert calls == [("org/model", agent.models, None, ["*.safetensors"])]


def test_model_download_rejects_invalid_input(agent):
    assert (
        agent.client.post("/models/download", json={"repo": "bad repo"}, headers=auth()).status_code
        == 400
    )
    assert (
        agent.client.post(
            "/models/download", json={"repo": "org/model", "root": 1}, headers=auth()
        ).status_code
        == 400
    )


def test_model_jobs_status_list_and_cancel(agent, monkeypatch):
    status = ModelJobStatus(
        id="job-1",
        repo="org/model",
        root=agent.models,
        state="running",
        phase="download",
        files_done=1,
        files_total=2,
        bytes_done=10,
        bytes_total=20,
        error=None,
        path=None,
        started_at=1.0,
        finished_at=None,
    )
    monkeypatch.setattr(agent.app.state.model_jobs, "status", lambda job_id: status)
    monkeypatch.setattr(agent.app.state.model_jobs, "list", lambda: [status])
    monkeypatch.setattr(agent.app.state.model_jobs, "cancel", lambda job_id: True)

    fetched = agent.client.get("/models/jobs/job-1", headers=auth())
    listed = agent.client.get("/models/jobs", headers=auth())
    cancelled = agent.client.post("/models/jobs/job-1/cancel", headers=auth())

    assert fetched.json()["state"] == "running"
    assert fetched.json()["root"] == str(agent.models)
    assert listed.json()["jobs"][0]["id"] == "job-1"
    assert cancelled.json() == {"cancelled": True}


def test_model_job_status_unknown_is_404(agent):
    response = agent.client.get("/models/jobs/missing", headers=auth())

    assert response.status_code == 404


def test_model_remove_requires_confirmation(agent, monkeypatch):
    calls = []

    def remove(repo, *, root=None, confirm=False, in_use=()):
        calls.append((repo, root, confirm, list(in_use)))

    monkeypatch.setattr(agent.app.state.model_jobs, "remove", remove)

    denied = agent.client.post(
        "/models/remove", json={"repo": "org/model", "confirm": False}, headers=auth()
    )
    assert denied.status_code == 400

    response = agent.client.post(
        "/models/remove",
        json={"repo": "org/model", "confirm": True, "in_use": ["org/model"]},
        headers=auth(),
    )

    assert response.json() == {"removed": "org/model"}
    assert calls == [("org/model", None, True, ["org/model"])]


def test_jobs_start_status_list_and_cancel(agent, monkeypatch):
    calls = []

    def start(name, image, command, *, labels, volumes=(), environment=None):
        calls.append((name, image, command, labels, volumes, environment))
        return SimpleNamespace(name=name)

    snapshot = {
        "job_id": "prep-1",
        "name": "prep-1",
        "state": "succeeded",
        "exit_code": 0,
        "logs": "ok",
        "cancelled": False,
    }
    monkeypatch.setattr(agent.app.state.jobs, "start", start)
    monkeypatch.setattr(agent.app.state.jobs, "status", lambda name: snapshot)
    monkeypatch.setattr(agent.app.state.jobs, "list", lambda: [snapshot])
    monkeypatch.setattr(agent.app.state.jobs, "cancel", lambda name: True)

    started = agent.client.post(
        "/jobs",
        json={
            "name": "prep-1",
            "image": "busybox",
            "command": ["true"],
            "labels": {"a": "b"},
            "environment": {"KEY": 1},
        },
        headers=auth(),
    )
    fetched = agent.client.get("/jobs/prep-1", headers=auth())
    listed = agent.client.get("/jobs", headers=auth())
    cancelled = agent.client.post("/jobs/prep-1/cancel", headers=auth())

    assert started.json() == {"job_id": "prep-1"}
    assert calls[0][5] == {"KEY": "1"}
    assert fetched.json()["state"] == "succeeded"
    assert listed.json()["jobs"][0]["name"] == "prep-1"
    assert cancelled.json() == {"cancelled": True}


def test_jobs_reject_invalid_payload(agent):
    response = agent.client.post(
        "/jobs",
        json={"name": "prep-1", "image": "busybox", "command": "true", "labels": {}},
        headers=auth(),
    )

    assert response.status_code == 400
    assert response.json()["detail"] == "command must be a list of strings"


def test_jobs_unknown_is_404(agent):
    response = agent.client.get("/jobs/missing", headers=auth())

    assert response.status_code == 404
