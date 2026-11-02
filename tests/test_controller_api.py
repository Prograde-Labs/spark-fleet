"""Tests for the controller HTTP API, gateway routes, and SSE stream."""

from __future__ import annotations

import json
from types import SimpleNamespace

import httpx
import pytest
from fastapi.testclient import TestClient

import spark_fleet.controller.api as api_module
from spark_fleet.controller.agent_client import AgentError
from spark_fleet.controller.api import build_app
from spark_fleet.controller.deployments import DeploymentError
from spark_fleet.controller.gateway import FleetGateway
from spark_fleet.controller.metrics import MetricsStore
from spark_fleet.controller.nodes import NodeRegistry
from spark_fleet.shared.db import Database
from spark_fleet.shared.events import EventBus
from spark_fleet.shared.security import SESSION_COOKIE_NAME, verify_session

SECRET = "session-secret"
PASSWORD = "hunter2"

RECIPE_YAML = """\
apiVersion: spark-fleet/v1
kind: Recipe
metadata:
  name: glm
spec:
  mode: deployment
  runtime: vllm
  model:
    repo: org/model
    servedName: GLM
  gateway:
    aliases: [glm]
  roles:
    - name: head
      node: spark-01
      class: heavy
      inferencePort: 8888
      service:
        image: vllm:tag
"""

SECOND_RECIPE_YAML = """\
apiVersion: spark-fleet/v1
kind: Recipe
metadata:
  name: other
spec:
  mode: deployment
  runtime: vllm
  model:
    repo: org/other
    servedName: OTHER
  gateway:
    aliases: [glm]
  roles:
    - name: head
      node: spark-01
      class: heavy
      inferencePort: 8889
      service:
        image: vllm:tag
"""

SERVICE_RECIPE_YAML = """\
apiVersion: spark-fleet/v1
kind: Recipe
metadata:
  name: lights
spec:
  mode: service
  runtime: custom
  roles:
    - name: light
      node: spark-01
      class: light
      service:
        image: busybox
"""


class FakeAgentClient:
    """In-memory stand-in for AgentClient used by node registry and models."""

    def __init__(self, base_url, token):
        self.base_url = base_url
        self.token = token
        self.calls = []

    async def health(self):
        self.calls.append("health")
        if STATE.get("health_error"):
            raise AgentError("unreachable")
        return {"status": "ok"}

    async def info(self):
        self.calls.append("info")
        if STATE.get("health_error"):
            raise AgentError("unreachable")
        return {"node_id": STATE.get("reported_node_id", "spark-01")}

    async def models(self):
        self.calls.append("models")
        if STATE.get("models_error"):
            raise AgentError("catalog unavailable")
        return {"models": STATE.get("models", [])}

    async def start_model_download(self, repo, *, root=None, revision=None, allow_patterns=None):
        self.calls.append("download")
        if STATE.get("download_error"):
            raise AgentError("download rejected")
        STATE["download"] = {
            "repo": repo,
            "root": root,
            "revision": revision,
            "allow_patterns": allow_patterns,
        }
        return "dl-job"

    async def remove_model(self, repo, *, root=None, confirm=False, in_use=()):
        self.calls.append("remove")
        if STATE.get("remove_error"):
            raise AgentError("remove rejected")
        STATE["removed"] = {"repo": repo, "root": root, "confirm": confirm, "in_use": list(in_use)}

    async def aclose(self):
        self.calls.append("close")


class FakeDeployments:
    def __init__(self):
        self.calls = []

    async def load(self, deployment_id, *, primary=False):
        self.calls.append(("load", deployment_id, primary))
        if STATE.get("deployment_error"):
            raise DeploymentError(STATE["deployment_error"])
        return "job-load"

    async def unload(self, deployment_id):
        self.calls.append(("unload", deployment_id))
        return "job-unload"

    async def redeploy(self, deployment_id, *, primary=False, recipe=None):
        self.calls.append(("redeploy", deployment_id, primary, recipe))
        return "job-redeploy"


class FakeRunner:
    def __init__(self):
        self.cancelled = []

    async def cancel(self, job_id):
        self.cancelled.append(job_id)
        return STATE.get("cancel_result", True)


STATE: dict = {}


@pytest.fixture(autouse=True)
def reset_state():
    STATE.clear()
    STATE["models"] = [{"repo": "org/model", "cached": True}]
    yield
    STATE.clear()


@pytest.fixture
def db(tmp_path):
    return Database(tmp_path / "fleet.db")


@pytest.fixture
def env(tmp_path, db):
    bus = EventBus()
    gateway = FleetGateway(db, transport=httpx.MockTransport(_gateway_handler))
    runner = FakeRunner()
    deployments = FakeDeployments()
    registry = NodeRegistry(db, "agent-token", client_factory=FakeAgentClient)
    metrics = MetricsStore(db)
    recipes_dir = tmp_path / "recipes"
    recipes_dir.mkdir()
    app = build_app(
        db=db,
        registry=registry,
        deployments=deployments,
        gateway=gateway,
        metrics_store=metrics,
        runner=runner,
        bus=bus,
        recipes_dir=recipes_dir,
        agent_token="agent-token",
        client_factory=FakeAgentClient,
    )
    return SimpleNamespace(
        app=app,
        db=db,
        bus=bus,
        gateway=gateway,
        runner=runner,
        deployments=deployments,
        registry=registry,
        metrics=metrics,
        recipes_dir=recipes_dir,
        client=TestClient(app),
    )


@pytest.fixture
def secured_env(tmp_path, db):
    bus = EventBus()
    gateway = FleetGateway(db, transport=httpx.MockTransport(_gateway_handler))
    runner = FakeRunner()
    deployments = FakeDeployments()
    registry = NodeRegistry(db, "agent-token", client_factory=FakeAgentClient)
    metrics = MetricsStore(db)
    recipes_dir = tmp_path / "recipes"
    recipes_dir.mkdir()
    app = build_app(
        db=db,
        registry=registry,
        deployments=deployments,
        gateway=gateway,
        metrics_store=metrics,
        runner=runner,
        bus=bus,
        recipes_dir=recipes_dir,
        agent_token="agent-token",
        client_factory=FakeAgentClient,
        session_secret=SECRET,
        ui_password=PASSWORD,
    )
    return SimpleNamespace(
        app=app,
        db=db,
        client=TestClient(app),
        recipes_dir=recipes_dir,
    )


def _gateway_handler(request):
    custom = STATE.get("gateway_handler")
    if custom is not None:
        return custom(request)
    return httpx.Response(200, json={"object": "chat.completion", "usage": {"prompt_tokens": 1}})


def add_node(client, node_id="spark-01", lan_ip="192.0.2.239"):
    response = client.post(
        "/api/nodes",
        json={
            "id": node_id,
            "displayName": node_id,
            "lanIp": lan_ip,
            "fabricIp": "198.51.100.10",
            "agentUrl": f"http://{lan_ip}:8092",
        },
    )
    assert response.status_code == 200, response.text
    return response.json()


def add_recipe(env, name="glm", yaml=RECIPE_YAML):
    path = env.recipes_dir / f"{name}.yaml"
    path.write_text(yaml)
    return path


def add_deployment(db, deployment_id="dep-1", name="glm", status="pending", recipe=None):
    if recipe is None:
        from spark_fleet.shared.recipe import parse_recipe_yaml

        recipe = parse_recipe_yaml(RECIPE_YAML).model_dump(by_alias=True, exclude_unset=True)
    db.create_deployment(deployment_id, name, name, "deployment", recipe, status=status)
    return deployment_id


# Health and session -------------------------------------------------------


def test_health(env):
    response = env.client.get("/health")

    assert response.status_code == 200
    assert response.json() == {"status": "ok"}


def test_session_reports_login_disabled(env):
    response = env.client.get("/api/session")

    assert response.json() == {"enabled": False, "authenticated": False}


def test_login_is_disabled_without_password(env):
    response = env.client.post("/api/login", json={"password": "anything"})

    assert response.status_code == 400
    assert response.json()["detail"] == "login is disabled"


def test_login_sets_cookie_and_requires_csrf(secured_env):
    client = secured_env.client

    assert client.post("/api/login", json={"password": "wrong"}).status_code == 401
    response = client.post("/api/login", json={"password": PASSWORD})
    assert response.status_code == 200
    assert response.json() == {"authenticated": True}
    assert SESSION_COOKIE_NAME in response.cookies

    session = verify_session(SECRET, response.cookies[SESSION_COOKIE_NAME])
    assert client.get("/api/session").json() == {"enabled": True, "authenticated": True}

    denied = client.put("/api/settings/theme", json={"value": "dark"})
    assert denied.status_code == 403
    assert denied.json()["detail"] == "missing CSRF token"

    allowed = client.put(
        "/api/settings/theme",
        json={"value": "dark"},
        headers={"X-CSRF-Token": session.csrf},
    )
    assert allowed.status_code == 200
    assert secured_env.db.list_settings() == {"theme": "dark"}

    client.post("/api/logout")
    assert client.get("/api/session").json() == {"enabled": True, "authenticated": False}


def test_secured_requires_login(secured_env):
    response = secured_env.client.get("/api/nodes")

    assert response.status_code == 401
    assert response.json()["detail"] == "login required"


# Nodes --------------------------------------------------------------------


def test_create_list_update_and_delete_node(env):
    node = add_node(env.client)

    assert node["id"] == "spark-01"
    assert node["lan_ip"] == "192.0.2.239"

    listed = env.client.get("/api/nodes").json()["nodes"]
    assert [item["id"] for item in listed] == ["spark-01"]

    updated = env.client.put("/api/nodes/spark-01", json={"displayName": "Head", "enabled": False})
    assert updated.status_code == 200
    assert updated.json()["display_name"] == "Head"
    assert updated.json()["enabled"] is False

    deleted = env.client.delete("/api/nodes/spark-01")
    assert deleted.status_code == 200
    assert env.client.get("/api/nodes").json()["nodes"] == []


def test_create_node_rejects_invalid_payload(env):
    response = env.client.post(
        "/api/nodes",
        json={
            "id": "Bad Id",
            "displayName": "Bad",
            "lanIp": "192.0.2.239",
            "fabricIp": "198.51.100.10",
            "agentUrl": "http://192.0.2.239:8092",
        },
    )

    assert response.status_code == 400
    assert "invalid node id" in response.json()["detail"]


def test_check_node_healthy_records_info(env):
    add_node(env.client)

    response = env.client.post("/api/nodes/spark-01/check")

    assert response.status_code == 200
    body = response.json()
    assert body["healthy"] is True
    assert body["info"] == {"node_id": "spark-01"}
    assert env.db.get_node("spark-01")["last_seen_at"] is not None


def test_check_node_reports_agent_failure(env):
    add_node(env.client)
    STATE["health_error"] = True

    response = env.client.post("/api/nodes/spark-01/check")

    assert response.status_code == 200
    body = response.json()
    assert body["healthy"] is False
    assert body["error"] == "unreachable"


def test_check_unknown_node_is_404(env):
    response = env.client.post("/api/nodes/nope/check")

    assert response.status_code == 404


# Recipes ------------------------------------------------------------------


def test_put_get_list_and_validate_recipe(env):
    add_node(env.client)

    put = env.client.put("/api/recipes/glm", json={"yaml": RECIPE_YAML})
    assert put.status_code == 200
    assert "name: glm" in put.json()["yaml"]

    listed = env.client.get("/api/recipes").json()["recipes"]
    assert listed[0]["name"] == "glm"
    assert listed[0]["servedName"] == "GLM"

    fetched = env.client.get("/api/recipes/glm")
    assert fetched.status_code == 200
    assert "repo: org/model" in fetched.json()["yaml"]

    validated = env.client.post("/api/recipes/validate", json={"yaml": SERVICE_RECIPE_YAML})
    assert validated.status_code == 200
    assert validated.json()["recipe"]["spec"]["mode"] == "service"


def test_put_recipe_rejects_name_mismatch(env):
    add_node(env.client)

    response = env.client.put("/api/recipes/other", json={"yaml": RECIPE_YAML})

    assert response.status_code == 400
    assert response.json()["detail"] == "recipe name does not match path"


def test_put_recipe_rejects_invalid_yaml(env):
    response = env.client.put("/api/recipes/bad", json={"yaml": "not: [valid"})

    assert response.status_code == 400


def test_recipe_alias_conflict_is_409(env):
    add_node(env.client)
    add_recipe(env, "glm", RECIPE_YAML)

    response = env.client.put("/api/recipes/other", json={"yaml": SECOND_RECIPE_YAML})

    assert response.status_code == 409
    assert "aliases already used by glm" in response.json()["detail"]


def test_recipe_validation_rejects_unknown_node(env):
    response = env.client.put("/api/recipes/glm", json={"yaml": RECIPE_YAML})

    assert response.status_code == 400
    assert "unknown node" in response.json()["detail"]


def test_delete_recipe(env):
    add_recipe(env, "glm")

    response = env.client.delete("/api/recipes/glm")

    assert response.status_code == 200
    assert env.client.get("/api/recipes/glm").status_code == 404


def test_delete_loaded_recipe_is_409(env):
    add_node(env.client)
    add_recipe(env, "glm")
    add_deployment(env.db, status="loaded")

    response = env.client.delete("/api/recipes/glm")

    assert response.status_code == 409
    assert response.json()["detail"] == "recipe is used by a loaded deployment"


def test_swap_model_returns_diff_and_applies(env):
    add_node(env.client)
    add_recipe(env, "glm")

    preview = env.client.post(
        "/api/recipes/glm/swap-model",
        json={"repo": "org/new-model", "servedName": "NEW"},
    )
    assert preview.status_code == 200
    assert "org/new-model" in preview.json()["yaml"]
    assert "GLM" not in preview.json()["yaml"]
    assert "-    servedName: GLM" in preview.json()["diff"]
    assert "org/model" in env.recipes_dir.joinpath("glm.yaml").read_text()

    applied = env.client.post(
        "/api/recipes/glm/swap-model",
        json={"repo": "org/new-model", "servedName": "NEW", "apply": True},
    )
    assert applied.status_code == 200
    assert "org/new-model" in env.recipes_dir.joinpath("glm.yaml").read_text()


def test_swap_model_rejects_invalid_repo(env):
    add_recipe(env, "glm")

    response = env.client.post(
        "/api/recipes/glm/swap-model", json={"repo": "bad repo", "servedName": "X"}
    )

    assert response.status_code == 400
    assert response.json()["detail"] == "invalid repo"


def test_swap_model_requires_model(env):
    add_recipe(env, "lights", SERVICE_RECIPE_YAML)

    response = env.client.post(
        "/api/recipes/lights/swap-model", json={"repo": "org/x", "servedName": "X"}
    )

    assert response.status_code == 400
    assert response.json()["detail"] == "recipe has no model"


# Deployments --------------------------------------------------------------


def test_create_list_get_and_delete_deployment(env):
    add_node(env.client)
    add_recipe(env)

    created = env.client.post("/api/deployments", json={"recipe": "glm"})
    assert created.status_code == 200
    deployment_id = created.json()["deployment"]["id"]
    assert created.json()["deployment"]["status"] == "pending"
    assert created.json()["roles"] == []

    listed = env.client.get("/api/deployments").json()["deployments"]
    assert [item["deployment"]["id"] for item in listed] == [deployment_id]

    fetched = env.client.get(f"/api/deployments/{deployment_id}")
    assert fetched.status_code == 200
    assert fetched.json()["deployment"]["name"] == "glm"

    deleted = env.client.delete(f"/api/deployments/{deployment_id}")
    assert deleted.status_code == 200
    assert env.client.get(f"/api/deployments/{deployment_id}").status_code == 404


def test_create_deployment_rejects_duplicate_name(env):
    add_node(env.client)
    add_recipe(env)
    add_deployment(env.db)

    response = env.client.post("/api/deployments", json={"recipe": "glm"})

    assert response.status_code == 409


def test_delete_loaded_deployment_is_409(env):
    add_deployment(env.db, status="loaded")

    response = env.client.delete("/api/deployments/dep-1")

    assert response.status_code == 409
    assert response.json()["detail"] == "unload before deleting"


def test_load_unload_redeploy_and_primary(env):
    add_node(env.client)
    add_recipe(env)
    add_deployment(env.db)

    loaded = env.client.post("/api/deployments/dep-1/load", json={"primary": True})
    assert loaded.status_code == 200
    assert loaded.json() == {"job_id": "job-load"}
    assert env.deployments.calls[-1] == ("load", "dep-1", True)

    unloaded = env.client.post("/api/deployments/dep-1/unload")
    assert unloaded.status_code == 200
    assert env.deployments.calls[-1] == ("unload", "dep-1")

    redeployed = env.client.post("/api/deployments/dep-1/redeploy", json={"primary": False})
    assert redeployed.status_code == 200
    assert env.deployments.calls[-1][0] == "redeploy"
    assert env.deployments.calls[-1][3].metadata.name == "glm"

    env.db.update_deployment("dep-1", status="loaded")
    primary = env.client.post("/api/deployments/dep-1/primary")
    assert primary.status_code == 200
    assert env.db.get_deployment("dep-1")["is_primary"] is True


def test_load_error_is_400(env):
    add_deployment(env.db)
    STATE["deployment_error"] = "model missing"

    response = env.client.post("/api/deployments/dep-1/load")

    assert response.status_code == 400
    assert response.json()["detail"] == "model missing"


def test_primary_requires_loaded_deployment(env):
    add_deployment(env.db, status="pending")

    response = env.client.post("/api/deployments/dep-1/primary")

    assert response.status_code == 409


# Models -------------------------------------------------------------------


def test_list_models_reports_per_node(env):
    add_node(env.client, "spark-01")
    add_node(env.client, "spark-02", "192.0.2.79")

    body = env.client.get("/api/models").json()

    assert body["nodes"][0]["models"]["models"] == [{"repo": "org/model", "cached": True}]
    assert body["nodes"][1]["node_id"] == "spark-02"


def test_list_models_reports_agent_error(env):
    add_node(env.client)
    STATE["models_error"] = True

    body = env.client.get("/api/models").json()

    assert body["nodes"][0]["error"] == "catalog unavailable"


def test_download_model_dispatches_to_nodes(env):
    add_node(env.client)

    response = env.client.post(
        "/api/models/download",
        json={"repo": "org/model", "nodes": ["spark-01"], "root": "/models"},
    )

    assert response.status_code == 200
    assert response.json()["jobs"] == [{"node_id": "spark-01", "job_id": "dl-job"}]
    assert STATE["download"]["root"] == "/models"


def test_download_model_validates_repo_and_nodes(env):
    assert (
        env.client.post(
            "/api/models/download", json={"repo": "bad repo", "nodes": ["spark-01"]}
        ).status_code
        == 400
    )
    assert (
        env.client.post("/api/models/download", json={"repo": "org/model", "nodes": []}).status_code
        == 400
    )
    assert (
        env.client.post(
            "/api/models/download", json={"repo": "org/model", "nodes": ["nope"]}
        ).status_code
        == 404
    )


def test_remove_model_unknown_node_is_404(env):
    response = env.client.delete(
        "/api/models/org/model", params={"node_id": "nope", "confirm": "true"}
    )

    assert response.status_code == 404


def test_remove_model_requires_confirmation_and_passes_in_use(env):
    add_node(env.client)
    add_deployment(env.db, status="loaded")

    denied = env.client.delete("/api/models/org/model", params={"node_id": "spark-01"})
    assert denied.status_code == 400
    assert denied.json()["detail"] == "explicit confirmation required"

    response = env.client.delete(
        "/api/models/org/model",
        params={"node_id": "spark-01", "confirm": "true", "root": "/models"},
    )
    assert response.status_code == 200
    assert response.json() == {"removed": "org/model", "node_id": "spark-01"}
    assert STATE["removed"]["in_use"] == ["org/model"]
    assert STATE["removed"]["confirm"] is True


def test_remove_model_reports_agent_failure(env):
    add_node(env.client)
    STATE["remove_error"] = True

    response = env.client.delete(
        "/api/models/org/model", params={"node_id": "spark-01", "confirm": "true"}
    )

    assert response.status_code == 502
    assert response.json()["detail"] == "remove rejected"


# Jobs ---------------------------------------------------------------------


def test_jobs_list_get_and_cancel(env):
    env.db.create_job("job-1", "deployment.load")
    env.db.append_job_event("job-1", "queued", phase="queued")

    listed = env.client.get("/api/jobs")
    assert listed.status_code == 200
    assert [job["id"] for job in listed.json()["jobs"]] == ["job-1"]

    fetched = env.client.get("/api/jobs/job-1")
    assert fetched.status_code == 200
    assert fetched.json()["events"][0]["message"] == "queued"

    cancelled = env.client.post("/api/jobs/job-1/cancel")
    assert cancelled.status_code == 200
    assert cancelled.json() == {"cancelled": True}
    assert env.runner.cancelled == ["job-1"]

    assert env.client.get("/api/jobs/missing").status_code == 404
    assert env.client.post("/api/jobs/missing/cancel").status_code == 404


# Metrics ------------------------------------------------------------------


def test_node_and_inference_metrics(env):
    env.db.upsert_node(
        "spark-01",
        display_name="spark-01",
        lan_ip="192.0.2.239",
        fabric_ip="198.51.100.10",
        agent_url="http://192.0.2.239:8092",
    )
    env.metrics.record_node_snapshot("spark-01", {"cache_bytes": 10}, ts=100.0)
    add_deployment(env.db, status="loaded")
    env.metrics.record_gateway_sample("dep-1", requests=2, ts=100.0)

    node = env.client.get("/api/metrics/nodes/spark-01").json()
    assert node["latest"] == {"cache_bytes": 10.0}
    assert node["series"]["cache_bytes"][0]["value"] == 10.0

    inference = env.client.get("/api/metrics/inference/dep-1").json()
    assert inference["totals"]["requests"] == 2
    assert inference["series"][0]["requests"] == 2


# Settings -----------------------------------------------------------------


def test_settings_round_trip(env):
    assert env.client.get("/api/settings").json() == {"settings": {}}

    put = env.client.put("/api/settings/theme", json={"value": "dark"})
    assert put.status_code == 200
    assert env.client.get("/api/settings").json() == {"settings": {"theme": "dark"}}

    invalid = env.client.put("/api/settings/theme", json={"value": 1})
    assert invalid.status_code == 400


# Import -------------------------------------------------------------------


class FakeOldClient:
    def __init__(self, base_url):
        self.base_url = base_url
        self.closed = False

    async def workloads(self):
        return [
            {
                "name": "train-head",
                "template": "vllm",
                "class": "heavy",
                "inference_port": 8888,
                "models": ["org/model"],
                "served_model": "TRAIN",
            }
        ]

    async def instance(self, name):
        return json.dumps(
            {
                "x-class": "heavy",
                "services": {
                    "workload": {
                        "image": "vllm:tag",
                        "environment": {"MASTER_ADDR": "198.51.100.10", "MASTER_PORT": "25000"},
                        "command": ["vllm", "serve", "org/model", "--served-model-name", "TRAIN"],
                    }
                },
            }
        )

    async def models(self):
        return []

    async def aclose(self):
        self.closed = True


def test_import_plan_and_apply(env, monkeypatch):
    add_node(env.client)
    monkeypatch.setattr(api_module, "OldControllerClient", FakeOldClient)

    plan = env.client.post(
        "/api/import/plan",
        json={"sources": [{"nodeId": "spark-01", "url": "http://old:8091"}]},
    )
    assert plan.status_code == 200
    body = plan.json()
    assert body["proposals"][0]["name"] == "train"
    assert "diff" in body["proposals"][0]

    denied = env.client.post(
        "/api/import/apply",
        json={"sources": [{"nodeId": "spark-01", "url": "http://old:8091"}]},
    )
    assert denied.status_code == 400

    applied = env.client.post(
        "/api/import/apply",
        json={
            "sources": [{"nodeId": "spark-01", "url": "http://old:8091"}],
            "confirm": True,
        },
    )
    assert applied.status_code == 200
    assert len(applied.json()["created"]) == 1


def test_import_rejects_invalid_source(env):
    response = env.client.post(
        "/api/import/plan",
        json={"sources": [{"nodeId": "spark-01", "url": "ftp://old:8091"}]},
    )

    assert response.status_code == 400
    assert "invalid controller URL" in response.json()["detail"]


# Gateway ------------------------------------------------------------------


def test_gateway_models_lists_loaded_deployments(env):
    add_node(env.client)
    add_deployment(env.db, status="loaded")

    response = env.client.get("/v1/models")

    assert response.status_code == 200
    ids = [item["id"] for item in response.json()["data"]]
    assert ids == ["GLM", "glm"]


def test_gateway_proxy_returns_upstream_response(env):
    add_node(env.client)
    add_deployment(env.db, status="loaded")
    seen = {}

    def handler(request):
        seen["model"] = json.loads(request.content)["model"]
        return httpx.Response(200, json={"ok": True})

    STATE["gateway_handler"] = handler

    response = env.client.post("/v1/chat/completions", json={"model": "glm", "messages": []})

    assert response.status_code == 200
    assert response.json() == {"ok": True}
    assert seen["model"] == "GLM"


def test_gateway_proxy_streams(env):
    add_node(env.client)
    add_deployment(env.db, status="loaded")

    body = (
        b'data: {"choices":[{"delta":{"content":"hi"}}]}\n\n'
        b'data: {"usage":{"prompt_tokens":2,"completion_tokens":1}}\n\n'
        b"data: [DONE]\n\n"
    )

    def handler(request):
        return httpx.Response(200, content=body, headers={"Content-Type": "text/event-stream"})

    STATE["gateway_handler"] = handler

    response = env.client.post("/v1/chat/completions", json={"model": "glm", "stream": True})

    assert response.status_code == 200
    assert response.content == body


def test_gateway_errors_map_to_status_codes(env):
    add_node(env.client)
    add_deployment(env.db, status="loading")

    unknown = env.client.post("/v1/chat/completions", json={"model": "nope"})
    assert unknown.status_code == 404

    loading = env.client.post("/v1/chat/completions", json={"model": "glm"})
    assert loading.status_code == 503
    assert loading.headers["Retry-After"] == "5"

    active = env.client.post("/v1/chat/completions", json={"model": "spark-active"})
    assert active.status_code == 503
    assert active.json()["detail"] == "no primary deployment is loaded"

    missing_model = env.client.post("/v1/chat/completions", json={"messages": []})
    assert missing_model.status_code == 400


# Events -------------------------------------------------------------------


def test_events_streams_frames(env, monkeypatch):
    async def fake_stream(keepalive_seconds=15.0):
        yield "event: ping\ndata: {}\n\n"

    monkeypatch.setattr(env.bus, "sse_stream", fake_stream)

    with env.client.stream("GET", "/events") as response:
        assert response.status_code == 200
        assert response.headers["content-type"].startswith("text/event-stream")
        assert b"event: ping" in response.read()
