"""Tests for controller wiring, reconciliation, pollers, and static serving."""

from __future__ import annotations

from types import SimpleNamespace

import pytest
from fastapi.testclient import TestClient

import spark_fleet.controller.main as main_module
from spark_fleet.controller.agent_client import AgentError
from spark_fleet.controller.main import create_controller
from spark_fleet.shared.config import ConfigError, ControllerConfig
from spark_fleet.shared.recipe import parse_recipe_yaml

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

STATE: dict = {}


class FakeAgentClient:
    def __init__(self, base_url, token):
        self.base_url = base_url
        self.token = token

    async def health(self):
        if STATE.get("health_error"):
            raise AgentError("unreachable")
        return {"status": "ok"}

    async def info(self):
        return {"node_id": STATE.get("reported_node_id", "spark-01")}

    async def metrics(self):
        if STATE.get("metrics_error"):
            raise AgentError("metrics unavailable")
        return STATE.get(
            "metrics",
            {
                "sampled_at": 1000.0,
                "host_total_bytes": 100,
                "host_available_bytes": 40,
                "cache_bytes": 10,
                "gpus": [],
            },
        )

    async def compose_status(self, project):
        if STATE.get("compose_error") or self.base_url == STATE.get("compose_error_url"):
            raise AgentError("agent down")
        return STATE.get("compose", {}).get(project, [])

    async def aclose(self):
        pass


@pytest.fixture(autouse=True)
def reset_state():
    STATE.clear()
    STATE["compose"] = {
        "spark-fleet-glm-head": [{"id": "c1", "name": "spark-fleet-glm-head", "state": "running"}]
    }
    yield
    STATE.clear()


def make_config(tmp_path, *, web_dist=None):
    data_dir = tmp_path / "data"
    return ControllerConfig(
        host="127.0.0.1",
        port=8091,
        data_dir=data_dir,
        recipes_dir=data_dir / "recipes",
        db_path=data_dir / "fleet.sqlite",
        web_dist=web_dist,
        agent_token="t" * 16,
        ui_password=None,
        session_secret=None,
    )


@pytest.fixture
def controller(tmp_path):
    controller = create_controller(make_config(tmp_path), client_factory=FakeAgentClient)
    yield controller
    try:
        controller.db.close()
    except Exception:  # noqa: BLE001 - fixture teardown is best-effort
        pass


def add_node(db, node_id="spark-01", agent_url="http://192.0.2.239:8092"):
    db.upsert_node(
        node_id,
        display_name=node_id,
        lan_ip="192.0.2.239",
        fabric_ip="198.51.100.10",
        agent_url=agent_url,
    )


def add_deployment(db, *, status="loaded", primary=False, recipe=None, with_role=True):
    if recipe is None:
        recipe = parse_recipe_yaml(RECIPE_YAML).model_dump(by_alias=True, exclude_unset=True)
    db.create_deployment("dep-1", "glm", "glm", "deployment", recipe, status=status)
    if with_role:
        db.upsert_role(
            "dep-1",
            "head",
            "spark-01",
            "heavy",
            20,
            status="running" if status == "loaded" else "pending",
            container_id="c1",
        )
    if primary:
        db.set_primary("dep-1")


def capture_events(controller):
    events = []

    async def publish(event):
        events.append(event)

    controller.bus.publish = publish
    return events


# Wiring -------------------------------------------------------------------


def test_create_controller_initializes_directories_and_app(tmp_path):
    config = make_config(tmp_path)

    controller = create_controller(config, client_factory=FakeAgentClient)

    assert config.data_dir.is_dir()
    assert config.recipes_dir.is_dir()
    assert config.db_path.is_file()
    assert controller.app.title == "spark-fleet controller"
    controller.db.close()


def test_identity_route_when_web_is_absent(tmp_path):
    controller = create_controller(make_config(tmp_path), client_factory=FakeAgentClient)

    with TestClient(controller.app) as client:
        response = client.get("/")

    assert response.status_code == 200
    assert response.json()["service"] == "spark-fleet"


def test_missing_web_dist_is_a_config_error(tmp_path):
    config = make_config(tmp_path, web_dist=tmp_path / "missing")

    with pytest.raises(ConfigError, match="WEB_DIST"):
        create_controller(config, client_factory=FakeAgentClient)


def test_static_web_serving(tmp_path):
    web_dist = tmp_path / "web"
    (web_dist / "assets").mkdir(parents=True)
    (web_dist / "index.html").write_text("<html><body>fleet</body></html>")
    (web_dist / "assets" / "app.js").write_text("console.log('fleet')")

    controller = create_controller(
        make_config(tmp_path, web_dist=web_dist), client_factory=FakeAgentClient
    )

    with TestClient(controller.app) as client:
        index = client.get("/")
        asset = client.get("/assets/app.js")
        health = client.get("/health")

    assert index.status_code == 200
    assert "fleet" in index.text
    assert asset.status_code == 200
    assert "console.log" in asset.text
    assert health.json() == {"status": "ok"}


def test_start_and_stop_lifecycle(tmp_path):
    controller = create_controller(make_config(tmp_path), client_factory=FakeAgentClient)

    import asyncio

    async def run():
        await controller.start()
        assert len(controller.tasks) == 2
        await controller.stop()

    asyncio.run(run())

    assert controller.tasks == []
    assert controller._started is False


# Reconciliation -----------------------------------------------------------


async def test_reconcile_keeps_healthy_loaded_deployment(controller):
    add_node(controller.db)
    add_deployment(controller.db)
    events = capture_events(controller)

    results = await controller.reconcile()

    assert results[0]["status"] == "ok"
    assert controller.db.get_deployment("dep-1")["status"] == "loaded"
    role = controller.db.list_roles("dep-1")[0]
    assert role["status"] == "running"
    assert role["container_id"] == "c1"
    assert events[-1].type == "deployment.reconciled"
    assert events[-1].data["status"] == "ok"


async def test_reconcile_fails_when_container_is_not_running(controller):
    add_node(controller.db)
    add_deployment(controller.db, primary=True)
    STATE["compose"]["spark-fleet-glm-head"] = [
        {"id": "c1", "name": "spark-fleet-glm-head", "state": "exited"}
    ]
    events = capture_events(controller)

    results = await controller.reconcile()

    assert results[0]["status"] == "failed"
    deployment = controller.db.get_deployment("dep-1")
    assert deployment["status"] == "failed"
    assert "container is not running" in deployment["error"]
    assert deployment["is_primary"] is False
    assert controller.db.list_roles("dep-1")[0]["status"] == "stopped"
    assert events[-1].data["status"] == "failed"


async def test_reconcile_fails_when_container_is_missing(controller):
    add_node(controller.db)
    add_deployment(controller.db)
    STATE["compose"]["spark-fleet-glm-head"] = []

    results = await controller.reconcile()

    assert results[0]["status"] == "failed"
    assert controller.db.get_deployment("dep-1")["status"] == "failed"


async def test_reconcile_keeps_state_when_agent_is_unreachable(controller):
    add_node(controller.db)
    add_deployment(controller.db)
    STATE["compose_error"] = True
    events = capture_events(controller)

    results = await controller.reconcile()

    assert results[0]["status"] == "unknown"
    assert controller.db.get_deployment("dep-1")["status"] == "loaded"
    assert events[-1].data["status"] == "unknown"


async def test_reconcile_fails_invalid_recipe(controller):
    add_node(controller.db)
    controller.db.create_deployment(
        "dep-1", "glm", "glm", "deployment", {"spec": "broken"}, status="loaded"
    )

    results = await controller.reconcile()

    assert results[0]["status"] == "failed"
    deployment = controller.db.get_deployment("dep-1")
    assert deployment["status"] == "failed"
    assert "invalid recipe" in deployment["error"]


async def test_reconcile_ignores_non_loaded_deployments(controller):
    add_node(controller.db)
    add_deployment(controller.db, status="pending")
    events = capture_events(controller)

    results = await controller.reconcile()

    assert results == []
    assert events == []
    assert controller.db.get_deployment("dep-1")["status"] == "pending"


async def test_reconcile_fails_unknown_node(controller):
    add_deployment(controller.db, with_role=False)

    results = await controller.reconcile()

    assert results[0]["status"] == "failed"
    assert "not registered" in controller.db.get_deployment("dep-1")["error"]


# Pollers ------------------------------------------------------------------


async def test_poll_nodes_once_updates_seen_and_publishes(controller):
    add_node(controller.db)
    events = capture_events(controller)

    statuses = await controller.poll_nodes_once()

    assert len(statuses) == 1
    assert statuses[0].healthy is True
    assert controller.db.get_node("spark-01")["last_seen_at"] is not None
    assert events[0].type == "node.health"
    assert events[0].data == {"node_id": "spark-01", "healthy": True, "error": None}


async def test_poll_nodes_once_reports_unreachable_agent(controller):
    add_node(controller.db)
    STATE["health_error"] = True
    events = capture_events(controller)

    statuses = await controller.poll_nodes_once()

    assert statuses[0].healthy is False
    assert events[0].data["error"] == "unreachable"


async def test_poll_metrics_once_records_snapshot_and_flushes_counters(controller):
    add_node(controller.db)
    add_deployment(controller.db)
    events = capture_events(controller)
    controller.gateway.counters.record(
        "dep-1", status_code=200, latency_ms=10.0, prompt_tokens=3, completion_tokens=2
    )

    errors = await controller.poll_metrics_once()

    assert errors == {}
    latest = controller.metrics.latest_node_metrics("spark-01")
    assert latest["host_total_bytes"] == 100.0
    assert latest["cache_bytes"] == 10.0
    rows = controller.metrics.inference_series("dep-1")
    assert rows[0]["requests"] == 1
    assert rows[0]["prompt_tokens"] == 3
    assert events[0].type == "metrics.sample"
    assert events[0].data["node_id"] == "spark-01"


async def test_poll_metrics_once_collects_agent_errors(controller):
    add_node(controller.db)
    STATE["metrics_error"] = True

    errors = await controller.poll_metrics_once()

    assert errors == {"spark-01": "metrics unavailable"}


# Entrypoint ---------------------------------------------------------------


def test_main_runs_uvicorn(monkeypatch):
    config = SimpleNamespace(host="127.0.0.1", port=8091, log_level="INFO", data_dir="/tmp/data")
    app = object()
    monkeypatch.setattr(main_module, "load_controller_config", lambda: config)
    monkeypatch.setattr(main_module, "create_controller", lambda cfg: SimpleNamespace(app=app))
    calls = []
    monkeypatch.setattr(
        main_module.uvicorn, "run", lambda *args, **kwargs: calls.append((args, kwargs))
    )

    main_module.main()

    assert calls == [((app,), {"host": "127.0.0.1", "port": 8091, "log_level": "info"})]


MULTI_RECIPE_YAML = """\
apiVersion: spark-fleet/v1
kind: Recipe
metadata:
  name: multi
spec:
  mode: deployment
  runtime: custom
  model:
    repo: org/model
    servedName: MULTI
  gateway:
    aliases: [multi]
  roles:
    - name: worker
      node: spark-02
      class: heavy
      startOrder: 10
      service:
        image: img
    - name: head
      node: spark-01
      class: heavy
      startOrder: 20
      inferencePort: 8888
      service:
        image: img
"""


async def test_reconcile_fails_on_definite_problem_despite_unreachable_agent(controller):
    add_node(controller.db, "spark-01", "http://192.0.2.239:8092")
    add_node(controller.db, "spark-02", "http://192.0.2.79:8092")
    recipe = parse_recipe_yaml(MULTI_RECIPE_YAML).model_dump(by_alias=True, exclude_unset=True)
    controller.db.create_deployment(
        "dep-1", "multi", "multi", "deployment", recipe, status="loaded"
    )
    controller.db.upsert_role(
        "dep-1", "head", "spark-01", "heavy", 20, status="running", container_id="c1"
    )
    controller.db.upsert_role(
        "dep-1", "worker", "spark-02", "heavy", 10, status="running", container_id="c2"
    )
    STATE["compose"] = {
        "spark-fleet-multi-head": [
            {"id": "c1", "name": "spark-fleet-multi-head", "state": "exited"}
        ]
    }
    STATE["compose_error_url"] = "http://192.0.2.79:8092"

    results = await controller.reconcile()

    assert results[0]["status"] == "failed"
    assert controller.db.get_deployment("dep-1")["status"] == "failed"


async def test_poll_metrics_once_skips_disabled_nodes(controller):
    controller.db.upsert_node(
        "spark-off",
        display_name="spark-off",
        lan_ip="192.0.2.250",
        fabric_ip="198.51.100.20",
        agent_url="http://192.0.2.250:8092",
        enabled=False,
    )

    errors = await controller.poll_metrics_once()

    assert errors == {}
    assert controller.metrics.latest_node_metrics("spark-off") == {}


def test_stored_settings_override_environment_defaults(tmp_path):
    config = make_config(tmp_path)
    first = create_controller(config, client_factory=FakeAgentClient)
    first.db.set_setting("rollback_policy", "leave-down")
    first.db.set_setting("metrics_retention_seconds", "120")
    first.db.set_setting("metrics_sample_interval_seconds", "5")
    first.db.set_setting("node_poll_interval_seconds", "30")
    first.db.close()

    second = create_controller(config, client_factory=FakeAgentClient)

    assert second.config.rollback_policy == "leave-down"
    assert second.config.metrics_retention_seconds == 120
    assert second.config.metrics_sample_interval_seconds == 5
    assert second.config.node_poll_interval_seconds == 30
    second.db.close()


def test_invalid_stored_settings_fall_back(tmp_path):
    config = make_config(tmp_path)
    first = create_controller(config, client_factory=FakeAgentClient)
    first.db.set_setting("rollback_policy", "explode")
    first.db.set_setting("metrics_retention_seconds", "not-a-number")
    first.db.set_setting("node_poll_interval_seconds", "99999")
    first.db.close()

    second = create_controller(config, client_factory=FakeAgentClient)

    assert second.config.rollback_policy == "restore"
    assert second.config.metrics_retention_seconds == 86400
    assert second.config.node_poll_interval_seconds == 15
    second.db.close()
