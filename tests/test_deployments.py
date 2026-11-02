"""Tests for fleet deployment load/unload/redeploy, readiness, and rollback."""

import asyncio

import pytest

from spark_fleet.controller.agent_client import AgentError
from spark_fleet.controller.deployments import (
    GLOBAL_LOCK,
    DeploymentError,
    DeploymentManager,
    check_readiness,
)
from spark_fleet.controller.nodes import NodeRegistry
from spark_fleet.shared.db import Database
from spark_fleet.shared.jobs import JobRunner
from spark_fleet.shared.recipe import parse_recipe

TOKEN = "agent-token-0123456789"
NODE_01_URL = "http://192.0.2.239:8092"
NODE_02_URL = "http://192.0.2.79:8092"


def recipe_dict(
    name="demo-glm",
    *,
    mode="deployment",
    roles=None,
    prepare=None,
    aliases=None,
    model=True,
    image="ghcr.io/org/img:tag",
    served_name="Demo-Model-B",
):
    roles = roles or [
        {
            "name": "worker",
            "node": "spark-02",
            "class": "heavy",
            "startOrder": 10,
            "service": {"image": image, "network_mode": "host"},
        },
        {
            "name": "head",
            "node": "spark-01",
            "class": "heavy",
            "startOrder": 20,
            "inferencePort": 8888,
            "service": {"image": image, "network_mode": "host"},
        },
    ]
    spec = {"mode": mode, "runtime": "custom", "roles": roles}
    if model:
        spec["model"] = {"repo": "org/model", "servedName": served_name}
    if aliases:
        spec["gateway"] = {"aliases": aliases}
    if prepare:
        spec["prepare"] = prepare
    return {
        "apiVersion": "spark-fleet/v1",
        "kind": "Recipe",
        "metadata": {"name": name},
        "spec": spec,
    }


@pytest.fixture
def db(tmp_path):
    return Database(tmp_path / "fleet.db")


@pytest.fixture
def registry(db):
    registry = NodeRegistry(db, TOKEN)
    registry.create(
        "spark-01",
        display_name="Spark 01",
        lan_ip="192.0.2.239",
        fabric_ip="198.51.100.10",
        agent_url=NODE_01_URL,
    )
    registry.create(
        "spark-02",
        display_name="Spark 02",
        lan_ip="192.0.2.79",
        fabric_ip="198.51.100.11",
        agent_url=NODE_02_URL,
    )
    return registry


@pytest.fixture
async def runner(db):
    runner = JobRunner(db, pool_size=1)
    await runner.start()
    yield runner
    await runner.stop()


class FakeAgent:
    def __init__(self, node_id):
        self.node_id = node_id
        self.calls = []
        self.fail_up = set()
        self.fail_down = set()
        self.validate_error = None
        self.status_response = [{"state": "running", "health": "healthy"}]
        self.models_response = {
            "roots": [],
            "models": [{"repo": "org/model", "cached": True}],
        }
        self.prepare_status = {"state": "succeeded"}
        self.composes = []
        self.closed = False

    async def models(self):
        self.calls.append(("models",))
        return self.models_response

    async def validate_compose(self, project, compose):
        self.calls.append(("validate", project))
        self.composes.append((project, compose))
        if self.validate_error is not None:
            raise self.validate_error
        return {"project": project, "config_hash": f"hash-{project}"}

    async def up_compose(self, project, compose):
        self.calls.append(("up", project))
        if project in self.fail_up:
            raise AgentError(f"up failed for {project}", status_code=500)
        name = compose["services"]["workload"]["container_name"]
        return [{"id": f"cid-{project}", "name": name, "state": "running", "health": "healthy"}]

    async def down_compose(self, project):
        self.calls.append(("down", project))
        if project in self.fail_down:
            raise AgentError(f"down failed for {project}", status_code=500)

    async def compose_status(self, project):
        self.calls.append(("status", project))
        return self.status_response

    async def start_job(self, **kwargs):
        self.calls.append(("job", kwargs))
        return "prepare-job-1"

    async def job_status(self, job_id):
        self.calls.append(("job_status", job_id))
        return self.prepare_status

    async def cancel_job(self, job_id):
        self.calls.append(("job_cancel", job_id))
        return True

    async def follow_container_logs(self, container_id, *, tail=200):
        self.calls.append(("follow", container_id, tail))
        for line in ("starting", "server ready", "listening"):
            yield line

    async def aclose(self):
        self.closed = True


async def no_readiness(role, node, project, client, container_id):
    return None


def create_deployment(db, deployment_id="dep-1", name="demo-glm", recipe=None, status="pending"):
    recipe_obj = parse_recipe(recipe or recipe_dict())
    db.create_deployment(
        deployment_id,
        name,
        recipe_obj.metadata.name,
        recipe_obj.spec.mode,
        recipe_obj.model_dump(by_alias=True, exclude_unset=True),
        status=status,
    )
    return recipe_obj


def add_roles(db, deployment_id, recipe_obj, status="running"):
    for role in recipe_obj.spec.roles:
        db.upsert_role(
            deployment_id,
            role.name,
            role.node,
            role.class_,
            role.start_order,
            status=status,
        )


def single_role_recipe(node, name, served_name):
    return recipe_dict(
        name=name,
        served_name=served_name,
        roles=[
            {
                "name": "head",
                "node": node,
                "class": "heavy",
                "startOrder": 10,
                "inferencePort": 8888,
                "service": {"image": "img"},
            }
        ],
    )


def make_manager(
    db, registry, runner, agents, *, policy="restore", readiness=None, clock=None, sleep=None
):
    return DeploymentManager(
        db,
        registry,
        runner,
        token=TOKEN,
        rollback_policy=policy,
        client_factory=lambda url, token: agents[url],
        readiness_checker=readiness or no_readiness,
        sleep=sleep or asyncio.sleep,
        clock=clock or (lambda: 0.0),
        prepare_poll_interval=0.01,
    )


async def wait_job(db, job_id, status, timeout=2.0):
    deadline = asyncio.get_running_loop().time() + timeout
    while asyncio.get_running_loop().time() < deadline:
        job = db.get_job(job_id)
        if job is not None and job["status"] == status:
            return job
        await asyncio.sleep(0.005)
    raise AssertionError(f"job {job_id} did not reach {status}: {db.get_job(job_id)}")


def agents_for():
    return {NODE_01_URL: FakeAgent("spark-01"), NODE_02_URL: FakeAgent("spark-02")}


# load ---------------------------------------------------------------------


async def test_load_starts_roles_in_start_order(db, registry, runner):
    create_deployment(db)
    agents = agents_for()
    readiness_calls = []

    async def readiness(role, node, project, client, container_id):
        readiness_calls.append((role.name, project, container_id))

    manager = make_manager(db, registry, runner, agents, readiness=readiness)

    job_id = await manager.load("dep-1")
    job = await wait_job(db, job_id, "succeeded")

    assert job["status"] == "succeeded"
    assert [call[0] for call in agents[NODE_02_URL].calls] == ["models", "validate", "up"]
    assert [call[0] for call in agents[NODE_01_URL].calls] == ["models", "validate", "up"]
    assert [call[0] for call in readiness_calls] == ["worker", "head"]
    assert readiness_calls[0][1] == "spark-fleet-demo-glm-worker"
    assert readiness_calls[1][1] == "spark-fleet-demo-glm-head"

    deployment = db.get_deployment("dep-1")
    assert deployment["status"] == "loaded"
    assert deployment["is_primary"] is True
    roles = db.list_roles("dep-1")
    assert [(role["role_name"], role["status"]) for role in roles] == [
        ("worker", "running"),
        ("head", "running"),
    ]
    assert all(role["config_hash"].startswith("hash-") for role in roles)


async def test_load_stops_conflicting_heavy_in_reverse_order(db, registry, runner):
    old_roles = [
        {
            "name": "worker",
            "node": "spark-02",
            "class": "heavy",
            "startOrder": 10,
            "service": {"image": "old:tag"},
        },
        {
            "name": "head",
            "node": "spark-01",
            "class": "heavy",
            "startOrder": 20,
            "inferencePort": 8888,
            "service": {"image": "old:tag"},
        },
        {
            "name": "helper",
            "node": "spark-01",
            "class": "light",
            "startOrder": 30,
            "service": {"image": "helper:tag"},
        },
    ]
    old_recipe = create_deployment(
        db,
        "dep-old",
        "old-deploy",
        recipe_dict(
            name="old-deploy",
            aliases=["old"],
            served_name="OLD",
            roles=old_roles,
        ),
    )
    db.update_deployment("dep-old", status="loaded")
    add_roles(db, "dep-old", old_recipe)
    db.set_primary("dep-old")
    create_deployment(
        db,
        "dep-new",
        "new-deploy",
        recipe_dict(name="new-deploy", aliases=["new"], served_name="NEW"),
    )
    agents = agents_for()
    manager = make_manager(db, registry, runner, agents)

    await wait_job(db, await manager.load("dep-new"), "succeeded")

    agent1_calls = agents[NODE_01_URL].calls
    assert ("down", "spark-fleet-old-deploy-helper") in agent1_calls
    assert ("down", "spark-fleet-old-deploy-head") in agent1_calls
    assert agent1_calls.index(("down", "spark-fleet-old-deploy-helper")) < agent1_calls.index(
        ("down", "spark-fleet-old-deploy-head")
    )
    assert agent1_calls.index(("down", "spark-fleet-old-deploy-head")) < agent1_calls.index(
        ("up", "spark-fleet-new-deploy-head")
    )
    assert db.get_deployment("dep-old")["status"] == "unloaded"
    assert db.get_deployment("dep-new")["status"] == "loaded"
    assert db.get_deployment("dep-new")["is_primary"] is True
    assert db.get_deployment("dep-old")["is_primary"] is False


async def test_load_allows_light_conflicts(db, registry, runner):
    light_recipe = create_deployment(
        db,
        "dep-light",
        "light",
        recipe_dict(
            name="light",
            mode="service",
            model=False,
            roles=[
                {
                    "name": "ui",
                    "node": "spark-01",
                    "class": "light",
                    "service": {"image": "ui:tag"},
                }
            ],
        ),
    )
    db.update_deployment("dep-light", status="loaded")
    add_roles(db, "dep-light", light_recipe)
    create_deployment(db)
    agents = agents_for()
    manager = make_manager(db, registry, runner, agents)

    await wait_job(db, await manager.load("dep-1"), "succeeded")

    assert db.get_deployment("dep-light")["status"] == "loaded"
    assert not any(call[0] == "down" for call in agents[NODE_01_URL].calls)


async def test_load_runs_prepare_jobs_before_roles(db, registry, runner):
    prepare = [
        {
            "name": "stage",
            "node": "spark-02",
            "image": "alpine:latest",
            "command": ["sh", "-c", "echo ready"],
            "volumes": ["/host:/stage"],
            "environment": {"A": "1"},
        }
    ]
    create_deployment(db, recipe=recipe_dict(prepare=prepare))
    agents = agents_for()
    manager = make_manager(db, registry, runner, agents)

    await wait_job(db, await manager.load("dep-1"), "succeeded")

    calls = agents[NODE_02_URL].calls
    assert calls[0][0] == "models"
    assert calls[1][0] == "job"
    assert calls[2] == ("job_status", "prepare-job-1")
    job_call = calls[1][1]
    assert job_call["name"] == "stage"
    assert job_call["image"] == "alpine:latest"
    assert job_call["command"] == ["sh", "-c", "echo ready"]
    assert job_call["environment"] == {"A": "1"}
    assert job_call["labels"]["spark.fleet.deployment"] == "demo-glm"
    assert job_call["labels"]["spark.fleet.role"] == "prepare-stage"
    assert calls[3][0] == "validate"


async def test_load_fails_when_prepare_fails(db, registry, runner):
    prepare = [
        {
            "name": "stage",
            "node": "spark-02",
            "image": "alpine:latest",
            "command": ["true"],
            "timeoutSeconds": 5,
        }
    ]
    create_deployment(db, recipe=recipe_dict(prepare=prepare))
    agents = agents_for()
    agents[NODE_02_URL].prepare_status = {
        "state": "failed",
        "error": "boom",
        "logs": "prepare output",
    }
    manager = make_manager(db, registry, runner, agents)

    job = await wait_job(db, await manager.load("dep-1"), "failed")

    assert "prepare stage failed" in job["error"]
    assert "boom" in job["error"]
    assert db.get_deployment("dep-1")["status"] == "failed"
    assert not any(call[0] == "validate" for call in agents[NODE_02_URL].calls)


async def test_load_prepare_timeout_cancels_job(db, registry, runner):
    prepare = [
        {
            "name": "stage",
            "node": "spark-02",
            "image": "alpine:latest",
            "command": ["sleep", "60"],
            "timeoutSeconds": 1,
        }
    ]
    create_deployment(db, recipe=recipe_dict(prepare=prepare))
    agents = agents_for()
    agents[NODE_02_URL].prepare_status = {"state": "running"}
    now = [0.0]

    def clock():
        return now[0]

    async def sleep(seconds):
        now[0] += seconds

    manager = make_manager(db, registry, runner, agents, clock=clock, sleep=sleep)

    job = await wait_job(db, await manager.load("dep-1"), "failed")

    assert "timed out" in job["error"]
    assert ("job_cancel", "prepare-job-1") in agents[NODE_02_URL].calls
    assert db.get_deployment("dep-1")["status"] == "failed"


async def test_load_failure_stops_started_roles_and_marks_failed(db, registry, runner):
    create_deployment(db)
    agents = agents_for()
    agents[NODE_01_URL].fail_up.add("spark-fleet-demo-glm-head")
    manager = make_manager(db, registry, runner, agents)

    job_id = await manager.load("dep-1")
    job = await wait_job(db, job_id, "failed")

    assert "up failed" in job["error"]
    assert ("down", "spark-fleet-demo-glm-worker") in agents[NODE_02_URL].calls
    deployment = db.get_deployment("dep-1")
    assert deployment["status"] == "failed"
    assert "up failed" in deployment["error"]
    roles = {role["role_name"]: role["status"] for role in db.list_roles("dep-1")}
    assert roles["worker"] == "stopped"
    assert roles["head"] == "stopped"


async def test_load_failure_restores_displaced_deployment(db, registry, runner):
    old_recipe = create_deployment(
        db,
        "dep-old",
        "old-deploy",
        recipe_dict(name="old-deploy", aliases=["old"], served_name="OLD"),
    )
    db.update_deployment("dep-old", status="loaded")
    add_roles(db, "dep-old", old_recipe)
    db.set_primary("dep-old")
    create_deployment(
        db,
        "dep-new",
        "new-deploy",
        recipe_dict(name="new-deploy", aliases=["new"], served_name="NEW"),
    )
    agents = agents_for()
    agents[NODE_01_URL].fail_up.add("spark-fleet-new-deploy-head")
    manager = make_manager(db, registry, runner, agents, policy="restore")

    await wait_job(db, await manager.load("dep-new"), "failed")

    assert db.get_deployment("dep-new")["status"] == "failed"
    assert db.get_deployment("dep-old")["status"] == "loaded"
    assert db.get_deployment("dep-old")["is_primary"] is True
    assert ("up", "spark-fleet-old-deploy-head") in agents[NODE_01_URL].calls


async def test_load_failure_leave_down_does_not_restore(db, registry, runner):
    old_recipe = create_deployment(
        db,
        "dep-old",
        "old-deploy",
        recipe_dict(name="old-deploy", aliases=["old"], served_name="OLD"),
    )
    db.update_deployment("dep-old", status="loaded")
    add_roles(db, "dep-old", old_recipe)
    create_deployment(
        db,
        "dep-new",
        "new-deploy",
        recipe_dict(name="new-deploy", aliases=["new"], served_name="NEW"),
    )
    agents = agents_for()
    agents[NODE_01_URL].fail_up.add("spark-fleet-new-deploy-head")
    manager = make_manager(db, registry, runner, agents, policy="leave-down")

    await wait_job(db, await manager.load("dep-new"), "failed")

    assert db.get_deployment("dep-old")["status"] == "unloaded"
    assert db.get_deployment("dep-new")["status"] == "failed"


async def test_load_rejects_unknown_node(db, registry, runner):
    create_deployment(
        db,
        recipe=recipe_dict(
            roles=[
                {
                    "name": "head",
                    "node": "spark-99",
                    "class": "heavy",
                    "service": {"image": "img"},
                }
            ]
        ),
    )
    manager = make_manager(db, registry, runner, agents_for())

    with pytest.raises(DeploymentError, match="unknown node"):
        await manager.load("dep-1")


async def test_load_rejects_missing_model(db, registry, runner):
    create_deployment(db)
    agents = agents_for()
    agents[NODE_01_URL].models_response = {"roots": [], "models": []}
    manager = make_manager(db, registry, runner, agents)

    job = await wait_job(db, await manager.load("dep-1"), "failed")

    assert "not cached" in job["error"]
    assert db.get_deployment("dep-1")["status"] == "failed"


async def test_load_rejects_long_project_name(db, registry, runner):
    create_deployment(
        db,
        name="a" * 50,
        recipe=recipe_dict(
            roles=[
                {
                    "name": "head",
                    "node": "spark-01",
                    "class": "heavy",
                    "service": {"image": "img"},
                }
            ]
        ),
    )
    manager = make_manager(db, registry, runner, agents_for())

    with pytest.raises(DeploymentError, match="too long"):
        await manager.load("dep-1")


async def test_load_rejects_duplicate_alias(db, registry, runner):
    old_recipe = create_deployment(
        db,
        "dep-old",
        "old-deploy",
        recipe_dict(name="old-deploy", aliases=["demo-glm"], served_name="OLD"),
    )
    db.update_deployment("dep-old", status="loaded")
    add_roles(db, "dep-old", old_recipe)
    create_deployment(
        db,
        "dep-new",
        "new-deploy",
        recipe_dict(name="new-deploy", aliases=["demo-glm"], served_name="NEW"),
    )
    manager = make_manager(db, registry, runner, agents_for())

    with pytest.raises(DeploymentError, match="aliases already in use"):
        await manager.load("dep-new")


async def test_load_rejects_duplicate_served_name(db, registry, runner):
    old_recipe = create_deployment(
        db,
        "dep-old",
        "old-deploy",
        recipe_dict(name="old-deploy", served_name="Demo-Model-B"),
        status="loaded",
    )
    add_roles(db, "dep-old", old_recipe)
    create_deployment(
        db,
        "dep-new",
        "new-deploy",
        recipe_dict(name="new-deploy", aliases=["Demo-Model-B"], served_name="NEW"),
    )
    manager = make_manager(db, registry, runner, agents_for())

    with pytest.raises(DeploymentError, match="aliases already in use"):
        await manager.load("dep-new")


async def test_load_rejects_reserved_served_name(db, registry, runner):
    create_deployment(
        db,
        recipe=recipe_dict(served_name="spark-active"),
    )
    manager = make_manager(db, registry, runner, agents_for())

    with pytest.raises(DeploymentError, match="reserved"):
        await manager.load("dep-1")


async def test_load_rejects_reserved_alias(db, registry, runner):
    create_deployment(db, recipe=recipe_dict(aliases=["spark-active"]))
    manager = make_manager(db, registry, runner, agents_for())

    with pytest.raises(DeploymentError, match="reserved"):
        await manager.load("dep-1")


async def test_load_rejects_already_loaded(db, registry, runner):
    create_deployment(db, status="loaded")
    manager = make_manager(db, registry, runner, agents_for())

    with pytest.raises(DeploymentError, match="already loaded"):
        await manager.load("dep-1")


async def test_load_missing_deployment_raises(db, registry, runner):
    manager = make_manager(db, registry, runner, agents_for())

    with pytest.raises(DeploymentError, match="not found"):
        await manager.load("missing")


async def test_unload_missing_deployment_raises(db, registry, runner):
    manager = make_manager(db, registry, runner, agents_for())

    with pytest.raises(DeploymentError, match="not found"):
        await manager.unload("missing")


async def test_load_submits_global_node_and_deployment_locks(db, registry, runner, monkeypatch):
    create_deployment(
        db,
        recipe=recipe_dict(
            mode="service",
            model=False,
            roles=[
                {
                    "name": "ui",
                    "node": "spark-01",
                    "class": "light",
                    "service": {"image": "ui:tag"},
                }
            ],
            prepare=[
                {
                    "name": "stage",
                    "node": "spark-02",
                    "image": "alpine:latest",
                    "command": ["true"],
                }
            ],
        ),
    )
    calls = []
    original = runner.submit

    async def submit(kind, payload=None, **kwargs):
        calls.append((kind, kwargs.get("locks")))
        return await original(kind, payload, **kwargs)

    monkeypatch.setattr(runner, "submit", submit)
    manager = make_manager(db, registry, runner, agents_for())

    await wait_job(db, await manager.load("dep-1"), "succeeded")

    assert calls == [
        (
            "deployment.load",
            [
                GLOBAL_LOCK,
                "deployment:dep-1",
                "node:spark-01",
                "node:spark-02",
            ],
        )
    ]


async def test_load_with_primary_promotes_new_deployment(db, registry, runner):
    old_recipe = create_deployment(
        db,
        "dep-old",
        "old-deploy",
        single_role_recipe("spark-01", "old-deploy", "OLD"),
        status="loaded",
    )
    add_roles(db, "dep-old", old_recipe)
    db.set_primary("dep-old")
    create_deployment(
        db, "dep-new", "new-deploy", single_role_recipe("spark-02", "new-deploy", "NEW")
    )
    manager = make_manager(db, registry, runner, agents_for())

    await wait_job(db, await manager.load("dep-new", primary=True), "succeeded")

    assert db.get_deployment("dep-new")["is_primary"] is True
    assert db.get_deployment("dep-old")["is_primary"] is False


async def test_load_without_primary_preserves_existing_primary(db, registry, runner):
    old_recipe = create_deployment(
        db,
        "dep-old",
        "old-deploy",
        single_role_recipe("spark-01", "old-deploy", "OLD"),
        status="loaded",
    )
    add_roles(db, "dep-old", old_recipe)
    db.set_primary("dep-old")
    create_deployment(
        db, "dep-new", "new-deploy", single_role_recipe("spark-02", "new-deploy", "NEW")
    )
    manager = make_manager(db, registry, runner, agents_for())

    await wait_job(db, await manager.load("dep-new"), "succeeded")

    assert db.get_deployment("dep-old")["is_primary"] is True
    assert db.get_deployment("dep-new")["is_primary"] is False


async def test_unload_non_primary_preserves_primary(db, registry, runner):
    primary_recipe = create_deployment(
        db,
        "dep-primary",
        "primary",
        single_role_recipe("spark-01", "primary", "PRIMARY"),
        status="loaded",
    )
    add_roles(db, "dep-primary", primary_recipe)
    db.set_primary("dep-primary")
    other_recipe = create_deployment(
        db,
        "dep-other",
        "other",
        single_role_recipe("spark-02", "other", "OTHER"),
        status="loaded",
    )
    add_roles(db, "dep-other", other_recipe)
    manager = make_manager(db, registry, runner, agents_for())

    await wait_job(db, await manager.unload("dep-other"), "succeeded")

    assert db.get_deployment("dep-primary")["is_primary"] is True
    assert db.get_deployment("dep-other")["is_primary"] is False


async def test_cancelled_load_stops_roles_and_clears_primary(db, registry, runner):
    create_deployment(db)
    readiness_started = asyncio.Event()

    async def blocking_readiness(role, node, project, client, container_id):
        readiness_started.set()
        await asyncio.Event().wait()

    agents = agents_for()
    manager = make_manager(db, registry, runner, agents, readiness=blocking_readiness)

    job_id = await manager.load("dep-1")
    await asyncio.wait_for(readiness_started.wait(), timeout=2)
    assert await runner.cancel(job_id) is True

    await wait_job(db, job_id, "cancelled")

    deployment = db.get_deployment("dep-1")
    assert deployment["status"] == "failed"
    assert deployment["error"] == "cancelled"
    assert deployment["is_primary"] is False
    roles = {role["role_name"]: role["status"] for role in db.list_roles("dep-1")}
    assert roles["worker"] == "stopped"
    assert "head" not in roles
    assert ("down", "spark-fleet-demo-glm-worker") in agents[NODE_02_URL].calls


# unload and redeploy ------------------------------------------------------


async def test_unload_stops_roles_in_reverse_order_and_clears_primary(db, registry, runner):
    roles = [
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
        {
            "name": "helper",
            "node": "spark-01",
            "class": "light",
            "startOrder": 30,
            "service": {"image": "helper:tag"},
        },
    ]
    recipe_obj = create_deployment(db, recipe=recipe_dict(roles=roles), status="loaded")
    add_roles(db, "dep-1", recipe_obj)
    db.set_primary("dep-1")
    agents = agents_for()
    manager = make_manager(db, registry, runner, agents)

    await wait_job(db, await manager.unload("dep-1"), "succeeded")

    assert agents[NODE_01_URL].calls == [
        ("down", "spark-fleet-demo-glm-helper"),
        ("down", "spark-fleet-demo-glm-head"),
    ]
    assert agents[NODE_02_URL].calls == [("down", "spark-fleet-demo-glm-worker")]
    deployment = db.get_deployment("dep-1")
    assert deployment["status"] == "unloaded"
    assert deployment["is_primary"] is False


async def test_unload_reports_down_failure(db, registry, runner):
    recipe_obj = create_deployment(db, status="loaded")
    add_roles(db, "dep-1", recipe_obj)
    agents = agents_for()
    agents[NODE_01_URL].fail_down.add("spark-fleet-demo-glm-head")
    manager = make_manager(db, registry, runner, agents)

    job = await wait_job(db, await manager.unload("dep-1"), "failed")

    assert "down failed" in job["error"]
    assert db.get_deployment("dep-1")["status"] == "failed"


async def test_redeploy_stops_then_starts_roles(db, registry, runner):
    recipe_obj = create_deployment(db, status="loaded")
    add_roles(db, "dep-1", recipe_obj)
    db.set_primary("dep-1")
    agents = agents_for()
    manager = make_manager(db, registry, runner, agents)

    await wait_job(db, await manager.redeploy("dep-1"), "succeeded")

    assert [call[0] for call in agents[NODE_01_URL].calls] == [
        "models",
        "down",
        "validate",
        "up",
    ]
    assert db.get_deployment("dep-1")["status"] == "loaded"
    assert db.get_deployment("dep-1")["is_primary"] is True


async def test_redeploy_applies_changed_recipe(db, registry, runner):
    recipe_obj = create_deployment(db, status="loaded")
    add_roles(db, "dep-1", recipe_obj)
    changed = parse_recipe(recipe_dict(image="ghcr.io/org/other:tag"))
    agents = agents_for()
    manager = make_manager(db, registry, runner, agents)

    assert manager.is_stale("dep-1", changed) is True

    await wait_job(db, await manager.redeploy("dep-1", recipe=changed), "succeeded")

    stored = db.get_deployment("dep-1")["recipe"]
    assert stored["spec"]["roles"][0]["service"]["image"] == "ghcr.io/org/other:tag"
    assert manager.is_stale("dep-1", changed) is False
    assert any(
        compose["services"]["workload"]["image"] == "ghcr.io/org/other:tag"
        for _project, compose in agents[NODE_01_URL].composes
    )


async def test_redeploy_fails_when_stop_fails(db, registry, runner):
    recipe_obj = create_deployment(db, status="loaded")
    add_roles(db, "dep-1", recipe_obj)
    agents = agents_for()
    agents[NODE_01_URL].fail_down.add("spark-fleet-demo-glm-head")
    manager = make_manager(db, registry, runner, agents)

    job = await wait_job(db, await manager.redeploy("dep-1"), "failed")

    assert "down failed" in job["error"]
    assert not any(call[0] == "up" for call in agents[NODE_01_URL].calls)


async def test_redeploy_model_failure_preserves_loaded_deployment(db, registry, runner):
    recipe_obj = create_deployment(db, status="loaded")
    add_roles(db, "dep-1", recipe_obj)
    db.set_primary("dep-1")
    agents = agents_for()
    agents[NODE_01_URL].models_response = {"roots": [], "models": []}
    manager = make_manager(db, registry, runner, agents)

    job = await wait_job(db, await manager.redeploy("dep-1"), "failed")

    assert "not cached" in job["error"]
    deployment = db.get_deployment("dep-1")
    assert deployment["status"] == "loaded"
    assert deployment["is_primary"] is True
    roles = {role["role_name"]: role["status"] for role in db.list_roles("dep-1")}
    assert roles == {"worker": "running", "head": "running"}
    assert not any(call[0] == "down" for call in agents[NODE_01_URL].calls)


async def test_redeploy_changed_roles_stops_old_roles(db, registry, runner):
    old_recipe = create_deployment(db, status="loaded")
    add_roles(db, "dep-1", old_recipe)
    changed = parse_recipe(
        recipe_dict(
            roles=[
                {
                    "name": "solo",
                    "node": "spark-01",
                    "class": "heavy",
                    "startOrder": 10,
                    "inferencePort": 8888,
                    "service": {"image": "new:tag"},
                }
            ]
        )
    )
    agents = agents_for()
    manager = make_manager(db, registry, runner, agents)

    await wait_job(db, await manager.redeploy("dep-1", recipe=changed), "succeeded")

    agent1_calls = agents[NODE_01_URL].calls
    agent2_calls = agents[NODE_02_URL].calls
    assert ("down", "spark-fleet-demo-glm-head") in agent1_calls
    assert ("down", "spark-fleet-demo-glm-worker") in agent2_calls
    assert ("up", "spark-fleet-demo-glm-solo") in agent1_calls
    assert db.get_deployment("dep-1")["status"] == "loaded"


async def test_redeploy_early_failure_preserves_old_recipe(db, registry, runner):
    old_recipe = create_deployment(db, status="loaded")
    add_roles(db, "dep-1", old_recipe)
    changed = parse_recipe(
        recipe_dict(
            roles=[
                {
                    "name": "solo",
                    "node": "spark-01",
                    "class": "heavy",
                    "startOrder": 10,
                    "inferencePort": 8888,
                    "service": {"image": "new:tag"},
                }
            ]
        )
    )
    agents = agents_for()
    agents[NODE_01_URL].models_response = {"roots": [], "models": []}
    manager = make_manager(db, registry, runner, agents)

    job = await wait_job(db, await manager.redeploy("dep-1", recipe=changed), "failed")

    assert "not cached" in job["error"]
    stored = db.get_deployment("dep-1")["recipe"]
    assert [role["name"] for role in stored["spec"]["roles"]] == ["worker", "head"]
    assert db.get_deployment("dep-1")["status"] == "loaded"

    await wait_job(db, await manager.unload("dep-1"), "succeeded")
    assert ("down", "spark-fleet-demo-glm-head") in agents[NODE_01_URL].calls
    assert ("down", "spark-fleet-demo-glm-worker") in agents[NODE_02_URL].calls


async def test_cancelled_redeploy_stops_new_roles(db, registry, runner):
    old_recipe = create_deployment(db, status="loaded")
    add_roles(db, "dep-1", old_recipe)
    changed = parse_recipe(
        recipe_dict(
            roles=[
                {
                    "name": "solo",
                    "node": "spark-01",
                    "class": "heavy",
                    "startOrder": 10,
                    "inferencePort": 8888,
                    "service": {"image": "new:tag"},
                }
            ]
        )
    )
    readiness_started = asyncio.Event()

    async def blocking_readiness(role, node, project, client, container_id):
        readiness_started.set()
        await asyncio.Event().wait()

    agents = agents_for()
    manager = make_manager(db, registry, runner, agents, readiness=blocking_readiness)

    job_id = await manager.redeploy("dep-1", recipe=changed)
    await asyncio.wait_for(readiness_started.wait(), timeout=2)
    assert await runner.cancel(job_id) is True
    await wait_job(db, job_id, "cancelled")

    assert ("up", "spark-fleet-demo-glm-solo") in agents[NODE_01_URL].calls
    assert ("down", "spark-fleet-demo-glm-solo") in agents[NODE_01_URL].calls
    deployment = db.get_deployment("dep-1")
    assert deployment["status"] == "failed"
    assert deployment["error"] == "cancelled"


async def test_failed_redeploy_clears_primary(db, registry, runner):
    recipe_obj = create_deployment(db, status="loaded")
    add_roles(db, "dep-1", recipe_obj)
    db.set_primary("dep-1")
    agents = agents_for()
    agents[NODE_01_URL].fail_up.add("spark-fleet-demo-glm-head")
    manager = make_manager(db, registry, runner, agents)

    await wait_job(db, await manager.redeploy("dep-1"), "failed")

    deployment = db.get_deployment("dep-1")
    assert deployment["status"] == "failed"
    assert deployment["is_primary"] is False


# stale detection ----------------------------------------------------------


def test_is_stale_compares_stored_recipe(db, registry, runner):
    recipe = create_deployment(db, status="loaded")
    manager = make_manager(db, registry, runner, agents_for())

    assert manager.is_stale("dep-1", recipe) is False
    changed = parse_recipe(recipe_dict(image="ghcr.io/org/other:tag"))
    assert manager.is_stale("dep-1", changed) is True


def test_is_stale_is_false_for_unloaded_deployment(db, registry, runner):
    create_deployment(db, status="pending")
    manager = make_manager(db, registry, runner, agents_for())
    changed = parse_recipe(recipe_dict(image="ghcr.io/org/other:tag"))

    assert manager.is_stale("dep-1", changed) is False


# readiness ----------------------------------------------------------------


class StatusClient:
    def __init__(self, statuses):
        self.statuses = statuses
        self.calls = []

    async def compose_status(self, project):
        self.calls.append(project)
        return self.statuses

    async def follow_container_logs(self, container_id, *, tail=200):
        self.calls.append((container_id, tail))
        for line in ("booting", "server ready"):
            yield line


def readiness_role(db, **overrides):
    recipe = parse_recipe(recipe_dict())
    role = recipe.spec.roles[0]
    return role.model_copy(update={"readiness": role.readiness.model_copy(update=overrides)})


def node(registry):
    return registry.require("spark-01")


async def test_check_readiness_running(db, registry):
    client = StatusClient([{"state": "running"}])

    await check_readiness(
        readiness_role(db, type="running", timeout_seconds=2, interval_seconds=1),
        node(registry),
        "project",
        client,
        "cid",
    )

    assert client.calls == ["project"]


async def test_check_readiness_container_health(db, registry):
    client = StatusClient([{"state": "running", "status": "Up 2 minutes (healthy)"}])

    await check_readiness(
        readiness_role(db, type="container-health", timeout_seconds=2, interval_seconds=1),
        node(registry),
        "project",
        client,
        "cid",
    )


async def test_check_readiness_container_health_waits_for_healthy(db, registry):
    client = StatusClient([{"state": "running", "status": "Up 2 seconds (health: starting)"}])
    now = [0.0]

    def clock():
        return now[0]

    async def sleep(seconds):
        now[0] += seconds

    with pytest.raises(DeploymentError, match="timed out"):
        await check_readiness(
            readiness_role(db, type="container-health", timeout_seconds=2, interval_seconds=1),
            node(registry),
            "project",
            client,
            "cid",
            clock=clock,
            sleep=sleep,
        )


async def test_check_readiness_http(db, registry):
    urls = []

    async def http_probe(url, timeout):
        urls.append((url, timeout))
        return 200

    await check_readiness(
        readiness_role(
            db, type="http", path="/v1/models", port=8888, timeout_seconds=2, interval_seconds=1
        ),
        node(registry),
        "project",
        StatusClient([]),
        "cid",
        http_probe=http_probe,
    )

    assert urls == [("http://192.0.2.239:8888/v1/models", 5.0)]


async def test_check_readiness_http_uses_inference_port(db, registry):
    urls = []

    async def http_probe(url, timeout):
        urls.append(url)
        return 200

    role = readiness_role(db, type="http", path="/health", timeout_seconds=2, interval_seconds=1)
    role = role.model_copy(update={"inference_port": 8000})

    await check_readiness(
        role, node(registry), "project", StatusClient([]), "cid", http_probe=http_probe
    )

    assert urls == ["http://192.0.2.239:8000/health"]


async def test_check_readiness_tcp(db, registry):
    calls = []

    async def tcp_probe(host, port, timeout):
        calls.append((host, port, timeout))

    await check_readiness(
        readiness_role(db, type="tcp", port=8888, timeout_seconds=2, interval_seconds=1),
        node(registry),
        "project",
        StatusClient([]),
        "cid",
        tcp_probe=tcp_probe,
    )

    assert calls == [("192.0.2.239", 8888, 5.0)]


async def test_check_readiness_log_pattern(db, registry):
    client = StatusClient([])

    await check_readiness(
        readiness_role(
            db,
            type="log-pattern",
            pattern="server ready",
            timeout_seconds=2,
            interval_seconds=1,
        ),
        node(registry),
        "project",
        client,
        "cid",
    )

    assert client.calls == [("cid", 200)]


async def test_check_readiness_times_out(db, registry):
    client = StatusClient([{"state": "exited"}])
    now = [0.0]

    def clock():
        return now[0]

    async def sleep(seconds):
        now[0] += seconds

    with pytest.raises(DeploymentError, match="timed out"):
        await check_readiness(
            readiness_role(db, type="running", timeout_seconds=2, interval_seconds=1),
            node(registry),
            "project",
            client,
            "cid",
            clock=clock,
            sleep=sleep,
        )


async def test_check_readiness_http_requires_port(db, registry):
    role = readiness_role(db, type="http", path="/health", timeout_seconds=2, interval_seconds=1)
    role = role.model_copy(update={"inference_port": None})

    with pytest.raises(DeploymentError, match="requires a port"):
        await check_readiness(role, node(registry), "project", StatusClient([]), "cid")


async def test_check_readiness_log_pattern_requires_container(db, registry):
    with pytest.raises(DeploymentError, match="requires a container"):
        await check_readiness(
            readiness_role(
                db,
                type="log-pattern",
                pattern="ready",
                timeout_seconds=2,
                interval_seconds=1,
            ),
            node(registry),
            "project",
            StatusClient([]),
            None,
        )


async def test_check_readiness_rejects_invalid_pattern(db, registry):
    with pytest.raises(DeploymentError, match="invalid log pattern"):
        await check_readiness(
            readiness_role(
                db,
                type="log-pattern",
                pattern="[",
                timeout_seconds=2,
                interval_seconds=1,
            ),
            node(registry),
            "project",
            StatusClient([]),
            "cid",
        )


async def test_up_failure_downs_partially_created_project(db, registry, runner):
    create_deployment(db)
    agents = agents_for()
    agents[NODE_01_URL].fail_up.add("spark-fleet-demo-glm-head")
    manager = make_manager(db, registry, runner, agents)

    await wait_job(db, await manager.load("dep-1"), "failed")

    assert ("down", "spark-fleet-demo-glm-head") in agents[NODE_01_URL].calls


async def test_prepare_failure_restores_displaced_deployment(db, registry, runner):
    old_recipe = create_deployment(
        db,
        "dep-old",
        "old-deploy",
        recipe_dict(name="old-deploy", aliases=["old"], served_name="OLD"),
    )
    db.update_deployment("dep-old", status="loaded")
    add_roles(db, "dep-old", old_recipe)
    db.set_primary("dep-old")
    create_deployment(
        db,
        "dep-new",
        "new-deploy",
        recipe_dict(
            name="new-deploy",
            aliases=["new"],
            served_name="NEW",
            prepare=[
                {
                    "name": "stage",
                    "node": "spark-02",
                    "image": "alpine:latest",
                    "command": ["false"],
                }
            ],
        ),
    )
    agents = agents_for()
    agents[NODE_02_URL].prepare_status = {"state": "failed", "error": "prepare boom"}
    manager = make_manager(db, registry, runner, agents, policy="restore")

    await wait_job(db, await manager.load("dep-new"), "failed")

    assert db.get_deployment("dep-new")["status"] == "failed"
    assert db.get_deployment("dep-old")["status"] == "loaded"
    assert db.get_deployment("dep-old")["is_primary"] is True
    assert ("up", "spark-fleet-old-deploy-head") in agents[NODE_01_URL].calls


async def test_unload_failure_clears_primary(db, registry, runner):
    recipe_obj = create_deployment(db, status="loaded")
    add_roles(db, "dep-1", recipe_obj)
    db.set_primary("dep-1")
    agents = agents_for()
    agents[NODE_01_URL].fail_down.add("spark-fleet-demo-glm-head")
    manager = make_manager(db, registry, runner, agents)

    await wait_job(db, await manager.unload("dep-1"), "failed")

    deployment = db.get_deployment("dep-1")
    assert deployment["status"] == "failed"
    assert deployment["is_primary"] is False


async def test_failed_primary_does_not_block_auto_promotion(db, registry, runner):
    old_recipe = create_deployment(db, "dep-old", "old-deploy")
    db.update_deployment("dep-old", status="failed")
    add_roles(db, "dep-old", old_recipe)
    db.set_primary("dep-old")
    create_deployment(db, "dep-new", "new-deploy")
    manager = make_manager(db, registry, runner, agents_for())

    await wait_job(db, await manager.load("dep-new"), "succeeded")

    assert db.get_deployment("dep-new")["is_primary"] is True
    assert db.get_deployment("dep-old")["is_primary"] is False


async def test_service_recipe_cannot_be_primary(db, registry, runner):
    create_deployment(
        db,
        recipe=recipe_dict(
            mode="service",
            model=False,
            roles=[
                {
                    "name": "light",
                    "node": "spark-01",
                    "class": "light",
                    "service": {"image": "img"},
                }
            ],
        ),
    )
    manager = make_manager(db, registry, runner, agents_for())

    with pytest.raises(DeploymentError, match="only deployment recipes can be primary"):
        await manager.load("dep-1", primary=True)
