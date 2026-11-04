"""Tests for old-controller import, grouping, recipe generation, and apply."""

import httpx
import pytest
import yaml

from spark_fleet.controller.importer import (
    Importer,
    ImporterError,
    ImportPlan,
    OldControllerClient,
    SkippedWorkload,
)
from spark_fleet.shared.db import Database
from spark_fleet.shared.recipe import parse_recipe
from spark_fleet.shared.render import NodeAddress

NODES = {
    "spark-01": NodeAddress(lan_ip="192.0.2.239", fabric_ip="198.51.100.10"),
    "spark-02": NodeAddress(lan_ip="192.0.2.79", fabric_ip="198.51.100.11"),
}


def instance_yaml(
    *,
    name="train-head",
    workload_class="heavy",
    inference_port=8888,
    models=("org/model",),
    served_model="Demo-Model-B",
    image="vllm:tag",
    master_addr="198.51.100.10",
    master_port="25000",
    node_rank="0",
    command=None,
):
    command = command or [
        "vllm",
        "serve",
        "org/model",
        "--served-model-name",
        served_model,
        "--port",
        str(inference_port),
    ]
    config = {
        "x-template": "vllm",
        "x-class": workload_class,
        "x-inference-port": inference_port,
        "x-models": list(models),
        "services": {
            "workload": {
                "image": image,
                "network_mode": "host",
                "environment": {
                    "MASTER_ADDR": master_addr,
                    "MASTER_PORT": master_port,
                    "NODE_RANK": node_rank,
                },
                "command": command,
            }
        },
    }
    if served_model is not None:
        config["x-served-model"] = served_model
    return yaml.safe_dump(config, sort_keys=False)


def workload(name, workload_class="heavy", inference_port=8888, models=("org/model",)):
    return {
        "name": name,
        "template": "vllm",
        "class": workload_class,
        "inference_port": inference_port,
        "models": list(models),
        "active": False,
    }


class FakeController:
    def __init__(self, workloads, configs):
        self.workloads_data = workloads
        self.configs = configs
        self.requests = []

    async def workloads(self):
        return self.workloads_data

    async def instance(self, name):
        return self.configs[name]

    async def models(self):
        return []

    async def aclose(self):
        pass


def fake_sources(workloads, configs):
    return {"spark-01": ("http://old-01:8091", FakeController(workloads, configs))}


async def test_client_reads_workloads_instance_and_models():
    def handler(request):
        if request.url.path == "/api/workloads":
            return httpx.Response(200, json={"workloads": [workload("train-head")]})
        if request.url.path == "/api/instances/train-head":
            return httpx.Response(200, json={"name": "train-head", "config": "x-class: heavy"})
        if request.url.path == "/api/models":
            return httpx.Response(200, json={"models": [{"repo_id": "org/model"}]})
        return httpx.Response(404)

    async with OldControllerClient(
        "http://old-01:8091", transport=httpx.MockTransport(handler)
    ) as client:
        assert await client.workloads() == [workload("train-head")]
        assert await client.instance("train-head") == "x-class: heavy"
        assert await client.models() == [{"repo_id": "org/model"}]


async def test_client_rejects_invalid_json():
    def handler(request):
        return httpx.Response(200, text="not json")

    async with OldControllerClient(
        "http://old-01:8091", transport=httpx.MockTransport(handler)
    ) as client:
        with pytest.raises(ImporterError, match="invalid JSON"):
            await client.workloads()


async def test_client_reports_http_errors():
    def handler(request):
        return httpx.Response(500, text="boom")

    async with OldControllerClient(
        "http://old-01:8091", transport=httpx.MockTransport(handler)
    ) as client:
        with pytest.raises(ImporterError, match="500"):
            await client.workloads()


async def test_plan_groups_distributed_workloads():
    configs = {
        "train-head": instance_yaml(name="train-head", node_rank="0"),
        "train-worker": instance_yaml(
            name="train-worker",
            node_rank="1",
            master_addr="198.51.100.10",
        ),
    }
    sources = {
        "spark-01": ("http://old-01:8091", FakeController([workload("train-head")], configs)),
        "spark-02": ("http://old-02:8091", FakeController([workload("train-worker")], configs)),
    }

    plan = await Importer(NODES).plan(sources)

    assert len(plan.proposals) == 1
    proposal = plan.proposals[0]
    assert proposal.name == "train"
    roles = proposal.recipe.spec.roles
    assert [(role.name, role.node, role.start_order) for role in roles] == [
        ("head", "spark-01", 20),
        ("worker", "spark-02", 10),
    ]
    assert roles[0].readiness.type == "http"
    assert roles[1].readiness.type == "running"
    assert proposal.recipe.spec.topology.master_port == 25000
    assert proposal.recipe.spec.model.served_name == "Demo-Model-B"
    head_service = roles[0].service.model_dump(exclude_unset=True)
    assert head_service["environment"]["MASTER_ADDR"] == "{{ node.spark-01.fabricIp }}"
    assert "---" in proposal.diff


async def test_plan_single_heavy_becomes_one_role_deployment():
    sources = fake_sources(
        [workload("demo-glm-head")],
        {"demo-glm-head": instance_yaml(name="demo-glm-head")},
    )

    plan = await Importer(NODES).plan(sources)

    proposal = plan.proposals[0]
    assert proposal.recipe.spec.mode == "deployment"
    assert [(role.name, role.node) for role in proposal.recipe.spec.roles] == [("head", "spark-01")]


async def test_plan_light_becomes_service_recipe():
    sources = fake_sources(
        [workload("openwebui", "light", inference_port=None, models=())],
        {
            "openwebui": instance_yaml(
                name="openwebui",
                workload_class="light",
                inference_port=None,
                models=(),
                served_model=None,
                image="openwebui:tag",
                command=["python", "-m", "open_webui"],
            )
        },
    )

    plan = await Importer(NODES).plan(sources)

    proposal = plan.proposals[0]
    assert proposal.recipe.spec.mode == "service"
    assert proposal.recipe.metadata.name == "openwebui"
    assert proposal.recipe.spec.roles[0].class_ == "light"


async def test_plan_skips_log_server():
    sources = fake_sources(
        [workload("demo-log-server", "light", inference_port=None, models=())],
        {
            "demo-log-server": instance_yaml(
                name="demo-log-server",
                workload_class="light",
                inference_port=None,
                models=(),
                served_model=None,
                image="log:tag",
                command=["log"],
            )
        },
    )

    plan = await Importer(NODES).plan(sources)

    assert plan.proposals == ()
    assert plan.skipped == (SkippedWorkload("demo-log-server", "obsolete log helper"),)


async def test_plan_folds_pull_helper_into_prepare():
    sources = fake_sources(
        [workload("demo-glm-head"), workload("demo-glm-pull-helper", "light", None, ())],
        {
            "demo-glm-head": instance_yaml(name="demo-glm-head"),
            "demo-glm-pull-helper": instance_yaml(
                name="demo-glm-pull-helper",
                workload_class="light",
                inference_port=None,
                models=(),
                served_model=None,
                image="pull:tag",
                command=["python", "pull.py", "--model", "org/model"],
            ),
        },
    )

    plan = await Importer(NODES).plan(sources)

    assert len(plan.proposals) == 1
    recipe = plan.proposals[0].recipe
    assert len(recipe.spec.prepare) == 1
    assert recipe.spec.prepare[0].name == "demo-glm-pull-helper"
    assert recipe.spec.prepare[0].command == ["python", "pull.py", "--model", "org/model"]
    assert any(item.name == "demo-glm-pull-helper" for item in plan.skipped)


async def test_plan_groups_by_command_flags():
    command = [
        "torchrun",
        "--nnodes",
        "2",
        "--node-rank",
        "0",
        "--master-addr",
        "198.51.100.10",
        "--master-port",
        "29500",
    ]
    sources = fake_sources(
        [workload("torch-head")],
        {
            "torch-head": instance_yaml(
                name="torch-head",
                command=command,
                master_addr=None,
                master_port=None,
                node_rank=None,
            )
        },
    )

    plan = await Importer(NODES).plan(sources)

    assert plan.proposals[0].recipe.spec.topology.master_port == 29500
    assert plan.proposals[0].recipe.spec.roles[0].name == "head"


async def test_apply_requires_confirmation():
    sources = fake_sources([workload("demo-glm-head")], {"demo-glm-head": instance_yaml()})
    plan = await Importer(NODES).plan(sources)
    db = Database(":memory:")

    with pytest.raises(ImporterError, match="confirmation"):
        plan.apply(db)

    assert db.list_deployments() == []


async def test_apply_creates_pending_deployments(tmp_path):
    sources = fake_sources([workload("demo-glm-head")], {"demo-glm-head": instance_yaml()})
    plan = await Importer(NODES).plan(sources)
    db = Database(tmp_path / "fleet.db")

    created = plan.apply(db, confirm=True)

    assert len(created) == 1
    deployment = db.get_deployment(created[0])
    assert deployment["name"] == "demo-glm"
    assert deployment["status"] == "pending"
    assert deployment["recipe"]["spec"]["mode"] == "deployment"


async def test_apply_rejects_duplicate_name(tmp_path):
    sources = fake_sources([workload("demo-glm-head")], {"demo-glm-head": instance_yaml()})
    plan = await Importer(NODES).plan(sources)
    db = Database(tmp_path / "fleet.db")
    db.create_deployment("existing", "demo-glm", "demo-glm", "deployment", {})

    with pytest.raises(ImporterError, match="already exists"):
        plan.apply(db, confirm=True)


async def test_plan_skips_invalid_recipes():
    sources = fake_sources(
        [workload("bad-head")],
        {"bad-head": instance_yaml(name="bad-head", image="")},
    )

    plan = await Importer(NODES).plan(sources)

    assert plan.proposals == ()
    assert plan.skipped


async def test_apply_writes_recipe_files(tmp_path):
    sources = fake_sources([workload("demo-glm-head")], {"demo-glm-head": instance_yaml()})
    plan = await Importer(NODES).plan(sources)
    db = Database(tmp_path / "fleet.db")
    recipes = tmp_path / "recipes"

    created = plan.apply(db, confirm=True, recipes_dir=recipes)

    assert len(created) == 1
    assert recipes.joinpath("demo-glm.yaml").is_file()
    assert "servedName: Demo-Model-B" in recipes.joinpath("demo-glm.yaml").read_text()


async def test_apply_rejects_duplicate_plan_names(tmp_path):
    sources = fake_sources([workload("demo-glm-head")], {"demo-glm-head": instance_yaml()})
    plan = await Importer(NODES).plan(sources)
    duplicate = ImportPlan((plan.proposals[0], plan.proposals[0]), ())
    db = Database(tmp_path / "fleet.db")

    with pytest.raises(ImporterError, match="duplicate recipe names"):
        duplicate.apply(db, confirm=True)


async def test_plan_preserves_served_model_case_and_dots():
    sources = fake_sources(
        [workload("demo-glm-head")],
        {"demo-glm-head": instance_yaml(served_model="Demo-Model-B")},
    )

    plan = await Importer(NODES).plan(sources)

    assert plan.proposals[0].recipe.spec.model.served_name == "Demo-Model-B"


async def test_plan_uses_running_readiness_when_head_has_no_port():
    sources = fake_sources(
        [workload("train-head", inference_port=None)],
        {"train-head": instance_yaml(name="train-head", inference_port=None)},
    )

    plan = await Importer(NODES).plan(sources)

    assert plan.proposals[0].recipe.spec.roles[0].readiness.type == "running"


async def test_plan_sanitizes_folded_prepare_fields():
    helper_config = yaml.safe_dump(
        {
            "x-class": "light",
            "services": {
                "workload": {
                    "image": "pull:tag",
                    "environment": ["A=1", "B=2"],
                    "volumes": ["/data:/data", {"type": "bind", "source": "/x", "target": "/x"}],
                    "command": ["pull"],
                }
            },
        },
        sort_keys=False,
    )
    sources = fake_sources(
        [workload("demo-glm-head"), workload("demo-glm-pull-helper", "light", None, ())],
        {
            "demo-glm-head": instance_yaml(name="demo-glm-head"),
            "demo-glm-pull-helper": helper_config,
        },
    )

    plan = await Importer(NODES).plan(sources)

    prepare = plan.proposals[0].recipe.spec.prepare[0]
    assert prepare.environment == {"A": "1", "B": "2"}
    assert prepare.volumes == ["/data:/data", "/x:/x"]


async def test_plan_reports_unreadable_instances():
    class FailingController(FakeController):
        async def instance(self, name):
            raise ImporterError("instance unavailable")

    sources = {
        "spark-01": (
            "http://old-01:8091",
            FailingController([workload("demo-glm-head")], {}),
        )
    }

    plan = await Importer(NODES).plan(sources)

    assert plan.proposals == ()
    assert plan.skipped[0].name == "demo-glm-head"
    assert "could not read instance" in plan.skipped[0].reason


async def test_apply_refuses_existing_recipe_file(tmp_path):
    sources = fake_sources([workload("demo-glm-head")], {"demo-glm-head": instance_yaml()})
    plan = await Importer(NODES).plan(sources)
    db = Database(tmp_path / "fleet.db")
    recipes = tmp_path / "recipes"
    recipes.mkdir()
    recipes.joinpath("demo-glm.yaml").write_text("USER RECIPE")

    with pytest.raises(ImporterError, match="already exists"):
        plan.apply(db, confirm=True, recipes_dir=recipes)

    assert recipes.joinpath("demo-glm.yaml").read_text() == "USER RECIPE"
    assert db.list_deployments() == []


async def test_apply_refuses_alias_conflict_with_loaded_deployment(tmp_path):
    sources = fake_sources([workload("demo-glm-head")], {"demo-glm-head": instance_yaml()})
    plan = await Importer(NODES).plan(sources)
    db = Database(tmp_path / "fleet.db")
    other = parse_recipe(
        {
            "apiVersion": "spark-fleet/v1",
            "kind": "Recipe",
            "metadata": {"name": "other"},
            "spec": {
                "mode": "deployment",
                "runtime": "custom",
                "model": {"repo": "org/other", "servedName": "OTHER"},
                "gateway": {"aliases": ["Demo-Model-B"]},
                "roles": [
                    {
                        "name": "head",
                        "node": "spark-01",
                        "class": "heavy",
                        "inferencePort": 8888,
                        "service": {"image": "img"},
                    }
                ],
            },
        }
    )
    db.create_deployment(
        "dep-other",
        "other",
        "other",
        "deployment",
        other.model_dump(by_alias=True, exclude_unset=True),
        status="loaded",
    )

    with pytest.raises(ImporterError, match="aliases already in use"):
        plan.apply(db, confirm=True)

    assert db.list_deployments()[0]["name"] == "other"


async def test_plan_reports_unusable_folded_helper():
    helper_config = yaml.safe_dump(
        {
            "x-class": "light",
            "services": {
                "workload": {
                    "image": "",
                    "command": ["pull"],
                }
            },
        },
        sort_keys=False,
    )
    sources = fake_sources(
        [workload("demo-glm-head"), workload("demo-glm-pull-helper", "light", None, ())],
        {
            "demo-glm-head": instance_yaml(name="demo-glm-head"),
            "demo-glm-pull-helper": helper_config,
        },
    )

    plan = await Importer(NODES).plan(sources)

    assert plan.proposals
    assert any(
        item.name == "demo-glm-pull-helper" and "no image" in item.reason for item in plan.skipped
    )
