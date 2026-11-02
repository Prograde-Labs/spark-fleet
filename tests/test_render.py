"""Tests for recipe rendering."""

import copy

import pytest

from spark_fleet.shared.recipe import parse_recipe
from spark_fleet.shared.render import NodeAddress, RenderError, render_deployment

NODES = {
    "spark-01": NodeAddress(lan_ip="192.0.2.239", fabric_ip="198.51.100.10"),
    "spark-02": NodeAddress(lan_ip="192.0.2.79", fabric_ip="198.51.100.11"),
}

DEPLOYMENT = {
    "apiVersion": "spark-fleet/v1",
    "kind": "Recipe",
    "metadata": {"name": "demo-glm", "displayName": "GLM-5.3-Flash"},
    "spec": {
        "mode": "deployment",
        "runtime": "custom",
        "model": {"repo": "org/model", "servedName": "Demo-Model-B"},
        "gateway": {"aliases": ["demo-glm"]},
        "topology": {"masterPort": 25000, "tensorParallelSize": 2},
        "roles": [
            {
                "name": "head",
                "node": "spark-01",
                "class": "heavy",
                "startOrder": 20,
                "inferencePort": 8888,
                "readiness": {"type": "http", "path": "/v1/models"},
                "service": {
                    "image": "ghcr.io/org/img:tag",
                    "network_mode": "host",
                    "environment": {
                        "MASTER": "{{ topology.masterAddr }}",
                        "MASTER_ADDR": "{{ node.spark-01.fabricIp }}",
                        "MASTER_PORT": "{{ topology.masterPort }}",
                        "NODE_RANK": "{{ topology.nodeRank }}",
                        "WORLD_SIZE": "{{ topology.worldSize }}",
                        "TP": "{{ topology.tensorParallelSize }}",
                        "PEER": "{{ role.worker.node.fabricIp }}",
                        "LAN": "{{ node.spark-02.lanIp }}",
                        "MODEL": "{{ model.repo }}",
                        "SERVED": "{{ model.servedName }}",
                        "DEPLOY": "{{ deployment.name }}",
                        "ID": "{{ deployment.id }}",
                    },
                    "command": ["vllm", "serve", "{{ model.repo }}", "--port", "8888"],
                    "volumes": ["{{ deployment.name }}-cache:/cache"],
                    "labels": {"owner": "chris", "spark.fleet.role": "override"},
                },
            },
            {
                "name": "worker",
                "node": "spark-02",
                "class": "heavy",
                "startOrder": 10,
                "inferencePort": 8888,
                "readiness": {"type": "running"},
                "service": {
                    "image": "ghcr.io/org/img:tag",
                    "network_mode": "host",
                    "environment": {"NODE_RANK": "{{ topology.nodeRank }}"},
                },
            },
        ],
    },
}

SERVICE = {
    "apiVersion": "spark-fleet/v1",
    "kind": "Recipe",
    "metadata": {"name": "hermes"},
    "spec": {
        "mode": "service",
        "runtime": "custom",
        "roles": [
            {
                "name": "gateway",
                "node": "spark-02",
                "class": "light",
                "service": {
                    "image": "ghcr.io/org/hermes:tag",
                    "network_mode": "host",
                    "labels": ["tier=edge", "owner=chris"],
                },
            }
        ],
    },
}


def deployment(**spec_overrides):
    data = copy.deepcopy(DEPLOYMENT)
    data["spec"].update(spec_overrides)
    return parse_recipe(data)


def render(recipe=None, name="demo-glm", deployment_id="dep-001", nodes=None, ranks=None):
    return render_deployment(
        recipe or deployment(),
        name,
        deployment_id,
        NODES if nodes is None else nodes,
        node_ranks=ranks,
    )


def test_renders_roles_in_declaration_order():
    rendered = render()

    assert [item.role for item in rendered] == ["head", "worker"]
    assert [item.node for item in rendered] == ["spark-01", "spark-02"]


def test_resolves_placeholders():
    head = render()[0].compose["services"]["workload"]["environment"]

    assert head["MASTER"] == "198.51.100.10"
    assert head["MASTER_ADDR"] == "198.51.100.10"
    assert head["MASTER_PORT"] == "25000"
    assert head["NODE_RANK"] == "0"
    assert head["WORLD_SIZE"] == "2"
    assert head["TP"] == "2"
    assert head["PEER"] == "198.51.100.11"
    assert head["LAN"] == "192.0.2.79"
    assert head["MODEL"] == "org/model"
    assert head["SERVED"] == "Demo-Model-B"
    assert head["DEPLOY"] == "demo-glm"
    assert head["ID"] == "dep-001"


def test_worker_gets_its_rank():
    worker = render()[1].compose["services"]["workload"]["environment"]

    assert worker["NODE_RANK"] == "1"


def test_ranks_can_be_overridden():
    rendered = render(ranks={"head": 1, "worker": 0})

    assert rendered[0].compose["services"]["workload"]["environment"]["NODE_RANK"] == "1"
    assert rendered[1].compose["services"]["workload"]["environment"]["NODE_RANK"] == "0"


def test_master_addr_follows_rank_zero():
    rendered = render(ranks={"head": 1, "worker": 0})

    head = rendered[0].compose["services"]["workload"]["environment"]
    assert head["MASTER"] == "198.51.100.11"
    assert rendered[1].compose["services"]["workload"]["environment"]["NODE_RANK"] == "0"


def test_resolves_placeholders_in_command_and_volumes():
    workload = render()[0].compose["services"]["workload"]

    assert workload["command"] == ["vllm", "serve", "org/model", "--port", "8888"]
    assert workload["volumes"] == ["demo-glm-cache:/cache"]


def test_injects_labels_and_container_name():
    head = render()[0]
    labels = head.compose["services"]["workload"]["labels"]

    assert head.container_name == "spark-fleet-demo-glm-head"
    assert head.compose["services"]["workload"]["container_name"] == head.container_name
    assert labels["spark.fleet.deployment"] == "demo-glm"
    assert labels["spark.fleet.recipe"] == "demo-glm"
    assert labels["spark.fleet.role"] == "head"
    assert labels["spark.fleet.config-hash"] == head.config_hash
    assert labels["owner"] == "chris"


def test_injected_labels_override_user_labels():
    head = render()[0]
    labels = head.compose["services"]["workload"]["labels"]

    assert labels["spark.fleet.role"] == "head"


def test_config_hash_is_deterministic():
    first = render()
    second = render()

    assert [item.config_hash for item in first] == [item.config_hash for item in second]
    assert len(first[0].config_hash) == 64


def test_config_hash_changes_with_input():
    baseline = render()[0].config_hash

    data = copy.deepcopy(DEPLOYMENT)
    data["spec"]["roles"][0]["service"]["command"].append("--enable-chunked-prefill")

    assert render(parse_recipe(data))[0].config_hash != baseline


def test_hash_ignores_label_insertion_order():
    data = copy.deepcopy(DEPLOYMENT)
    labels = data["spec"]["roles"][0]["service"]["labels"]
    data["spec"]["roles"][0]["service"]["labels"] = dict(reversed(list(labels.items())))

    assert render(parse_recipe(data))[0].config_hash == render()[0].config_hash


def test_compose_contains_single_workload_service():
    for item in render():
        assert list(item.compose["services"]) == ["workload"]


def test_raw_extra_keys_are_preserved():
    data = copy.deepcopy(DEPLOYMENT)
    data["spec"]["roles"][0]["service"]["stop_grace_period"] = "30s"

    workload = render(parse_recipe(data))[0].compose["services"]["workload"]

    assert workload["stop_grace_period"] == "30s"


def test_empty_collections_are_omitted():
    worker = render()[1].compose["services"]["workload"]

    for key in ("ports", "volumes", "devices", "cap_add"):
        assert key not in worker


def test_service_mode_renders_single_role():
    rendered = render(parse_recipe(SERVICE), name="hermes")

    assert len(rendered) == 1
    assert rendered[0].role == "gateway"
    labels = rendered[0].compose["services"]["workload"]["labels"]
    assert labels["tier"] == "edge"
    assert labels["owner"] == "chris"
    assert labels["spark.fleet.role"] == "gateway"


def test_unknown_placeholder_raises():
    data = copy.deepcopy(DEPLOYMENT)
    data["spec"]["roles"][0]["service"]["environment"]["BAD"] = "{{ node.nope.fabricIp }}"

    with pytest.raises(RenderError, match="unknown placeholder"):
        render(parse_recipe(data))


def test_model_placeholder_without_model_raises():
    data = copy.deepcopy(DEPLOYMENT)
    del data["spec"]["model"]
    del data["spec"]["gateway"]

    with pytest.raises(RenderError, match="unknown placeholder"):
        render(parse_recipe(data))


def test_missing_node_raises():
    with pytest.raises(RenderError, match="unknown node"):
        render(nodes={"spark-01": NODES["spark-01"]})


def test_missing_tensor_parallel_size_raises():
    data = copy.deepcopy(DEPLOYMENT)
    del data["spec"]["topology"]["tensorParallelSize"]

    with pytest.raises(RenderError, match="unknown placeholder"):
        render(parse_recipe(data))


def test_model_revision_placeholder():
    data = copy.deepcopy(DEPLOYMENT)
    data["spec"]["model"]["revision"] = "abc123"
    data["spec"]["roles"][0]["service"]["environment"]["REV"] = "{{ model.revision }}"

    workload = render(parse_recipe(data))[0].compose["services"]["workload"]

    assert workload["environment"]["REV"] == "abc123"


def test_missing_model_revision_raises():
    data = copy.deepcopy(DEPLOYMENT)
    data["spec"]["roles"][0]["service"]["environment"]["REV"] = "{{ model.revision }}"

    with pytest.raises(RenderError, match="unknown placeholder"):
        render(parse_recipe(data))


def test_invalid_deployment_name_rejected():
    with pytest.raises(RenderError, match="deployment name"):
        render(name="Bad Name")


def test_render_error_is_value_error():
    assert issubclass(RenderError, ValueError)
