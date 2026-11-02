"""Tests for recipe rendering."""

import copy
import hashlib
import json
from datetime import date

import pytest

from spark_fleet.shared.recipe import parse_recipe
from spark_fleet.shared.render import (
    LABEL_CONFIG_HASH,
    LABEL_DEPLOYMENT,
    LABEL_RECIPE,
    NodeAddress,
    RenderError,
    render_deployment,
)

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
                        "ROLE_LAN": "{{ role.worker.node.lanIp }}",
                        "LAN": "{{ node.spark-02.lanIp }}",
                        "MODEL": "{{ model.repo }}",
                        "SERVED": "{{ model.servedName }}",
                        "DEPLOY": "{{ deployment.name }}",
                        "ID": "{{ deployment.id }}",
                    },
                    "command": ["vllm", "serve", "{{ model.repo }}", "--port", "8888"],
                    "volumes": ["{{ deployment.name }}-cache:/cache"],
                    "labels": {
                        "owner": "chris",
                        "tier": "{{ deployment.name }}",
                        "spark.fleet.role": "override",
                    },
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
    assert head["ROLE_LAN"] == "192.0.2.79"
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

    service_workload = render(parse_recipe(SERVICE), name="hermes")[0].compose["services"][
        "workload"
    ]
    assert "environment" not in service_workload


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


def test_empty_deployment_id_rejected():
    with pytest.raises(RenderError, match="deployment id"):
        render(deployment_id="")


def test_resolves_placeholders_in_labels():
    labels = render()[0].compose["services"]["workload"]["labels"]

    assert labels["tier"] == "demo-glm"


def test_resolves_nested_placeholders():
    data = copy.deepcopy(DEPLOYMENT)
    data["spec"]["roles"][0]["service"]["x-nested"] = {
        "a": ["{{ deployment.name }}"],
        "b": {"c": "{{ model.repo }}"},
    }

    workload = render(parse_recipe(data))[0].compose["services"]["workload"]

    assert workload["x-nested"] == {
        "a": ["demo-glm"],
        "b": {"c": "org/model"},
    }


def test_recipe_label_is_distinct_from_deployment_name():
    rendered = render(name="other-deploy")[0]
    labels = rendered.compose["services"]["workload"]["labels"]

    assert labels[LABEL_RECIPE] == "demo-glm"
    assert labels[LABEL_DEPLOYMENT] == "other-deploy"
    assert rendered.container_name == "spark-fleet-other-deploy-head"


def test_explicit_null_extra_key_is_preserved():
    data = copy.deepcopy(DEPLOYMENT)
    data["spec"]["roles"][0]["service"]["x-flag"] = None

    workload = render(parse_recipe(data))[0].compose["services"]["workload"]

    assert "x-flag" in workload
    assert workload["x-flag"] is None


def test_empty_raw_collection_is_omitted():
    data = copy.deepcopy(DEPLOYMENT)
    data["spec"]["roles"][0]["service"]["extra_hosts"] = []

    workload = render(parse_recipe(data))[0].compose["services"]["workload"]

    assert "extra_hosts" not in workload


def test_non_json_serializable_extra_rejected():
    data = copy.deepcopy(DEPLOYMENT)
    data["spec"]["roles"][0]["service"]["x-date"] = date(2026, 1, 1)

    with pytest.raises(RenderError, match="JSON"):
        render(parse_recipe(data))


def test_hash_matches_document_without_hash_label():
    rendered = render()[0]
    workload = copy.deepcopy(rendered.compose["services"]["workload"])
    del workload["labels"][LABEL_CONFIG_HASH]

    payload = json.dumps(
        {"services": {"workload": workload}}, sort_keys=True, separators=(",", ":")
    )

    assert hashlib.sha256(payload.encode()).hexdigest() == rendered.config_hash


def test_light_helper_role_does_not_take_a_rank():
    data = copy.deepcopy(DEPLOYMENT)
    helper = {
        "name": "helper",
        "node": "spark-01",
        "class": "light",
        "startOrder": 5,
        "service": {
            "image": "alpine:latest",
            "network_mode": "host",
            "command": ["sleep", "infinity"],
        },
    }
    data["spec"]["roles"].insert(0, helper)

    rendered = render(parse_recipe(data))

    assert [item.role for item in rendered] == ["helper", "head", "worker"]
    head = rendered[1].compose["services"]["workload"]["environment"]
    assert head["NODE_RANK"] == "0"
    assert head["WORLD_SIZE"] == "2"
    assert head["MASTER"] == "198.51.100.10"
    assert "environment" not in rendered[0].compose["services"]["workload"]


@pytest.mark.parametrize(
    "ranks",
    [
        {"head": 0, "worker": 0},
        {"head": 0, "worker": 2},
        {"head": 0, "worker": -1},
        {"head": 0, "worker": "1"},
        {"head": 0, "worker": 1.5},
        {"head": 0, "worker": True},
        {"head": 0, "worker": None},
        {"head": 0},
    ],
)
def test_invalid_node_ranks_rejected(ranks):
    with pytest.raises(RenderError):
        render(ranks=ranks)


def test_rank_for_non_heavy_role_rejected():
    data = copy.deepcopy(DEPLOYMENT)
    helper = {
        "name": "helper",
        "node": "spark-01",
        "class": "light",
        "service": {"image": "alpine:latest", "network_mode": "host"},
    }
    data["spec"]["roles"].insert(0, helper)

    with pytest.raises(RenderError, match="non-heavy roles: helper"):
        render(parse_recipe(data), ranks={"head": 0, "worker": 1, "helper": True})


def test_deployment_without_heavy_role_rejected():
    data = copy.deepcopy(DEPLOYMENT)
    for role in data["spec"]["roles"]:
        role["class"] = "light"

    with pytest.raises(RenderError, match="heavy role"):
        render(parse_recipe(data))


def test_light_role_cannot_reference_node_rank():
    data = copy.deepcopy(DEPLOYMENT)
    helper = {
        "name": "helper",
        "node": "spark-01",
        "class": "light",
        "service": {
            "image": "alpine:latest",
            "network_mode": "host",
            "environment": {"NODE_RANK": "{{ topology.nodeRank }}"},
        },
    }
    data["spec"]["roles"].insert(0, helper)

    with pytest.raises(RenderError, match="role helper: unknown placeholder"):
        render(parse_recipe(data))


def test_unterminated_placeholder_rejected():
    data = copy.deepcopy(DEPLOYMENT)
    data["spec"]["roles"][0]["service"]["environment"]["BAD"] = "{{ deployment.name"

    with pytest.raises(RenderError, match="unterminated placeholder"):
        render(parse_recipe(data))


def test_brace_only_placeholder_rejected():
    data = copy.deepcopy(DEPLOYMENT)
    data["spec"]["roles"][0]["service"]["environment"]["BAD"] = "{{}}"

    with pytest.raises(RenderError, match="empty placeholder"):
        render(parse_recipe(data))


def test_resolved_value_may_contain_braces():
    data = copy.deepcopy(DEPLOYMENT)
    data["spec"]["roles"][0]["service"]["environment"]["ID"] = "{{ deployment.id }}"

    rendered = render(parse_recipe(data), deployment_id="id}}x")

    assert rendered[0].compose["services"]["workload"]["environment"]["ID"] == "id}}x"


def test_literal_braces_are_preserved():
    data = copy.deepcopy(DEPLOYMENT)
    data["spec"]["roles"][0]["service"]["environment"]["AWK"] = "awk '{print $1}}'"

    workload = render(parse_recipe(data))[0].compose["services"]["workload"]

    assert workload["environment"]["AWK"] == "awk '{print $1}}'"


def test_placeholder_in_environment_key_is_resolved():
    data = copy.deepcopy(DEPLOYMENT)
    data["spec"]["roles"][0]["service"]["environment"]["{{ deployment.name }}"] = "value"

    workload = render(parse_recipe(data))[0].compose["services"]["workload"]

    assert workload["environment"]["demo-glm"] == "value"


def test_rank_key_of_wrong_type_is_reported():
    with pytest.raises(RenderError, match="unknown or non-heavy roles: 3"):
        render(ranks={"head": 0, "worker": 1, 3: 2})


def test_rank_for_unknown_role_is_reported():
    with pytest.raises(RenderError, match="unknown or non-heavy roles: ghost"):
        render(ranks={"head": 0, "worker": 1, "ghost": 2})


def test_nan_extra_rejected():
    data = copy.deepcopy(DEPLOYMENT)
    data["spec"]["roles"][0]["service"]["x-nan"] = float("nan")

    with pytest.raises(RenderError, match="JSON"):
        render(parse_recipe(data))


def test_invalid_label_names_the_role():
    data = copy.deepcopy(DEPLOYMENT)
    data["spec"]["roles"][0]["service"]["labels"] = ["no-separator"]

    with pytest.raises(RenderError, match="role head: invalid label"):
        render(parse_recipe(data))


def test_non_json_error_names_the_role():
    data = copy.deepcopy(DEPLOYMENT)
    data["spec"]["roles"][0]["service"]["x-date"] = date(2026, 1, 1)

    with pytest.raises(RenderError, match="role head: compose document is not JSON"):
        render(parse_recipe(data))


def test_unknown_placeholder_names_the_role():
    data = copy.deepcopy(DEPLOYMENT)
    data["spec"]["roles"][1]["service"]["environment"]["BAD"] = "{{ node.nope.fabricIp }}"

    with pytest.raises(RenderError, match="role worker"):
        render(parse_recipe(data))


def test_empty_placeholder_rejected():
    data = copy.deepcopy(DEPLOYMENT)
    data["spec"]["roles"][0]["service"]["environment"]["BAD"] = "{{ }}"

    with pytest.raises(RenderError, match="empty placeholder"):
        render(parse_recipe(data))


def test_render_error_is_value_error():
    assert issubclass(RenderError, ValueError)
