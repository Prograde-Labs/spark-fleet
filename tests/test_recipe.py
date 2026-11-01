"""Tests for recipe schema validation."""

import copy
from pathlib import Path

import pytest

from spark_fleet.shared.recipe import (
    RecipeError,
    load_recipe_file,
    parse_recipe,
    parse_recipe_yaml,
    recipe_to_yaml,
    validate_against_nodes,
)

DEPLOYMENT = {
    "apiVersion": "spark-fleet/v1",
    "kind": "Recipe",
    "metadata": {"name": "demo-glm", "displayName": "GLM-5.3-Flash"},
    "spec": {
        "mode": "deployment",
        "runtime": "custom",
        "model": {
            "repo": "org/model",
            "servedName": "Demo-Model-B",
            "extraRepos": ["org/extra"],
        },
        "gateway": {"aliases": ["demo-glm", "Demo-Model-B"]},
        "topology": {"preset": "custom", "masterPort": 25000},
        "roles": [
            {
                "name": "head",
                "node": "spark-01",
                "class": "heavy",
                "startOrder": 20,
                "inferencePort": 8888,
                "readiness": {"type": "http", "path": "/v1/models", "timeoutSeconds": 3600},
                "service": {
                    "image": "ghcr.io/org/img:tag",
                    "network_mode": "host",
                    "gpus": "all",
                    "ipc": "host",
                    "shm_size": "32gb",
                    "environment": {
                        "MASTER_ADDR": "{{ node.spark-01.fabricIp }}",
                        "MASTER_PORT": "{{ topology.masterPort }}",
                        "NODE_RANK": "0",
                    },
                    "command": ["vllm", "serve", "{{ model.repo }}", "--port", "8888"],
                },
            },
            {
                "name": "worker",
                "node": "spark-02",
                "class": "heavy",
                "startOrder": 10,
                "inferencePort": 8888,
                "readiness": {"type": "running", "timeoutSeconds": 3600},
                "service": {
                    "image": "ghcr.io/org/img:tag",
                    "network_mode": "host",
                    "gpus": "all",
                    "ipc": "host",
                    "shm_size": "32gb",
                    "environment": {
                        "MASTER_ADDR": "{{ node.spark-01.fabricIp }}",
                        "MASTER_PORT": "{{ topology.masterPort }}",
                        "NODE_RANK": "1",
                    },
                    "command": ["vllm", "serve", "{{ model.repo }}", "--port", "8888"],
                },
            },
        ],
        "prepare": [
            {
                "name": "stage-overlays",
                "node": "spark-01",
                "image": "alpine:latest",
                "command": ["/bin/sh", "-lc", "echo hi"],
                "volumes": ["/srv/spark-fleet/demo-glm-staging:/stage"],
            }
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
                    "environment": {"HERMES_HOME": "/data"},
                    "restart": "unless-stopped",
                },
            }
        ],
    },
}


def deployment(**spec_overrides):
    data = copy.deepcopy(DEPLOYMENT)
    data["spec"].update(spec_overrides)
    return data


def service(**spec_overrides):
    data = copy.deepcopy(SERVICE)
    data["spec"].update(spec_overrides)
    return data


def test_parses_valid_deployment():
    recipe = parse_recipe(DEPLOYMENT)

    assert recipe.metadata.name == "demo-glm"
    assert recipe.metadata.display_name == "GLM-5.3-Flash"
    assert recipe.spec.mode == "deployment"
    assert [role.name for role in recipe.spec.roles] == ["head", "worker"]
    assert recipe.spec.roles[0].class_ == "heavy"
    assert recipe.spec.roles[0].readiness.type == "http"
    assert recipe.spec.roles[0].service.image == "ghcr.io/org/img:tag"
    assert recipe.spec.prepare[0].name == "stage-overlays"


def test_parses_valid_service():
    recipe = parse_recipe(SERVICE)

    assert recipe.spec.mode == "service"
    assert len(recipe.spec.roles) == 1
    assert recipe.spec.roles[0].class_ == "light"
    assert recipe.spec.roles[0].service.restart == "unless-stopped"


def test_yaml_round_trip():
    recipe = parse_recipe(DEPLOYMENT)
    text = recipe_to_yaml(recipe)
    again = parse_recipe_yaml(text)

    assert again == recipe


def test_load_recipe_file(tmp_path):
    path = tmp_path / "recipe.yaml"
    path.write_text(recipe_to_yaml(parse_recipe(SERVICE)))

    recipe = load_recipe_file(path)

    assert recipe.metadata.name == "hermes"


def test_rejects_unknown_top_level_key():
    data = copy.deepcopy(DEPLOYMENT)
    data["extra"] = True

    with pytest.raises(RecipeError, match="extra"):
        parse_recipe(data)


def test_rejects_unknown_spec_key():
    with pytest.raises(RecipeError, match="unknown"):
        parse_recipe(deployment(unknown=True))


def test_rejects_unknown_role_key():
    data = copy.deepcopy(DEPLOYMENT)
    data["spec"]["roles"][0]["gpu"] = 1

    with pytest.raises(RecipeError, match="gpu"):
        parse_recipe(data)


def test_rejects_bad_api_version():
    data = copy.deepcopy(DEPLOYMENT)
    data["apiVersion"] = "spark-fleet/v2"

    with pytest.raises(RecipeError, match="apiVersion"):
        parse_recipe(data)


def test_rejects_bad_kind():
    data = copy.deepcopy(DEPLOYMENT)
    data["kind"] = "Deployment"

    with pytest.raises(RecipeError, match="kind"):
        parse_recipe(data)


@pytest.mark.parametrize("name", ["GLM 5.3", "-leading", "trailing-", ""])
def test_rejects_invalid_metadata_name(name):
    data = copy.deepcopy(DEPLOYMENT)
    data["metadata"]["name"] = name

    with pytest.raises(RecipeError, match="name"):
        parse_recipe(data)


def test_deployment_requires_roles():
    with pytest.raises(RecipeError, match="at least one role"):
        parse_recipe(deployment(roles=[]))


def test_service_requires_exactly_one_role():
    data = copy.deepcopy(SERVICE)
    data["spec"]["roles"].append(copy.deepcopy(data["spec"]["roles"][0]))
    data["spec"]["roles"][1]["name"] = "second"

    with pytest.raises(RecipeError, match="exactly one role"):
        parse_recipe(data)


def test_service_role_must_be_light():
    data = copy.deepcopy(SERVICE)
    data["spec"]["roles"][0]["class"] = "heavy"

    with pytest.raises(RecipeError, match="light role"):
        parse_recipe(data)


def test_role_requires_service():
    data = copy.deepcopy(SERVICE)
    del data["spec"]["roles"][0]["service"]

    with pytest.raises(RecipeError, match="service"):
        parse_recipe(data)


def test_duplicate_role_names_rejected():
    data = copy.deepcopy(DEPLOYMENT)
    data["spec"]["roles"][1]["name"] = "head"

    with pytest.raises(RecipeError, match="duplicate role name"):
        parse_recipe(data)


def test_duplicate_prepare_names_rejected():
    data = copy.deepcopy(DEPLOYMENT)
    data["spec"]["prepare"].append(copy.deepcopy(data["spec"]["prepare"][0]))

    with pytest.raises(RecipeError, match="duplicate prepare name"):
        parse_recipe(data)


def test_two_heavy_roles_on_same_node_rejected():
    data = copy.deepcopy(DEPLOYMENT)
    data["spec"]["roles"][1]["node"] = "spark-01"

    with pytest.raises(RecipeError, match="heavy"):
        parse_recipe(data)


def test_duplicate_inference_port_on_same_node_rejected():
    data = copy.deepcopy(DEPLOYMENT)
    for role in data["spec"]["roles"]:
        role["class"] = "light"
        role["node"] = "spark-01"

    with pytest.raises(RecipeError, match="inference port"):
        parse_recipe(data)


def test_same_inference_port_on_different_nodes_allowed():
    recipe = parse_recipe(DEPLOYMENT)

    assert recipe.spec.roles[0].inference_port == recipe.spec.roles[1].inference_port == 8888


def test_duplicate_aliases_rejected():
    with pytest.raises(RecipeError, match="duplicate alias"):
        parse_recipe(deployment(gateway={"aliases": ["demo-glm", "demo-glm"]}))


def test_alias_may_repeat_served_name():
    recipe = parse_recipe(DEPLOYMENT)

    assert recipe.spec.model.served_name in recipe.spec.gateway.aliases


def test_gateway_aliases_require_model():
    data = deployment(gateway={"aliases": ["demo-glm"]})
    del data["spec"]["model"]

    with pytest.raises(RecipeError, match="aliases require"):
        parse_recipe(data)


def test_service_mode_rejects_gateway():
    data = copy.deepcopy(SERVICE)
    data["spec"]["gateway"] = {"aliases": ["hermes"]}

    with pytest.raises(RecipeError, match="service recipes"):
        parse_recipe(data)


def test_invalid_runtime_rejected():
    with pytest.raises(RecipeError, match="runtime"):
        parse_recipe(deployment(runtime="tgi"))


def test_invalid_role_class_rejected():
    data = copy.deepcopy(DEPLOYMENT)
    data["spec"]["roles"][0]["class"] = "medium"

    with pytest.raises(RecipeError, match="class"):
        parse_recipe(data)


def test_negative_start_order_rejected():
    data = copy.deepcopy(DEPLOYMENT)
    data["spec"]["roles"][0]["startOrder"] = -1

    with pytest.raises(RecipeError, match="startOrder"):
        parse_recipe(data)


def test_nonpositive_readiness_timeout_rejected():
    data = copy.deepcopy(DEPLOYMENT)
    data["spec"]["roles"][0]["readiness"]["timeoutSeconds"] = 0

    with pytest.raises(RecipeError, match="timeoutSeconds"):
        parse_recipe(data)


def test_http_readiness_requires_path():
    data = copy.deepcopy(DEPLOYMENT)
    del data["spec"]["roles"][0]["readiness"]["path"]

    with pytest.raises(RecipeError, match="http readiness requires path"):
        parse_recipe(data)


def test_tcp_readiness_requires_port():
    data = copy.deepcopy(DEPLOYMENT)
    data["spec"]["roles"][0]["readiness"] = {"type": "tcp"}

    with pytest.raises(RecipeError, match="tcp readiness requires port"):
        parse_recipe(data)


def test_log_pattern_readiness_requires_pattern():
    data = copy.deepcopy(DEPLOYMENT)
    data["spec"]["roles"][0]["readiness"] = {"type": "log-pattern"}

    with pytest.raises(RecipeError, match="log-pattern readiness requires pattern"):
        parse_recipe(data)


def test_invalid_readiness_type_rejected():
    data = copy.deepcopy(DEPLOYMENT)
    data["spec"]["roles"][0]["readiness"] = {"type": "vibes"}

    with pytest.raises(RecipeError, match="readiness"):
        parse_recipe(data)


@pytest.mark.parametrize("node", ["spark/01", "spark 01", "", "a" * 64])
def test_invalid_node_id_rejected(node):
    data = copy.deepcopy(DEPLOYMENT)
    data["spec"]["roles"][0]["node"] = node

    with pytest.raises(RecipeError, match="node"):
        parse_recipe(data)


def test_validate_against_nodes_accepts_known_nodes():
    validate_against_nodes(parse_recipe(DEPLOYMENT), {"spark-01", "spark-02"})


def test_validate_against_nodes_rejects_unknown_role_node():
    with pytest.raises(RecipeError, match="role worker"):
        validate_against_nodes(parse_recipe(DEPLOYMENT), {"spark-01"})


def test_validate_against_nodes_rejects_unknown_prepare_node():
    data = copy.deepcopy(DEPLOYMENT)
    data["spec"]["prepare"][0]["node"] = "spark-03"

    with pytest.raises(RecipeError, match="prepare stage-overlays"):
        validate_against_nodes(parse_recipe(data), {"spark-01", "spark-02"})


def test_model_requires_repo_and_served_name():
    with pytest.raises(RecipeError, match="repo"):
        parse_recipe(deployment(model={"servedName": "x"}))


def test_compose_escape_hatch_preserves_unknown_keys():
    data = copy.deepcopy(SERVICE)
    data["spec"]["roles"][0]["service"]["ulimits"] = {"memlock": {"soft": -1, "hard": -1}}
    data["spec"]["roles"][0]["service"]["stop_grace_period"] = "30s"

    recipe = parse_recipe(data)
    dumped = recipe.spec.roles[0].service.model_dump()

    assert dumped["ulimits"] == {"memlock": {"soft": -1, "hard": -1}}
    assert dumped["stop_grace_period"] == "30s"


def test_recipe_error_is_value_error():
    assert issubclass(RecipeError, ValueError)


def test_parse_yaml_rejects_empty_document():
    with pytest.raises(RecipeError, match="empty"):
        parse_recipe_yaml("")


def test_parse_yaml_rejects_invalid_yaml():
    with pytest.raises(RecipeError, match="invalid YAML"):
        parse_recipe_yaml("key: [unclosed")


def test_load_recipe_file_missing(tmp_path):
    with pytest.raises(RecipeError, match="could not read"):
        load_recipe_file(Path(tmp_path / "missing.yaml"))
