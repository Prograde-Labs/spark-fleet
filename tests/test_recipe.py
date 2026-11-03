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

ROOT = Path(__file__).resolve().parents[1]

SHIPPED_CONTEXT_LENGTHS = {
    "ds4-flash": 1048576,
    "demo-heavy": 524288,
    "demo-glm": 850000,
    "demo-glm-xl": 960000,
    "demo-qwen": 262144,
    "demo-qwen-dflash2": 262144,
    "demo-qwen-heretic": 262144,
    "demo-qwen-sglang": 262144,
    "demo-qwen-sglang-dflash2": 262144,
    "demo-qwen-sglang-plain": 262144,
    "demo-qwen-sglang-plain-radixark": 262144,
}

SHIPPED_RECIPE_PATHS = sorted((ROOT / "recipes").glob("*.yaml"))

# The llama.cpp recipe leaves contextLength unset so the gateway reads the
# serving limit from the upstream model card.
UPSTREAM_CONTEXT_RECIPES = {"demo-llamacpp"}

# These recipes intentionally carry no model.
MODEL_LESS_RECIPES = {"comfyui", "demo-log-server"}

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


def test_model_context_length_round_trips():
    data = copy.deepcopy(DEPLOYMENT)
    data["spec"]["model"]["contextLength"] = 524288

    recipe = parse_recipe(data)

    assert recipe.spec.model.context_length == 524288
    text = recipe_to_yaml(recipe)
    assert "contextLength: 524288" in text
    assert parse_recipe_yaml(text).spec.model.context_length == 524288


def test_model_context_length_accepts_null():
    data = copy.deepcopy(DEPLOYMENT)
    data["spec"]["model"]["contextLength"] = None

    recipe = parse_recipe(data)

    assert recipe.spec.model.context_length is None


def test_model_context_length_is_omitted_when_unset():
    recipe = parse_recipe(DEPLOYMENT)

    assert recipe.spec.model.context_length is None
    assert "contextLength" not in recipe_to_yaml(recipe)


@pytest.mark.parametrize("value,expected", [("4096", 4096), (4096.0, 4096)])
def test_model_context_length_accepts_integer_forms(value, expected):
    data = copy.deepcopy(DEPLOYMENT)
    data["spec"]["model"]["contextLength"] = value

    recipe = parse_recipe(data)

    assert recipe.spec.model.context_length == expected


@pytest.mark.parametrize("value", [0, -1, 1.5, "abc", True, False])
def test_model_context_length_rejects_invalid_values(value):
    data = copy.deepcopy(DEPLOYMENT)
    data["spec"]["model"]["contextLength"] = value

    with pytest.raises(RecipeError, match=r"spec\.model\.contextLength"):
        parse_recipe(data)


def test_shipped_recipes_are_present():
    assert SHIPPED_RECIPE_PATHS


@pytest.mark.parametrize(
    "path",
    SHIPPED_RECIPE_PATHS,
    ids=lambda path: path.stem,
)
def test_shipped_recipes_parse_and_declare_context(path):
    recipe = load_recipe_file(path)
    if recipe.spec.model is None:
        assert path.stem in MODEL_LESS_RECIPES, (
            f"{path.name} has no model but is not in MODEL_LESS_RECIPES"
        )
        return
    if path.stem in UPSTREAM_CONTEXT_RECIPES:
        assert recipe.spec.model.context_length is None
        assert recipe.spec.runtime == "llamacpp"
        return
    assert path.stem in SHIPPED_CONTEXT_LENGTHS, (
        f"{path.name} declares a model but is not in SHIPPED_CONTEXT_LENGTHS"
    )
    assert recipe.spec.model.context_length == SHIPPED_CONTEXT_LENGTHS[path.stem]


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

    with pytest.raises(RecipeError, match="spark-fleet/v2"):
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

    assert recipe.spec.roles[0].inference_port == 8888
    assert recipe.spec.roles[1].inference_port == 8888


def test_duplicate_aliases_rejected():
    with pytest.raises(RecipeError, match="duplicate alias"):
        parse_recipe(deployment(gateway={"aliases": ["demo-glm", "demo-glm"]}))


def test_alias_may_repeat_served_name():
    recipe = parse_recipe(DEPLOYMENT)

    assert recipe.spec.gateway.aliases == ["demo-glm", "Demo-Model-B"]
    assert recipe.spec.model.served_name == "Demo-Model-B"


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
    data["spec"]["roles"][0]["service"]["stop_grace_period"] = "30s"
    data["spec"]["roles"][0]["service"]["x-custom"] = {"a": 1}

    recipe = parse_recipe(data)
    dumped = recipe.spec.roles[0].service.model_dump()

    assert dumped["stop_grace_period"] == "30s"
    assert dumped["x-custom"] == {"a": 1}


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


def test_parse_recipe_rejects_non_mapping():
    with pytest.raises(RecipeError, match="must be a mapping"):
        parse_recipe(["not", "a", "recipe"])


def test_parse_yaml_rejects_sequence():
    with pytest.raises(RecipeError, match="must be a mapping"):
        parse_recipe_yaml("- one\n- two\n")


def test_rejects_unknown_metadata_key():
    data = copy.deepcopy(DEPLOYMENT)
    data["metadata"]["owner"] = "chris"

    with pytest.raises(RecipeError, match="owner"):
        parse_recipe(data)


def test_rejects_unknown_topology_key():
    data = copy.deepcopy(DEPLOYMENT)
    data["spec"]["topology"]["gpus"] = "all"

    with pytest.raises(RecipeError, match="gpus"):
        parse_recipe(data)


def test_rejects_unknown_readiness_key():
    data = copy.deepcopy(DEPLOYMENT)
    data["spec"]["roles"][0]["readiness"]["command"] = "curl"

    with pytest.raises(RecipeError, match="command"):
        parse_recipe(data)


def test_rejects_unknown_prepare_key():
    data = copy.deepcopy(DEPLOYMENT)
    data["spec"]["prepare"][0]["network"] = "host"

    with pytest.raises(RecipeError, match="network"):
        parse_recipe(data)


def test_invalid_extra_repo_rejected():
    with pytest.raises(RecipeError, match="invalid extra repo"):
        parse_recipe(
            deployment(model={"repo": "org/model", "servedName": "x", "extraRepos": ["not-a-repo"]})
        )


def test_duplicate_extra_repo_rejected():
    with pytest.raises(RecipeError, match="duplicate extra repo"):
        parse_recipe(
            deployment(
                model={
                    "repo": "org/model",
                    "servedName": "x",
                    "extraRepos": ["org/a", "org/a"],
                }
            )
        )


def test_duplicate_service_port_on_same_node_rejected():
    data = copy.deepcopy(DEPLOYMENT)
    for role in data["spec"]["roles"]:
        role["class"] = "light"
        role["node"] = "spark-01"
        del role["inferencePort"]
        role["service"]["ports"] = ["9000:9000"]

    with pytest.raises(RecipeError, match="duplicate port 9000"):
        parse_recipe(data)


def test_service_accepts_compose_list_and_long_forms():
    data = copy.deepcopy(SERVICE)
    compose_service = data["spec"]["roles"][0]["service"]
    compose_service["environment"] = ["FOO=bar", "EMPTY="]
    compose_service["labels"] = ["com.example=one"]
    compose_service["ports"] = [{"target": 8080, "published": "8080", "protocol": "tcp"}]
    compose_service["volumes"] = [{"type": "bind", "source": "/a", "target": "/b"}]
    compose_service["devices"] = [{"host_path": "/dev/a", "container_path": "/dev/a"}]
    compose_service["shm_size"] = 1073741824
    compose_service["gpus"] = ["device=0"]

    recipe = parse_recipe(data)
    parsed = recipe.spec.roles[0].service

    assert parsed.environment == ["FOO=bar", "EMPTY="]
    assert parsed.labels == ["com.example=one"]
    assert parsed.ports == [{"target": 8080, "published": "8080", "protocol": "tcp"}]
    assert parsed.volumes == [{"type": "bind", "source": "/a", "target": "/b"}]
    assert parsed.devices == [{"host_path": "/dev/a", "container_path": "/dev/a"}]
    assert parsed.shm_size == 1073741824
    assert parsed.gpus == ["device=0"]


def test_load_recipe_file_rejects_invalid_utf8(tmp_path):
    path = tmp_path / "recipe.yaml"
    path.write_bytes(b"\xff\xfe\x00")

    with pytest.raises(RecipeError, match="could not read"):
        load_recipe_file(path)


def test_parse_yaml_rejects_duplicate_keys():
    text = (
        "apiVersion: spark-fleet/v1\n"
        "kind: Recipe\n"
        "kind: Recipe\n"
        "metadata:\n  name: dup\n"
        "spec:\n  mode: service\n  runtime: custom\n  roles: []\n"
    )

    with pytest.raises(RecipeError, match="duplicate key"):
        parse_recipe_yaml(text)


def test_validate_against_nodes_reports_all_errors():
    with pytest.raises(RecipeError) as excinfo:
        validate_against_nodes(parse_recipe(DEPLOYMENT), {"spark-02"})

    assert len(excinfo.value.errors) == 2
    assert "role head" in excinfo.value.errors[0]
    assert "prepare stage-overlays" in excinfo.value.errors[1]


def test_yaml_round_trip_preserves_null_extra_key():
    data = copy.deepcopy(SERVICE)
    data["spec"]["roles"][0]["service"]["x-flag"] = None

    recipe = parse_recipe(data)
    text = recipe_to_yaml(recipe)

    assert "x-flag: null" in text
    again = parse_recipe_yaml(text)
    assert again.spec.roles[0].service.model_dump()["x-flag"] is None


def test_inference_port_may_match_own_published_port():
    data = copy.deepcopy(DEPLOYMENT)
    data["spec"]["roles"][0]["service"]["ports"] = ["8888:8888"]

    recipe = parse_recipe(data)

    assert recipe.spec.roles[0].inference_port == 8888


def test_container_only_ports_do_not_collide():
    data = copy.deepcopy(DEPLOYMENT)
    for role in data["spec"]["roles"]:
        role["class"] = "light"
        role["node"] = "spark-01"
        del role["inferencePort"]
        role["service"]["ports"] = [8080, "8080"]

    recipe = parse_recipe(data)

    assert len(recipe.spec.roles) == 2


def test_same_published_port_on_different_nodes_allowed():
    data = copy.deepcopy(DEPLOYMENT)
    for role in data["spec"]["roles"]:
        role["class"] = "light"
        del role["inferencePort"]
        role["service"]["ports"] = ["9000:9000"]

    recipe = parse_recipe(data)

    assert recipe.spec.roles[0].service.ports == ["9000:9000"]
    assert recipe.spec.roles[1].service.ports == ["9000:9000"]
    assert recipe.spec.roles[0].node != recipe.spec.roles[1].node


@pytest.mark.parametrize(
    "port_entry",
    [
        "9000-9002:9000-9002",
        "127.0.0.1:9100:9100",
        "9200:9200/udp",
        {"target": 9300, "published": 9300},
        {"target": 9400, "published": "9400-9401"},
    ],
)
def test_published_port_shapes_are_claimed(port_entry):
    data = copy.deepcopy(DEPLOYMENT)
    for role in data["spec"]["roles"]:
        role["class"] = "light"
        role["node"] = "spark-01"
        del role["inferencePort"]
        role["service"]["ports"] = [port_entry]

    with pytest.raises(RecipeError, match="duplicate port"):
        parse_recipe(data)


@pytest.mark.parametrize(
    "port_entry",
    [
        "9000-8000:9000-8000",
        "0:0",
        "70000:70000",
        "9000-abc:9000-abc",
        "abc",
        "1:2:3:4",
        "9000:abc",
        "9000:",
        "[::1]9000:9000",
        "[::1]:8080",
        "[::1]:9000:9000:9001",
        "[::1]:abc:9000:9000",
        "[not-an-ip]:9000:9000",
        "not-an-ip:9000:9000",
        "9000:9000:9000",
        "9000:8000-9500",
        "9000:9000/",
        70000,
        0,
        -1,
        {"target": 70000, "published": None},
        {"target": "8000-9500"},
        {"published": 9000},
        {"target": 8080, "published": 9000, "host_ip": "300.300.300.300"},
    ],
)
def test_invalid_published_ports_rejected(port_entry):
    data = copy.deepcopy(SERVICE)
    data["spec"]["roles"][0]["service"]["ports"] = [port_entry]

    with pytest.raises(RecipeError, match="port|bind address"):
        parse_recipe(data)


def test_boolean_port_rejected():
    data = copy.deepcopy(SERVICE)
    data["spec"]["roles"][0]["service"]["ports"] = [True]

    with pytest.raises(RecipeError, match="ports"):
        parse_recipe(data)


def test_long_form_port_requires_target():
    data = copy.deepcopy(SERVICE)
    data["spec"]["roles"][0]["service"]["ports"] = [{"published": 9000}]

    with pytest.raises(RecipeError, match="requires target"):
        parse_recipe(data)


def test_yaml_merge_keys_allowed():
    text = """
apiVersion: spark-fleet/v1
kind: Recipe
metadata:
  name: merged
spec:
  mode: service
  runtime: custom
  roles:
    - name: svc
      node: spark-01
      class: light
      service:
        <<: &defaults
          image: alpine
          restart: always
        image: alpine:3.20
"""

    recipe = parse_recipe_yaml(text)

    assert recipe.spec.roles[0].service.image == "alpine:3.20"
    assert recipe.spec.roles[0].service.restart == "always"


@pytest.mark.parametrize("repo", ["../..", "./x", "org/..", ".hidden/x"])
def test_repo_traversal_rejected(repo):
    with pytest.raises(RecipeError, match="repo"):
        parse_recipe(deployment(model={"repo": repo, "servedName": "x"}))


def test_pattern_error_includes_offending_value():
    data = copy.deepcopy(DEPLOYMENT)
    data["metadata"]["name"] = "GLM 5.3"

    with pytest.raises(RecipeError, match=r"GLM 5\.3"):
        parse_recipe(data)


def test_pattern_error_truncates_long_value():
    data = copy.deepcopy(DEPLOYMENT)
    data["metadata"]["name"] = "a" * 100

    with pytest.raises(RecipeError) as excinfo:
        parse_recipe(data)

    assert "..." in str(excinfo.value)


def test_duplicate_keys_inside_merge_anchor_rejected():
    text = """
apiVersion: spark-fleet/v1
kind: Recipe
metadata:
  name: merged-dup
spec:
  mode: service
  runtime: custom
  roles:
    - name: svc
      node: spark-01
      class: light
      service:
        <<: &defaults
          image: alpine
          image: alpine:3.20
"""

    with pytest.raises(RecipeError, match="duplicate key 'image'"):
        parse_recipe_yaml(text)


def test_yaml_sequence_merge_allowed():
    text = """
apiVersion: spark-fleet/v1
kind: Recipe
metadata:
  name: seq-merge
spec:
  mode: service
  runtime: custom
  roles:
    - name: svc
      node: spark-01
      class: light
      service:
        <<: [&a {image: alpine}, &b {restart: always}]
"""

    recipe = parse_recipe_yaml(text)

    assert recipe.spec.roles[0].service.image == "alpine"
    assert recipe.spec.roles[0].service.restart == "always"


def test_ephemeral_host_port_is_ignored():
    data = copy.deepcopy(SERVICE)
    data["spec"]["roles"][0]["service"]["ports"] = ["127.0.0.1::8080"]

    recipe = parse_recipe(data)

    assert recipe.spec.roles[0].service.ports == ["127.0.0.1::8080"]


def test_duplicate_published_ports_within_service_rejected():
    data = copy.deepcopy(SERVICE)
    data["spec"]["roles"][0]["service"]["ports"] = ["9000:9000", "9000:9001"]

    with pytest.raises(RecipeError, match="duplicate published port 9000"):
        parse_recipe(data)


def test_boolean_published_port_rejected():
    data = copy.deepcopy(SERVICE)
    data["spec"]["roles"][0]["service"]["ports"] = [{"target": 80, "published": True}]

    with pytest.raises(RecipeError, match="boolean"):
        parse_recipe(data)


def test_float_published_port_rejected():
    data = copy.deepcopy(SERVICE)
    data["spec"]["roles"][0]["service"]["ports"] = [{"target": 80, "published": 8080.5}]

    with pytest.raises(RecipeError, match="invalid published port"):
        parse_recipe(data)


def test_null_published_port_is_container_only():
    data = copy.deepcopy(SERVICE)
    data["spec"]["roles"][0]["service"]["ports"] = [{"target": 80, "published": None}]

    recipe = parse_recipe(data)

    assert recipe.spec.roles[0].service.ports == [{"target": 80, "published": None}]


def test_tcp_and_udp_same_port_allowed():
    data = copy.deepcopy(SERVICE)
    data["spec"]["roles"][0]["service"]["ports"] = ["53:53/tcp", "53:53/udp"]

    recipe = parse_recipe(data)

    assert recipe.spec.roles[0].service.ports == ["53:53/tcp", "53:53/udp"]


def test_ipv6_published_port_collision_rejected():
    data = copy.deepcopy(DEPLOYMENT)
    for role in data["spec"]["roles"]:
        role["class"] = "light"
        role["node"] = "spark-01"
        del role["inferencePort"]
        role["service"]["ports"] = ["[::1]:9000:9000"]

    with pytest.raises(RecipeError, match="duplicate port 9000"):
        parse_recipe(data)


def test_resolved_duplicate_yaml_keys_rejected():
    text = """
apiVersion: spark-fleet/v1
kind: Recipe
metadata:
  name: bool-keys
spec:
  mode: service
  runtime: custom
  roles:
    - name: svc
      node: spark-01
      class: light
      service:
        image: alpine
        x-a: 1
        true: 2
        yes: 3
"""

    with pytest.raises(RecipeError, match="duplicate key True"):
        parse_recipe_yaml(text)


def test_merge_sequence_anchor_duplicate_rejected():
    text = """
apiVersion: spark-fleet/v1
kind: Recipe
metadata:
  name: seq-dup
spec:
  mode: service
  runtime: custom
  roles:
    - name: svc
      node: spark-01
      class: light
      service:
        <<: [&a {image: alpine, image: alpine:3.20}]
"""

    with pytest.raises(RecipeError, match="duplicate key 'image'"):
        parse_recipe_yaml(text)


def test_cross_shape_published_duplicate_rejected():
    data = copy.deepcopy(SERVICE)
    data["spec"]["roles"][0]["service"]["ports"] = ["9000-9001:9000-9001", "9001:9001"]

    with pytest.raises(RecipeError, match="duplicate published port 9001"):
        parse_recipe(data)


def test_long_form_and_short_form_duplicate_rejected():
    data = copy.deepcopy(SERVICE)
    data["spec"]["roles"][0]["service"]["ports"] = [
        {"target": 9000, "published": "9000"},
        "9000:9000",
    ]

    with pytest.raises(RecipeError, match="duplicate published port 9000"):
        parse_recipe(data)


@pytest.mark.parametrize(
    "port_entry",
    [
        "9000:9000/xyz",
        {"target": 9000, "published": 9000, "protocol": "xyz"},
    ],
)
def test_invalid_port_protocol_rejected(port_entry):
    data = copy.deepcopy(SERVICE)
    data["spec"]["roles"][0]["service"]["ports"] = [port_entry]

    with pytest.raises(RecipeError, match="invalid port protocol"):
        parse_recipe(data)


def test_tcp_and_udp_same_port_across_roles_allowed():
    data = copy.deepcopy(DEPLOYMENT)
    for role in data["spec"]["roles"]:
        role["class"] = "light"
        role["node"] = "spark-01"
        del role["inferencePort"]
    data["spec"]["roles"][0]["service"]["ports"] = ["53:53/tcp"]
    data["spec"]["roles"][1]["service"]["ports"] = ["53:53/udp"]

    recipe = parse_recipe(data)

    assert recipe.spec.roles[0].service.ports == ["53:53/tcp"]
    assert recipe.spec.roles[1].service.ports == ["53:53/udp"]


def test_same_protocol_across_roles_rejected():
    data = copy.deepcopy(DEPLOYMENT)
    for role in data["spec"]["roles"]:
        role["class"] = "light"
        role["node"] = "spark-01"
        del role["inferencePort"]
        role["service"]["ports"] = ["53:53/tcp"]

    with pytest.raises(RecipeError, match="duplicate port 53"):
        parse_recipe(data)


def test_long_form_udp_port_allowed():
    data = copy.deepcopy(DEPLOYMENT)
    for role in data["spec"]["roles"]:
        role["class"] = "light"
        role["node"] = "spark-01"
        del role["inferencePort"]
    data["spec"]["roles"][0]["service"]["ports"] = [
        {"target": 53, "published": 53, "protocol": "udp"}
    ]
    data["spec"]["roles"][1]["service"]["ports"] = ["53:53/tcp"]

    recipe = parse_recipe(data)

    assert recipe.spec.roles[0].service.ports == [
        {"target": 53, "published": 53, "protocol": "udp"}
    ]
    assert recipe.spec.roles[1].service.ports == ["53:53/tcp"]


def test_wide_container_port_range_allowed():
    data = copy.deepcopy(SERVICE)
    data["spec"]["roles"][0]["service"]["ports"] = ["8000-9500"]

    recipe = parse_recipe(data)

    assert recipe.spec.roles[0].service.ports == ["8000-9500"]


def test_host_range_with_single_container_port_allowed():
    data = copy.deepcopy(SERVICE)
    data["spec"]["roles"][0]["service"]["ports"] = ["9000-9005:8080", "8000-8999:9090"]

    recipe = parse_recipe(data)

    assert recipe.spec.roles[0].service.ports == ["9000-9005:8080", "8000-8999:9090"]


def test_matching_host_and_container_ranges_allowed():
    data = copy.deepcopy(SERVICE)
    data["spec"]["roles"][0]["service"]["ports"] = ["9000-9005:8080-8085"]

    recipe = parse_recipe(data)

    assert recipe.spec.roles[0].service.ports == ["9000-9005:8080-8085"]


def test_empty_bind_ip_allowed():
    data = copy.deepcopy(SERVICE)
    data["spec"]["roles"][0]["service"]["ports"] = [":9000:9000"]

    recipe = parse_recipe(data)

    assert recipe.spec.roles[0].service.ports == [":9000:9000"]


def test_unbracketed_ipv6_bind_allowed():
    data = copy.deepcopy(SERVICE)
    data["spec"]["roles"][0]["service"]["ports"] = ["::1:9000:9000"]

    recipe = parse_recipe(data)

    assert recipe.spec.roles[0].service.ports == ["::1:9000:9000"]


def test_same_port_on_different_bind_ips_conflicts():
    data = copy.deepcopy(DEPLOYMENT)
    for role in data["spec"]["roles"]:
        role["class"] = "light"
        role["node"] = "spark-01"
        del role["inferencePort"]
    data["spec"]["roles"][0]["service"]["ports"] = ["127.0.0.1:9000:9000"]
    data["spec"]["roles"][1]["service"]["ports"] = ["192.0.2.79:9000:9000"]

    with pytest.raises(RecipeError, match="duplicate port 9000"):
        parse_recipe(data)


def test_host_range_wider_than_1024_allowed():
    data = copy.deepcopy(SERVICE)
    data["spec"]["roles"][0]["service"]["ports"] = ["8000-9500:8080"]

    recipe = parse_recipe(data)

    assert recipe.spec.roles[0].service.ports == ["8000-9500:8080"]


def test_wide_long_form_published_range_allowed():
    data = copy.deepcopy(SERVICE)
    data["spec"]["roles"][0]["service"]["ports"] = [{"target": 8080, "published": "8000-9500"}]

    recipe = parse_recipe(data)

    assert recipe.spec.roles[0].service.ports == [{"target": 8080, "published": "8000-9500"}]


def test_partially_overlapping_ranges_across_roles_rejected():
    data = copy.deepcopy(DEPLOYMENT)
    for role in data["spec"]["roles"]:
        role["class"] = "light"
        role["node"] = "spark-01"
        del role["inferencePort"]
    data["spec"]["roles"][0]["service"]["ports"] = ["9000-9005:8080"]
    data["spec"]["roles"][1]["service"]["ports"] = ["9004-9010:8081"]

    with pytest.raises(RecipeError, match="duplicate port 9004"):
        parse_recipe(data)


def test_host_range_claim_covers_entire_range():
    data = copy.deepcopy(DEPLOYMENT)
    for role in data["spec"]["roles"]:
        role["class"] = "light"
        role["node"] = "spark-01"
        del role["inferencePort"]
    data["spec"]["roles"][0]["service"]["ports"] = ["9000-9005:8080"]
    data["spec"]["roles"][1]["service"]["ports"] = ["9005:9005"]

    with pytest.raises(RecipeError, match="duplicate port 9005"):
        parse_recipe(data)


def test_wide_host_range_claim_covers_entire_range():
    data = copy.deepcopy(DEPLOYMENT)
    for role in data["spec"]["roles"]:
        role["class"] = "light"
        role["node"] = "spark-01"
        del role["inferencePort"]
    data["spec"]["roles"][0]["service"]["ports"] = ["8000-9500:8080"]
    data["spec"]["roles"][1]["service"]["ports"] = ["9500:9500"]

    with pytest.raises(RecipeError, match="duplicate port 9500"):
        parse_recipe(data)


def test_overlap_where_later_range_starts_before_owner_rejected():
    data = copy.deepcopy(DEPLOYMENT)
    for role in data["spec"]["roles"]:
        role["class"] = "light"
        role["node"] = "spark-01"
        del role["inferencePort"]
    data["spec"]["roles"][0]["service"]["ports"] = ["9000-9005:8080"]
    data["spec"]["roles"][1]["service"]["ports"] = ["8990-9002:8081"]

    with pytest.raises(RecipeError, match="duplicate port 9000"):
        parse_recipe(data)


def test_overlap_ending_at_owner_low_rejected():
    data = copy.deepcopy(DEPLOYMENT)
    for role in data["spec"]["roles"]:
        role["class"] = "light"
        role["node"] = "spark-01"
        del role["inferencePort"]
    data["spec"]["roles"][0]["service"]["ports"] = ["9005-9010:8080"]
    data["spec"]["roles"][1]["service"]["ports"] = ["9000-9005:8081"]

    with pytest.raises(RecipeError, match="duplicate port 9005"):
        parse_recipe(data)


def test_overlap_where_later_range_encloses_owner_rejected():
    data = copy.deepcopy(DEPLOYMENT)
    for role in data["spec"]["roles"]:
        role["class"] = "light"
        role["node"] = "spark-01"
        del role["inferencePort"]
    data["spec"]["roles"][0]["service"]["ports"] = ["9000-9005:8080"]
    data["spec"]["roles"][1]["service"]["ports"] = ["8990-9010:8081"]

    with pytest.raises(RecipeError, match="duplicate port 9000"):
        parse_recipe(data)


def test_long_form_published_range_claims_entire_range():
    data = copy.deepcopy(DEPLOYMENT)
    for role in data["spec"]["roles"]:
        role["class"] = "light"
        role["node"] = "spark-01"
        del role["inferencePort"]
    data["spec"]["roles"][0]["service"]["ports"] = [{"target": 8080, "published": "8000-9500"}]
    data["spec"]["roles"][1]["service"]["ports"] = ["9500:9500"]

    with pytest.raises(RecipeError, match="duplicate port 9500"):
        parse_recipe(data)


def test_long_form_host_ip_allowed():
    data = copy.deepcopy(SERVICE)
    data["spec"]["roles"][0]["service"]["ports"] = [
        {"target": 8080, "published": 8080, "host_ip": "127.0.0.1"}
    ]

    recipe = parse_recipe(data)

    assert recipe.spec.roles[0].service.ports == [
        {"target": 8080, "published": 8080, "host_ip": "127.0.0.1"}
    ]
