"""Validate the shipped example recipes against the schema."""

from pathlib import Path

import pytest

from spark_fleet.shared.recipe import parse_recipe_yaml, validate_against_nodes

EXAMPLES = Path(__file__).resolve().parents[1] / "recipes"
NODES = {"spark-01", "spark-02"}


@pytest.mark.parametrize("path", sorted(EXAMPLES.glob("*.yaml")), ids=lambda p: p.stem)
def test_example_recipe_parses_and_targets_known_nodes(path):
    recipe = parse_recipe_yaml(path.read_text(encoding="utf-8"))

    assert recipe.metadata.name == path.stem
    validate_against_nodes(recipe, NODES)
    assert recipe.spec.roles


def test_example_names_and_aliases_are_unique():
    names = set()
    aliases = set()
    for path in EXAMPLES.glob("*.yaml"):
        recipe = parse_recipe_yaml(path.read_text(encoding="utf-8"))
        assert recipe.metadata.name not in names
        names.add(recipe.metadata.name)
        current = set(recipe.spec.gateway.aliases if recipe.spec.gateway else [])
        if recipe.spec.model is not None:
            current.add(recipe.spec.model.served_name)
        assert not (aliases & current)
        aliases |= current


def test_vllm_example_starts_worker_before_head():
    recipe = parse_recipe_yaml((EXAMPLES / "vllm-multi-node.yaml").read_text(encoding="utf-8"))

    roles = recipe.spec.roles
    assert [role.name for role in roles] == ["worker", "head"]
    assert roles[0].start_order < roles[1].start_order
    assert roles[1].readiness.type == "http"
    assert roles[1].inference_port == 8888


def test_service_examples_have_no_gateway_aliases():
    for name in ("openwebui-service", "comfyui-service"):
        recipe = parse_recipe_yaml((EXAMPLES / f"{name}.yaml").read_text(encoding="utf-8"))
        assert recipe.spec.mode == "service"
        assert recipe.spec.gateway is None
        assert len(recipe.spec.roles) == 1
