"""Render recipe roles into per-node Compose documents.

Placeholders use ``{{ ... }}`` (not ``${...}``) so Compose interpolation is
untouched. Rendering resolves them, merges the structured service fields with
the preserved raw Compose keys, injects fleet labels and a deterministic
container name, and returns a stable config hash per role.
"""

from __future__ import annotations

import hashlib
import json
import re
from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any

from spark_fleet.shared.recipe import NAME_PATTERN, Recipe, RoleSpec

_PLACEHOLDER_RE = re.compile(r"\{\{\s*([^{}]+?)\s*\}\}")
_DEPLOYMENT_NAME_RE = re.compile(NAME_PATTERN)

LABEL_DEPLOYMENT = "spark.fleet.deployment"
LABEL_RECIPE = "spark.fleet.recipe"
LABEL_ROLE = "spark.fleet.role"
LABEL_CONFIG_HASH = "spark.fleet.config-hash"

_EMPTY_COLLECTION_KEYS = ("environment", "volumes", "ports", "devices", "cap_add")


class RenderError(ValueError):
    """Raised when a recipe cannot be rendered."""


@dataclass(frozen=True)
class NodeAddress:
    lan_ip: str
    fabric_ip: str


@dataclass(frozen=True)
class RenderedRole:
    role: str
    node: str
    container_name: str
    config_hash: str
    compose: dict[str, Any]


def render_deployment(
    recipe: Recipe,
    deployment_name: str,
    deployment_id: str,
    nodes: Mapping[str, NodeAddress],
    node_ranks: Mapping[str, int] | None = None,
) -> list[RenderedRole]:
    """Render every role of ``recipe`` into a Compose document for its node."""

    if not _DEPLOYMENT_NAME_RE.fullmatch(deployment_name):
        raise RenderError(f"invalid deployment name: {deployment_name!r}")
    if not deployment_id:
        raise RenderError("deployment id must not be empty")

    ranks = _resolve_ranks(recipe, node_ranks)
    return [
        _render_role(recipe, role, deployment_name, deployment_id, nodes, ranks)
        for role in recipe.spec.roles
    ]


def _resolve_ranks(recipe: Recipe, node_ranks: Mapping[str, int] | None) -> dict[str, int]:
    if node_ranks is None:
        return {role.name: index for index, role in enumerate(recipe.spec.roles)}

    missing = [role.name for role in recipe.spec.roles if role.name not in node_ranks]
    if missing:
        raise RenderError(f"node ranks missing for roles: {', '.join(missing)}")
    return {role.name: int(node_ranks[role.name]) for role in recipe.spec.roles}


def _base_context(
    recipe: Recipe,
    deployment_name: str,
    deployment_id: str,
    nodes: Mapping[str, NodeAddress],
) -> dict[str, str]:
    context: dict[str, str] = {
        "deployment.name": deployment_name,
        "deployment.id": deployment_id,
        "topology.masterPort": str(recipe.spec.topology.master_port),
        "topology.worldSize": str(len(recipe.spec.roles)),
    }

    for node_id, address in nodes.items():
        context[f"node.{node_id}.lanIp"] = address.lan_ip
        context[f"node.{node_id}.fabricIp"] = address.fabric_ip

    for role in recipe.spec.roles:
        address = nodes.get(role.node)
        if address is None:
            raise RenderError(f"unknown node {role.node!r} for role {role.name}")
        context[f"role.{role.name}.node.lanIp"] = address.lan_ip
        context[f"role.{role.name}.node.fabricIp"] = address.fabric_ip

    model = recipe.spec.model
    if model is not None:
        context["model.repo"] = model.repo
        context["model.servedName"] = model.served_name
        if model.revision is not None:
            context["model.revision"] = model.revision

    tensor_parallel_size = recipe.spec.topology.tensor_parallel_size
    if tensor_parallel_size is not None:
        context["topology.tensorParallelSize"] = str(tensor_parallel_size)

    return context


def _render_role(
    recipe: Recipe,
    role: RoleSpec,
    deployment_name: str,
    deployment_id: str,
    nodes: Mapping[str, NodeAddress],
    ranks: Mapping[str, int],
) -> RenderedRole:
    context = _base_context(recipe, deployment_name, deployment_id, nodes)

    rank_zero = next((item for item in recipe.spec.roles if ranks[item.name] == 0), None)
    if rank_zero is None:
        raise RenderError("no role has nodeRank 0")
    master_address = nodes.get(rank_zero.node)
    if master_address is None:
        raise RenderError(f"unknown node {rank_zero.node!r} for rank-zero role")
    context["topology.masterAddr"] = master_address.fabric_ip
    context["topology.nodeRank"] = str(ranks[role.name])

    service = role.service.model_dump(exclude_none=True)
    for key in _EMPTY_COLLECTION_KEYS:
        if key in service and not service[key]:
            del service[key]

    user_labels = _normalize_labels(role.service.labels)
    service.pop("labels", None)
    service = _resolve_value(service, context)
    user_labels = _resolve_value(user_labels, context)

    container_name = f"spark-fleet-{deployment_name}-{role.name}"
    user_labels.update(
        {
            LABEL_DEPLOYMENT: deployment_name,
            LABEL_RECIPE: recipe.metadata.name,
            LABEL_ROLE: role.name,
        }
    )
    service["labels"] = user_labels
    service["container_name"] = container_name

    compose: dict[str, Any] = {"services": {"workload": service}}
    config_hash = _config_hash(compose)
    service["labels"][LABEL_CONFIG_HASH] = config_hash

    return RenderedRole(
        role=role.name,
        node=role.node,
        container_name=container_name,
        config_hash=config_hash,
        compose=compose,
    )


def _normalize_labels(labels: Mapping[str, str] | list[str]) -> dict[str, str]:
    if isinstance(labels, dict):
        return dict(labels)
    normalized: dict[str, str] = {}
    for entry in labels:
        key, separator, value = entry.partition("=")
        if not separator or not key:
            raise RenderError(f"invalid label: {entry!r}")
        normalized[key] = value
    return normalized


def _resolve_value(value: Any, context: Mapping[str, str]) -> Any:
    if isinstance(value, str):
        return _PLACEHOLDER_RE.sub(
            lambda match: _resolve_placeholder(match.group(1).strip(), context), value
        )
    if isinstance(value, list):
        return [_resolve_value(item, context) for item in value]
    if isinstance(value, dict):
        return {key: _resolve_value(item, context) for key, item in value.items()}
    return value


def _resolve_placeholder(key: str, context: Mapping[str, str]) -> str:
    try:
        return context[key]
    except KeyError:
        raise RenderError(f"unknown placeholder: {{{{ {key} }}}}") from None


def _config_hash(compose: Mapping[str, Any]) -> str:
    payload = json.dumps(compose, sort_keys=True, separators=(",", ":"), default=str)
    return hashlib.sha256(payload.encode()).hexdigest()
