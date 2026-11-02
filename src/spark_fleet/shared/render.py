"""Render recipe roles into per-node Compose documents.

Placeholders use ``{{ ... }}`` (not ``${...}``) so Compose interpolation is
untouched. Rendering resolves them, merges the structured service fields with
the preserved raw Compose keys, injects fleet labels and a deterministic
container name, and returns a stable config hash per role.

The returned ``config_hash`` is the SHA-256 of the rendered document *before*
the ``spark.fleet.config-hash`` label is added, so the hash label can carry its
own value without a circular dependency. The agent's ``/compose/validate`` pass
returns the authoritative hash of the Compose-normalized document; the
controller stores that hash per role and uses it for stale detection.

Node ranks apply to heavy roles only: light helper roles are not part of the
distributed topology and do not receive ``topology.nodeRank``. Ranks default to
declaration order among heavy roles and must be unique and contiguous from 0.
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
    if recipe.spec.mode == "deployment" and not _heavy_roles(recipe):
        raise RenderError("deployment recipes require at least one heavy role")

    ranks = _resolve_ranks(recipe, node_ranks)
    return [
        _render_role(recipe, role, deployment_name, deployment_id, nodes, ranks)
        for role in recipe.spec.roles
    ]


def _heavy_roles(recipe: Recipe) -> list[RoleSpec]:
    return [role for role in recipe.spec.roles if role.class_ == "heavy"]


def _resolve_ranks(recipe: Recipe, node_ranks: Mapping[str, int] | None) -> dict[str, int]:
    heavy = _heavy_roles(recipe)
    if node_ranks is None:
        return {role.name: index for index, role in enumerate(heavy)}

    heavy_names = {role.name for role in heavy}
    unknown = sorted((set(node_ranks) - heavy_names), key=str)
    if unknown:
        names = ", ".join(str(name) for name in unknown)
        raise RenderError(f"node ranks provided for unknown or non-heavy roles: {names}")

    ranks: dict[str, int] = {}
    for role in heavy:
        if role.name not in node_ranks:
            raise RenderError(f"node ranks missing for role {role.name}")
        value = node_ranks[role.name]
        if isinstance(value, bool) or not isinstance(value, int):
            raise RenderError(f"node rank for role {role.name} must be an integer")
        if value < 0:
            raise RenderError(f"node rank for role {role.name} must not be negative")
        ranks[role.name] = value

    if set(ranks.values()) != set(range(len(heavy))):
        raise RenderError(
            f"node ranks must be unique and contiguous from 0, got {sorted(ranks.values())}"
        )
    return ranks


def _base_context(
    recipe: Recipe,
    deployment_name: str,
    deployment_id: str,
    nodes: Mapping[str, NodeAddress],
) -> dict[str, str]:
    heavy = _heavy_roles(recipe)
    context: dict[str, str] = {
        "deployment.name": deployment_name,
        "deployment.id": deployment_id,
        "topology.masterPort": str(recipe.spec.topology.master_port),
        "topology.worldSize": str(len(heavy)),
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

    heavy = _heavy_roles(recipe)
    if heavy:
        rank_zero = next(item for item in heavy if ranks[item.name] == 0)
        master_address = nodes.get(rank_zero.node)
        if master_address is None:
            raise RenderError(f"unknown node {rank_zero.node!r} for rank-zero role")
        context["topology.masterAddr"] = master_address.fabric_ip
    if role.name in ranks:
        context["topology.nodeRank"] = str(ranks[role.name])

    service = role.service.model_dump(exclude_unset=True)
    service = {
        key: value
        for key, value in service.items()
        if not (isinstance(value, (dict, list)) and not value)
    }

    service.pop("labels", None)

    try:
        user_labels = _normalize_labels(role.service.labels)
        service = _resolve_value(service, context)
        user_labels = _resolve_value(user_labels, context)
    except RenderError as exc:
        raise RenderError(f"role {role.name}: {exc}") from exc

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
    try:
        config_hash = _config_hash(compose)
    except RenderError as exc:
        raise RenderError(f"role {role.name}: {exc}") from exc
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
        _check_placeholder_syntax(value)
        return _PLACEHOLDER_RE.sub(
            lambda match: _resolve_placeholder(match.group(1).strip(), context), value
        )
    if isinstance(value, list):
        return [_resolve_value(item, context) for item in value]
    if isinstance(value, dict):
        return {
            (_resolve_value(key, context) if isinstance(key, str) else key): _resolve_value(
                item, context
            )
            for key, item in value.items()
        }
    return value


def _check_placeholder_syntax(value: str) -> None:
    """Reject malformed ``{{ ... }}`` markers without touching literal braces."""

    index = 0
    while True:
        start = value.find("{{", index)
        if start == -1:
            return
        end = value.find("}}", start + 2)
        if end == -1:
            raise RenderError(f"unterminated placeholder: {value!r}")
        nested = value.find("{{", start + 2)
        if nested != -1 and nested < end:
            raise RenderError(f"malformed placeholder: {value!r}")
        if not value[start + 2 : end].strip():
            raise RenderError(f"empty placeholder: {value!r}")
        index = end + 2


def _resolve_placeholder(key: str, context: Mapping[str, str]) -> str:
    if not key:
        raise RenderError("empty placeholder")
    try:
        return context[key]
    except KeyError:
        raise RenderError(f"unknown placeholder: {{{{ {key} }}}}") from None


def _config_hash(compose: Mapping[str, Any]) -> str:
    try:
        payload = json.dumps(compose, sort_keys=True, separators=(",", ":"), allow_nan=False)
    except (TypeError, ValueError) as exc:
        raise RenderError(f"compose document is not JSON-serializable: {exc}") from exc
    return hashlib.sha256(payload.encode()).hexdigest()
