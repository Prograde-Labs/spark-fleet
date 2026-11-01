"""Recipe schema, parsing, and validation.

A recipe is the declarative manifest for either a multi-node ``deployment`` or a
single-node ``service``. Structural models are strict (unknown keys are
rejected), except for the Compose ``service`` mapping: it keeps unknown Compose
keys so the raw-YAML escape hatch can express options the structured form does
not know about.

Cross-recipe concerns such as alias uniqueness across deployments and node
existence belong to the controller; :func:`validate_against_nodes` covers the
registry check once nodes are known.
"""

from __future__ import annotations

import re
from collections.abc import Iterable, Mapping
from pathlib import Path
from typing import Any, Literal

import yaml
from pydantic import BaseModel, ConfigDict, Field, ValidationError, field_validator, model_validator

API_VERSION = "spark-fleet/v1"
KIND = "Recipe"
NAME_PATTERN = r"^[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?$"
NODE_ID_PATTERN = r"^[A-Za-z0-9][A-Za-z0-9._-]{0,62}$"
ALIAS_PATTERN = r"^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$"
REPO_PATTERN = r"^[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+$"

_NAME_RE = re.compile(NAME_PATTERN)
_NODE_ID_RE = re.compile(NODE_ID_PATTERN)
_ALIAS_RE = re.compile(ALIAS_PATTERN)

Runtime = Literal["vllm", "sglang", "llamacpp", "tensorfold", "comfyui", "custom"]
RoleClass = Literal["heavy", "light"]
ReadinessType = Literal["container-health", "tcp", "http", "log-pattern", "running"]


class RecipeError(ValueError):
    """Raised when a recipe is malformed or fails semantic validation."""

    def __init__(self, errors: Iterable[str]):
        self.errors = list(errors)
        super().__init__("; ".join(self.errors))


class StrictModel(BaseModel):
    model_config = ConfigDict(extra="forbid", populate_by_name=True)


class Metadata(StrictModel):
    name: str = Field(pattern=NAME_PATTERN)
    display_name: str | None = Field(default=None, alias="displayName")
    description: str | None = None


class ModelSpec(StrictModel):
    repo: str = Field(pattern=REPO_PATTERN)
    served_name: str = Field(alias="servedName", pattern=ALIAS_PATTERN)
    revision: str | None = None
    extra_repos: list[str] = Field(default_factory=list, alias="extraRepos")


class GatewaySpec(StrictModel):
    aliases: list[str] = Field(default_factory=list)

    @field_validator("aliases")
    @classmethod
    def _validate_aliases(cls, aliases: list[str]) -> list[str]:
        seen: set[str] = set()
        for alias in aliases:
            if not _ALIAS_RE.fullmatch(alias):
                raise ValueError(f"invalid alias: {alias!r}")
            if alias in seen:
                raise ValueError(f"duplicate alias: {alias}")
            seen.add(alias)
        return aliases


class TopologySpec(StrictModel):
    preset: str = Field(default="custom", min_length=1)
    master_port: int = Field(default=25000, alias="masterPort", ge=1, le=65535)
    tensor_parallel_size: int | None = Field(default=None, alias="tensorParallelSize", ge=1)


class ReadinessSpec(StrictModel):
    type: ReadinessType = "running"
    path: str | None = None
    port: int | None = Field(default=None, ge=1, le=65535)
    pattern: str | None = None
    timeout_seconds: int = Field(default=3600, alias="timeoutSeconds", ge=1)
    interval_seconds: int = Field(default=5, alias="intervalSeconds", ge=1)

    @model_validator(mode="after")
    def _require_probe_fields(self) -> ReadinessSpec:
        if self.type == "http" and not self.path:
            raise ValueError("http readiness requires path")
        if self.type == "tcp" and self.port is None:
            raise ValueError("tcp readiness requires port")
        if self.type == "log-pattern" and not self.pattern:
            raise ValueError("log-pattern readiness requires pattern")
        return self


class ServiceSpec(BaseModel):
    """One Compose service. Unknown Compose keys are preserved (raw YAML escape hatch)."""

    model_config = ConfigDict(extra="allow", populate_by_name=True)

    image: str = Field(min_length=1)
    command: list[str] | str | None = None
    entrypoint: list[str] | str | None = None
    environment: dict[str, str | int | float | bool] = Field(default_factory=dict)
    volumes: list[str] = Field(default_factory=list)
    ports: list[str | int] = Field(default_factory=list)
    network_mode: str | None = None
    gpus: str | None = None
    ipc: str | None = None
    shm_size: str | None = None
    ulimits: dict[str, Any] | None = None
    devices: list[str] = Field(default_factory=list)
    cap_add: list[str] = Field(default_factory=list)
    restart: str | None = None
    healthcheck: dict[str, Any] | None = None
    labels: dict[str, str] = Field(default_factory=dict)
    working_dir: str | None = None


class RoleSpec(StrictModel):
    name: str = Field(pattern=NAME_PATTERN)
    node: str = Field(pattern=NODE_ID_PATTERN)
    class_: RoleClass = Field(alias="class")
    start_order: int = Field(default=0, alias="startOrder", ge=0)
    inference_port: int | None = Field(default=None, alias="inferencePort", ge=1, le=65535)
    readiness: ReadinessSpec = Field(default_factory=ReadinessSpec)
    service: ServiceSpec


class PrepareSpec(StrictModel):
    name: str = Field(pattern=NAME_PATTERN)
    node: str = Field(pattern=NODE_ID_PATTERN)
    image: str = Field(min_length=1)
    command: list[str] = Field(min_length=1)
    volumes: list[str] = Field(default_factory=list)
    environment: dict[str, str | int | float | bool] = Field(default_factory=dict)
    timeout_seconds: int = Field(default=600, alias="timeoutSeconds", ge=1)


class Spec(StrictModel):
    mode: Literal["deployment", "service"]
    runtime: Runtime
    model: ModelSpec | None = None
    gateway: GatewaySpec | None = None
    topology: TopologySpec = Field(default_factory=TopologySpec)
    roles: list[RoleSpec]
    prepare: list[PrepareSpec] = Field(default_factory=list)

    @model_validator(mode="after")
    def _validate_spec(self) -> Spec:
        self._validate_roles()
        self._validate_prepare()
        self._validate_gateway()
        return self

    def _validate_roles(self) -> None:
        if not self.roles:
            raise ValueError("spec.roles must contain at least one role")

        names: set[str] = set()
        heavy_by_node: dict[str, str] = {}
        ports_by_node: dict[tuple[str, int], str] = {}

        for role in self.roles:
            if role.name in names:
                raise ValueError(f"duplicate role name: {role.name}")
            names.add(role.name)

            if role.class_ == "heavy":
                if role.node in heavy_by_node:
                    raise ValueError(
                        f"node {role.node} has more than one heavy role: "
                        f"{heavy_by_node[role.node]} and {role.name}"
                    )
                heavy_by_node[role.node] = role.name

            if role.inference_port is not None:
                key = (role.node, role.inference_port)
                if key in ports_by_node:
                    raise ValueError(
                        f"duplicate inference port {role.inference_port} on node {role.node}: "
                        f"{ports_by_node[key]} and {role.name}"
                    )
                ports_by_node[key] = role.name

        if self.mode == "service":
            if len(self.roles) != 1:
                raise ValueError("service recipes must have exactly one role")
            if self.roles[0].class_ != "light":
                raise ValueError("service recipes require a light role")

    def _validate_prepare(self) -> None:
        names: set[str] = set()
        for prepare in self.prepare:
            if prepare.name in names:
                raise ValueError(f"duplicate prepare name: {prepare.name}")
            names.add(prepare.name)

    def _validate_gateway(self) -> None:
        if self.gateway is None or not self.gateway.aliases:
            return
        if self.mode == "service":
            raise ValueError("service recipes must not declare gateway aliases")
        if self.model is None:
            raise ValueError("gateway aliases require spec.model.servedName")


class Recipe(StrictModel):
    api_version: Literal["spark-fleet/v1"] = Field(alias="apiVersion")
    kind: Literal["Recipe"]
    metadata: Metadata
    spec: Spec


def _format_validation_error(exc: ValidationError) -> list[str]:
    messages: list[str] = []
    for error in exc.errors():
        location = ".".join(str(part) for part in error["loc"])
        message = error["msg"]
        prefix = "Value error, "
        if message.startswith(prefix):
            message = message[len(prefix) :]
        messages.append(f"{location}: {message}" if location else message)
    return messages


def parse_recipe(data: Mapping[str, Any]) -> Recipe:
    """Validate a recipe mapping, raising :class:`RecipeError` with readable messages."""

    if not isinstance(data, Mapping):
        raise RecipeError(["recipe must be a mapping"])
    try:
        return Recipe.model_validate(data)
    except ValidationError as exc:
        raise RecipeError(_format_validation_error(exc)) from exc


def parse_recipe_yaml(text: str) -> Recipe:
    """Parse and validate a recipe from YAML text."""

    try:
        data = yaml.safe_load(text)
    except yaml.YAMLError as exc:
        raise RecipeError([f"invalid YAML: {exc}"]) from exc
    if data is None:
        raise RecipeError(["recipe is empty"])
    return parse_recipe(data)


def load_recipe_file(path: Path) -> Recipe:
    """Read and validate a recipe YAML file."""

    try:
        text = path.read_text()
    except OSError as exc:
        raise RecipeError([f"could not read {path}: {exc}"]) from exc
    return parse_recipe_yaml(text)


def recipe_to_yaml(recipe: Recipe) -> str:
    """Serialize a recipe back to YAML, omitting unset optional fields."""

    data = recipe.model_dump(by_alias=True, exclude_none=True)
    return yaml.safe_dump(data, sort_keys=False)


def validate_against_nodes(recipe: Recipe, known_nodes: Iterable[str]) -> None:
    """Check that every role and prepare job targets a registered node."""

    known = set(known_nodes)
    errors: list[str] = []
    for role in recipe.spec.roles:
        if role.node not in known:
            errors.append(f"role {role.name} references unknown node {role.node!r}")
    for prepare in recipe.spec.prepare:
        if prepare.node not in known:
            errors.append(f"prepare {prepare.name} references unknown node {prepare.node!r}")
    if errors:
        raise RecipeError(errors)
