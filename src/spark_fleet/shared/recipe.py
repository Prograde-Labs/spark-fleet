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

import ipaddress
import re
from collections.abc import Iterable, Mapping
from pathlib import Path
from typing import Any, Final, Literal

import yaml
from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    StrictInt,
    ValidationError,
    field_validator,
    model_validator,
)

API_VERSION: Final = "spark-fleet/v1"
KIND: Final = "Recipe"
NAME_PATTERN = r"^[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?$"
NODE_ID_PATTERN = r"^[A-Za-z0-9][A-Za-z0-9._-]{0,62}$"
ALIAS_PATTERN = r"^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$"
REPO_SEGMENT_PATTERN = r"[A-Za-z0-9](?:[A-Za-z0-9._-]*[A-Za-z0-9])?"
REPO_PATTERN = rf"^{REPO_SEGMENT_PATTERN}/{REPO_SEGMENT_PATTERN}$"

_ALIAS_RE = re.compile(ALIAS_PATTERN)
_REPO_RE = re.compile(REPO_PATTERN)

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
    context_length: StrictInt | None = Field(
        default=None, alias="contextLength", ge=1
    )

    @field_validator("extra_repos")
    @classmethod
    def _validate_extra_repos(cls, repos: list[str]) -> list[str]:
        seen: set[str] = set()
        for repo in repos:
            if not _REPO_RE.fullmatch(repo):
                raise ValueError(f"invalid extra repo: {repo!r}")
            if repo in seen:
                raise ValueError(f"duplicate extra repo: {repo}")
            seen.add(repo)
        return repos


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
    environment: dict[str, str | int | float | bool | None] | list[str] = Field(
        default_factory=dict
    )
    volumes: list[str | dict[str, Any]] = Field(default_factory=list)
    ports: list[str | StrictInt | dict[str, Any]] = Field(default_factory=list)
    network_mode: str | None = None
    gpus: str | list[Any] | None = None
    ipc: str | None = None
    shm_size: str | int | None = None
    ulimits: dict[str, Any] | None = None
    devices: list[str | dict[str, Any]] = Field(default_factory=list)
    cap_add: list[str] = Field(default_factory=list)
    restart: str | None = None
    healthcheck: dict[str, Any] | None = None
    labels: dict[str, str] | list[str] = Field(default_factory=dict)
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
        ports_by_node: dict[tuple[str, str], list[tuple[str, int, int]]] = {}

        def claim_port(
            node: str,
            low: int,
            high: int,
            role_name: str,
            label: str,
            protocol: str = "tcp",
        ) -> None:
            claims = ports_by_node.setdefault((node, protocol), [])
            for owner, owner_low, owner_high in claims:
                if owner == role_name:
                    continue
                if high < owner_low or low > owner_high:
                    continue
                overlap = max(low, owner_low)
                shown = f"{overlap}/{protocol}" if protocol != "tcp" else str(overlap)
                raise ValueError(
                    f"duplicate {label} {shown} on node {node}: {owner} and {role_name}"
                )
            claims.append((role_name, low, high))

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
                claim_port(
                    role.node,
                    role.inference_port,
                    role.inference_port,
                    role.name,
                    "inference port",
                )
            for low, high, protocol in _published_ports(role.service):
                claim_port(role.node, low, high, role.name, "port", protocol)

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
    api_version: Literal[API_VERSION] = Field(alias="apiVersion")
    kind: Literal[KIND]
    metadata: Metadata
    spec: Spec


def _check_port(port: int, label: str) -> int:
    if not 1 <= port <= 65535:
        raise ValueError(f"{label} port out of range: {port}")
    return port


def _parse_port_range(spec: str, label: str) -> tuple[int, int]:
    if "-" in spec:
        start, _, end = spec.partition("-")
        if not (start.isascii() and start.isdigit() and end.isascii() and end.isdigit()):
            raise ValueError(f"invalid {label} port range: {spec!r}")
        low, high = int(start), int(end)
        if low > high:
            raise ValueError(f"invalid {label} port range: {spec!r}")
        _check_port(low, label)
        _check_port(high, label)
        return low, high
    if not (spec.isascii() and spec.isdigit()):
        raise ValueError(f"invalid {label} port: {spec!r}")
    port = _check_port(int(spec), label)
    return port, port


def _validate_container_spec(spec: str) -> None:
    """Validate a container-side port or range without expanding it."""

    _parse_port_range(spec, "container")


def _validate_bind_ip(value: str) -> None:
    try:
        ipaddress.ip_address(value)
    except ValueError as exc:
        raise ValueError(f"invalid bind address: {value!r}") from exc


def _published_ports(service: ServiceSpec) -> list[tuple[int, int, str]]:
    """Extract published host port ranges (and protocols) from Compose syntax.

    Container-only entries (bare integers and ``"8080"``) are skipped because
    Compose assigns them an ephemeral host port; they cannot collide.

    Bind addresses are validated but deliberately excluded from the conflict
    key: the same host port on a node is treated as a conflict even when bound
    to different interfaces. That conservative rule is safer for a fleet where
    a node is expected to expose a single published endpoint.
    """

    ranges: list[tuple[int, int, str]] = []

    def add(low: int, high: int, protocol: str) -> None:
        for seen_low, seen_high, seen_protocol in ranges:
            if seen_protocol == protocol and not (high < seen_low or low > seen_high):
                overlap = max(low, seen_low)
                raise ValueError(f"duplicate published port {overlap}/{protocol} in service")
        ranges.append((low, high, protocol))

    for entry in service.ports:
        if isinstance(entry, dict):
            target = entry.get("target")
            if target is None:
                raise ValueError("long-form port requires target")
            if isinstance(target, bool):
                raise ValueError("container port must not be a boolean")
            if isinstance(target, int):
                _check_port(target, "container")
            elif isinstance(target, str):
                if not (target.isascii() and target.isdigit()):
                    raise ValueError(f"invalid container port: {target!r}")
                _check_port(int(target), "container")
            else:
                raise ValueError(f"invalid container port: {target!r}")

            published = entry.get("published")
            host_ip = entry.get("host_ip")
            if host_ip is not None:
                if not isinstance(host_ip, str):
                    raise ValueError(f"invalid bind address: {host_ip!r}")
                if host_ip:
                    _validate_bind_ip(host_ip)
            protocol = entry.get("protocol", "tcp")
            if not isinstance(protocol, str):
                raise ValueError(f"invalid port protocol: {protocol!r}")
            protocol = protocol.lower()
            if protocol not in _PORT_PROTOCOLS:
                raise ValueError(f"invalid port protocol: {protocol!r}")
            if isinstance(published, bool):
                raise ValueError("published port must not be a boolean")
            if isinstance(published, int):
                port = _check_port(published, "published")
                add(port, port, protocol)
            elif isinstance(published, str):
                low, high = _parse_port_range(published, "published")
                add(low, high, protocol)
            elif published is not None:
                raise ValueError(f"invalid published port: {published!r}")
            continue
        if isinstance(entry, int):
            _check_port(entry, "container")
            continue
        if not isinstance(entry, str):
            raise ValueError(f"invalid container port: {entry!r}")

        port_spec, slash, protocol = entry.partition("/")
        if slash and not protocol:
            raise ValueError(f"invalid port protocol: {protocol!r}")
        protocol = protocol.lower() or "tcp"
        if protocol not in _PORT_PROTOCOLS:
            raise ValueError(f"invalid port protocol: {protocol!r}")
        bracketed = port_spec.startswith("[")
        if bracketed:
            closing = port_spec.find("]:")
            if closing == -1:
                raise ValueError(f"invalid published port: {entry!r}")
            _validate_bind_ip(port_spec[1:closing])
            port_spec = port_spec[closing + 2 :]
        parts = port_spec.split(":")
        if bracketed and len(parts) > 2:
            raise ValueError(f"invalid published port: {entry!r}")
        if len(parts) == 1:
            if bracketed:
                raise ValueError(f"invalid published port: {entry!r}")
            _validate_container_spec(parts[0])
            continue
        if len(parts) == 2:
            host, container = parts
        else:
            bind_ip = ":".join(parts[:-2])
            host, container = parts[-2], parts[-1]
            if bind_ip:
                _validate_bind_ip(bind_ip)
        if not container:
            raise ValueError(f"invalid published port: {entry!r}")
        container_low, container_high = _parse_port_range(container, "container")
        if not host:
            # Ephemeral host port (e.g. "127.0.0.1::8080").
            continue
        host_low, host_high = _parse_port_range(host, "published")
        if (
            container_low != container_high
            and host_high - host_low != container_high - container_low
        ):
            raise ValueError(f"host and container port ranges must match: {entry!r}")
        add(host_low, host_high, protocol)
    return ranges


def _format_validation_error(exc: ValidationError) -> list[str]:
    messages: list[str] = []
    for error in exc.errors():
        location = ".".join(str(part) for part in error["loc"])
        message = error["msg"]
        prefix = "Value error, "
        if message.startswith(prefix):
            message = message[len(prefix) :]
        if error["type"] in {"literal_error", "string_pattern_mismatch"} and "input" in error:
            given = repr(error["input"])
            if len(given) > 80:
                given = given[:77] + "..."
            message = f"{message} (got {given})"
        messages.append(f"{location}: {message}" if location else message)
    return messages


_MERGE_TAG = "tag:yaml.org,2002:merge"
_PORT_PROTOCOLS = {"tcp", "udp", "sctp"}


class _RecipeLoader(yaml.SafeLoader):
    """YAML loader used for recipe parsing."""


def _construct_mapping(loader: _RecipeLoader, node: yaml.MappingNode, deep: bool = False):
    loader.flatten_mapping(node)
    mapping: dict[Any, Any] = {}
    for key_node, value_node in node.value:
        key = loader.construct_object(key_node, deep=deep)
        try:
            hash(key)
        except TypeError as exc:
            raise RecipeError([f"unhashable YAML key: {key!r}"]) from exc
        mapping[key] = loader.construct_object(value_node, deep=deep)
    return mapping


_RecipeLoader.add_constructor(yaml.resolver.BaseResolver.DEFAULT_MAPPING_TAG, _construct_mapping)


def _reject_duplicate_keys(
    node: yaml.Node | None, visited: set[int], loader: _RecipeLoader
) -> None:
    """Walk the composed YAML tree and reject duplicate keys in every mapping.

    Keys are resolved through the loader (so ``true`` and ``yes`` collide), and
    this runs before construction so duplicates inside merge anchors are caught
    too, while merge keys themselves stay legal.
    """

    if node is None or id(node) in visited:
        return
    visited.add(id(node))

    if isinstance(node, yaml.MappingNode):
        seen: set[Any] = set()
        for key_node, value_node in node.value:
            if key_node.tag == _MERGE_TAG:
                _reject_duplicate_keys(value_node, visited, loader)
                continue
            key = loader.construct_object(key_node)
            try:
                duplicate = key in seen
            except TypeError as exc:
                raise RecipeError([f"unhashable YAML key: {key!r}"]) from exc
            if duplicate:
                raise RecipeError([f"duplicate key {key!r} at line {key_node.start_mark.line + 1}"])
            seen.add(key)
            _reject_duplicate_keys(value_node, visited, loader)
    elif isinstance(node, yaml.SequenceNode):
        for item in node.value:
            _reject_duplicate_keys(item, visited, loader)


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

    loader = _RecipeLoader(text)
    try:
        root = loader.get_single_node()
        _reject_duplicate_keys(root, set(), loader)
        data = loader.construct_document(root) if root is not None else None
    except RecipeError:
        raise
    except yaml.YAMLError as exc:
        raise RecipeError([f"invalid YAML: {exc}"]) from exc
    if data is None:
        raise RecipeError(["recipe is empty"])
    return parse_recipe(data)


def load_recipe_file(path: Path) -> Recipe:
    """Read and validate a recipe YAML file."""

    try:
        text = path.read_text(encoding="utf-8")
    except (OSError, UnicodeDecodeError) as exc:
        raise RecipeError([f"could not read {path}: {exc}"]) from exc
    return parse_recipe_yaml(text)


def recipe_to_yaml(recipe: Recipe) -> str:
    """Serialize a recipe back to YAML, omitting fields the author never set."""

    data = recipe.model_dump(by_alias=True, exclude_unset=True)
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
