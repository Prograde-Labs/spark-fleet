"""Import old legacy-controller workloads into fleet recipes.

The importer reads each old controller's workload list, instance YAML, and model
catalog, groups distributed heavy workloads by their master address/port (or
``-head``/``-worker`` naming), generates fleet recipes with per-node roles, turns
lights into service recipes, folds pull/staging/fetch helpers into prepare jobs,
and replaces known LAN/fabric IPs with node placeholders. It produces proposed
recipes and diffs; applying them requires explicit confirmation.
"""

from __future__ import annotations

import difflib
import os
import re
import shlex
import tempfile
import uuid
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import httpx
import yaml

from spark_fleet.shared.db import Database
from spark_fleet.shared.recipe import (
    ALIAS_PATTERN,
    NAME_PATTERN,
    Recipe,
    RecipeError,
    parse_recipe,
    parse_recipe_yaml,
    recipe_to_yaml,
)
from spark_fleet.shared.render import NodeAddress

_NAME_RE = re.compile(NAME_PATTERN)
_ALIAS_RE = re.compile(ALIAS_PATTERN)
_HEAVY_SUFFIXES = ("-head", "-worker", "-rank0", "-rank1", "-rank2", "-rank3")
_HELPER_PATTERNS = ("pull", "staging", "fetch")
_SKIP_PATTERNS = ("log-server", "log_server", "logserver")
_MASTER_ENV = ("MASTER_ADDR", "TF_TP_MASTER")
_MASTER_PORT_ENV = ("MASTER_PORT", "TF_TP_PORT")
_RANK_ENV = ("NODE_RANK", "RANK")


class ImporterError(RuntimeError):
    """Raised when importing workloads or applying a plan fails."""


@dataclass(frozen=True)
class OldWorkload:
    name: str
    node_id: str
    template: str
    workload_class: str
    inference_port: int | None
    models: tuple[str, ...]
    served_model: str | None
    config: Mapping[str, Any]


@dataclass(frozen=True)
class ImportProposal:
    name: str
    recipe: Recipe
    old_yaml: str
    diff: str


@dataclass(frozen=True)
class SkippedWorkload:
    name: str
    reason: str


@dataclass(frozen=True)
class ImportPlan:
    proposals: tuple[ImportProposal, ...]
    skipped: tuple[SkippedWorkload, ...]

    def apply(
        self,
        db: Database,
        *,
        confirm: bool = False,
        recipes_dir: Path | None = None,
    ) -> list[str]:
        if not confirm:
            raise ImporterError("import requires explicit confirmation")
        names = [proposal.recipe.metadata.name for proposal in self.proposals]
        duplicates = sorted({name for name in names if names.count(name) > 1})
        if duplicates:
            raise ImporterError(f"duplicate recipe names: {', '.join(duplicates)}")
        existing_recipe_names = {item["recipe_name"] for item in db.list_deployments()}
        for name in names:
            if db.get_deployment_by_name(name) is not None:
                raise ImporterError(f"deployment {name!r} already exists")
            if name in existing_recipe_names:
                raise ImporterError(f"recipe {name!r} is already used by a deployment")
            if recipes_dir is not None and (recipes_dir / f"{name}.yaml").exists():
                raise ImporterError(f"recipe file {name!r} already exists")

        aliases: set[str] = set()
        for proposal in self.proposals:
            proposal_aliases = _aliases(proposal.recipe)
            overlap = aliases & proposal_aliases
            if overlap:
                raise ImporterError(f"duplicate aliases in import: {', '.join(sorted(overlap))}")
            aliases |= proposal_aliases
        for deployment in db.list_deployments():
            if deployment["status"] != "loaded":
                continue
            try:
                other = parse_recipe(deployment["recipe"])
            except RecipeError:
                continue
            overlap = aliases & _aliases(other)
            if overlap:
                raise ImporterError(
                    f"aliases already in use by {deployment['name']}: {', '.join(sorted(overlap))}"
                )
        if recipes_dir is not None and recipes_dir.is_dir():
            for path in recipes_dir.glob("*.yaml"):
                try:
                    other = parse_recipe_yaml(path.read_text(encoding="utf-8"))
                except (OSError, UnicodeDecodeError, RecipeError):
                    continue
                overlap = aliases & _aliases(other)
                if overlap:
                    raise ImporterError(
                        f"aliases already used by {path.stem}: {', '.join(sorted(overlap))}"
                    )

        created: list[str] = []
        for proposal in self.proposals:
            name = proposal.recipe.metadata.name
            deployment_id = uuid.uuid4().hex
            db.create_deployment(
                deployment_id,
                name,
                name,
                proposal.recipe.spec.mode,
                proposal.recipe.model_dump(by_alias=True, exclude_unset=True),
                status="pending",
            )
            if recipes_dir is not None:
                recipes_dir.mkdir(parents=True, exist_ok=True)
                _write_recipe_file(recipes_dir / f"{name}.yaml", recipe_to_yaml(proposal.recipe))
            created.append(deployment_id)
        return created


class OldControllerClient:
    """Read-only client for an old legacy-controller controller."""

    def __init__(
        self,
        base_url: str,
        *,
        timeout: float = 15.0,
        transport: httpx.AsyncBaseTransport | None = None,
    ):
        parsed = httpx.URL(base_url)
        if parsed.scheme not in ("http", "https") or not parsed.host:
            raise ValueError(f"invalid controller URL: {base_url!r}")
        self._client = httpx.AsyncClient(
            base_url=base_url.rstrip("/"),
            timeout=timeout,
            transport=transport,
        )

    async def aclose(self) -> None:
        await self._client.aclose()

    async def __aenter__(self) -> OldControllerClient:
        return self

    async def __aexit__(self, *exc_info) -> None:
        await self.aclose()

    async def workloads(self) -> list[dict[str, Any]]:
        payload = await self._get("/api/workloads")
        workloads = payload.get("workloads") if isinstance(payload, Mapping) else None
        if not isinstance(workloads, list):
            raise ImporterError("old controller returned an invalid workload list")
        return [item for item in workloads if isinstance(item, dict)]

    async def instance(self, name: str) -> str:
        payload = await self._get(f"/api/instances/{httpx.URL(name).path}")
        config = payload.get("config") if isinstance(payload, Mapping) else None
        if not isinstance(config, str):
            raise ImporterError(f"old controller returned no config for {name!r}")
        return config

    async def models(self) -> list[dict[str, Any]]:
        payload = await self._get("/api/models")
        models = payload.get("models") if isinstance(payload, Mapping) else None
        if not isinstance(models, list):
            raise ImporterError("old controller returned an invalid model list")
        return [item for item in models if isinstance(item, dict)]

    async def _get(self, path: str) -> Any:
        try:
            response = await self._client.get(path)
        except httpx.HTTPError as exc:
            raise ImporterError(f"old controller request failed: {exc}") from exc
        if response.status_code >= 400:
            raise ImporterError(f"old controller returned {response.status_code} for {path}")
        try:
            return response.json()
        except ValueError as exc:
            raise ImporterError(f"old controller returned invalid JSON for {path}") from exc


class Importer:
    """Build fleet recipes and diffs from old controller workloads."""

    def __init__(self, nodes: Mapping[str, NodeAddress]):
        self._nodes = dict(nodes)
        self._ip_placeholders: dict[str, str] = {}
        for node_id, address in self._nodes.items():
            self._ip_placeholders[address.lan_ip] = f"{{{{ node.{node_id}.lanIp }}}}"
            self._ip_placeholders[address.fabric_ip] = f"{{{{ node.{node_id}.fabricIp }}}}"

    async def plan(self, sources: Mapping[str, tuple[str, OldControllerClient]]) -> ImportPlan:
        workloads: list[OldWorkload] = []
        skipped: list[SkippedWorkload] = []
        for node_id, (_url, client) in sources.items():
            for item in await client.workloads():
                name = item.get("name")
                if not isinstance(name, str) or not name:
                    continue
                try:
                    config_text = await client.instance(name)
                    config = yaml.safe_load(config_text)
                except (ImporterError, yaml.YAMLError) as exc:
                    skipped.append(SkippedWorkload(name, f"could not read instance: {exc}"))
                    continue
                if not isinstance(config, Mapping):
                    skipped.append(SkippedWorkload(name, "instance is not a mapping"))
                    continue
                workloads.append(
                    OldWorkload(
                        name=name,
                        node_id=node_id,
                        template=str(item.get("template") or ""),
                        workload_class=str(item.get("class") or "heavy"),
                        inference_port=(
                            item.get("inference_port")
                            if isinstance(item.get("inference_port"), int)
                            else None
                        ),
                        models=tuple(
                            model for model in item.get("models", []) if isinstance(model, str)
                        ),
                        served_model=(
                            item.get("served_model")
                            if isinstance(item.get("served_model"), str)
                            else None
                        ),
                        config=config,
                    )
                )
        return self._build_plan(workloads, skipped)

    def _build_plan(
        self, workloads: Sequence[OldWorkload], skipped: list[SkippedWorkload] | None = None
    ) -> ImportPlan:
        skipped = list(skipped or [])
        heavy = [item for item in workloads if item.workload_class == "heavy"]
        lights = [item for item in workloads if item.workload_class == "light"]

        groups: dict[str, list[OldWorkload]] = {}
        for item in heavy:
            groups.setdefault(_group_key(item), []).append(item)

        proposals: list[ImportProposal] = []
        group_recipes: dict[str, dict[str, Any]] = {}
        for key, members in groups.items():
            name = _group_name(key, members)
            try:
                recipe_dict = self._deployment_recipe(name, members)
                recipe = parse_recipe(recipe_dict)
            except (ImporterError, RecipeError) as exc:
                skipped.append(SkippedWorkload(name, str(exc)))
                continue
            group_recipes[name] = recipe_dict
            old_yaml = _old_yaml(members)
            proposals.append(
                ImportProposal(
                    name=name,
                    recipe=recipe,
                    old_yaml=old_yaml,
                    diff=_diff(old_yaml, recipe_to_yaml(recipe), name),
                )
            )

        for item in lights:
            lowered = item.name.lower()
            if any(pattern in lowered for pattern in _SKIP_PATTERNS):
                skipped.append(SkippedWorkload(item.name, "obsolete log helper"))
                continue
            if any(pattern in lowered for pattern in _HELPER_PATTERNS):
                try:
                    attached = _attach_prepare(group_recipes, item, self._replace)
                except ImporterError as exc:
                    skipped.append(SkippedWorkload(item.name, str(exc)))
                    continue
                if attached:
                    skipped.append(
                        SkippedWorkload(item.name, f"folded into {attached} prepare jobs")
                    )
                else:
                    skipped.append(SkippedWorkload(item.name, "no matching deployment"))
                continue
            try:
                recipe_dict = self._service_recipe(item)
                recipe = parse_recipe(recipe_dict)
            except (ImporterError, RecipeError) as exc:
                skipped.append(SkippedWorkload(item.name, str(exc)))
                continue
            proposals.append(
                ImportProposal(
                    name=recipe.metadata.name,
                    recipe=recipe,
                    old_yaml=item.name,
                    diff=_diff(item.name, recipe_to_yaml(recipe), recipe.metadata.name),
                )
            )

        # Prepare jobs are attached after proposals are built; rebuild diffs.
        rebuilt: list[ImportProposal] = []
        for proposal in proposals:
            recipe_dict = group_recipes.get(proposal.name)
            if recipe_dict is None:
                rebuilt.append(proposal)
                continue
            try:
                recipe = parse_recipe(recipe_dict)
            except RecipeError as exc:
                skipped.append(SkippedWorkload(proposal.name, str(exc)))
                continue
            rebuilt.append(
                ImportProposal(
                    name=proposal.name,
                    recipe=recipe,
                    old_yaml=proposal.old_yaml,
                    diff=_diff(
                        proposal.old_yaml,
                        recipe_to_yaml(recipe),
                        proposal.name,
                    ),
                )
            )
        return ImportPlan(tuple(rebuilt), tuple(skipped))

    def _deployment_recipe(self, name: str, members: Sequence[OldWorkload]) -> dict[str, Any]:
        ordered = sorted(members, key=_rank)
        roles: list[dict[str, Any]] = []
        head_seen = False
        worker_index = 0
        for item in ordered:
            rank = _rank(item)
            if rank == 0 and not head_seen:
                role_name = "head"
                start_order = 20
                head_seen = True
                if item.inference_port is not None:
                    readiness = {
                        "type": "http",
                        "path": "/v1/models",
                        "timeoutSeconds": 3600,
                    }
                else:
                    readiness = {"type": "running", "timeoutSeconds": 3600}
            else:
                worker_index += 1
                role_name = "worker" if worker_index == 1 else f"worker-{worker_index}"
                start_order = 10
                readiness = {"type": "running", "timeoutSeconds": 3600}
            roles.append(
                {
                    "name": role_name,
                    "node": item.node_id,
                    "class": "heavy",
                    "startOrder": start_order,
                    "inferencePort": item.inference_port,
                    "readiness": readiness,
                    "service": self._service(item),
                }
            )
        first = ordered[0]
        model = _model_spec(first)
        recipe: dict[str, Any] = {
            "apiVersion": "spark-fleet/v1",
            "kind": "Recipe",
            "metadata": {"name": name},
            "spec": {
                "mode": "deployment",
                "runtime": _runtime(first),
                "roles": roles,
            },
        }
        if model is not None:
            recipe["spec"]["model"] = model
            recipe["spec"]["gateway"] = {"aliases": [model["servedName"]]}
        port = _master_port(first)
        if port is not None:
            recipe["spec"]["topology"] = {"preset": "custom", "masterPort": port}
        return recipe

    def _service_recipe(self, item: OldWorkload) -> dict[str, Any]:
        return {
            "apiVersion": "spark-fleet/v1",
            "kind": "Recipe",
            "metadata": {"name": _sanitize(item.name)},
            "spec": {
                "mode": "service",
                "runtime": _runtime(item),
                "roles": [
                    {
                        "name": "service",
                        "node": item.node_id,
                        "class": "light",
                        "startOrder": 0,
                        "service": self._service(item),
                    }
                ],
            },
        }

    def _service(self, item: OldWorkload) -> dict[str, Any]:
        config = item.config.get("services")
        workload = config.get("workload") if isinstance(config, Mapping) else None
        if not isinstance(workload, Mapping):
            raise ImporterError(f"workload {item.name!r} has no services.workload")
        service = self._replace(dict(workload))
        service.pop("container_name", None)
        service.pop("labels", None)
        if not isinstance(service.get("image"), str) or not service["image"]:
            raise ImporterError(f"workload {item.name!r} has no image")
        return service

    def _replace(self, value: Any) -> Any:
        if isinstance(value, str):
            result = value
            for ip, placeholder in sorted(
                self._ip_placeholders.items(), key=lambda pair: len(pair[0]), reverse=True
            ):
                result = result.replace(ip, placeholder)
            return result
        if isinstance(value, list):
            return [self._replace(item) for item in value]
        if isinstance(value, Mapping):
            return {key: self._replace(item) for key, item in value.items()}
        return value


def _env(config: Mapping[str, Any]) -> dict[str, str]:
    services = config.get("services")
    workload = services.get("workload") if isinstance(services, Mapping) else None
    environment = workload.get("environment") if isinstance(workload, Mapping) else None
    result: dict[str, str] = {}
    if isinstance(environment, Mapping):
        result.update(
            {str(key): str(value) for key, value in environment.items() if value is not None}
        )
    elif isinstance(environment, list):
        for entry in environment:
            key, separator, value = str(entry).partition("=")
            if separator:
                result[key] = value
    return result


def _command(config: Mapping[str, Any]) -> list[str]:
    services = config.get("services")
    workload = services.get("workload") if isinstance(services, Mapping) else None
    command = workload.get("command") if isinstance(workload, Mapping) else None
    if isinstance(command, list):
        return [str(item) for item in command]
    if isinstance(command, str):
        return shlex.split(command)
    return []


def _flag(command: Sequence[str], name: str) -> str | None:
    for index, value in enumerate(command):
        if value == name and index + 1 < len(command):
            return command[index + 1]
        prefix = f"{name}="
        if value.startswith(prefix):
            return value[len(prefix) :]
    return None


def _group_key(item: OldWorkload) -> str:
    env = _env(item.config)
    command = _command(item.config)
    master = env.get("MASTER_ADDR") or env.get("TF_TP_MASTER") or _flag(command, "--master-addr")
    port = env.get("MASTER_PORT") or env.get("TF_TP_PORT") or _flag(command, "--master-port")
    if master:
        return f"{master}:{port or ''}"
    for suffix in _HEAVY_SUFFIXES:
        if item.name.endswith(suffix):
            return item.name[: -len(suffix)]
    return item.name


def _group_name(key: str, members: Sequence[OldWorkload]) -> str:
    if ":" in key:
        if len(members) == 1:
            return _sanitize(_strip_heavy_suffix(members[0].name))
        prefix = _common_prefix([item.name for item in members])
        return _sanitize(prefix or members[0].name)
    return _sanitize(key)


def _strip_heavy_suffix(name: str) -> str:
    for suffix in _HEAVY_SUFFIXES:
        if name.endswith(suffix):
            return name[: -len(suffix)]
    return name


def _common_prefix(names: Sequence[str]) -> str:
    if not names:
        return ""
    prefix = names[0]
    for name in names[1:]:
        while prefix and not name.startswith(prefix):
            prefix = prefix[:-1]
    return prefix.rstrip("-_")


def _rank(item: OldWorkload) -> int:
    env = _env(item.config)
    for key in _RANK_ENV:
        value = env.get(key)
        if value is not None:
            try:
                return int(value)
            except ValueError:
                pass
    command = _command(item.config)
    value = _flag(command, "--node-rank") or _flag(command, "--rank")
    if value is not None:
        try:
            return int(value)
        except ValueError:
            pass
    if item.name.endswith("-head"):
        return 0
    if item.name.endswith("-worker"):
        return 1
    return 0


def _master_port(item: OldWorkload) -> int | None:
    env = _env(item.config)
    for key in _MASTER_PORT_ENV:
        value = env.get(key)
        if value is not None:
            try:
                return int(value)
            except ValueError:
                pass
    value = _flag(_command(item.config), "--master-port")
    if value is not None:
        try:
            return int(value)
        except ValueError:
            pass
    return None


def _model_spec(item: OldWorkload) -> dict[str, Any] | None:
    if not item.models:
        return None
    served = item.served_model or _flag(_command(item.config), "--served-model-name")
    if not served:
        served = item.name
    if not _ALIAS_RE.fullmatch(served):
        served = _sanitize(served)
    return {
        "repo": item.models[0],
        "servedName": served,
        "extraRepos": list(item.models[1:]),
    }


def _runtime(item: OldWorkload) -> str:
    text = " ".join(_command(item.config)).lower()
    image = ""
    services = item.config.get("services")
    workload = services.get("workload") if isinstance(services, Mapping) else None
    if isinstance(workload, Mapping) and isinstance(workload.get("image"), str):
        image = workload["image"].lower()
    combined = f"{text} {image}"
    for runtime in ("vllm", "sglang", "llama.cpp", "llamacpp", "comfyui"):
        if runtime in combined:
            return "llamacpp" if runtime in ("llama.cpp", "llamacpp") else runtime
    return "custom"


def _attach_prepare(
    group_recipes: dict[str, dict[str, Any]],
    item: OldWorkload,
    replace: Any,
) -> str | None:
    for name in sorted(group_recipes, key=len, reverse=True):
        if item.name.startswith(name):
            services = item.config.get("services")
            workload = services.get("workload") if isinstance(services, Mapping) else None
            if not isinstance(workload, Mapping):
                raise ImporterError("helper has no services.workload")
            service = replace(dict(workload))
            image = service.get("image")
            if not isinstance(image, str) or not image:
                raise ImporterError("helper has no image")
            command = service.get("command")
            if isinstance(command, str):
                command = shlex.split(command)
            if not isinstance(command, list) or not command:
                raise ImporterError("helper has no command")
            volumes = service.get("volumes", [])
            if not isinstance(volumes, list):
                volumes = []
            prepare = {
                "name": _sanitize(item.name),
                "node": item.node_id,
                "image": image,
                "command": [str(part) for part in command],
                "volumes": [_volume_string(volume) for volume in volumes],
                "environment": _prepare_environment(service.get("environment")),
            }
            group_recipes[name].setdefault("spec", {}).setdefault("prepare", []).append(prepare)
            return name
    return None


def _prepare_environment(value: Any) -> dict[str, Any]:
    if isinstance(value, Mapping):
        result: dict[str, Any] = {}
        for key, item in value.items():
            if not isinstance(item, (str, int, float, bool)):
                raise ImporterError(f"helper environment {key!r} has a non-scalar value")
            result[str(key)] = item
        return result
    if isinstance(value, list):
        result = {}
        for entry in value:
            key, separator, item = str(entry).partition("=")
            if not separator:
                raise ImporterError(f"helper environment entry {entry!r} is not KEY=VALUE")
            result[key] = item
        return result
    return {}


def _volume_string(value: Any) -> str:
    if isinstance(value, str):
        return value
    if isinstance(value, Mapping):
        source = value.get("source")
        target = value.get("target")
        if isinstance(source, str) and isinstance(target, str):
            mount = f"{source}:{target}"
            if value.get("read_only") is True:
                mount += ":ro"
            return mount
    raise ImporterError(f"unsupported helper volume: {value!r}")


def _aliases(recipe: Recipe) -> set[str]:
    aliases: set[str] = set()
    if recipe.spec.gateway is not None:
        aliases.update(recipe.spec.gateway.aliases)
    if recipe.spec.model is not None:
        aliases.add(recipe.spec.model.served_name)
    return aliases


def _write_recipe_file(path: Path, text: str) -> None:
    temporary: Path | None = None
    try:
        with tempfile.NamedTemporaryFile(
            mode="w", dir=path.parent, suffix=".tmp", delete=False, encoding="utf-8"
        ) as handle:
            temporary = Path(handle.name)
            handle.write(text)
        os.replace(temporary, path)
    finally:
        if temporary is not None and temporary.exists():
            temporary.unlink(missing_ok=True)


def _sanitize(value: str) -> str:
    cleaned = re.sub(r"[^a-z0-9-]+", "-", value.lower()).strip("-")
    cleaned = re.sub(r"-{2,}", "-", cleaned)
    if not cleaned:
        cleaned = "imported"
    return cleaned[:63].strip("-")


def _old_yaml(members: Sequence[OldWorkload]) -> str:
    return "\n---\n".join(yaml.safe_dump(dict(item.config), sort_keys=False) for item in members)


def _diff(old: str, new: str, name: str) -> str:
    return "\n".join(
        difflib.unified_diff(
            old.splitlines(),
            new.splitlines(),
            fromfile=f"{name}.old.yaml",
            tofile=f"{name}.recipe.yaml",
            lineterm="",
        )
    )
