"""Fleet deployment lifecycle: load, unload, redeploy, readiness, rollback.

A deployment job validates the stored recipe against the node registry and alias
space, acquires a global lifecycle lock plus per-node locks, stops conflicting
heavy deployments in reverse start order, runs prepare jobs, starts roles in
``startOrder``, waits for each role's readiness probe, and marks the deployment
loaded. Failures stop started roles and, when the rollback policy is ``restore``,
best-effort reload the displaced deployments. Unload stops roles in reverse order
and clears the primary flag if it pointed at that deployment.
"""

from __future__ import annotations

import asyncio
import json
import re
import time
from collections.abc import Awaitable, Callable, Mapping, Sequence
from typing import Any

import httpx

from spark_fleet.controller.agent_client import AgentClient, AgentError
from spark_fleet.controller.nodes import Node, NodeRegistry
from spark_fleet.shared.db import Database
from spark_fleet.shared.jobs import Job, JobRunner
from spark_fleet.shared.recipe import (
    Recipe,
    RecipeError,
    RoleSpec,
    parse_recipe,
    validate_against_nodes,
)
from spark_fleet.shared.render import (
    LABEL_DEPLOYMENT,
    LABEL_RECIPE,
    LABEL_ROLE,
    NodeAddress,
    RenderError,
    render_deployment,
)

ClientFactory = Callable[[str, str], AgentClient]
ReadinessChecker = Callable[[RoleSpec, Node, str, AgentClient, str | None], Awaitable[None]]

GLOBAL_LOCK = "deployment:global"
_PROJECT_RE = re.compile(r"[a-z0-9][a-z0-9_-]{0,62}")


class DeploymentError(RuntimeError):
    """Raised when a deployment operation is invalid or fails."""


class DeploymentManager:
    """Submit and execute fleet-wide deployment lifecycle jobs."""

    def __init__(
        self,
        db: Database,
        registry: NodeRegistry,
        runner: JobRunner,
        *,
        token: str,
        rollback_policy: str = "restore",
        client_factory: ClientFactory | None = None,
        readiness_checker: ReadinessChecker | None = None,
        sleep: Callable[[float], Awaitable[None]] = asyncio.sleep,
        clock: Callable[[], float] = time.monotonic,
        prepare_poll_interval: float = 1.0,
    ):
        if rollback_policy not in ("restore", "leave-down"):
            raise ValueError(f"invalid rollback policy: {rollback_policy!r}")
        if prepare_poll_interval <= 0:
            raise ValueError("prepare poll interval must be positive")
        self._db = db
        self._registry = registry
        self._runner = runner
        self._token = token
        self._rollback_policy = rollback_policy
        self._client_factory = client_factory or AgentClient
        self._readiness_checker = readiness_checker or self._check_readiness
        self._sleep = sleep
        self._clock = clock
        self._prepare_poll_interval = prepare_poll_interval

        runner.register("deployment.load", self._load_job)
        runner.register("deployment.redeploy", self._load_job)
        runner.register("deployment.unload", self._unload_job)

    async def load(self, deployment_id: str, *, primary: bool = False) -> str:
        deployment = self._require_deployment(deployment_id)
        recipe = self._recipe(deployment)
        self._validate_load(deployment, recipe, allow_loaded=False)
        if primary and recipe.spec.mode != "deployment":
            raise DeploymentError("only deployment recipes can be primary")
        return await self._runner.submit(
            "deployment.load",
            {"deployment_id": deployment_id, "primary": primary},
            deployment_id=deployment_id,
            locks=self._locks(deployment_id, recipe),
        )

    async def redeploy(
        self,
        deployment_id: str,
        *,
        primary: bool = False,
        recipe: Recipe | None = None,
    ) -> str:
        deployment = self._require_deployment(deployment_id)
        previous_recipe = self._recipe(deployment)
        current = recipe if recipe is not None else previous_recipe
        self._validate_load(deployment, current, allow_loaded=True)
        if primary and current.spec.mode != "deployment":
            raise DeploymentError("only deployment recipes can be primary")
        payload = {
            "deployment_id": deployment_id,
            "primary": primary,
        }
        if recipe is not None:
            payload["recipe"] = recipe.model_dump(by_alias=True, exclude_unset=True)
        return await self._runner.submit(
            "deployment.redeploy",
            payload,
            deployment_id=deployment_id,
            locks=self._locks(deployment_id, current),
        )

    async def unload(self, deployment_id: str) -> str:
        deployment = self._require_deployment(deployment_id)
        recipe = self._recipe(deployment)
        known = {node.id for node in self._registry.list()}
        missing = sorted({role.node for role in recipe.spec.roles if role.node not in known})
        if missing:
            raise DeploymentError(f"unknown node(s): {', '.join(missing)}")
        self._validate_projects(deployment, recipe)
        return await self._runner.submit(
            "deployment.unload",
            {"deployment_id": deployment_id},
            deployment_id=deployment_id,
            locks=self._locks(deployment_id, recipe),
        )

    def is_stale(self, deployment_id: str, recipe: Recipe) -> bool:
        deployment = self._require_deployment(deployment_id)
        if deployment["status"] != "loaded":
            return False
        stored = deployment.get("recipe")
        current = recipe.model_dump(by_alias=True, exclude_unset=True)
        return _canonical(stored) != _canonical(current)

    # Job handlers ---------------------------------------------------------

    async def _load_job(self, job: Job) -> dict[str, Any]:
        payload = job.payload if isinstance(job.payload, Mapping) else {}
        deployment_id = payload.get("deployment_id") or job.deployment_id
        if not deployment_id:
            raise DeploymentError("deployment id is required")
        deployment = self._require_deployment(deployment_id)
        recipe = self._recipe(deployment)
        payload_recipe = payload.get("recipe")
        if isinstance(payload_recipe, Mapping):
            try:
                recipe = parse_recipe(payload_recipe)
            except RecipeError as exc:
                raise DeploymentError(f"deployment recipe is invalid: {exc}") from exc
        self._validate_load(
            deployment,
            recipe,
            allow_loaded=job.kind == "deployment.redeploy",
        )
        primary_requested = bool(payload.get("primary"))
        if primary_requested and recipe.spec.mode != "deployment":
            raise DeploymentError("only deployment recipes can be primary")
        was_loaded = deployment["status"] == "loaded"
        stop_recipe = self._recipe(deployment)

        clients = _ClientPool(self._registry, self._token, self._client_factory)
        started: list[tuple[RoleSpec, str, AgentClient]] = []
        displaced: list[tuple[dict[str, Any], bool]] = []
        stop_attempted = False
        try:
            await self._validate_models(recipe, clients)

            if was_loaded:
                stop_attempted = True
                self._event(job, "stopping current roles", phase="stop")
                errors = await self._stop_roles(deployment, stop_recipe, clients)
                if errors:
                    raise DeploymentError("; ".join(errors))

            displaced = [
                (other, other["is_primary"]) for other in self._conflicts(deployment_id, recipe)
            ]
            if displaced:
                self._event(job, "stopping conflicting deployments", phase="conflicts")
                await self._stop_conflicts(displaced, clients)

            self._event(job, "running prepare jobs", phase="prepare")
            await self._run_prepare(recipe, deployment, clients, job)

            self._event(job, "starting roles", phase="start")
            await self._start_roles(recipe, deployment, clients, job, started)

            if isinstance(payload_recipe, Mapping):
                self._db.update_deployment_recipe(
                    deployment_id,
                    recipe.model_dump(by_alias=True, exclude_unset=True),
                    recipe_name=recipe.metadata.name,
                    mode=recipe.spec.mode,
                )
            self._db.update_deployment(
                deployment_id, status="loaded", error=None, loaded_at=time.time()
            )
            has_primary = any(
                item["is_primary"]
                for item in self._db.list_deployments()
                if item["status"] == "loaded" and item["mode"] == "deployment"
            )
            if recipe.spec.mode == "deployment" and (primary_requested or not has_primary):
                self._db.set_primary(deployment_id)
            self._event(job, "deployment loaded", phase="loaded")
            return {"deployment_id": deployment_id, "status": "loaded"}
        except asyncio.CancelledError:
            current = asyncio.current_task()
            uncancel = getattr(current, "uncancel", None)
            if uncancel is not None:
                uncancel()
            self._event(job, "deployment cancelled", level="error", phase="cancelled")
            try:
                if stop_attempted:
                    await self._stop_started(deployment_id, started)
                    await self._stop_roles(deployment, stop_recipe, clients)
                elif started:
                    await self._stop_started(deployment_id, started)
            finally:
                if stop_attempted or started or not was_loaded:
                    self._mark_existing_roles_stopped(deployment_id)
                if stop_attempted:
                    self._db.update_deployment(deployment_id, status="failed", error="cancelled")
                    if deployment["is_primary"]:
                        self._db.clear_primary()
                elif started or not was_loaded:
                    self._db.update_deployment(deployment_id, status="failed", error="cancelled")
                    if deployment["is_primary"]:
                        self._db.clear_primary()
            if displaced:
                try:
                    await self._rollback(displaced, clients, job)
                except asyncio.CancelledError:
                    pass
            raise
        except Exception as exc:
            error = str(exc)
            self._event(job, f"deployment failed: {error}", level="error", phase="failed")
            cleanup_cancelled = False
            try:
                if stop_attempted or started:
                    await self._stop_started(deployment_id, started)
            except asyncio.CancelledError:
                current = asyncio.current_task()
                uncancel = getattr(current, "uncancel", None)
                if uncancel is not None:
                    uncancel()
                cleanup_cancelled = True
            finally:
                if stop_attempted or started or not was_loaded:
                    self._mark_existing_roles_stopped(deployment_id)
                if stop_attempted:
                    self._db.update_deployment(deployment_id, status="failed", error=error)
                    if deployment["is_primary"]:
                        self._db.clear_primary()
                elif started or not was_loaded:
                    self._db.update_deployment(deployment_id, status="failed", error=error)
                    if deployment["is_primary"]:
                        self._db.clear_primary()
            if displaced or stop_attempted or started:
                rollback_errors = await self._rollback(displaced, clients, job)
                if rollback_errors:
                    error = f"{error}; rollback: {'; '.join(rollback_errors)}"
            if cleanup_cancelled:
                raise asyncio.CancelledError()
            raise DeploymentError(error) from exc
        finally:
            await clients.close()

    async def _unload_job(self, job: Job) -> dict[str, Any]:
        payload = job.payload if isinstance(job.payload, Mapping) else {}
        deployment_id = payload.get("deployment_id") or job.deployment_id
        if not deployment_id:
            raise DeploymentError("deployment id is required")
        deployment = self._require_deployment(deployment_id)
        recipe = self._recipe(deployment)

        clients = _ClientPool(self._registry, self._token, self._client_factory)
        try:
            errors = await self._stop_roles(deployment, recipe, clients)
        except Exception as exc:
            message = str(exc)
            self._db.update_deployment(deployment_id, status="failed", error=message)
            if deployment["is_primary"]:
                self._db.clear_primary()
            raise DeploymentError(message) from exc
        finally:
            await clients.close()

        if errors:
            message = "; ".join(errors)
            self._db.update_deployment(deployment_id, status="failed", error=message)
            if deployment["is_primary"]:
                self._db.clear_primary()
            raise DeploymentError(message)
        self._db.update_deployment(deployment_id, status="unloaded", error=None)
        if deployment["is_primary"]:
            self._db.clear_primary()
        self._event(job, "deployment unloaded", phase="unloaded")
        return {"deployment_id": deployment_id, "status": "unloaded"}

    # Lifecycle steps ------------------------------------------------------

    async def _start_roles(
        self,
        recipe: Recipe,
        deployment: Mapping[str, Any],
        clients: _ClientPool,
        job: Job,
        started: list[tuple[RoleSpec, str, AgentClient]],
    ) -> None:
        try:
            rendered = render_deployment(
                recipe,
                deployment["name"],
                deployment["id"],
                self._node_addresses(),
            )
        except RenderError as exc:
            raise DeploymentError(str(exc)) from exc
        rendered_by_role = {item.role: item for item in rendered}

        for role in sorted(recipe.spec.roles, key=lambda item: (item.start_order, item.name)):
            node = self._registry.require(role.node)
            client = await clients.get(role.node)
            project = project_name(deployment["name"], role.name)
            rendered_role = rendered_by_role[role.name]
            self._event(job, f"starting role {role.name}", phase="role")

            try:
                validated = await client.validate_compose(project, rendered_role.compose)
            except AgentError as exc:
                raise DeploymentError(f"role {role.name} validation failed: {exc}") from exc
            config_hash = validated.get("config_hash") if isinstance(validated, Mapping) else None
            if not isinstance(config_hash, str) or not config_hash:
                raise DeploymentError(f"role {role.name} validation returned no config hash")
            self._db.upsert_role(
                deployment["id"],
                role.name,
                role.node,
                role.class_,
                role.start_order,
                config_hash=config_hash,
                status="starting",
            )

            # Record the project before ``up`` so a failure after Docker has
            # created containers still gets a best-effort down.
            started.append((role, project, client))
            try:
                statuses = await client.up_compose(project, rendered_role.compose)
            except AgentError as exc:
                raise DeploymentError(f"role {role.name} failed to start: {exc}") from exc
            container_id = _container_id(statuses, rendered_role.container_name)
            self._db.upsert_role(
                deployment["id"],
                role.name,
                role.node,
                role.class_,
                role.start_order,
                config_hash=config_hash,
                status="starting",
                container_id=container_id,
            )

            self._event(job, f"waiting for role {role.name} readiness", phase="readiness")
            try:
                await self._readiness_checker(role, node, project, client, container_id)
            except Exception as exc:
                raise DeploymentError(f"role {role.name} not ready: {exc}") from exc
            self._db.upsert_role(
                deployment["id"],
                role.name,
                role.node,
                role.class_,
                role.start_order,
                config_hash=config_hash,
                status="running",
                container_id=container_id,
            )

    async def _stop_roles(
        self,
        deployment: Mapping[str, Any],
        recipe: Recipe,
        clients: _ClientPool,
    ) -> list[str]:
        errors: list[str] = []
        for role in sorted(
            recipe.spec.roles, key=lambda item: (item.start_order, item.name), reverse=True
        ):
            client = await clients.get(role.node)
            project = project_name(deployment["name"], role.name)
            try:
                await client.down_compose(project)
            except AgentError as exc:
                errors.append(f"role {role.name}: {exc}")
            self._db.upsert_role(
                deployment["id"],
                role.name,
                role.node,
                role.class_,
                role.start_order,
                status="stopped",
            )
        return errors

    async def _stop_conflicts(
        self,
        displaced: Sequence[tuple[dict[str, Any], bool]],
        clients: _ClientPool,
    ) -> None:
        entries: list[tuple[int, str, dict[str, Any], RoleSpec]] = []
        for deployment, _was_primary in displaced:
            recipe = self._recipe(deployment)
            for role in recipe.spec.roles:
                entries.append((role.start_order, role.name, deployment, role))

        errors: list[str] = []
        for _start_order, _name, deployment, role in sorted(
            entries, key=lambda item: (item[0], item[1]), reverse=True
        ):
            client = await clients.get(role.node)
            project = project_name(deployment["name"], role.name)
            try:
                await client.down_compose(project)
            except AgentError as exc:
                errors.append(f"{deployment['name']}/{role.name}: {exc}")
            self._db.upsert_role(
                deployment["id"],
                role.name,
                role.node,
                role.class_,
                role.start_order,
                status="stopped",
            )

        for deployment, was_primary in displaced:
            self._db.update_deployment(deployment["id"], status="unloaded", error=None)
            if was_primary:
                self._db.clear_primary()
        if errors:
            raise DeploymentError("; ".join(errors))

    async def _stop_started(
        self,
        deployment_id: str,
        started: Sequence[tuple[RoleSpec, str, AgentClient]],
    ) -> None:
        for role, project, client in reversed(started):
            try:
                await client.down_compose(project)
            except AgentError:
                pass
            self._db.upsert_role(
                deployment_id,
                role.name,
                role.node,
                role.class_,
                role.start_order,
                status="stopped",
            )

    async def _rollback(
        self,
        displaced: Sequence[tuple[dict[str, Any], bool]],
        clients: _ClientPool,
        job: Job,
    ) -> list[str]:
        if self._rollback_policy != "restore" or not displaced:
            return []
        self._event(job, "restoring displaced deployments", phase="rollback")
        errors: list[str] = []
        for deployment, was_primary in displaced:
            rollback_started: list[tuple[RoleSpec, str, AgentClient]] = []
            try:
                recipe = self._recipe(deployment)
                await self._run_prepare(recipe, deployment, clients, job)
                await self._start_roles(recipe, deployment, clients, job, rollback_started)
                self._db.update_deployment(
                    deployment["id"], status="loaded", error=None, loaded_at=time.time()
                )
                if was_primary:
                    self._db.set_primary(deployment["id"])
            except asyncio.CancelledError:
                current = asyncio.current_task()
                uncancel = getattr(current, "uncancel", None)
                if uncancel is not None:
                    uncancel()
                await self._stop_started(deployment["id"], rollback_started)
                raise
            except Exception as exc:
                await self._stop_started(deployment["id"], rollback_started)
                errors.append(f"{deployment['name']}: {exc}")
        return errors

    async def _run_prepare(
        self,
        recipe: Recipe,
        deployment: Mapping[str, Any],
        clients: _ClientPool,
        job: Job,
    ) -> None:
        for prepare in recipe.spec.prepare:
            client = await clients.get(prepare.node)
            labels = {
                LABEL_DEPLOYMENT: deployment["name"],
                LABEL_RECIPE: recipe.metadata.name,
                LABEL_ROLE: f"prepare-{prepare.name}",
            }
            self._event(job, f"running prepare {prepare.name}", phase="prepare")
            try:
                job_id = await client.start_job(
                    name=prepare.name,
                    image=prepare.image,
                    command=prepare.command,
                    labels=labels,
                    volumes=prepare.volumes,
                    environment={key: str(value) for key, value in prepare.environment.items()},
                )
            except AgentError as exc:
                raise DeploymentError(f"prepare {prepare.name} failed to start: {exc}") from exc

            deadline = self._clock() + prepare.timeout_seconds
            while True:
                try:
                    status = await client.job_status(job_id)
                except AgentError as exc:
                    try:
                        await client.cancel_job(job_id)
                    except AgentError:
                        pass
                    raise DeploymentError(f"prepare {prepare.name} status failed: {exc}") from exc
                except asyncio.CancelledError:
                    try:
                        await asyncio.shield(client.cancel_job(job_id))
                    except (AgentError, asyncio.CancelledError):
                        pass
                    raise
                state = status.get("state") if isinstance(status, Mapping) else None
                if state == "succeeded":
                    break
                if state in ("failed", "cancelled"):
                    detail = status.get("error") or status.get("logs") or state
                    raise DeploymentError(f"prepare {prepare.name} {state}: {detail}")
                if self._clock() >= deadline:
                    try:
                        await client.cancel_job(job_id)
                    except AgentError:
                        pass
                    raise DeploymentError(
                        f"prepare {prepare.name} timed out after {prepare.timeout_seconds}s"
                    )
                try:
                    await self._sleep(self._prepare_poll_interval)
                except asyncio.CancelledError:
                    try:
                        await asyncio.shield(client.cancel_job(job_id))
                    except (AgentError, asyncio.CancelledError):
                        pass
                    raise

    # Validation and helpers ----------------------------------------------

    def _require_deployment(self, deployment_id: str) -> dict[str, Any]:
        deployment = self._db.get_deployment(deployment_id)
        if deployment is None:
            raise DeploymentError(f"deployment {deployment_id} not found")
        return deployment

    def _recipe(self, deployment: Mapping[str, Any]) -> Recipe:
        try:
            return parse_recipe(deployment["recipe"])
        except RecipeError as exc:
            raise DeploymentError(
                f"deployment {deployment['id']} recipe is invalid: {exc}"
            ) from exc

    def _validate_nodes(self, recipe: Recipe) -> None:
        try:
            validate_against_nodes(recipe, [node.id for node in self._registry.list()])
        except RecipeError as exc:
            raise DeploymentError(str(exc)) from exc

    def _validate_projects(self, deployment: Mapping[str, Any], recipe: Recipe) -> None:
        for role in recipe.spec.roles:
            project = project_name(deployment["name"], role.name)
            if not _PROJECT_RE.fullmatch(project):
                raise DeploymentError(
                    f"project name {project!r} is too long or invalid; "
                    "shorten the deployment or role name"
                )

    async def _validate_models(self, recipe: Recipe, clients: _ClientPool) -> None:
        model = recipe.spec.model
        if model is None:
            return
        repos = {model.repo, *model.extra_repos}
        nodes = sorted({role.node for role in recipe.spec.roles})
        for node_id in nodes:
            client = await clients.get(node_id)
            try:
                catalog = await client.models()
            except AgentError as exc:
                raise DeploymentError(f"could not read model catalog on {node_id}: {exc}") from exc
            entries = catalog.get("models") if isinstance(catalog, Mapping) else None
            cached = {
                entry["repo"]
                for entry in entries or ()
                if isinstance(entry, Mapping)
                and entry.get("cached") is True
                and isinstance(entry.get("repo"), str)
            }
            missing = sorted(repos - cached)
            if missing:
                raise DeploymentError(f"model(s) not cached on {node_id}: {', '.join(missing)}")

    def _validate_load(
        self, deployment: Mapping[str, Any], recipe: Recipe, *, allow_loaded: bool
    ) -> None:
        self._validate_nodes(recipe)
        self._validate_projects(deployment, recipe)
        if not allow_loaded and deployment["status"] == "loaded":
            raise DeploymentError(f"deployment {deployment['name']} is already loaded")
        aliases = _recipe_aliases(recipe)
        if "spark-active" in aliases:
            raise DeploymentError("spark-active is reserved for the primary deployment")
        if aliases:
            for other in self._db.list_deployments():
                if other["id"] == deployment["id"] or other["status"] != "loaded":
                    continue
                try:
                    other_recipe = parse_recipe(other["recipe"])
                except RecipeError:
                    continue
                overlap = aliases & _recipe_aliases(other_recipe)
                if overlap:
                    raise DeploymentError(
                        f"aliases already in use by {other['name']}: {', '.join(sorted(overlap))}"
                    )

    def _conflicts(self, deployment_id: str, recipe: Recipe) -> list[dict[str, Any]]:
        target_nodes = {role.node for role in recipe.spec.roles if role.class_ == "heavy"}
        if not target_nodes:
            return []
        conflicts: list[dict[str, Any]] = []
        for other in self._db.list_deployments():
            if other["id"] == deployment_id or other["status"] != "loaded":
                continue
            heavy_nodes = {
                role["node_id"]
                for role in self._db.list_roles(other["id"])
                if role["class"] == "heavy"
            }
            if heavy_nodes & target_nodes:
                conflicts.append(other)
        return conflicts

    def _node_addresses(self) -> dict[str, NodeAddress]:
        return {
            node.id: NodeAddress(lan_ip=node.lan_ip, fabric_ip=node.fabric_ip)
            for node in self._registry.list()
        }

    def _locks(self, deployment_id: str, recipe: Recipe) -> list[str]:
        nodes = {role.node for role in recipe.spec.roles}
        nodes.update(prepare.node for prepare in recipe.spec.prepare)
        return [
            GLOBAL_LOCK,
            f"deployment:{deployment_id}",
            *(f"node:{node}" for node in sorted(nodes)),
        ]

    def _event(
        self,
        job: Job,
        message: str,
        *,
        phase: str,
        level: str = "info",
    ) -> None:
        self._db.append_job_event(job.id, message, level=level, phase=phase)

    def _mark_role_stopped(self, deployment_id: str, role: RoleSpec) -> None:
        self._db.upsert_role(
            deployment_id,
            role.name,
            role.node,
            role.class_,
            role.start_order,
            status="stopped",
        )

    def _mark_existing_roles_stopped(self, deployment_id: str) -> None:
        for row in self._db.list_roles(deployment_id):
            self._db.upsert_role(
                deployment_id,
                row["role_name"],
                row["node_id"],
                row["class"],
                row["start_order"],
                status="stopped",
            )

    async def _check_readiness(
        self,
        role: RoleSpec,
        node: Node,
        project: str,
        client: AgentClient,
        container_id: str | None,
    ) -> None:
        await check_readiness(
            role,
            node,
            project,
            client,
            container_id,
            sleep=self._sleep,
            clock=self._clock,
        )


class _ClientPool:
    def __init__(self, registry: NodeRegistry, token: str, factory: ClientFactory):
        self._registry = registry
        self._token = token
        self._factory = factory
        self._clients: dict[str, AgentClient] = {}

    async def get(self, node_id: str) -> AgentClient:
        client = self._clients.get(node_id)
        if client is None:
            node = self._registry.require(node_id)
            client = self._factory(node.agent_url, self._token)
            self._clients[node_id] = client
        return client

    async def close(self) -> None:
        for client in self._clients.values():
            try:
                await client.aclose()
            except Exception:
                pass


async def check_readiness(
    role: RoleSpec,
    node: Node,
    project: str,
    client: AgentClient,
    container_id: str | None,
    *,
    sleep: Callable[[float], Awaitable[None]] = asyncio.sleep,
    clock: Callable[[], float] = time.monotonic,
    http_probe: Callable[[str, float], Awaitable[int]] | None = None,
    tcp_probe: Callable[[str, int, float], Awaitable[None]] | None = None,
) -> None:
    """Poll the role's configured readiness probe until it passes or times out."""

    readiness = role.readiness
    deadline = clock() + readiness.timeout_seconds
    last_error: str | None = None
    while True:
        try:
            ready = await _probe(
                role,
                node,
                project,
                client,
                container_id,
                readiness,
                deadline,
                clock,
                http_probe or _default_http_probe,
                tcp_probe or _default_tcp_probe,
            )
        except DeploymentError:
            raise
        except Exception as exc:
            ready = False
            last_error = str(exc)
        if ready:
            return
        if clock() >= deadline:
            detail = f": {last_error}" if last_error else ""
            raise DeploymentError(
                f"role {role.name} readiness timed out after {readiness.timeout_seconds}s{detail}"
            )
        await sleep(readiness.interval_seconds)


async def _probe(
    role: RoleSpec,
    node: Node,
    project: str,
    client: AgentClient,
    container_id: str | None,
    readiness,
    deadline: float,
    clock: Callable[[], float],
    http_probe: Callable[[str, float], Awaitable[int]],
    tcp_probe: Callable[[str, int, float], Awaitable[None]],
) -> bool:
    if readiness.type == "running":
        statuses = await client.compose_status(project)
        return any(status.get("state") == "running" for status in statuses)
    if readiness.type == "container-health":
        statuses = await client.compose_status(project)
        return any(_container_healthy(status) for status in statuses)
    if readiness.type == "http":
        port = readiness.port or role.inference_port
        if port is None:
            raise DeploymentError(f"role {role.name} http readiness requires a port")
        url = f"http://{node.lan_ip}:{port}{readiness.path}"
        status_code = await http_probe(url, 5.0)
        return 200 <= status_code < 400
    if readiness.type == "tcp":
        if readiness.port is None:
            raise DeploymentError(f"role {role.name} tcp readiness requires a port")
        await tcp_probe(node.lan_ip, readiness.port, 5.0)
        return True
    if readiness.type == "log-pattern":
        if container_id is None:
            raise DeploymentError(f"role {role.name} log-pattern readiness requires a container")
        try:
            pattern = re.compile(readiness.pattern)
        except re.error as exc:
            raise DeploymentError(f"role {role.name} has an invalid log pattern: {exc}") from exc
        remaining = max(0.1, deadline - clock())
        try:
            return await asyncio.wait_for(
                _consume_logs(client, container_id, pattern), timeout=remaining
            )
        except TimeoutError:
            return False
    raise DeploymentError(f"role {role.name} has unknown readiness type {readiness.type!r}")


async def _consume_logs(client: AgentClient, container_id: str, pattern: re.Pattern[str]) -> bool:
    async for line in client.follow_container_logs(container_id, tail=200):
        if pattern.search(line):
            return True
    return False


def _container_healthy(status: Mapping[str, Any]) -> bool:
    health = status.get("health")
    if health == "healthy":
        return True
    status_text = status.get("status")
    return isinstance(status_text, str) and "(healthy)" in status_text


async def _default_http_probe(url: str, timeout: float) -> int:
    async with httpx.AsyncClient(timeout=timeout) as client:
        response = await client.get(url)
        return response.status_code


async def _default_tcp_probe(host: str, port: int, timeout: float) -> None:
    _reader, writer = await asyncio.wait_for(asyncio.open_connection(host, port), timeout=timeout)
    writer.close()
    await writer.wait_closed()


def project_name(deployment_name: str, role_name: str) -> str:
    return f"spark-fleet-{deployment_name}-{role_name}"


def _container_id(statuses: Sequence[Mapping[str, Any]], container_name: str) -> str:
    if not statuses:
        raise DeploymentError("agent returned no containers")
    for status in statuses:
        if status.get("name") == container_name:
            container_id = status.get("id")
            if isinstance(container_id, str) and container_id:
                return container_id
    container_id = statuses[0].get("id")
    if not isinstance(container_id, str) or not container_id:
        raise DeploymentError("agent returned a container without an id")
    return container_id


def _recipe_aliases(recipe: Recipe) -> set[str]:
    aliases: set[str] = set()
    if recipe.spec.gateway is not None:
        aliases.update(recipe.spec.gateway.aliases)
    if recipe.spec.model is not None:
        aliases.add(recipe.spec.model.served_name)
    return aliases


def _canonical(value: Any) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False)
