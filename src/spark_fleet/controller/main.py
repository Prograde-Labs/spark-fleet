"""Controller runtime: configuration, storage, pollers, reconciliation, and web.

``create_controller`` wires the database, event bus, job runner, node registry,
deployment manager, gateway, and metrics store into one FastAPI application with
a lifespan that starts the job runner, reconciles loaded deployments against
agent container state, and runs the node-health and metrics pollers until
shutdown. The built Vue SPA is mounted at ``/`` when ``SPARK_FLEET_WEB_DIST`` is
configured; otherwise ``/`` returns a small JSON identity document.
"""

from __future__ import annotations

import asyncio
import contextlib
import logging
from collections.abc import Awaitable, Callable, Mapping
from dataclasses import dataclass, field, replace
from typing import Any

import uvicorn
from fastapi import FastAPI
from fastapi.staticfiles import StaticFiles

import spark_fleet
from spark_fleet.controller.agent_client import AgentClient, AgentError
from spark_fleet.controller.api import build_app
from spark_fleet.controller.deployments import DeploymentManager, project_name
from spark_fleet.controller.gateway import FleetGateway
from spark_fleet.controller.metrics import MetricsStore
from spark_fleet.controller.nodes import NodeRegistry, NodeStatus
from spark_fleet.shared.config import ConfigError, ControllerConfig, load_controller_config
from spark_fleet.shared.db import Database, DatabaseError
from spark_fleet.shared.events import Event, EventBus
from spark_fleet.shared.jobs import JobRunner
from spark_fleet.shared.recipe import RecipeError, parse_recipe

log = logging.getLogger(__name__)

ClientFactory = Callable[[str, str], AgentClient]


@dataclass
class Controller:
    """Owns the controller components and their background work."""

    config: ControllerConfig
    db: Database
    bus: EventBus
    runner: JobRunner
    registry: NodeRegistry
    deployments: DeploymentManager
    gateway: FleetGateway
    metrics: MetricsStore
    app: FastAPI
    client_factory: ClientFactory
    tasks: list[asyncio.Task] = field(default_factory=list)
    _started: bool = False

    async def start(self) -> None:
        if self._started:
            return
        await self.runner.start()
        await self.reconcile()
        self.tasks.append(
            asyncio.create_task(
                self._loop(
                    "node health", self.config.node_poll_interval_seconds, self.poll_nodes_once
                ),
                name="node-poller",
            )
        )
        self.tasks.append(
            asyncio.create_task(
                self._loop(
                    "metrics",
                    self.config.metrics_sample_interval_seconds,
                    self.poll_metrics_once,
                ),
                name="metrics-poller",
            )
        )
        self._started = True

    async def stop(self) -> None:
        if not self._started:
            return
        self._started = False
        for task in self.tasks:
            task.cancel()
        await asyncio.gather(*self.tasks, return_exceptions=True)
        self.tasks.clear()
        await self.runner.stop()
        await self.gateway.aclose()
        await self.bus.close()
        self.db.close()

    async def _loop(self, label: str, interval: float, fn: Callable[[], Awaitable[Any]]) -> None:
        while True:
            try:
                await fn()
            except asyncio.CancelledError:
                raise
            except Exception:  # noqa: BLE001 - a poller must not die on one bad sample
                log.exception("%s poll failed", label)
            await asyncio.sleep(interval)

    async def poll_nodes_once(self) -> list[NodeStatus]:
        statuses = await self.registry.refresh_all()
        for status in statuses:
            await self.bus.publish(
                Event(
                    "node.health",
                    {
                        "node_id": status.node.id,
                        "healthy": status.healthy,
                        "error": status.error,
                    },
                )
            )
        return statuses

    async def poll_metrics_once(self) -> dict[str, str]:
        errors: dict[str, str] = {}
        for node in self.registry.list():
            if not node.enabled:
                continue
            client = self.client_factory(node.agent_url, self.config.agent_token)
            try:
                snapshot = await client.metrics()
            except AgentError as exc:
                errors[node.id] = str(exc)
                continue
            finally:
                await client.aclose()
            try:
                self.metrics.record_node_snapshot(node.id, snapshot)
            except (ValueError, TypeError) as exc:
                errors[node.id] = str(exc)
                continue
            await self.bus.publish(
                Event(
                    "metrics.sample",
                    {
                        "node_id": node.id,
                        "sampled_at": snapshot.get("sampled_at"),
                        "host_available_bytes": snapshot.get("host_available_bytes"),
                        "cache_bytes": snapshot.get("cache_bytes"),
                    },
                )
            )
        self.gateway.counters.flush(self.metrics)
        try:
            self.metrics.maintenance(self.config.metrics_retention_seconds)
        except DatabaseError as exc:
            log.warning("metrics maintenance failed: %s", exc)
        return errors

    async def reconcile(self) -> list[dict[str, Any]]:
        """Compare loaded deployments with the containers agents report."""

        results: list[dict[str, Any]] = []
        for deployment in self.db.list_deployments():
            if deployment["status"] != "loaded":
                continue
            try:
                results.append(await self._reconcile_deployment(deployment))
            except Exception as exc:  # noqa: BLE001 - one bad deployment must not stop startup
                log.exception("reconciliation failed for %s", deployment["id"])
                results.append(
                    {
                        "deployment_id": deployment["id"],
                        "name": deployment["name"],
                        "status": "unknown",
                        "problems": [str(exc)],
                    }
                )
        return results

    async def _reconcile_deployment(self, deployment: Mapping[str, Any]) -> dict[str, Any]:
        try:
            recipe = parse_recipe(deployment["recipe"])
        except RecipeError as exc:
            return await self._fail(
                deployment,
                [f"recipe is invalid: {exc}"],
                reason="invalid recipe",
            )

        roles = {row["role_name"]: row for row in self.db.list_roles(deployment["id"])}
        definite: list[str] = []
        unreachable_problems: list[str] = []
        refreshed: dict[str, Mapping[str, Any]] = {}

        for role in recipe.spec.roles:
            row = roles.get(role.name)
            node = self.registry.get(role.node)
            if node is None:
                definite.append(f"{role.name}: node {role.node} is not registered")
                continue
            client = self.client_factory(node.agent_url, self.config.agent_token)
            try:
                statuses = await client.compose_status(project_name(deployment["name"], role.name))
            except AgentError as exc:
                unreachable_problems.append(f"{role.name}: status unavailable ({exc})")
                continue
            finally:
                await client.aclose()
            match = _match_container(
                statuses,
                name=f"spark-fleet-{deployment['name']}-{role.name}",
                container_id=row.get("container_id") if row else None,
            )
            if match is None or match.get("state") != "running":
                definite.append(f"{role.name}: container is not running")
                continue
            refreshed[role.name] = match

        if definite:
            return await self._fail(
                deployment, definite + unreachable_problems, reason="reconciliation"
            )

        if unreachable_problems:
            # An unreachable agent does not prove the workload is down. Keep the
            # stored state and surface the uncertainty instead of failing it.
            await self._publish(deployment, "unknown", unreachable_problems)
            return {
                "deployment_id": deployment["id"],
                "name": deployment["name"],
                "status": "unknown",
                "problems": unreachable_problems,
            }

        for role in recipe.spec.roles:
            row = roles.get(role.name)
            match = refreshed[role.name]
            if row is None:
                continue
            container_id = match.get("id")
            self.db.upsert_role(
                deployment["id"],
                role.name,
                role.node,
                role.class_,
                role.start_order,
                status="running",
                container_id=container_id if isinstance(container_id, str) else None,
            )
        await self._publish(deployment, "ok", [])
        return {
            "deployment_id": deployment["id"],
            "name": deployment["name"],
            "status": "ok",
            "problems": [],
        }

    async def _fail(
        self,
        deployment: Mapping[str, Any],
        problems: list[str],
        *,
        reason: str,
    ) -> dict[str, Any]:
        message = f"{reason}: " + "; ".join(problems)
        self.db.update_deployment(deployment["id"], status="failed", error=message)
        for role in self.db.list_roles(deployment["id"]):
            self.db.upsert_role(
                deployment["id"],
                role["role_name"],
                role["node_id"],
                role["class"],
                role["start_order"],
                status="stopped",
            )
        if deployment["is_primary"]:
            self.db.clear_primary()
        await self._publish(deployment, "failed", problems)
        return {
            "deployment_id": deployment["id"],
            "name": deployment["name"],
            "status": "failed",
            "problems": problems,
        }

    async def _publish(
        self, deployment: Mapping[str, Any], status: str, problems: list[str]
    ) -> None:
        await self.bus.publish(
            Event(
                "deployment.reconciled",
                {
                    "deployment_id": deployment["id"],
                    "name": deployment["name"],
                    "status": status,
                    "problems": list(problems),
                },
            )
        )


def _match_container(
    statuses: list[dict[str, Any]],
    *,
    name: str,
    container_id: str | None,
) -> Mapping[str, Any] | None:
    for status in statuses:
        if status.get("name") == name:
            return status
    if container_id:
        for status in statuses:
            if status.get("id") == container_id:
                return status
    return None


def create_controller(
    config: ControllerConfig,
    *,
    client_factory: ClientFactory | None = None,
) -> Controller:
    """Wire a controller from validated configuration."""

    config.data_dir.mkdir(parents=True, exist_ok=True)
    config.recipes_dir.mkdir(parents=True, exist_ok=True)
    if config.web_dist is not None and not config.web_dist.is_dir():
        raise ConfigError(f"SPARK_FLEET_WEB_DIST is not a directory: {config.web_dist}")

    db = Database(config.db_path)
    effective = _apply_stored_settings(config, db)
    bus = EventBus()
    runner = JobRunner(db, bus)
    registry = NodeRegistry(db, config.agent_token, client_factory=client_factory)
    deployments = DeploymentManager(
        db,
        registry,
        runner,
        token=config.agent_token,
        rollback_policy=effective.rollback_policy,
        client_factory=client_factory,
    )
    gateway = FleetGateway(db)
    metrics = MetricsStore(db)
    factory: ClientFactory = client_factory or AgentClient

    controller: Controller | None = None

    @contextlib.asynccontextmanager
    async def lifespan(_app: FastAPI):
        if controller is None:
            raise RuntimeError("controller was not initialized")
        await controller.start()
        try:
            yield
        finally:
            await controller.stop()

    app = build_app(
        db=db,
        registry=registry,
        deployments=deployments,
        gateway=gateway,
        metrics_store=metrics,
        runner=runner,
        bus=bus,
        recipes_dir=config.recipes_dir,
        agent_token=config.agent_token,
        session_secret=config.session_secret,
        ui_password=config.ui_password,
        client_factory=factory,
        lifespan=lifespan,
        rollback_policy=effective.rollback_policy,
        metrics_retention_seconds=effective.metrics_retention_seconds,
        metrics_sample_interval_seconds=effective.metrics_sample_interval_seconds,
        node_poll_interval_seconds=effective.node_poll_interval_seconds,
    )

    if config.web_dist is not None:
        app.mount("/", StaticFiles(directory=config.web_dist, html=True), name="web")
    else:

        @app.get("/")
        def identity() -> dict[str, str]:
            return {"service": "spark-fleet", "version": spark_fleet.__version__}

    controller = Controller(
        config=effective,
        db=db,
        bus=bus,
        runner=runner,
        registry=registry,
        deployments=deployments,
        gateway=gateway,
        metrics=metrics,
        app=app,
        client_factory=factory,
    )
    return controller


def _apply_stored_settings(config: ControllerConfig, db: Database) -> ControllerConfig:
    """Overlay persisted operator settings on the environment config.

    Settings are read once at startup; a controller restart applies changes.
    Invalid stored values fall back to the environment defaults.
    """

    settings = db.list_settings()

    rollback = settings.get("rollback_policy", config.rollback_policy)
    rollback = str(rollback).strip().lower().replace("_", "-")
    if rollback not in ("restore", "leave-down"):
        rollback = config.rollback_policy

    def integer(key: str, default: int, minimum: int, maximum: int) -> int:
        raw = settings.get(key)
        if raw is None:
            return default
        try:
            value = int(raw)
        except (TypeError, ValueError):
            return default
        return value if minimum <= value <= maximum else default

    return replace(
        config,
        rollback_policy=rollback,
        metrics_retention_seconds=integer(
            "metrics_retention_seconds", config.metrics_retention_seconds, 60, 30 * 86400
        ),
        metrics_sample_interval_seconds=integer(
            "metrics_sample_interval_seconds",
            config.metrics_sample_interval_seconds,
            1,
            3600,
        ),
        node_poll_interval_seconds=integer(
            "node_poll_interval_seconds", config.node_poll_interval_seconds, 1, 3600
        ),
    )


def main() -> None:
    config = load_controller_config()
    logging.basicConfig(
        level=getattr(logging, config.log_level, logging.INFO),
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    controller = create_controller(config)
    uvicorn.run(
        controller.app,
        host=config.host,
        port=config.port,
        log_level=config.log_level.lower(),
    )
