"""Controller HTTP API, SSE stream, gateway routes, and optional UI login."""

from __future__ import annotations

import json
import os
import re
import tempfile
import uuid
from collections.abc import AsyncIterator, Callable, Mapping
from dataclasses import asdict
from pathlib import Path
from typing import Any

from fastapi import Depends, FastAPI, HTTPException, Query, Request, Response
from fastapi.responses import JSONResponse, StreamingResponse

from spark_fleet.controller.agent_client import AgentClient, AgentError
from spark_fleet.controller.deployments import DeploymentError, DeploymentManager
from spark_fleet.controller.gateway import FleetGateway, GatewayError, filter_response_headers
from spark_fleet.controller.importer import (
    Importer,
    ImporterError,
    OldControllerClient,
)
from spark_fleet.controller.metrics import MetricsStore
from spark_fleet.controller.nodes import NodeError, NodeRegistry, NodeStatus
from spark_fleet.shared.db import Database
from spark_fleet.shared.events import EventBus
from spark_fleet.shared.jobs import JobRunner
from spark_fleet.shared.recipe import (
    NAME_PATTERN,
    REPO_PATTERN,
    Recipe,
    RecipeError,
    parse_recipe,
    parse_recipe_yaml,
    recipe_to_yaml,
    validate_against_nodes,
)
from spark_fleet.shared.render import NodeAddress, RenderError, render_deployment
from spark_fleet.shared.security import (
    SESSION_COOKIE_NAME,
    AuthError,
    SessionError,
    login_session,
    mutation_dependency,
    session_cookie_settings,
    ui_session_dependency,
    verify_session,
)

_NAME_RE = re.compile(NAME_PATTERN)
_REPO_RE = re.compile(REPO_PATTERN)


def build_app(
    *,
    db: Database,
    registry: NodeRegistry,
    deployments: DeploymentManager,
    gateway: FleetGateway,
    metrics_store: MetricsStore,
    runner: JobRunner,
    bus: EventBus,
    recipes_dir: Path,
    agent_token: str,
    session_secret: str | None = None,
    ui_password: str | None = None,
    client_factory: Callable[[str, str], AgentClient] | None = None,
    lifespan: Callable[[FastAPI], AsyncIterator[None]] | None = None,
) -> FastAPI:
    app = FastAPI(title="spark-fleet controller", lifespan=lifespan)
    recipes_dir = Path(recipes_dir)
    client_factory = client_factory or AgentClient
    session_dep = ui_session_dependency(session_secret)
    mutation_dep = mutation_dependency(session_secret)

    @app.get("/health")
    def health() -> dict[str, Any]:
        return {"status": "ok"}

    # Login ----------------------------------------------------------------

    @app.post("/api/login")
    def login(body: Mapping[str, Any], response: Response) -> dict[str, Any]:
        if ui_password is None or session_secret is None:
            raise HTTPException(status_code=400, detail="login is disabled")
        password = body.get("password")
        if not isinstance(password, str):
            raise HTTPException(status_code=400, detail="password is required")
        try:
            token = login_session(ui_password, session_secret, password)
            session = verify_session(session_secret, token)
        except AuthError as exc:
            raise HTTPException(status_code=401, detail=str(exc)) from exc
        response.set_cookie(value=token, **session_cookie_settings())
        return {"authenticated": True, "csrf": session.csrf}

    @app.post("/api/logout", dependencies=[Depends(mutation_dep)])
    def logout(response: Response) -> dict[str, Any]:
        response.delete_cookie(SESSION_COOKIE_NAME, path="/")
        return {"authenticated": False}

    @app.get("/api/session")
    def session_status(request: Request) -> dict[str, Any]:
        enabled = session_secret is not None
        authenticated = False
        csrf = None
        if enabled:
            token = request.cookies.get(SESSION_COOKIE_NAME)
            if token:
                try:
                    csrf = verify_session(session_secret, token).csrf
                    authenticated = True
                except SessionError:
                    authenticated = False
        return {"enabled": enabled, "authenticated": authenticated, "csrf": csrf}

    # Nodes ----------------------------------------------------------------

    @app.get("/api/nodes", dependencies=[Depends(session_dep)])
    def list_nodes() -> dict[str, Any]:
        return {"nodes": [_node_dict(node) for node in registry.list()]}

    @app.post("/api/nodes", dependencies=[Depends(mutation_dep)])
    def create_node(body: Mapping[str, Any]) -> dict[str, Any]:
        try:
            node = registry.create(
                str(body.get("id", "")),
                display_name=str(body.get("displayName", "")),
                lan_ip=str(body.get("lanIp", "")),
                fabric_ip=str(body.get("fabricIp", "")),
                agent_url=str(body.get("agentUrl", "")),
                enabled=bool(body.get("enabled", True)),
            )
        except NodeError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        return _node_dict(node)

    @app.put("/api/nodes/{node_id}", dependencies=[Depends(mutation_dep)])
    def update_node(node_id: str, body: Mapping[str, Any]) -> dict[str, Any]:
        try:
            node = registry.update(
                node_id,
                display_name=body.get("displayName"),
                lan_ip=body.get("lanIp"),
                fabric_ip=body.get("fabricIp"),
                agent_url=body.get("agentUrl"),
                enabled=body.get("enabled"),
            )
        except NodeError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        return _node_dict(node)

    @app.delete("/api/nodes/{node_id}", dependencies=[Depends(mutation_dep)])
    def delete_node(node_id: str) -> dict[str, Any]:
        try:
            registry.delete(node_id)
        except NodeError as exc:
            raise HTTPException(status_code=409, detail=str(exc)) from exc
        return {"deleted": node_id}

    @app.post("/api/nodes/{node_id}/check", dependencies=[Depends(mutation_dep)])
    async def check_node(node_id: str) -> dict[str, Any]:
        try:
            status = await registry.check(node_id)
        except NodeError as exc:
            raise HTTPException(status_code=404, detail=str(exc)) from exc
        return _node_status_dict(status)

    # Recipes --------------------------------------------------------------

    @app.get("/api/recipes", dependencies=[Depends(session_dep)])
    def list_recipes() -> dict[str, Any]:
        return {"recipes": _list_recipes(recipes_dir)}

    @app.get("/api/recipes/{name}", dependencies=[Depends(session_dep)])
    def get_recipe(name: str) -> dict[str, Any]:
        recipe = _read_recipe(recipes_dir, name)
        return {"name": name, "yaml": recipe_to_yaml(recipe)}

    @app.put("/api/recipes/{name}", dependencies=[Depends(mutation_dep)])
    def put_recipe(name: str, body: Mapping[str, Any]) -> dict[str, Any]:
        recipe = _parse_body(body)
        if recipe.metadata.name != name:
            raise HTTPException(status_code=400, detail="recipe name does not match path")
        _validate_recipe(recipe, recipes_dir, exclude=name, registry=registry)
        _write_recipe(recipes_dir, recipe)
        return {"name": name, "yaml": recipe_to_yaml(recipe)}

    @app.delete("/api/recipes/{name}", dependencies=[Depends(mutation_dep)])
    def delete_recipe(name: str) -> dict[str, Any]:
        path = _recipe_path(recipes_dir, name)
        if not path.is_file():
            raise HTTPException(status_code=404, detail=f"recipe {name!r} not found")
        if any(
            deployment["recipe_name"] == name and deployment["status"] == "loaded"
            for deployment in db.list_deployments()
        ):
            raise HTTPException(status_code=409, detail="recipe is used by a loaded deployment")
        path.unlink()
        return {"deleted": name}

    @app.post("/api/recipes/validate", dependencies=[Depends(mutation_dep)])
    def validate_recipe(body: Mapping[str, Any]) -> dict[str, Any]:
        recipe = _parse_body(body)
        _validate_recipe(recipe, recipes_dir, exclude=recipe.metadata.name, registry=registry)
        try:
            rendered = render_deployment(
                recipe,
                recipe.metadata.name,
                "preview",
                _node_addresses(registry),
            )
        except RenderError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        return {
            "recipe": recipe.model_dump(by_alias=True),
            "yaml": recipe_to_yaml(recipe),
            "rendered": [
                {
                    "role": item.role,
                    "node": item.node,
                    "containerName": item.container_name,
                    "configHash": item.config_hash,
                    "compose": item.compose,
                }
                for item in rendered
            ],
        }

    @app.post("/api/recipes/{name}/swap-model", dependencies=[Depends(mutation_dep)])
    def swap_model(name: str, body: Mapping[str, Any]) -> dict[str, Any]:
        recipe = _read_recipe(recipes_dir, name)
        if recipe.spec.model is None:
            raise HTTPException(status_code=400, detail="recipe has no model")
        repo = body.get("repo")
        served = body.get("servedName")
        if not isinstance(repo, str) or not _REPO_RE.fullmatch(repo):
            raise HTTPException(status_code=400, detail="invalid repo")
        if not isinstance(served, str):
            raise HTTPException(status_code=400, detail="servedName is required")
        data = recipe.model_dump(by_alias=True, exclude_unset=True)
        data["spec"]["model"]["repo"] = repo
        data["spec"]["model"]["servedName"] = served
        if "revision" in body:
            data["spec"]["model"]["revision"] = body["revision"]
        updated = parse_recipe(data)
        _validate_recipe(updated, recipes_dir, exclude=name, registry=registry)
        diff = _diff(recipe_to_yaml(recipe), recipe_to_yaml(updated), name)
        if body.get("apply") is True:
            _write_recipe(recipes_dir, updated)
        return {"name": name, "yaml": recipe_to_yaml(updated), "diff": diff}

    # Deployments ----------------------------------------------------------

    @app.get("/api/deployments", dependencies=[Depends(session_dep)])
    def list_deployments() -> dict[str, Any]:
        return {"deployments": _deployment_dicts(db, recipes_dir)}

    @app.post("/api/deployments", dependencies=[Depends(mutation_dep)])
    def create_deployment(body: Mapping[str, Any]) -> dict[str, Any]:
        recipe_name = body.get("recipe")
        if not isinstance(recipe_name, str):
            raise HTTPException(status_code=400, detail="recipe is required")
        recipe = _read_recipe(recipes_dir, recipe_name)
        _validate_recipe(recipe, recipes_dir, exclude=recipe_name, registry=registry)
        name = body.get("name") or recipe.metadata.name
        if not isinstance(name, str) or not _NAME_RE.fullmatch(name):
            raise HTTPException(status_code=400, detail="invalid deployment name")
        if db.get_deployment_by_name(name) is not None:
            raise HTTPException(status_code=409, detail=f"deployment {name!r} already exists")
        deployment_id = uuid.uuid4().hex
        db.create_deployment(
            deployment_id,
            name,
            recipe_name,
            recipe.spec.mode,
            recipe.model_dump(by_alias=True, exclude_unset=True),
            status="pending",
        )
        return _deployment_dict(db, deployment_id, recipes_dir)

    @app.get("/api/deployments/{deployment_id}", dependencies=[Depends(session_dep)])
    def get_deployment(deployment_id: str) -> dict[str, Any]:
        return _deployment_dict(db, deployment_id, recipes_dir)

    @app.delete("/api/deployments/{deployment_id}", dependencies=[Depends(mutation_dep)])
    def delete_deployment(deployment_id: str) -> dict[str, Any]:
        deployment = db.get_deployment(deployment_id)
        if deployment is None:
            raise HTTPException(status_code=404, detail="deployment not found")
        if deployment["status"] == "loaded":
            raise HTTPException(status_code=409, detail="unload before deleting")
        active = [
            job
            for job in db.list_jobs_by_status(("pending", "running"))
            if job.get("deployment_id") == deployment_id
        ]
        if active:
            raise HTTPException(
                status_code=409,
                detail="deployment has a pending or running job",
            )
        db.delete_deployment(deployment_id)
        return {"deleted": deployment_id}

    @app.post("/api/deployments/{deployment_id}/load", dependencies=[Depends(mutation_dep)])
    async def load_deployment(
        deployment_id: str, body: Mapping[str, Any] | None = None
    ) -> dict[str, Any]:
        try:
            job_id = await deployments.load(
                deployment_id, primary=bool((body or {}).get("primary"))
            )
        except DeploymentError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        return {"job_id": job_id}

    @app.post("/api/deployments/{deployment_id}/unload", dependencies=[Depends(mutation_dep)])
    async def unload_deployment(deployment_id: str) -> dict[str, Any]:
        try:
            job_id = await deployments.unload(deployment_id)
        except DeploymentError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        return {"job_id": job_id}

    @app.post("/api/deployments/{deployment_id}/redeploy", dependencies=[Depends(mutation_dep)])
    async def redeploy_deployment(
        deployment_id: str, body: Mapping[str, Any] | None = None
    ) -> dict[str, Any]:
        deployment = db.get_deployment(deployment_id)
        if deployment is None:
            raise HTTPException(status_code=404, detail="deployment not found")
        recipe = _deployment_recipe(deployment, recipes_dir)
        try:
            job_id = await deployments.redeploy(
                deployment_id,
                primary=bool((body or {}).get("primary")),
                recipe=recipe,
            )
        except DeploymentError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        return {"job_id": job_id}

    @app.post("/api/deployments/{deployment_id}/primary", dependencies=[Depends(mutation_dep)])
    def set_primary(deployment_id: str) -> dict[str, Any]:
        deployment = db.get_deployment(deployment_id)
        if deployment is None:
            raise HTTPException(status_code=404, detail="deployment not found")
        if deployment["status"] != "loaded":
            raise HTTPException(status_code=409, detail="deployment is not loaded")
        if deployment["mode"] != "deployment":
            raise HTTPException(status_code=409, detail="only deployment recipes can be primary")
        db.set_primary(deployment_id)
        return {"primary": deployment_id}

    # Models ---------------------------------------------------------------

    @app.get("/api/models", dependencies=[Depends(session_dep)])
    async def list_models() -> dict[str, Any]:
        nodes = []
        for node in registry.list():
            client = client_factory(node.agent_url, agent_token)
            try:
                catalog = await client.models()
                nodes.append({"node_id": node.id, "models": catalog})
            except AgentError as exc:
                nodes.append({"node_id": node.id, "error": str(exc)})
            finally:
                await client.aclose()
        return {"nodes": nodes}

    @app.post("/api/models/download", dependencies=[Depends(mutation_dep)])
    async def download_model(body: Mapping[str, Any]) -> dict[str, Any]:
        repo = body.get("repo")
        if not isinstance(repo, str) or not _REPO_RE.fullmatch(repo):
            raise HTTPException(status_code=400, detail="invalid repo")
        node_ids = body.get("nodes")
        if not isinstance(node_ids, list) or not node_ids:
            raise HTTPException(status_code=400, detail="nodes is required")
        jobs: list[dict[str, str]] = []
        for node_id in node_ids:
            try:
                node = registry.require(str(node_id))
            except NodeError as exc:
                raise HTTPException(status_code=404, detail=str(exc)) from exc
            client = client_factory(node.agent_url, agent_token)
            try:
                job_id = await client.start_model_download(
                    repo,
                    root=body.get("root"),
                    revision=body.get("revision"),
                    allow_patterns=body.get("allowPatterns"),
                )
                jobs.append({"node_id": node.id, "job_id": job_id})
            except AgentError as exc:
                raise HTTPException(status_code=502, detail=str(exc)) from exc
            finally:
                await client.aclose()
        return {"jobs": jobs}

    @app.delete("/api/models/{repo:path}", dependencies=[Depends(mutation_dep)])
    async def remove_model(
        repo: str,
        node_id: str,
        root: str | None = None,
        confirm: bool = False,
    ) -> dict[str, Any]:
        if not _REPO_RE.fullmatch(repo):
            raise HTTPException(status_code=400, detail="invalid repo")
        if not confirm:
            raise HTTPException(status_code=400, detail="explicit confirmation required")
        in_use = _in_use_repos(db)
        try:
            node = registry.require(node_id)
        except NodeError as exc:
            raise HTTPException(status_code=404, detail=str(exc)) from exc
        client = client_factory(node.agent_url, agent_token)
        try:
            await client.remove_model(repo, root=root, confirm=True, in_use=sorted(in_use))
        except AgentError as exc:
            raise HTTPException(status_code=502, detail=str(exc)) from exc
        finally:
            await client.aclose()
        return {"removed": repo, "node_id": node.id}

    # Jobs -----------------------------------------------------------------

    @app.get("/api/jobs", dependencies=[Depends(session_dep)])
    def list_jobs(limit: int = Query(100, ge=1, le=1000)) -> dict[str, Any]:
        return {"jobs": db.list_jobs(limit)}

    @app.get("/api/jobs/{job_id}", dependencies=[Depends(session_dep)])
    def get_job(job_id: str) -> dict[str, Any]:
        job = db.get_job(job_id)
        if job is None:
            raise HTTPException(status_code=404, detail="job not found")
        return {"job": job, "events": db.list_job_events(job_id)}

    @app.post("/api/jobs/{job_id}/cancel", dependencies=[Depends(mutation_dep)])
    async def cancel_job(job_id: str) -> dict[str, Any]:
        if db.get_job(job_id) is None:
            raise HTTPException(status_code=404, detail="job not found")
        return {"cancelled": await runner.cancel(job_id)}

    # Metrics --------------------------------------------------------------

    @app.get("/api/metrics/nodes/{node_id}", dependencies=[Depends(session_dep)])
    def node_metrics(
        node_id: str,
        since: float | None = None,
        until: float | None = None,
        limit: int = Query(1000, ge=1, le=10_000),
    ) -> dict[str, Any]:
        latest = metrics_store.latest_node_metrics(node_id)
        series = {
            metric: metrics_store.node_series(
                node_id, metric, since=since, until=until, limit=limit
            )
            for metric in latest
        }
        return {"latest": latest, "series": series}

    @app.get("/api/metrics/inference/{deployment_id}", dependencies=[Depends(session_dep)])
    def inference_metrics(
        deployment_id: str,
        since: float | None = None,
        until: float | None = None,
    ) -> dict[str, Any]:
        totals = metrics_store.inference_totals(deployment_id, since=since, until=until)
        return {
            "totals": asdict(totals),
            "series": metrics_store.inference_series(deployment_id, since=since, until=until),
        }

    # Settings -------------------------------------------------------------

    @app.get("/api/settings", dependencies=[Depends(session_dep)])
    def get_settings() -> dict[str, Any]:
        return {"settings": db.list_settings()}

    @app.put("/api/settings/{key}", dependencies=[Depends(mutation_dep)])
    def put_setting(key: str, body: Mapping[str, Any]) -> dict[str, Any]:
        value = body.get("value")
        if not isinstance(value, str):
            raise HTTPException(status_code=400, detail="value must be a string")
        db.set_setting(key, value)
        return {"key": key, "value": value}

    # Import ---------------------------------------------------------------

    @app.post("/api/import/plan", dependencies=[Depends(mutation_dep)])
    async def import_plan(body: Mapping[str, Any]) -> dict[str, Any]:
        sources = body.get("sources")
        if not isinstance(sources, list) or not sources:
            raise HTTPException(status_code=400, detail="sources is required")
        clients: dict[str, tuple[str, OldControllerClient]] = {}
        try:
            for source in sources:
                if not isinstance(source, Mapping):
                    raise HTTPException(status_code=400, detail="invalid source")
                node_id = source.get("nodeId")
                url = source.get("url")
                if not isinstance(node_id, str) or not isinstance(url, str):
                    raise HTTPException(status_code=400, detail="invalid source")
                try:
                    registry.require(node_id)
                except NodeError as exc:
                    raise HTTPException(status_code=404, detail=str(exc)) from exc
                previous = clients.get(node_id)
                if previous is not None:
                    await previous[1].aclose()
                try:
                    clients[node_id] = (url, OldControllerClient(url))
                except ValueError as exc:
                    raise HTTPException(status_code=400, detail=str(exc)) from exc
            plan = await Importer(_node_addresses(registry)).plan(clients)
        except ImporterError as exc:
            raise HTTPException(status_code=502, detail=str(exc)) from exc
        finally:
            for _url, client in clients.values():
                await client.aclose()
        return _plan_dict(plan)

    @app.post("/api/import/apply", dependencies=[Depends(mutation_dep)])
    async def import_apply(body: Mapping[str, Any]) -> dict[str, Any]:
        if body.get("confirm") is not True:
            raise HTTPException(status_code=400, detail="explicit confirmation required")
        sources = body.get("sources")
        if not isinstance(sources, list) or not sources:
            raise HTTPException(status_code=400, detail="sources is required")
        clients: dict[str, tuple[str, OldControllerClient]] = {}
        try:
            for source in sources:
                if not isinstance(source, Mapping):
                    raise HTTPException(status_code=400, detail="invalid source")
                node_id = source.get("nodeId")
                url = source.get("url")
                if not isinstance(node_id, str) or not isinstance(url, str):
                    raise HTTPException(status_code=400, detail="invalid source")
                try:
                    registry.require(node_id)
                except NodeError as exc:
                    raise HTTPException(status_code=404, detail=str(exc)) from exc
                previous = clients.get(node_id)
                if previous is not None:
                    await previous[1].aclose()
                try:
                    clients[node_id] = (url, OldControllerClient(url))
                except ValueError as exc:
                    raise HTTPException(status_code=400, detail=str(exc)) from exc
            plan = await Importer(_node_addresses(registry)).plan(clients)
            created = plan.apply(db, confirm=True, recipes_dir=recipes_dir)
        except ImporterError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        finally:
            for _url, client in clients.values():
                await client.aclose()
        return {"created": created}

    # Gateway --------------------------------------------------------------

    @app.get("/v1/models")
    def gateway_models() -> dict[str, Any]:
        try:
            return gateway.models()
        except GatewayError as exc:
            return JSONResponse({"detail": str(exc)}, status_code=exc.status_code)

    @app.api_route("/v1/{path:path}", methods=["POST"])
    async def gateway_proxy(path: str, request: Request) -> Response:
        body = await request.body()
        headers = dict(request.headers)
        try:
            stream_requested = False
            if body:
                try:
                    payload = json.loads(body)
                except ValueError:
                    payload = None
                if isinstance(payload, dict):
                    stream_requested = bool(payload.get("stream"))
                    model = payload.get("model")
                else:
                    model = None
            else:
                model = None
            if not isinstance(model, str):
                raise HTTPException(status_code=400, detail="model is required")
            target = f"/v1/{path}"
            if request.url.query:
                target = f"{target}?{request.url.query}"
            if stream_requested:
                stream = await gateway.stream(model, request.method, target, headers, body)
                return StreamingResponse(
                    stream.aiter_bytes(),
                    status_code=stream.status_code,
                    headers=filter_response_headers(stream.headers),
                )
            response = await gateway.proxy(model, request.method, target, headers, body)
            return Response(
                content=response.content,
                status_code=response.status_code,
                headers=filter_response_headers(response.headers),
            )
        except GatewayError as exc:
            headers_out = {}
            if exc.retry_after is not None:
                headers_out["Retry-After"] = str(exc.retry_after)
            return JSONResponse(
                {"detail": str(exc)}, status_code=exc.status_code, headers=headers_out
            )

    # Events ---------------------------------------------------------------

    @app.get("/events", dependencies=[Depends(session_dep)])
    async def events() -> StreamingResponse:
        return StreamingResponse(
            bus.sse_stream(),
            media_type="text/event-stream",
            headers={"Cache-Control": "no-cache"},
        )

    return app


def _node_dict(node) -> dict[str, Any]:
    return asdict(node)


def _node_status_dict(status: NodeStatus) -> dict[str, Any]:
    return {
        "node": asdict(status.node),
        "healthy": status.healthy,
        "health": status.health,
        "info": status.info,
        "error": status.error,
    }


def _recipe_path(recipes_dir: Path, name: str) -> Path:
    if not _NAME_RE.fullmatch(name):
        raise HTTPException(status_code=400, detail="invalid recipe name")
    return recipes_dir / f"{name}.yaml"


def _read_recipe(recipes_dir: Path, name: str) -> Recipe:
    path = _recipe_path(recipes_dir, name)
    try:
        return parse_recipe_yaml(path.read_text(encoding="utf-8"))
    except OSError as exc:
        raise HTTPException(status_code=404, detail=f"recipe {name!r} not found") from exc
    except UnicodeDecodeError as exc:
        raise HTTPException(status_code=400, detail=f"recipe {name!r} is not valid UTF-8") from exc
    except RecipeError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


def _write_recipe(recipes_dir: Path, recipe: Recipe) -> None:
    recipes_dir.mkdir(parents=True, exist_ok=True)
    path = _recipe_path(recipes_dir, recipe.metadata.name)
    temporary: Path | None = None
    try:
        with tempfile.NamedTemporaryFile(
            mode="w", dir=recipes_dir, suffix=".tmp", delete=False, encoding="utf-8"
        ) as handle:
            temporary = Path(handle.name)
            handle.write(recipe_to_yaml(recipe))
        os.replace(temporary, path)
    finally:
        if temporary is not None and temporary.exists():
            temporary.unlink(missing_ok=True)


def _parse_body(body: Mapping[str, Any]) -> Recipe:
    if "yaml" in body:
        text = body["yaml"]
        if not isinstance(text, str):
            raise HTTPException(status_code=400, detail="yaml must be a string")
        try:
            return parse_recipe_yaml(text)
        except RecipeError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
    try:
        return parse_recipe(dict(body))
    except RecipeError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


def _list_recipes(recipes_dir: Path) -> list[dict[str, Any]]:
    result = []
    if not recipes_dir.is_dir():
        return result
    for path in sorted(recipes_dir.glob("*.yaml")):
        try:
            recipe = parse_recipe_yaml(path.read_text(encoding="utf-8"))
        except (OSError, RecipeError) as exc:
            result.append({"name": path.stem, "error": str(exc)})
            continue
        result.append(
            {
                "name": recipe.metadata.name,
                "mode": recipe.spec.mode,
                "runtime": recipe.spec.runtime,
                "roles": len(recipe.spec.roles),
                "servedName": (recipe.spec.model.served_name if recipe.spec.model else None),
            }
        )
    return result


def _validate_recipe(
    recipe: Recipe, recipes_dir: Path, *, exclude: str | None, registry: NodeRegistry
) -> None:
    try:
        validate_against_nodes(recipe, [node.id for node in registry.list()])
    except RecipeError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    aliases = _recipe_aliases(recipe)
    if "spark-active" in aliases:
        raise HTTPException(status_code=400, detail="spark-active is reserved")
    if not aliases:
        return
    for path in recipes_dir.glob("*.yaml") if recipes_dir.is_dir() else ():
        if path.stem == exclude:
            continue
        try:
            other = parse_recipe_yaml(path.read_text(encoding="utf-8"))
        except (OSError, RecipeError):
            continue
        overlap = aliases & _recipe_aliases(other)
        if overlap:
            raise HTTPException(
                status_code=409,
                detail=f"aliases already used by {path.stem}: {', '.join(sorted(overlap))}",
            )


def _recipe_aliases(recipe: Recipe) -> set[str]:
    aliases: set[str] = set()
    if recipe.spec.gateway is not None:
        aliases.update(recipe.spec.gateway.aliases)
    if recipe.spec.model is not None:
        aliases.add(recipe.spec.model.served_name)
    return aliases


def _diff(old: str, new: str, name: str) -> str:
    import difflib

    return "\n".join(
        difflib.unified_diff(
            old.splitlines(),
            new.splitlines(),
            fromfile=f"{name}.old.yaml",
            tofile=f"{name}.new.yaml",
            lineterm="",
        )
    )


def _deployment_recipe(deployment: Mapping[str, Any], recipes_dir: Path) -> Recipe:
    try:
        return _read_recipe(recipes_dir, deployment["recipe_name"])
    except HTTPException as exc:
        if exc.status_code not in (400, 404):
            raise
    try:
        return parse_recipe(deployment["recipe"])
    except RecipeError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


def _deployment_dict(db: Database, deployment_id: str, recipes_dir: Path) -> dict[str, Any]:
    deployment = db.get_deployment(deployment_id)
    if deployment is None:
        raise HTTPException(status_code=404, detail="deployment not found")
    return {
        "deployment": deployment,
        "roles": db.list_roles(deployment_id),
        "stale": _is_stale(deployment, recipes_dir),
    }


def _deployment_dicts(db: Database, recipes_dir: Path) -> list[dict[str, Any]]:
    return [
        {
            "deployment": deployment,
            "roles": db.list_roles(deployment["id"]),
            "stale": _is_stale(deployment, recipes_dir),
        }
        for deployment in db.list_deployments()
    ]


def _is_stale(deployment: Mapping[str, Any], recipes_dir: Path) -> bool:
    if deployment["status"] != "loaded":
        return False
    try:
        path = _recipe_path(recipes_dir, deployment["recipe_name"])
        recipe = parse_recipe_yaml(path.read_text(encoding="utf-8"))
    except (HTTPException, OSError, UnicodeDecodeError, RecipeError):
        return False
    current = recipe.model_dump(by_alias=True, exclude_unset=True)
    return json.dumps(deployment["recipe"], sort_keys=True, default=str) != json.dumps(
        current, sort_keys=True, default=str
    )


def _in_use_repos(db: Database) -> set[str]:
    repos: set[str] = set()
    for deployment in db.list_deployments():
        if deployment["status"] != "loaded":
            continue
        try:
            recipe = parse_recipe(deployment["recipe"])
        except RecipeError:
            continue
        if recipe.spec.model is not None:
            repos.add(recipe.spec.model.repo)
            repos.update(recipe.spec.model.extra_repos)
    return repos


def _node_addresses(registry: NodeRegistry) -> dict[str, NodeAddress]:
    return {
        node.id: NodeAddress(lan_ip=node.lan_ip, fabric_ip=node.fabric_ip)
        for node in registry.list()
    }


def _plan_dict(plan) -> dict[str, Any]:
    return {
        "proposals": [
            {
                "name": proposal.name,
                "recipe": proposal.recipe.model_dump(by_alias=True),
                "yaml": recipe_to_yaml(proposal.recipe),
                "diff": proposal.diff,
            }
            for proposal in plan.proposals
        ],
        "skipped": [{"name": item.name, "reason": item.reason} for item in plan.skipped],
    }
