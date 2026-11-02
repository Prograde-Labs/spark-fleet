"""FastAPI application for a node agent.

``/health`` stays public so container and controller liveness probes work without
the shared secret; every operational endpoint (Compose, containers, models,
prepare jobs, and metrics) requires the bearer token configured in
``SPARK_FLEET_AGENT_TOKEN``. Errors are reported as ``{"detail": ...}`` with a
400 for invalid input or a 502 when Docker or the filesystem fails.
"""

from __future__ import annotations

import platform
import socket
from collections.abc import Mapping
from dataclasses import asdict
from pathlib import Path
from typing import Any

from fastapi import Depends, FastAPI, HTTPException, Query
from fastapi.responses import StreamingResponse

import spark_fleet
from spark_fleet.agent.compose import (
    PROJECT_PATTERN,
    ComposeError,
    ProjectSpec,
    down_project,
    status_project,
    up_project,
    validate_project,
)
from spark_fleet.agent.containers import (
    ContainerError,
    container_logs,
    follow_container_logs,
    managed_containers,
)
from spark_fleet.agent.jobs import JobError, JobManager
from spark_fleet.agent.metrics import MetricsError, collect_metrics, snapshot_to_dict
from spark_fleet.agent.models import ModelCatalog, ModelError, ModelJobManager
from spark_fleet.shared.config import AgentConfig
from spark_fleet.shared.security import agent_auth_dependency


def build_app(config: AgentConfig) -> FastAPI:
    """Build the agent application from validated configuration."""

    app = FastAPI(title="spark-fleet agent", version=spark_fleet.__version__)
    auth = agent_auth_dependency(config.agent_token)
    catalog = ModelCatalog(config.model_cache_roots)
    model_jobs = ModelJobManager(catalog)
    jobs = JobManager()
    app.state.catalog = catalog
    app.state.model_jobs = model_jobs
    app.state.jobs = jobs

    @app.get("/health")
    def health() -> dict[str, Any]:
        return {"status": "ok", "node_id": config.node_id}

    @app.get("/info", dependencies=[Depends(auth)])
    def info() -> dict[str, Any]:
        return {
            "node_id": config.node_id,
            "version": spark_fleet.__version__,
            "hostname": socket.gethostname(),
            "platform": platform.platform(),
            "python_version": platform.python_version(),
            "data_dir": str(config.data_dir),
            "compose_projects_dir": str(config.compose_projects_dir),
            "model_cache_roots": [str(root) for root in config.model_cache_roots],
        }

    # Compose --------------------------------------------------------------

    def spec_for(project: str) -> ProjectSpec:
        if not isinstance(project, str) or not PROJECT_PATTERN.fullmatch(project):
            raise ComposeError(f"invalid project name: {project!r}")
        return ProjectSpec(
            project=project,
            directory=config.compose_projects_dir / project,
            deployment="",
            recipe="",
            role="",
            config_hash="",
        )

    def compose_document(body: Mapping[str, Any]) -> tuple[str, Mapping[str, Any]]:
        project = body.get("project")
        compose = body.get("compose")
        if not isinstance(project, str):
            raise ComposeError("project is required")
        if not isinstance(compose, Mapping):
            raise ComposeError("compose must be a mapping")
        return project, compose

    @app.post("/compose/validate", dependencies=[Depends(auth)])
    def compose_validate(body: Mapping[str, Any]) -> dict[str, Any]:
        try:
            project, compose = compose_document(body)
            spec = validate_project(config.compose_projects_dir / project, compose, project)
        except ComposeError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        return {
            "project": spec.project,
            "directory": str(spec.directory),
            "config_hash": spec.config_hash,
        }

    @app.post("/compose/up", dependencies=[Depends(auth)])
    def compose_up(body: Mapping[str, Any]) -> dict[str, Any]:
        try:
            project, compose = compose_document(body)
            spec = validate_project(config.compose_projects_dir / project, compose, project)
            containers = up_project(spec)
        except ComposeError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        return {"containers": [asdict(container) for container in containers]}

    @app.post("/compose/down", dependencies=[Depends(auth)])
    def compose_down(body: Mapping[str, Any]) -> dict[str, Any]:
        project = body.get("project")
        try:
            spec = spec_for(project)
            down_project(spec)
        except ComposeError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        return {"stopped": spec.project}

    @app.get("/compose/status", dependencies=[Depends(auth)])
    def compose_status(project: str = Query(...)) -> dict[str, Any]:
        try:
            containers = status_project(spec_for(project))
        except ComposeError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        return {"containers": [asdict(container) for container in containers]}

    # Containers -----------------------------------------------------------

    @app.get("/containers", dependencies=[Depends(auth)])
    def list_containers(deployment: str | None = None, role: str | None = None) -> dict[str, Any]:
        try:
            containers = managed_containers(deployment, role)
        except ContainerError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        return {"containers": [asdict(container) for container in containers]}

    @app.get("/containers/logs", dependencies=[Depends(auth)])
    def logs(container_id: str = Query(...), tail: int = Query(200)) -> dict[str, Any]:
        try:
            text = container_logs(container_id, tail=tail)
        except ContainerError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        return {"logs": text}

    @app.get("/containers/logs/stream", dependencies=[Depends(auth)])
    def stream_logs(container_id: str = Query(...), tail: int = Query(200)) -> StreamingResponse:
        try:
            lines = follow_container_logs(container_id, tail=tail)
        except ContainerError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        return StreamingResponse(lines, media_type="text/plain")

    # Models ---------------------------------------------------------------

    @app.get("/models", dependencies=[Depends(auth)])
    def models() -> dict[str, Any]:
        return _catalog_dict(catalog.scan())

    @app.post("/models/download", dependencies=[Depends(auth)])
    def download_model(body: Mapping[str, Any]) -> dict[str, Any]:
        repo = body.get("repo")
        root = body.get("root")
        revision = body.get("revision")
        allow_patterns = body.get("allow_patterns")
        if not isinstance(repo, str):
            raise HTTPException(status_code=400, detail="repo is required")
        if root is not None and not isinstance(root, str):
            raise HTTPException(status_code=400, detail="root must be a string")
        if revision is not None and not isinstance(revision, str):
            raise HTTPException(status_code=400, detail="revision must be a string")
        if allow_patterns is not None and not isinstance(allow_patterns, list):
            raise HTTPException(status_code=400, detail="allow_patterns must be a list")
        try:
            job_id = model_jobs.start(
                repo,
                root=Path(root) if root else None,
                revision=revision,
                allow_patterns=allow_patterns,
            )
        except ModelError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        return {"job_id": job_id}

    @app.get("/models/jobs", dependencies=[Depends(auth)])
    def model_jobs_list() -> dict[str, Any]:
        return {"jobs": [_model_status(job) for job in model_jobs.list()]}

    @app.get("/models/jobs/{job_id}", dependencies=[Depends(auth)])
    def model_job_status(job_id: str) -> dict[str, Any]:
        try:
            return _model_status(model_jobs.status(job_id))
        except ModelError as exc:
            raise HTTPException(status_code=404, detail=str(exc)) from exc

    @app.post("/models/jobs/{job_id}/cancel", dependencies=[Depends(auth)])
    def model_job_cancel(job_id: str) -> dict[str, Any]:
        try:
            cancelled = model_jobs.cancel(job_id)
        except ModelError as exc:
            raise HTTPException(status_code=404, detail=str(exc)) from exc
        return {"cancelled": cancelled}

    @app.post("/models/remove", dependencies=[Depends(auth)])
    def remove_model(body: Mapping[str, Any]) -> dict[str, Any]:
        repo = body.get("repo")
        root = body.get("root")
        confirm = body.get("confirm")
        in_use = body.get("in_use", [])
        if not isinstance(repo, str):
            raise HTTPException(status_code=400, detail="repo is required")
        if root is not None and not isinstance(root, str):
            raise HTTPException(status_code=400, detail="root must be a string")
        if confirm is not True:
            raise HTTPException(status_code=400, detail="explicit confirmation required")
        if not isinstance(in_use, list) or any(not isinstance(item, str) for item in in_use):
            raise HTTPException(status_code=400, detail="in_use must be a list of strings")
        try:
            model_jobs.remove(
                repo,
                root=Path(root) if root else None,
                confirm=True,
                in_use=in_use,
            )
        except ModelError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        return {"removed": repo}

    # Prepare jobs ---------------------------------------------------------

    @app.post("/jobs", dependencies=[Depends(auth)])
    def start_job(body: Mapping[str, Any]) -> dict[str, Any]:
        name = body.get("name")
        image = body.get("image")
        command = body.get("command")
        labels = body.get("labels")
        volumes = body.get("volumes", [])
        environment = body.get("environment", {})
        if not isinstance(name, str) or not isinstance(image, str):
            raise HTTPException(status_code=400, detail="name and image are required")
        if not isinstance(command, list) or any(not isinstance(item, str) for item in command):
            raise HTTPException(status_code=400, detail="command must be a list of strings")
        if not isinstance(labels, Mapping):
            raise HTTPException(status_code=400, detail="labels must be a mapping")
        if not isinstance(volumes, list) or any(not isinstance(item, str) for item in volumes):
            raise HTTPException(status_code=400, detail="volumes must be a list of strings")
        if not isinstance(environment, Mapping):
            raise HTTPException(status_code=400, detail="environment must be a mapping")
        try:
            job = jobs.start(
                name,
                image,
                command,
                labels=labels,
                volumes=volumes,
                environment={str(key): str(value) for key, value in environment.items()},
            )
        except JobError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        return {"job_id": job.name}

    @app.get("/jobs", dependencies=[Depends(auth)])
    def list_jobs() -> dict[str, Any]:
        return {"jobs": jobs.list()}

    @app.get("/jobs/{job_id}", dependencies=[Depends(auth)])
    def job_status(job_id: str) -> dict[str, Any]:
        try:
            return jobs.status(job_id)
        except JobError as exc:
            raise HTTPException(status_code=404, detail=str(exc)) from exc

    @app.post("/jobs/{job_id}/cancel", dependencies=[Depends(auth)])
    def job_cancel(job_id: str) -> dict[str, Any]:
        try:
            cancelled = jobs.cancel(job_id)
        except JobError as exc:
            raise HTTPException(status_code=404, detail=str(exc)) from exc
        return {"cancelled": cancelled}

    # Metrics --------------------------------------------------------------

    @app.get("/metrics", dependencies=[Depends(auth)])
    def metrics_snapshot() -> dict[str, Any]:
        try:
            snapshot = collect_metrics(config.model_cache_roots)
        except MetricsError as exc:
            raise HTTPException(status_code=503, detail=str(exc)) from exc
        return snapshot_to_dict(snapshot)

    return app


def _catalog_dict(catalog: Any) -> dict[str, Any]:
    return {
        "roots": [
            {"path": str(root.path), "exists": root.exists, "error": root.error}
            for root in catalog.roots
        ],
        "models": [
            {
                "repo": entry.repo,
                "root": str(entry.root),
                "path": str(entry.path),
                "cached": entry.cached,
                "partial": entry.partial,
                "size_bytes": entry.size_bytes,
                "error": entry.error,
            }
            for entry in catalog.models
        ],
    }


def _model_status(status: Any) -> dict[str, Any]:
    data = asdict(status)
    data["root"] = str(status.root)
    return data
