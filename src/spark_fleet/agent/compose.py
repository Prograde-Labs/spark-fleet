"""Write, validate, and run per-role Compose projects on a node.

The controller renders one Compose document per role with a single ``workload``
service. Before touching Docker the agent validates the document's shape and
fleet labels, writes it atomically under the project directory, and runs
``docker compose config`` for the normalized document and config hash. Lifecycle
helpers start, stop, and inspect only containers carrying the project's Compose
label, and verify the spark-fleet labels before reporting success.
"""

from __future__ import annotations

import hashlib
import re
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import yaml

from spark_fleet.agent import docker_ops
from spark_fleet.shared.render import (
    LABEL_CONFIG_HASH,
    LABEL_DEPLOYMENT,
    LABEL_RECIPE,
    LABEL_ROLE,
)

COMPOSE_FILE_NAME = "compose.yaml"
PROJECT_PATTERN = re.compile(r"[a-z0-9][a-z0-9_-]{0,62}")
_REQUIRED_LABELS = (LABEL_DEPLOYMENT, LABEL_RECIPE, LABEL_ROLE, LABEL_CONFIG_HASH)
_ALLOWED_TOP_LEVEL = frozenset({"services", "volumes", "networks"})


class ComposeError(RuntimeError):
    """Raised when a Compose project is invalid or a lifecycle call fails."""


@dataclass(frozen=True)
class ContainerStatus:
    id: str
    name: str
    service: str
    state: str
    status: str
    labels: Mapping[str, str]


@dataclass(frozen=True)
class ProjectSpec:
    project: str
    directory: Path
    deployment: str
    role: str
    config_hash: str

    @property
    def compose_file(self) -> Path:
        return self.directory / COMPOSE_FILE_NAME


def validate_project(directory: Path, compose: Mapping[str, Any], project: str) -> ProjectSpec:
    """Validate ``compose``, write it, and normalize it with Docker Compose."""

    if not PROJECT_PATTERN.fullmatch(project):
        raise ComposeError(f"invalid project name: {project!r}")

    document = _validate_document(compose)
    labels = document["services"]["workload"]["labels"]
    expected = f"spark-fleet-{labels[LABEL_DEPLOYMENT]}-{labels[LABEL_ROLE]}"
    if project != expected:
        raise ComposeError(
            f"project name {project!r} does not match deployment/role labels {expected!r}"
        )

    directory = Path(directory)
    file = directory / COMPOSE_FILE_NAME
    try:
        directory.mkdir(parents=True, exist_ok=True)
        _write_compose(file, document)
    except OSError as exc:
        raise ComposeError(f"could not write {file}: {exc}") from exc

    try:
        normalized = docker_ops.compose_config(file, project)
    except docker_ops.DockerError as exc:
        raise ComposeError(str(exc)) from exc

    return ProjectSpec(
        project=project,
        directory=directory,
        deployment=labels[LABEL_DEPLOYMENT],
        role=labels[LABEL_ROLE],
        config_hash=hashlib.sha256(normalized.encode()).hexdigest(),
    )


def up_project(spec: ProjectSpec) -> list[ContainerStatus]:
    """Start the project and verify every container carries its fleet labels."""

    try:
        docker_ops.compose_up(spec.compose_file, spec.project)
        statuses = status_project(spec)
    except docker_ops.DockerError as exc:
        raise ComposeError(str(exc)) from exc

    if not statuses:
        raise ComposeError(f"no containers found for project {spec.project}")
    for container in statuses:
        _verify_labels(container, spec)
    return statuses


def down_project(spec: ProjectSpec) -> None:
    """Stop and remove the project's containers."""

    try:
        docker_ops.compose_down(spec.compose_file, spec.project)
    except docker_ops.DockerError as exc:
        raise ComposeError(str(exc)) from exc


def status_project(spec: ProjectSpec) -> list[ContainerStatus]:
    """Report the project's containers, including IDs, state, and labels."""

    try:
        rows = docker_ops.list_containers(spec.project)
    except docker_ops.DockerError as exc:
        raise ComposeError(str(exc)) from exc
    return [_container_status(row) for row in rows]


def _validate_document(compose: Mapping[str, Any]) -> dict[str, Any]:
    if not isinstance(compose, Mapping):
        raise ComposeError("compose document must be a mapping")

    unknown = sorted(set(compose) - _ALLOWED_TOP_LEVEL)
    if unknown:
        raise ComposeError(f"unsupported top-level keys: {', '.join(unknown)}")

    services = compose.get("services")
    if not isinstance(services, Mapping):
        raise ComposeError("compose document requires a services mapping")
    if set(services) != {"workload"}:
        raise ComposeError("compose document must define exactly one workload service")

    workload = services["workload"]
    if not isinstance(workload, Mapping):
        raise ComposeError("workload service must be a mapping")

    labels = workload.get("labels")
    if not isinstance(labels, Mapping):
        raise ComposeError("workload service requires a labels mapping")
    missing = [
        label
        for label in _REQUIRED_LABELS
        if not isinstance(labels.get(label), str) or not labels.get(label)
    ]
    if missing:
        raise ComposeError(f"workload service is missing fleet labels: {', '.join(missing)}")

    return dict(compose)


def _write_compose(file: Path, document: Mapping[str, Any]) -> None:
    payload = yaml.safe_dump(dict(document), sort_keys=False)
    temporary = file.with_name(f"{file.name}.tmp")
    temporary.write_text(payload, encoding="utf-8")
    temporary.replace(file)


def _container_status(row: Mapping[str, Any]) -> ContainerStatus:
    labels = docker_ops.parse_labels(
        row.get("Labels") if isinstance(row.get("Labels"), str) else None
    )
    service = row.get("Service")
    if not isinstance(service, str) or not service:
        service = labels.get("com.docker.compose.service", "")
    return ContainerStatus(
        id=str(row.get("ID") or ""),
        name=str(row.get("Names") or ""),
        service=service,
        state=str(row.get("State") or ""),
        status=str(row.get("Status") or ""),
        labels=labels,
    )


def _verify_labels(container: ContainerStatus, spec: ProjectSpec) -> None:
    expected = {
        LABEL_DEPLOYMENT: spec.deployment,
        LABEL_ROLE: spec.role,
    }
    for label, value in expected.items():
        actual = container.labels.get(label)
        if actual != value:
            raise ComposeError(
                f"container {container.id or container.name} has {label}={actual!r}, "
                f"expected {value!r}"
            )
    if not container.labels.get(LABEL_CONFIG_HASH):
        raise ComposeError(
            f"container {container.id or container.name} is missing {LABEL_CONFIG_HASH}"
        )
