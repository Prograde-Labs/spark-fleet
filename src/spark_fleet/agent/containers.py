"""Container inspection and log streaming for a node's managed containers.

The agent lists only containers carrying spark-fleet labels, merges
``docker inspect`` (state, health, labels) with a one-shot ``docker stats``
sample (memory usage/limit/percent), and exposes log tail/follow helpers. Names
and IDs are validated so no caller can smuggle arbitrary Docker arguments.
"""

from __future__ import annotations

import re
from collections.abc import Iterator, Mapping
from dataclasses import dataclass
from typing import Any

from spark_fleet.agent import docker_ops
from spark_fleet.shared.recipe import NAME_PATTERN
from spark_fleet.shared.render import LABEL_DEPLOYMENT, LABEL_ROLE

CONTAINER_ID_PATTERN = re.compile(r"[A-Za-z0-9][A-Za-z0-9_.-]{0,127}")
_NAME_PATTERN = re.compile(NAME_PATTERN)
_SIZE_PATTERN = re.compile(r"([0-9]+(?:\.[0-9]+)?)\s*([A-Za-z]+)")
_SIZE_UNITS = {
    "b": 1,
    "kb": 1000,
    "mb": 1000**2,
    "gb": 1000**3,
    "tb": 1000**4,
    "kib": 1024,
    "mib": 1024**2,
    "gib": 1024**3,
    "tib": 1024**4,
}
_MAX_LOG_TAIL = 10_000


class ContainerError(RuntimeError):
    """Raised when container inspection or log streaming fails."""


@dataclass(frozen=True)
class ContainerInfo:
    id: str
    name: str
    state: str
    status: str
    health: str | None
    memory_usage: int | None
    memory_limit: int | None
    memory_percent: float | None
    labels: Mapping[str, str]


def managed_containers(
    deployment: str | None = None, role: str | None = None
) -> list[ContainerInfo]:
    """Inspect fleet containers, optionally narrowed to a deployment and role."""

    if deployment is not None and not _NAME_PATTERN.fullmatch(deployment):
        raise ContainerError(f"invalid deployment name: {deployment!r}")
    if role is not None:
        if deployment is None:
            raise ContainerError("a role filter requires a deployment")
        if not _NAME_PATTERN.fullmatch(role):
            raise ContainerError(f"invalid role name: {role!r}")

    filters: dict[str, str | None] = {LABEL_DEPLOYMENT: deployment}
    if role is not None:
        filters[LABEL_ROLE] = role

    try:
        rows = docker_ops.list_containers_by_labels(filters)
        if not rows:
            return []
        ids = [row["ID"] for row in rows if isinstance(row.get("ID"), str) and row["ID"]]
        if not ids:
            raise ContainerError("docker ps returned containers without IDs")
    except docker_ops.DockerError as exc:
        raise ContainerError(str(exc)) from exc

    # Inspect and stats are enrichment: a container may vanish mid-redeploy, so
    # fall back to the `docker ps` fields instead of failing the whole listing.
    inspections = _inspect_containers(ids)
    try:
        samples = docker_ops.container_stats(ids)
    except docker_ops.DockerError:
        samples = []

    inspected = {
        record["Id"]: record for record in inspections if isinstance(record.get("Id"), str)
    }
    sampled = {sample["ID"]: sample for sample in samples if isinstance(sample.get("ID"), str)}

    infos: list[ContainerInfo] = []
    for row in rows:
        container_id = row.get("ID")
        if not isinstance(container_id, str) or not container_id:
            raise ContainerError(f"docker ps returned a container without an ID: {row!r}")
        record = _match(inspected, container_id)
        infos.append(_container_info(container_id, row, record, _match(sampled, container_id)))
    return infos


def _inspect_containers(ids: list[str]) -> list[dict[str, Any]]:
    try:
        return docker_ops.inspect_containers(ids)
    except docker_ops.DockerError:
        pass
    if len(ids) == 1:
        return []
    records: list[dict[str, Any]] = []
    for container_id in ids:
        try:
            records.extend(docker_ops.inspect_containers([container_id]))
        except docker_ops.DockerError:
            continue
    return records


def container_logs(container_id: str, *, tail: int = 200) -> str:
    """Return the last ``tail`` lines of a managed container's combined output."""

    _validate_container_id(container_id)
    _validate_tail(tail)
    _require_managed(container_id)
    try:
        return docker_ops.container_logs(container_id, tail=tail)
    except docker_ops.DockerError as exc:
        raise ContainerError(str(exc)) from exc


def follow_container_logs(container_id: str, *, tail: int = 200) -> Iterator[str]:
    """Yield a managed container's log lines until the stream closes.

    Validation and the managed-label check run eagerly, before the caller starts
    streaming, so an unknown or non-fleet container is rejected up front.
    """

    _validate_container_id(container_id)
    _validate_tail(tail)
    _require_managed(container_id)
    return _follow(container_id, tail)


def _follow(container_id: str, tail: int) -> Iterator[str]:
    try:
        yield from docker_ops.follow_container_logs(container_id, tail=tail)
    except docker_ops.DockerError as exc:
        raise ContainerError(str(exc)) from exc


def parse_size(text: str) -> int | None:
    """Parse a Docker human-readable size such as ``1.5GiB`` into bytes."""

    match = _SIZE_PATTERN.fullmatch(text.strip())
    if match is None:
        return None
    factor = _SIZE_UNITS.get(match.group(2).lower())
    if factor is None:
        return None
    return int(float(match.group(1)) * factor)


def _container_info(
    container_id: str,
    row: Mapping[str, Any],
    record: Mapping[str, Any] | None,
    sample: Mapping[str, Any] | None,
) -> ContainerInfo:
    labels: dict[str, str] = {}
    health: str | None = None
    state_text = ""
    name = ""

    if isinstance(record, Mapping):
        state = record.get("State")
        state = state if isinstance(state, Mapping) else {}
        health_data = state.get("Health")
        health = health_data.get("Status") if isinstance(health_data, Mapping) else None
        config = record.get("Config")
        raw_labels = config.get("Labels") if isinstance(config, Mapping) else None
        if isinstance(raw_labels, Mapping):
            labels = dict(raw_labels)
        raw_name = record.get("Name")
        if isinstance(raw_name, str):
            name = raw_name.lstrip("/")
        raw_state = state.get("Status")
        if isinstance(raw_state, str):
            state_text = raw_state

    if not labels:
        raw_labels = row.get("Labels")
        labels = docker_ops.parse_labels(raw_labels if isinstance(raw_labels, str) else None)
    if not name:
        raw_name = row.get("Names")
        name = raw_name if isinstance(raw_name, str) else ""
    if not state_text:
        raw_state = row.get("State")
        state_text = raw_state if isinstance(raw_state, str) else ""

    status = row.get("Status") if isinstance(row.get("Status"), str) else ""
    if not isinstance(health, str):
        health = _health_from_status(status)

    usage, limit, percent = _memory(sample)
    return ContainerInfo(
        id=container_id,
        name=name,
        state=state_text,
        status=status,
        health=health,
        memory_usage=usage,
        memory_limit=limit,
        memory_percent=percent,
        labels=labels,
    )


def _health_from_status(status: str) -> str | None:
    for health in ("healthy", "unhealthy", "starting"):
        if f"({health})" in status or f"(health: {health})" in status:
            return health
    return None


def _memory(sample: Mapping[str, Any] | None) -> tuple[int | None, int | None, float | None]:
    if sample is None:
        return None, None, None

    usage_text = sample.get("MemUsage")
    usage = limit = None
    if isinstance(usage_text, str):
        used, separator, total = usage_text.partition("/")
        if separator:
            usage = parse_size(used)
            limit = parse_size(total)

    percent = None
    percent_text = sample.get("MemPerc")
    if isinstance(percent_text, str):
        try:
            percent = float(percent_text.strip().removesuffix("%"))
        except ValueError:
            percent = None
    return usage, limit, percent


def _match(records: Mapping[str, Any], container_id: str) -> Mapping[str, Any] | None:
    record = records.get(container_id)
    if record is not None:
        return record
    for key, value in records.items():
        if key.startswith(container_id):
            return value
    return None


def _validate_container_id(container_id: str) -> None:
    if not isinstance(container_id, str) or not CONTAINER_ID_PATTERN.fullmatch(container_id):
        raise ContainerError(f"invalid container id: {container_id!r}")


def _require_managed(container_id: str) -> None:
    try:
        records = docker_ops.inspect_containers([container_id])
    except docker_ops.DockerError as exc:
        raise ContainerError(str(exc)) from exc
    if not records:
        raise ContainerError(f"container {container_id} not found")
    config = records[0].get("Config")
    labels = config.get("Labels") if isinstance(config, Mapping) else None
    if (
        not isinstance(labels, Mapping)
        or not labels.get(LABEL_DEPLOYMENT)
        or not labels.get(LABEL_ROLE)
    ):
        raise ContainerError(f"container {container_id} is not managed by spark-fleet")


def _validate_tail(tail: int) -> None:
    if isinstance(tail, bool) or not isinstance(tail, int) or not 0 <= tail <= _MAX_LOG_TAIL:
        raise ContainerError(f"tail must be an integer between 0 and {_MAX_LOG_TAIL}, got {tail!r}")
