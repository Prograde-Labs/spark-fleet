"""Thin subprocess wrapper around the Docker CLI and Docker Compose.

The agent never accepts arbitrary commands: every call is built from a fixed
argument list, the compose file path, and a validated project name. Failures are
reported as :class:`DockerError` carrying the CLI's stderr so the controller can
surface a useful job event.
"""

from __future__ import annotations

import json
import subprocess
from collections.abc import Iterator, Mapping, Sequence
from pathlib import Path
from typing import Any

DOCKER = "docker"
DEFAULT_TIMEOUT_SECONDS = 120.0
UP_TIMEOUT_SECONDS = 600.0


class DockerError(RuntimeError):
    """Raised when a Docker or Docker Compose command fails."""


def _run(args: list[str], timeout: float) -> subprocess.CompletedProcess[str]:
    try:
        return subprocess.run(
            args,
            capture_output=True,
            text=True,
            check=False,
            timeout=timeout,
        )
    except subprocess.TimeoutExpired as exc:
        raise DockerError(f"command timed out after {timeout} seconds: {' '.join(args)}") from exc
    except OSError as exc:
        raise DockerError(f"could not run {' '.join(args)}: {exc}") from exc


def _require_success(result: subprocess.CompletedProcess[str], description: str) -> None:
    if result.returncode:
        detail = (result.stderr or result.stdout or "").strip()
        raise DockerError(f"{description} failed: {detail or f'exit {result.returncode}'}")


def _compose_args(file: Path, project: str, *command: str) -> list[str]:
    return [DOCKER, "compose", "-p", project, "-f", str(file), *command]


def compose_config(file: Path, project: str, *, timeout: float = DEFAULT_TIMEOUT_SECONDS) -> str:
    """Return the Compose-normalized document for ``file``."""

    result = _run(_compose_args(file, project, "config"), timeout)
    _require_success(result, f"docker compose config for {project}")
    return result.stdout


def compose_up(file: Path, project: str, *, timeout: float = UP_TIMEOUT_SECONDS) -> None:
    """Create and start the project's containers in the background."""

    result = _run(_compose_args(file, project, "up", "-d"), timeout)
    _require_success(result, f"docker compose up for {project}")


def compose_down(file: Path, project: str, *, timeout: float = DEFAULT_TIMEOUT_SECONDS) -> None:
    """Stop and remove the project's containers and networks."""

    result = _run(_compose_args(file, project, "down"), timeout)
    _require_success(result, f"docker compose down for {project}")


def list_containers(project: str, *, timeout: float = 60.0) -> list[dict[str, Any]]:
    """List containers labelled with ``com.docker.compose.project``."""

    return list_containers_by_labels({"com.docker.compose.project": project}, timeout=timeout)


def list_containers_by_labels(
    labels: Mapping[str, str | None], *, timeout: float = 60.0
) -> list[dict[str, Any]]:
    """List all containers matching every label filter.

    A ``None`` value filters on label presence only.
    """

    args = [DOCKER, "ps", "-a"]
    for key, value in sorted(labels.items()):
        spec = f"label={key}" if value is None else f"label={key}={value}"
        args.extend(["--filter", spec])
    args.extend(["--format", "{{json .}}"])

    result = _run(args, timeout)
    _require_success(result, "docker ps")
    return _parse_ndjson(result.stdout, "docker ps")


def inspect_containers(ids: Sequence[str], *, timeout: float = 60.0) -> list[dict[str, Any]]:
    """Return ``docker inspect`` records for the given container IDs."""

    if not ids:
        return []
    result = _run([DOCKER, "inspect", *ids], timeout)
    _require_success(result, "docker inspect")
    try:
        data = json.loads(result.stdout)
    except json.JSONDecodeError as exc:
        raise DockerError("docker inspect returned invalid JSON") from exc
    if not isinstance(data, list) or not all(isinstance(item, dict) for item in data):
        raise DockerError("docker inspect returned unexpected JSON")
    return data


def container_stats(ids: Sequence[str], *, timeout: float = 60.0) -> list[dict[str, Any]]:
    """Return one-shot ``docker stats`` samples for the given container IDs."""

    if not ids:
        return []
    result = _run(
        [DOCKER, "stats", "--no-stream", "--format", "{{json .}}", *ids],
        timeout,
    )
    _require_success(result, "docker stats")
    return _parse_ndjson(result.stdout, "docker stats")


def container_logs(container_id: str, *, tail: int = 200, timeout: float = 60.0) -> str:
    """Return the last ``tail`` lines of a container's combined output."""

    try:
        result = subprocess.run(
            [DOCKER, "logs", "--tail", str(tail), container_id],
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            check=False,
            timeout=timeout,
        )
    except subprocess.TimeoutExpired as exc:
        raise DockerError(
            f"docker logs for {container_id} timed out after {timeout} seconds"
        ) from exc
    except OSError as exc:
        raise DockerError(f"could not run docker logs for {container_id}: {exc}") from exc
    if result.returncode:
        detail = (result.stdout or "").strip()
        raise DockerError(
            f"docker logs for {container_id} failed: {detail or f'exit {result.returncode}'}"
        )
    return result.stdout


def follow_container_logs(container_id: str, *, tail: int = 200) -> Iterator[str]:
    """Yield a container's log lines as they arrive until the stream closes."""

    try:
        process = subprocess.Popen(
            [DOCKER, "logs", "--follow", "--tail", str(tail), container_id],
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
        )
    except OSError as exc:
        raise DockerError(f"could not run docker logs for {container_id}: {exc}") from exc

    assert process.stdout is not None
    try:
        yield from process.stdout
    finally:
        if process.poll() is None:
            process.terminate()
            try:
                process.wait(timeout=5)
            except subprocess.TimeoutExpired:
                process.kill()
        if hasattr(process.stdout, "close"):
            process.stdout.close()


def _parse_ndjson(output: str, description: str) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for line in output.splitlines():
        line = line.strip()
        if not line:
            continue
        try:
            row = json.loads(line)
        except json.JSONDecodeError as exc:
            raise DockerError(f"{description} returned invalid JSON: {line!r}") from exc
        if not isinstance(row, dict):
            raise DockerError(f"{description} returned unexpected JSON: {line!r}")
        rows.append(row)
    return rows


def parse_labels(value: str | None) -> dict[str, str]:
    """Parse the comma-separated ``Labels`` field from ``docker ps``."""

    labels: dict[str, str] = {}
    if not value:
        return labels
    for item in value.split(","):
        key, separator, label = item.partition("=")
        if separator and key:
            labels[key] = label
    return labels
