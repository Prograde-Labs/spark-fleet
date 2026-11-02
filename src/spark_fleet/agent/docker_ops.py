"""Thin subprocess wrapper around the Docker CLI and Docker Compose.

The agent never accepts arbitrary commands: every call is built from a fixed
argument list, the compose file path, and a validated project name. Failures are
reported as :class:`DockerError` carrying the CLI's stderr so the controller can
surface a useful job event.
"""

from __future__ import annotations

import json
import subprocess
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

    result = _run(
        [
            DOCKER,
            "ps",
            "--filter",
            f"label=com.docker.compose.project={project}",
            "--format",
            "{{json .}}",
        ],
        timeout,
    )
    _require_success(result, f"docker ps for {project}")

    rows: list[dict[str, Any]] = []
    for line in result.stdout.splitlines():
        line = line.strip()
        if not line:
            continue
        try:
            row = json.loads(line)
        except json.JSONDecodeError as exc:
            raise DockerError(f"docker ps returned invalid JSON: {line!r}") from exc
        if not isinstance(row, dict):
            raise DockerError(f"docker ps returned unexpected JSON: {line!r}")
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
