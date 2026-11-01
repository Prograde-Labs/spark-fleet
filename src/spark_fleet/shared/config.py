"""Environment-driven configuration for the controller and node agents.

Every value comes from the process environment (or an explicit mapping in
tests). Invalid values raise :class:`ConfigError` naming the offending variable
so a failed container start explains itself. Secrets are validated without being
trimmed or echoed, and are excluded from dataclass ``repr`` output.
"""

from __future__ import annotations

import os
import re
from collections.abc import Mapping
from dataclasses import dataclass, field
from pathlib import Path

LOG_LEVELS = ("DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL")
ROLLBACK_POLICIES = ("restore", "leave-down")
MIN_TOKEN_LENGTH = 16
MIN_SESSION_SECRET_LENGTH = 32
DEFAULT_DATA_DIR = Path("/var/lib/spark-fleet")
DEFAULT_AGENT_DATA_DIR = Path("/var/lib/spark-fleet-agent")
DEFAULT_MODEL_CACHE_ROOT = Path("/srv/spark-fleet/hub")
_TOKEN_PATTERN = re.compile(r"[A-Za-z0-9._~-]+")
_NODE_ID_PATTERN = re.compile(r"[A-Za-z0-9._-]{1,63}")


class ConfigError(ValueError):
    """Raised when an environment-derived configuration value is invalid."""


@dataclass(frozen=True)
class ControllerConfig:
    host: str
    port: int
    data_dir: Path
    recipes_dir: Path
    db_path: Path
    web_dist: Path | None
    agent_token: str = field(repr=False)
    ui_password: str | None = field(repr=False)
    session_secret: str | None = field(repr=False)
    rollback_policy: str = "restore"
    metrics_retention_seconds: int = 86400
    metrics_sample_interval_seconds: int = 10
    node_poll_interval_seconds: int = 15
    log_level: str = "INFO"


@dataclass(frozen=True)
class AgentConfig:
    host: str
    port: int
    node_id: str | None
    data_dir: Path
    compose_projects_dir: Path
    model_cache_roots: tuple[Path, ...]
    agent_token: str = field(repr=False)
    log_level: str = "INFO"


def _secret(env: Mapping[str, str], name: str, *, required: bool = False) -> str | None:
    raw = env.get(name)
    if raw is None:
        if required:
            raise ConfigError(f"{name} is required")
        return None
    if not raw.strip():
        raise ConfigError(f"{name} must not be empty")
    if raw != raw.strip():
        raise ConfigError(f"{name} must not have leading or trailing whitespace")
    return raw


def _optional_str(env: Mapping[str, str], name: str) -> str | None:
    raw = env.get(name)
    if raw is None:
        return None
    value = raw.strip()
    return value or None


def _required_str(env: Mapping[str, str], name: str, default: str) -> str:
    raw = env.get(name)
    if raw is None:
        return default
    value = raw.strip()
    if not value:
        raise ConfigError(f"{name} must not be empty")
    return value


def _int(env: Mapping[str, str], name: str, default: int, minimum: int, maximum: int) -> int:
    raw = env.get(name)
    if raw is None:
        return default
    raw = raw.strip()
    if not (raw.isascii() and raw.isdigit()):
        raise ConfigError(f"{name} must be an integer between {minimum} and {maximum}, got {raw!r}")
    value = int(raw)
    if not minimum <= value <= maximum:
        raise ConfigError(f"{name} must be between {minimum} and {maximum}, got {value}")
    return value


def _absolute_path(name: str, raw: str) -> Path:
    try:
        path = Path(raw).expanduser()
    except RuntimeError as exc:
        raise ConfigError(f"{name} could not be expanded, got {raw!r}") from exc
    if not path.is_absolute():
        raise ConfigError(f"{name} must be an absolute path, got {raw!r}")
    return path


def _path(env: Mapping[str, str], name: str, default: Path) -> Path:
    raw = env.get(name)
    if raw is None:
        return default
    raw = raw.strip()
    if not raw:
        raise ConfigError(f"{name} must not be empty")
    return _absolute_path(name, raw)


def _optional_path(env: Mapping[str, str], name: str) -> Path | None:
    raw = env.get(name)
    if raw is None:
        return None
    raw = raw.strip()
    if not raw:
        return None
    return _absolute_path(name, raw)


def _agent_token(env: Mapping[str, str]) -> str:
    token = _secret(env, "SPARK_FLEET_AGENT_TOKEN", required=True)
    assert token is not None  # required=True raises instead of returning None
    if len(token) < MIN_TOKEN_LENGTH:
        raise ConfigError(f"SPARK_FLEET_AGENT_TOKEN must be at least {MIN_TOKEN_LENGTH} characters")
    if not _TOKEN_PATTERN.fullmatch(token):
        raise ConfigError(
            "SPARK_FLEET_AGENT_TOKEN must contain only unreserved URL-safe characters"
        )
    return token


def _node_id(env: Mapping[str, str]) -> str | None:
    node_id = _optional_str(env, "SPARK_FLEET_AGENT_NODE_ID")
    if node_id is None:
        return None
    if not _NODE_ID_PATTERN.fullmatch(node_id):
        raise ConfigError("SPARK_FLEET_AGENT_NODE_ID must match [A-Za-z0-9._-]{1,63}")
    return node_id


def _log_level(env: Mapping[str, str], name: str) -> str:
    raw = env.get(name)
    if raw is None:
        return "INFO"
    level = raw.strip().upper()
    if not level:
        raise ConfigError(f"{name} must not be empty")
    if level not in LOG_LEVELS:
        raise ConfigError(f"{name} must be one of {', '.join(LOG_LEVELS)}, got {raw!r}")
    return level


def _rollback_policy(env: Mapping[str, str]) -> str:
    raw = env.get("SPARK_FLEET_ROLLBACK_POLICY")
    if raw is None:
        return "restore"
    policy = raw.strip().lower().replace("_", "-")
    if policy not in ROLLBACK_POLICIES:
        raise ConfigError(
            f"SPARK_FLEET_ROLLBACK_POLICY must be one of "
            f"{', '.join(ROLLBACK_POLICIES)}, got {raw!r}"
        )
    return policy


def _model_cache_roots(env: Mapping[str, str]) -> tuple[Path, ...]:
    raw = env.get("SPARK_FLEET_MODEL_CACHE_ROOTS")
    if raw is None:
        return (DEFAULT_MODEL_CACHE_ROOT,)
    roots: list[Path] = []
    for part in raw.split(","):
        part = part.strip()
        if not part:
            continue
        path = _absolute_path("SPARK_FLEET_MODEL_CACHE_ROOTS", part)
        if path not in roots:
            roots.append(path)
    if not roots:
        raise ConfigError("SPARK_FLEET_MODEL_CACHE_ROOTS must contain at least one path")
    return tuple(roots)


def load_controller_config(env: Mapping[str, str] | None = None) -> ControllerConfig:
    """Load and validate controller configuration from ``env`` or the environment."""

    env = os.environ if env is None else env

    data_dir = _path(env, "SPARK_FLEET_DATA_DIR", DEFAULT_DATA_DIR)
    ui_password = _secret(env, "SPARK_FLEET_UI_PASSWORD")
    session_secret = _secret(env, "SPARK_FLEET_SESSION_SECRET")

    if ui_password is not None and session_secret is None:
        raise ConfigError(
            "SPARK_FLEET_SESSION_SECRET is required when SPARK_FLEET_UI_PASSWORD is set"
        )
    if session_secret is not None and len(session_secret) < MIN_SESSION_SECRET_LENGTH:
        raise ConfigError(
            f"SPARK_FLEET_SESSION_SECRET must be at least {MIN_SESSION_SECRET_LENGTH} characters"
        )

    return ControllerConfig(
        host=_required_str(env, "SPARK_FLEET_CONTROLLER_HOST", "0.0.0.0"),
        port=_int(env, "SPARK_FLEET_CONTROLLER_PORT", 8091, 1, 65535),
        data_dir=data_dir,
        recipes_dir=_path(env, "SPARK_FLEET_RECIPES_DIR", data_dir / "recipes"),
        db_path=_path(env, "SPARK_FLEET_DB_PATH", data_dir / "fleet.sqlite"),
        web_dist=_optional_path(env, "SPARK_FLEET_WEB_DIST"),
        agent_token=_agent_token(env),
        ui_password=ui_password,
        session_secret=session_secret,
        rollback_policy=_rollback_policy(env),
        metrics_retention_seconds=_int(
            env, "SPARK_FLEET_METRICS_RETENTION_SECONDS", 86400, 60, 30 * 86400
        ),
        metrics_sample_interval_seconds=_int(
            env, "SPARK_FLEET_METRICS_SAMPLE_INTERVAL_SECONDS", 10, 1, 3600
        ),
        node_poll_interval_seconds=_int(env, "SPARK_FLEET_NODE_POLL_INTERVAL_SECONDS", 15, 1, 3600),
        log_level=_log_level(env, "SPARK_FLEET_LOG_LEVEL"),
    )


def load_agent_config(env: Mapping[str, str] | None = None) -> AgentConfig:
    """Load and validate node-agent configuration from ``env`` or the environment."""

    env = os.environ if env is None else env

    data_dir = _path(env, "SPARK_FLEET_AGENT_DATA_DIR", DEFAULT_AGENT_DATA_DIR)

    return AgentConfig(
        host=_required_str(env, "SPARK_FLEET_AGENT_HOST", "0.0.0.0"),
        port=_int(env, "SPARK_FLEET_AGENT_PORT", 8092, 1, 65535),
        node_id=_node_id(env),
        data_dir=data_dir,
        compose_projects_dir=_path(env, "SPARK_FLEET_COMPOSE_PROJECTS_DIR", data_dir / "projects"),
        model_cache_roots=_model_cache_roots(env),
        agent_token=_agent_token(env),
        log_level=_log_level(env, "SPARK_FLEET_LOG_LEVEL"),
    )
