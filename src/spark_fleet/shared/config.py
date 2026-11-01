"""Environment-driven configuration for the controller and node agents.

Every value comes from the process environment (or an explicit mapping in
tests). Invalid values raise :class:`ConfigError` naming the offending variable
so a failed container start explains itself.
"""

from __future__ import annotations

import os
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path

LOG_LEVELS = ("DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL")
ROLLBACK_POLICIES = ("restore", "leave-down")
MIN_TOKEN_LENGTH = 16
MIN_SESSION_SECRET_LENGTH = 32
DEFAULT_DATA_DIR = Path("/var/lib/spark-fleet")
DEFAULT_AGENT_DATA_DIR = Path("/var/lib/spark-fleet-agent")
DEFAULT_MODEL_CACHE_ROOT = Path("/srv/spark-fleet/hub")


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
    agent_token: str
    ui_password: str | None
    session_secret: str | None
    rollback_policy: str
    metrics_retention_seconds: int
    metrics_sample_interval_seconds: int
    node_poll_interval_seconds: int
    log_level: str


@dataclass(frozen=True)
class AgentConfig:
    host: str
    port: int
    node_id: str | None
    data_dir: Path
    compose_projects_dir: Path
    model_cache_roots: tuple[Path, ...]
    agent_token: str
    log_level: str


def _optional_str(env: Mapping[str, str], name: str) -> str | None:
    raw = env.get(name)
    if raw is None:
        return None
    value = raw.strip()
    return value or None


def _required_str(env: Mapping[str, str], name: str, default: str | None = None) -> str:
    value = _optional_str(env, name)
    if value is None:
        if default is None:
            raise ConfigError(f"{name} is required")
        return default
    return value


def _int(env: Mapping[str, str], name: str, default: int, minimum: int, maximum: int) -> int:
    raw = env.get(name)
    if raw is None:
        return default
    raw = raw.strip()
    try:
        value = int(raw)
    except ValueError:
        raise ConfigError(
            f"{name} must be an integer between {minimum} and {maximum}, got {raw!r}"
        ) from None
    if not minimum <= value <= maximum:
        raise ConfigError(f"{name} must be between {minimum} and {maximum}, got {value}")
    return value


def _path(env: Mapping[str, str], name: str, default: Path) -> Path:
    raw = env.get(name)
    if raw is None:
        return default
    raw = raw.strip()
    if not raw:
        raise ConfigError(f"{name} must not be empty")
    return Path(raw).expanduser()


def _agent_token(env: Mapping[str, str]) -> str:
    token = _optional_str(env, "SPARK_FLEET_AGENT_TOKEN")
    if token is None:
        raise ConfigError("SPARK_FLEET_AGENT_TOKEN is required")
    if len(token) < MIN_TOKEN_LENGTH:
        raise ConfigError(f"SPARK_FLEET_AGENT_TOKEN must be at least {MIN_TOKEN_LENGTH} characters")
    return token


def _log_level(env: Mapping[str, str]) -> str:
    raw = _optional_str(env, "SPARK_FLEET_LOG_LEVEL")
    if raw is None:
        return "INFO"
    level = raw.upper()
    if level not in LOG_LEVELS:
        raise ConfigError(
            f"SPARK_FLEET_LOG_LEVEL must be one of {', '.join(LOG_LEVELS)}, got {raw!r}"
        )
    return level


def _rollback_policy(env: Mapping[str, str]) -> str:
    raw = _optional_str(env, "SPARK_FLEET_ROLLBACK_POLICY")
    if raw is None:
        return "restore"
    policy = raw.lower().replace("_", "-")
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
        path = Path(part).expanduser()
        if path not in roots:
            roots.append(path)
    if not roots:
        raise ConfigError("SPARK_FLEET_MODEL_CACHE_ROOTS must contain at least one path")
    return tuple(roots)


def load_controller_config(env: Mapping[str, str] | None = None) -> ControllerConfig:
    """Load and validate controller configuration from ``env`` or the environment."""

    env = os.environ if env is None else env

    data_dir = _path(env, "SPARK_FLEET_DATA_DIR", DEFAULT_DATA_DIR)
    ui_password = _optional_str(env, "SPARK_FLEET_UI_PASSWORD")
    session_secret = _optional_str(env, "SPARK_FLEET_SESSION_SECRET")

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
        log_level=_log_level(env),
    )


def load_agent_config(env: Mapping[str, str] | None = None) -> AgentConfig:
    """Load and validate node-agent configuration from ``env`` or the environment."""

    env = os.environ if env is None else env

    data_dir = _path(env, "SPARK_FLEET_AGENT_DATA_DIR", DEFAULT_AGENT_DATA_DIR)

    return AgentConfig(
        host=_required_str(env, "SPARK_FLEET_AGENT_HOST", "0.0.0.0"),
        port=_int(env, "SPARK_FLEET_AGENT_PORT", 8092, 1, 65535),
        node_id=_optional_str(env, "SPARK_FLEET_AGENT_NODE_ID"),
        data_dir=data_dir,
        compose_projects_dir=_path(env, "SPARK_FLEET_COMPOSE_PROJECTS_DIR", data_dir / "projects"),
        model_cache_roots=_model_cache_roots(env),
        agent_token=_agent_token(env),
        log_level=_log_level(env),
    )


def _optional_path(env: Mapping[str, str], name: str) -> Path | None:
    raw = _optional_str(env, name)
    return Path(raw).expanduser() if raw is not None else None
