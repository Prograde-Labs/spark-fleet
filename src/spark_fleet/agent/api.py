"""FastAPI application for a node agent.

``/health`` stays public so container and controller liveness probes work without
the shared secret; every operational endpoint (``/info`` and later Compose,
container, model, and metrics routes) requires the bearer token configured in
``SPARK_FLEET_AGENT_TOKEN``.
"""

from __future__ import annotations

import platform
import socket
from typing import Any

from fastapi import Depends, FastAPI

import spark_fleet
from spark_fleet.shared.config import AgentConfig
from spark_fleet.shared.security import agent_auth_dependency


def build_app(config: AgentConfig) -> FastAPI:
    """Build the agent application from validated configuration."""

    app = FastAPI(title="spark-fleet agent", version=spark_fleet.__version__)
    auth = agent_auth_dependency(config.agent_token)

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

    return app
