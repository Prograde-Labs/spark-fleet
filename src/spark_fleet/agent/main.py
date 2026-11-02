"""Agent entrypoint: build the app from environment config and serve it."""

from __future__ import annotations

import uvicorn

from spark_fleet.agent.api import build_app
from spark_fleet.shared.config import load_agent_config


def main() -> None:
    config = load_agent_config()
    uvicorn.run(
        build_app(config),
        host=config.host,
        port=config.port,
        log_level=config.log_level.lower(),
    )
