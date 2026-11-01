# spark-fleet

Multi-node DGX Spark workload control plane.

A central controller on the head Spark manages a dynamic fleet of Spark nodes.
Each node runs a small agent that executes Docker Compose workloads, model-cache
operations, log streaming, and telemetry locally. The controller owns recipes,
deployments, the fleet gateway, and the mission-control UI.

## Layout

```
src/spark_fleet/
  shared/      configuration, recipes, rendering, state store, jobs, security
  agent/       per-node API: compose lifecycle, containers, jobs, models, metrics
  controller/  node registry, deployments, gateway, importer, API, wiring
web/           Vue 3 + Vite mission-control SPA (later phase)
tests/         pytest suite
```

## Development

```sh
uv venv
uv pip install -e '.[dev]'
uv run pytest -v
uv run ruff check .
uv run ruff format --check .
```

The web app is built separately:

```sh
cd web && npm ci && npm run build
```

## Security boundary

This release targets the trusted LAN only. The gateway, management API, agent
API, and UI are unauthenticated by default apart from the shared agent bearer
token. Do not expose any port to the public internet, and do not add TLS
termination until authentication and encryption are designed in.

## Status

Under active construction. See `docs/architecture.md` once written for the full
design.
