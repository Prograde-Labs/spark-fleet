# spark-fleet

Multi-node DGX Spark workload control plane.

A controller on the head Spark owns recipes, deployments, the fleet gateway, and
a dark mission-control UI. Each Spark node runs a small agent that executes
Docker Compose workloads, model-cache operations, log streaming, and telemetry
locally. Agents are the only components with a Docker socket.

## Layout

```
src/spark_fleet/
  shared/      configuration, recipes, rendering, state store, jobs, security
  agent/       per-node API: compose lifecycle, containers, jobs, models, metrics
  controller/  node registry, deployments, gateway, importer, API, wiring
web/           Vue 3 + Vite mission-control SPA
recipes/       example fleet recipes
docs/          architecture, security, and recipe authoring
tests/         pytest suite
```

## Components and ports

| Surface | Host | Port |
| --- | --- | --- |
| Controller API/UI/gateway | head node | `8091` |
| Agent API | each node | `8092` |
| Inference (recipe-declared) | head role node | e.g. `8000`, `8888` |

The controller exposes:

- `/api/*` management endpoints and `/events` SSE telemetry
- `/v1/models` and `/v1/*` inference gateway routes
- the built SPA at `/` when `SPARK_FLEET_WEB_DIST` points at `web/dist`

## Quick start

1. Copy `.env.example` to `.env` and replace `SPARK_FLEET_AGENT_TOKEN=CHANGE_ME`
   with at least 16 URL-safe characters. The same token must be used by the
   controller and every agent; config validation rejects the placeholder. To
   require UI login, also set both `SPARK_FLEET_UI_PASSWORD` and
   `SPARK_FLEET_SESSION_SECRET` (32+ characters).
2. Build and start the controller on the head node:

   ```sh
   docker compose --env-file .env -f compose.controller.yaml up -d --build
   ```

3. Build and start an agent on each node. `SPARK_FLEET_AGENT_NODE_ID` must
   match the id you register in the controller; override it per node (for
   example `spark-02`). `SPARK_FLEET_MODEL_CACHE_HOST_DIR` is the single host
   cache root mounted at `/srv/spark-fleet/hub` inside the agent.

   ```sh
   SPARK_FLEET_AGENT_NODE_ID=spark-01 \
   docker compose --env-file .env -f compose.agent.yaml up -d --build
   ```

4. Register each node in the UI (Nodes → Register node) with its LAN IP, fabric
   IP, and agent URL `http://<lan-ip>:8092`. Then create/load a deployment from a
   recipe or import the old controllers.

The controller image serves the SPA from `/app/web`; the `SPARK_FLEET_WEB_DIST`
environment variable is already set in `compose.controller.yaml`. Host bind
paths are `SPARK_FLEET_DATA_HOST_DIR` (default `./data`) and
`SPARK_FLEET_RECIPES_HOST_DIR` (default `./recipes`, which includes the example
recipes). The controller entrypoint chowns only those top-level directories and
drops to uid 10001. For local development run the API with `uvicorn` and the SPA
with `npm run dev` (Vite proxies `/api`, `/v1`, and `/events` to
`127.0.0.1:8091`).

GPU telemetry requires the NVIDIA container toolkit on the node; the agent
Compose stack requests `gpus: all` so `nvidia-smi` is available for metrics.

## Development

```sh
uv venv
uv pip install -e '.[dev]'
uv run pytest -v
uv run ruff check .
uv run ruff format --check .

cd web && npm ci && npm run build
```

Static Compose validation:

```sh
docker compose --env-file .env.example -f compose.controller.yaml config --quiet
docker compose --env-file .env.example -f compose.agent.yaml config --quiet
```

## Cutover from a legacy controller

1. Register the fleet nodes in the new controller (Nodes → Register node) with
   the agent URLs on port `8092`.
2. Start the new controller on a free temporary port (the old head controller
   and the agent already use `8091`/`8092`; use e.g. `8099`):
   `SPARK_FLEET_CONTROLLER_PORT=8099 docker compose --env-file .env -f compose.controller.yaml up -d --build`.
3. Open `http://<head-lan-ip>:8099`, run Import against the old controller URLs
   (`http://<head>:8091` and `http://<worker>:8091`), review the proposed diffs,
   and apply. Import is read-only against the old controllers.
4. Load one imported deployment and verify the gateway (`/v1/models`) and role
   logs on the temporary port.
5. Stop the old head controller. Recreate the new controller with
   `SPARK_FLEET_CONTROLLER_PORT=8091` (or edit `.env`) and start it on the
   production port.
6. Leave the old worker controller running until the new fleet is stable; it is
   the rollback path. To roll back, stop the new controller and restart the old
   head controller on `8091`.

## Recipes

Recipes are YAML files under the controller's recipes directory. A deployment
declares per-node roles, start order, readiness, model, gateway aliases, and
optional prepare jobs. A service declares one light role managed independently.
See [docs/recipes.md](docs/recipes.md) and `recipes/examples/` for vLLM
multi-node, TensorFold, SGLang, llama.cpp, OpenWebUI, and ComfyUI examples.

## Security boundary

This release targets the trusted LAN only. The controller API, UI, gateway, and
agent API are unauthenticated or plain HTTP apart from the shared agent bearer
token and optional UI login. Never expose these ports to the public internet
without authentication and TLS. The controller has no Docker socket; agents
mount only the Docker socket plus narrow data and model-cache paths. See
[docs/security.md](docs/security.md).

## Documentation

- [Architecture](docs/architecture.md)
- [Security and LAN boundary](docs/security.md)
- [Recipe authoring](docs/recipes.md)

## Status

The controller, agents, gateway, importer, mission-control UI, packaging,
docs, and example recipes are implemented and covered by the test suite.
GPU smoke tests and image builds run on ARM64 DGX Spark hardware.
