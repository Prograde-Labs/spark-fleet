# Architecture

## Control plane

```
                 trusted LAN (192.0.2.0/24)
┌──────────────────────────────┐        ┌──────────────────────────────┐
│ head node (.239)             │        │ worker node (.79)            │
│                              │ fabric │                              │
│  spark-fleet controller      │◀──────▶│  spark-fleet agent           │
│  :8091 API/UI/gateway        │        │  :8092 API                   │
│  SQLite state                │  HTTP  │  Docker socket               │
│  no Docker socket            │        │  Compose projects            │
│                              │        │  model cache                 │
└──────────────┬───────────────┘        └──────────────────────────────┘
               │ bearer-token HTTP
               ▼
        other Spark agents
```

The controller is the only writer of fleet state. It stores nodes, recipes,
deployments, role rows, jobs, events, metrics, inference samples, and settings in
one SQLite database. Agents are stateless apart from their Compose project
directory and model cache; all state changes flow through controller jobs.

## Components

- **Controller** (`spark-fleet-controller`): FastAPI app. Owns node CRUD, recipe
  files, deployment lifecycle jobs, the inference gateway, the importer, the SSE
  event stream, and the SPA. It never talks to Docker directly.
- **Agent** (`spark-fleet-agent`): FastAPI app on each node. Validates and runs
  per-role Compose projects, lists managed containers, streams logs, runs one-shot
  prepare jobs, scans/downloads/removes models, and collects host/GPU telemetry.
- **Gateway**: routes by unique alias or served model name to the head role's
  inference port. `spark-active` maps to the deployment flagged primary. Streaming
  responses pass through while token usage and latency are recorded.
- **UI**: Vue 3 + Vite + Tailwind SPA served by the controller. Hash routing, SSE
  for live state, mission-control styling.

## Recipe and deployment model

A recipe is a versionable YAML document:

- `spec.mode: deployment` — one or more roles, at least one `heavy`, each bound
  to a node. Heavy roles cannot share a node. The head exposes `inferencePort`
  and gateway aliases; workers run first and the head last by convention.
- `spec.mode: service` — exactly one `light` role, managed independently. No
  gateway aliases.
- `spec.prepare` — one-shot jobs (image pull, staging, fetch) run before roles
  start, with captured logs and a timeout.
- `{{ node.<id>.lanIp }}`, `{{ node.<id>.fabricIp }}`, `{{ topology.masterPort }}`,
  `{{ model.repo }}`, and `{{ model.servedName }}` placeholders are resolved by
  the renderer.

Loading a deployment runs one fleet-wide job:

1. Validate nodes, project names, aliases, and model cache availability.
2. Stop the current roles if redeploying.
3. Stop conflicting heavy deployments in reverse start order.
4. Run prepare jobs and wait for completion.
5. Render and start roles in `startOrder`, waiting for readiness
   (`running`, `container-health`, `http`, `tcp`, `log-pattern`).
6. Persist role container ids/config hashes and mark the deployment loaded.
7. Set `spark-active` when requested or when no deployment is primary.

Failures clean up started roles, mark the deployment failed, clear the primary
flag, and apply the rollback policy: `restore` reloads displaced deployments,
`leave-down` leaves everything stopped. Cancellation cleanup is best-effort.

## Gateway

- Routes are built from loaded deployment recipes.
- The head is the heavy role with an inference port, preferring a role named
  `head`, then the highest `startOrder`.
- Duplicate aliases are rejected (409) rather than silently choosing a winner.
- Unknown models return 404; loading deployments return 503 with `Retry-After`.
- `POST /v1/*` rewrites the JSON model field to the served name, strips
  hop-by-hop headers plus client credentials, forwards query strings, and
  filters stale response encoding headers.
- The upstream client timeout is `SPARK_FLEET_GATEWAY_TIMEOUT_SECONDS` (1800s
  default, also settable as the stored `gateway_timeout_seconds` setting). It
  bounds each wait for upstream data, notably the first-byte wait while a
  prompt prefill completes, and aborts a stalled stream. It must exceed the
  slowest prefill: aborting a 256k-token prefill makes the client retry, and
  the retries keep every stream busy pre-filling the same context. Connect
  attempts are capped at 10s separately.

## Telemetry and state

- Agents expose `/metrics` with host memory, managed-container memory, cache
  bytes and cache filesystem capacity/free space, and per-GPU values when
  `nvidia-smi` is available. The packaged agent requests `gpus: all`, which
  requires the NVIDIA container toolkit on the node; without it, GPU collection
  degrades to an error entry.
- The controller polls nodes and metrics on configured intervals, writes series
  to SQLite, prunes by retention (24h default at 10s), and incrementally vacuums.
- Gateway counters flush into inference samples: request/error counts, latency,
  TTFT, prompt/completion tokens.
- Startup reconciliation compares loaded deployments with agent container state:
  definite missing/stopped containers fail the deployment; an unreachable agent
  alone leaves stored state intact and reports uncertainty.
- Interrupted jobs are marked failed on restart. The design assumes one
  controller instance.

## Import and cutover

The importer reads old legacy-controller `/api/workloads`, instance YAML, and model
catalog. It groups distributed heavy workloads by master address/port or
`-head`/`-worker` naming, builds recipes with node placeholders, folds helper
lights into prepare jobs, skips obsolete log helpers, and shows old-vs-new diffs.
Apply requires explicit confirmation and writes pending deployments plus recipe
files.

Cutover is manual. The old head controller and the agent already occupy
`8091`/`8092`, so run the new controller on a free temporary port such as
`8099`, register nodes, and import recipes from the old controllers. Import
fidelity is imperfect: served names come from `--served-model-name` only when
the flag is a separate command token, so shell-embedded flags and
`x-served-model` fall back to the workload name; grouping by master address/port
can conflate alternative configs that share one rendezvous port; and
`light`-classed worker ranks are treated as services. Review the plan and
correct recipe roles and served names before loading.

The live `demo-heavy` deployment is fleet-owned: its containers carry
`spark.fleet.*` labels, so startup reconciliation keeps it loaded across
controller restarts and no adoption step is needed. Readiness for HTTP and TCP
roles fails fast when the role container has exited, died, or is restarting
instead of waiting out the full timeout, and compose up/down calls use a longer
client timeout so large model containers can shut down cleanly.

### Adopted deployments (zero-downtime controller switch)

A running legacy-controller heavy can stay live while the controller is replaced by
recording it as a loaded deployment whose recipe points at the existing
inference port, without running Compose. The gateway routes by node LAN IP and
the role's inference port, so the adopted deployment proxies the old container
directly.

Startup reconciliation compares loaded deployments with agent container state.
Old containers carry `legacy.*` labels rather than `spark.fleet.*`, so an
adopted deployment is marked failed on the next controller restart. Adopt only
after the final controller startup, and re-adopt after any restart. Never load
or redeploy an adopted deployment: its fleet project does not own the running
containers, so a load would create duplicates that collide on GPU memory and
ports. To migrate the workload for real, unload it through the old controller
first, then load the fleet deployment.

See the README for the exact cutover and rollback commands.

## Known v1 limits

- Single controller only; no HA or multi-writer coordination.
- Routing tables are rebuilt per gateway request; no cache.
- Job rows are not pruned; metrics queries issue per-series reads.
- Settings overrides are read at startup and require a restart.
- UI login has no rate limiting; TLS must terminate in front of the controller.
- Import URLs are caller-supplied HTTP(S) and trust the LAN boundary.
