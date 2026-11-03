# Security and LAN boundary

## Trust model

spark-fleet is a **trusted-LAN control plane**. The controller API, mission
control UI, inference gateway, and agent APIs are not designed for public
exposure. Do not publish their ports on the internet and do not add TLS
termination until authentication and encryption are designed together.

| Surface | Default auth |
| --- | --- |
| Controller `/api/*`, `/events`, SPA | none, unless optional UI login is enabled |
| Controller `/v1/*` gateway | none |
| Agent `/health` | public |
| Agent operational routes | shared bearer token |

## Agent bearer token

Every agent request except `/health` requires
`Authorization: Bearer $SPARK_FLEET_AGENT_TOKEN`. The controller compares the
token in constant time and sends it to agents; browsers never receive it.
Tokens must be at least 16 URL-safe characters and identical fleet-wide.

The controller proxies agent container lists and logs so the UI never needs the
agent token.

## Optional UI login

Login is off by default on the trusted LAN. Set **both**
`SPARK_FLEET_UI_PASSWORD` and `SPARK_FLEET_SESSION_SECRET` (32+ characters) to
enable it. Setting only one is a configuration error.

- `/api/login` verifies the password in constant time and issues an HMAC-signed,
  HttpOnly, SameSite=strict session cookie with an expiry and a per-session CSRF
  token.
- Mutations require the `X-CSRF-Token` header; logout also requires it.
- `/api/session` reports auth state and returns the CSRF token for an
  authenticated session so the SPA can recover after reload.
- Sessions default to 12 hours. There is no rate limiting or lockout in v1.
- The cookie is not `Secure` because v1 is plain HTTP on the LAN. Add TLS and
  `Secure` before any wider exposure.

## Docker and host boundaries

- The **controller has no Docker socket** and does not run host commands.
- Agents mount the Docker socket because they execute Compose on their node.
- Agent containers use host networking so recipe-declared inference ports and
  host-network workloads work directly. They are not privileged and do not add
  devices, capabilities, host PID/IPC namespaces, or broad host-home mounts.
- Agent mounts are narrow: `/var/run/docker.sock`, a dedicated agent data
  directory, and the configured model cache root.
- Controller mounts are a data directory and a recipes directory only.

## Recipes and workloads

Recipes can request host-network services, GPUs, devices, capabilities, and
volumes through the raw Compose escape hatch. Treat recipe files as privileged
configuration: only trusted operators should edit them. The controller validates
schema, node references, ports, aliases, and project names, but Compose services
still run with whatever the recipe declares.

Model removal is blocked when a repo is used by a loaded deployment and requires
explicit confirmation. Partial-download cleanup only targets entries that are
not cached.

## Import

The importer reads old controller URLs supplied by an operator. It accepts any
HTTP(S) URL on the LAN, so it can reach non-agent services; restrict access to
the controller and only import from controllers you trust. Import is read-only
against old controllers and does not stop or cut over workloads.

## Residual risks accepted for v1

- LAN services are unauthenticated by default and plain HTTP.
- UI login has no rate limiting.
- SSE and log streams authorize once at connection time; a session expiry does
  not terminate an open stream.
- Settings endpoints accept only known keys but are still LAN-trusted.
- Gateway routes are rebuilt per request and duplicate aliases fail all routes
  until corrected.
- The controller assumes a single instance; do not run two controllers against
  the same database.
