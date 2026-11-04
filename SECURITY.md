# Security policy

## Scope and threat model

spark-fleet is a **trusted-LAN control plane**, not an internet-facing service.
The controller API, mission-control UI, inference gateway, and agent APIs are
plain HTTP and unauthenticated apart from the shared agent bearer token and the
optional UI login. Do not expose them publicly. See
[docs/security.md](docs/security.md) for the full boundary description.

Reports that amount to "the service is reachable and unauthenticated when
published to the internet" are expected behavior, not vulnerabilities. Reports
about escaping the documented Docker/host boundaries, bypassing the agent token
or CSRF checks, path traversal in recipe or model handling, or injection through
recipe/import input are in scope.

## Reporting a vulnerability

Please do **not** open a public issue for a suspected vulnerability. Instead,
use GitHub's private vulnerability reporting for this repository:

1. Open the **Security** tab.
2. Choose **Report a vulnerability**.
3. Include the affected version or commit, reproduction steps, and impact.

If private reporting is unavailable, contact the maintainers directly and ask
for a private channel before sharing details.

## Response

Maintainers will acknowledge a report, investigate, and coordinate a fix and
disclosure timeline with the reporter. Please allow reasonable time for a fix
before public disclosure.

## Supported versions

This project is pre-1.0. Security fixes land on `main`; there are no maintained
release branches yet.
