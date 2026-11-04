# Contributing

Thanks for your interest in spark-fleet. This guide covers local setup, the
checks we expect before a pull request, and how changes are reviewed.

## Development setup

Requirements:

- Python 3.12+
- [uv](https://docs.astral.sh/uv/)
- Node.js 20+ and npm
- Docker with the Compose plugin (for static Compose validation)

```sh
uv venv
uv pip install -e '.[dev]'
cd web && npm ci
```

## Checks

Run these before opening a pull request:

```sh
uv run pytest -v
uv run ruff check .
uv run ruff format --check .

cd web && npm test && npm run build
```

Static Compose validation (does not start containers):

```sh
docker compose --env-file .env.example -f compose.controller.yaml config --quiet
docker compose --env-file .env.example -f compose.agent.yaml config --quiet
```

## Guidelines

- Keep changes focused. Separate refactors from behavior changes.
- Add or update tests for behavior changes; keep the suite green.
- Follow the existing style: 100-character lines, `ruff` formatting, typed
  Python where the surrounding code is typed.
- Treat recipe files as privileged configuration. Changes to the recipe schema
  or renderer need tests for validation and rendering.
- Do not commit real hostnames, IP addresses, host paths, model repositories,
  registry credentials, or production recipes. Use documentation-safe
  placeholders such as `192.0.2.x`, `198.51.100.x`, `spark-01`, and
  `example-org/your-model`.
- Update documentation when behavior or configuration changes.

## Pull requests

- Describe the problem and the approach, and link related issues.
- Note any deployment or security implications.
- Expect review for correctness, tests, and the security boundary described in
  [docs/security.md](docs/security.md).

## Reporting security issues

Do not open a public issue for a suspected vulnerability. See
[SECURITY.md](SECURITY.md).

## License

By contributing, you agree that your contributions are licensed under the
Apache License, Version 2.0 (see [LICENSE](LICENSE)).
