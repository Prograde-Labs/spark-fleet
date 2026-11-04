## Summary

Describe the change and the problem it solves.

## Related issues

Closes #

## Type of change

- [ ] Bug fix
- [ ] New feature
- [ ] Refactor
- [ ] Documentation
- [ ] Test or CI change

## Verification

List the checks you ran:

- [ ] `pytest -v`
- [ ] `ruff check .`
- [ ] `ruff format --check .`
- [ ] `cd web && npm test && npm run build`
- [ ] `docker compose --env-file .env.example -f compose.controller.yaml config --quiet`
- [ ] `docker compose --env-file .env.example -f compose.agent.yaml config --quiet`

## Security and deployment notes

Describe any effect on the trusted-LAN security boundary, Docker/host mounts,
recipe privileges, or live deployment behavior. Write "none" if not applicable.

## Checklist

- [ ] Tests cover the change, or the change is documentation-only.
- [ ] No secrets, real hostnames/IPs, host paths, or production recipes are included.
- [ ] Documentation is updated where behavior or configuration changed.
