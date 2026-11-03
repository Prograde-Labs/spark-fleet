"""Typed HTTP client for talking to a node agent.

Every operational request carries the shared bearer token; ``/health`` stays
public for liveness probes. Non-2xx responses become :class:`AgentError` with the
agent's ``detail`` and status code, and transport failures become
:class:`AgentUnreachableError`. Methods are deliberately narrow: the controller
never sends arbitrary Docker commands, only the typed operations the agent
exposes.
"""

from __future__ import annotations

from collections.abc import AsyncIterator, Mapping, Sequence
from typing import Any
from urllib.parse import quote, urlparse

import httpx

DEFAULT_TIMEOUT_SECONDS = 10.0
# Compose up/down can take far longer than a status call: a large model
# container may need tens of seconds to shut down. Keep the short default for
# probes and use this for lifecycle mutations.
COMPOSE_TIMEOUT_SECONDS = 300.0
STREAM_TIMEOUT = httpx.Timeout(DEFAULT_TIMEOUT_SECONDS, read=None)


class AgentError(RuntimeError):
    """Raised when an agent returns an error response."""

    def __init__(self, message: str, *, status_code: int | None = None):
        super().__init__(message)
        self.status_code = status_code


class AgentUnreachableError(AgentError):
    """Raised when the agent cannot be reached or the request times out."""


class AgentClient:
    """A typed async client bound to one agent's base URL and token."""

    def __init__(
        self,
        base_url: str,
        token: str,
        *,
        timeout: float = DEFAULT_TIMEOUT_SECONDS,
        transport: httpx.AsyncBaseTransport | None = None,
    ):
        _validate_base_url(base_url)
        if not token:
            raise ValueError("agent token must not be empty")
        if timeout <= 0:
            raise ValueError("agent timeout must be positive")
        self._token = token
        self._client = httpx.AsyncClient(
            base_url=base_url.rstrip("/"),
            timeout=timeout,
            transport=transport,
        )

    @property
    def is_closed(self) -> bool:
        return self._client.is_closed

    async def aclose(self) -> None:
        await self._client.aclose()

    async def __aenter__(self) -> AgentClient:
        return self

    async def __aexit__(self, *exc_info) -> None:
        await self.aclose()

    # Health and info ------------------------------------------------------

    async def health(self) -> dict[str, Any]:
        return await self._request("GET", "/health", auth=False)

    async def info(self) -> dict[str, Any]:
        return await self._request("GET", "/info")

    # Compose --------------------------------------------------------------

    async def validate_compose(self, project: str, compose: Mapping[str, Any]) -> dict[str, Any]:
        return await self._request(
            "POST",
            "/compose/validate",
            json_body={"project": project, "compose": dict(compose)},
        )

    async def up_compose(self, project: str, compose: Mapping[str, Any]) -> list[dict[str, Any]]:
        payload = await self._request(
            "POST",
            "/compose/up",
            json_body={"project": project, "compose": dict(compose)},
            timeout=COMPOSE_TIMEOUT_SECONDS,
        )
        return _require_list(payload, "containers", "compose up")

    async def down_compose(self, project: str) -> None:
        await self._request(
            "POST",
            "/compose/down",
            json_body={"project": project},
            timeout=COMPOSE_TIMEOUT_SECONDS,
        )

    async def compose_status(self, project: str) -> list[dict[str, Any]]:
        payload = await self._request("GET", "/compose/status", params={"project": project})
        return _require_list(payload, "containers", "compose status")

    # Containers and logs --------------------------------------------------

    async def list_containers(
        self, *, deployment: str | None = None, role: str | None = None
    ) -> list[dict[str, Any]]:
        params: dict[str, str] = {}
        if deployment is not None:
            params["deployment"] = deployment
        if role is not None:
            params["role"] = role
        payload = await self._request("GET", "/containers", params=params or None)
        return _require_list(payload, "containers", "container list")

    async def container_logs(self, container_id: str, *, tail: int = 200) -> str:
        payload = await self._request(
            "GET",
            "/containers/logs",
            params={"container_id": container_id, "tail": tail},
        )
        return _require_str(payload, "logs", "container logs")

    async def follow_container_logs(
        self, container_id: str, *, tail: int = 200
    ) -> AsyncIterator[str]:
        headers = self._auth_headers()
        try:
            async with self._client.stream(
                "GET",
                "/containers/logs/stream",
                params={"container_id": container_id, "tail": tail},
                headers=headers,
                timeout=STREAM_TIMEOUT,
            ) as response:
                if response.status_code >= 400:
                    await response.aread()
                    raise AgentError(
                        f"GET /containers/logs/stream returned {response.status_code}: "
                        f"{_error_detail(response)}",
                        status_code=response.status_code,
                    )
                async for line in response.aiter_lines():
                    yield line
        except httpx.HTTPError as exc:
            raise AgentUnreachableError(f"GET /containers/logs/stream failed: {exc}") from exc

    # Models ---------------------------------------------------------------

    async def models(self) -> dict[str, Any]:
        return await self._request("GET", "/models")

    async def start_model_download(
        self,
        repo: str,
        *,
        root: str | None = None,
        revision: str | None = None,
        allow_patterns: list[str] | None = None,
    ) -> str:
        payload = await self._request(
            "POST",
            "/models/download",
            json_body={
                "repo": repo,
                "root": root,
                "revision": revision,
                "allow_patterns": allow_patterns,
            },
        )
        return _require_str(payload, "job_id", "model download")

    async def list_model_jobs(self) -> list[dict[str, Any]]:
        payload = await self._request("GET", "/models/jobs")
        return _require_list(payload, "jobs", "model jobs")

    async def model_job(self, job_id: str) -> dict[str, Any]:
        return await self._request("GET", f"/models/jobs/{_quote(job_id)}")

    async def cancel_model_job(self, job_id: str) -> bool:
        payload = await self._request("POST", f"/models/jobs/{_quote(job_id)}/cancel")
        return _require_bool(payload, "cancelled", "model job cancel")

    async def remove_model(
        self,
        repo: str,
        *,
        root: str | None = None,
        confirm: bool = False,
        in_use: Sequence[str] = (),
    ) -> None:
        await self._request(
            "POST",
            "/models/remove",
            json_body={
                "repo": repo,
                "root": root,
                "confirm": confirm,
                "in_use": list(in_use),
            },
        )

    # Jobs -----------------------------------------------------------------

    async def start_job(
        self,
        *,
        name: str,
        image: str,
        command: Sequence[str],
        labels: Mapping[str, str],
        volumes: Sequence[str] = (),
        environment: Mapping[str, str] | None = None,
    ) -> str:
        payload = await self._request(
            "POST",
            "/jobs",
            json_body={
                "name": name,
                "image": image,
                "command": list(command),
                "labels": dict(labels),
                "volumes": list(volumes),
                "environment": dict(environment or {}),
            },
        )
        return _require_str(payload, "job_id", "job start")

    async def job_status(self, job_id: str) -> dict[str, Any]:
        return await self._request("GET", f"/jobs/{_quote(job_id)}")

    async def cancel_job(self, job_id: str) -> bool:
        payload = await self._request("POST", f"/jobs/{_quote(job_id)}/cancel")
        return _require_bool(payload, "cancelled", "job cancel")

    async def list_jobs(self) -> list[dict[str, Any]]:
        payload = await self._request("GET", "/jobs")
        return _require_list(payload, "jobs", "job list")

    # Metrics --------------------------------------------------------------

    async def metrics(self) -> dict[str, Any]:
        return await self._request("GET", "/metrics")

    # Internals ------------------------------------------------------------

    def _auth_headers(self) -> dict[str, str]:
        return {"Authorization": f"Bearer {self._token}"}

    async def _request(
        self,
        method: str,
        path: str,
        *,
        auth: bool = True,
        json_body: Any = None,
        params: Mapping[str, Any] | None = None,
        timeout: float | None = None,
    ) -> Any:
        headers = self._auth_headers() if auth else {}
        try:
            response = await self._client.request(
                method,
                path,
                json=json_body,
                params=params,
                headers=headers,
                timeout=timeout,
            )
        except httpx.HTTPError as exc:
            raise AgentUnreachableError(f"{method} {path} failed: {exc}") from exc

        if response.status_code >= 400:
            raise AgentError(
                f"{method} {path} returned {response.status_code}: {_error_detail(response)}",
                status_code=response.status_code,
            )
        if response.status_code == 204 or not response.content:
            return None
        try:
            return response.json()
        except ValueError as exc:
            raise AgentError(
                f"{method} {path} returned invalid JSON",
                status_code=response.status_code,
            ) from exc


def _validate_base_url(base_url: str) -> None:
    if not isinstance(base_url, str):
        raise ValueError("agent base URL must be a string")
    try:
        parsed = urlparse(base_url)
    except ValueError as exc:
        raise ValueError(f"invalid agent base URL: {base_url!r}") from exc
    if parsed.scheme.lower() not in ("http", "https"):
        raise ValueError(f"invalid agent base URL scheme: {base_url!r}")
    if not parsed.hostname:
        raise ValueError(f"invalid agent base URL host: {base_url!r}")
    if parsed.username or parsed.password:
        raise ValueError("agent base URL must not include credentials")
    if parsed.path not in ("", "/"):
        raise ValueError("agent base URL must not include a path")
    if parsed.query or parsed.fragment:
        raise ValueError("agent base URL must not include a query or fragment")
    try:
        parsed.port
    except ValueError as exc:
        raise ValueError(f"invalid agent base URL port: {base_url!r}") from exc


def _quote(value: str) -> str:
    return quote(str(value), safe="")


def _error_detail(response: httpx.Response) -> str:
    try:
        body = response.json()
    except ValueError:
        return response.text.strip() or response.reason_phrase
    if isinstance(body, dict) and "detail" in body:
        return str(body["detail"])
    return response.text.strip() or response.reason_phrase


def _require_key(payload: Any, key: str, context: str) -> Any:
    if not isinstance(payload, dict) or key not in payload:
        raise AgentError(f"{context} response is missing {key!r}")
    return payload[key]


def _require_list(payload: Any, key: str, context: str) -> list[dict[str, Any]]:
    value = _require_key(payload, key, context)
    if not isinstance(value, list):
        raise AgentError(f"{context} response {key!r} is not a list")
    return value


def _require_str(payload: Any, key: str, context: str) -> str:
    value = _require_key(payload, key, context)
    if not isinstance(value, str):
        raise AgentError(f"{context} response {key!r} is not a string")
    return value


def _require_bool(payload: Any, key: str, context: str) -> bool:
    value = _require_key(payload, key, context)
    if not isinstance(value, bool):
        raise AgentError(f"{context} response {key!r} is not a boolean")
    return value
