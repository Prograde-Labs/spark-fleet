"""Fleet gateway: alias routing, primary routing, and inference accounting.

The gateway builds a routing table from loaded deployments. Each deployment's
first heavy role is its head; aliases and the served model name route to that
head's inference port. ``spark-active`` routes to the primary deployment and is
served from the primary's served name. Unknown models return 404, loading
deployments return 503 with a retry hint, and duplicate aliases are rejected.
Requests are forwarded to the upstream inference server with the model field
rewritten when needed; streaming responses pass through while usage is parsed
from JSON or final SSE chunks and accumulated for periodic flushing into the
metrics store.
"""

from __future__ import annotations

import json
import sqlite3
import time
from collections.abc import AsyncIterator, Mapping
from dataclasses import dataclass
from typing import Any

import httpx

from spark_fleet.controller.metrics import MetricsStore
from spark_fleet.shared.db import Database
from spark_fleet.shared.recipe import RecipeError, parse_recipe

LOADING_STATUSES = frozenset({"pending", "loading", "starting"})
_STRIP_REQUEST = frozenset(
    {
        "authorization",
        "cookie",
        "x-csrf-token",
    }
)
_RESPONSE_STRIP = frozenset({"content-length", "content-encoding"})
_HOP_BY_HOP = frozenset(
    {
        "host",
        "content-length",
        "connection",
        "keep-alive",
        "proxy-authenticate",
        "proxy-authorization",
        "te",
        "trailers",
        "transfer-encoding",
        "upgrade",
    }
)


class GatewayError(RuntimeError):
    """Base class for gateway routing failures."""

    def __init__(
        self,
        message: str,
        *,
        status_code: int,
        retry_after: int | None = None,
    ):
        super().__init__(message)
        self.status_code = status_code
        self.retry_after = retry_after


class ModelNotFound(GatewayError):
    def __init__(self, model: str):
        super().__init__(f"unknown model: {model}", status_code=404)


class ModelLoading(GatewayError):
    def __init__(self, model: str, retry_after: int = 5):
        super().__init__(f"model {model} is loading", status_code=503, retry_after=retry_after)


class GatewayUnavailable(GatewayError):
    def __init__(self, message: str):
        super().__init__(message, status_code=503)


@dataclass(frozen=True)
class Route:
    deployment_id: str
    deployment_name: str
    served_name: str
    aliases: frozenset[str]
    upstream_url: str
    status: str


@dataclass
class Counter:
    requests: int = 0
    errors: int = 0
    latency_total_ms: float = 0.0
    latency_count: int = 0
    ttft_total_ms: float = 0.0
    ttft_count: int = 0
    prompt_tokens: int = 0
    completion_tokens: int = 0

    def average_latency_ms(self) -> float | None:
        return self.latency_total_ms / self.latency_count if self.latency_count else None

    def average_ttft_ms(self) -> float | None:
        return self.ttft_total_ms / self.ttft_count if self.ttft_count else None


class InferenceCounters:
    """Accumulate per-deployment request, latency, and token counters."""

    def __init__(self) -> None:
        self._counters: dict[str, Counter] = {}

    def record(
        self,
        deployment_id: str,
        *,
        status_code: int,
        latency_ms: float,
        ttft_ms: float | None = None,
        prompt_tokens: int = 0,
        completion_tokens: int = 0,
    ) -> None:
        counter = self._counters.setdefault(deployment_id, Counter())
        counter.requests += 1
        if status_code >= 400:
            counter.errors += 1
        counter.latency_total_ms += latency_ms
        counter.latency_count += 1
        if ttft_ms is not None:
            counter.ttft_total_ms += ttft_ms
            counter.ttft_count += 1
        counter.prompt_tokens += max(0, prompt_tokens)
        counter.completion_tokens += max(0, completion_tokens)

    def snapshot(self) -> dict[str, Counter]:
        return {
            deployment_id: Counter(
                requests=counter.requests,
                errors=counter.errors,
                latency_total_ms=counter.latency_total_ms,
                latency_count=counter.latency_count,
                ttft_total_ms=counter.ttft_total_ms,
                ttft_count=counter.ttft_count,
                prompt_tokens=counter.prompt_tokens,
                completion_tokens=counter.completion_tokens,
            )
            for deployment_id, counter in self._counters.items()
        }

    def flush(self, store: MetricsStore, *, ts: float | None = None) -> None:
        try:
            for deployment_id, counter in self._counters.items():
                try:
                    store.record_gateway_sample(
                        deployment_id,
                        requests=counter.requests,
                        errors=counter.errors,
                        latency_ms=counter.average_latency_ms(),
                        ttft_ms=counter.average_ttft_ms(),
                        prompt_tokens=counter.prompt_tokens,
                        completion_tokens=counter.completion_tokens,
                        ts=ts,
                    )
                except (ValueError, sqlite3.Error):
                    # A deployment may have been deleted between routing and
                    # flushing; drop that sample rather than wedging the loop.
                    continue
        finally:
            self._counters.clear()


class FleetGateway:
    """Resolve aliases to deployment heads and proxy inference requests."""

    def __init__(
        self,
        db: Database,
        *,
        transport: httpx.AsyncBaseTransport | None = None,
        timeout: float = 300.0,
    ):
        self._db = db
        self._client = httpx.AsyncClient(timeout=timeout, transport=transport)
        self.counters = InferenceCounters()

    async def aclose(self) -> None:
        await self._client.aclose()

    async def __aenter__(self) -> FleetGateway:
        return self

    async def __aexit__(self, *exc_info) -> None:
        await self.aclose()

    def routing_table(self) -> dict[str, Route]:
        routes: dict[str, Route] = {}
        primary: Route | None = None
        primary_loading = False

        for deployment in self._db.list_deployments():
            status = deployment["status"]
            if status != "loaded" and status not in LOADING_STATUSES:
                continue
            try:
                recipe = parse_recipe(deployment["recipe"])
            except RecipeError:
                continue
            heavy = [role for role in recipe.spec.roles if role.class_ == "heavy"]
            if recipe.spec.mode != "deployment" or not heavy:
                continue
            head = max(heavy, key=lambda role: (role.start_order, role.name == "head"))
            if head.inference_port is None:
                continue
            node = self._db.get_node(head.node)
            if node is None:
                continue
            aliases = set(recipe.spec.gateway.aliases) if recipe.spec.gateway else set()
            served_name = recipe.spec.model.served_name if recipe.spec.model else None
            if served_name is not None:
                aliases.add(served_name)
            if not aliases:
                continue
            if "spark-active" in aliases:
                raise GatewayError(
                    "spark-active is reserved for the primary deployment", status_code=409
                )
            route = Route(
                deployment_id=deployment["id"],
                deployment_name=deployment["name"],
                served_name=served_name or deployment["name"],
                aliases=frozenset(aliases),
                upstream_url=f"http://{node['lan_ip']}:{head.inference_port}",
                status="loaded" if status == "loaded" else "loading",
            )
            for alias in aliases:
                existing = routes.get(alias)
                if existing is not None and existing.deployment_id != route.deployment_id:
                    raise GatewayError(
                        f"duplicate alias {alias!r} for {existing.deployment_name} "
                        f"and {route.deployment_name}",
                        status_code=409,
                    )
                routes[alias] = route
            if deployment["is_primary"]:
                primary = route
                primary_loading = status != "loaded"

        if primary is not None:
            routes["spark-active"] = Route(
                deployment_id=primary.deployment_id,
                deployment_name=primary.deployment_name,
                served_name=primary.served_name,
                aliases=primary.aliases | {"spark-active"},
                upstream_url=primary.upstream_url,
                status="loading" if primary_loading else "loaded",
            )
        return routes

    def resolve(self, model: str) -> Route:
        routes = self.routing_table()
        if model == "spark-active":
            route = routes.get("spark-active")
            if route is None:
                raise GatewayUnavailable("no primary deployment is loaded")
            if route.status != "loaded":
                raise ModelLoading(model)
            return route
        route = routes.get(model)
        if route is None:
            raise ModelNotFound(model)
        if route.status != "loaded":
            raise ModelLoading(model)
        return route

    def models(self) -> dict[str, Any]:
        routes = self.routing_table()
        ids: set[str] = set()
        for route in routes.values():
            if route.status != "loaded":
                continue
            ids.add(route.served_name)
            ids.update(route.aliases)
        return {
            "object": "list",
            "data": [
                {
                    "id": model_id,
                    "object": "model",
                    "created": 0,
                    "owned_by": "spark-fleet",
                }
                for model_id in sorted(ids)
            ],
        }

    async def proxy(
        self,
        model: str,
        method: str,
        path: str,
        headers: Mapping[str, str],
        body: bytes,
    ) -> httpx.Response:
        route = self.resolve(model)
        request = self._build_request(route, model, method, path, headers, body)
        started = time.perf_counter()
        try:
            response = await self._client.send(request)
        except httpx.HTTPError as exc:
            self.counters.record(
                route.deployment_id,
                status_code=502,
                latency_ms=(time.perf_counter() - started) * 1000,
            )
            raise GatewayError(f"upstream request failed: {exc}", status_code=502) from exc
        latency_ms = (time.perf_counter() - started) * 1000
        prompt, completion = extract_usage(response)
        self.counters.record(
            route.deployment_id,
            status_code=response.status_code,
            latency_ms=latency_ms,
            prompt_tokens=prompt,
            completion_tokens=completion,
        )
        return response

    async def stream(
        self,
        model: str,
        method: str,
        path: str,
        headers: Mapping[str, str],
        body: bytes,
    ) -> StreamProxy:
        route = self.resolve(model)
        request = self._build_request(route, model, method, path, headers, body)
        started = time.perf_counter()
        try:
            upstream = await self._client.send(request, stream=True)
        except httpx.HTTPError as exc:
            self.counters.record(
                route.deployment_id,
                status_code=502,
                latency_ms=(time.perf_counter() - started) * 1000,
            )
            raise GatewayError(f"upstream request failed: {exc}", status_code=502) from exc
        return StreamProxy(
            upstream=upstream,
            deployment_id=route.deployment_id,
            started=started,
            counters=self.counters,
        )

    def _build_request(
        self,
        route: Route,
        model: str,
        method: str,
        path: str,
        headers: Mapping[str, str],
        body: bytes,
    ) -> httpx.Request:
        forwarded = {
            key.lower(): value
            for key, value in headers.items()
            if key.lower() not in _HOP_BY_HOP and key.lower() not in _STRIP_REQUEST
        }
        content_type = forwarded.get("content-type", "").lower()
        if "application/json" in content_type and body:
            try:
                payload = json.loads(body)
            except (ValueError, UnicodeDecodeError):
                payload = None
            if isinstance(payload, dict) and payload.get("model") == model:
                payload["model"] = route.served_name
                body = json.dumps(payload, separators=(",", ":")).encode()
        return self._client.build_request(
            method,
            f"{route.upstream_url}{path}",
            headers=forwarded,
            content=body,
        )


class StreamProxy:
    """A streamed upstream response that records usage when fully consumed."""

    def __init__(
        self,
        *,
        upstream: httpx.Response,
        deployment_id: str,
        started: float,
        counters: InferenceCounters,
    ):
        self._upstream = upstream
        self._deployment_id = deployment_id
        self._started = started
        self._counters = counters
        self._chunks: list[str] = []
        self._ttft_ms: float | None = None

    @property
    def status_code(self) -> int:
        return self._upstream.status_code

    @property
    def headers(self) -> Mapping[str, str]:
        return self._upstream.headers

    async def aiter_bytes(self) -> AsyncIterator[bytes]:
        try:
            async for chunk in self._upstream.aiter_bytes():
                if self._ttft_ms is None:
                    self._ttft_ms = (time.perf_counter() - self._started) * 1000
                self._chunks.append(chunk.decode("utf-8", errors="replace"))
                yield chunk
        finally:
            latency_ms = (time.perf_counter() - self._started) * 1000
            prompt, completion = parse_sse_usage("".join(self._chunks))
            self._counters.record(
                self._deployment_id,
                status_code=self.status_code,
                latency_ms=latency_ms,
                ttft_ms=self._ttft_ms,
                prompt_tokens=prompt,
                completion_tokens=completion,
            )
            await self._upstream.aclose()

    async def aclose(self) -> None:
        await self._upstream.aclose()


def filter_response_headers(headers: Mapping[str, str]) -> dict[str, str]:
    """Drop headers that no longer describe the decoded response body."""

    return {
        key: value
        for key, value in headers.items()
        if key.lower() not in _HOP_BY_HOP and key.lower() not in _RESPONSE_STRIP
    }


def extract_usage(response: httpx.Response) -> tuple[int, int]:
    try:
        payload = response.json()
    except ValueError:
        return 0, 0
    return usage_from_payload(payload)


def usage_from_payload(payload: Any) -> tuple[int, int]:
    if not isinstance(payload, Mapping):
        return 0, 0
    usage = payload.get("usage")
    if not isinstance(usage, Mapping):
        return 0, 0
    prompt = usage.get("prompt_tokens", usage.get("input_tokens", 0))
    completion = usage.get("completion_tokens", usage.get("output_tokens", 0))
    return _as_int(prompt), _as_int(completion)


def parse_sse_usage(text: str) -> tuple[int, int]:
    for line in text.splitlines():
        line = line.strip()
        if not line.startswith("data:"):
            continue
        data = line[len("data:") :].strip()
        if not data or data == "[DONE]":
            continue
        try:
            payload = json.loads(data)
        except ValueError:
            continue
        prompt, completion = usage_from_payload(payload)
        if prompt or completion:
            return prompt, completion
    return 0, 0


def _as_int(value: Any) -> int:
    if isinstance(value, bool):
        return 0
    try:
        return max(0, int(value))
    except (TypeError, ValueError):
        return 0
