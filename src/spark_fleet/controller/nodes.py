"""Node registry: validated CRUD plus agent health/info checks.

Nodes are stored in SQLite and validated before they reach the database. Health
checks call the agent's public ``/health`` and authenticated ``/info`` endpoints,
then record ``last_seen_at`` and the reported node metadata. A node that cannot
be reached stays registered with its last known info so the UI can show it as
offline.
"""

from __future__ import annotations

import asyncio
import ipaddress
import re
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from typing import Any
from urllib.parse import urlparse, urlunsplit

from spark_fleet.controller.agent_client import AgentClient, AgentError
from spark_fleet.shared.db import Database, DatabaseError
from spark_fleet.shared.recipe import NODE_ID_PATTERN

_NODE_ID_RE = re.compile(NODE_ID_PATTERN)
_MAX_DISPLAY_NAME = 128


class NodeError(RuntimeError):
    """Raised when a node definition or operation is invalid."""


@dataclass(frozen=True)
class Node:
    id: str
    display_name: str
    lan_ip: str
    fabric_ip: str
    agent_url: str
    enabled: bool
    created_at: float
    updated_at: float
    last_seen_at: float | None
    info: Mapping[str, Any] | None

    @classmethod
    def from_row(cls, row: Mapping[str, Any]) -> Node:
        info = row.get("info")
        return cls(
            id=row["id"],
            display_name=row["display_name"],
            lan_ip=row["lan_ip"],
            fabric_ip=row["fabric_ip"],
            agent_url=row["agent_url"],
            enabled=bool(row["enabled"]),
            created_at=row["created_at"],
            updated_at=row["updated_at"],
            last_seen_at=row["last_seen_at"],
            info=info if isinstance(info, Mapping) else None,
        )


@dataclass(frozen=True)
class NodeStatus:
    node: Node
    healthy: bool
    health: Mapping[str, Any] | None
    info: Mapping[str, Any] | None
    error: str | None


class NodeRegistry:
    """Validated node CRUD and health checks backed by the fleet store."""

    def __init__(
        self,
        db: Database,
        token: str,
        *,
        client_factory: Callable[[str, str], AgentClient] | None = None,
    ):
        if not token:
            raise ValueError("agent token must not be empty")
        self._db = db
        self._token = token
        self._client_factory = client_factory or AgentClient

    def create(
        self,
        node_id: str,
        *,
        display_name: str,
        lan_ip: str,
        fabric_ip: str,
        agent_url: str,
        enabled: bool = True,
    ) -> Node:
        node_id = _validate_node_id(node_id)
        display_name = _validate_display_name(display_name)
        lan_ip = _validate_ip(lan_ip, "lan ip")
        fabric_ip = _validate_ip(fabric_ip, "fabric ip")
        agent_url = _validate_agent_url(agent_url)
        enabled = _validate_enabled(enabled)
        if self._db.get_node(node_id) is not None:
            raise NodeError(f"node {node_id} already exists")
        self._db.upsert_node(
            node_id,
            display_name=display_name,
            lan_ip=lan_ip,
            fabric_ip=fabric_ip,
            agent_url=agent_url,
            enabled=enabled,
            info=None,
        )
        return self.require(node_id)

    def update(
        self,
        node_id: str,
        *,
        display_name: str | None = None,
        lan_ip: str | None = None,
        fabric_ip: str | None = None,
        agent_url: str | None = None,
        enabled: bool | None = None,
    ) -> Node:
        existing = self.require(node_id)
        self._db.upsert_node(
            node_id,
            display_name=(
                _validate_display_name(display_name)
                if display_name is not None
                else existing.display_name
            ),
            lan_ip=(_validate_ip(lan_ip, "lan ip") if lan_ip is not None else existing.lan_ip),
            fabric_ip=(
                _validate_ip(fabric_ip, "fabric ip")
                if fabric_ip is not None
                else existing.fabric_ip
            ),
            agent_url=(
                _validate_agent_url(agent_url) if agent_url is not None else existing.agent_url
            ),
            enabled=(_validate_enabled(enabled) if enabled is not None else existing.enabled),
        )
        return self.require(node_id)

    def delete(self, node_id: str) -> None:
        self.require(node_id)
        try:
            self._db.delete_node(node_id)
        except DatabaseError as exc:
            raise NodeError(str(exc)) from exc

    def get(self, node_id: str) -> Node | None:
        _validate_node_id(node_id)
        row = self._db.get_node(node_id)
        return None if row is None else Node.from_row(row)

    def require(self, node_id: str) -> Node:
        node = self.get(node_id)
        if node is None:
            raise NodeError(f"node {node_id} not found")
        return node

    def list(self) -> list[Node]:
        return [Node.from_row(row) for row in self._db.list_nodes()]

    async def check(self, node_id: str) -> NodeStatus:
        node = self.require(node_id)
        client = self._client_factory(node.agent_url, self._token)
        try:
            health = await client.health()
            info = await client.info()
        except AgentError as exc:
            return NodeStatus(
                node=node,
                healthy=False,
                health=None,
                info=None,
                error=str(exc),
            )
        finally:
            await client.aclose()

        self._db.touch_node(node_id)
        self._db.upsert_node(
            node_id,
            display_name=node.display_name,
            lan_ip=node.lan_ip,
            fabric_ip=node.fabric_ip,
            agent_url=node.agent_url,
            enabled=node.enabled,
            info=info,
        )
        return NodeStatus(
            node=self.require(node_id),
            healthy=True,
            health=health,
            info=info,
            error=None,
        )

    async def refresh_all(self) -> list[NodeStatus]:
        nodes = self.list()
        if not nodes:
            return []
        return await asyncio.gather(*(self.check(node.id) for node in nodes))


def _validate_node_id(node_id: str) -> str:
    if not isinstance(node_id, str) or not _NODE_ID_RE.fullmatch(node_id):
        raise NodeError(f"invalid node id: {node_id!r}")
    return node_id


def _validate_display_name(value: str) -> str:
    if not isinstance(value, str):
        raise NodeError("display name must be a string")
    value = value.strip()
    if (
        not value
        or len(value) > _MAX_DISPLAY_NAME
        or any(character in value for character in ("\x00", "\r", "\n"))
    ):
        raise NodeError(f"invalid display name: {value!r}")
    return value


def _validate_ip(value: str, field: str) -> str:
    if not isinstance(value, str):
        raise NodeError(f"invalid {field}: {value!r}")
    try:
        address = ipaddress.ip_address(value)
    except ValueError as exc:
        raise NodeError(f"invalid {field}: {value!r}") from exc
    return str(address)


def _validate_agent_url(value: str) -> str:
    if not isinstance(value, str):
        raise NodeError("agent url must be a string")
    try:
        parsed = urlparse(value)
    except ValueError as exc:
        raise NodeError(f"invalid agent url: {value!r}") from exc
    if parsed.scheme.lower() not in ("http", "https"):
        raise NodeError(f"invalid agent url scheme: {value!r}")
    if not parsed.hostname:
        raise NodeError(f"invalid agent url host: {value!r}")
    if parsed.username or parsed.password:
        raise NodeError("agent url must not include credentials")
    if parsed.path not in ("", "/"):
        raise NodeError("agent url must not include a path")
    if parsed.query or parsed.fragment:
        raise NodeError("agent url must not include a query or fragment")
    try:
        parsed.port
    except ValueError as exc:
        raise NodeError(f"invalid agent url port: {value!r}") from exc
    return urlunsplit((parsed.scheme.lower(), parsed.netloc, "", "", ""))


def _validate_enabled(value: bool) -> bool:
    if not isinstance(value, bool):
        raise NodeError("enabled must be a boolean")
    return value
