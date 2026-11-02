"""Tests for node registry CRUD and health/info checks."""

import pytest

from spark_fleet.controller.agent_client import AgentUnreachableError
from spark_fleet.controller.nodes import Node, NodeError, NodeRegistry, NodeStatus
from spark_fleet.shared.db import Database, DatabaseError

TOKEN = "agent-token-0123456789"


@pytest.fixture
def db(tmp_path):
    return Database(tmp_path / "fleet.db")


@pytest.fixture
def registry(db):
    return NodeRegistry(db, TOKEN)


def make_node(registry, node_id="spark-01", **overrides):
    values = {
        "display_name": "Spark 01",
        "lan_ip": "192.0.2.239",
        "fabric_ip": "198.51.100.10",
        "agent_url": "http://192.0.2.239:8092",
    }
    values.update(overrides)
    return registry.create(node_id, **values)


def test_create_node(registry):
    node = make_node(registry)

    assert isinstance(node, Node)
    assert node.id == "spark-01"
    assert node.display_name == "Spark 01"
    assert node.lan_ip == "192.0.2.239"
    assert node.fabric_ip == "198.51.100.10"
    assert node.agent_url == "http://192.0.2.239:8092"
    assert node.enabled is True
    assert node.last_seen_at is None
    assert node.info is None


def test_create_duplicate_node_raises(registry):
    make_node(registry)

    with pytest.raises(NodeError, match="already exists"):
        make_node(registry)


@pytest.mark.parametrize("node_id", ["", "bad id", "spark/01", "a" * 64])
def test_create_rejects_invalid_node_id(registry, node_id):
    with pytest.raises(NodeError, match="invalid node id"):
        make_node(registry, node_id=node_id)


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("display_name", ""),
        ("display_name", "bad\nname"),
        ("display_name", "x" * 129),
        ("lan_ip", "not-an-ip"),
        ("lan_ip", "999.1.1.1"),
        ("fabric_ip", "198.51.100"),
    ],
)
def test_create_rejects_invalid_fields(registry, field, value):
    with pytest.raises(NodeError):
        make_node(registry, **{field: value})


@pytest.mark.parametrize(
    "url",
    [
        "ftp://192.0.2.239:8092",
        "192.0.2.239:8092",
        "http://user:pass@192.0.2.239:8092",
        "http://192.0.2.239:8092/path",
        "http://192.0.2.239:8092?x=1",
        "http://192.0.2.239:8092#frag",
        "http://192.0.2.239:99999",
        "http://:8092",
    ],
)
def test_create_rejects_invalid_agent_url(registry, url):
    with pytest.raises(NodeError, match="agent url"):
        make_node(registry, agent_url=url)


def test_create_normalizes_agent_url(registry):
    node = make_node(registry, agent_url="HTTP://192.0.2.239:8092/")

    assert node.agent_url == "http://192.0.2.239:8092"


def test_update_node_partially(registry, db):
    node = make_node(registry)
    db.upsert_node(
        node.id,
        display_name=node.display_name,
        lan_ip=node.lan_ip,
        fabric_ip=node.fabric_ip,
        agent_url=node.agent_url,
        info={"node_id": node.id},
    )

    updated = registry.update(node.id, display_name="Renamed", enabled=False)

    assert updated.display_name == "Renamed"
    assert updated.enabled is False
    assert updated.lan_ip == node.lan_ip
    assert updated.fabric_ip == node.fabric_ip
    assert updated.agent_url == node.agent_url
    assert updated.info == {"node_id": node.id}
    assert updated.created_at == node.created_at


def test_update_missing_node_raises(registry):
    with pytest.raises(NodeError, match="not found"):
        registry.update("missing", display_name="Renamed")


def test_delete_node(registry):
    make_node(registry)

    registry.delete("spark-01")

    assert registry.get("spark-01") is None


def test_delete_missing_node_raises(registry):
    with pytest.raises(NodeError, match="not found"):
        registry.delete("missing")


def test_delete_referenced_node_wraps_database_error(registry, monkeypatch):
    make_node(registry)

    def fail(_node_id):
        raise DatabaseError("node spark-01 is referenced by deployment roles")

    monkeypatch.setattr(registry._db, "delete_node", fail)

    with pytest.raises(NodeError, match="referenced"):
        registry.delete("spark-01")


def test_list_nodes_sorted_by_id(registry):
    make_node(registry, "spark-02")
    make_node(registry, "spark-01")

    assert [node.id for node in registry.list()] == ["spark-01", "spark-02"]


def test_get_and_require(registry):
    make_node(registry)

    assert registry.get("spark-01").display_name == "Spark 01"
    assert registry.get("missing") is None
    assert registry.require("spark-01").id == "spark-01"
    with pytest.raises(NodeError, match="not found"):
        registry.require("missing")


class FakeClient:
    def __init__(self, health=None, info=None, error=None):
        self.health_value = health
        self.info_value = info
        self.error = error
        self.closed = False

    async def health(self):
        if self.error is not None:
            raise self.error
        return self.health_value

    async def info(self):
        if self.error is not None:
            raise self.error
        return self.info_value

    async def aclose(self):
        self.closed = True


def fake_factory(client):
    calls = []

    def factory(url, token):
        calls.append((url, token))
        return client

    return factory, calls


async def test_check_healthy_updates_last_seen_and_info(registry, db):
    make_node(registry)
    info = {"node_id": "spark-01", "model_cache_roots": ["/srv/spark-fleet/hub"]}
    client = FakeClient(health={"status": "ok"}, info=info)
    factory, calls = fake_factory(client)
    registry = NodeRegistry(db, TOKEN, client_factory=factory)

    status = await registry.check("spark-01")

    assert isinstance(status, NodeStatus)
    assert status.healthy is True
    assert status.health == {"status": "ok"}
    assert status.info == info
    assert status.error is None
    assert calls == [("http://192.0.2.239:8092", TOKEN)]
    assert client.closed is True

    stored = db.get_node("spark-01")
    assert stored["last_seen_at"] is not None
    assert stored["info"] == info


async def test_check_unhealthy_reports_error_without_updating(registry, db):
    make_node(registry)
    client = FakeClient(error=AgentUnreachableError("connection refused"))
    factory, _ = fake_factory(client)
    registry = NodeRegistry(db, TOKEN, client_factory=factory)

    status = await registry.check("spark-01")

    assert status.healthy is False
    assert status.health is None
    assert status.info is None
    assert "connection refused" in status.error
    assert client.closed is True
    assert db.get_node("spark-01")["last_seen_at"] is None


async def test_check_missing_node_raises(registry):
    with pytest.raises(NodeError, match="not found"):
        await registry.check("missing")


async def test_refresh_all_checks_every_node(registry, db):
    make_node(registry, "spark-01")
    make_node(
        registry,
        "spark-02",
        lan_ip="192.0.2.79",
        fabric_ip="198.51.100.11",
        agent_url="http://192.0.2.79:8092",
    )
    clients = {
        "spark-01": FakeClient(health={"status": "ok"}, info={"node_id": "spark-01"}),
        "spark-02": FakeClient(error=AgentUnreachableError("down")),
    }

    def factory(url, token):
        return clients["spark-01" if "239" in url else "spark-02"]

    registry = NodeRegistry(db, TOKEN, client_factory=factory)

    statuses = await registry.refresh_all()

    assert [status.node.id for status in statuses] == ["spark-01", "spark-02"]
    assert [status.healthy for status in statuses] == [True, False]
    assert db.get_node("spark-02")["last_seen_at"] is None
