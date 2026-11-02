"""Tests for agent container inspection, memory stats, health, and log streaming."""

import subprocess

import pytest

from spark_fleet.agent import docker_ops
from spark_fleet.agent.containers import (
    ContainerError,
    ContainerInfo,
    container_logs,
    follow_container_logs,
    managed_containers,
    parse_size,
)
from spark_fleet.shared.render import LABEL_DEPLOYMENT, LABEL_ROLE

PROJECT = "spark-fleet-demo-glm-head"


def completed(args, returncode=0, stdout="", stderr=""):
    return subprocess.CompletedProcess(
        args=args, returncode=returncode, stdout=stdout, stderr=stderr
    )


def ps_row(container_id="abc123def456", state="running", status="Up 2 minutes (healthy)"):
    return {
        "ID": container_id,
        "Names": "spark-fleet-demo-glm-head",
        "State": state,
        "Status": status,
        "Labels": f"{LABEL_DEPLOYMENT}=demo-glm,{LABEL_ROLE}=head",
    }


def inspect_record(container_id="abc123def456", health="healthy"):
    return {
        "Id": container_id + "0" * 52,
        "Name": "/spark-fleet-demo-glm-head",
        "State": {
            "Status": "running",
            "Health": {"Status": health} if health is not None else {},
        },
        "Config": {"Labels": {LABEL_DEPLOYMENT: "demo-glm", LABEL_ROLE: "head"}},
    }


def stats_sample(container_id="abc123def456", usage="1.5GiB / 8GiB", percent="18.75%"):
    return {
        "ID": container_id,
        "Name": "spark-fleet-demo-glm-head",
        "MemUsage": usage,
        "MemPerc": percent,
    }


def patch_listing(
    monkeypatch,
    rows=None,
    inspections=None,
    samples=None,
    filters=None,
):
    rows = [ps_row()] if rows is None else rows
    inspections = [inspect_record()] if inspections is None else inspections
    samples = [stats_sample()] if samples is None else samples
    calls = {"filters": [], "inspect": [], "stats": []}

    def fake_list(label_filters, **kwargs):
        calls["filters"].append(dict(label_filters))
        return rows

    def fake_inspect(ids, **kwargs):
        calls["inspect"].append(list(ids))
        return inspections

    def fake_stats(ids, **kwargs):
        calls["stats"].append(list(ids))
        return samples

    monkeypatch.setattr(
        "spark_fleet.agent.containers.docker_ops.list_containers_by_labels", fake_list
    )
    monkeypatch.setattr("spark_fleet.agent.containers.docker_ops.inspect_containers", fake_inspect)
    monkeypatch.setattr("spark_fleet.agent.containers.docker_ops.container_stats", fake_stats)
    return calls


# docker_ops ---------------------------------------------------------------


def test_list_containers_by_labels_builds_presence_and_value_filters(monkeypatch):
    calls = []
    monkeypatch.setattr(
        docker_ops.subprocess,
        "run",
        lambda args, **kwargs: calls.append(args) or completed(args, stdout=""),
    )

    assert docker_ops.list_containers_by_labels({LABEL_DEPLOYMENT: None, LABEL_ROLE: "head"}) == []
    assert calls[0] == [
        "docker",
        "ps",
        "-a",
        "--filter",
        f"label={LABEL_DEPLOYMENT}",
        "--filter",
        f"label={LABEL_ROLE}=head",
        "--format",
        "{{json .}}",
    ]


def test_inspect_containers_parses_json(monkeypatch):
    payload = '[{"Id": "abc", "State": {"Status": "running"}}]'
    monkeypatch.setattr(
        docker_ops.subprocess, "run", lambda args, **kwargs: completed(args, stdout=payload)
    )

    records = docker_ops.inspect_containers(["abc"])

    assert records == [{"Id": "abc", "State": {"Status": "running"}}]


def test_inspect_containers_skips_empty_ids(monkeypatch):
    monkeypatch.setattr(
        docker_ops.subprocess,
        "run",
        lambda *a, **k: pytest.fail("docker inspect should not run"),
    )

    assert docker_ops.inspect_containers([]) == []


def test_container_stats_parses_ndjson(monkeypatch):
    output = '{"ID": "abc", "MemUsage": "1GiB / 8GiB"}\n{"ID": "def", "MemUsage": "2GiB / 8GiB"}\n'
    monkeypatch.setattr(
        docker_ops.subprocess, "run", lambda args, **kwargs: completed(args, stdout=output)
    )

    samples = docker_ops.container_stats(["abc", "def"])

    assert [sample["ID"] for sample in samples] == ["abc", "def"]


def test_container_logs_builds_command(monkeypatch):
    calls = []
    monkeypatch.setattr(
        docker_ops.subprocess,
        "run",
        lambda args, **kwargs: calls.append((args, kwargs)) or completed(args, stdout="line\n"),
    )

    assert docker_ops.container_logs("abc", tail=50) == "line\n"
    assert calls[0][0] == ["docker", "logs", "--tail", "50", "abc"]
    assert calls[0][1]["stdout"] is subprocess.PIPE
    assert calls[0][1]["stderr"] is subprocess.STDOUT
    assert calls[0][1]["text"] is True


def test_container_stats_builds_command(monkeypatch):
    calls = []
    monkeypatch.setattr(
        docker_ops.subprocess,
        "run",
        lambda args, **kwargs: calls.append(args) or completed(args, stdout=""),
    )

    docker_ops.container_stats(["abc", "def"])

    assert calls[0] == [
        "docker",
        "stats",
        "--no-stream",
        "--format",
        "{{json .}}",
        "abc",
        "def",
    ]


class FakeProcess:
    def __init__(self, lines):
        self.stdout = iter(lines)
        self.terminated = False
        self.killed = False
        self.waited = False

    def poll(self):
        return None

    def terminate(self):
        self.terminated = True

    def wait(self, timeout=None):
        self.waited = True
        return 0

    def kill(self):
        self.killed = True


def test_follow_container_logs_streams_and_terminates(monkeypatch):
    process = FakeProcess(["one\n", "two\n"])
    calls = []
    monkeypatch.setattr(
        docker_ops.subprocess,
        "Popen",
        lambda args, **kwargs: calls.append((args, kwargs)) or process,
    )

    lines = list(docker_ops.follow_container_logs("abc", tail=10))

    assert lines == ["one\n", "two\n"]
    assert calls[0][0] == ["docker", "logs", "--follow", "--tail", "10", "abc"]
    assert calls[0][1]["stderr"] is subprocess.STDOUT
    assert calls[0][1]["text"] is True
    assert process.terminated is True
    assert process.waited is True


# managed_containers -------------------------------------------------------


def test_managed_containers_merges_inspect_and_stats(monkeypatch):
    calls = patch_listing(monkeypatch)

    infos = managed_containers("demo-glm", "head")

    assert calls["filters"] == [{LABEL_DEPLOYMENT: "demo-glm", LABEL_ROLE: "head"}]
    assert calls["inspect"] == [["abc123def456"]]
    assert calls["stats"] == [["abc123def456"]]
    assert infos == [
        ContainerInfo(
            id="abc123def456",
            name="spark-fleet-demo-glm-head",
            state="running",
            status="Up 2 minutes (healthy)",
            health="healthy",
            memory_usage=1610612736,
            memory_limit=8589934592,
            memory_percent=18.75,
            labels={LABEL_DEPLOYMENT: "demo-glm", LABEL_ROLE: "head"},
        )
    ]


def test_managed_containers_lists_all_fleet_containers(monkeypatch):
    calls = patch_listing(monkeypatch)

    managed_containers()

    assert calls["filters"] == [{LABEL_DEPLOYMENT: None}]


def test_managed_containers_requires_deployment_for_role():
    with pytest.raises(ContainerError, match="deployment"):
        managed_containers(role="head")


@pytest.mark.parametrize(
    ("deployment", "role"),
    [("Bad Name", None), ("demo-glm", "Head"), ("demo-glm", "../x")],
)
def test_managed_containers_rejects_invalid_names(deployment, role):
    with pytest.raises(ContainerError, match="invalid"):
        managed_containers(deployment, role)


def test_managed_containers_without_containers_skips_inspect(monkeypatch):
    calls = patch_listing(monkeypatch, rows=[])

    assert managed_containers("demo-glm") == []
    assert calls["inspect"] == []
    assert calls["stats"] == []


def test_managed_containers_reports_missing_inspection(monkeypatch):
    patch_listing(monkeypatch, inspections=[])

    with pytest.raises(ContainerError, match="did not return"):
        managed_containers("demo-glm")


def test_managed_containers_wraps_docker_errors(monkeypatch):
    def fail(*args, **kwargs):
        raise docker_ops.DockerError("docker ps failed")

    monkeypatch.setattr("spark_fleet.agent.containers.docker_ops.list_containers_by_labels", fail)

    with pytest.raises(ContainerError, match="docker ps failed"):
        managed_containers("demo-glm")


def test_managed_containers_wraps_inspect_errors(monkeypatch):
    patch_listing(monkeypatch)

    def fail(*args, **kwargs):
        raise docker_ops.DockerError("inspect failed")

    monkeypatch.setattr("spark_fleet.agent.containers.docker_ops.inspect_containers", fail)

    with pytest.raises(ContainerError, match="inspect failed"):
        managed_containers("demo-glm")


def test_managed_containers_wraps_stats_errors(monkeypatch):
    patch_listing(monkeypatch)

    def fail(*args, **kwargs):
        raise docker_ops.DockerError("stats failed")

    monkeypatch.setattr("spark_fleet.agent.containers.docker_ops.container_stats", fail)

    with pytest.raises(ContainerError, match="stats failed"):
        managed_containers("demo-glm")


def test_managed_containers_reports_missing_health(monkeypatch):
    patch_listing(
        monkeypatch,
        inspections=[inspect_record(health=None)],
        samples=[],
    )

    info = managed_containers("demo-glm")[0]

    assert info.health is None
    assert info.memory_usage is None
    assert info.memory_limit is None
    assert info.memory_percent is None


def test_managed_containers_reports_starting_health(monkeypatch):
    patch_listing(monkeypatch, inspections=[inspect_record(health="starting")])

    assert managed_containers("demo-glm")[0].health == "starting"


# memory parsing -----------------------------------------------------------


@pytest.mark.parametrize(
    ("text", "expected"),
    [
        ("1B", 1),
        ("1KiB", 1024),
        ("1.5MiB", 1572864),
        ("2GiB", 2 * 1024**3),
        ("1kB", 1000),
        ("1.5MB", 1_500_000),
        ("8GiB", 8 * 1024**3),
        ("1TB", 1000**4),
        ("1TiB", 1024**4),
    ],
)
def test_parse_size(text, expected):
    assert parse_size(text) == expected


@pytest.mark.parametrize("text", ["", "abc", "1XB", "12", "GiB"])
def test_parse_size_rejects_invalid(text):
    assert parse_size(text) is None


# logs ---------------------------------------------------------------------


def test_container_logs_validates_id():
    with pytest.raises(ContainerError, match="invalid container id"):
        container_logs("abc; rm -rf /")


def test_container_logs_rejects_negative_tail():
    with pytest.raises(ContainerError, match="tail"):
        container_logs("abc", tail=-1)


def test_container_logs_rejects_excessive_tail():
    with pytest.raises(ContainerError, match="tail"):
        container_logs("abc", tail=10_001)


def test_container_logs_rejects_unmanaged_container(monkeypatch):
    monkeypatch.setattr(
        "spark_fleet.agent.containers.docker_ops.inspect_containers",
        lambda ids, **kwargs: [
            {"Id": ids[0], "Config": {"Labels": {"com.docker.compose.service": "hermes"}}}
        ],
    )

    with pytest.raises(ContainerError, match="not managed"):
        container_logs("abc")


def test_container_logs_rejects_missing_container(monkeypatch):
    monkeypatch.setattr(
        "spark_fleet.agent.containers.docker_ops.inspect_containers",
        lambda ids, **kwargs: [],
    )

    with pytest.raises(ContainerError, match="not found"):
        container_logs("abc")


def test_container_logs_wraps_docker_errors(monkeypatch):
    monkeypatch.setattr(
        "spark_fleet.agent.containers.docker_ops.inspect_containers",
        lambda ids, **kwargs: [inspect_record()],
    )

    def fail(*args, **kwargs):
        raise docker_ops.DockerError("logs failed")

    monkeypatch.setattr("spark_fleet.agent.containers.docker_ops.container_logs", fail)

    with pytest.raises(ContainerError, match="logs failed"):
        container_logs("abc")


def test_container_logs_returns_output(monkeypatch):
    monkeypatch.setattr(
        "spark_fleet.agent.containers.docker_ops.inspect_containers",
        lambda ids, **kwargs: [inspect_record()],
    )
    monkeypatch.setattr(
        "spark_fleet.agent.containers.docker_ops.container_logs",
        lambda cid, **kwargs: "hello\n",
    )

    assert container_logs("abc", tail=5) == "hello\n"


def test_follow_container_logs_validates_id():
    with pytest.raises(ContainerError, match="invalid container id"):
        list(follow_container_logs("abc; rm -rf /"))


def test_follow_container_logs_rejects_unmanaged_container(monkeypatch):
    monkeypatch.setattr(
        "spark_fleet.agent.containers.docker_ops.inspect_containers",
        lambda ids, **kwargs: [{"Id": ids[0], "Config": {"Labels": {}}}],
    )

    with pytest.raises(ContainerError, match="not managed"):
        list(follow_container_logs("abc"))


def test_follow_container_logs_yields_lines(monkeypatch):
    monkeypatch.setattr(
        "spark_fleet.agent.containers.docker_ops.inspect_containers",
        lambda ids, **kwargs: [inspect_record()],
    )
    monkeypatch.setattr(
        "spark_fleet.agent.containers.docker_ops.follow_container_logs",
        lambda cid, **kwargs: iter(["one\n", "two\n"]),
    )

    assert list(follow_container_logs("abc", tail=10)) == ["one\n", "two\n"]


def test_follow_container_logs_wraps_docker_errors(monkeypatch):
    monkeypatch.setattr(
        "spark_fleet.agent.containers.docker_ops.inspect_containers",
        lambda ids, **kwargs: [inspect_record()],
    )

    def fail(cid, **kwargs):
        raise docker_ops.DockerError("follow failed")
        yield  # pragma: no cover

    monkeypatch.setattr("spark_fleet.agent.containers.docker_ops.follow_container_logs", fail)

    with pytest.raises(ContainerError, match="follow failed"):
        list(follow_container_logs("abc"))
