"""Tests for agent Compose project validation, lifecycle, and Docker CLI wiring."""

import hashlib
import subprocess
from pathlib import Path

import pytest

from spark_fleet.agent import docker_ops
from spark_fleet.agent.compose import (
    COMPOSE_FILE_NAME,
    ComposeError,
    ContainerStatus,
    ProjectSpec,
    down_project,
    status_project,
    up_project,
    validate_project,
)
from spark_fleet.shared.render import (
    LABEL_CONFIG_HASH,
    LABEL_DEPLOYMENT,
    LABEL_RECIPE,
    LABEL_ROLE,
)

PROJECT = "spark-fleet-demo-glm-head"


def completed(args, returncode=0, stdout="", stderr=""):
    return subprocess.CompletedProcess(
        args=args, returncode=returncode, stdout=stdout, stderr=stderr
    )


def compose_doc(labels=None):
    return {
        "services": {
            "workload": {
                "image": "ghcr.io/org/img:tag",
                "labels": labels
                if labels is not None
                else {
                    LABEL_DEPLOYMENT: "demo-glm",
                    LABEL_RECIPE: "demo-glm",
                    LABEL_ROLE: "head",
                    LABEL_CONFIG_HASH: "abc",
                },
            }
        }
    }


def container_row(**overrides):
    row = {
        "ID": "container-1",
        "Names": "spark-fleet-demo-glm-head",
        "State": "running",
        "Status": "Up 2 minutes",
        "Labels": ",".join(
            [
                f"{LABEL_DEPLOYMENT}=demo-glm",
                f"{LABEL_RECIPE}=demo-glm",
                f"{LABEL_ROLE}=head",
                f"{LABEL_CONFIG_HASH}=abc",
                "com.docker.compose.project=spark-fleet-demo-glm-head",
                "com.docker.compose.service=workload",
            ]
        ),
    }
    row.update(overrides)
    return row


# docker_ops ---------------------------------------------------------------


def test_compose_config_builds_expected_command(monkeypatch):
    calls = []
    monkeypatch.setattr(
        docker_ops.subprocess,
        "run",
        lambda args, **kwargs: (
            calls.append((args, kwargs)) or completed(args, stdout="services:\n  workload: {}\n")
        ),
    )
    file = Path("/tmp/project/compose.yaml")

    output = docker_ops.compose_config(file, PROJECT)

    assert output == "services:\n  workload: {}\n"
    assert calls[0][0] == [
        "docker",
        "compose",
        "-p",
        PROJECT,
        "-f",
        str(file),
        "config",
    ]
    assert calls[0][1]["capture_output"] is True
    assert calls[0][1]["text"] is True


def test_compose_up_and_down_build_expected_commands(monkeypatch):
    calls = []
    monkeypatch.setattr(
        docker_ops.subprocess,
        "run",
        lambda args, **kwargs: calls.append(args) or completed(args),
    )
    file = Path("/tmp/project/compose.yaml")

    docker_ops.compose_up(file, PROJECT)
    docker_ops.compose_down(file, PROJECT)

    assert calls[0][-2:] == ["up", "-d"]
    assert calls[1][-1] == "down"


def test_compose_failure_raises_with_stderr(monkeypatch):
    monkeypatch.setattr(
        docker_ops.subprocess,
        "run",
        lambda args, **kwargs: completed(args, returncode=1, stderr="no such service"),
    )

    with pytest.raises(docker_ops.DockerError, match="no such service"):
        docker_ops.compose_config(Path("/tmp/compose.yaml"), PROJECT)


def test_compose_timeout_raises(monkeypatch):
    def raise_timeout(args, **kwargs):
        raise subprocess.TimeoutExpired(args, 1)

    monkeypatch.setattr(docker_ops.subprocess, "run", raise_timeout)

    with pytest.raises(docker_ops.DockerError, match="timed out"):
        docker_ops.compose_up(Path("/tmp/compose.yaml"), PROJECT)


def test_compose_oserror_raises(monkeypatch):
    def raise_oserror(args, **kwargs):
        raise FileNotFoundError("docker")

    monkeypatch.setattr(docker_ops.subprocess, "run", raise_oserror)

    with pytest.raises(docker_ops.DockerError, match="could not run"):
        docker_ops.compose_down(Path("/tmp/compose.yaml"), PROJECT)


def test_list_containers_parses_ndjson(monkeypatch):
    output = '{"ID": "one", "State": "running"}\n\n{"ID": "two", "State": "exited"}\n'
    monkeypatch.setattr(
        docker_ops.subprocess, "run", lambda args, **kwargs: completed(args, stdout=output)
    )

    rows = docker_ops.list_containers(PROJECT)

    assert [row["ID"] for row in rows] == ["one", "two"]


def test_list_containers_rejects_invalid_json(monkeypatch):
    monkeypatch.setattr(
        docker_ops.subprocess, "run", lambda args, **kwargs: completed(args, stdout="not json")
    )

    with pytest.raises(docker_ops.DockerError, match="invalid JSON"):
        docker_ops.list_containers(PROJECT)


def test_parse_labels():
    assert docker_ops.parse_labels(None) == {}
    assert docker_ops.parse_labels("") == {}
    assert docker_ops.parse_labels("a=1,b=2,broken") == {"a": "1", "b": "2"}


# validate_project ---------------------------------------------------------


def fake_config(monkeypatch, output="services:\n  workload: {}\n"):
    monkeypatch.setattr(
        "spark_fleet.agent.compose.docker_ops.compose_config",
        lambda file, project, **kwargs: output,
    )


def test_validate_project_writes_file_and_hashes_normalized_config(tmp_path, monkeypatch):
    fake_config(monkeypatch, output="normalized: true\n")
    directory = tmp_path / "projects" / "role"

    spec = validate_project(directory, compose_doc(), PROJECT)

    assert isinstance(spec, ProjectSpec)
    assert spec.project == PROJECT
    assert spec.directory == directory
    assert spec.deployment == "demo-glm"
    assert spec.role == "head"
    assert spec.config_hash == hashlib.sha256(b"normalized: true\n").hexdigest()
    written = (directory / COMPOSE_FILE_NAME).read_text()
    assert "spark.fleet.deployment: demo-glm" in written
    assert not (directory / f"{COMPOSE_FILE_NAME}.tmp").exists()


def test_validate_project_rejects_invalid_project_name(tmp_path, monkeypatch):
    fake_config(monkeypatch)

    with pytest.raises(ComposeError, match="project name"):
        validate_project(tmp_path, compose_doc(), "Spark Fleet")


def test_validate_project_rejects_project_not_matching_labels(tmp_path, monkeypatch):
    fake_config(monkeypatch)

    with pytest.raises(ComposeError, match="does not match"):
        validate_project(tmp_path, compose_doc(), "spark-fleet-demo-glm-worker")


def test_validate_project_rejects_unknown_top_level_keys(tmp_path, monkeypatch):
    fake_config(monkeypatch)
    doc = compose_doc()
    doc["secrets"] = {"token": {"file": "/run/secret"}}

    with pytest.raises(ComposeError, match="unsupported top-level"):
        validate_project(tmp_path, doc, PROJECT)


def test_validate_project_requires_services(tmp_path, monkeypatch):
    fake_config(monkeypatch)

    with pytest.raises(ComposeError, match="services"):
        validate_project(tmp_path, {"volumes": {}}, PROJECT)


def test_validate_project_requires_exactly_one_workload(tmp_path, monkeypatch):
    fake_config(monkeypatch)
    doc = compose_doc()
    doc["services"]["extra"] = {"image": "alpine"}

    with pytest.raises(ComposeError, match="workload"):
        validate_project(tmp_path, doc, PROJECT)


def test_validate_project_requires_fleet_labels(tmp_path, monkeypatch):
    fake_config(monkeypatch)
    labels = {
        LABEL_DEPLOYMENT: "demo-glm",
        LABEL_RECIPE: "demo-glm",
        LABEL_ROLE: "head",
    }

    with pytest.raises(ComposeError, match=LABEL_CONFIG_HASH):
        validate_project(tmp_path, compose_doc(labels), PROJECT)


def test_validate_project_requires_label_mapping(tmp_path, monkeypatch):
    fake_config(monkeypatch)
    doc = compose_doc()
    doc["services"]["workload"]["labels"] = ["owner=chris"]

    with pytest.raises(ComposeError, match="labels"):
        validate_project(tmp_path, doc, PROJECT)


def test_validate_project_wraps_docker_errors(tmp_path, monkeypatch):
    def fail(file, project, **kwargs):
        raise docker_ops.DockerError("docker compose config failed")

    monkeypatch.setattr("spark_fleet.agent.compose.docker_ops.compose_config", fail)

    with pytest.raises(ComposeError, match="config failed"):
        validate_project(tmp_path, compose_doc(), PROJECT)


# lifecycle ----------------------------------------------------------------


def make_spec(tmp_path):
    return ProjectSpec(
        project=PROJECT,
        directory=tmp_path,
        deployment="demo-glm",
        role="head",
        config_hash="abc",
    )


def test_up_project_verifies_labels(tmp_path, monkeypatch):
    calls = []
    monkeypatch.setattr(
        "spark_fleet.agent.compose.docker_ops.compose_up",
        lambda file, project, **kwargs: calls.append((file, project)),
    )
    monkeypatch.setattr(
        "spark_fleet.agent.compose.docker_ops.list_containers",
        lambda project, **kwargs: [container_row()],
    )
    spec = make_spec(tmp_path)

    statuses = up_project(spec)

    assert calls == [(tmp_path / COMPOSE_FILE_NAME, PROJECT)]
    assert len(statuses) == 1
    assert statuses[0] == ContainerStatus(
        id="container-1",
        name="spark-fleet-demo-glm-head",
        service="workload",
        state="running",
        status="Up 2 minutes",
        labels={
            LABEL_DEPLOYMENT: "demo-glm",
            LABEL_RECIPE: "demo-glm",
            LABEL_ROLE: "head",
            LABEL_CONFIG_HASH: "abc",
            "com.docker.compose.project": PROJECT,
            "com.docker.compose.service": "workload",
        },
    )


def test_up_project_requires_containers(tmp_path, monkeypatch):
    monkeypatch.setattr("spark_fleet.agent.compose.docker_ops.compose_up", lambda *a, **k: None)
    monkeypatch.setattr("spark_fleet.agent.compose.docker_ops.list_containers", lambda *a, **k: [])

    with pytest.raises(ComposeError, match="no containers"):
        up_project(make_spec(tmp_path))


def test_up_project_rejects_mismatched_deployment(tmp_path, monkeypatch):
    monkeypatch.setattr("spark_fleet.agent.compose.docker_ops.compose_up", lambda *a, **k: None)
    monkeypatch.setattr(
        "spark_fleet.agent.compose.docker_ops.list_containers",
        lambda *a, **k: [
            container_row(
                Labels=f"{LABEL_DEPLOYMENT}=other,{LABEL_ROLE}=head,{LABEL_CONFIG_HASH}=abc"
            )
        ],
    )

    with pytest.raises(ComposeError, match=LABEL_DEPLOYMENT):
        up_project(make_spec(tmp_path))


def test_up_project_rejects_missing_role_label(tmp_path, monkeypatch):
    monkeypatch.setattr("spark_fleet.agent.compose.docker_ops.compose_up", lambda *a, **k: None)
    monkeypatch.setattr(
        "spark_fleet.agent.compose.docker_ops.list_containers",
        lambda *a, **k: [
            container_row(Labels=f"{LABEL_DEPLOYMENT}=demo-glm,{LABEL_CONFIG_HASH}=abc")
        ],
    )

    with pytest.raises(ComposeError, match=LABEL_ROLE):
        up_project(make_spec(tmp_path))


def test_down_project_calls_compose_down(tmp_path, monkeypatch):
    calls = []
    monkeypatch.setattr(
        "spark_fleet.agent.compose.docker_ops.compose_down",
        lambda file, project, **kwargs: calls.append((file, project)),
    )

    down_project(make_spec(tmp_path))

    assert calls == [(tmp_path / COMPOSE_FILE_NAME, PROJECT)]


def test_down_project_wraps_docker_errors(tmp_path, monkeypatch):
    def fail(*args, **kwargs):
        raise docker_ops.DockerError("down failed")

    monkeypatch.setattr("spark_fleet.agent.compose.docker_ops.compose_down", fail)

    with pytest.raises(ComposeError, match="down failed"):
        down_project(make_spec(tmp_path))


def test_status_project_parses_rows(tmp_path, monkeypatch):
    monkeypatch.setattr(
        "spark_fleet.agent.compose.docker_ops.list_containers",
        lambda *a, **k: [container_row()],
    )

    statuses = status_project(make_spec(tmp_path))

    assert statuses[0].id == "container-1"
    assert statuses[0].state == "running"
    assert statuses[0].labels[LABEL_ROLE] == "head"


def test_status_project_wraps_docker_errors(tmp_path, monkeypatch):
    def fail(*args, **kwargs):
        raise docker_ops.DockerError("ps failed")

    monkeypatch.setattr("spark_fleet.agent.compose.docker_ops.list_containers", fail)

    with pytest.raises(ComposeError, match="ps failed"):
        status_project(make_spec(tmp_path))
