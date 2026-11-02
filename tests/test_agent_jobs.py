"""Tests for agent one-shot jobs: image pulls, docker run, logs, and cancellation."""

import io
import subprocess
import sys

import pytest

from spark_fleet.agent import docker_ops
from spark_fleet.agent.jobs import (
    JobError,
    JobManager,
    JobResult,
    OneShotJob,
    pull_image,
    start_job,
)
from spark_fleet.shared.render import LABEL_DEPLOYMENT, LABEL_RECIPE, LABEL_ROLE

LABELS = {
    LABEL_DEPLOYMENT: "demo-glm",
    LABEL_RECIPE: "demo-glm",
    LABEL_ROLE: "head",
}


def completed(args, returncode=0, stdout="", stderr=""):
    return subprocess.CompletedProcess(
        args=args, returncode=returncode, stdout=stdout, stderr=stderr
    )


class FakeProcess:
    def __init__(self, exit_code=0, logs="job output\n"):
        self.stdout = io.StringIO(logs)
        self.returncode = None
        self.exit_code = exit_code
        self.terminated = False
        self.killed = False
        self.wait_calls = 0

    def poll(self):
        return self.returncode

    def wait(self, timeout=None):
        self.wait_calls += 1
        if self.returncode is None:
            self.returncode = self.exit_code
        return self.returncode

    def terminate(self):
        self.terminated = True
        self.returncode = -15

    def kill(self):
        self.killed = True
        self.returncode = -9


class TimeoutProcess(FakeProcess):
    def __init__(self, exit_code=0, logs=""):
        super().__init__(exit_code, logs)
        self.first_wait = True

    def wait(self, timeout=None):
        self.wait_calls += 1
        if self.first_wait:
            self.first_wait = False
            raise subprocess.TimeoutExpired("docker", timeout)
        if self.returncode is None:
            self.returncode = self.exit_code
        return self.returncode


def patch_popen(monkeypatch, process):
    calls = []

    def fake_popen(args, **kwargs):
        calls.append((args, kwargs))
        return process

    monkeypatch.setattr("spark_fleet.agent.jobs.subprocess.Popen", fake_popen)
    return calls


def patch_remove(monkeypatch, fail=False):
    calls = []

    def fake_remove(name, **kwargs):
        calls.append(name)
        if fail:
            raise docker_ops.DockerError("no such container")

    monkeypatch.setattr("spark_fleet.agent.jobs.docker_ops.remove_container", fake_remove)
    return calls


# start_job ----------------------------------------------------------------


def test_start_job_builds_docker_run_command(monkeypatch):
    process = FakeProcess()
    calls = patch_popen(monkeypatch, process)

    job = start_job(
        "stage-overlays",
        "alpine:latest",
        ["/bin/sh", "-lc", "echo hi"],
        labels=LABELS,
        volumes=["/host:/stage"],
        environment={"STAGE": "/stage"},
    )

    assert isinstance(job, OneShotJob)
    assert job.name == "stage-overlays"
    args, kwargs = calls[0]
    assert args == [
        "docker",
        "run",
        "--rm",
        "--name",
        "spark-fleet-job-stage-overlays",
        "--label",
        f"{LABEL_DEPLOYMENT}=demo-glm",
        "--label",
        f"{LABEL_RECIPE}=demo-glm",
        "--label",
        f"{LABEL_ROLE}=head",
        "-v",
        "/host:/stage",
        "-e",
        "STAGE=/stage",
        "alpine:latest",
        "/bin/sh",
        "-lc",
        "echo hi",
    ]
    assert kwargs["stdout"] is subprocess.PIPE
    assert kwargs["stderr"] is subprocess.STDOUT
    assert kwargs["text"] is True
    assert kwargs["errors"] == "replace"


def test_start_job_requires_fleet_labels(monkeypatch):
    patch_popen(monkeypatch, FakeProcess())

    with pytest.raises(JobError, match=LABEL_ROLE):
        start_job(
            "stage",
            "alpine:latest",
            ["true"],
            labels={LABEL_DEPLOYMENT: "demo-glm", LABEL_RECIPE: "demo-glm"},
        )


@pytest.mark.parametrize(
    "extra",
    [
        {"BAD KEY": "x"},
        {"owner": "bad\nvalue"},
        {"owner": ""},
    ],
)
def test_start_job_rejects_invalid_labels(monkeypatch, extra):
    patch_popen(monkeypatch, FakeProcess())

    with pytest.raises(JobError, match="label"):
        start_job("stage", "alpine:latest", ["true"], labels={**LABELS, **extra})


@pytest.mark.parametrize("name", ["Bad Name", "", "UPPER", "../escape"])
def test_start_job_rejects_invalid_name(monkeypatch, name):
    patch_popen(monkeypatch, FakeProcess())

    with pytest.raises(JobError, match="invalid job name"):
        start_job(name, "alpine:latest", ["true"], labels=LABELS)


@pytest.mark.parametrize("image", ["", "alpine latest", "alpine\nlatest", "-alpine"])
def test_start_job_rejects_invalid_image(monkeypatch, image):
    patch_popen(monkeypatch, FakeProcess())

    with pytest.raises(JobError, match="invalid image"):
        start_job("stage", image, ["true"], labels=LABELS)


@pytest.mark.parametrize(
    "command",
    [(), ("echo", 1), ("",), ("echo\nhi",), ("echo\x00",)],
)
def test_start_job_rejects_invalid_command(monkeypatch, command):
    patch_popen(monkeypatch, FakeProcess())

    with pytest.raises(JobError, match="command"):
        start_job("stage", "alpine:latest", command, labels=LABELS)


@pytest.mark.parametrize("volume", ["", "host:container with space", "a\nb"])
def test_start_job_rejects_invalid_volume(monkeypatch, volume):
    patch_popen(monkeypatch, FakeProcess())

    with pytest.raises(JobError, match="volume"):
        start_job("stage", "alpine:latest", ["true"], labels=LABELS, volumes=[volume])


@pytest.mark.parametrize("key", ["", "BAD KEY", "1KEY"])
def test_start_job_rejects_invalid_environment_key(monkeypatch, key):
    patch_popen(monkeypatch, FakeProcess())

    with pytest.raises(JobError, match="environment"):
        start_job("stage", "alpine:latest", ["true"], labels=LABELS, environment={key: "x"})


def test_start_job_rejects_environment_newline(monkeypatch):
    patch_popen(monkeypatch, FakeProcess())

    with pytest.raises(JobError, match="environment"):
        start_job("stage", "alpine:latest", ["true"], labels=LABELS, environment={"A": "x\ny"})


def test_start_job_rejects_volumes_as_string(monkeypatch):
    patch_popen(monkeypatch, FakeProcess())

    with pytest.raises(JobError, match="volumes"):
        start_job("stage", "alpine:latest", ["true"], labels=LABELS, volumes="/host:/stage")


def test_start_job_rejects_environment_as_string(monkeypatch):
    patch_popen(monkeypatch, FakeProcess())

    with pytest.raises(JobError, match="environment"):
        start_job("stage", "alpine:latest", ["true"], labels=LABELS, environment="A=1")


# wait / cancel ------------------------------------------------------------


def test_wait_returns_exit_code_and_logs(monkeypatch):
    process = FakeProcess(exit_code=3, logs="hello\nworld\n")
    patch_popen(monkeypatch, process)

    job = start_job("stage", "alpine:latest", ["true"], labels=LABELS)

    result = job.wait(timeout=5)

    assert result == JobResult(
        name="stage",
        exit_code=3,
        logs="hello\nworld\n",
        cancelled=False,
        timed_out=False,
    )


def test_wait_drains_output_larger_than_pipe_buffer(monkeypatch):
    real_popen = subprocess.Popen

    def fake_popen(args, **kwargs):
        del args
        return real_popen(
            [sys.executable, "-c", "import sys; sys.stdout.write('x' * 300000)"],
            **kwargs,
        )

    monkeypatch.setattr("spark_fleet.agent.jobs.subprocess.Popen", fake_popen)

    job = start_job("stage", "alpine:latest", ["true"], labels=LABELS)
    result = job.wait(timeout=10)

    assert result.exit_code == 0
    assert result.cancelled is False
    assert result.timed_out is False
    assert len(result.logs) == 300_000


def test_cancel_terminates_process_and_removes_container(monkeypatch):
    process = FakeProcess()
    patch_popen(monkeypatch, process)
    removed = patch_remove(monkeypatch)

    job = start_job("stage", "alpine:latest", ["true"], labels=LABELS)
    job.cancel()

    assert process.terminated is True
    assert removed == ["spark-fleet-job-stage"]
    assert job.wait().cancelled is True


def test_cancel_ignores_missing_container(monkeypatch):
    process = FakeProcess()
    patch_popen(monkeypatch, process)
    patch_remove(monkeypatch, fail=True)

    job = start_job("stage", "alpine:latest", ["true"], labels=LABELS)

    job.cancel()

    assert process.terminated is True


def test_cancel_finished_job_is_noop(monkeypatch):
    process = FakeProcess()
    process.returncode = 0
    patch_popen(monkeypatch, process)
    removed = patch_remove(monkeypatch)

    job = start_job("stage", "alpine:latest", ["true"], labels=LABELS)
    job.cancel()

    assert process.terminated is False
    assert removed == []


def test_wait_timeout_cancels_job(monkeypatch):
    process = TimeoutProcess(logs="partial\n")
    patch_popen(monkeypatch, process)
    removed = patch_remove(monkeypatch)

    job = start_job("stage", "alpine:latest", ["sleep", "60"], labels=LABELS)
    result = job.wait(timeout=0.01)

    assert result.timed_out is True
    assert result.cancelled is True
    assert result.exit_code is not None
    assert process.terminated is True
    assert removed == ["spark-fleet-job-stage"]


class StubbornProcess(FakeProcess):
    def wait(self, timeout=None):
        self.wait_calls += 1
        raise subprocess.TimeoutExpired("docker", timeout)


def test_cancel_kills_unresponsive_process(monkeypatch):
    process = StubbornProcess()
    patch_popen(monkeypatch, process)
    removed = patch_remove(monkeypatch)

    job = start_job("stage", "alpine:latest", ["true"], labels=LABELS)
    job.cancel()

    assert process.terminated is True
    assert process.killed is True
    assert process.wait_calls == 2
    assert removed == ["spark-fleet-job-stage"]


# manager ------------------------------------------------------------------


def test_manager_rejects_duplicate_running_name(monkeypatch):
    process = FakeProcess()
    patch_popen(monkeypatch, process)
    manager = JobManager()

    manager.start("stage", "alpine:latest", ["true"], labels=LABELS)

    with pytest.raises(JobError, match="already running"):
        manager.start("stage", "alpine:latest", ["true"], labels=LABELS)


def test_manager_cancel_and_wait(monkeypatch):
    process = FakeProcess()
    patch_popen(monkeypatch, process)
    patch_remove(monkeypatch)
    manager = JobManager()
    manager.start("stage", "alpine:latest", ["true"], labels=LABELS)

    assert manager.active() == ["stage"]
    assert manager.cancel("stage") is True
    result = manager.wait("stage")

    assert result.cancelled is True
    assert manager.active() == []
    assert manager.cancel("stage") is False


def test_manager_wait_unknown_raises():
    with pytest.raises(JobError, match="unknown job"):
        JobManager().wait("missing")


# pull_image ---------------------------------------------------------------


def test_pull_image_builds_command(monkeypatch):
    calls = []
    monkeypatch.setattr(
        docker_ops.subprocess,
        "run",
        lambda args, **kwargs: (
            calls.append((args, kwargs))
            or completed(args, stdout="Status: Downloaded newer image\n")
        ),
    )

    output = pull_image("ghcr.io/org/img:tag")

    assert output == "Status: Downloaded newer image\n"
    assert calls[0][0] == ["docker", "pull", "ghcr.io/org/img:tag"]
    assert calls[0][1]["stdout"] is subprocess.PIPE
    assert calls[0][1]["stderr"] is subprocess.STDOUT
    assert calls[0][1]["text"] is True
    assert calls[0][1]["errors"] == "replace"


def test_pull_image_failure_raises_with_output(monkeypatch):
    monkeypatch.setattr(
        docker_ops.subprocess,
        "run",
        lambda args, **kwargs: completed(args, returncode=1, stdout="pull access denied\n"),
    )

    with pytest.raises(docker_ops.DockerError, match="pull access denied"):
        docker_ops.pull_image("ghcr.io/org/img:tag")


def test_remove_container_builds_command(monkeypatch):
    calls = []
    monkeypatch.setattr(
        docker_ops.subprocess,
        "run",
        lambda args, **kwargs: calls.append(args) or completed(args),
    )

    docker_ops.remove_container("spark-fleet-job-stage")

    assert calls[0] == ["docker", "rm", "-f", "spark-fleet-job-stage"]


def test_pull_image_rejects_invalid_reference(monkeypatch):
    monkeypatch.setattr(
        docker_ops.subprocess,
        "run",
        lambda *a, **k: pytest.fail("docker pull should not run"),
    )

    with pytest.raises(JobError, match="invalid image"):
        pull_image("bad image")


def test_pull_image_wraps_docker_errors(monkeypatch):
    def fail(*args, **kwargs):
        raise docker_ops.DockerError("pull failed")

    monkeypatch.setattr("spark_fleet.agent.jobs.docker_ops.pull_image", fail)

    with pytest.raises(JobError, match="pull failed"):
        pull_image("alpine:latest")


def test_manager_status_omits_logs_while_running(monkeypatch):
    process = FakeProcess(exit_code=0, logs="lots of output\n")
    patch_popen(monkeypatch, process)
    manager = JobManager()
    manager.start("stage", "alpine:latest", ["true"], labels=LABELS)

    snapshot = manager.status("stage")

    assert snapshot["state"] == "running"
    assert snapshot["logs"] == ""


def test_manager_status_and_list_after_completion(monkeypatch):
    process = FakeProcess(exit_code=0, logs="done\n")
    patch_popen(monkeypatch, process)
    manager = JobManager()
    manager.start("stage", "alpine:latest", ["true"], labels=LABELS)
    manager.wait("stage")

    # ``wait`` removes the job, so status is unknown afterwards.
    with pytest.raises(JobError, match="unknown job"):
        manager.status("stage")


def test_manager_status_reports_failure_and_logs(monkeypatch):
    process = FakeProcess(exit_code=1, logs="boom\n")
    process.returncode = 1
    patch_popen(monkeypatch, process)
    manager = JobManager()
    manager.start("stage", "alpine:latest", ["false"], labels=LABELS)

    snapshot = manager.status("stage")

    assert snapshot["state"] == "failed"
    assert snapshot["exit_code"] == 1
    assert snapshot["logs"] == "boom\n"


def test_manager_caps_captured_logs(monkeypatch):
    process = FakeProcess(exit_code=0, logs="x" * 1_100_000)
    patch_popen(monkeypatch, process)
    manager = JobManager()
    manager.start("stage", "alpine:latest", ["true"], labels=LABELS)

    result = manager.wait("stage")

    assert len(result.logs) <= 1_000_000
