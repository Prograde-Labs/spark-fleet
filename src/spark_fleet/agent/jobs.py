"""One-shot prepare jobs: image pulls, ``docker run``, captured logs, cancellation.

Prepare jobs are typed operations only. Names, image references, labels, volumes,
and environment entries are validated before the fixed ``docker run`` argument
list is built; commands are passed as an argv list, never through a shell. The
job runs attached so its combined output can be captured, carries spark-fleet
labels for ownership, and is force-removed by name when cancelled or timed out.
"""

from __future__ import annotations

import re
import subprocess
import threading
from collections.abc import Mapping, Sequence
from dataclasses import dataclass

from spark_fleet.agent import docker_ops
from spark_fleet.shared.render import LABEL_DEPLOYMENT, LABEL_RECIPE, LABEL_ROLE

JOB_NAME_PATTERN = re.compile(r"[a-z0-9][a-z0-9_-]{0,62}")
IMAGE_PATTERN = re.compile(r"[A-Za-z0-9][A-Za-z0-9._/:@-]{0,254}")
LABEL_KEY_PATTERN = re.compile(r"[A-Za-z0-9][A-Za-z0-9._-]{0,127}")
ENV_KEY_PATTERN = re.compile(r"[A-Za-z_][A-Za-z0-9_]{0,127}")
_REQUIRED_LABELS = (LABEL_DEPLOYMENT, LABEL_RECIPE, LABEL_ROLE)
_CONTAINER_PREFIX = "spark-fleet-job-"
_CANCEL_GRACE_SECONDS = 5.0


class JobError(RuntimeError):
    """Raised when a one-shot job is invalid or its process fails to start."""


@dataclass(frozen=True)
class JobResult:
    name: str
    exit_code: int | None
    logs: str
    cancelled: bool = False
    timed_out: bool = False


class OneShotJob:
    """A running ``docker run`` process with captured output and cancellation."""

    def __init__(self, name: str, container_name: str, process: subprocess.Popen[str]):
        self._name = name
        self._container_name = container_name
        self._process = process
        self._cancelled = False
        self._chunks: list[str] = []
        self._reader = threading.Thread(target=self._drain, daemon=True)
        self._reader.start()

    def _drain(self) -> None:
        stdout = self._process.stdout
        if stdout is None:
            return
        try:
            for chunk in stdout:
                self._chunks.append(chunk)
        except (OSError, ValueError):
            # The pipe closed while cancelling; keep whatever was captured.
            pass

    @property
    def name(self) -> str:
        return self._name

    @property
    def running(self) -> bool:
        return self._process.poll() is None

    def cancel(self) -> None:
        """Terminate the CLI process and force-remove its container."""

        if self._process.poll() is not None:
            return
        self._cancelled = True
        self._process.terminate()
        try:
            self._process.wait(timeout=_CANCEL_GRACE_SECONDS)
        except subprocess.TimeoutExpired:
            self._process.kill()
            try:
                self._process.wait(timeout=_CANCEL_GRACE_SECONDS)
            except subprocess.TimeoutExpired:
                pass
        try:
            docker_ops.remove_container(self._container_name)
        except docker_ops.DockerError:
            # The container may already be gone (--rm) or never have started.
            pass

    def wait(self, timeout: float | None = None) -> JobResult:
        """Wait for completion, cancelling the job if ``timeout`` expires."""

        timed_out = False
        try:
            exit_code: int | None = self._process.wait(timeout=timeout)
        except subprocess.TimeoutExpired:
            timed_out = True
            self.cancel()
            try:
                exit_code = self._process.wait(timeout=_CANCEL_GRACE_SECONDS)
            except subprocess.TimeoutExpired:
                exit_code = None

        self._reader.join(timeout=_CANCEL_GRACE_SECONDS)
        return JobResult(
            name=self._name,
            exit_code=exit_code,
            logs="".join(self._chunks),
            cancelled=self._cancelled,
            timed_out=timed_out,
        )


def start_job(
    name: str,
    image: str,
    command: Sequence[str],
    *,
    labels: Mapping[str, str],
    volumes: Sequence[str] = (),
    environment: Mapping[str, str] | None = None,
) -> OneShotJob:
    """Start a labeled one-shot container and return its handle."""

    _validate_name(name)
    _validate_image(image)
    _validate_command(command)
    _validate_labels(labels)
    if isinstance(volumes, str) or not isinstance(volumes, Sequence):
        raise JobError("volumes must be a sequence of mount strings")
    volume_list = list(volumes)
    for volume in volume_list:
        _validate_volume(volume)
    if environment is not None and not isinstance(environment, Mapping):
        raise JobError("environment must be a mapping")
    env = dict(environment or {})
    for key, value in env.items():
        _validate_environment(key, value)

    container_name = f"{_CONTAINER_PREFIX}{name}"
    args = ["docker", "run", "--rm", "--name", container_name]
    for key in sorted(labels):
        args.extend(["--label", f"{key}={labels[key]}"])
    for volume in volume_list:
        args.extend(["-v", volume])
    for key in sorted(env):
        args.extend(["-e", f"{key}={env[key]}"])
    args.append(image)
    args.extend(command)

    try:
        process = subprocess.Popen(
            args,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            errors="replace",
        )
    except OSError as exc:
        raise JobError(f"could not start docker run for {name}: {exc}") from exc
    return OneShotJob(name, container_name, process)


def pull_image(image: str, *, timeout: float = 1800.0) -> str:
    """Pull an image after validating the reference."""

    _validate_image(image)
    try:
        return docker_ops.pull_image(image, timeout=timeout)
    except docker_ops.DockerError as exc:
        raise JobError(str(exc)) from exc


class JobManager:
    """Track one-shot jobs by name so they can be cancelled and awaited."""

    def __init__(self) -> None:
        self._jobs: dict[str, OneShotJob] = {}

    def start(
        self,
        name: str,
        image: str,
        command: Sequence[str],
        *,
        labels: Mapping[str, str],
        volumes: Sequence[str] = (),
        environment: Mapping[str, str] | None = None,
    ) -> OneShotJob:
        existing = self._jobs.get(name)
        if existing is not None and existing.running:
            raise JobError(f"job {name!r} is already running")
        job = start_job(
            name,
            image,
            command,
            labels=labels,
            volumes=volumes,
            environment=environment,
        )
        self._jobs[name] = job
        return job

    def active(self) -> list[str]:
        return sorted(name for name, job in self._jobs.items() if job.running)

    def cancel(self, name: str) -> bool:
        job = self._jobs.get(name)
        if job is None or not job.running:
            self._jobs.pop(name, None)
            return False
        job.cancel()
        return True

    def wait(self, name: str, timeout: float | None = None) -> JobResult:
        job = self._jobs.get(name)
        if job is None:
            raise JobError(f"unknown job: {name}")
        result = job.wait(timeout=timeout)
        self._jobs.pop(name, None)
        return result


def _validate_name(name: str) -> None:
    if not isinstance(name, str) or not JOB_NAME_PATTERN.fullmatch(name):
        raise JobError(f"invalid job name: {name!r}")


def _validate_image(image: str) -> None:
    if not isinstance(image, str) or not IMAGE_PATTERN.fullmatch(image):
        raise JobError(f"invalid image reference: {image!r}")


def _validate_command(command: Sequence[str]) -> None:
    if isinstance(command, str) or not isinstance(command, Sequence) or not command:
        raise JobError("command must be a non-empty sequence of arguments")
    for argument in command:
        if not isinstance(argument, str) or not argument:
            raise JobError(f"invalid command argument: {argument!r}")
        if any(character in argument for character in ("\x00", "\n", "\r")):
            raise JobError(f"invalid command argument: {argument!r}")


def _validate_labels(labels: Mapping[str, str]) -> None:
    if not isinstance(labels, Mapping):
        raise JobError("labels must be a mapping")
    for key, value in labels.items():
        if not isinstance(key, str) or not LABEL_KEY_PATTERN.fullmatch(key):
            raise JobError(f"invalid label key: {key!r}")
        if (
            not isinstance(value, str)
            or not value
            or any(character in value for character in ("\x00", "\n", "\r"))
        ):
            raise JobError(f"invalid label value for {key}: {value!r}")
    missing = [label for label in _REQUIRED_LABELS if label not in labels]
    if missing:
        raise JobError(f"missing fleet labels: {', '.join(missing)}")


def _validate_volume(volume: str) -> None:
    if (
        not isinstance(volume, str)
        or not volume
        or any(character.isspace() for character in volume)
        or "\x00" in volume
    ):
        raise JobError(f"invalid volume mount: {volume!r}")


def _validate_environment(key: str, value: str) -> None:
    if not isinstance(key, str) or not ENV_KEY_PATTERN.fullmatch(key):
        raise JobError(f"invalid environment key: {key!r}")
    if not isinstance(value, str) or any(character in value for character in ("\x00", "\n", "\r")):
        raise JobError(f"invalid environment value for {key}: {value!r}")
