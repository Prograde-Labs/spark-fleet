"""Agent metrics: host memory, managed-container memory, cache usage, and GPUs.

The agent samples ``/proc/meminfo``, the managed fleet containers, the model
cache roots, and ``nvidia-smi`` when it is usable. Container, cache, and GPU
failures degrade to empty values plus an entry in ``errors`` so a missing Docker
socket or driver does not make the whole snapshot unavailable; host memory is
required because a node snapshot without it is meaningless. GPU telemetry also
sums ``nvidia-smi --query-compute-apps`` memory per GPU UUID so unified-memory
devices (GB10) can report workload residency even when ``memory.used`` is N/A.
"""

from __future__ import annotations

import csv
import os
import re
import shutil
import subprocess
import time
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass, replace
from pathlib import Path
from stat import S_ISDIR
from typing import Any

from spark_fleet.agent import containers
from spark_fleet.agent.containers import ContainerInfo

MEMINFO_PATH = Path("/proc/meminfo")
GPU_TIMEOUT_SECONDS = 5.0
GPU_QUERY = (
    "--query-gpu=index,name,memory.total,memory.used,utilization.gpu,"
    "temperature.gpu,uuid"
)
COMPUTE_QUERY = "--query-compute-apps=gpu_uuid,pid,process_name,used_memory"
_GPU_FORMAT = "--format=csv,noheader,nounits"
_MEMINFO_RE = re.compile(r"^\s*(MemTotal|MemAvailable):\s+(\d+)\s+kB\s*$")
_MEMINFO_FIELDS = ("MemTotal", "MemAvailable")
_MIB = 1024 * 1024


class MetricsError(RuntimeError):
    """Raised when a required metrics source cannot be read."""


@dataclass(frozen=True)
class HostMemory:
    total_bytes: int
    available_bytes: int


@dataclass(frozen=True)
class GpuInfo:
    index: int
    name: str
    memory_total_bytes: int | None
    memory_used_bytes: int | None
    utilization_percent: int | None
    temperature_celsius: int | None
    uuid: str | None = None
    compute_processes: int | None = None
    compute_memory_bytes: int | None = None


@dataclass(frozen=True)
class MetricsSnapshot:
    sampled_at: float
    host_total_bytes: int
    host_available_bytes: int
    containers: tuple[ContainerInfo, ...]
    container_memory_bytes: int
    cache_bytes: int | None
    cache_total_bytes: int | None
    cache_free_bytes: int | None
    gpus: tuple[GpuInfo, ...]
    errors: Mapping[str, str]


def parse_meminfo(text: str) -> HostMemory:
    """Parse the two fields the fleet needs from ``/proc/meminfo``."""

    values: dict[str, int] = {}
    for line in text.splitlines():
        if not line.lstrip().startswith(_MEMINFO_FIELDS):
            continue
        match = _MEMINFO_RE.fullmatch(line)
        if match is None:
            raise MetricsError(f"invalid meminfo line: {line!r}")
        values.setdefault(match.group(1), int(match.group(2)) * 1024)

    missing = [field for field in _MEMINFO_FIELDS if field not in values]
    if missing:
        raise MetricsError(f"missing meminfo fields: {', '.join(missing)}")
    if values["MemTotal"] <= 0:
        raise MetricsError("MemTotal must be positive")
    if values["MemAvailable"] < 0:
        raise MetricsError("MemAvailable must not be negative")
    if values["MemAvailable"] > values["MemTotal"]:
        raise MetricsError("MemAvailable must not exceed MemTotal")
    return HostMemory(values["MemTotal"], values["MemAvailable"])


def read_host_memory(path: Path = MEMINFO_PATH) -> HostMemory:
    try:
        return parse_meminfo(path.read_text())
    except OSError as exc:
        raise MetricsError(f"could not read {path}: {exc}") from exc


def collect_gpus(
    *,
    timeout: float = GPU_TIMEOUT_SECONDS,
    runner: Callable[..., Any] = subprocess.run,
    clock: Callable[[], float] = time.monotonic,
) -> tuple[tuple[GpuInfo, ...], str | None]:
    """Query ``nvidia-smi`` and return GPU data plus an error when unavailable.

    GPU capacity/utilization is required; the compute-app query is best effort so
    that a driver that rejects ``--query-compute-apps`` still yields GPU data.
    Compute fields stay ``None`` when the query fails so consumers can tell an
    idle GPU from an unknown one.
    """

    started = clock()
    try:
        result = runner(
            ["nvidia-smi", GPU_QUERY, _GPU_FORMAT],
            capture_output=True,
            text=True,
            check=False,
            timeout=timeout,
        )
    except FileNotFoundError:
        return (), "nvidia-smi not available"
    except subprocess.TimeoutExpired:
        return (), f"nvidia-smi timed out after {timeout} seconds"
    except OSError as exc:
        return (), f"could not run nvidia-smi: {exc}"

    if result.returncode:
        detail = (result.stderr or result.stdout or "").strip()
        return (), detail or f"nvidia-smi exited {result.returncode}"

    gpus: list[GpuInfo] = []
    errors: list[str] = []
    for line in result.stdout.splitlines():
        if not line.strip():
            continue
        try:
            gpus.append(_parse_gpu_line(line))
        except ValueError as exc:
            errors.append(str(exc))
    if errors:
        errors = [f"could not parse nvidia-smi output: {'; '.join(errors)}"]

    if not gpus:
        return (), "; ".join(errors) or None

    remaining = max(0.0, timeout - (clock() - started))
    if remaining <= 0:
        errors.append("could not query compute processes: no time budget remaining")
        return tuple(gpus), "; ".join(errors)
    compute_counts, compute_error = _collect_compute_apps(
        timeout=remaining, runner=runner
    )
    if compute_error is not None:
        errors.append(compute_error)
    else:
        known = {gpu.uuid for gpu in gpus if gpu.uuid is not None}
        unmatched = sorted(set(compute_counts) - known)
        if unmatched:
            errors.append(
                "could not attribute compute processes: " + ", ".join(unmatched)
            )
        gpus = list(
            _apply_compute_apps(
                gpus, compute_counts, all_attributed=not unmatched
            )
        )
    return tuple(gpus), "; ".join(errors) or None


def _collect_compute_apps(
    *, timeout: float, runner: Callable[..., Any]
) -> tuple[dict[str, tuple[int, int | None]], str | None]:
    """Return per-GPU compute process counts and summed memory, best effort."""

    try:
        result = runner(
            ["nvidia-smi", COMPUTE_QUERY, _GPU_FORMAT],
            capture_output=True,
            text=True,
            check=False,
            timeout=timeout,
        )
    except FileNotFoundError:
        return {}, "could not query compute processes: nvidia-smi not available"
    except subprocess.TimeoutExpired:
        return {}, (
            "could not query compute processes: timed out after "
            f"{timeout:.1f}s of remaining budget"
        )
    except OSError as exc:
        return {}, f"could not query compute processes: {exc}"

    if result.returncode:
        detail = (result.stderr or result.stdout or "").strip()
        return {}, (
            f"could not query compute processes: {detail}"
            if detail
            else f"could not query compute processes: exited {result.returncode}"
        )

    counts: dict[str, tuple[int, int | None]] = {}
    parse_errors: list[str] = []
    for line in result.stdout.splitlines():
        if not line.strip():
            continue
        try:
            uuid, memory_bytes = _parse_compute_line(line)
        except ValueError as exc:
            parse_errors.append(str(exc))
            continue
        if uuid in counts:
            count, total = counts[uuid]
            counts[uuid] = (
                count + 1,
                None if total is None or memory_bytes is None else total + memory_bytes,
            )
        else:
            counts[uuid] = (1, memory_bytes)
    if parse_errors:
        return counts, (
            "could not parse nvidia-smi compute-apps output: "
            + "; ".join(parse_errors)
        )
    return counts, None


def _apply_compute_apps(
    gpus: Sequence[GpuInfo],
    counts: Mapping[str, tuple[int, int | None]],
    *,
    all_attributed: bool,
) -> tuple[GpuInfo, ...]:
    updated: list[GpuInfo] = []
    for gpu in gpus:
        if gpu.uuid is None or gpu.uuid not in counts:
            if all_attributed:
                # Every compute row matched a known GPU (or none exist), so this
                # device cannot be running a compute process.
                updated.append(
                    replace(gpu, compute_processes=0, compute_memory_bytes=0)
                )
            else:
                # Unattributable rows exist, so this device's residency is
                # unknown rather than provably idle.
                updated.append(gpu)
            continue
        processes, memory_bytes = counts[gpu.uuid]
        updated.append(
            replace(
                gpu,
                compute_processes=processes,
                compute_memory_bytes=memory_bytes,
            )
        )
    return tuple(updated)


def _error_text(exc: BaseException) -> str:
    return str(exc) or type(exc).__name__


class _ErrorLog:
    """Collect a bounded number of error messages plus an overflow count."""

    def __init__(self, limit: int = 5):
        self._limit = limit
        self._messages: list[str] = []
        self._extra = 0

    def add(self, error: BaseException | str) -> None:
        if len(self._messages) < self._limit:
            self._messages.append(
                _error_text(error) if isinstance(error, BaseException) else error
            )
        else:
            self._extra += 1

    def summary(self) -> str | None:
        if not self._messages:
            return None
        text = "; ".join(self._messages)
        if self._extra:
            text += f"; ... and {self._extra} more"
        return text


def cache_disk_usage(roots: Sequence[Path]) -> tuple[int, str | None]:
    """Sum inode-deduplicated cache bytes and report unreadable subtrees."""

    seen: set[tuple[int, int]] = set()
    walked: set[tuple[int, int]] = set()
    total = 0
    errors = _ErrorLog()
    for root in sorted(roots, key=lambda path: len(path.parts)):
        try:
            info = root.stat()
        except OSError:
            continue
        if not S_ISDIR(info.st_mode):
            continue
        if (info.st_dev, info.st_ino) in walked:
            continue
        for dirpath, _dirnames, filenames in os.walk(root, onerror=errors.add):
            try:
                dir_info = os.stat(dirpath)
            except OSError:
                continue
            walked.add((dir_info.st_dev, dir_info.st_ino))
            for filename in filenames:
                path = Path(dirpath) / filename
                try:
                    if path.is_symlink():
                        continue
                    info = path.stat()
                except OSError as exc:
                    errors.add(exc)
                    continue
                inode = (info.st_dev, info.st_ino)
                if inode in seen:
                    continue
                seen.add(inode)
                total += info.st_size
    return total, errors.summary()


def cache_filesystem_usage(
    roots: Sequence[Path],
    *,
    stat: Callable[[Path], os.stat_result] = os.stat,
    disk_usage: Callable[[Path], Any] = shutil.disk_usage,
) -> tuple[int | None, int | None, str | None]:
    """Return deduplicated filesystem capacity and free bytes for cache roots."""

    seen: set[int] = set()
    resolved = 0
    total = 0
    free = 0
    errors = _ErrorLog()
    for root in roots:
        try:
            info = stat(root)
        except FileNotFoundError:
            errors.add(f"{root}: not found")
            continue
        except OSError as exc:
            errors.add(f"{root}: {_error_text(exc)}")
            continue
        if not S_ISDIR(info.st_mode):
            errors.add(f"{root}: not a directory")
            continue
        if info.st_dev in seen:
            continue
        try:
            usage = disk_usage(root)
        except OSError as exc:
            errors.add(f"{root}: {_error_text(exc)}")
            continue
        seen.add(info.st_dev)
        resolved += 1
        total += usage.total
        free += usage.free
    summary = errors.summary()
    if not roots:
        prefix = "no cache roots configured"
    elif resolved == 0:
        prefix = "no usable cache root: " + ", ".join(str(root) for root in roots)
    else:
        prefix = None
    if prefix is not None:
        summary = prefix + (f"; {summary}" if summary else "")
    if resolved == 0:
        return None, None, summary
    return total, free, summary


def collect_metrics(
    cache_roots: Sequence[Path],
    *,
    meminfo_path: Path = MEMINFO_PATH,
    container_sampler: Callable[[], Sequence[ContainerInfo]] | None = None,
    cache_sampler: Callable[[], int] | None = None,
    cache_usage_sampler: (
        Callable[[], tuple[int | None, int | None, str | None]] | None
    ) = None,
    gpu_sampler: Callable[[], tuple[tuple[GpuInfo, ...], str | None]] | None = None,
    clock: Callable[[], float] = time.time,
) -> MetricsSnapshot:
    """Collect one metrics snapshot, degrading optional sources on failure."""

    host = read_host_memory(meminfo_path)
    errors: dict[str, str] = {}
    cache_errors: list[str] = []

    try:
        container_list = tuple(
            container_sampler()
            if container_sampler is not None
            else containers.managed_containers()
        )
    except Exception as exc:
        container_list = ()
        errors["containers"] = str(exc)
    container_memory_bytes = sum(info.memory_usage or 0 for info in container_list)

    if cache_sampler is not None:
        try:
            cache_bytes = cache_sampler()
        except Exception as exc:
            cache_bytes = None
            cache_errors.append(_error_text(exc))
    else:
        cache_bytes, cache_error = cache_disk_usage(cache_roots)
        if cache_error:
            cache_errors.append(cache_error)

    if cache_usage_sampler is not None:
        try:
            cache_total_bytes, cache_free_bytes, usage_error = cache_usage_sampler()
        except Exception as exc:
            cache_total_bytes, cache_free_bytes = None, None
            usage_error = _error_text(exc)
    else:
        cache_total_bytes, cache_free_bytes, usage_error = cache_filesystem_usage(
            cache_roots
        )
    if usage_error:
        cache_errors.append(usage_error)
    if cache_bytes is None and not cache_errors:
        cache_errors.append("cache bytes unavailable")
    if cache_total_bytes is None or cache_free_bytes is None:
        if not usage_error:
            cache_errors.append("cache capacity unknown")
    if cache_errors:
        errors["cache"] = "; ".join(cache_errors)

    if gpu_sampler is not None:
        gpus, gpu_error = gpu_sampler()
    else:
        gpus, gpu_error = collect_gpus()
    if gpu_error is not None:
        errors["gpu"] = gpu_error

    return MetricsSnapshot(
        sampled_at=clock(),
        host_total_bytes=host.total_bytes,
        host_available_bytes=host.available_bytes,
        containers=container_list,
        container_memory_bytes=container_memory_bytes,
        cache_bytes=cache_bytes,
        cache_total_bytes=cache_total_bytes,
        cache_free_bytes=cache_free_bytes,
        gpus=gpus,
        errors=errors,
    )


def snapshot_to_dict(snapshot: MetricsSnapshot) -> dict[str, Any]:
    """Serialize a snapshot for the agent's JSON endpoint."""

    return {
        "sampled_at": snapshot.sampled_at,
        "host_total_bytes": snapshot.host_total_bytes,
        "host_available_bytes": snapshot.host_available_bytes,
        "container_memory_bytes": snapshot.container_memory_bytes,
        "cache_bytes": snapshot.cache_bytes,
        "cache_total_bytes": snapshot.cache_total_bytes,
        "cache_free_bytes": snapshot.cache_free_bytes,
        "containers": [
            {
                "id": info.id,
                "name": info.name,
                "state": info.state,
                "status": info.status,
                "health": info.health,
                "memory_usage_bytes": info.memory_usage,
                "memory_limit_bytes": info.memory_limit,
                "memory_percent": info.memory_percent,
                "labels": dict(info.labels),
            }
            for info in snapshot.containers
        ],
        "gpus": [
            {
                "index": gpu.index,
                "name": gpu.name,
                "memory_total_bytes": gpu.memory_total_bytes,
                "memory_used_bytes": gpu.memory_used_bytes,
                "utilization_percent": gpu.utilization_percent,
                "temperature_celsius": gpu.temperature_celsius,
                "uuid": gpu.uuid,
                "compute_processes": gpu.compute_processes,
                "compute_memory_bytes": gpu.compute_memory_bytes,
                "resident": (
                    None
                    if gpu.compute_processes is None
                    else gpu.compute_processes > 0
                ),
            }
            for gpu in snapshot.gpus
        ],
        "errors": dict(snapshot.errors),
    }


def _parse_gpu_line(line: str) -> GpuInfo:
    try:
        rows = list(csv.reader([line], skipinitialspace=True))
    except csv.Error as exc:
        raise ValueError(f"invalid CSV row: {line!r}") from exc
    if len(rows) != 1 or len(rows[0]) != 7:
        count = len(rows[0]) if len(rows) == 1 else 0
        raise ValueError(f"expected 7 fields, got {count}: {line!r}")
    fields = [field.strip() for field in rows[0]]
    index = _required_int(fields[0], "index")
    name = fields[1]
    if not name:
        raise ValueError(f"missing GPU name: {line!r}")
    return GpuInfo(
        index=index,
        name=name,
        memory_total_bytes=_mib(fields[2]),
        memory_used_bytes=_mib(fields[3]),
        utilization_percent=_optional_int(fields[4]),
        temperature_celsius=_optional_int(fields[5]),
        uuid=_optional_text(fields[6]),
    )


def _parse_compute_line(line: str) -> tuple[str, int | None]:
    try:
        rows = list(csv.reader([line], skipinitialspace=True))
    except csv.Error as exc:
        raise ValueError(f"invalid CSV row: {line!r}") from exc
    if len(rows) != 1 or len(rows[0]) != 4:
        count = len(rows[0]) if len(rows) == 1 else 0
        raise ValueError(f"expected 4 fields, got {count}: {line!r}")
    fields = [field.strip() for field in rows[0]]
    uuid = _optional_text(fields[0])
    if uuid is None:
        raise ValueError(f"missing GPU UUID: {line!r}")
    return uuid, _mib(fields[3])


def _required_int(value: str, field: str) -> int:
    try:
        return int(value)
    except ValueError as exc:
        raise ValueError(f"invalid {field}: {value!r}") from exc


def _optional_int(value: str) -> int | None:
    if not value or (value.startswith("[") and value.endswith("]")):
        return None
    try:
        return int(value)
    except ValueError as exc:
        raise ValueError(f"invalid numeric value: {value!r}") from exc


def _optional_text(value: str) -> str | None:
    if not value or (value.startswith("[") and value.endswith("]")):
        return None
    return value


def _mib(value: str) -> int | None:
    number = _optional_int(value)
    return None if number is None else number * _MIB
