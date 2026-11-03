"""Tests for agent metrics sampling, GPU parsing, and the snapshot endpoint."""

import os
import stat
import subprocess
from pathlib import Path
from types import SimpleNamespace

import pytest
from fastapi.testclient import TestClient

from spark_fleet.agent import metrics
from spark_fleet.agent.api import build_app
from spark_fleet.agent.containers import ContainerInfo
from spark_fleet.agent.metrics import (
    GpuInfo,
    HostMemory,
    MetricsError,
    MetricsSnapshot,
    cache_disk_usage,
    cache_filesystem_usage,
    collect_gpus,
    collect_metrics,
    parse_meminfo,
    read_host_memory,
    snapshot_to_dict,
)
from spark_fleet.shared.config import AgentConfig

TOKEN = "agent-token-0123456789"

MEMINFO = """MemTotal:       131072000 kB
MemAvailable:    65536000 kB
"""

GPU_CSV = """0, NVIDIA GB10, 65536, 1024, 5, 40, GPU-aaa
1, NVIDIA GB10, 65536, [N/A], [N/A], [N/A], GPU-bbb
"""

COMPUTE_CSV = """GPU-aaa, 111, python, 512
GPU-aaa, 222, python, 256
GPU-bbb, 333, tf-demo-lanes, 106268
"""


def completed(args, returncode=0, stdout="", stderr=""):
    return subprocess.CompletedProcess(
        args=args, returncode=returncode, stdout=stdout, stderr=stderr
    )


def dispatch_runner(gpu_stdout=GPU_CSV, compute_stdout=COMPUTE_CSV):
    """Fake nvidia-smi that answers the GPU and compute-app queries separately."""

    def runner(args, **kwargs):
        if any("--query-compute-apps" in arg for arg in args):
            return completed(args, stdout=compute_stdout)
        return completed(args, stdout=gpu_stdout)

    return runner


def container(
    name="spark-fleet-demo-glm-head",
    memory_usage=100,
    memory_limit=200,
    memory_percent=50.0,
):
    return ContainerInfo(
        id="abc123",
        name=name,
        state="running",
        status="Up 2 minutes",
        health="healthy",
        memory_usage=memory_usage,
        memory_limit=memory_limit,
        memory_percent=memory_percent,
        labels={"spark.fleet.deployment": "demo-glm", "spark.fleet.role": "head"},
    )


def write_meminfo(tmp_path, text=MEMINFO):
    path = tmp_path / "meminfo"
    path.write_text(text)
    return path


def config(tmp_path, roots=(Path("/srv/spark-fleet/hub"),)):
    return AgentConfig(
        host="127.0.0.1",
        port=8092,
        node_id="spark-01",
        data_dir=tmp_path / "agent",
        compose_projects_dir=tmp_path / "agent" / "projects",
        model_cache_roots=roots,
        agent_token=TOKEN,
    )


def auth(token=TOKEN):
    return {"Authorization": f"Bearer {token}"}


# host memory --------------------------------------------------------------


def test_parse_meminfo():
    host = parse_meminfo(MEMINFO)

    assert host == HostMemory(
        total_bytes=131_072_000 * 1024,
        available_bytes=65_536_000 * 1024,
    )


def test_parse_meminfo_ignores_other_fields():
    host = parse_meminfo("MemFree: 123 kB\nMemTotal: 100 kB\nBuffers: 1 kB\nMemAvailable: 50 kB\n")

    assert host.total_bytes == 100 * 1024
    assert host.available_bytes == 50 * 1024


@pytest.mark.parametrize(
    "text",
    [
        "",
        "MemTotal: 100 kB\n",
        "MemAvailable: 50 kB\n",
        "MemTotal: 0 kB\nMemAvailable: 0 kB\n",
        "MemTotal: -1 kB\nMemAvailable: 0 kB\n",
        "MemTotal: 100 kB\nMemAvailable: 200 kB\n",
        "MemTotal: abc kB\nMemAvailable: 50 kB\n",
        "MemTotal: 100\nMemAvailable: 50 kB\n",
    ],
)
def test_parse_meminfo_rejects_invalid_input(text):
    with pytest.raises(MetricsError):
        parse_meminfo(text)


def test_read_host_memory_reads_file(tmp_path):
    path = write_meminfo(tmp_path)

    assert read_host_memory(path).total_bytes == 131_072_000 * 1024


def test_read_host_memory_wraps_oserror(tmp_path):
    with pytest.raises(MetricsError, match="could not read"):
        read_host_memory(tmp_path / "missing")


# GPU ----------------------------------------------------------------------


def test_collect_gpus_parses_output():
    calls = []

    def runner(args, **kwargs):
        calls.append((args, kwargs))
        return dispatch_runner()(args, **kwargs)

    gpus, error = collect_gpus(runner=runner)

    assert error is None
    assert gpus == (
        GpuInfo(
            index=0,
            name="NVIDIA GB10",
            memory_total_bytes=65_536 * 1024 * 1024,
            memory_used_bytes=1024 * 1024 * 1024,
            utilization_percent=5,
            temperature_celsius=40,
            uuid="GPU-aaa",
            compute_processes=2,
            compute_memory_bytes=768 * 1024 * 1024,
        ),
        GpuInfo(
            index=1,
            name="NVIDIA GB10",
            memory_total_bytes=65_536 * 1024 * 1024,
            memory_used_bytes=None,
            utilization_percent=None,
            temperature_celsius=None,
            uuid="GPU-bbb",
            compute_processes=1,
            compute_memory_bytes=106_268 * 1024 * 1024,
        ),
    )
    assert calls[0][0] == [
        "nvidia-smi",
        "--query-gpu=index,name,memory.total,memory.used,utilization.gpu,"
        "temperature.gpu,uuid",
        "--format=csv,noheader,nounits",
    ]
    assert calls[0][1]["capture_output"] is True
    assert calls[0][1]["text"] is True
    assert calls[1][0] == [
        "nvidia-smi",
        "--query-compute-apps=gpu_uuid,pid,process_name,used_memory",
        "--format=csv,noheader,nounits",
    ]


def test_collect_gpus_handles_unsupported_fields_and_quoted_names():
    stdout = (
        '0, "NVIDIA GB10, Rev 2", 65536, 1024, [Not Supported], '
        "[Insufficient Permissions], GPU-ccc\n"
    )

    gpus, error = collect_gpus(runner=dispatch_runner(gpu_stdout=stdout, compute_stdout=""))

    assert error is None
    assert gpus[0].name == "NVIDIA GB10, Rev 2"
    assert gpus[0].memory_total_bytes == 65_536 * 1024 * 1024
    assert gpus[0].memory_used_bytes == 1024 * 1024 * 1024
    assert gpus[0].utilization_percent is None
    assert gpus[0].temperature_celsius is None
    assert gpus[0].uuid == "GPU-ccc"
    assert gpus[0].compute_processes == 0


def test_collect_gpus_keeps_valid_rows_when_one_is_malformed():
    stdout = "0, NVIDIA GB10, 65536, 1024, 5, 40, GPU-aaa\nnot,a,valid,row\n"

    gpus, error = collect_gpus(
        runner=dispatch_runner(gpu_stdout=stdout, compute_stdout="")
    )

    assert len(gpus) == 1
    assert gpus[0].index == 0
    assert "could not parse" in error


def test_collect_gpus_returns_empty_for_no_gpus():
    gpus, error = collect_gpus(runner=dispatch_runner(gpu_stdout="", compute_stdout=""))

    assert gpus == ()
    assert error is None


def test_collect_gpus_reports_missing_binary():
    def runner(args, **kwargs):
        raise FileNotFoundError("nvidia-smi")

    gpus, error = collect_gpus(runner=runner)

    assert gpus == ()
    assert "not available" in error


def test_collect_gpus_reports_timeout():
    def runner(args, **kwargs):
        raise subprocess.TimeoutExpired(args, 1)

    gpus, error = collect_gpus(runner=runner, timeout=1)

    assert gpus == ()
    assert "timed out" in error


def test_collect_gpus_reports_nonzero_exit():
    gpus, error = collect_gpus(
        runner=lambda args, **kwargs: completed(args, returncode=1, stderr="driver not loaded")
    )

    assert gpus == ()
    assert error == "driver not loaded"


def test_collect_gpus_reports_malformed_output():
    gpus, error = collect_gpus(
        runner=lambda args, **kwargs: completed(args, stdout="not,a,valid,row\n")
    )

    assert gpus == ()
    assert "could not parse" in error


def test_collect_gpus_reports_compute_app_failure():
    def runner(args, **kwargs):
        if any("--query-compute-apps" in arg for arg in args):
            return completed(args, returncode=1, stderr="compute query failed")
        return completed(args, stdout=GPU_CSV)

    gpus, error = collect_gpus(runner=runner)

    assert len(gpus) == 2
    assert [gpu.compute_processes for gpu in gpus] == [None, None]
    assert [gpu.compute_memory_bytes for gpu in gpus] == [None, None]
    assert "could not query compute processes" in error
    assert "compute query failed" in error


def test_collect_gpus_reports_compute_app_missing_binary():
    def runner(args, **kwargs):
        if any("--query-compute-apps" in arg for arg in args):
            raise FileNotFoundError("nvidia-smi")
        return completed(args, stdout=GPU_CSV)

    gpus, error = collect_gpus(runner=runner)

    assert len(gpus) == 2
    assert [gpu.compute_processes for gpu in gpus] == [None, None]
    assert "nvidia-smi not available" in error


def test_collect_gpus_reports_compute_app_oserror():
    def runner(args, **kwargs):
        if any("--query-compute-apps" in arg for arg in args):
            raise OSError("driver busy")
        return completed(args, stdout=GPU_CSV)

    gpus, error = collect_gpus(runner=runner)

    assert len(gpus) == 2
    assert [gpu.compute_processes for gpu in gpus] == [None, None]
    assert "driver busy" in error


def test_collect_gpus_reports_compute_app_timeout():
    def runner(args, **kwargs):
        if any("--query-compute-apps" in arg for arg in args):
            raise subprocess.TimeoutExpired(args, 1)
        return completed(args, stdout=GPU_CSV)

    gpus, error = collect_gpus(runner=runner, timeout=1)

    assert len(gpus) == 2
    assert [gpu.compute_processes for gpu in gpus] == [None, None]
    assert "could not query compute processes" in error
    assert "timed out" in error


def test_collect_gpus_reports_compute_app_parse_error_as_unknown():
    compute = "GPU-aaa, 111, python, 512\nnot,a,valid,row,extra\n"

    gpus, error = collect_gpus(
        runner=dispatch_runner(gpu_stdout=GPU_CSV, compute_stdout=compute)
    )

    assert [gpu.compute_processes for gpu in gpus] == [None, None]
    assert "could not parse nvidia-smi compute-apps output" in error


def test_collect_gpus_rejects_bracketed_compute_uuid():
    compute = "[N/A], 111, python, 512\n"

    gpus, error = collect_gpus(
        runner=dispatch_runner(gpu_stdout=GPU_CSV, compute_stdout=compute)
    )

    assert [gpu.compute_processes for gpu in gpus] == [None, None]
    assert "could not parse nvidia-smi compute-apps output" in error
    assert "missing GPU UUID" in error


def test_collect_gpus_reports_missing_gpu_uuid_when_compute_rows_exist():
    gpu_csv = "0, NVIDIA GB10, 65536, [N/A], 5, 40, [N/A]\n"

    gpus, error = collect_gpus(
        runner=dispatch_runner(gpu_stdout=gpu_csv, compute_stdout=COMPUTE_CSV)
    )

    assert gpus[0].uuid is None
    assert gpus[0].compute_processes is None
    assert "could not attribute compute processes" in error
    assert "GPU-aaa" in error


def test_collect_gpus_marks_uuidless_gpu_idle_when_compute_query_is_empty():
    gpu_csv = "0, NVIDIA GB10, 65536, [N/A], 5, 40, [N/A]\n"

    gpus, error = collect_gpus(
        runner=dispatch_runner(gpu_stdout=gpu_csv, compute_stdout="")
    )

    assert error is None
    assert gpus[0].compute_processes == 0
    assert gpus[0].compute_memory_bytes == 0
    assert snapshot_to_dict(
        MetricsSnapshot(
            sampled_at=1.0,
            host_total_bytes=1000,
            host_available_bytes=500,
            containers=(),
            container_memory_bytes=0,
            cache_bytes=0,
            cache_total_bytes=0,
            cache_free_bytes=0,
            gpus=gpus,
            errors={},
        )
    )["gpus"][0]["resident"] is False


def test_collect_gpus_shares_timeout_budget():
    ticks = iter([0.0, 1.5])
    calls = []

    def runner(args, **kwargs):
        calls.append((args, kwargs))
        if any("--query-compute-apps" in arg for arg in args):
            return completed(args, stdout="")
        return completed(args, stdout=GPU_CSV)

    gpus, error = collect_gpus(
        runner=runner, timeout=5.0, clock=lambda: next(ticks)
    )

    assert error is None
    assert calls[1][1]["timeout"] == pytest.approx(3.5)


def test_collect_gpus_skips_compute_query_when_budget_is_exhausted():
    ticks = iter([0.0, 6.0])
    calls = []

    def runner(args, **kwargs):
        calls.append((args, kwargs))
        return completed(args, stdout=GPU_CSV)

    gpus, error = collect_gpus(
        runner=runner, timeout=5.0, clock=lambda: next(ticks)
    )

    assert len(calls) == 1
    assert [gpu.compute_processes for gpu in gpus] == [None, None]
    assert "no time budget remaining" in error


def test_collect_gpus_applies_compute_rows_when_another_gpu_row_is_malformed():
    gpu_csv = "0, NVIDIA GB10, 65536, 1024, 5, 40, GPU-aaa\nnot,a,valid,row\n"

    gpus, error = collect_gpus(
        runner=dispatch_runner(gpu_stdout=gpu_csv, compute_stdout="GPU-aaa, 111, python, 512\n")
    )

    assert len(gpus) == 1
    assert gpus[0].compute_processes == 1
    assert gpus[0].compute_memory_bytes == 512 * 1024 * 1024
    assert "could not parse nvidia-smi output" in error


def test_collect_gpus_counts_processes_with_unknown_memory():
    compute = "GPU-aaa, 111, python, [N/A]\nGPU-aaa, 222, python, 256\n"

    gpus, error = collect_gpus(
        runner=dispatch_runner(gpu_stdout=GPU_CSV, compute_stdout=compute)
    )

    assert error is None
    assert gpus[0].compute_processes == 2
    assert gpus[0].compute_memory_bytes is None
    assert gpus[1].compute_processes == 0


def test_collect_gpus_reports_unattributable_compute_uuid():
    compute = "GPU-zzz, 999, python, 4096\n"

    gpus, error = collect_gpus(
        runner=dispatch_runner(gpu_stdout=GPU_CSV, compute_stdout=compute)
    )

    assert [gpu.compute_processes for gpu in gpus] == [None, None]
    assert [gpu.compute_memory_bytes for gpu in gpus] == [None, None]
    assert "could not attribute compute processes" in error
    assert "GPU-zzz" in error


def test_collect_gpus_keeps_matched_counts_when_another_uuid_is_unattributable():
    compute = "GPU-aaa, 111, python, 512\nGPU-zzz, 999, python, 4096\n"

    gpus, error = collect_gpus(
        runner=dispatch_runner(gpu_stdout=GPU_CSV, compute_stdout=compute)
    )

    assert gpus[0].compute_processes == 1
    assert gpus[0].compute_memory_bytes == 512 * 1024 * 1024
    assert gpus[1].compute_processes is None
    assert gpus[1].compute_memory_bytes is None
    assert "could not attribute compute processes" in error
    assert "GPU-zzz" in error


# snapshot -----------------------------------------------------------------


def test_collect_metrics_sums_container_memory(tmp_path, monkeypatch):
    monkeypatch.setattr(
        metrics.containers,
        "managed_containers",
        lambda: [container(memory_usage=100), container(memory_usage=None)],
    )
    snapshot = collect_metrics(
        [tmp_path / "hub"],
        meminfo_path=write_meminfo(tmp_path),
        cache_sampler=lambda: 500,
        cache_usage_sampler=lambda: (0, 0, None),
        gpu_sampler=lambda: ((), None),
        clock=lambda: 1234.5,
    )

    assert snapshot.sampled_at == 1234.5
    assert snapshot.host_total_bytes == 131_072_000 * 1024
    assert snapshot.host_available_bytes == 65_536_000 * 1024
    assert len(snapshot.containers) == 2
    assert snapshot.container_memory_bytes == 100
    assert snapshot.cache_bytes == 500
    assert snapshot.gpus == ()
    assert snapshot.errors == {}


def test_collect_metrics_uses_default_container_sampler(tmp_path, monkeypatch):
    monkeypatch.setattr(
        metrics.containers, "managed_containers", lambda: [container(memory_usage=42)]
    )

    snapshot = collect_metrics(
        [tmp_path / "hub"],
        meminfo_path=write_meminfo(tmp_path),
        cache_sampler=lambda: 0,
        gpu_sampler=lambda: ((), None),
    )

    assert snapshot.container_memory_bytes == 42


def test_collect_metrics_records_container_error(tmp_path):
    def fail():
        raise RuntimeError("docker socket unavailable")

    snapshot = collect_metrics(
        [tmp_path / "hub"],
        meminfo_path=write_meminfo(tmp_path),
        container_sampler=fail,
        cache_sampler=lambda: 0,
        gpu_sampler=lambda: ((), None),
    )

    assert snapshot.containers == ()
    assert snapshot.container_memory_bytes == 0
    assert "docker socket unavailable" in snapshot.errors["containers"]


def test_collect_metrics_records_cache_error(tmp_path):
    def fail():
        raise OSError("permission denied")

    snapshot = collect_metrics(
        [tmp_path / "hub"],
        meminfo_path=write_meminfo(tmp_path),
        container_sampler=lambda: [],
        cache_sampler=fail,
        gpu_sampler=lambda: ((), None),
    )

    assert snapshot.cache_bytes is None
    assert "permission denied" in snapshot.errors["cache"]


def test_collect_metrics_uses_default_cache_sampler(tmp_path):
    root = tmp_path / "hub"
    snap = root / "models--org--name" / "snapshots" / "rev"
    snap.mkdir(parents=True)
    (snap / "config.json").write_bytes(b"x" * 10)

    snapshot = collect_metrics(
        [root],
        meminfo_path=write_meminfo(tmp_path),
        container_sampler=lambda: [],
        gpu_sampler=lambda: ((), None),
    )

    assert snapshot.cache_bytes == 10


def test_cache_disk_usage_sums_and_reports_walk_errors(tmp_path, monkeypatch):
    root = tmp_path / "hub"
    root.mkdir()
    (root / "file").write_bytes(b"1234")

    def fake_walk(path, onerror=None):
        onerror(OSError("permission denied"))
        yield str(path), [], ["file"]

    monkeypatch.setattr(metrics.os, "walk", fake_walk)

    total, error = cache_disk_usage([root])

    assert total == 4
    assert "permission denied" in error


def test_cache_disk_usage_uses_exception_type_for_empty_error_text(tmp_path, monkeypatch):
    root = tmp_path / "hub"
    root.mkdir()

    def fake_walk(path, onerror=None):
        onerror(OSError())
        yield str(path), [], []

    monkeypatch.setattr(metrics.os, "walk", fake_walk)

    total, error = cache_disk_usage([root])

    assert total == 0
    assert error == "OSError"


def test_cache_disk_usage_skips_symlinks(tmp_path):
    root = tmp_path / "hub"
    root.mkdir()
    (root / "real").write_bytes(b"1234")
    outside = tmp_path / "outside"
    outside.write_bytes(b"x" * 100)
    (root / "link").symlink_to(outside)
    (root / "dangling").symlink_to(tmp_path / "missing")

    total, error = cache_disk_usage([root])

    assert total == 4
    assert error is None


def test_cache_disk_usage_dedupes_nested_roots(tmp_path):
    root = tmp_path / "hub"
    nested = root / "models"
    nested.mkdir(parents=True)
    (nested / "file").write_bytes(b"1234")

    total, error = cache_disk_usage([root, nested])

    assert total == 4
    assert error is None


def test_cache_disk_usage_dedupes_nested_roots_before_walking(tmp_path, monkeypatch):
    root = tmp_path / "hub"
    nested = root / "models"
    nested.mkdir(parents=True)
    (nested / "file").write_bytes(b"1234")
    walk_calls = []

    def fake_walk(path, onerror=None):
        walk_calls.append(str(path))
        yield str(path), ["models"] if path == root else [], []
        if path == root:
            yield str(nested), [], ["file"]

    monkeypatch.setattr(metrics.os, "walk", fake_walk)

    total, error = cache_disk_usage([nested, root])

    assert total == 4
    assert error is None
    assert walk_calls == [str(root)]


def test_cache_disk_usage_reports_missing_and_non_directory_roots(tmp_path):
    missing = tmp_path / "missing"
    file_root = tmp_path / "file"
    file_root.write_bytes(b"x")
    root = tmp_path / "hub"
    root.mkdir()
    (root / "file").write_bytes(b"1234")

    total, error = cache_disk_usage([missing, file_root, root])

    assert total == 4
    assert f"{missing}: not found" in error
    assert f"{file_root}: not a directory" in error


def test_cache_disk_usage_records_directory_stat_error(tmp_path):
    root = tmp_path / "hub"
    nested = root / "nested"
    nested.mkdir(parents=True)
    (nested / "file").write_bytes(b"1234")
    real_stat = os.stat

    def fake_stat(path, *args, **kwargs):
        assert isinstance(path, Path)
        if path == nested:
            raise OSError("dir stat failed")
        return real_stat(path, *args, **kwargs)

    total, error = cache_disk_usage([root], stat=fake_stat)

    assert total == 0
    assert "dir stat failed" in error


def test_cache_disk_usage_reports_unreadable_root_beside_valid_root(tmp_path):
    blocked = tmp_path / "blocked"
    root = tmp_path / "hub"
    blocked.mkdir()
    root.mkdir()
    (root / "file").write_bytes(b"1234")
    real_stat = os.stat

    def fake_stat(path, *args, **kwargs):
        if Path(path) == blocked:
            raise PermissionError("permission denied")
        return real_stat(path, *args, **kwargs)

    total, error = cache_disk_usage([blocked, root], stat=fake_stat)

    assert total == 4
    assert f"{blocked}: permission denied" in error


def test_cache_disk_usage_dedupes_hardlinked_files(tmp_path):
    first = tmp_path / "hub"
    second = tmp_path / "scratch"
    first.mkdir()
    second.mkdir()
    source = first / "file"
    source.write_bytes(b"1234")
    (second / "file").hardlink_to(source)

    total, error = cache_disk_usage([first, second])

    assert total == 4
    assert error is None


def test_cache_disk_usage_summarizes_many_errors(tmp_path, monkeypatch):
    root = tmp_path / "hub"
    root.mkdir()

    def fake_walk(path, onerror=None):
        for index in range(7):
            onerror(OSError(f"error {index}"))
        yield str(path), [], []

    monkeypatch.setattr(metrics.os, "walk", fake_walk)

    total, error = cache_disk_usage([root])

    assert total == 0
    assert "error 0" in error
    assert "error 4" in error
    assert "error 5" not in error
    assert "... and 2 more" in error


def test_cache_disk_usage_records_empty_stat_error(tmp_path):
    root = tmp_path / "hub"
    root.mkdir()
    (root / "good").write_bytes(b"1234")
    (root / "bad").write_bytes(b"x")
    real_stat = os.stat

    def fake_stat(path, *args, **kwargs):
        if Path(path).name == "bad":
            raise OSError()
        return real_stat(path, *args, **kwargs)

    total, error = cache_disk_usage([root], stat=fake_stat)

    assert total == 4
    assert error == "OSError"


def test_cache_filesystem_usage_probes_later_root_after_usage_failure(tmp_path):
    first = tmp_path / "hub"
    second = tmp_path / "hub" / "models"
    first.mkdir()
    second.mkdir()
    calls = []

    def fake_stat(path):
        return SimpleNamespace(st_dev=7, st_mode=stat.S_IFDIR)

    def fake_usage(path):
        calls.append(path)
        if path == first:
            raise OSError("first failed")
        return SimpleNamespace(total=1000, free=250)

    total, free, error = cache_filesystem_usage(
        [first, second], stat=fake_stat, disk_usage=fake_usage
    )

    assert (total, free) == (1000, 250)
    assert calls == [first, second]
    assert error == f"{first}: first failed"


def test_cache_filesystem_usage_skips_duplicate_device_without_probing(tmp_path):
    first = tmp_path / "hub"
    second = tmp_path / "hub" / "models"
    first.mkdir()
    second.mkdir()
    calls = []

    def fake_stat(path):
        return SimpleNamespace(st_dev=7, st_mode=stat.S_IFDIR)

    def fake_usage(path):
        calls.append(path)
        if path == second:
            raise OSError("duplicate device should not be probed")
        return SimpleNamespace(total=1000, free=250)

    total, free, error = cache_filesystem_usage(
        [first, second], stat=fake_stat, disk_usage=fake_usage
    )

    assert (total, free, error) == (1000, 250, None)
    assert calls == [first]


def test_cache_filesystem_usage_sums_distinct_devices(tmp_path):
    first = tmp_path / "hub"
    second = tmp_path / "scratch"
    first.mkdir()
    second.mkdir()
    devices = {first: 1, second: 2}
    usages = {
        first: SimpleNamespace(total=1000, free=250),
        second: SimpleNamespace(total=2000, free=500),
    }

    total, free, error = cache_filesystem_usage(
        [first, second],
        stat=lambda path: SimpleNamespace(
            st_dev=devices[path], st_mode=stat.S_IFDIR
        ),
        disk_usage=lambda path: usages[path],
    )

    assert (total, free, error) == (3000, 750, None)


def test_cache_filesystem_usage_dedupes_real_roots(tmp_path):
    first = tmp_path / "hub"
    second = tmp_path / "hub" / "models"
    first.mkdir()
    second.mkdir()
    calls = []

    def fake_usage(path):
        calls.append(path)
        return SimpleNamespace(total=1000, free=250)

    total, free, error = cache_filesystem_usage([first, second], disk_usage=fake_usage)

    assert (total, free, error) == (1000, 250, None)
    assert calls == [first]


def test_cache_filesystem_usage_reports_when_no_root_resolves(tmp_path):
    missing = tmp_path / "missing"
    file_root = tmp_path / "file"
    file_root.write_bytes(b"x")

    total, free, error = cache_filesystem_usage([missing, file_root])

    assert (total, free) == (None, None)
    assert "no usable cache root" in error
    assert str(missing) in error
    assert str(file_root) in error


def test_cache_filesystem_usage_reports_empty_root_list():
    total, free, error = cache_filesystem_usage([])

    assert (total, free) == (None, None)
    assert error == "no cache roots configured"


def test_cache_filesystem_usage_reports_missing_root_beside_valid_root(tmp_path):
    root = tmp_path / "hub"
    missing = tmp_path / "missing"
    root.mkdir()

    total, free, error = cache_filesystem_usage(
        [missing, root],
        disk_usage=lambda path: SimpleNamespace(total=1000, free=250),
    )

    assert (total, free) == (1000, 250)
    assert f"{missing}: not found" in error
    assert not error.startswith("no usable cache root")


def test_cache_filesystem_usage_summarizes_many_errors(tmp_path):
    roots = [tmp_path / f"root-{index}" for index in range(7)]

    def fake_stat(path):
        raise OSError(f"error {path.name}")

    total, free, error = cache_filesystem_usage(roots, stat=fake_stat)

    assert (total, free) == (None, None)
    assert "no usable cache root" in error
    assert "error root-0" in error
    assert "error root-4" in error
    assert "error root-5" not in error
    assert error.count("... and 2 more") == 2


def test_cache_filesystem_usage_caps_root_prefix(tmp_path):
    roots = [tmp_path / f"root-{index}" for index in range(7)]

    def fake_stat(path):
        raise FileNotFoundError(path)

    total, free, error = cache_filesystem_usage(roots, stat=fake_stat)

    assert (total, free) == (None, None)
    assert "no usable cache root" in error
    assert "root-4" in error
    assert "root-5" not in error
    assert error.count("... and 2 more") == 2


def test_cache_filesystem_usage_uses_default_disk_usage(tmp_path):
    total, free, error = cache_filesystem_usage([tmp_path])

    assert error is None
    assert total > 0
    assert 0 <= free <= total


def test_cache_filesystem_usage_keeps_five_errors_without_suffix(tmp_path):
    roots = [tmp_path / f"root-{index}" for index in range(5)]

    def fake_stat(path):
        raise OSError(f"error {path.name}")

    total, free, error = cache_filesystem_usage(roots, stat=fake_stat)

    assert (total, free) == (None, None)
    assert "no usable cache root" in error
    assert "error root-4" in error
    assert "more" not in error


def test_cache_filesystem_usage_reports_stat_and_usage_errors(tmp_path):
    stat_fails = tmp_path / "stat-fails"
    usage_fails = tmp_path / "usage-fails"
    stat_fails.mkdir()
    usage_fails.mkdir()

    def fake_stat(path):
        if path == stat_fails:
            raise OSError("stat denied")
        return SimpleNamespace(st_dev=3, st_mode=stat.S_IFDIR)

    def fake_usage(path):
        if path == usage_fails:
            raise OSError("usage denied")
        return SimpleNamespace(total=1000, free=250)

    total, free, error = cache_filesystem_usage(
        [stat_fails, usage_fails],
        stat=fake_stat,
        disk_usage=fake_usage,
    )

    assert (total, free) == (None, None)
    assert "stat denied" in error
    assert "usage denied" in error


def test_collect_metrics_includes_cache_filesystem_capacity(tmp_path):
    root = tmp_path / "hub"
    root.mkdir()

    snapshot = collect_metrics(
        [root],
        meminfo_path=write_meminfo(tmp_path),
        container_sampler=lambda: [],
        cache_sampler=lambda: 500,
        cache_usage_sampler=lambda: (1000, 250, None),
        gpu_sampler=lambda: ((), None),
    )

    assert snapshot.cache_bytes == 500
    assert snapshot.cache_total_bytes == 1000
    assert snapshot.cache_free_bytes == 250


def test_collect_metrics_uses_default_cache_usage_sampler(tmp_path, monkeypatch):
    monkeypatch.setattr(
        metrics, "cache_filesystem_usage", lambda roots: (1000, 250, None)
    )

    snapshot = collect_metrics(
        [tmp_path / "hub"],
        meminfo_path=write_meminfo(tmp_path),
        container_sampler=lambda: [],
        cache_sampler=lambda: 500,
        gpu_sampler=lambda: ((), None),
    )

    assert snapshot.cache_total_bytes == 1000
    assert snapshot.cache_free_bytes == 250


def test_collect_metrics_records_cache_capacity_error(tmp_path):
    snapshot = collect_metrics(
        [tmp_path / "hub"],
        meminfo_path=write_meminfo(tmp_path),
        container_sampler=lambda: [],
        cache_sampler=lambda: 500,
        cache_usage_sampler=lambda: (None, None, "disk usage failed"),
        gpu_sampler=lambda: ((), None),
    )

    assert snapshot.cache_total_bytes is None
    assert snapshot.cache_free_bytes is None
    assert "disk usage failed" in snapshot.errors["cache"]


def test_collect_metrics_keeps_partial_capacity_beside_a_bad_root(tmp_path):
    snapshot = collect_metrics(
        [tmp_path / "hub"],
        meminfo_path=write_meminfo(tmp_path),
        container_sampler=lambda: [],
        cache_sampler=lambda: 500,
        cache_usage_sampler=lambda: (1000, 250, "one root missing"),
        gpu_sampler=lambda: ((), None),
    )

    assert snapshot.cache_total_bytes == 1000
    assert snapshot.cache_free_bytes == 250
    assert "one root missing" in snapshot.errors["cache"]


def test_collect_metrics_records_cache_usage_sampler_exception(tmp_path):
    def fail():
        raise OSError("statvfs failed")

    snapshot = collect_metrics(
        [tmp_path / "hub"],
        meminfo_path=write_meminfo(tmp_path),
        container_sampler=lambda: [],
        cache_sampler=lambda: 500,
        cache_usage_sampler=fail,
        gpu_sampler=lambda: ((), None),
    )

    assert snapshot.cache_total_bytes is None
    assert snapshot.cache_free_bytes is None
    assert "statvfs failed" in snapshot.errors["cache"]


def test_collect_metrics_aggregates_cache_errors_from_both_sources(tmp_path):
    def fail_bytes():
        raise OSError("walk failed")

    snapshot = collect_metrics(
        [tmp_path / "hub"],
        meminfo_path=write_meminfo(tmp_path),
        container_sampler=lambda: [],
        cache_sampler=fail_bytes,
        cache_usage_sampler=lambda: (None, None, "statvfs failed"),
        gpu_sampler=lambda: ((), None),
    )

    assert snapshot.cache_bytes is None
    assert snapshot.cache_total_bytes is None
    assert snapshot.cache_free_bytes is None
    assert "walk failed" in snapshot.errors["cache"]
    assert "statvfs failed" in snapshot.errors["cache"]


def test_collect_metrics_uses_exception_type_for_empty_error_text(tmp_path):
    def fail():
        raise OSError()

    snapshot = collect_metrics(
        [tmp_path / "hub"],
        meminfo_path=write_meminfo(tmp_path),
        container_sampler=lambda: [],
        cache_sampler=lambda: 500,
        cache_usage_sampler=fail,
        gpu_sampler=lambda: ((), None),
    )

    assert snapshot.cache_total_bytes is None
    assert snapshot.cache_free_bytes is None
    assert snapshot.errors["cache"] == "OSError"


def test_collect_metrics_ignores_empty_capacity_error_text(tmp_path):
    snapshot = collect_metrics(
        [tmp_path / "hub"],
        meminfo_path=write_meminfo(tmp_path),
        container_sampler=lambda: [],
        cache_sampler=lambda: 500,
        cache_usage_sampler=lambda: (1000, 250, ""),
        gpu_sampler=lambda: ((), None),
    )

    assert snapshot.cache_total_bytes == 1000
    assert snapshot.cache_free_bytes == 250
    assert "cache" not in snapshot.errors


def test_collect_metrics_reports_no_cache_roots(tmp_path):
    snapshot = collect_metrics(
        [],
        meminfo_path=write_meminfo(tmp_path),
        container_sampler=lambda: [],
        cache_sampler=lambda: 0,
        gpu_sampler=lambda: ((), None),
    )

    assert snapshot.cache_total_bytes is None
    assert snapshot.cache_free_bytes is None
    assert "no cache roots configured" in snapshot.errors["cache"]


def test_collect_metrics_uses_default_samplers_for_empty_roots(tmp_path):
    snapshot = collect_metrics(
        [],
        meminfo_path=write_meminfo(tmp_path),
        container_sampler=lambda: [],
        gpu_sampler=lambda: ((), None),
    )

    assert snapshot.cache_bytes == 0
    assert snapshot.cache_total_bytes is None
    assert snapshot.cache_free_bytes is None
    assert "no cache roots configured" in snapshot.errors["cache"]


def test_collect_metrics_reports_unknown_capacity_without_error_text(tmp_path):
    snapshot = collect_metrics(
        [tmp_path / "hub"],
        meminfo_path=write_meminfo(tmp_path),
        container_sampler=lambda: [],
        cache_sampler=lambda: 500,
        cache_usage_sampler=lambda: (None, None, ""),
        gpu_sampler=lambda: ((), None),
    )

    assert snapshot.cache_total_bytes is None
    assert snapshot.cache_free_bytes is None
    assert "cache capacity unknown" in snapshot.errors["cache"]


def test_collect_metrics_reports_unknown_cache_bytes_without_error_text(tmp_path):
    snapshot = collect_metrics(
        [tmp_path / "hub"],
        meminfo_path=write_meminfo(tmp_path),
        container_sampler=lambda: [],
        cache_sampler=lambda: None,
        cache_usage_sampler=lambda: (1000, 250, None),
        gpu_sampler=lambda: ((), None),
    )

    assert snapshot.cache_bytes is None
    assert "cache bytes unavailable" in snapshot.errors["cache"]


def test_collect_metrics_nulls_half_known_capacity(tmp_path):
    snapshot = collect_metrics(
        [tmp_path / "hub"],
        meminfo_path=write_meminfo(tmp_path),
        container_sampler=lambda: [],
        cache_sampler=lambda: 500,
        cache_usage_sampler=lambda: (1000, None, None),
        gpu_sampler=lambda: ((), None),
    )

    assert snapshot.cache_total_bytes is None
    assert snapshot.cache_free_bytes is None
    assert "cache capacity unknown" in snapshot.errors["cache"]


def test_collect_metrics_reports_bytes_and_capacity_errors_together(tmp_path):
    snapshot = collect_metrics(
        [tmp_path / "hub"],
        meminfo_path=write_meminfo(tmp_path),
        container_sampler=lambda: [],
        cache_sampler=lambda: None,
        cache_usage_sampler=lambda: (None, None, "statvfs failed"),
        gpu_sampler=lambda: ((), None),
    )

    assert snapshot.cache_bytes is None
    assert snapshot.cache_total_bytes is None
    assert snapshot.cache_free_bytes is None
    assert "cache bytes unavailable" in snapshot.errors["cache"]
    assert "statvfs failed" in snapshot.errors["cache"]


def test_collect_metrics_records_empty_walk_error(tmp_path, monkeypatch):
    root = tmp_path / "hub"
    root.mkdir()

    def fake_walk(path, onerror=None):
        onerror(OSError())
        yield str(path), [], []

    monkeypatch.setattr(metrics.os, "walk", fake_walk)

    snapshot = collect_metrics(
        [root],
        meminfo_path=write_meminfo(tmp_path),
        container_sampler=lambda: [],
        cache_usage_sampler=lambda: (1000, 250, None),
        gpu_sampler=lambda: ((), None),
    )

    assert snapshot.cache_bytes == 0
    assert "OSError" in snapshot.errors["cache"]


def test_collect_metrics_records_default_cache_error(tmp_path, monkeypatch):
    monkeypatch.setattr(metrics, "cache_disk_usage", lambda roots: (100, "permission denied"))

    snapshot = collect_metrics(
        [tmp_path / "hub"],
        meminfo_path=write_meminfo(tmp_path),
        container_sampler=lambda: [],
        gpu_sampler=lambda: ((), None),
    )

    assert snapshot.cache_bytes == 100
    assert "permission denied" in snapshot.errors["cache"]


def test_collect_metrics_includes_gpu_data(tmp_path):
    gpu = GpuInfo(
        index=0,
        name="NVIDIA GB10",
        memory_total_bytes=1000,
        memory_used_bytes=500,
        utilization_percent=5,
        temperature_celsius=40,
    )

    snapshot = collect_metrics(
        [tmp_path / "hub"],
        meminfo_path=write_meminfo(tmp_path),
        container_sampler=lambda: [],
        cache_sampler=lambda: 0,
        gpu_sampler=lambda: ((gpu,), None),
    )

    assert snapshot.gpus == (gpu,)
    assert "gpu" not in snapshot.errors


def test_collect_metrics_records_gpu_error(tmp_path):
    snapshot = collect_metrics(
        [tmp_path / "hub"],
        meminfo_path=write_meminfo(tmp_path),
        container_sampler=lambda: [],
        cache_sampler=lambda: 0,
        gpu_sampler=lambda: ((), "nvidia-smi not available"),
    )

    assert snapshot.gpus == ()
    assert snapshot.errors["gpu"] == "nvidia-smi not available"


def test_collect_metrics_uses_default_gpu_sampler(tmp_path, monkeypatch):
    gpu = GpuInfo(0, "NVIDIA GB10", 1000, 500, 5, 40)
    monkeypatch.setattr(metrics, "collect_gpus", lambda: ((gpu,), None))

    snapshot = collect_metrics(
        [tmp_path / "hub"],
        meminfo_path=write_meminfo(tmp_path),
        container_sampler=lambda: [],
        cache_sampler=lambda: 0,
    )

    assert snapshot.gpus == (gpu,)


def test_snapshot_to_dict_shape():
    snapshot = MetricsSnapshot(
        sampled_at=1234.5,
        host_total_bytes=1000,
        host_available_bytes=500,
        containers=(container(),),
        container_memory_bytes=100,
        cache_bytes=200,
        cache_total_bytes=10_000,
        cache_free_bytes=4_000,
        gpus=(
            GpuInfo(
                index=0,
                name="NVIDIA GB10",
                memory_total_bytes=1000,
                memory_used_bytes=500,
                utilization_percent=5,
                temperature_celsius=40,
                uuid="GPU-aaa",
                compute_processes=2,
                compute_memory_bytes=768,
            ),
            GpuInfo(
                index=1,
                name="NVIDIA GB10",
                memory_total_bytes=None,
                memory_used_bytes=None,
                utilization_percent=None,
                temperature_celsius=None,
                uuid="GPU-bbb",
                compute_processes=0,
                compute_memory_bytes=0,
            ),
        ),
        errors={"gpu": "unavailable"},
    )

    body = snapshot_to_dict(snapshot)

    assert body["sampled_at"] == 1234.5
    assert body["host_total_bytes"] == 1000
    assert body["host_available_bytes"] == 500
    assert body["container_memory_bytes"] == 100
    assert body["cache_bytes"] == 200
    assert body["cache_total_bytes"] == 10_000
    assert body["cache_free_bytes"] == 4_000
    assert body["containers"] == [
        {
            "id": "abc123",
            "name": "spark-fleet-demo-glm-head",
            "state": "running",
            "status": "Up 2 minutes",
            "health": "healthy",
            "memory_usage_bytes": 100,
            "memory_limit_bytes": 200,
            "memory_percent": 50.0,
            "labels": {
                "spark.fleet.deployment": "demo-glm",
                "spark.fleet.role": "head",
            },
        }
    ]
    assert body["gpus"] == [
        {
            "index": 0,
            "name": "NVIDIA GB10",
            "memory_total_bytes": 1000,
            "memory_used_bytes": 500,
            "utilization_percent": 5,
            "temperature_celsius": 40,
            "uuid": "GPU-aaa",
            "compute_processes": 2,
            "compute_memory_bytes": 768,
            "resident": True,
        },
        {
            "index": 1,
            "name": "NVIDIA GB10",
            "memory_total_bytes": None,
            "memory_used_bytes": None,
            "utilization_percent": None,
            "temperature_celsius": None,
            "uuid": "GPU-bbb",
            "compute_processes": 0,
            "compute_memory_bytes": 0,
            "resident": False,
        },
    ]
    assert body["errors"] == {"gpu": "unavailable"}


def test_snapshot_to_dict_reports_unknown_residency_when_compute_is_unavailable():
    snapshot = MetricsSnapshot(
        sampled_at=1.0,
        host_total_bytes=1000,
        host_available_bytes=500,
        containers=(),
        container_memory_bytes=0,
        cache_bytes=0,
        cache_total_bytes=0,
        cache_free_bytes=0,
        gpus=(
            GpuInfo(
                index=0,
                name="NVIDIA GB10",
                memory_total_bytes=None,
                memory_used_bytes=None,
                utilization_percent=95,
                temperature_celsius=60,
                uuid="GPU-aaa",
            ),
        ),
        errors={"gpu": "could not query compute processes"},
    )

    body = snapshot_to_dict(snapshot)

    assert body["gpus"][0]["compute_processes"] is None
    assert body["gpus"][0]["compute_memory_bytes"] is None
    assert body["gpus"][0]["resident"] is None


# endpoint -----------------------------------------------------------------


def test_metrics_endpoint_requires_token(tmp_path):
    client = TestClient(build_app(config(tmp_path)))

    response = client.get("/metrics")

    assert response.status_code == 401
    assert response.headers["www-authenticate"] == "Bearer"


def test_metrics_endpoint_returns_snapshot(tmp_path, monkeypatch):
    agent_config = config(tmp_path)
    snapshot = MetricsSnapshot(
        sampled_at=1234.5,
        host_total_bytes=1000,
        host_available_bytes=500,
        containers=(),
        container_memory_bytes=0,
        cache_bytes=200,
        cache_total_bytes=10_000,
        cache_free_bytes=4_000,
        gpus=(),
        errors={},
    )

    def fake_collect(roots):
        assert tuple(roots) == agent_config.model_cache_roots
        return snapshot

    monkeypatch.setattr("spark_fleet.agent.api.collect_metrics", fake_collect)
    client = TestClient(build_app(agent_config))

    response = client.get("/metrics", headers=auth())

    assert response.status_code == 200
    body = response.json()
    assert body["sampled_at"] == 1234.5
    assert body["cache_bytes"] == 200
    assert body["cache_total_bytes"] == 10_000
    assert body["cache_free_bytes"] == 4_000


def test_metrics_endpoint_reports_host_error(tmp_path, monkeypatch):
    def fail(roots):
        raise MetricsError("could not read /proc/meminfo")

    monkeypatch.setattr("spark_fleet.agent.api.collect_metrics", fail)
    client = TestClient(build_app(config(tmp_path)))

    response = client.get("/metrics", headers=auth())

    assert response.status_code == 503
    assert "meminfo" in response.json()["detail"]
