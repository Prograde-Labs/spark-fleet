"""Tests for the agent model cache catalog, downloads, removal, and job manager."""

import io
import logging
import os
import threading
import time
from pathlib import Path

import pytest

from spark_fleet.agent import models
from spark_fleet.agent.models import (
    CacheRoot,
    Catalog,
    DownloadCancelled,
    ModelCatalog,
    ModelEntry,
    ModelError,
    ModelJobManager,
    ModelJobStatus,
)


def cache_dir(root: Path, repo: str) -> Path:
    return root / f"models--{repo.replace('/', '--')}"


def write_cached(root: Path, repo: str, payload: bytes = b"{}") -> Path:
    snap = cache_dir(root, repo) / "snapshots" / "abc123"
    snap.mkdir(parents=True)
    (snap / "config.json").write_bytes(payload)
    return cache_dir(root, repo)


def write_partial(root: Path, repo: str, payload: bytes = b"partial") -> Path:
    blobs = cache_dir(root, repo) / "blobs"
    blobs.mkdir(parents=True)
    (blobs / "weights.incomplete").write_bytes(payload)
    return cache_dir(root, repo)


# scan ---------------------------------------------------------------------


def test_scan_reports_cached_entry(tmp_path):
    root = tmp_path / "hub"
    path = write_cached(root, "org/name", b"12345")

    result = ModelCatalog([root]).scan()

    assert isinstance(result, Catalog)
    assert result.roots == (CacheRoot(root, exists=True, error=None),)
    assert len(result.models) == 1
    entry = result.models[0]
    assert isinstance(entry, ModelEntry)
    assert entry.repo == "org/name"
    assert entry.root == root
    assert entry.path == path
    assert entry.cached is True
    assert entry.partial is False
    assert entry.size_bytes == 5
    assert entry.error is None


def test_scan_reports_partial_entry(tmp_path):
    root = tmp_path / "hub"
    path = write_partial(root, "org/name", b"1234567")

    entry = ModelCatalog([root]).scan().models[0]

    assert entry.path == path
    assert entry.cached is False
    assert entry.partial is True
    assert entry.size_bytes == 7


def test_scan_reports_empty_cache_dir_as_partial(tmp_path):
    root = tmp_path / "hub"
    cache_dir(root, "org/name").mkdir(parents=True)

    entry = ModelCatalog([root]).scan().models[0]

    assert entry.cached is False
    assert entry.partial is True
    assert entry.size_bytes == 0


def test_scan_prefers_cached_when_incomplete_files_are_present(tmp_path):
    root = tmp_path / "hub"
    path = write_cached(root, "org/name", b"{}")
    (path / "blobs").mkdir()
    (path / "blobs" / "weights.incomplete").write_bytes(b"x" * 10)

    entry = ModelCatalog([root]).scan().models[0]

    assert entry.cached is True
    assert entry.partial is False


def test_scan_counts_symlinked_snapshot_file_once(tmp_path):
    root = tmp_path / "hub"
    path = cache_dir(root, "org/name")
    blob = path / "blobs" / "weights"
    blob.parent.mkdir(parents=True)
    blob.write_bytes(b"x" * 100)
    snap = path / "snapshots" / "rev"
    snap.mkdir(parents=True)
    (snap / "model.safetensors").symlink_to(blob)

    entry = ModelCatalog([root]).scan().models[0]

    assert entry.cached is True
    assert entry.size_bytes == 100


def test_scan_counts_hardlinked_files_once(tmp_path):
    root = tmp_path / "hub"
    path = cache_dir(root, "org/name")
    blobs = path / "blobs"
    blobs.mkdir(parents=True)
    first = blobs / "a"
    first.write_bytes(b"x" * 100)
    os.link(first, blobs / "b")

    entry = ModelCatalog([root]).scan().models[0]

    assert entry.size_bytes == 100


def test_scan_ignores_unrelated_and_ambiguous_cache_dirs(tmp_path):
    root = tmp_path / "hub"
    root.mkdir()
    (root / "other").mkdir()
    (root / "models--single").mkdir()
    (root / "models--org--a--b").mkdir()
    (root / "models--org--").mkdir()

    assert ModelCatalog([root]).scan().models == ()


def test_scan_ignores_symlinked_cache_dirs(tmp_path):
    root = tmp_path / "hub"
    outside = tmp_path / "outside"
    write_cached(outside, "org/name")
    root.mkdir()
    (root / "models--org--name").symlink_to(cache_dir(outside, "org/name"))

    assert ModelCatalog([root]).scan().models == ()


def test_scan_reports_missing_root(tmp_path):
    root = tmp_path / "missing"

    result = ModelCatalog([root]).scan()

    assert result.roots == (CacheRoot(root, exists=False, error=None),)
    assert result.models == ()


def test_scan_reports_unreadable_root(tmp_path, monkeypatch):
    root = tmp_path / "hub"
    root.mkdir()

    def fail(_root):
        raise OSError("permission denied")

    monkeypatch.setattr(models, "_list_cache_dirs", fail)

    result = ModelCatalog([root]).scan()

    assert result.roots == (CacheRoot(root, exists=True, error="permission denied"),)
    assert result.models == ()


def test_scan_reports_entry_error(tmp_path, monkeypatch):
    root = tmp_path / "hub"
    write_cached(root, "org/name")

    def fail(_path):
        raise OSError("stat failed")

    monkeypatch.setattr(models, "_directory_size", fail)

    entry = ModelCatalog([root]).scan().models[0]

    assert entry.error == "stat failed"
    assert entry.cached is False
    assert entry.partial is True
    assert entry.size_bytes == 0


def test_scan_sorts_by_repo_then_root(tmp_path):
    root_b = tmp_path / "b"
    root_a = tmp_path / "a"
    write_cached(root_b, "org/zeta")
    write_cached(root_a, "org/alpha")
    write_cached(root_a, "org/zeta")

    result = ModelCatalog([root_b, root_a]).scan()

    assert [(entry.repo, entry.root.name) for entry in result.models] == [
        ("org/alpha", "a"),
        ("org/zeta", "a"),
        ("org/zeta", "b"),
    ]


def test_catalog_rejects_empty_and_relative_roots(tmp_path):
    with pytest.raises(ModelError, match="root"):
        ModelCatalog([])
    with pytest.raises(ModelError, match="absolute"):
        ModelCatalog([Path("relative")])


def test_catalog_deduplicates_roots(tmp_path):
    root = tmp_path / "hub"

    assert ModelCatalog([root, root]).roots == (root,)


def test_find_and_is_cached(tmp_path):
    root_a = tmp_path / "a"
    root_b = tmp_path / "b"
    write_cached(root_a, "org/name")
    write_partial(root_b, "org/name")
    catalog = ModelCatalog([root_a, root_b])

    entries = catalog.find("org/name")

    assert [entry.root for entry in entries] == [root_a, root_b]
    assert catalog.is_cached("org/name") is True
    assert catalog.is_cached("org/missing") is False


def test_find_rejects_invalid_repo():
    with pytest.raises(ModelError, match="invalid repository"):
        ModelCatalog([Path("/tmp/hub")]).find("org/a--b")


# download -----------------------------------------------------------------


def test_download_uses_first_root_and_passes_options(tmp_path, monkeypatch):
    root = tmp_path / "hub"
    calls = {}

    def fake(repo_id, *, cache_dir, revision=None, allow_patterns=None, tqdm_class=None, **kwargs):
        calls.update(
            repo_id=repo_id,
            cache_dir=cache_dir,
            revision=revision,
            allow_patterns=allow_patterns,
            tqdm_class=tqdm_class,
        )
        return str(Path(cache_dir) / f"models--{repo_id.replace('/', '--')}" / "snapshots" / "rev")

    monkeypatch.setattr(models, "snapshot_download", fake)

    path = ModelCatalog([root]).download("org/name", revision="v1", allow_patterns=["*.json"])

    assert path == root / "models--org--name" / "snapshots" / "rev"
    assert calls["repo_id"] == "org/name"
    assert calls["cache_dir"] == str(root)
    assert calls["revision"] == "v1"
    assert calls["allow_patterns"] == ["*.json"]
    assert calls["tqdm_class"] is not None


def test_download_prefers_root_with_existing_cache(tmp_path, monkeypatch):
    root_a = tmp_path / "a"
    root_b = tmp_path / "b"
    write_partial(root_b, "org/name")
    calls = []

    def fake(repo_id, *, cache_dir, **kwargs):
        calls.append(cache_dir)
        return str(Path(cache_dir) / "snapshots" / "rev")

    monkeypatch.setattr(models, "snapshot_download", fake)

    ModelCatalog([root_a, root_b]).download("org/name")

    assert calls == [str(root_b)]


def test_download_uses_explicit_root(tmp_path, monkeypatch):
    root_a = tmp_path / "a"
    root_b = tmp_path / "b"
    calls = []

    def fake(repo_id, *, cache_dir, **kwargs):
        calls.append(cache_dir)
        return str(cache_dir)

    monkeypatch.setattr(models, "snapshot_download", fake)

    ModelCatalog([root_a, root_b]).download("org/name", root=root_b)

    assert calls == [str(root_b)]


def test_download_rejects_unconfigured_root(tmp_path, monkeypatch):
    monkeypatch.setattr(
        models, "snapshot_download", lambda *a, **k: pytest.fail("download should not run")
    )

    with pytest.raises(ModelError, match="not configured"):
        ModelCatalog([tmp_path / "hub"]).download("org/name", root=tmp_path / "other")


@pytest.mark.parametrize("repo", ["", "org", "org/name/x", "org/a--b", "../x", "org/na me"])
def test_download_rejects_invalid_repo(tmp_path, monkeypatch, repo):
    monkeypatch.setattr(
        models, "snapshot_download", lambda *a, **k: pytest.fail("download should not run")
    )

    with pytest.raises(ModelError, match="invalid repository"):
        ModelCatalog([tmp_path / "hub"]).download(repo)


def test_download_rejects_cancelled_request_before_start(tmp_path, monkeypatch):
    cancel = threading.Event()
    cancel.set()
    monkeypatch.setattr(
        models, "snapshot_download", lambda *a, **k: pytest.fail("download should not run")
    )

    with pytest.raises(DownloadCancelled):
        ModelCatalog([tmp_path / "hub"]).download("org/name", cancel_event=cancel)


def test_download_reports_progress(tmp_path, monkeypatch):
    progress = []

    def fake(repo_id, *, cache_dir, tqdm_class, **kwargs):
        bar = tqdm_class(desc="config.json", total=10, unit="B", file=io.StringIO())
        bar.update(4)
        bar.update(6)
        bar.close()
        return str(cache_dir)

    monkeypatch.setattr(models, "snapshot_download", fake)

    ModelCatalog([tmp_path / "hub"]).download("org/name", on_progress=progress.append)

    assert progress == [
        {"repo": "org/name", "file": "config.json", "done": 4, "total": 10},
        {"repo": "org/name", "file": "config.json", "done": 10, "total": 10},
    ]


def test_download_cancel_event_aborts_progress(tmp_path, monkeypatch):
    cancel = threading.Event()

    def fake(repo_id, *, cache_dir, tqdm_class, **kwargs):
        bar = tqdm_class(desc="config.json", total=10, unit="B", file=io.StringIO())
        bar.update(1)
        cancel.set()
        bar.update(1)
        return str(cache_dir)

    monkeypatch.setattr(models, "snapshot_download", fake)

    with pytest.raises(DownloadCancelled):
        ModelCatalog([tmp_path / "hub"]).download("org/name", cancel_event=cancel)


def test_download_survives_progress_observer_failure(tmp_path, monkeypatch, caplog):
    def fake(repo_id, *, cache_dir, tqdm_class, **kwargs):
        bar = tqdm_class(desc="config.json", total=1, unit="B", file=io.StringIO())
        bar.update(1)
        return str(cache_dir)

    monkeypatch.setattr(models, "snapshot_download", fake)

    def observer(_progress):
        raise RuntimeError("observer boom")

    with caplog.at_level(logging.WARNING, logger="spark_fleet.agent.models"):
        path = ModelCatalog([tmp_path / "hub"]).download("org/name", on_progress=observer)

    assert path == tmp_path / "hub"
    assert any("progress observer failed" in record.message for record in caplog.records)


def test_download_wraps_download_errors(tmp_path, monkeypatch):
    def fake(repo_id, *, cache_dir, **kwargs):
        raise RuntimeError("network down")

    monkeypatch.setattr(models, "snapshot_download", fake)

    with pytest.raises(ModelError, match="network down"):
        ModelCatalog([tmp_path / "hub"]).download("org/name")


# remove -------------------------------------------------------------------


def test_remove_requires_confirmation(tmp_path):
    root = tmp_path / "hub"
    path = write_cached(root, "org/name")

    with pytest.raises(ModelError, match="confirm"):
        ModelCatalog([root]).remove("org/name")

    assert path.is_dir()


def test_remove_rejects_in_use_model(tmp_path):
    root = tmp_path / "hub"
    path = write_cached(root, "org/name")

    with pytest.raises(ModelError, match="in use"):
        ModelCatalog([root]).remove("org/name", confirm=True, in_use=["org/name"])

    assert path.is_dir()


def test_remove_deletes_cached_entry(tmp_path):
    root = tmp_path / "hub"
    path = write_cached(root, "org/name")

    ModelCatalog([root]).remove("org/name", confirm=True)

    assert not path.exists()


def test_remove_deletes_partial_entry(tmp_path):
    root = tmp_path / "hub"
    path = write_partial(root, "org/name")

    ModelCatalog([root]).remove("org/name", confirm=True)

    assert not path.exists()


def test_remove_rejects_missing_entry(tmp_path):
    with pytest.raises(ModelError, match="not cached"):
        ModelCatalog([tmp_path / "hub"]).remove("org/name", confirm=True)


def test_remove_requires_root_when_entry_is_in_multiple_roots(tmp_path):
    root_a = tmp_path / "a"
    root_b = tmp_path / "b"
    write_cached(root_a, "org/name")
    write_cached(root_b, "org/name")
    catalog = ModelCatalog([root_a, root_b])

    with pytest.raises(ModelError, match="multiple"):
        catalog.remove("org/name", confirm=True)

    catalog.remove("org/name", root=root_a, confirm=True)

    assert not cache_dir(root_a, "org/name").exists()
    assert cache_dir(root_b, "org/name").is_dir()


def test_remove_rejects_unconfigured_root(tmp_path):
    with pytest.raises(ModelError, match="not configured"):
        ModelCatalog([tmp_path / "hub"]).remove("org/name", root=tmp_path / "other", confirm=True)


def test_disk_usage_sums_roots(tmp_path):
    root_a = tmp_path / "a"
    root_b = tmp_path / "b"
    write_cached(root_a, "org/a", b"1234")
    write_partial(root_b, "org/b", b"12")

    assert ModelCatalog([root_a, root_b]).disk_usage() == 6


# job manager --------------------------------------------------------------


def quick_download(cache_root: Path):
    def fake(repo_id, *, cache_dir, tqdm_class, **kwargs):
        bar = tqdm_class(desc="model.safetensors", total=100, unit="B", file=io.StringIO())
        bar.update(100)
        target = Path(cache_dir) / f"models--{repo_id.replace('/', '--')}" / "snapshots" / "rev"
        target.mkdir(parents=True)
        (target / "config.json").write_text("{}")
        return str(target)

    return fake


def test_job_manager_runs_download_and_reports_success(tmp_path, monkeypatch):
    root = tmp_path / "hub"
    monkeypatch.setattr(models, "snapshot_download", quick_download(root))
    manager = ModelJobManager(ModelCatalog([root]))

    job_id = manager.start("org/name")
    status = manager.wait(job_id, timeout=2)

    assert isinstance(status, ModelJobStatus)
    assert status.id == job_id
    assert status.repo == "org/name"
    assert status.root == root
    assert status.state == "succeeded"
    assert status.error is None
    assert status.file == "model.safetensors"
    assert status.bytes_done == 100
    assert status.bytes_total == 100
    assert status.started_at is not None
    assert status.finished_at is not None
    assert status.path == str(cache_dir(root, "org/name") / "snapshots" / "rev")
    assert manager.active() == []


def test_job_manager_reports_live_progress(tmp_path, monkeypatch):
    root = tmp_path / "hub"
    started = threading.Event()
    release = threading.Event()

    def fake(repo_id, *, cache_dir, tqdm_class, **kwargs):
        bar = tqdm_class(desc="model.safetensors", total=100, unit="B", file=io.StringIO())
        bar.update(25)
        started.set()
        release.wait(2)
        bar.update(75)
        return str(cache_dir)

    monkeypatch.setattr(models, "snapshot_download", fake)
    manager = ModelJobManager(ModelCatalog([root]))

    job_id = manager.start("org/name")
    assert started.wait(1)

    live = manager.status(job_id)
    assert live.state == "running"
    assert live.bytes_done == 25
    assert live.bytes_total == 100

    release.set()
    final = manager.wait(job_id, timeout=2)
    assert final.state == "succeeded"
    assert final.bytes_done == 100


def test_job_manager_reports_failure(tmp_path, monkeypatch):
    root = tmp_path / "hub"

    def fake(repo_id, *, cache_dir, **kwargs):
        raise RuntimeError("network down")

    monkeypatch.setattr(models, "snapshot_download", fake)
    manager = ModelJobManager(ModelCatalog([root]))

    status = manager.wait(manager.start("org/name"), timeout=2)

    assert status.state == "failed"
    assert "network down" in status.error


def test_job_manager_cancels_running_download(tmp_path, monkeypatch):
    root = tmp_path / "hub"
    started = threading.Event()

    def fake(repo_id, *, cache_dir, tqdm_class, **kwargs):
        started.set()
        bar = tqdm_class(desc="model.safetensors", total=100, unit="B", file=io.StringIO())
        while True:
            bar.update(1)
            time.sleep(0.01)

    monkeypatch.setattr(models, "snapshot_download", fake)
    manager = ModelJobManager(ModelCatalog([root]))

    job_id = manager.start("org/name")
    assert started.wait(1)
    assert manager.cancel(job_id) is True

    status = manager.wait(job_id, timeout=2)

    assert status.state == "cancelled"
    assert status.error == "cancelled"
    assert manager.cancel(job_id) is False


def test_job_manager_rejects_duplicate_active_repo(tmp_path, monkeypatch):
    root = tmp_path / "hub"
    release = threading.Event()

    def fake(repo_id, *, cache_dir, **kwargs):
        release.wait(2)
        return str(cache_dir)

    monkeypatch.setattr(models, "snapshot_download", fake)
    manager = ModelJobManager(ModelCatalog([root]))

    first = manager.start("org/name")
    with pytest.raises(ModelError, match="already running"):
        manager.start("org/name")

    release.set()
    manager.wait(first, timeout=2)

    second = manager.start("org/name")
    release.set()
    assert manager.wait(second, timeout=2).state == "succeeded"


def test_job_manager_rejects_unconfigured_root(tmp_path):
    manager = ModelJobManager(ModelCatalog([tmp_path / "hub"]))

    with pytest.raises(ModelError, match="not configured"):
        manager.start("org/name", root=tmp_path / "other")


def test_job_manager_unknown_job_raises(tmp_path):
    manager = ModelJobManager(ModelCatalog([tmp_path / "hub"]))

    with pytest.raises(ModelError, match="unknown job"):
        manager.status("missing")
    with pytest.raises(ModelError, match="unknown job"):
        manager.cancel("missing")
    with pytest.raises(ModelError, match="unknown job"):
        manager.wait("missing")


def test_job_manager_lists_jobs(tmp_path, monkeypatch):
    root = tmp_path / "hub"
    monkeypatch.setattr(models, "snapshot_download", quick_download(root))
    manager = ModelJobManager(ModelCatalog([root]))

    first = manager.start("org/first")
    second = manager.start("org/second")
    manager.wait(first, timeout=2)
    manager.wait(second, timeout=2)

    statuses = manager.list()

    assert {status.repo for status in statuses} == {"org/first", "org/second"}
    assert all(status.state == "succeeded" for status in statuses)


def test_job_manager_refuses_removal_while_downloading(tmp_path, monkeypatch):
    root = tmp_path / "hub"
    started = threading.Event()
    release = threading.Event()

    def fake(repo_id, *, cache_dir, **kwargs):
        started.set()
        release.wait(2)
        return str(cache_dir)

    monkeypatch.setattr(models, "snapshot_download", fake)
    manager = ModelJobManager(ModelCatalog([root]))

    job_id = manager.start("org/name")
    assert started.wait(1)

    with pytest.raises(ModelError, match="download in progress"):
        manager.remove("org/name", confirm=True)

    manager.cancel(job_id)
    manager.wait(job_id, timeout=2)
