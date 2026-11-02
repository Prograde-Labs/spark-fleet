"""Model cache catalog, Hugging Face downloads, removal, and progress jobs.

Each configured cache root is a Hugging Face hub cache directory containing
``models--<org>--<name>`` entries. Scanning reports per-root entries with their
cached/partial state, size, and any error; duplicate entries across roots are
kept separate so the controller can de-duplicate the fleet view. Downloads go
through ``snapshot_download`` into a configured root and report progress through
a tqdm subclass; removal requires explicit confirmation and refuses models the
controller reports as in use. A small job manager runs downloads on daemon
threads so the agent API can poll progress and cancel.
"""

from __future__ import annotations

import logging
import re
import shutil
import threading
import time
import uuid
from collections.abc import Iterable, Sequence
from dataclasses import dataclass, field
from pathlib import Path

from huggingface_hub import snapshot_download
from tqdm import tqdm

from spark_fleet.shared.recipe import REPO_PATTERN

_LOGGER = logging.getLogger(__name__)
_REPO_RE = re.compile(REPO_PATTERN)
_CACHE_PREFIX = "models--"
_ACTIVE_STATES = frozenset({"pending", "running"})


class ModelError(RuntimeError):
    """Raised when a cache entry, download, or removal is invalid."""


class DownloadCancelled(ModelError):
    """Raised when a model download is cancelled by its owner."""


@dataclass(frozen=True)
class CacheRoot:
    path: Path
    exists: bool
    error: str | None


@dataclass(frozen=True)
class ModelEntry:
    repo: str
    root: Path
    path: Path
    cached: bool
    partial: bool
    size_bytes: int
    error: str | None


@dataclass(frozen=True)
class Catalog:
    roots: tuple[CacheRoot, ...]
    models: tuple[ModelEntry, ...]


@dataclass(frozen=True)
class ModelJobStatus:
    id: str
    repo: str
    root: Path
    state: str
    file: str | None
    bytes_done: int
    bytes_total: int | None
    error: str | None
    path: str | None
    started_at: float | None
    finished_at: float | None


@dataclass
class _Job:
    id: str
    repo: str
    root: Path
    revision: str | None
    allow_patterns: list[str] | None
    state: str = "pending"
    file: str | None = None
    bytes_done: int = 0
    bytes_total: int | None = None
    error: str | None = None
    path: str | None = None
    started_at: float | None = None
    finished_at: float | None = None
    cancel_event: threading.Event = field(default_factory=threading.Event)
    done_event: threading.Event = field(default_factory=threading.Event)
    thread: threading.Thread | None = None


class ModelCatalog:
    """Scan, download, and remove models across the configured cache roots."""

    def __init__(self, roots: Iterable[Path]):
        unique: list[Path] = []
        for root in roots:
            path = Path(root)
            if not path.is_absolute():
                raise ModelError(f"cache roots must be absolute paths: {path!r}")
            if path not in unique:
                unique.append(path)
        if not unique:
            raise ModelError("at least one cache root is required")
        self._roots = tuple(unique)

    @property
    def roots(self) -> tuple[Path, ...]:
        return self._roots

    def scan(self) -> Catalog:
        roots: list[CacheRoot] = []
        models: list[ModelEntry] = []
        for root in self._roots:
            status, entries = _scan_root(root)
            roots.append(status)
            models.extend(entries)
        models.sort(key=lambda entry: (entry.repo, str(entry.root)))
        return Catalog(tuple(roots), tuple(models))

    def find(self, repo: str) -> list[ModelEntry]:
        _validate_repo(repo)
        return [entry for entry in self.scan().models if entry.repo == repo]

    def is_cached(self, repo: str) -> bool:
        return any(entry.cached for entry in self.find(repo))

    def resolve_root(
        self, root: Path | None = None, *, repo: str | None = None, prefer_cached: bool = False
    ) -> Path:
        if root is None:
            if prefer_cached and repo is not None:
                for candidate in self._roots:
                    if self._cache_dir(candidate, repo).is_dir():
                        return candidate
            return self._roots[0]
        path = Path(root)
        if path not in self._roots:
            raise ModelError(f"cache root {path} is not configured")
        return path

    def download(
        self,
        repo: str,
        *,
        root: Path | None = None,
        revision: str | None = None,
        allow_patterns: list[str] | None = None,
        on_progress=None,
        cancel_event: threading.Event | None = None,
    ) -> Path:
        _validate_repo(repo)
        target_root = self.resolve_root(root, repo=repo, prefer_cached=True)
        if cancel_event is not None and cancel_event.is_set():
            raise DownloadCancelled(f"download of {repo} cancelled")

        progress_class = _progress_class(repo, on_progress, cancel_event)
        try:
            path = snapshot_download(
                repo_id=repo,
                cache_dir=str(target_root),
                revision=revision,
                allow_patterns=allow_patterns,
                tqdm_class=progress_class,
            )
        except ModelError:
            raise
        except Exception as exc:
            raise ModelError(f"download of {repo} failed: {exc}") from exc
        return Path(path)

    def remove(
        self,
        repo: str,
        *,
        root: Path | None = None,
        confirm: bool = False,
        in_use: Sequence[str] = (),
    ) -> None:
        _validate_repo(repo)
        if not confirm:
            raise ModelError(f"removal of {repo} requires explicit confirmation")
        if repo in set(in_use):
            raise ModelError(f"model {repo} is in use by a loaded deployment")

        if root is None:
            candidates = [
                candidate for candidate in self._roots if self._cache_dir(candidate, repo).exists()
            ]
            if not candidates:
                raise ModelError(f"model {repo} is not cached")
            if len(candidates) > 1:
                raise ModelError(f"model {repo} is cached in multiple roots; specify the root")
            target_root = candidates[0]
        else:
            target_root = self.resolve_root(root)

        path = self._cache_dir(target_root, repo)
        if not path.exists():
            raise ModelError(f"model {repo} is not cached in {target_root}")
        if path.is_symlink() or not path.is_dir():
            raise ModelError(f"cache entry {path} is not a directory")
        try:
            shutil.rmtree(path)
        except OSError as exc:
            raise ModelError(f"could not remove {repo}: {exc}") from exc

    def disk_usage(self) -> int:
        seen: set[tuple[int, int]] = set()
        total = 0
        for root in self._roots:
            if not root.is_dir():
                continue
            try:
                total += _directory_size(root, seen)
            except OSError:
                continue
        return total

    def _cache_dir(self, root: Path, repo: str) -> Path:
        return root / f"{_CACHE_PREFIX}{repo.replace('/', '--')}"


class ModelJobManager:
    """Run model downloads on daemon threads and expose progress/cancellation."""

    def __init__(self, catalog: ModelCatalog):
        self._catalog = catalog
        self._lock = threading.Lock()
        self._jobs: dict[str, _Job] = {}

    def start(
        self,
        repo: str,
        *,
        root: Path | None = None,
        revision: str | None = None,
        allow_patterns: list[str] | None = None,
    ) -> str:
        _validate_repo(repo)
        target_root = self._catalog.resolve_root(root, repo=repo, prefer_cached=True)
        with self._lock:
            if any(job.repo == repo and job.state in _ACTIVE_STATES for job in self._jobs.values()):
                raise ModelError(f"download for {repo} is already running")
            job = _Job(
                id=uuid.uuid4().hex[:12],
                repo=repo,
                root=target_root,
                revision=revision,
                allow_patterns=allow_patterns,
            )
            self._jobs[job.id] = job
            thread = threading.Thread(
                target=self._run, args=(job,), name=f"model-download-{job.id}", daemon=True
            )
            job.thread = thread
            thread.start()
        return job.id

    def status(self, job_id: str) -> ModelJobStatus:
        with self._lock:
            return _snapshot(self._job(job_id))

    def list(self) -> list[ModelJobStatus]:
        with self._lock:
            jobs = sorted(self._jobs.values(), key=lambda job: (job.started_at or 0.0, job.id))
            return [_snapshot(job) for job in jobs]

    def active(self) -> list[str]:
        with self._lock:
            return sorted(job.id for job in self._jobs.values() if job.state in _ACTIVE_STATES)

    def cancel(self, job_id: str) -> bool:
        with self._lock:
            job = self._job(job_id)
            if job.state not in _ACTIVE_STATES:
                return False
            job.cancel_event.set()
            return True

    def wait(self, job_id: str, timeout: float | None = None) -> ModelJobStatus:
        with self._lock:
            job = self._job(job_id)
        job.done_event.wait(timeout)
        with self._lock:
            return _snapshot(job)

    def remove(
        self,
        repo: str,
        *,
        root: Path | None = None,
        confirm: bool = False,
        in_use: Sequence[str] = (),
    ) -> None:
        _validate_repo(repo)
        with self._lock:
            if any(job.repo == repo and job.state in _ACTIVE_STATES for job in self._jobs.values()):
                raise ModelError(f"download in progress for {repo}")
        self._catalog.remove(repo, root=root, confirm=confirm, in_use=in_use)

    def _job(self, job_id: str) -> _Job:
        job = self._jobs.get(job_id)
        if job is None:
            raise ModelError(f"unknown job: {job_id}")
        return job

    def _run(self, job: _Job) -> None:
        with self._lock:
            job.state = "running"
            job.started_at = time.time()

        def observer(progress: dict) -> None:
            with self._lock:
                job.file = progress.get("file")
                done = progress.get("done")
                if isinstance(done, (int, float)):
                    job.bytes_done = int(done)
                total = progress.get("total")
                job.bytes_total = int(total) if isinstance(total, (int, float)) else None

        try:
            if job.cancel_event.is_set():
                raise DownloadCancelled(f"download of {job.repo} cancelled")
            path = self._catalog.download(
                job.repo,
                root=job.root,
                revision=job.revision,
                allow_patterns=job.allow_patterns,
                on_progress=observer,
                cancel_event=job.cancel_event,
            )
        except DownloadCancelled:
            with self._lock:
                job.state = "cancelled"
                job.error = "cancelled"
        except Exception as exc:
            with self._lock:
                job.state = "failed"
                job.error = str(exc)
        else:
            with self._lock:
                if job.cancel_event.is_set():
                    job.state = "cancelled"
                    job.error = "cancelled"
                else:
                    job.state = "succeeded"
                    job.path = str(path)
        finally:
            with self._lock:
                job.finished_at = time.time()
                job.done_event.set()


def _snapshot(job: _Job) -> ModelJobStatus:
    return ModelJobStatus(
        id=job.id,
        repo=job.repo,
        root=job.root,
        state=job.state,
        file=job.file,
        bytes_done=job.bytes_done,
        bytes_total=job.bytes_total,
        error=job.error,
        path=job.path,
        started_at=job.started_at,
        finished_at=job.finished_at,
    )


def _progress_class(repo: str, on_progress, cancel_event: threading.Event | None):
    observer_failed = False

    class Progress(tqdm):
        def __init__(self, *args, **kwargs):
            self._fleet_desc = kwargs.get("desc")
            super().__init__(*args, **kwargs)

        def update(self, n=1):
            nonlocal observer_failed
            if cancel_event is not None and cancel_event.is_set():
                raise DownloadCancelled(f"download of {repo} cancelled")
            result = super().update(n)
            if on_progress is not None:
                desc = getattr(self, "desc", None) or self._fleet_desc
                try:
                    on_progress(
                        {
                            "repo": repo,
                            "file": desc if isinstance(desc, str) else None,
                            "done": self.n,
                            "total": self.total,
                        }
                    )
                except DownloadCancelled:
                    raise
                except Exception:
                    if not observer_failed:
                        _LOGGER.warning(
                            "progress observer failed for model %r", repo, exc_info=True
                        )
                        observer_failed = True
            return result

    return Progress


def _validate_repo(repo: str) -> None:
    if not isinstance(repo, str) or "--" in repo or not _REPO_RE.fullmatch(repo):
        raise ModelError(f"invalid repository id: {repo!r}")


def _scan_root(root: Path) -> tuple[CacheRoot, list[ModelEntry]]:
    if not root.is_dir():
        return CacheRoot(root, exists=False, error=None), []
    try:
        cache_dirs = _list_cache_dirs(root)
    except OSError as exc:
        return CacheRoot(root, exists=True, error=str(exc)), []

    entries: list[ModelEntry] = []
    for path in cache_dirs:
        repo = _repo_from_cache_dir(path.name)
        if repo is None:
            continue
        entries.append(_model_entry(root, path, repo))
    return CacheRoot(root, exists=True, error=None), entries


def _list_cache_dirs(root: Path) -> list[Path]:
    return sorted(
        (
            path
            for path in root.iterdir()
            if path.name.startswith(_CACHE_PREFIX) and not path.is_symlink() and path.is_dir()
        ),
        key=lambda path: path.name,
    )


def _repo_from_cache_dir(name: str) -> str | None:
    if not name.startswith(_CACHE_PREFIX):
        return None
    parts = name[len(_CACHE_PREFIX) :].split("--")
    if len(parts) != 2:
        return None
    repo = f"{parts[0]}/{parts[1]}"
    if "--" in repo or not _REPO_RE.fullmatch(repo):
        return None
    return repo


def _model_entry(root: Path, path: Path, repo: str) -> ModelEntry:
    try:
        cached = _snapshot_files(path)
        size_bytes = _directory_size(path)
    except OSError as exc:
        return ModelEntry(
            repo=repo,
            root=root,
            path=path,
            cached=False,
            partial=True,
            size_bytes=0,
            error=str(exc),
        )
    return ModelEntry(
        repo=repo,
        root=root,
        path=path,
        cached=cached,
        partial=not cached,
        size_bytes=size_bytes,
        error=None,
    )


def _snapshot_files(path: Path) -> bool:
    snapshots = path / "snapshots"
    if not snapshots.is_dir():
        return False
    return any(candidate.is_file() for candidate in snapshots.rglob("*"))


def _directory_size(directory: Path, seen: set[tuple[int, int]] | None = None) -> int:
    if not directory.is_dir():
        return 0
    seen = set() if seen is None else seen
    total = 0
    for path in directory.rglob("*"):
        if path.is_symlink() or not path.is_file():
            continue
        stat = path.stat()
        inode = (stat.st_dev, stat.st_ino)
        if inode in seen:
            continue
        seen.add(inode)
        total += stat.st_size
    return total
