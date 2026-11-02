"""SQLite state store for the fleet controller.

The store keeps the node registry, deployments and their roles, jobs and job
events, node/inference metric series, and settings. It uses WAL mode, foreign
keys, and a partial unique index that allows at most one primary deployment.
Retention pruning and incremental vacuum bound file growth.
"""

from __future__ import annotations

import json
import sqlite3
import threading
import time
from collections.abc import Iterable, Sequence
from pathlib import Path
from typing import Any

SCHEMA_VERSION = 1
_UNSET = object()


class DatabaseError(RuntimeError):
    """Raised when the database cannot be opened or migrated."""


_SCHEMA_V1 = """
CREATE TABLE nodes (
    id TEXT PRIMARY KEY,
    display_name TEXT NOT NULL,
    lan_ip TEXT NOT NULL,
    fabric_ip TEXT NOT NULL,
    agent_url TEXT NOT NULL,
    enabled INTEGER NOT NULL DEFAULT 1,
    created_at REAL NOT NULL,
    updated_at REAL NOT NULL,
    last_seen_at REAL,
    info_json TEXT
);

CREATE TABLE deployments (
    id TEXT PRIMARY KEY,
    name TEXT NOT NULL UNIQUE,
    recipe_name TEXT NOT NULL,
    mode TEXT NOT NULL,
    status TEXT NOT NULL,
    is_primary INTEGER NOT NULL DEFAULT 0,
    created_at REAL NOT NULL,
    updated_at REAL NOT NULL,
    loaded_at REAL,
    error TEXT,
    recipe_json TEXT NOT NULL
);

CREATE UNIQUE INDEX idx_deployments_primary ON deployments(is_primary) WHERE is_primary = 1;

CREATE TABLE deployment_roles (
    deployment_id TEXT NOT NULL REFERENCES deployments(id) ON DELETE CASCADE,
    role_name TEXT NOT NULL,
    node_id TEXT NOT NULL REFERENCES nodes(id),
    "class" TEXT NOT NULL,
    start_order INTEGER NOT NULL,
    config_hash TEXT,
    status TEXT NOT NULL,
    container_id TEXT,
    PRIMARY KEY (deployment_id, role_name)
);

CREATE TABLE jobs (
    id TEXT PRIMARY KEY,
    kind TEXT NOT NULL,
    status TEXT NOT NULL,
    deployment_id TEXT REFERENCES deployments(id) ON DELETE SET NULL,
    payload_json TEXT,
    result_json TEXT,
    error TEXT,
    created_at REAL NOT NULL,
    started_at REAL,
    finished_at REAL
);

CREATE TABLE job_events (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    job_id TEXT NOT NULL REFERENCES jobs(id) ON DELETE CASCADE,
    ts REAL NOT NULL,
    level TEXT NOT NULL,
    phase TEXT,
    message TEXT NOT NULL
);

CREATE INDEX idx_job_events_job ON job_events(job_id, id);

CREATE TABLE node_metrics (
    node_id TEXT NOT NULL REFERENCES nodes(id) ON DELETE CASCADE,
    metric TEXT NOT NULL,
    ts REAL NOT NULL,
    value REAL NOT NULL
);

CREATE INDEX idx_node_metrics ON node_metrics(node_id, metric, ts);

CREATE TABLE inference_samples (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    deployment_id TEXT NOT NULL REFERENCES deployments(id) ON DELETE CASCADE,
    ts REAL NOT NULL,
    requests INTEGER NOT NULL DEFAULT 0,
    errors INTEGER NOT NULL DEFAULT 0,
    latency_ms REAL,
    ttft_ms REAL,
    prompt_tokens INTEGER NOT NULL DEFAULT 0,
    completion_tokens INTEGER NOT NULL DEFAULT 0
);

CREATE INDEX idx_inference_samples ON inference_samples(deployment_id, ts);

CREATE TABLE settings (
    key TEXT PRIMARY KEY,
    value TEXT NOT NULL
);
"""


def _json_encode(value: Any) -> str | None:
    if value is None:
        return None
    return json.dumps(value, sort_keys=True)


def _json_decode(value: str | None) -> Any:
    if value is None:
        return None
    return json.loads(value)


def _validate_limit(limit: int) -> int:
    if limit < 0:
        raise ValueError("limit must not be negative")
    return limit


def _split_statements(script: str) -> list[str]:
    """Split a SQL script without breaking on semicolons inside literals/triggers."""

    statements: list[str] = []
    buffer = ""
    for character in script:
        buffer += character
        if sqlite3.complete_statement(buffer):
            statement = buffer.strip()
            if statement:
                statements.append(statement)
            buffer = ""
    if buffer.strip():
        statements.append(buffer.strip())
    return statements


class Database:
    """A thread-safe SQLite store shared by controller components."""

    def __init__(self, path: str | Path):
        self.path = str(path)
        self._lock = threading.RLock()

        if self.path != ":memory:":
            try:
                Path(self.path).parent.mkdir(parents=True, exist_ok=True)
            except OSError as exc:
                raise DatabaseError(f"database open failed: {exc}") from exc

        try:
            self._conn = sqlite3.connect(self.path, check_same_thread=False)
        except sqlite3.Error as exc:
            raise DatabaseError(f"database open failed: {exc}") from exc

        try:
            self._conn.row_factory = sqlite3.Row
            self._conn.execute("PRAGMA busy_timeout = 5000")
            self._conn.execute("PRAGMA foreign_keys = ON")

            version = int(self._conn.execute("PRAGMA user_version").fetchone()[0])
            if version > SCHEMA_VERSION:
                raise DatabaseError(
                    f"database schema version {version} is newer than supported {SCHEMA_VERSION}"
                )

            if self.auto_vacuum_mode != 2:
                self._conn.execute("PRAGMA auto_vacuum = INCREMENTAL")
                self._conn.execute("VACUUM")

            if self.path != ":memory:":
                self._conn.execute("PRAGMA journal_mode = WAL")

            self._migrate()
        except DatabaseError:
            self._conn.close()
            raise
        except sqlite3.Error as exc:
            self._conn.close()
            raise DatabaseError(f"database open failed: {exc}") from exc

    def _migrate(self) -> None:
        with self._lock:
            try:
                self._conn.execute("BEGIN IMMEDIATE")
                version = self._conn.execute("PRAGMA user_version").fetchone()[0]
                if version > SCHEMA_VERSION:
                    raise DatabaseError(
                        f"database schema version {version} is newer than supported "
                        f"{SCHEMA_VERSION}"
                    )
                if version < 1:
                    for statement in _split_statements(_SCHEMA_V1):
                        self._conn.execute(statement)
                    self._conn.execute(f"PRAGMA user_version = {SCHEMA_VERSION}")
                self._conn.commit()
            except DatabaseError:
                self._conn.rollback()
                raise
            except sqlite3.Error as exc:
                self._conn.rollback()
                raise DatabaseError(f"database migration failed: {exc}") from exc

    @property
    def schema_version(self) -> int:
        with self._lock:
            return int(self._conn.execute("PRAGMA user_version").fetchone()[0])

    @property
    def auto_vacuum_mode(self) -> int:
        with self._lock:
            return int(self._conn.execute("PRAGMA auto_vacuum").fetchone()[0])

    @property
    def freelist_count(self) -> int:
        with self._lock:
            return int(self._conn.execute("PRAGMA freelist_count").fetchone()[0])

    # Nodes -----------------------------------------------------------------

    def upsert_node(
        self,
        node_id: str,
        *,
        display_name: str,
        lan_ip: str,
        fabric_ip: str,
        agent_url: str,
        enabled: bool = True,
        info: Any = _UNSET,
        now: float | None = None,
    ) -> None:
        now = time.time() if now is None else now
        with self._lock, self._conn:
            if info is _UNSET:
                row = self._conn.execute(
                    "SELECT info_json FROM nodes WHERE id = ?", (node_id,)
                ).fetchone()
                info_json = row["info_json"] if row else None
            else:
                info_json = _json_encode(info)
            self._conn.execute(
                """
                INSERT INTO nodes (
                    id, display_name, lan_ip, fabric_ip, agent_url,
                    enabled, created_at, updated_at, info_json
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(id) DO UPDATE SET
                    display_name = excluded.display_name,
                    lan_ip = excluded.lan_ip,
                    fabric_ip = excluded.fabric_ip,
                    agent_url = excluded.agent_url,
                    enabled = excluded.enabled,
                    updated_at = excluded.updated_at,
                    info_json = excluded.info_json
                """,
                (
                    node_id,
                    display_name,
                    lan_ip,
                    fabric_ip,
                    agent_url,
                    int(enabled),
                    now,
                    now,
                    info_json,
                ),
            )

    def touch_node(self, node_id: str, now: float | None = None) -> None:
        now = time.time() if now is None else now
        with self._lock, self._conn:
            self._conn.execute(
                "UPDATE nodes SET last_seen_at = ?, updated_at = ? WHERE id = ?",
                (now, now, node_id),
            )

    def get_node(self, node_id: str) -> dict[str, Any] | None:
        with self._lock:
            row = self._conn.execute("SELECT * FROM nodes WHERE id = ?", (node_id,)).fetchone()
        return None if row is None else self._node_row(row)

    def list_nodes(self) -> list[dict[str, Any]]:
        with self._lock:
            rows = self._conn.execute("SELECT * FROM nodes ORDER BY id").fetchall()
        return [self._node_row(row) for row in rows]

    def delete_node(self, node_id: str) -> None:
        with self._lock, self._conn:
            references = self._conn.execute(
                "SELECT COUNT(*) FROM deployment_roles WHERE node_id = ?", (node_id,)
            ).fetchone()[0]
            if references:
                raise DatabaseError(f"node {node_id} is referenced by deployment roles")
            self._conn.execute("DELETE FROM nodes WHERE id = ?", (node_id,))

    @staticmethod
    def _node_row(row: sqlite3.Row) -> dict[str, Any]:
        data = dict(row)
        data["enabled"] = bool(data["enabled"])
        data["info"] = _json_decode(data.pop("info_json"))
        return data

    # Deployments -----------------------------------------------------------

    def create_deployment(
        self,
        deployment_id: str,
        name: str,
        recipe_name: str,
        mode: str,
        recipe: Any,
        status: str = "pending",
        now: float | None = None,
    ) -> None:
        now = time.time() if now is None else now
        with self._lock, self._conn:
            self._conn.execute(
                """
                INSERT INTO deployments (
                    id, name, recipe_name, mode, status, is_primary,
                    created_at, updated_at, recipe_json
                ) VALUES (?, ?, ?, ?, ?, 0, ?, ?, ?)
                """,
                (deployment_id, name, recipe_name, mode, status, now, now, _json_encode(recipe)),
            )

    def update_deployment(
        self,
        deployment_id: str,
        *,
        status: str | None = None,
        error: str | None | object = _UNSET,
        loaded_at: float | None = None,
        now: float | None = None,
    ) -> None:
        now = time.time() if now is None else now
        assignments = ["updated_at = ?"]
        values: list[Any] = [now]
        if status is not None:
            assignments.append("status = ?")
            values.append(status)
        if error is not _UNSET:
            assignments.append("error = ?")
            values.append(error)
        if loaded_at is not None:
            assignments.append("loaded_at = ?")
            values.append(loaded_at)
        values.append(deployment_id)

        with self._lock, self._conn:
            cursor = self._conn.execute(
                f"UPDATE deployments SET {', '.join(assignments)} WHERE id = ?",
                tuple(values),
            )
        if cursor.rowcount == 0:
            raise KeyError(deployment_id)

    def set_primary(self, deployment_id: str, now: float | None = None) -> None:
        now = time.time() if now is None else now
        with self._lock, self._conn:
            self._conn.execute(
                "UPDATE deployments SET is_primary = 0, updated_at = ? WHERE is_primary = 1",
                (now,),
            )
            cursor = self._conn.execute(
                "UPDATE deployments SET is_primary = 1, updated_at = ? WHERE id = ?",
                (now, deployment_id),
            )
            if cursor.rowcount == 0:
                raise KeyError(deployment_id)

    def get_deployment(self, deployment_id: str) -> dict[str, Any] | None:
        with self._lock:
            row = self._conn.execute(
                "SELECT * FROM deployments WHERE id = ?", (deployment_id,)
            ).fetchone()
        return None if row is None else self._deployment_row(row)

    def get_deployment_by_name(self, name: str) -> dict[str, Any] | None:
        with self._lock:
            row = self._conn.execute("SELECT * FROM deployments WHERE name = ?", (name,)).fetchone()
        return None if row is None else self._deployment_row(row)

    def list_deployments(self) -> list[dict[str, Any]]:
        with self._lock:
            rows = self._conn.execute("SELECT * FROM deployments ORDER BY id").fetchall()
        return [self._deployment_row(row) for row in rows]

    def delete_deployment(self, deployment_id: str) -> None:
        with self._lock, self._conn:
            self._conn.execute("DELETE FROM deployments WHERE id = ?", (deployment_id,))

    @staticmethod
    def _deployment_row(row: sqlite3.Row) -> dict[str, Any]:
        data = dict(row)
        data["is_primary"] = bool(data["is_primary"])
        data["recipe"] = _json_decode(data.pop("recipe_json"))
        return data

    # Roles -----------------------------------------------------------------

    def upsert_role(
        self,
        deployment_id: str,
        role_name: str,
        node_id: str,
        role_class: str,
        start_order: int,
        *,
        config_hash: str | None = None,
        status: str | None = None,
        container_id: str | None = None,
    ) -> None:
        with self._lock, self._conn:
            existing = self._conn.execute(
                """
                SELECT node_id, status, config_hash, container_id
                FROM deployment_roles
                WHERE deployment_id = ? AND role_name = ?
                """,
                (deployment_id, role_name),
            ).fetchone()

            if existing is None:
                self._conn.execute(
                    """
                    INSERT INTO deployment_roles (
                        deployment_id, role_name, node_id, "class", start_order,
                        config_hash, status, container_id
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        deployment_id,
                        role_name,
                        node_id,
                        role_class,
                        start_order,
                        config_hash,
                        status or "pending",
                        container_id,
                    ),
                )
                return

            if existing["node_id"] != node_id:
                # A reassigned role starts fresh on its new node.
                new_status = status or "pending"
                new_hash = config_hash
                new_container = container_id
            else:
                new_status = status if status is not None else existing["status"]
                new_hash = config_hash if config_hash is not None else existing["config_hash"]
                new_container = (
                    container_id if container_id is not None else existing["container_id"]
                )

            self._conn.execute(
                """
                UPDATE deployment_roles SET
                    node_id = ?, "class" = ?, start_order = ?,
                    config_hash = ?, status = ?, container_id = ?
                WHERE deployment_id = ? AND role_name = ?
                """,
                (
                    node_id,
                    role_class,
                    start_order,
                    new_hash,
                    new_status,
                    new_container,
                    deployment_id,
                    role_name,
                ),
            )

    def list_roles(self, deployment_id: str) -> list[dict[str, Any]]:
        with self._lock:
            rows = self._conn.execute(
                """
                SELECT * FROM deployment_roles
                WHERE deployment_id = ?
                ORDER BY start_order, role_name
                """,
                (deployment_id,),
            ).fetchall()
        return [dict(row) for row in rows]

    # Jobs ------------------------------------------------------------------

    def create_job(
        self,
        job_id: str,
        kind: str,
        *,
        deployment_id: str | None = None,
        payload: Any = None,
        status: str = "pending",
        now: float | None = None,
    ) -> None:
        now = time.time() if now is None else now
        with self._lock, self._conn:
            self._conn.execute(
                """
                INSERT INTO jobs (
                    id, kind, status, deployment_id, payload_json, created_at
                ) VALUES (?, ?, ?, ?, ?, ?)
                """,
                (job_id, kind, status, deployment_id, _json_encode(payload), now),
            )

    def update_job(
        self,
        job_id: str,
        *,
        status: str | None = None,
        result: Any = _UNSET,
        error: str | None | object = _UNSET,
        started_at: float | None = None,
        finished_at: float | None = None,
    ) -> None:
        assignments: list[str] = []
        values: list[Any] = []
        if status is not None:
            assignments.append("status = ?")
            values.append(status)
        if result is not _UNSET:
            assignments.append("result_json = ?")
            values.append(_json_encode(result))
        if error is not _UNSET:
            assignments.append("error = ?")
            values.append(error)
        if started_at is not None:
            assignments.append("started_at = ?")
            values.append(started_at)
        if finished_at is not None:
            assignments.append("finished_at = ?")
            values.append(finished_at)
        if not assignments:
            raise ValueError("no job fields to update")
        values.append(job_id)

        with self._lock, self._conn:
            cursor = self._conn.execute(
                f"UPDATE jobs SET {', '.join(assignments)} WHERE id = ?", tuple(values)
            )
        if cursor.rowcount == 0:
            raise KeyError(job_id)

    def get_job(self, job_id: str) -> dict[str, Any] | None:
        with self._lock:
            row = self._conn.execute("SELECT * FROM jobs WHERE id = ?", (job_id,)).fetchone()
        return None if row is None else self._job_row(row)

    def list_jobs(self, limit: int = 100) -> list[dict[str, Any]]:
        limit = _validate_limit(limit)
        with self._lock:
            rows = self._conn.execute(
                "SELECT * FROM jobs ORDER BY created_at DESC, id DESC LIMIT ?", (limit,)
            ).fetchall()
        return [self._job_row(row) for row in rows]

    @staticmethod
    def _job_row(row: sqlite3.Row) -> dict[str, Any]:
        data = dict(row)
        data["payload"] = _json_decode(data.pop("payload_json"))
        data["result"] = _json_decode(data.pop("result_json"))
        return data

    def append_job_event(
        self,
        job_id: str,
        message: str,
        *,
        level: str = "info",
        phase: str | None = None,
        ts: float | None = None,
    ) -> int:
        ts = time.time() if ts is None else ts
        with self._lock, self._conn:
            cursor = self._conn.execute(
                """
                INSERT INTO job_events (job_id, ts, level, phase, message)
                VALUES (?, ?, ?, ?, ?)
                """,
                (job_id, ts, level, phase, message),
            )
        return int(cursor.lastrowid)

    def list_job_events(
        self, job_id: str, *, after_id: int = 0, limit: int = 500
    ) -> list[dict[str, Any]]:
        limit = _validate_limit(limit)
        with self._lock:
            rows = self._conn.execute(
                """
                SELECT * FROM job_events
                WHERE job_id = ? AND id > ?
                ORDER BY id
                LIMIT ?
                """,
                (job_id, after_id, limit),
            ).fetchall()
        return [dict(row) for row in rows]

    def mark_interrupted_jobs_failed(self, now: float | None = None) -> int:
        now = time.time() if now is None else now
        with self._lock, self._conn:
            cursor = self._conn.execute(
                """
                UPDATE jobs
                SET status = 'failed', error = 'interrupted by restart', finished_at = ?
                WHERE status IN ('pending', 'running')
                """,
                (now,),
            )
        return int(cursor.rowcount)

    # Metrics ---------------------------------------------------------------

    def record_metric(
        self, node_id: str, metric: str, value: float, ts: float | None = None
    ) -> None:
        ts = time.time() if ts is None else ts
        with self._lock, self._conn:
            self._conn.execute(
                "INSERT INTO node_metrics (node_id, metric, ts, value) VALUES (?, ?, ?, ?)",
                (node_id, metric, ts, value),
            )

    def record_metrics(self, rows: Iterable[Sequence[Any]]) -> None:
        now = time.time()
        payload: list[tuple[Any, Any, Any, Any]] = []
        for row in rows:
            if len(row) not in (3, 4):
                raise ValueError("metric rows must be (node_id, metric, value[, ts])")
            node_id, metric, value = row[0], row[1], row[2]
            ts = now if len(row) == 3 or row[3] is None else row[3]
            payload.append((node_id, metric, ts, value))
        if not payload:
            return
        with self._lock, self._conn:
            self._conn.executemany(
                "INSERT INTO node_metrics (node_id, metric, ts, value) VALUES (?, ?, ?, ?)",
                payload,
            )

    def latest_metric(self, node_id: str, metric: str) -> dict[str, Any] | None:
        with self._lock:
            row = self._conn.execute(
                """
                SELECT * FROM node_metrics
                WHERE node_id = ? AND metric = ?
                ORDER BY ts DESC, rowid DESC
                LIMIT 1
                """,
                (node_id, metric),
            ).fetchone()
        return None if row is None else dict(row)

    def metric_series(
        self,
        node_id: str,
        metric: str,
        *,
        since: float | None = None,
        until: float | None = None,
        limit: int = 1000,
    ) -> list[dict[str, Any]]:
        limit = _validate_limit(limit)
        clauses = ["node_id = ?", "metric = ?"]
        values: list[Any] = [node_id, metric]
        if since is not None:
            clauses.append("ts >= ?")
            values.append(since)
        if until is not None:
            clauses.append("ts <= ?")
            values.append(until)
        values.append(limit)

        with self._lock:
            rows = self._conn.execute(
                f"""
                SELECT * FROM (
                    SELECT rowid AS _rowid, * FROM node_metrics
                    WHERE {" AND ".join(clauses)}
                    ORDER BY ts DESC, rowid DESC
                    LIMIT ?
                )
                ORDER BY ts ASC, _rowid ASC
                """,
                tuple(values),
            ).fetchall()
        return [{key: value for key, value in dict(row).items() if key != "_rowid"} for row in rows]

    # Inference -------------------------------------------------------------

    def record_inference_sample(
        self,
        deployment_id: str,
        *,
        requests: int = 0,
        errors: int = 0,
        latency_ms: float | None = None,
        ttft_ms: float | None = None,
        prompt_tokens: int = 0,
        completion_tokens: int = 0,
        ts: float | None = None,
    ) -> None:
        ts = time.time() if ts is None else ts
        with self._lock, self._conn:
            self._conn.execute(
                """
                INSERT INTO inference_samples (
                    deployment_id, ts, requests, errors, latency_ms, ttft_ms,
                    prompt_tokens, completion_tokens
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    deployment_id,
                    ts,
                    requests,
                    errors,
                    latency_ms,
                    ttft_ms,
                    prompt_tokens,
                    completion_tokens,
                ),
            )

    def inference_series(
        self,
        deployment_id: str,
        *,
        since: float | None = None,
        until: float | None = None,
        limit: int = 1000,
    ) -> list[dict[str, Any]]:
        limit = _validate_limit(limit)
        clauses = ["deployment_id = ?"]
        values: list[Any] = [deployment_id]
        if since is not None:
            clauses.append("ts >= ?")
            values.append(since)
        if until is not None:
            clauses.append("ts <= ?")
            values.append(until)
        values.append(limit)

        with self._lock:
            rows = self._conn.execute(
                f"""
                SELECT * FROM (
                    SELECT * FROM inference_samples
                    WHERE {" AND ".join(clauses)}
                    ORDER BY ts DESC, id DESC
                    LIMIT ?
                )
                ORDER BY ts ASC, id ASC
                """,
                tuple(values),
            ).fetchall()
        return [dict(row) for row in rows]

    # Settings --------------------------------------------------------------

    def get_setting(self, key: str, default: str | None = None) -> str | None:
        with self._lock:
            row = self._conn.execute("SELECT value FROM settings WHERE key = ?", (key,)).fetchone()
        return default if row is None else row["value"]

    def set_setting(self, key: str, value: str) -> None:
        with self._lock, self._conn:
            self._conn.execute(
                """
                INSERT INTO settings (key, value) VALUES (?, ?)
                ON CONFLICT(key) DO UPDATE SET value = excluded.value
                """,
                (key, value),
            )

    def delete_setting(self, key: str) -> None:
        with self._lock, self._conn:
            self._conn.execute("DELETE FROM settings WHERE key = ?", (key,))

    def list_settings(self) -> dict[str, str]:
        with self._lock:
            rows = self._conn.execute("SELECT key, value FROM settings ORDER BY key").fetchall()
        return {row["key"]: row["value"] for row in rows}

    # Maintenance -----------------------------------------------------------

    def prune(self, retention_seconds: float, now: float | None = None) -> dict[str, int]:
        if retention_seconds < 0:
            raise ValueError("retention_seconds must not be negative")
        now = time.time() if now is None else now
        cutoff = now - retention_seconds
        with self._lock, self._conn:
            metrics = self._conn.execute(
                "DELETE FROM node_metrics WHERE ts < ?", (cutoff,)
            ).rowcount
            inference = self._conn.execute(
                "DELETE FROM inference_samples WHERE ts < ?", (cutoff,)
            ).rowcount
            events = self._conn.execute("DELETE FROM job_events WHERE ts < ?", (cutoff,)).rowcount
        return {
            "metrics": int(metrics),
            "inference": int(inference),
            "job_events": int(events),
        }

    def incremental_vacuum(self) -> None:
        with self._lock:
            mode = self._conn.execute("PRAGMA auto_vacuum").fetchone()[0]
            if mode != 2:
                raise DatabaseError("incremental vacuum requires auto_vacuum=INCREMENTAL")
            self._conn.execute("PRAGMA incremental_vacuum")
            self._conn.commit()

    def vacuum(self) -> None:
        with self._lock:
            self._conn.commit()
            self._conn.execute("VACUUM")

    def close(self) -> None:
        with self._lock:
            self._conn.close()

    def __enter__(self) -> Database:
        return self

    def __exit__(self, *exc_info: object) -> None:
        self.close()
