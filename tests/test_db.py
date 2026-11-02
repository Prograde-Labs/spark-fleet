"""Tests for the SQLite state store."""

import sqlite3

import pytest

from spark_fleet.shared.db import (
    SCHEMA_VERSION,
    Database,
    DatabaseError,
    _split_statements,
)


@pytest.fixture
def db(tmp_path):
    database = Database(tmp_path / "fleet.sqlite")
    yield database
    database.close()


def add_node(db, node_id="spark-01"):
    db.upsert_node(
        node_id,
        display_name=f"Spark {node_id}",
        lan_ip="192.0.2.239",
        fabric_ip="198.51.100.10",
        agent_url=f"http://{node_id}:8092",
        info={"gpu": "gb10"},
    )


def test_schema_version(db):
    assert db.schema_version == SCHEMA_VERSION == 1


def test_creates_parent_directory(tmp_path):
    path = tmp_path / "nested" / "dir" / "fleet.sqlite"
    database = Database(path)
    try:
        assert path.exists()
    finally:
        database.close()


def test_node_upsert_and_get(db):
    add_node(db)
    node = db.get_node("spark-01")

    assert node["id"] == "spark-01"
    assert node["display_name"] == "Spark spark-01"
    assert node["lan_ip"] == "192.0.2.239"
    assert node["fabric_ip"] == "198.51.100.10"
    assert node["agent_url"] == "http://spark-01:8092"
    assert node["enabled"] is True
    assert node["info"] == {"gpu": "gb10"}


def test_node_upsert_preserves_created_at_and_info(db):
    add_node(db)
    created_at = db.get_node("spark-01")["created_at"]

    db.upsert_node(
        "spark-01",
        display_name="Renamed",
        lan_ip="192.0.2.240",
        fabric_ip="198.51.100.12",
        agent_url="http://spark-01:8092",
        enabled=False,
    )
    node = db.get_node("spark-01")

    assert node["display_name"] == "Renamed"
    assert node["enabled"] is False
    assert node["created_at"] == created_at
    assert node["info"] == {"gpu": "gb10"}


def test_touch_node(db):
    add_node(db)
    assert db.get_node("spark-01")["last_seen_at"] is None

    db.touch_node("spark-01", now=123.0)

    assert db.get_node("spark-01")["last_seen_at"] == 123.0


def test_list_and_delete_nodes(db):
    add_node(db, "spark-01")
    add_node(db, "spark-02")

    assert [node["id"] for node in db.list_nodes()] == ["spark-01", "spark-02"]

    db.delete_node("spark-01")

    assert db.get_node("spark-01") is None
    assert [node["id"] for node in db.list_nodes()] == ["spark-02"]


def test_deployment_create_get_and_json(db):
    db.create_deployment(
        "dep-1",
        name="demo-glm",
        recipe_name="demo-glm",
        mode="deployment",
        recipe={"apiVersion": "spark-fleet/v1"},
        status="loading",
    )

    deployment = db.get_deployment("dep-1")

    assert deployment["name"] == "demo-glm"
    assert deployment["mode"] == "deployment"
    assert deployment["status"] == "loading"
    assert deployment["is_primary"] is False
    assert deployment["recipe"] == {"apiVersion": "spark-fleet/v1"}
    assert db.get_deployment_by_name("demo-glm")["id"] == "dep-1"


def test_deployment_update(db):
    db.create_deployment("dep-1", "demo-glm", "demo-glm", "deployment", {})

    db.update_deployment("dep-1", status="loaded", loaded_at=10.0)

    deployment = db.get_deployment("dep-1")
    assert deployment["status"] == "loaded"
    assert deployment["loaded_at"] == 10.0


def test_set_primary_is_exclusive(db):
    db.create_deployment("dep-1", "one", "one", "deployment", {})
    db.create_deployment("dep-2", "two", "two", "deployment", {})

    db.set_primary("dep-1")
    assert db.get_deployment("dep-1")["is_primary"] is True
    assert db.get_deployment("dep-2")["is_primary"] is False

    db.set_primary("dep-2")
    assert db.get_deployment("dep-1")["is_primary"] is False
    assert db.get_deployment("dep-2")["is_primary"] is True


def test_set_primary_unknown_deployment_raises(db):
    with pytest.raises(KeyError):
        db.set_primary("missing")


def test_deployment_name_is_unique(db):
    db.create_deployment("dep-1", "same", "same", "deployment", {})

    with pytest.raises(sqlite3.IntegrityError):
        db.create_deployment("dep-2", "same", "same", "deployment", {})


def test_roles_upsert_and_list(db):
    add_node(db)
    db.create_deployment("dep-1", "demo-glm", "demo-glm", "deployment", {})

    db.upsert_role("dep-1", "worker", "spark-01", "heavy", 10)
    db.upsert_role("dep-1", "head", "spark-01", "heavy", 20, config_hash="abc")

    roles = db.list_roles("dep-1")
    assert [role["role_name"] for role in roles] == ["worker", "head"]

    db.upsert_role(
        "dep-1",
        "head",
        "spark-01",
        "heavy",
        20,
        status="running",
        config_hash="abc",
        container_id="container-1",
    )
    head = db.list_roles("dep-1")[1]
    assert head["status"] == "running"
    assert head["container_id"] == "container-1"


def test_role_requires_known_node(db):
    db.create_deployment("dep-1", "demo-glm", "demo-glm", "deployment", {})

    with pytest.raises(sqlite3.IntegrityError):
        db.upsert_role("dep-1", "head", "ghost", "heavy", 0)


def test_delete_deployment_cascades_roles(db):
    add_node(db)
    db.create_deployment("dep-1", "demo-glm", "demo-glm", "deployment", {})
    db.upsert_role("dep-1", "head", "spark-01", "heavy", 0)

    db.delete_deployment("dep-1")

    assert db.get_deployment("dep-1") is None
    assert db.list_roles("dep-1") == []


def test_jobs_and_events(db):
    db.create_deployment("dep-1", "demo-glm", "demo-glm", "deployment", {})
    db.create_job("job-1", "load", deployment_id="dep-1", payload={"recipe": "demo-glm"})

    job = db.get_job("job-1")
    assert job["kind"] == "load"
    assert job["status"] == "pending"
    assert job["payload"] == {"recipe": "demo-glm"}

    db.update_job("job-1", status="running", started_at=10.0)
    first_id = db.append_job_event("job-1", "started", phase="starting", ts=10.0)
    second_id = db.append_job_event("job-1", "worker ready", ts=11.0)

    events = db.list_job_events("job-1")
    assert [event["message"] for event in events] == ["started", "worker ready"]
    assert events[0]["phase"] == "starting"

    later = db.list_job_events("job-1", after_id=first_id)
    assert [event["id"] for event in later] == [second_id]


def test_update_job_result(db):
    db.create_job("job-1", "load")

    db.update_job("job-1", status="succeeded", result={"loaded": True}, finished_at=20.0)

    job = db.get_job("job-1")
    assert job["status"] == "succeeded"
    assert job["result"] == {"loaded": True}
    assert job["finished_at"] == 20.0


def test_mark_interrupted_jobs_failed(db):
    db.create_job("pending-job", "load")
    db.create_job("running-job", "load")
    db.create_job("done-job", "load")
    db.update_job("running-job", status="running")
    db.update_job("done-job", status="succeeded")

    count = db.mark_interrupted_jobs_failed(now=100.0)

    assert count == 2
    for job_id in ("pending-job", "running-job"):
        job = db.get_job(job_id)
        assert job["status"] == "failed"
        assert job["error"] == "interrupted by restart"
        assert job["finished_at"] == 100.0
    assert db.get_job("done-job")["status"] == "succeeded"


def test_metrics_latest_and_series(db):
    add_node(db)
    db.record_metric("spark-01", "mem_available", 100.0, ts=10.0)
    db.record_metric("spark-01", "mem_available", 90.0, ts=20.0)
    db.record_metrics([("spark-01", "mem_available", 80.0, 30.0)])

    assert db.latest_metric("spark-01", "mem_available")["value"] == 80.0

    series = db.metric_series("spark-01", "mem_available")
    assert [sample["ts"] for sample in series] == [10.0, 20.0, 30.0]

    window = db.metric_series("spark-01", "mem_available", since=15.0, until=25.0)
    assert [sample["value"] for sample in window] == [90.0]


def test_latest_metric_missing_returns_none(db):
    add_node(db)

    assert db.latest_metric("spark-01", "missing") is None


def test_inference_samples(db):
    db.create_deployment("dep-1", "demo-glm", "demo-glm", "deployment", {})
    db.record_inference_sample(
        "dep-1",
        requests=10,
        errors=1,
        latency_ms=20.0,
        ttft_ms=5.0,
        prompt_tokens=100,
        completion_tokens=50,
        ts=10.0,
    )

    samples = db.inference_series("dep-1")
    assert len(samples) == 1
    assert samples[0]["requests"] == 10
    assert samples[0]["errors"] == 1
    assert samples[0]["latency_ms"] == 20.0
    assert samples[0]["prompt_tokens"] == 100
    assert samples[0]["completion_tokens"] == 50


def test_settings(db):
    assert db.get_setting("missing") is None
    assert db.get_setting("missing", "fallback") == "fallback"

    db.set_setting("rollback_policy", "restore")
    db.set_setting("rollback_policy", "leave-down")

    assert db.get_setting("rollback_policy") == "leave-down"
    assert db.list_settings() == {"rollback_policy": "leave-down"}

    db.delete_setting("rollback_policy")
    assert db.get_setting("rollback_policy") is None


def test_prune_retention(db):
    add_node(db)
    db.create_deployment("dep-1", "demo-glm", "demo-glm", "deployment", {})
    db.record_metric("spark-01", "mem_available", 1.0, ts=10.0)
    db.record_metric("spark-01", "mem_available", 2.0, ts=100.0)
    db.record_inference_sample("dep-1", requests=1, ts=10.0)
    db.record_inference_sample("dep-1", requests=2, ts=100.0)
    db.create_job("job-1", "load", deployment_id="dep-1")
    db.append_job_event("job-1", "old", ts=10.0)
    db.append_job_event("job-1", "new", ts=100.0)

    counts = db.prune(retention_seconds=50, now=110.0)

    assert counts == {"metrics": 1, "inference": 1, "job_events": 1}
    assert [sample["ts"] for sample in db.metric_series("spark-01", "mem_available")] == [100.0]
    assert [sample["ts"] for sample in db.inference_series("dep-1")] == [100.0]
    assert [event["message"] for event in db.list_job_events("job-1")] == ["new"]


def test_incremental_vacuum_runs(db):
    add_node(db)
    db.record_metric("spark-01", "mem_available", 1.0, ts=10.0)

    db.incremental_vacuum()

    assert db.auto_vacuum_mode == 2
    assert db.schema_version == SCHEMA_VERSION


def test_vacuum_reclaims_freelist(db):
    add_node(db)
    for index in range(2000):
        db.record_metric("spark-01", "m", float(index), ts=float(index))
    db.prune(retention_seconds=0, now=10**9)

    assert db.freelist_count > 0

    db.vacuum()

    assert db.freelist_count == 0
    assert db.get_node("spark-01") is not None


def test_memory_database_supports_incremental_vacuum():
    database = Database(":memory:")
    try:
        assert database.auto_vacuum_mode == 2
        database.incremental_vacuum()
    finally:
        database.close()


def test_reopen_preserves_data(db, tmp_path):
    path = tmp_path / "fleet.sqlite"
    add_node(db)
    db.close()

    reopened = Database(path)
    try:
        assert reopened.schema_version == SCHEMA_VERSION
        assert reopened.get_node("spark-01")["display_name"] == "Spark spark-01"
    finally:
        reopened.close()


def test_future_schema_version_rejected(tmp_path):
    path = tmp_path / "fleet.sqlite"
    database = Database(path)
    database.close()

    connection = sqlite3.connect(path)
    connection.execute("PRAGMA user_version = 99")
    connection.commit()
    connection.close()

    with pytest.raises(DatabaseError, match="newer"):
        Database(path)


def test_context_manager(tmp_path):
    with Database(tmp_path / "fleet.sqlite") as database:
        assert database.schema_version == SCHEMA_VERSION

    with pytest.raises(sqlite3.ProgrammingError):
        database.get_node("spark-01")


def test_auto_vacuum_is_incremental(db):
    assert db.auto_vacuum_mode == 2


def test_node_info_can_be_cleared(db):
    add_node(db)

    db.upsert_node(
        "spark-01",
        display_name="Spark spark-01",
        lan_ip="192.0.2.239",
        fabric_ip="198.51.100.10",
        agent_url="http://spark-01:8092",
        info=None,
    )

    assert db.get_node("spark-01")["info"] is None


def test_delete_node_with_roles_refused(db):
    add_node(db)
    db.create_deployment("dep-1", "demo-glm", "demo-glm", "deployment", {})
    db.upsert_role("dep-1", "head", "spark-01", "heavy", 0)

    with pytest.raises(DatabaseError, match="referenced"):
        db.delete_node("spark-01")


def test_role_reassignment_resets_status(db):
    add_node(db)
    add_node(db, "spark-02")
    db.create_deployment("dep-1", "demo-glm", "demo-glm", "deployment", {})
    db.upsert_role(
        "dep-1",
        "head",
        "spark-01",
        "heavy",
        0,
        config_hash="abc",
        status="running",
        container_id="container-1",
    )

    db.upsert_role("dep-1", "head", "spark-02", "heavy", 0)

    head = db.list_roles("dep-1")[0]
    assert head["node_id"] == "spark-02"
    assert head["status"] == "pending"
    assert head["container_id"] is None
    assert head["config_hash"] is None


def test_update_job_requires_a_field(db):
    db.create_job("job-1", "load")

    with pytest.raises(ValueError, match="no job fields"):
        db.update_job("job-1")


def test_role_upsert_preserves_status_and_container(db):
    add_node(db)
    db.create_deployment("dep-1", "demo-glm", "demo-glm", "deployment", {})
    db.upsert_role(
        "dep-1", "head", "spark-01", "heavy", 0, status="running", container_id="container-1"
    )

    db.upsert_role("dep-1", "head", "spark-01", "heavy", 0)

    head = db.list_roles("dep-1")[0]
    assert head["status"] == "running"
    assert head["container_id"] == "container-1"


def test_deployment_error_can_be_cleared(db):
    db.create_deployment("dep-1", "demo-glm", "demo-glm", "deployment", {})

    db.update_deployment("dep-1", error="boom")
    assert db.get_deployment("dep-1")["error"] == "boom"

    db.update_deployment("dep-1", error=None)
    assert db.get_deployment("dep-1")["error"] is None


def test_job_result_can_be_cleared(db):
    db.create_job("job-1", "load")

    db.update_job("job-1", result={"loaded": True})
    assert db.get_job("job-1")["result"] == {"loaded": True}

    db.update_job("job-1", result=None)
    assert db.get_job("job-1")["result"] is None


def test_job_with_unknown_deployment_rejected(db):
    with pytest.raises(sqlite3.IntegrityError):
        db.create_job("job-1", "load", deployment_id="ghost")


def test_inference_with_unknown_deployment_rejected(db):
    with pytest.raises(sqlite3.IntegrityError):
        db.record_inference_sample("ghost")


def test_delete_deployment_detaches_jobs_and_drops_samples(db):
    add_node(db)
    db.create_deployment("dep-1", "demo-glm", "demo-glm", "deployment", {})
    db.create_job("job-1", "load", deployment_id="dep-1")
    db.record_inference_sample("dep-1", requests=1, ts=10.0)

    db.delete_deployment("dep-1")

    assert db.get_job("job-1")["deployment_id"] is None
    assert db.inference_series("dep-1") == []


def test_record_metrics_rejects_bad_rows(db):
    with pytest.raises(ValueError, match="metric rows"):
        db.record_metrics([("spark-01", "mem_available")])


@pytest.mark.parametrize(
    "call",
    [
        lambda db: db.list_jobs(limit=-1),
        lambda db: db.list_job_events("job-1", limit=-1),
        lambda db: db.metric_series("spark-01", "m", limit=-1),
        lambda db: db.inference_series("dep-1", limit=-1),
    ],
)
def test_negative_limits_rejected(db, call):
    with pytest.raises(ValueError, match="limit"):
        call(db)


def test_series_limit_returns_newest_samples(db):
    add_node(db)
    for ts in (1.0, 2.0, 3.0):
        db.record_metric("spark-01", "mem_available", ts, ts=ts)
    db.create_deployment("dep-1", "demo-glm", "demo-glm", "deployment", {})
    for ts in (1.0, 2.0, 3.0):
        db.record_inference_sample("dep-1", requests=int(ts), ts=ts)

    assert [sample["ts"] for sample in db.metric_series("spark-01", "mem_available", limit=2)] == [
        2.0,
        3.0,
    ]
    assert [sample["ts"] for sample in db.inference_series("dep-1", limit=2)] == [2.0, 3.0]


def test_prune_rejects_negative_retention(db):
    with pytest.raises(ValueError, match="retention"):
        db.prune(-1)


def test_failed_open_closes_connection(tmp_path):
    path = tmp_path / "fleet.sqlite"
    database = Database(path)
    database.close()

    connection = sqlite3.connect(path)
    connection.execute("PRAGMA user_version = 99")
    connection.commit()
    connection.close()

    with pytest.raises(DatabaseError, match="newer"):
        Database(path)

    connection = sqlite3.connect(path)
    try:
        connection.execute("CREATE TABLE probe (x INTEGER)")
        connection.execute("INSERT INTO probe VALUES (1)")
        connection.commit()
    finally:
        connection.close()


def test_migration_failure_is_wrapped(tmp_path):
    path = tmp_path / "fleet.sqlite"
    database = Database(path)
    database.close()

    connection = sqlite3.connect(path)
    connection.execute("PRAGMA user_version = 0")
    connection.commit()
    connection.close()

    with pytest.raises(DatabaseError, match="migration failed"):
        Database(path)


def test_directory_path_rejected(tmp_path):
    with pytest.raises(DatabaseError, match="open failed"):
        Database(tmp_path)


def test_parent_that_is_a_file_rejected(tmp_path):
    parent = tmp_path / "not-a-directory"
    parent.write_text("x")

    with pytest.raises(DatabaseError, match="open failed"):
        Database(parent / "fleet.sqlite")


def test_newer_legacy_database_is_not_mutated(tmp_path):
    path = tmp_path / "fleet.sqlite"
    connection = sqlite3.connect(path)
    connection.execute("CREATE TABLE probe (x INTEGER)")
    connection.execute("PRAGMA auto_vacuum = NONE")
    connection.execute("VACUUM")
    connection.execute("PRAGMA user_version = 99")
    connection.commit()
    connection.close()

    with pytest.raises(DatabaseError, match="newer"):
        Database(path)

    connection = sqlite3.connect(path)
    try:
        mode = connection.execute("PRAGMA auto_vacuum").fetchone()[0]
    finally:
        connection.close()

    assert mode == 0


def test_legacy_database_converts_to_incremental_vacuum(tmp_path):
    path = tmp_path / "fleet.sqlite"
    database = Database(path)
    database.close()

    connection = sqlite3.connect(path)
    connection.execute("PRAGMA auto_vacuum = NONE")
    connection.execute("VACUUM")
    assert connection.execute("PRAGMA auto_vacuum").fetchone()[0] == 0
    connection.close()

    database = Database(path)
    try:
        assert database.auto_vacuum_mode == 2
        database.incremental_vacuum()
    finally:
        database.close()


def test_split_statements_handles_semicolon_in_literal():
    script = "CREATE TABLE t (x TEXT DEFAULT ';');\nCREATE TABLE u (y INT);\n"

    statements = _split_statements(script)

    assert statements == [
        "CREATE TABLE t (x TEXT DEFAULT ';');",
        "CREATE TABLE u (y INT);",
    ]


def test_split_statements_handles_multiple_statements_on_one_line():
    script = "CREATE TABLE a(x); CREATE TABLE b(y);\n"

    assert _split_statements(script) == ["CREATE TABLE a(x);", "CREATE TABLE b(y);"]


def test_migration_rolls_back_on_failure(tmp_path):
    path = tmp_path / "fleet.sqlite"
    connection = sqlite3.connect(path)
    connection.execute("CREATE TABLE settings (key TEXT PRIMARY KEY, value TEXT NOT NULL)")
    connection.execute("PRAGMA user_version = 0")
    connection.commit()
    connection.close()

    with pytest.raises(DatabaseError, match="migration failed"):
        Database(path)

    connection = sqlite3.connect(path)
    try:
        tables = {
            row[0]
            for row in connection.execute("SELECT name FROM sqlite_master WHERE type = 'table'")
        }
        version = connection.execute("PRAGMA user_version").fetchone()[0]
    finally:
        connection.close()

    assert tables == {"settings"}
    assert version == 0
