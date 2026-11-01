"""Tests for the shared controller/agent configuration loader."""

from pathlib import Path

import pytest

from spark_fleet.shared.config import ConfigError, load_agent_config, load_controller_config

TOKEN = "0123456789abcdef"
SECRET = "s" * 32


def test_controller_defaults():
    config = load_controller_config({"SPARK_FLEET_AGENT_TOKEN": TOKEN})

    assert config.host == "0.0.0.0"
    assert config.port == 8091
    assert config.data_dir == Path("/var/lib/spark-fleet")
    assert config.recipes_dir == Path("/var/lib/spark-fleet/recipes")
    assert config.db_path == Path("/var/lib/spark-fleet/fleet.sqlite")
    assert config.web_dist is None
    assert config.agent_token == TOKEN
    assert config.ui_password is None
    assert config.session_secret is None
    assert config.rollback_policy == "restore"
    assert config.metrics_retention_seconds == 86400
    assert config.metrics_sample_interval_seconds == 10
    assert config.node_poll_interval_seconds == 15
    assert config.log_level == "INFO"


def test_controller_overrides():
    config = load_controller_config(
        {
            "SPARK_FLEET_AGENT_TOKEN": TOKEN,
            "SPARK_FLEET_CONTROLLER_HOST": "127.0.0.1",
            "SPARK_FLEET_CONTROLLER_PORT": "8092",
            "SPARK_FLEET_DATA_DIR": "/srv/fleet",
            "SPARK_FLEET_RECIPES_DIR": "/srv/fleet/custom-recipes",
            "SPARK_FLEET_DB_PATH": "/srv/fleet/state.sqlite",
            "SPARK_FLEET_WEB_DIST": "/srv/fleet/web",
            "SPARK_FLEET_UI_PASSWORD": "hunter2",
            "SPARK_FLEET_SESSION_SECRET": SECRET,
            "SPARK_FLEET_ROLLBACK_POLICY": "leave-down",
            "SPARK_FLEET_METRICS_RETENTION_SECONDS": "3600",
            "SPARK_FLEET_METRICS_SAMPLE_INTERVAL_SECONDS": "5",
            "SPARK_FLEET_NODE_POLL_INTERVAL_SECONDS": "30",
            "SPARK_FLEET_LOG_LEVEL": "debug",
        }
    )

    assert config.host == "127.0.0.1"
    assert config.port == 8092
    assert config.data_dir == Path("/srv/fleet")
    assert config.recipes_dir == Path("/srv/fleet/custom-recipes")
    assert config.db_path == Path("/srv/fleet/state.sqlite")
    assert config.web_dist == Path("/srv/fleet/web")
    assert config.ui_password == "hunter2"
    assert config.session_secret == SECRET
    assert config.rollback_policy == "leave-down"
    assert config.metrics_retention_seconds == 3600
    assert config.metrics_sample_interval_seconds == 5
    assert config.node_poll_interval_seconds == 30
    assert config.log_level == "DEBUG"


def test_controller_reads_process_environment(monkeypatch):
    monkeypatch.setenv("SPARK_FLEET_AGENT_TOKEN", TOKEN)
    monkeypatch.setenv("SPARK_FLEET_CONTROLLER_PORT", "9090")

    assert load_controller_config().port == 9090


def test_controller_requires_agent_token():
    with pytest.raises(ConfigError, match="SPARK_FLEET_AGENT_TOKEN is required"):
        load_controller_config({})


def test_controller_rejects_short_agent_token():
    with pytest.raises(ConfigError, match="at least 16 characters"):
        load_controller_config({"SPARK_FLEET_AGENT_TOKEN": "short"})


@pytest.mark.parametrize("value", ["0", "65536", "not-a-port", ""])
def test_controller_rejects_invalid_port(value):
    with pytest.raises(ConfigError, match="SPARK_FLEET_CONTROLLER_PORT"):
        load_controller_config(
            {"SPARK_FLEET_AGENT_TOKEN": TOKEN, "SPARK_FLEET_CONTROLLER_PORT": value}
        )


def test_controller_rejects_unknown_rollback_policy():
    with pytest.raises(ConfigError, match="SPARK_FLEET_ROLLBACK_POLICY"):
        load_controller_config(
            {"SPARK_FLEET_AGENT_TOKEN": TOKEN, "SPARK_FLEET_ROLLBACK_POLICY": "explode"}
        )


def test_controller_requires_session_secret_with_ui_password():
    with pytest.raises(ConfigError, match="SPARK_FLEET_SESSION_SECRET"):
        load_controller_config({"SPARK_FLEET_AGENT_TOKEN": TOKEN, "SPARK_FLEET_UI_PASSWORD": "pw"})


def test_controller_rejects_short_session_secret():
    with pytest.raises(ConfigError, match="at least 32 characters"):
        load_controller_config(
            {
                "SPARK_FLEET_AGENT_TOKEN": TOKEN,
                "SPARK_FLEET_UI_PASSWORD": "pw",
                "SPARK_FLEET_SESSION_SECRET": "too-short",
            }
        )


def test_controller_rejects_too_short_retention():
    with pytest.raises(ConfigError, match="SPARK_FLEET_METRICS_RETENTION_SECONDS"):
        load_controller_config(
            {"SPARK_FLEET_AGENT_TOKEN": TOKEN, "SPARK_FLEET_METRICS_RETENTION_SECONDS": "10"}
        )


def test_controller_rejects_zero_sample_interval():
    with pytest.raises(ConfigError, match="SPARK_FLEET_METRICS_SAMPLE_INTERVAL_SECONDS"):
        load_controller_config(
            {"SPARK_FLEET_AGENT_TOKEN": TOKEN, "SPARK_FLEET_METRICS_SAMPLE_INTERVAL_SECONDS": "0"}
        )


def test_controller_rejects_unknown_log_level():
    with pytest.raises(ConfigError, match="SPARK_FLEET_LOG_LEVEL"):
        load_controller_config({"SPARK_FLEET_AGENT_TOKEN": TOKEN, "SPARK_FLEET_LOG_LEVEL": "LOUD"})


def test_agent_defaults():
    config = load_agent_config({"SPARK_FLEET_AGENT_TOKEN": TOKEN})

    assert config.host == "0.0.0.0"
    assert config.port == 8092
    assert config.node_id is None
    assert config.data_dir == Path("/var/lib/spark-fleet-agent")
    assert config.compose_projects_dir == Path("/var/lib/spark-fleet-agent/projects")
    assert config.model_cache_roots == (Path("/srv/spark-fleet/hub"),)
    assert config.agent_token == TOKEN
    assert config.log_level == "INFO"


def test_agent_overrides():
    config = load_agent_config(
        {
            "SPARK_FLEET_AGENT_TOKEN": TOKEN,
            "SPARK_FLEET_AGENT_HOST": "192.0.2.79",
            "SPARK_FLEET_AGENT_PORT": "18092",
            "SPARK_FLEET_AGENT_NODE_ID": "spark-02",
            "SPARK_FLEET_AGENT_DATA_DIR": "/srv/agent",
            "SPARK_FLEET_COMPOSE_PROJECTS_DIR": "/srv/agent/projects",
            "SPARK_FLEET_MODEL_CACHE_ROOTS": "/models/a, /models/b,/models/a",
            "SPARK_FLEET_LOG_LEVEL": "warning",
        }
    )

    assert config.host == "192.0.2.79"
    assert config.port == 18092
    assert config.node_id == "spark-02"
    assert config.data_dir == Path("/srv/agent")
    assert config.compose_projects_dir == Path("/srv/agent/projects")
    assert config.model_cache_roots == (Path("/models/a"), Path("/models/b"))
    assert config.log_level == "WARNING"


def test_agent_requires_agent_token():
    with pytest.raises(ConfigError, match="SPARK_FLEET_AGENT_TOKEN is required"):
        load_agent_config({})


def test_agent_rejects_empty_cache_roots():
    with pytest.raises(ConfigError, match="SPARK_FLEET_MODEL_CACHE_ROOTS"):
        load_agent_config(
            {"SPARK_FLEET_AGENT_TOKEN": TOKEN, "SPARK_FLEET_MODEL_CACHE_ROOTS": " , "}
        )


@pytest.mark.parametrize("value", ["0", "65536", "abc"])
def test_agent_rejects_invalid_port(value):
    with pytest.raises(ConfigError, match="SPARK_FLEET_AGENT_PORT"):
        load_agent_config({"SPARK_FLEET_AGENT_TOKEN": TOKEN, "SPARK_FLEET_AGENT_PORT": value})


def test_config_error_is_value_error():
    assert issubclass(ConfigError, ValueError)
