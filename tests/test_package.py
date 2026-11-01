"""Smoke tests for the spark-fleet package scaffold."""

from importlib.metadata import distribution, version

import spark_fleet

EXPECTED_SCRIPTS = {
    "spark-fleet-controller": "spark_fleet.controller.main:main",
    "spark-fleet-agent": "spark_fleet.agent.main:main",
}


def _console_scripts():
    return [ep for ep in distribution("spark-fleet").entry_points if ep.group == "console_scripts"]


def test_package_version_matches_installed_metadata():
    assert spark_fleet.__version__ == version("spark-fleet"), (
        "installed metadata is stale; reinstall the editable package"
    )


def test_console_scripts_match_expected_set():
    declared = {ep.name: ep.value for ep in _console_scripts()}
    assert declared == EXPECTED_SCRIPTS, f"unexpected console scripts: {declared}"


def test_console_scripts_load_callables():
    for ep in _console_scripts():
        target = ep.load()
        assert callable(target), f"{ep.name} does not resolve to a callable"


def test_package_has_docstring():
    assert spark_fleet.__doc__
