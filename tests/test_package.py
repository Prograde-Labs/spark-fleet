"""Smoke tests for the spark-fleet package scaffold."""

from importlib.metadata import PackageNotFoundError, distribution

import pytest

import spark_fleet

EXPECTED_SCRIPTS = {
    "spark-fleet-controller": "spark_fleet.controller.main:main",
    "spark-fleet-agent": "spark_fleet.agent.main:main",
}


def _distribution():
    try:
        return distribution("spark-fleet")
    except PackageNotFoundError:
        pytest.fail("spark-fleet is not installed; run `uv pip install -e '.[dev]'`")


def _console_scripts():
    return [ep for ep in _distribution().entry_points if ep.group == "console_scripts"]


def test_package_version_matches_installed_metadata():
    assert spark_fleet.__version__ == _distribution().version, (
        "installed metadata is stale; reinstall the editable package"
    )


def test_console_scripts_match_expected_set():
    declared = {ep.name: ep.value for ep in _console_scripts()}
    assert declared == EXPECTED_SCRIPTS, f"declared={declared} expected={EXPECTED_SCRIPTS}"


def test_console_scripts_load_callables():
    scripts = _console_scripts()
    assert scripts, "distribution declares no console scripts"
    for ep in scripts:
        assert callable(ep.load()), f"{ep.name} does not resolve to a callable"


def test_readme_is_embedded_in_metadata():
    metadata = _distribution().metadata
    assert metadata["Description-Content-Type"] == "text/markdown"
    assert "# spark-fleet" in metadata.get_payload()


def test_package_has_docstring():
    assert spark_fleet.__doc__
