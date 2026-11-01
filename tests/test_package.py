"""Smoke tests for the spark-fleet package scaffold."""

import importlib
from importlib.metadata import distribution, version

import spark_fleet

EXPECTED_SCRIPTS = {
    "spark-fleet-controller": "spark_fleet.controller.main:main",
    "spark-fleet-agent": "spark_fleet.agent.main:main",
}


def test_package_version_matches_installed_metadata():
    assert spark_fleet.__version__ == version("spark-fleet")


def test_console_scripts_are_declared_on_our_distribution():
    declared = {ep.name: ep.value for ep in distribution("spark-fleet").entry_points}
    assert {name: declared[name] for name in EXPECTED_SCRIPTS} == EXPECTED_SCRIPTS


def test_console_script_targets_resolve_to_callables():
    for target in EXPECTED_SCRIPTS.values():
        module_name, _, attr = target.partition(":")
        module = importlib.import_module(module_name)
        assert callable(getattr(module, attr))


def test_package_has_docstring():
    assert spark_fleet.__doc__
