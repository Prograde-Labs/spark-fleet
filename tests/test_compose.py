"""Static checks for the multi-target image and Compose stacks."""

from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parents[1]


def load(path):
    return yaml.safe_load(path.read_text(encoding="utf-8"))


def test_dockerfile_builds_web_wheel_controller_and_agent():
    text = (ROOT / "Dockerfile").read_text(encoding="utf-8")

    for stage in ("AS web", "AS python-build", "AS controller", "AS agent"):
        assert stage in text
    assert "spark-fleet-controller" in text
    assert "spark-fleet-agent" in text
    assert "controller-entrypoint.sh" in text
    assert "gosu sparkfleet" in (ROOT / "docker" / "controller-entrypoint.sh").read_text()
    assert "HEALTHCHECK" in text
    assert "docker-${DOCKER_CLI_VERSION}.tgz" in text
    assert "docker-compose-linux-${ARCH}" in text
    # Controller image never receives the Docker socket.
    assert "docker.sock" not in text.split("AS controller")[1].split("AS agent")[0]


def test_controller_compose_is_host_network_without_docker_socket():
    document = load(ROOT / "compose.controller.yaml")
    service = document["services"]["controller"]

    assert service["network_mode"] == "host"
    assert service["build"]["target"] == "controller"
    assert "healthcheck" in service
    assert service["cap_drop"] == ["ALL"]
    assert "no-new-privileges:true" in service["security_opt"]
    volumes = service.get("volumes", [])
    assert not any("docker.sock" in volume for volume in volumes)
    environment = service["environment"]
    assert "SPARK_FLEET_AGENT_TOKEN" in environment
    assert environment["SPARK_FLEET_WEB_DIST"] == "/app/web"
    assert environment["SPARK_FLEET_DATA_DIR"] == "/var/lib/spark-fleet"
    env_file = service.get("env_file")
    assert env_file
    assert env_file[0]["required"] is False


def test_agent_compose_has_narrow_mounts_and_no_extra_privileges():
    document = load(ROOT / "compose.agent.yaml")
    service = document["services"]["agent"]

    assert service["network_mode"] == "host"
    assert service["build"]["target"] == "agent"
    assert "healthcheck" in service
    assert service["cap_drop"] == ["ALL"]
    assert "no-new-privileges:true" in service["security_opt"]
    volumes = service.get("volumes", [])
    assert "/var/run/docker.sock:/var/run/docker.sock" in volumes
    assert any("/var/lib/spark-fleet-agent" in volume for volume in volumes)
    assert any("/srv/spark-fleet/hub" in volume for volume in volumes)

    forbidden = {
        "privileged",
        "pid",
        "ipc",
        "devices",
        "cap_add",
        "userns_mode",
    }
    assert forbidden.isdisjoint(service)
    assert service.get("privileged") is not True


def test_env_example_documents_required_and_optional_secrets():
    text = (ROOT / ".env.example").read_text(encoding="utf-8")

    assert "SPARK_FLEET_AGENT_TOKEN=" in text
    assert "SPARK_FLEET_UI_PASSWORD=" in text
    assert "SPARK_FLEET_SESSION_SECRET=" in text
    assert "SPARK_FLEET_WEB_DIST" not in text or "replace-with" in text
    assert "replace-with-at-least-16" in text
    assert "replace-with-a-strong-password" in text
    assert "replace-with-at-least-32" in text
