# syntax=docker/dockerfile:1.7

ARG PYTHON_VERSION=3.12
ARG NODE_VERSION=22

# --- Web build -------------------------------------------------------------
FROM node:${NODE_VERSION}-alpine AS web
WORKDIR /app/web
COPY web/package.json web/package-lock.json ./
RUN npm ci --no-audit --no-fund
COPY web/ ./
RUN npm test
RUN npm run build

# --- Python wheel ----------------------------------------------------------
FROM python:${PYTHON_VERSION}-slim AS python-build
WORKDIR /build
RUN pip install --no-cache-dir build
COPY pyproject.toml README.md ./
COPY src/ src/
RUN python -m build --wheel --outdir /dist

# --- Controller ------------------------------------------------------------
FROM python:${PYTHON_VERSION}-slim AS controller
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    SPARK_FLEET_DATA_DIR=/var/lib/spark-fleet \
    SPARK_FLEET_WEB_DIST=/app/web
WORKDIR /app
COPY --from=python-build /dist/*.whl /tmp/
RUN apt-get update \
    && apt-get install -y --no-install-recommends gosu \
    && rm -rf /var/lib/apt/lists/*
RUN pip install --no-cache-dir /tmp/*.whl && rm -rf /tmp/*.whl
COPY --from=web /app/web/dist /app/web
RUN useradd --uid 10001 --create-home sparkfleet \
    && mkdir -p /var/lib/spark-fleet \
    && chown -R sparkfleet:sparkfleet /var/lib/spark-fleet /app
COPY docker/controller-entrypoint.sh /usr/local/bin/controller-entrypoint.sh
RUN chmod +x /usr/local/bin/controller-entrypoint.sh
EXPOSE 8091
HEALTHCHECK --interval=20s --timeout=5s --start-period=20s --retries=3 \
    CMD python -c "import os, urllib.request; host=os.environ.get('SPARK_FLEET_CONTROLLER_HOST','0.0.0.0'); host='127.0.0.1' if host in ('0.0.0.0','::') else host; port=os.environ.get('SPARK_FLEET_CONTROLLER_PORT','8091'); urllib.request.urlopen(f'http://{host}:{port}/health', timeout=3)"
ENTRYPOINT ["/usr/local/bin/controller-entrypoint.sh"]
CMD ["spark-fleet-controller"]

# --- Agent -----------------------------------------------------------------
FROM python:${PYTHON_VERSION}-slim AS agent
ARG TARGETARCH
ARG DOCKER_CLI_VERSION=27.5.1
ARG COMPOSE_VERSION=2.32.4
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    SPARK_FLEET_AGENT_DATA_DIR=/var/lib/spark-fleet-agent \
    SPARK_FLEET_COMPOSE_PROJECTS_DIR=/var/lib/spark-fleet-agent/projects
WORKDIR /app
RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates curl \
    && rm -rf /var/lib/apt/lists/*
RUN set -eux; \
    case "${TARGETARCH:?TARGETARCH is required}" in \
        amd64) ARCH=x86_64 ;; \
        arm64) ARCH=aarch64 ;; \
        *) echo "unsupported TARGETARCH=${TARGETARCH}" >&2; exit 1 ;; \
    esac; \
    curl -fsSL "https://download.docker.com/linux/static/stable/${ARCH}/docker-${DOCKER_CLI_VERSION}.tgz" \
        | tar -xz --strip-components=1 -C /usr/local/bin docker/docker; \
    mkdir -p /usr/local/lib/docker/cli-plugins; \
    curl -fsSL "https://github.com/docker/compose/releases/download/v${COMPOSE_VERSION}/docker-compose-linux-${ARCH}" \
        -o /usr/local/lib/docker/cli-plugins/docker-compose; \
    chmod +x /usr/local/bin/docker /usr/local/lib/docker/cli-plugins/docker-compose
COPY --from=python-build /dist/*.whl /tmp/
RUN pip install --no-cache-dir /tmp/*.whl && rm -rf /tmp/*.whl
RUN mkdir -p /var/lib/spark-fleet-agent
EXPOSE 8092
HEALTHCHECK --interval=20s --timeout=5s --start-period=15s --retries=3 \
    CMD python -c "import os, urllib.request; host=os.environ.get('SPARK_FLEET_AGENT_HOST','0.0.0.0'); host='127.0.0.1' if host in ('0.0.0.0','::') else host; port=os.environ.get('SPARK_FLEET_AGENT_PORT','8092'); urllib.request.urlopen(f'http://{host}:{port}/health', timeout=3)"
ENTRYPOINT ["spark-fleet-agent"]
