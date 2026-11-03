#!/bin/sh
# Prepare mounted data directories, then drop from root to the sparkfleet user.
set -eu

if [ "$(id -u)" = "0" ]; then
  mkdir -p "${SPARK_FLEET_DATA_DIR:-/var/lib/spark-fleet}"
  mkdir -p "${SPARK_FLEET_RECIPES_DIR:-/var/lib/spark-fleet/recipes}"
  # Only the top-level directories are chowned; recipe files stay readable by
  # the host operator and are replaced atomically by the controller.
  chown sparkfleet:sparkfleet \
    "${SPARK_FLEET_DATA_DIR:-/var/lib/spark-fleet}" \
    "${SPARK_FLEET_RECIPES_DIR:-/var/lib/spark-fleet/recipes}"
  exec gosu sparkfleet "$@"
fi

exec "$@"
