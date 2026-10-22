#!/usr/bin/env bash
# Obelisk Dagster service entrypoint: daemon (schedules/sensors) + webserver (UI/GraphQL).
# Run/event storage lives in DAGSTER_HOME (named volume obelisk_dagster_home) —
# SQLite by default, shared by both processes.
set -m
mkdir -p "${DAGSTER_HOME}"
cd /opt/obelisk/git_repo/dagster_workspace
dagster-daemon run -w workspace.yaml &
exec dagster-webserver -h 0.0.0.0 -p 3000 -w workspace.yaml
