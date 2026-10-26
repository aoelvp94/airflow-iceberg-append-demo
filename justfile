set shell := ["bash", "-cu"]

default:
    @just --list

# SQLite catalog, file warehouse, four scenarios. Creates .venv on first run.
check:
    #!/usr/bin/env bash
    set -euo pipefail
    if [ ! -x .venv/bin/python ]; then
        python3 -m venv .venv
        .venv/bin/pip install -q -e iceberg_pkg
    fi
    .venv/bin/python scripts/demo.py

up:
    astro dev start --no-browser

task:
    #!/usr/bin/env bash
    set -euo pipefail
    scheduler=$(docker ps -qf name=scheduler | head -1)
    [ -n "$scheduler" ] || { echo "scheduler is not running — run 'just up' first"; exit 1; }
    docker exec "$scheduler" airflow tasks test iceberg_append_demo run_scenarios 2026-01-01

e2e: up task

down:
    astro dev stop

clean: down
    astro dev kill || true
    rm -rf .venv
