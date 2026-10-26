"""Same Iceberg scenarios as ``scripts/demo.py``, run as one Airflow task."""

from __future__ import annotations

import shutil
import tempfile
from pathlib import Path

from airflow.sdk import dag, task


@dag(dag_id="iceberg_append_demo", schedule=None, catchup=False, tags=["iceberg", "demo"])
def iceberg_append_demo():
    @task
    def run_scenarios() -> list[dict]:
        from iceberg_append.scenarios import format_report, run

        directory = Path(tempfile.mkdtemp(prefix="iceberg-append-demo-"))
        try:
            results = run(directory)
        finally:
            shutil.rmtree(directory, ignore_errors=True)
        print(format_report(results))
        failed = [row["name"] for row in results if not row["ok"]]
        if failed:
            raise RuntimeError(f"iceberg scenarios failed: {failed}")
        return results

    run_scenarios()


iceberg_append_demo()
