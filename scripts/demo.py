"""Local Iceberg append reproduction. No Airflow and no cloud catalog."""

from __future__ import annotations

import shutil
import sys
import tempfile
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "iceberg_pkg" / "src"))

from iceberg_append.scenarios import format_report, run  # noqa: E402


def main() -> int:
    directory = Path(tempfile.mkdtemp(prefix="iceberg-append-demo-"))
    try:
        results = run(directory)
    finally:
        shutil.rmtree(directory)
    print(format_report(results))
    return 0 if all(row["ok"] for row in results) else 1


if __name__ == "__main__":
    raise SystemExit(main())
