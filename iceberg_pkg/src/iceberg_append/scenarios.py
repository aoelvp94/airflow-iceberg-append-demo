"""Local scenarios shared by ``scripts/demo.py`` and the DAG."""

from __future__ import annotations

from pathlib import Path

import pyarrow as pa
from pyiceberg.exceptions import CommitFailedException
from pyiceberg.schema import Schema
from pyiceberg.types import LongType, NestedField, StringType

from iceberg_append.append import append_records, open_local_catalog


def run(directory: Path) -> list[dict]:
    catalog = open_local_catalog(str(directory))
    catalog.create_namespace("demo")
    schema = Schema(
        NestedField(field_id=1, name="id", field_type=LongType(), required=True),
        NestedField(field_id=2, name="name", field_type=StringType(), required=False),
    )
    catalog.create_table("demo.events", schema=schema)
    catalog.create_table(
        "demo.races",
        schema=schema,
        properties={"commit.retry.num-retries": "0"},
    )
    return [
        _append_and_read(catalog),
        _extra_key_dropped(catalog),
        _append_arrow_table(catalog),
        _unqualified_name_rejected(catalog),
        _retries_must_be_at_least_one(catalog),
        _stale_handle_raises(catalog),
        _reload_after_conflict(catalog),
    ]


def format_report(results: list[dict]) -> str:
    width = max(len(row["name"]) for row in results)
    lines = [f"  {row['name']:<{width}}  {'pass' if row['ok'] else 'FAIL'}  {row['detail']}" for row in results]
    failed = [row["name"] for row in results if not row["ok"]]
    lines.append("")
    lines.append(f"{len(failed)} failed: {', '.join(failed)}" if failed else f"all {len(results)} scenarios passed")
    return "\n".join(lines)


def _scan(catalog, table_name: str) -> list[dict]:
    rows = catalog.load_table(table_name).scan().to_arrow().to_pylist()
    return sorted(rows, key=lambda row: row["id"])


def _append_and_read(catalog) -> dict:
    append_records(
        catalog,
        "demo.events",
        [{"id": 1, "name": "alpha"}, {"id": 2, "name": "beta"}],
    )
    rows = _scan(catalog, "demo.events")
    ok = rows == [{"id": 1, "name": "alpha"}, {"id": 2, "name": "beta"}]
    return {"name": "append", "ok": ok, "detail": f"read back {rows}"}


def _extra_key_dropped(catalog) -> dict:
    append_records(catalog, "demo.events", [{"id": 3, "name": "gamma", "note": "not a column yet"}])
    rows = _scan(catalog, "demo.events")
    stored = next(row for row in rows if row["id"] == 3)
    ok = stored == {"id": 3, "name": "gamma"} and "note" not in stored
    return {"name": "unknown key", "ok": ok, "detail": f"stored {stored}"}


def _append_arrow_table(catalog) -> dict:
    src = pa.table({"id": [4], "name": ["delta"], "extra": ["not a column yet"]})
    append_records(catalog, "demo.events", src)
    rows = _scan(catalog, "demo.events")
    stored = next(row for row in rows if row["id"] == 4)
    ok = stored == {"id": 4, "name": "delta"} and "extra" not in stored
    return {"name": "pa.Table", "ok": ok, "detail": f"stored {stored}"}


def _unqualified_name_rejected(catalog) -> dict:
    try:
        append_records(catalog, "events", [{"id": 99, "name": "nope"}])
    except ValueError as error:
        ok = "namespace.table" in str(error)
        return {"name": "unqualified name", "ok": ok, "detail": str(error)}
    return {"name": "unqualified name", "ok": False, "detail": "bare table name was accepted"}


def _retries_must_be_at_least_one(catalog) -> dict:
    try:
        append_records(catalog, "demo.events", [{"id": 1}], retries=0)
    except ValueError as error:
        ok = "at least 1" in str(error)
        return {"name": "retries >= 1", "ok": ok, "detail": str(error)}
    return {"name": "retries >= 1", "ok": False, "detail": "retries=0 was accepted"}


def _stale_handle_raises(catalog) -> dict:
    stale = catalog.load_table("demo.races")
    arrow = stale.schema().as_arrow()
    catalog.load_table("demo.races").append(pa.Table.from_pylist([{"id": 1, "name": "first"}], schema=arrow))
    try:
        stale.append(pa.Table.from_pylist([{"id": 2, "name": "second"}], schema=arrow))
    except CommitFailedException:
        return {
            "name": "stale append",
            "ok": True,
            "detail": "CommitFailedException after another writer committed (table retry count is 0)",
        }
    return {"name": "stale append", "ok": False, "detail": "the stale append succeeded; the conflict was not visible"}


def _reload_after_conflict(catalog) -> dict:
    append_records(catalog, "demo.races", [{"id": 2, "name": "second"}])
    rows = _scan(catalog, "demo.races")
    ok = rows == [{"id": 1, "name": "first"}, {"id": 2, "name": "second"}]
    return {"name": "reload and retry", "ok": ok, "detail": f"both writers present: {rows}"}
