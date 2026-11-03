"""Append rows to an Iceberg table.

``IcebergHook`` can load a table, its schema, and its snapshots. It cannot
write a row. ``append_records`` is the method that would sit next to
``load_table``:

- accept ``list[dict]`` or ``pa.Table``
- schema-align: dict keys / Arrow columns the table does not have yet are
  dropped instead of failing the write
- reject unqualified table names (must be ``namespace.table``)
- call ``Table.append``
- on ``CommitFailedException``, reload the table and try again with no sleep

pyiceberg 0.12 already retries a few times inside the transaction and refreshes
the same table object. That loop does not run when the table property
``commit.retry.num-retries`` is ``0``, and it does not help a caller that kept
a table object from before someone else committed. Reloading from the catalog
is the step this method adds for that case.

Pass a ``list[dict]`` or a ``pa.Table``, not a ``RecordBatchReader``. A reader
is consumed by the first attempt, so a retry would append nothing.
"""

from __future__ import annotations

from typing import Any

import pyarrow as pa
from pyiceberg.catalog import Catalog, load_catalog
from pyiceberg.exceptions import CommitFailedException
from pyiceberg.table import Table


def open_local_catalog(directory: str) -> Catalog:
    """SQLite catalog and a file warehouse. No cloud, no REST server."""
    return load_catalog(
        "demo",
        **{
            "type": "sql",
            "uri": f"sqlite:///{directory}/catalog.db",
            "warehouse": f"file://{directory}/warehouse",
        },
    )


def load_table(catalog: Catalog, table_name: str) -> Table:
    """Load ``table_name``. Names must be ``namespace.table``."""
    if "." not in table_name:
        raise ValueError(f"Expected fully-qualified table name (namespace.table), got: {table_name!r}")
    return catalog.load_table(table_name)


def append_records(
    catalog: Catalog,
    table_name: str,
    records: list[dict[str, Any]] | pa.Table,
    *,
    retries: int = 8,
) -> None:
    """Append ``records`` to ``table_name``, reloading after a commit conflict.

    ``records`` may be a list of dicts or a ``pyarrow.Table``. Dict keys that
    are not in the table schema are dropped (same behaviour as
    ``pa.Table.from_pylist(..., schema=table.schema().as_arrow())``).
    Do not pass a ``RecordBatchReader``: it is consumed on the first attempt,
    so a retry after ``CommitFailedException`` would append no rows.

    On ``CommitFailedException`` the table is reloaded from the catalog and
    the write is retried up to ``retries`` times. That covers a stale table
    handle held across another writer's commit (pyiceberg's in-transaction
    retry does not help that case, and is off when the table property
    ``commit.retry.num-retries`` is ``0``). There is no sleep between attempts.

    This is the body proposed for ``IcebergHook.append``. The catalog is the
    one ``IcebergHook.catalog`` already builds from the Airflow connection.

    :param table_name: Fully-qualified table name (``namespace.table``).
    :param records: Rows to append.
    :param retries: Total attempts including the first (must be >= 1).
    """
    if retries < 1:
        raise ValueError("retries must be at least 1")

    last_error: CommitFailedException | None = None
    for _ in range(retries):
        table = load_table(catalog, table_name)
        arrow_schema = table.schema().as_arrow()
        if isinstance(records, pa.Table):
            arrow = records.select(
                [name for name in records.column_names if name in arrow_schema.names]
            ).cast(arrow_schema)
        else:
            arrow = pa.Table.from_pylist(records, schema=arrow_schema)
        try:
            table.append(arrow)
            return
        except CommitFailedException as error:
            last_error = error
    raise RuntimeError(f"append to {table_name} failed after {retries} attempts") from last_error
