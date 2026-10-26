"""Append a list of dicts to an Iceberg table.

``IcebergHook`` can load a table, its schema, and its snapshots. It cannot
write a row. ``append_records`` is the method that would sit next to
``load_table``:

- build an Arrow table from the dicts using the table's own schema, so a key
  the table does not have yet is dropped instead of failing the write
- call ``Table.append``
- on ``CommitFailedException``, reload the table and try again

pyiceberg 0.12 already retries a few times inside the transaction and refreshes
the same table object. That loop does not run when the table property
``commit.retry.num-retries`` is ``0``, and it does not help a caller that kept
a table object from before someone else committed. Reloading from the catalog
is the step this method adds for that case.

Pass a ``pa.Table``, not a ``RecordBatchReader``. A reader is consumed by the
first attempt, so a retry would append nothing.
"""

from __future__ import annotations

import time
from typing import Any

import pyarrow as pa
from pyiceberg.catalog import Catalog, load_catalog
from pyiceberg.exceptions import CommitFailedException


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


def append_records(
    catalog: Catalog,
    table_name: str,
    records: list[dict[str, Any]],
    *,
    retries: int = 8,
) -> None:
    """Append ``records`` to ``table_name``, reloading after a commit conflict.

    This is the body proposed for ``IcebergHook.append``. The catalog is the
    one ``IcebergHook.catalog`` already builds from the Airflow connection.
    """
    if retries < 1:
        raise ValueError("retries must be at least 1")

    delay = 0.05
    last_error: CommitFailedException | None = None
    for _attempt in range(retries):
        table = catalog.load_table(table_name)
        arrow = pa.Table.from_pylist(records, schema=table.schema().as_arrow())
        try:
            table.append(arrow)
            return
        except CommitFailedException as error:
            last_error = error
            time.sleep(delay)
            delay = min(delay * 2, 2.0)
    raise RuntimeError(f"append to {table_name} failed after {retries} attempts") from last_error
