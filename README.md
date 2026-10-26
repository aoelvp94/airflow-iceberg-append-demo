# Append to an Iceberg table from Airflow

A local reproduction of a gap in `apache-airflow-providers-apache-iceberg`:
`IcebergHook` can load a table, its schema, and its snapshots. It has no
method that adds rows.

`just check` uses a SQLite catalog and a file warehouse under a temp directory.
No AWS, no REST catalog, no Airflow. `just e2e` runs the same four checks as
an Airflow task.

```
today                                         this demo

hook.load_table("demo.events")                append_records(catalog, "demo.events", rows)
hook.get_table_schema(...)                    Arrow schema taken from the table
hook.get_snapshots(...)                       unknown keys dropped
you build the catalog client yourself         CommitFailedException reloads and retries
```

pyiceberg 0.12 already retries a commit conflict inside the transaction and
refreshes that table object. The table in the conflict scenario sets
`commit.retry.num-retries` to `0`, so a stale handle raises
`CommitFailedException` instead of hiding it. `append_records` then loads the
table again and writes. That reload is the part a hook method has to do when
the caller is holding a snapshot from before someone else committed.

Records are passed as dicts. `pyarrow.Table.from_pylist` is called with the
table's Arrow schema, so a key the table does not have yet is left out of the
file. Adding the column later does not require the writer to change first.

Use a `pa.Table`, not a `RecordBatchReader`. The reader is consumed by the
first attempt, so a retry would append no rows.

## Reproduce

Python 3.10+. The first run creates `.venv` and installs `pyiceberg` and
`pyarrow`.

```bash
just check
```

Sample output:

```
  append            pass  read back [{'id': 1, 'name': 'alpha'}, {'id': 2, 'name': 'beta'}]
  unknown key       pass  stored {'id': 3, 'name': 'gamma'}
  stale append      pass  CommitFailedException after another writer committed (table retry count is 0)
  reload and retry  pass  both writers present: [{'id': 1, 'name': 'first'}, {'id': 2, 'name': 'second'}]

all 4 scenarios passed
```

## The same checks inside Airflow

Requires [Docker](https://docs.docker.com/get-docker/), the
[Astro CLI](https://www.astronomer.io/docs/astro/cli/install-cli), and
[just](https://github.com/casey/just).

```bash
just e2e
```

The image installs `iceberg_pkg/`. The task `iceberg_append_demo.run_scenarios`
prints the same report. The catalog files live in a temp directory inside the
task and are deleted when it finishes. The UI is at http://localhost:8080.

`just down` stops it. `just clean` deletes the local Astro state and `.venv`.

## Where this would land upstream

A method on
[`IcebergHook`](https://github.com/apache/airflow/blob/main/providers/apache/iceberg/src/airflow/providers/apache/iceberg/hooks/iceberg.py),
next to `load_table`:

```python
IcebergHook().append("demo.events", [{"id": 1, "name": "alpha"}])
```

The catalog is the one the hook already builds from the connection. This repo
points that call at SQLite so a reviewer can run it without a catalog service.
`iceberg_pkg/src/iceberg_append/append.py` is the method body.

## Layout

```
iceberg_pkg/          append_records and the four scenarios
scripts/demo.py       local entry point
dags/iceberg_append_demo.py
```
