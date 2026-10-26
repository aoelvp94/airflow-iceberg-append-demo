"""The ``append`` method proposed for ``IcebergHook``."""

from iceberg_append.append import append_records, open_local_catalog

__all__ = ["append_records", "open_local_catalog"]
