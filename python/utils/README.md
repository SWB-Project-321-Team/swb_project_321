# python/utils/

Shared helper code used by ingest, transform, and export.

- Path helpers (OneDrive project root, curated version resolution).
- Logging, config loading, or environment variables.
- Database/engine helpers (e.g. DuckDB connection over Parquet paths).

No data files or credentials; only reusable, testable utilities.
