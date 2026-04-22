# python/utils/

Shared helper code used by ingest, transform, and export.

- **paths.py** — Repo root (`BASE`) and project data root (`DATA` = `01_data`). Override data root with env `SWB_321_DATA_ROOT`. Ingest/transform scripts import `from utils.paths import DATA` (or `BASE`, `get_data_root()`).
- Path helpers (OneDrive project root, curated version resolution).
- Logging, config loading, or environment variables.
- Database/engine helpers (e.g. DuckDB connection over Parquet paths).

No data files or credentials; only reusable, testable utilities.
