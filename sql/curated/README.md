# sql/curated/

SQL that turns **staging** tables into **curated** (gold) analysis-ready datasets.

- Input: OneDrive `01_data/staging/`.
- Output: OneDrive `01_data/curated/` (versioned snapshots, e.g. `v1_YYYY-MM-DD/*.parquet`).

Curated tables are filtered and shaped for standard reports and benchmarks. Do not commit data—only `.sql` files.
