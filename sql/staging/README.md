# sql/staging/

SQL that turns **raw** data (in OneDrive) into **staging** (silver) tables: cleaned, normalized, one row per entity or filing.

- Input: OneDrive `01_data/raw/` (and reference tables).
- Output: OneDrive `01_data/staging/` (org, filing, financial_fact, geo_bridge, etc.).

Use an engine that can read Parquet/CSV (e.g. DuckDB). Do not commit data files—only `.sql` files.
