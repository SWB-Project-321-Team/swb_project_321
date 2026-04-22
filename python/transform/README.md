# python/transform/

Data validation and pipeline orchestration: run SQL staging/curated and write results to OneDrive.

- Execute `sql/staging` and `sql/curated` (e.g. via DuckDB over Parquet/CSV).
- Write staging output to OneDrive `01_data/staging/`, curated to `01_data/curated/`.
- Validation, schema checks, or quality checks that run as part of the pipeline.

Scripts must be reproducible. No data files or credentials in repo.
