# sql/

SQL logic for the project. Runs against data in OneDrive (Parquet/CSV); no database files are required for the pipeline.

- **staging/** — Load and normalize raw source extracts (writes to OneDrive/staging).
- **curated/** — Build analysis-ready tables from staging (writes to OneDrive/curated).
- **meta/** — ETL logs, run metadata, and schema documentation.

See `docs/OneDrive_Structure.md` for how SQL fits into the data flow.
