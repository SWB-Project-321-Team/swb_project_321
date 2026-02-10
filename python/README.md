# python/

Python code for ingestion, transformation, and export. All scripts should be reproducible and assume data lives in OneDrive (paths from env or config).

- **ingest/** — Pull raw data from sources into OneDrive `01_data/raw/`.
- **transform/** — Orchestrate running SQL staging/curated and writing to OneDrive staging/curated.
- **export/** — Export CSV/Excel and other deliverables to OneDrive `02_outputs/`.
- **utils/** — Shared helpers (paths, logging, DB connection, etc.). No data or credentials in repo.
