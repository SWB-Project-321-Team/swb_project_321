# python/ingest/

Scripts that pull **raw** data from sources and write to OneDrive `01_data/raw/`.

- IRS 990 extracts, Giving Tuesday data, Census/ACS, or other APIs/files.
- Output goes to the appropriate subfolder under `01_data/raw/` (e.g. `irs_990/`, `census_acs/`).
- No data or credentials in this repo—paths and secrets from environment or config.
