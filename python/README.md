# python/

Python code for ingestion, transformation, and export. All scripts should be reproducible and
assume data lives in OneDrive (paths from env or config).

- **ingest/** - Pull raw data from sources into OneDrive `01_data/raw/`.
- **transform/** - Orchestrate running SQL staging/curated and writing to OneDrive staging/curated.
- **export/** - Export CSV/Excel and other deliverables to OneDrive `02_outputs/`.
- **utils/** - Shared helpers (paths, logging, DB connection, etc.). No data or credentials in repo.

## Run order (ingest)

Run from **repo root** (e.g. `cd` to the repo, then run the commands below).

**990_irs** (IRS TEOS 990 XML -> S3 -> staging): run **01 -> 02 -> 03**.
```bash
python python/ingest/990_irs/01_upload_irs_990_index_to_s3.py
python python/ingest/990_irs/02_upload_irs_990_zips_to_s3.py
python python/ingest/990_irs/03_parse_irs_990_zips_to_staging.py
```
Or use the runner: `python python/run_990_irs.py` (see `ingest/990_irs/README.md` for options).

**990_givingtuesday**: run **01 -> 02 -> 03 -> 04**. See `ingest/990_givingtuesday/api/README.md`
and `ingest/README.md`.

**irs_soi** (IRS SOI county raw -> S3 -> benchmark county CSVs): run **01 -> 06**.
```bash
python python/ingest/irs_soi/01_discover_county_release.py
python python/ingest/irs_soi/02_download_county_release.py
python python/ingest/irs_soi/03_upload_county_release_to_s3.py
python python/ingest/irs_soi/04_verify_county_release_source_local_s3.py
python python/ingest/irs_soi/05_filter_county_release_to_benchmark_local.py
python python/ingest/irs_soi/06_upload_filtered_county_release_to_s3.py
```
Or use the runner: `python python/run_irs_soi_county.py` (see `ingest/irs_soi/README.md`).

**nccs_990_core** (NCCS Core raw + Unified BMF bridge -> S3 -> benchmark county CSVs): run **01 -> 06**.
```bash
python python/ingest/nccs_990_core/01_discover_core_release.py
python python/ingest/nccs_990_core/02_download_core_release.py
python python/ingest/nccs_990_core/03_upload_core_release_to_s3.py
python python/ingest/nccs_990_core/04_verify_core_source_local_s3.py
python python/ingest/nccs_990_core/05_filter_core_to_benchmark_local.py
python python/ingest/nccs_990_core/06_upload_filtered_core_to_s3.py
```
Or use the runner: `python python/run_nccs_990_core.py` (see `ingest/nccs_990_core/README.md`).
