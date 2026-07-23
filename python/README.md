# python/

Python code for ingestion, transformation, analysis, and export. Active local processing uses the configured data root (normally the project OneDrive location) through environment or repository path helpers.

**Infrastructure status as of 2026-07-23:** the former bucket `swb-321-irs990-teos` was intentionally deleted and must not be recreated. S3 upload, download, and parity-check stages remain in the codebase for historical reproducibility but are inactive unless a different, separately approved target is configured.

- **ingest/** - Pull raw data from public sources into the configured local `01_data/raw/`; retained pipelines also document their former S3 publication stages.
- **transform/** - Orchestrate running SQL staging/curated and writing to OneDrive staging/curated.
- **export/** - Export CSV/Excel and other deliverables to OneDrive `02_outputs/`.
- **utils/** - Shared helpers (paths, logging, DB connection, etc.). No data or credentials in repo.

## Retained ingest run order

Run local/public-source stages from the **repo root**. Commands below that upload to or verify S3 describe the former end-to-end workflow and must not be run against the deleted bucket.

**irs_990** (IRS TEOS 990 XML -> S3 -> staging): run **01 -> 02 -> 03**.
```bash
python python/ingest/irs_990/01_upload_irs_990_index_to_s3.py
python python/ingest/irs_990/02_upload_irs_990_zips_to_s3.py
python python/ingest/irs_990/03_parse_irs_990_zips_to_staging.py
```
Or use the runner: `python python/run_irs_990.py` (see `ingest/irs_990/README.md` for options).

**givingtuesday_990**: run **01 -> 02 -> 03 -> 04**. See `ingest/givingtuesday_990/api/README.md`
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
Or use the runner: `python python/run_irs_soi.py` (see `ingest/irs_soi/README.md`).

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

**nccs_990_postcard** (NCCS 990-N monthly snapshots -> S3 -> combined benchmark county CSV): run **01 -> 06**.
```bash
python python/ingest/nccs_990_postcard/01_discover_postcard_release.py
python python/ingest/nccs_990_postcard/02_download_postcard_release.py
python python/ingest/nccs_990_postcard/03_upload_postcard_release_to_s3.py
python python/ingest/nccs_990_postcard/04_verify_postcard_source_local_s3.py
python python/ingest/nccs_990_postcard/05_filter_postcard_to_benchmark_local.py
python python/ingest/nccs_990_postcard/06_upload_filtered_postcard_to_s3.py
```
Or use the runner: `python python/run_nccs_990_postcard.py` (see `ingest/nccs_990_postcard/README.md`).

**nccs_bmf** (NCCS BMF yearly 2022-present -> S3 -> benchmark county Parquets): run **01 -> 06**.
```bash
python python/ingest/nccs_bmf/01_discover_bmf_release.py
python python/ingest/nccs_bmf/02_download_bmf_release.py
python python/ingest/nccs_bmf/03_upload_bmf_release_to_s3.py
python python/ingest/nccs_bmf/04_verify_bmf_source_local_s3.py
python python/ingest/nccs_bmf/05_filter_bmf_to_benchmark_local.py
python python/ingest/nccs_bmf/06_upload_filtered_bmf_to_s3.py
```
Or use the runner: `python python/run_nccs_bmf.py` (see `ingest/nccs_bmf/README.md`).

**combined_990** (source-preserving union of already-filtered GivingTuesday + NCCS postcard + NCCS Core + NCCS BMF): run **01 -> 03**.
```bash
python python/ingest/combined_990/01_build_combined_filtered_local.py
python python/ingest/combined_990/02_upload_combined_filtered_to_s3.py
python python/ingest/combined_990/03_verify_combined_filtered_local_s3.py
```
Or use the runner: `python python/run_combined_990.py` (see `ingest/combined_990/README.md`).
