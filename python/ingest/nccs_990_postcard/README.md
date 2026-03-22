# NCCS e-Postcard 990 Pipeline

Pipeline for official NCCS 990-N e-Postcard monthly snapshots.

This path:
- discovers the latest snapshot year and all available months in that year
- downloads the official monthly postcard CSV snapshots
- uploads raw assets and metadata to S3 Bronze
- verifies source/local/S3 byte parity
- builds one combined benchmark-filtered annual derivative from the monthly snapshots
- uploads the filtered output to S3 Silver

## Run order

Run from repo root.

```bash
python python/ingest/nccs_990_postcard/01_discover_postcard_release.py
python python/ingest/nccs_990_postcard/02_download_postcard_release.py
python python/ingest/nccs_990_postcard/03_upload_postcard_release_to_s3.py
python python/ingest/nccs_990_postcard/04_verify_postcard_source_local_s3.py
python python/ingest/nccs_990_postcard/05_filter_postcard_to_benchmark_local.py
python python/ingest/nccs_990_postcard/06_upload_filtered_postcard_to_s3.py
```

Or run everything:

```bash
python python/run_nccs_990_postcard.py
```

## Local layout

- `01_data/raw/nccs_990/postcard/raw/snapshot_year=YYYY/snapshot_month=YYYY-MM/<official filename>`
- `01_data/raw/nccs_990/postcard/metadata/latest_release.json`
- `01_data/raw/nccs_990/postcard/metadata/postcard_page.html`
- `01_data/raw/nccs_990/postcard/metadata/release_manifest_snapshot_year=YYYY.csv`
- `01_data/raw/nccs_990/postcard/metadata/size_verification_snapshot_year=YYYY.csv`
- `01_data/staging/nccs_990/postcard/snapshot_year=YYYY/nccs_990_postcard_benchmark_snapshot_year=YYYY.csv`
- `01_data/staging/nccs_990/postcard/snapshot_year=YYYY/filter_manifest_snapshot_year=YYYY.csv`

## S3 layout

- `bronze/nccs_990/postcard/raw/snapshot_year=YYYY/snapshot_month=YYYY-MM/<official filename>`
- `bronze/nccs_990/postcard/metadata/<metadata files>`
- `silver/nccs_990/postcard/snapshot_year=YYYY/nccs_990_postcard_benchmark_snapshot_year=YYYY.csv`
- `silver/nccs_990/postcard/snapshot_year=YYYY/filter_manifest_snapshot_year=YYYY.csv`

## Filtering notes

The postcard files are filtered to the project benchmark counties using postcard ZIP fields.

Geography precedence:
1. derive `county_fips` from `organization_zip -> zip_to_county_fips.csv`
2. fallback to `officer_zip -> zip_to_county_fips.csv` only when the organization ZIP does not produce a county match

The annual combined output preserves the original postcard columns and appends:
- `county_fips`
- `region`
- `benchmark_match_source`
- `snapshot_month`
- `snapshot_year`
- `is_benchmark_county`

The final annual derivative keeps the latest available monthly snapshot row per EIN.
