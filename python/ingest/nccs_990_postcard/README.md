# NCCS e-Postcard 990 Pipeline

Pipeline for official NCCS 990-N e-Postcard monthly snapshots.

This path:
- discovers the latest snapshot year and all available months in that year
- downloads the official monthly postcard CSV snapshots
- uploads raw assets and metadata to S3 Bronze
- verifies source/local/S3 byte parity
- builds one combined benchmark-filtered annual derivative from the monthly snapshots
- derives a second benchmark postcard artifact restricted to `tax_year >= 2022`
- uploads both filtered outputs to S3 Silver
- builds official `2022-2024` postcard analysis outputs and metadata
- uploads the official analysis outputs to S3 Silver analysis prefixes

## Run order

Run from repo root.

```bash
python python/ingest/nccs_990_postcard/01_discover_postcard_release.py
python python/ingest/nccs_990_postcard/02_download_postcard_release.py
python python/ingest/nccs_990_postcard/03_upload_postcard_release_to_s3.py
python python/ingest/nccs_990_postcard/04_verify_postcard_source_local_s3.py
python python/ingest/nccs_990_postcard/05_filter_postcard_to_benchmark_local.py
python python/ingest/nccs_990_postcard/06_upload_filtered_postcard_to_s3.py
python python/ingest/nccs_990_postcard/07_extract_analysis_variables_local.py
python python/ingest/nccs_990_postcard/08_upload_analysis_outputs.py
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
- `01_data/staging/nccs_990/postcard/snapshot_year=YYYY/nccs_990_postcard_benchmark_tax_year_start=2022_snapshot_year=YYYY.csv`
- `01_data/staging/nccs_990/postcard/snapshot_year=YYYY/filter_manifest_snapshot_year=YYYY_tax_year_start=2022.csv`
- `01_data/staging/nccs_990/postcard/nccs_990_postcard_analysis_variables.parquet`
- `01_data/staging/nccs_990/postcard/nccs_990_postcard_analysis_geography_metrics.parquet`
- `01_data/raw/nccs_990/postcard/metadata/nccs_990_postcard_analysis_variable_coverage.csv`
- `docs/final_preprocessing_docs/technical_docs/analysis_variable_mappings/nccs_990_postcard_analysis_variable_mapping.md`
- `docs/final_preprocessing_docs/technical_docs/pipeline_docs/nccs_990_postcard_pipeline.md`

## S3 layout

- `bronze/nccs_990/postcard/raw/snapshot_year=YYYY/snapshot_month=YYYY-MM/<official filename>`
- `bronze/nccs_990/postcard/metadata/<metadata files>`
- `silver/nccs_990/postcard/snapshot_year=YYYY/nccs_990_postcard_benchmark_snapshot_year=YYYY.csv`
- `silver/nccs_990/postcard/snapshot_year=YYYY/filter_manifest_snapshot_year=YYYY.csv`
- `silver/nccs_990/postcard/snapshot_year=YYYY/nccs_990_postcard_benchmark_tax_year_start=2022_snapshot_year=YYYY.csv`
- `silver/nccs_990/postcard/snapshot_year=YYYY/filter_manifest_snapshot_year=YYYY_tax_year_start=2022.csv`
- `silver/nccs_990/postcard/analysis/nccs_990_postcard_analysis_variables.parquet`
- `silver/nccs_990/postcard/analysis/nccs_990_postcard_analysis_geography_metrics.parquet`
- `silver/nccs_990/postcard/analysis/documentation/nccs_990_postcard_pipeline.md`
- `silver/nccs_990/postcard/analysis/variable_mappings/nccs_990_postcard_analysis_variable_mapping.md`
- `silver/nccs_990/postcard/analysis/quality/coverage/nccs_990_postcard_analysis_variable_coverage.csv`

## Filtering notes

The postcard files are filtered to the project benchmark counties using postcard ZIP fields.
The monthly combine is filter-first: each monthly file is chunk-filtered to benchmark rows before
those retained rows are concatenated and deduped into the annual derivative.

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

The final annual derivative keeps the latest available monthly snapshot row per EIN, but only from
the already benchmark-filtered retained rows.

The secondary derivative keeps only rows where `tax_year >= 2022`.

In this repo, postcard "`2022-present`" means:
- use the latest accessible published postcard snapshot year
- keep only rows whose filing `tax_year >= 2022`
- do not assume separate public postcard snapshot-year archives exist for every year since 2022
