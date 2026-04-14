# NCCS efile 990 Pipeline

Pipeline for published NCCS efile table downloads beginning with tax year `2022`.

This path:
- discovers all officially published efile tax years with the required tables
- downloads the required raw efile CSV tables
- uploads raw assets and metadata to S3 Bronze
- verifies source/local/S3 byte parity
- builds one compact benchmark-filtered annual parquet per efile tax year
- uploads the annualized benchmark outputs to S3 Silver
- compares annualized efile outputs against the current NCCS Core benchmark years
- builds official `2022-2024` efile analysis outputs and metadata
- uploads the official analysis outputs to S3 Silver analysis prefixes

Filter-first combine contract:
- HEADER rows are filtered to benchmark geography before any wide table combine happens
- filing ranking is applied inside that already-admitted header subset
- SUMMARY and Schedule A are joined only for the retained benchmark-selected filings

## Run order

Run from repo root.

```bash
python python/ingest/nccs_efile/01_discover_efile_release.py
python python/ingest/nccs_efile/02_download_efile_release.py
python python/ingest/nccs_efile/03_upload_efile_release_to_s3.py
python python/ingest/nccs_efile/04_verify_efile_source_local_s3.py
python python/ingest/nccs_efile/05_build_efile_benchmark_local.py
python python/ingest/nccs_efile/06_upload_filtered_efile_to_s3.py
python python/ingest/nccs_efile/07_compare_efile_vs_core_local.py
python python/ingest/nccs_efile/08_extract_analysis_variables_local.py
python python/ingest/nccs_efile/09_upload_analysis_outputs.py
```

Or run everything:

```bash
python python/run_nccs_efile.py
```

## Local layout

- `01_data/raw/nccs_efile/raw/tax_year=YYYY/table=<table_name>/<official filename>`
- `01_data/raw/nccs_efile/metadata/latest_release.json`
- `01_data/raw/nccs_efile/metadata/dataset_efile.html`
- `01_data/raw/nccs_efile/metadata/catalog_efile.html`
- `01_data/raw/nccs_efile/metadata/release_manifest_start_year=2022.csv`
- `01_data/raw/nccs_efile/metadata/size_verification_start_year=2022.csv`
- `01_data/staging/nccs_efile/tax_year=YYYY/nccs_efile_benchmark_tax_year=YYYY.parquet`
- `01_data/staging/nccs_efile/tax_year=YYYY/filter_manifest_tax_year=YYYY.csv`
- `01_data/staging/nccs_efile/comparison/tax_year=YYYY/efile_vs_core_row_counts_tax_year=YYYY.csv`
- `01_data/staging/nccs_efile/comparison/tax_year=YYYY/efile_vs_core_fill_rates_tax_year=YYYY.csv`
- `01_data/staging/nccs_efile/comparison/tax_year=YYYY/efile_vs_core_conflicts_tax_year=YYYY.csv`
- `01_data/staging/nccs_efile/comparison/tax_year=YYYY/efile_vs_core_summary_tax_year=YYYY.json`
- `01_data/staging/nccs_efile/nccs_efile_analysis_variables.parquet`
- `01_data/staging/nccs_efile/nccs_efile_analysis_geography_metrics.parquet`
- `01_data/raw/nccs_efile/metadata/nccs_efile_analysis_variable_coverage.csv`
- `docs/analysis/nccs_efile_analysis_variable_mapping.md`
- `docs/data_processing/nccs_efile_pipeline.md`

## S3 layout

- `bronze/nccs_efile/raw/tax_year=YYYY/table=<table_name>/<official filename>`
- `bronze/nccs_efile/metadata/<metadata files>`
- `silver/nccs_efile/tax_year=YYYY/nccs_efile_benchmark_tax_year=YYYY.parquet`
- `silver/nccs_efile/tax_year=YYYY/filter_manifest_tax_year=YYYY.csv`
- `silver/nccs_efile/comparison/tax_year=YYYY/<comparison artifacts>`
- `silver/nccs_efile/analysis/nccs_efile_analysis_variables.parquet`
- `silver/nccs_efile/analysis/nccs_efile_analysis_geography_metrics.parquet`
- `silver/nccs_efile/analysis/metadata/<coverage, mapping, pipeline doc>`

## Build notes

The annualized benchmark output keeps one selected filing per `EIN + tax_year`.

Selection order inside the benchmark-admitted filing subset:
1. most recent nonblank return timestamp
2. amended return before non-amended when timestamps tie
3. most recent nonblank build timestamp
4. URL ascending for a stable final tie-break

Benchmark geography is derived from the efile organization ZIP through the same ZIP-to-county and GEOID reference files used elsewhere in the repo.
That geography admission happens on HEADER rows before SUMMARY and Schedule A are combined, so downstream consumers never combine the broad national annual rowset directly.

The latest published efile year is marked partial in discovery metadata and carried into the annualized benchmark parquet as `efile_is_partial_year`.
