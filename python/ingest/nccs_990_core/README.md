# NCCS Core 990 Pipeline

Pipeline for official NCCS Core Series 990 data plus Unified BMF bridge geography.

This path:
- discovers the latest common complete Core year across the required PZ/PC/PF files
- downloads the selected Core CSVs, dictionaries, and benchmark-state Unified BMF files
- uploads raw assets and metadata to S3 Bronze
- verifies source/local/S3 byte parity
- builds benchmark-filtered derivatives using Unified BMF as the geography bridge
- uploads the filtered outputs to S3 Silver

## Run order

Run from repo root.

```bash
python python/ingest/nccs_990_core/01_discover_core_release.py
python python/ingest/nccs_990_core/02_download_core_release.py
python python/ingest/nccs_990_core/03_upload_core_release_to_s3.py
python python/ingest/nccs_990_core/04_verify_core_source_local_s3.py
python python/ingest/nccs_990_core/05_filter_core_to_benchmark_local.py
python python/ingest/nccs_990_core/06_upload_filtered_core_to_s3.py
```

Or run everything:

```bash
python python/run_nccs_990_core.py
```

## Local layout

- `01_data/raw/nccs_990/core/raw/year=YYYY/<official core filename>`
- `01_data/raw/nccs_990/core/bridge_bmf/state=XX/<official bmf filename>`
- `01_data/raw/nccs_990/core/metadata/latest_release.json`
- `01_data/raw/nccs_990/core/metadata/catalog_core.html`
- `01_data/raw/nccs_990/core/metadata/catalog_bmf.html`
- `01_data/raw/nccs_990/core/metadata/release_manifest_year=YYYY.csv`
- `01_data/raw/nccs_990/core/metadata/size_verification_year=YYYY.csv`
- `01_data/staging/nccs_990/core/year=YYYY/<original_stem>__benchmark.csv`
- `01_data/staging/nccs_990/core/year=YYYY/filter_manifest_year=YYYY.csv`

## S3 layout

- `bronze/nccs_990/core/raw/year=YYYY/<official core filename>`
- `bronze/nccs_990/core/bridge_bmf/state=XX/<official bmf filename>`
- `bronze/nccs_990/core/metadata/<metadata files>`
- `silver/nccs_990/core/year=YYYY/<original_stem>__benchmark.csv`
- `silver/nccs_990/core/year=YYYY/filter_manifest_year=YYYY.csv`

## Filtering notes

The Core files are filtered to the project benchmark counties using Unified BMF as a bridge.

Bridge precedence:
1. derive `county_fips` from `CENSUS_BLOCK_FIPS[:5]`
2. fallback to `F990_ORG_ADDR_ZIP -> zip_to_county_fips.csv` only when block FIPS is blank

Filtered outputs preserve the original Core columns and append:
- `county_fips`
- `region`
- `benchmark_match_source`
- `is_benchmark_county`

This uses current Unified BMF geography as the best available bridge for the 2022 Core files.
It should be treated as a current-address approximation, not a strict historical address record.
