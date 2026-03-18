# 990_givingtuesday/datamart

DataMart-first pipeline for SWB 321:
- uses **Combined Forms Datamart** + **Basic Fields** (990, 990EZ, 990PF)
- stores full raw files locally
- uploads full raw + dictionary to Bronze before filtering
- writes benchmark-filtered file to Silver
- enforces strict source/local/S3 byte-size parity for required raw files
- uses **Polars lazy** processing in steps 06/07/09 to reduce memory pressure and improve throughput

## Independence Contract

- This pipeline is self-contained in `python/ingest/990_givingtuesday/datamart/`.
- It does **not** read code outputs from `990_givingtuesday/api` or `990_irs` pipelines.
- The only external pipeline inputs are location-reference CSVs:
  - `01_data/reference/GEOID_reference.csv`
  - `01_data/reference/zip_to_county_fips.csv`
- No `GEOID_reference.xlsx` fallback is used in this pipeline.

## Data Sources

- DataMarts page: <https://990data.givingtuesday.org/datamarts/>
- Public catalog API: `https://grantstory-api.gtdata.org/v2/public/678d25291eceb5d121099200/items?limit=200`

## Script Order

Run from repo root:

```powershell
python python/ingest/990_givingtuesday/datamart/01_fetch_datamart_catalog.py
python python/ingest/990_givingtuesday/datamart/02_export_data_dictionary.py
python python/ingest/990_givingtuesday/datamart/03_download_raw_datamarts.py
python python/ingest/990_givingtuesday/datamart/04_upload_bronze_raw_and_dictionary.py
python python/ingest/990_givingtuesday/datamart/05_verify_source_local_s3_sizes.py
python python/ingest/990_givingtuesday/datamart/06_build_basic_allforms_unfiltered.py
python python/ingest/990_givingtuesday/datamart/07_build_basic_plus_combined_unfiltered.py
python python/ingest/990_givingtuesday/datamart/08_upload_bronze_unfiltered_outputs.py
python python/ingest/990_givingtuesday/datamart/09_filter_to_benchmark_and_write_silver_local.py
python python/ingest/990_givingtuesday/datamart/10_upload_silver_filtered.py
```

Or run orchestrator:

```powershell
python python/ingest/990_givingtuesday/datamart/run_990_datamart_pipeline.py
```

Useful orchestrator options:

```powershell
python python/ingest/990_givingtuesday/datamart/run_990_datamart_pipeline.py --start-step 1 --end-step 5 --overwrite-download --refresh-catalog
python python/ingest/990_givingtuesday/datamart/run_990_datamart_pipeline.py --bucket swb-321-irs990-teos --region us-east-2 --tax-year-min 2021
```

## Local Outputs

- Raw files: `01_data/raw/givingtuesday_990/datamarts/raw/`
- Dictionary metadata: `01_data/raw/givingtuesday_990/datamarts/metadata/`
- Unfiltered parquet:
  - `01_data/staging/filing/givingtuesday_990_basic_allforms_unfiltered.parquet`
  - `01_data/staging/filing/givingtuesday_990_basic_plus_combined_unfiltered.parquet`
- Filtered parquet:
  - `01_data/staging/filing/givingtuesday_990_filings_benchmark.parquet`

## S3 Layout

- Bronze raw: `bronze/givingtuesday_990/datamarts/raw/`
- Bronze metadata: `bronze/givingtuesday_990/datamarts/metadata/`
- Bronze unfiltered: `bronze/givingtuesday_990/datamarts/unfiltered/`
- Silver filtered: `silver/givingtuesday_990/filing/`

Default bucket: `swb-321-irs990-teos`
