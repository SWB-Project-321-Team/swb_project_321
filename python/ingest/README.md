# python/ingest/

Scripts that pull raw data from sources and write to OneDrive `01_data/raw/`.

- IRS 990 extracts, Giving Tuesday data, Census/ACS, or other APIs/files.
- Output goes to the appropriate subfolder under `01_data/raw/` (for example `irs_990/`, `census_acs/`).
- No data or credentials in this repo - paths and secrets come from environment or config.

## location_processing/ - Census location and GEOID scripts

- `fetch_location_data.py` - Optional utility; fetches US county GEOIDs (5-digit FIPS) from Census 2020 Decennial API. Writes CSV with GEOID, County, State, State_FIPS, County_FIPS, NAME_full, City, ZIPs.
- `01_fetch_geoid_reference.py` - Writes `GEOID_reference.csv` (18 benchmark counties) plus `geoid_zip_codes.csv`.
- `02_fetch_zip_to_county.py` - Downloads Census ZCTA-county (or normalizes `--local` CSV) and writes `zip_to_county_fips.csv` (ZIP, FIPS).
- `03_build_zip_list_for_geoids.py` - Builds `01_data/reference/zip_codes_in_benchmark_regions.csv` from GEOID reference plus ZIP-to-county mapping.

Run from repo root, for example:
`python python/ingest/location_processing/01_fetch_geoid_reference.py`

## 990_givingtuesday/datamart/ - DataMart-first 990 pipeline (run in order)

Scripts to fetch, verify, merge, and filter 990 DataMart files for benchmark analysis.
Run from repo root. See `990_givingtuesday/datamart/README.md`.
This pipeline runs independently of `990_givingtuesday/api` and `990_irs`; it only reads
`reference/GEOID_reference.csv` and `reference/zip_to_county_fips.csv` from `location_processing`.

| Step | Script | Output |
|------|--------|--------|
| 01 | `990_givingtuesday/datamart/01_fetch_datamart_catalog.py` | raw DataMart catalog JSON |
| 02 | `990_givingtuesday/datamart/02_export_data_dictionary.py` | metadata CSV/MD + fields CSV/MD |
| 03 | `990_givingtuesday/datamart/03_download_raw_datamarts.py` | full raw DataMart CSVs (Combined + Basic forms) |
| 04 | `990_givingtuesday/datamart/04_upload_bronze_raw_and_dictionary.py` | Bronze raw + metadata upload |
| 05 | `990_givingtuesday/datamart/05_verify_source_local_s3_sizes.py` | strict size verification report |
| 06 | `990_givingtuesday/datamart/06_build_basic_allforms_unfiltered.py` | unfiltered basic-allforms parquet |
| 07 | `990_givingtuesday/datamart/07_build_basic_plus_combined_unfiltered.py` | unfiltered basic+combined parquet |
| 08 | `990_givingtuesday/datamart/08_upload_bronze_unfiltered_outputs.py` | Bronze unfiltered parquet upload |
| 09 | `990_givingtuesday/datamart/09_filter_to_benchmark_and_write_silver_local.py` | local Silver filtered parquet |
| 10 | `990_givingtuesday/datamart/10_upload_silver_filtered.py` | Silver upload |
| Run all | `990_givingtuesday/datamart/run_990_datamart_pipeline.py` | orchestrates steps 01-10 |

## 990_givingtuesday/api/ - Legacy per-EIN API pipeline

This path is retained for reference/legacy support but is not the primary DataMart-first flow.

## 990_irs/ - IRS TEOS 990 XML -> S3 (2021-present)

Scripts that stream IRS Form 990 series XML from IRS TEOS downloads directly to the project AWS bucket.
Run 01 -> 02 -> 03 (optionally 04 to merge silver parts). See `990_irs/README.md`.

| Step | Script | Output |
|------|--------|--------|
| 01 | `990_irs/01_upload_irs_990_index_to_s3.py` | S3: `{prefix}/index/year={YEAR}/index_{YEAR}.csv` |
| 02 | `990_irs/02_upload_irs_990_zips_to_s3.py` | S3: `{prefix}/zips/year={YEAR}/{YEAR}_TEOS_XML_*.zip` |
| 03 | `990_irs/03_parse_irs_990_zips_to_staging.py` | `01_data/staging/filing/irs_990_filings.parquet` (or CSV fallback); optional `--output-parts-dir` |
| 04 | `990_irs/04_merge_990_parts_to_staging.py` | merges silver part Parquets to single staging table |

Default bucket: `swb-321-irs990-teos`, prefix: `bronze/irs990/teos_xml`.

---

## Output file checklist (01_data paths)

| Script | Output | Columns / contents | Sanity check |
|--------|--------|--------------------|--------------|
| location_processing/02_fetch_zip_to_county | reference/zip_to_county_fips.csv | ZIP (5-digit), FIPS (5-digit county); one row per ZIP | ~33.8k rows |
| 990_givingtuesday/datamart/01_fetch_datamart_catalog | raw/givingtuesday_990/datamarts/metadata/datamart_catalog_raw.json | Full catalog payload from public API | Non-empty documents array |
| 990_givingtuesday/datamart/02_export_data_dictionary | raw/givingtuesday_990/datamarts/metadata/datamart_catalog.csv/.md + datamart_fields.csv/.md | Flattened dataset metadata and full field dictionary | Required dataset rows present; field rows > 0 |
| 990_givingtuesday/datamart/03_download_raw_datamarts | raw/givingtuesday_990/datamarts/raw/*.csv | Full unfiltered raw CSVs (Combined + Basic forms) | Local bytes == source Content-Length |
| 990_givingtuesday/datamart/05_verify_source_local_s3_sizes | raw/givingtuesday_990/datamarts/metadata/size_verification_report.csv | source/local/S3 bytes for required datasets | Every required row has size_match=TRUE |
| 990_givingtuesday/datamart/06_build_basic_allforms_unfiltered | staging/filing/givingtuesday_990_basic_allforms_unfiltered.parquet | Union of Basic 990 + 990EZ + 990PF with normalized keys and provenance | Non-zero rows; expected key columns present |
| 990_givingtuesday/datamart/07_build_basic_plus_combined_unfiltered | staging/filing/givingtuesday_990_basic_plus_combined_unfiltered.parquet | Combined backbone enriched from Basic by key, unmatched Basic retained | Non-zero rows; fill/provenance columns present |
| 990_givingtuesday/datamart/09_filter_to_benchmark_and_write_silver_local | staging/filing/givingtuesday_990_filings_benchmark.parquet | Benchmark-filtered filing table with county_fips + region | Rows reduced vs unfiltered and region populated |
| 990_givingtuesday/api/02_fetch_bmf | raw/irs_bmf/eo_<state>.csv | EIN, NAME, STREET, CITY, STATE, ZIP, etc. | One CSV per state (default: sd, mn, mt, az) |
| 990_givingtuesday/api/03_build_ein_list | reference/eins_in_benchmark_regions.csv | EIN (9-digit, leading zeros) | One EIN per row |
| 990_givingtuesday/api/04_fetch_990_givingtuesday | raw/givingtuesday_990/api_responses/ein_*.json | statusCode, body.query, body.no_results, body.results | One JSON per EIN |
| 990_givingtuesday/api/04_fetch_990_givingtuesday | raw/givingtuesday_990/990_basic120_combined.csv | Basic 120 fields, one row per EIN x TAXYEAR (2021+) | Written only after full run completes |
| location_processing/fetch_location_data | data/locations.csv (default) | GEOID, County, State, State_name, State_FIPS, County_FIPS, NAME_full, City, ZIPs | State_name and ZIPs populated |
| location_processing/01_fetch_geoid_reference | reference/GEOID_reference.csv, geoid_zip_codes.csv | GEOID, Cluster_ID, Cluster_name, ZIPs; GEOID, ZIP | 18 benchmark rows |
| location_processing/03_build_zip_list_for_geoids | reference/zip_codes_in_benchmark_regions.csv | ZIP, GEOID, Region | One row per ZIP in 18 counties |
| 990_irs/03_parse_irs_990_zips_to_staging | staging/filing/irs_990_filings.parquet | ein, tax_year, form_type, revenue, expenses, assets, source_file, source_zip, region | Parquet or CSV; geography filter + region |
| 990_irs/04_merge_990_parts_to_staging | staging/filing/irs_990_filings.parquet | same as 03 | Merge from silver part Parquets; geography filter + region |