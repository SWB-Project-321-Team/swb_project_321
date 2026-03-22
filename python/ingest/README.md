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

## irs_soi/ - IRS SOI county data -> local raw -> S3 -> benchmark filter

Scripts that discover the latest IRS SOI county release, preserve the official raw county files
locally, upload them to S3, and build separate benchmark-county filtered CSVs using the same
benchmark county scope as `990_irs`.

| Step | Script | Output |
|------|--------|--------|
| 01 | `irs_soi/01_discover_county_release.py` | metadata JSON with latest release and asset URLs |
| 02 | `irs_soi/02_download_county_release.py` | raw county CSVs + users guide under `raw/irs_soi/county/` |
| 03 | `irs_soi/03_upload_county_release_to_s3.py` | Bronze raw + metadata upload |
| 04 | `irs_soi/04_verify_county_release_source_local_s3.py` | raw source/local/S3 size verification report |
| 05 | `irs_soi/05_filter_county_release_to_benchmark_local.py` | local benchmark-filtered county CSVs |
| 06 | `irs_soi/06_upload_filtered_county_release_to_s3.py` | Silver filtered CSV upload |
| Run all | `../run_irs_soi_county.py` | orchestrates steps 01-06 |

Default bucket: `swb-321-irs990-teos`, raw prefix: `bronze/irs_soi/county/raw`, filtered prefix: `silver/irs_soi/county`.

## nccs_990_core/ - NCCS Core Series + Unified BMF -> local raw -> S3 -> benchmark filter

Scripts that discover the latest common NCCS Core year across the required PZ/PC/PF families,
download the official Core CSVs plus Unified BMF bridge files, upload them to S3, verify
source/local/S3 size parity, and build separate benchmark-county filtered CSVs.

| Step | Script | Output |
|------|--------|--------|
| 01 | `nccs_990_core/01_discover_core_release.py` | metadata JSON with latest common year, asset URLs, and benchmark states |
| 02 | `nccs_990_core/02_download_core_release.py` | Core CSVs, Unified BMF bridge files, and dictionaries under `raw/nccs_990/core/` |
| 03 | `nccs_990_core/03_upload_core_release_to_s3.py` | Bronze raw + metadata upload |
| 04 | `nccs_990_core/04_verify_core_source_local_s3.py` | raw source/local/S3 size verification report |
| 05 | `nccs_990_core/05_filter_core_to_benchmark_local.py` | local benchmark-filtered Core CSVs |
| 06 | `nccs_990_core/06_upload_filtered_core_to_s3.py` | Silver filtered CSV upload |
| Run all | `../run_nccs_990_core.py` | orchestrates steps 01-06 |

Default bucket: `swb-321-irs990-teos`, raw prefix: `bronze/nccs_990/core/raw`, bridge prefix: `bronze/nccs_990/core/bridge_bmf`, filtered prefix: `silver/nccs_990/core`.

## nccs_990_postcard/ - NCCS 990-N e-Postcard monthly snapshots -> local raw -> S3 -> annual benchmark filter

Scripts that discover the latest NCCS e-Postcard snapshot year, download all available monthly
snapshots in that year, upload them to S3, verify source/local/S3 size parity, and build one
combined benchmark-county annual derivative using postcard ZIP fields.

| Step | Script | Output |
|------|--------|--------|
| 01 | `nccs_990_postcard/01_discover_postcard_release.py` | metadata JSON with latest snapshot year, latest month, and available monthly asset URLs |
| 02 | `nccs_990_postcard/02_download_postcard_release.py` | monthly postcard CSVs under `raw/nccs_990/postcard/` |
| 03 | `nccs_990_postcard/03_upload_postcard_release_to_s3.py` | Bronze raw + metadata upload |
| 04 | `nccs_990_postcard/04_verify_postcard_source_local_s3.py` | raw source/local/S3 size verification report |
| 05 | `nccs_990_postcard/05_filter_postcard_to_benchmark_local.py` | combined local benchmark-filtered postcard CSV |
| 06 | `nccs_990_postcard/06_upload_filtered_postcard_to_s3.py` | Silver filtered CSV upload |
| Run all | `../run_nccs_990_postcard.py` | orchestrates steps 01-06 |

Default bucket: `swb-321-irs990-teos`, raw prefix: `bronze/nccs_990/postcard/raw`, filtered prefix: `silver/nccs_990/postcard`.

## nccs_bmf/ - NCCS BMF 2022-present yearly snapshots -> local raw -> S3 -> benchmark filter

Scripts that discover one representative NCCS BMF snapshot per year from `2022-present`,
download the selected raw files, upload them to S3, verify source/local/S3 size parity, and
write one benchmark-county filtered Parquet per year.

| Step | Script | Output |
|------|--------|--------|
| 01 | `nccs_bmf/01_discover_bmf_release.py` | metadata JSON with selected yearly BMF asset URLs |
| 02 | `nccs_bmf/02_download_bmf_release.py` | yearly raw BMF CSVs under `raw/nccs_bmf/` |
| 03 | `nccs_bmf/03_upload_bmf_release_to_s3.py` | Bronze raw + metadata upload |
| 04 | `nccs_bmf/04_verify_bmf_source_local_s3.py` | raw source/local/S3 size verification report |
| 05 | `nccs_bmf/05_filter_bmf_to_benchmark_local.py` | local benchmark-filtered yearly BMF Parquets |
| 06 | `nccs_bmf/06_upload_filtered_bmf_to_s3.py` | Silver filtered yearly BMF Parquet upload |
| Run all | `../run_nccs_bmf.py` | orchestrates steps 01-06 |

Default bucket: `swb-321-irs990-teos`, raw prefix: `bronze/nccs_bmf/raw`, filtered prefix: `silver/nccs_bmf`.

## combined_990/ - Combined filtered source-preserving union table

Scripts that read only the existing filtered outputs from GivingTuesday, NCCS postcard, NCCS Core
charities/nonprofits, and NCCS BMF yearly benchmark files, then build one combined
source-preserving union table plus diagnostics and upload those artifacts to Silver.

| Step | Script | Output |
|------|--------|--------|
| 01 | `combined_990/01_build_combined_filtered_local.py` | combined parquet + metadata + diagnostics under `staging/combined_990/` |
| 02 | `combined_990/02_upload_combined_filtered_to_s3.py` | Silver combined parquet + metadata upload |
| 03 | `combined_990/03_verify_combined_filtered_local_s3.py` | local/S3 size verification report |
| Run all | `../run_combined_990.py` | orchestrates steps 01-03 |

Default bucket: `swb-321-irs990-teos`, silver prefix: `silver/combined_990`.

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
| irs_soi/02_download_county_release | raw/irs_soi/county/raw/tax_year=YYYY/*.csv/.docx | official IRS county AGI CSV, no-AGI CSV, and users guide | Local bytes == source Content-Length |
| irs_soi/04_verify_county_release_source_local_s3 | raw/irs_soi/county/metadata/size_verification_tax_year=YYYY.csv | source/local/S3 bytes for raw county assets | Every row has size_match=TRUE |
| irs_soi/05_filter_county_release_to_benchmark_local | staging/irs_soi/tax_year=YYYY/irs_soi_county_benchmark_*.csv | benchmark-county subset with county_fips + region | Rows reduced vs raw and region populated |
| nccs_990_core/02_download_core_release | raw/nccs_990/core/raw/year=YYYY/*.csv + raw/nccs_990/core/bridge_bmf/state=XX/*.csv | official NCCS Core CSVs, dictionaries, and Unified BMF bridge files | Local bytes == source Content-Length |
| nccs_990_core/04_verify_core_source_local_s3 | raw/nccs_990/core/metadata/size_verification_year=YYYY.csv | source/local/S3 bytes for Core raw assets | Every required row has size_match=TRUE |
| nccs_990_core/05_filter_core_to_benchmark_local | staging/nccs_990/core/year=YYYY/*__benchmark.csv | benchmark-county subset with county_fips + region + match source | Rows reduced vs raw and region populated |
| nccs_990_postcard/02_download_postcard_release | raw/nccs_990/postcard/raw/snapshot_year=YYYY/snapshot_month=YYYY-MM/*.csv | official NCCS postcard monthly snapshot CSVs | Local bytes == source Content-Length |
| nccs_990_postcard/04_verify_postcard_source_local_s3 | raw/nccs_990/postcard/metadata/size_verification_snapshot_year=YYYY.csv | source/local/S3 bytes for postcard raw assets | Every required row has size_match=TRUE |
| nccs_990_postcard/05_filter_postcard_to_benchmark_local | staging/nccs_990/postcard/snapshot_year=YYYY/nccs_990_postcard_benchmark_snapshot_year=YYYY.csv | combined benchmark-county postcard output with county_fips + region + match source | Rows reduced vs raw and region populated |
| nccs_bmf/02_download_bmf_release | raw/nccs_bmf/raw/year=YYYY/*.csv | selected yearly NCCS BMF raw CSVs | Local bytes == source Content-Length |
| nccs_bmf/04_verify_bmf_source_local_s3 | raw/nccs_bmf/metadata/size_verification_start_year=2022.csv | source/local/S3 bytes for selected raw BMF assets | Every required row has size_match=TRUE |
| nccs_bmf/05_filter_bmf_to_benchmark_local | staging/nccs_bmf/year=YYYY/nccs_bmf_benchmark_year=YYYY.parquet | benchmark-county subset of each yearly BMF snapshot with county_fips + region + snapshot fields | Rows reduced vs raw and region populated |
| combined_990/01_build_combined_filtered_local | staging/combined_990/combined_990_filtered_source_union.parquet | source-preserving union table plus row provenance, harmonized fields, per-field provenance, overlay flags, diagnostics | Non-zero rows and schema metadata written |
