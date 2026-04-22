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
Per `secrets/coding_rules/CODING_RULES.md`, combine stages must consume filtered or explicitly
admitted inputs rather than broad national intermediate outputs.

| Step | Script | Output |
|------|--------|--------|
| 01 | `990_givingtuesday/datamart/01_fetch_datamart_catalog.py` | raw DataMart catalog JSON |
| 02 | `990_givingtuesday/datamart/02_export_data_dictionary.py` | metadata CSV/MD + fields CSV/MD |
| 03 | `990_givingtuesday/datamart/03_download_raw_datamarts.py` | full raw DataMart CSVs (Combined + Basic forms) |
| 04 | `990_givingtuesday/datamart/04_upload_bronze_raw_and_dictionary.py` | Bronze raw + metadata upload |
| 05 | `990_givingtuesday/datamart/05_verify_source_local_s3_sizes.py` | strict size verification report |
| 06 | `990_givingtuesday/datamart/06_build_basic_allforms_presilver.py` | admitted Basic pre-Silver parquet |
| 07 | `990_givingtuesday/datamart/07_build_basic_plus_combined_presilver.py` | admitted Basic+Combined pre-Silver parquet |
| 08 | `990_givingtuesday/datamart/08_filter_benchmark_outputs_local.py` | local Silver filtered parquets + manifests |
| 09 | `990_givingtuesday/datamart/09_upload_bronze_presilver_outputs.py` | Bronze pre-Silver parquet upload |
| 10 | `990_givingtuesday/datamart/10_upload_silver_filtered_outputs.py` | Silver filtered parquet + manifest upload |
| 11 | `990_givingtuesday/datamart/11_verify_curated_outputs_local_s3.py` | curated local/S3 parity report + GT artifact manifest |
| 13 | `990_givingtuesday/datamart/13_extract_basic_analysis_variables_local.py` | GT analysis parquet + region metrics + coverage/mapping outputs |
| 14 | `990_givingtuesday/datamart/14_upload_analysis_outputs.py` | GT analysis parquet + metadata upload |
| Run all | `990_givingtuesday/datamart/run_990_datamart_pipeline.py` | orchestrates steps 01-11, 13, and 14 |

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

## irs_bmf/ - IRS EO BMF state extracts -> local raw -> S3 -> benchmark filter -> analysis layer

Scripts that download the IRS EO BMF state CSV extracts, upload and verify the raw files,
filter the state files to benchmark geography before any cross-state combine, and then build
an IRS EO BMF analysis package for tax years `2022-2024` inferred from `TAX_PERIOD`.
This pipeline follows the filter-first combine rule explicitly and keeps the older
`990_irs/00_fetch_bmf.py` and `00b_filter_bmf_to_benchmark_upload_silver.py` scripts as
compatibility wrappers.

| Step | Script | Output |
|------|--------|--------|
| 01 | `irs_bmf/01_fetch_bmf_release.py` | raw state CSVs + raw manifest |
| 02 | `irs_bmf/02_upload_bmf_release_to_s3.py` | Bronze raw + metadata upload |
| 03 | `irs_bmf/03_verify_bmf_source_local_s3.py` | raw source/local/S3 size verification report |
| 04 | `irs_bmf/04_filter_bmf_to_benchmark_local.py` | combined filtered parquet + yearly benchmark parquets + filter manifest |
| 05 | `irs_bmf/05_upload_filtered_bmf_to_s3.py` | Silver filtered output upload |
| 06 | `irs_bmf/06_verify_filtered_bmf_local_s3.py` | filtered local/S3 size verification report |
| 07 | `irs_bmf/07_extract_analysis_variables_local.py` | analysis parquet + geography metrics + field metrics + coverage/mapping docs |
| 08 | `irs_bmf/08_upload_analysis_outputs.py` | Silver analysis output upload |
| Run all | `../run_irs_bmf.py` | orchestrates steps 01-08 |

Default bucket: `swb-321-irs990-teos`, raw prefix: `bronze/irs990/bmf`, filtered prefix: `silver/irs990/bmf`.

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

## nccs_990_core/ - NCCS Core Series + Unified BMF -> local raw -> S3 -> benchmark filter -> Core analysis layer

Scripts that discover the latest common NCCS Core year across the required PZ/PC/PF families,
download the official Core CSVs plus Unified BMF bridge files, upload them to S3, verify
source/local/S3 size parity, build separate benchmark-county filtered CSVs, and then build a
2022-only Core analysis package from those already filtered Core benchmark files.
This pipeline now follows the filter-first combine rule explicitly: the analysis union step
combines only filtered Core benchmark files and does not combine raw Core files directly.

| Step | Script | Output |
|------|--------|--------|
| 01 | `nccs_990_core/01_discover_core_release.py` | metadata JSON with latest common year, asset URLs, and benchmark states |
| 02 | `nccs_990_core/02_download_core_release.py` | Core CSVs, Unified BMF bridge files, and dictionaries under `raw/nccs_990/core/` |
| 03 | `nccs_990_core/03_upload_core_release_to_s3.py` | Bronze raw + metadata upload |
| 04 | `nccs_990_core/04_verify_core_source_local_s3.py` | raw source/local/S3 size verification report |
| 05 | `nccs_990_core/05_filter_core_to_benchmark_local.py` | local benchmark-filtered Core CSVs |
| 06 | `nccs_990_core/06_upload_filtered_core_to_s3.py` | Silver filtered CSV upload |
| 07 | `nccs_990_core/07_extract_analysis_variables_local.py` | Core analysis parquet + geography metrics + coverage + mapping + processing doc |
| 08 | `nccs_990_core/08_upload_analysis_outputs.py` | Silver analysis upload |
| Run all | `../run_nccs_990_core.py` | orchestrates steps 01-08 |

Default bucket: `swb-321-irs990-teos`, raw prefix: `bronze/nccs_990/core/raw`, bridge prefix: `bronze/nccs_990/core/bridge_bmf`, filtered prefix: `silver/nccs_990/core`.

## nccs_efile/ - NCCS efile parsed-return tables -> local raw -> S3 -> annual benchmark filter

Scripts that discover published NCCS efile tax years, download the required parsed-return tables,
upload them to S3, verify source/local/S3 size parity, and build one benchmark-filtered annual
efile parquet per tax year.
This pipeline now follows the filter-first combine rule explicitly: HEADER rows are admitted to
benchmark geography and ranked to one retained filing per EIN-year before SUMMARY and Schedule A
are joined.

| Step | Script | Output |
|------|--------|--------|
| 01 | `nccs_efile/01_discover_efile_release.py` | metadata JSON with selected annual efile assets |
| 02 | `nccs_efile/02_download_efile_release.py` | raw annual efile CSVs under `raw/nccs_efile/` |
| 03 | `nccs_efile/03_upload_efile_release_to_s3.py` | Bronze raw + metadata upload |
| 04 | `nccs_efile/04_verify_efile_source_local_s3.py` | raw source/local/S3 size verification report |
| 05 | `nccs_efile/05_build_efile_benchmark_local.py` | local benchmark-filtered yearly efile Parquets |
| 06 | `nccs_efile/06_upload_filtered_efile_to_s3.py` | Silver filtered yearly efile Parquet upload |
| 07 | `nccs_efile/07_compare_efile_vs_core_local.py` | local benchmark comparison artifacts |
| 08 | `nccs_efile/08_extract_analysis_variables_local.py` | efile analysis outputs + coverage/mapping docs |
| 09 | `nccs_efile/09_upload_analysis_outputs.py` | Silver analysis output upload |
| Run all | `../run_nccs_efile.py` | orchestrates steps 01-09 |

Default bucket: `swb-321-irs990-teos`, raw prefix: `bronze/nccs_efile/raw`, filtered prefix: `silver/nccs_efile`.

## nccs_990_postcard/ - NCCS 990-N e-Postcard monthly snapshots -> local raw -> S3 -> annual benchmark filter

Scripts that discover the latest NCCS e-Postcard snapshot year, download all available monthly
snapshots in that year, upload them to S3, verify source/local/S3 size parity, and build one
combined benchmark-county annual derivative using postcard ZIP fields.
This pipeline is already compliant with the filter-first combine rule: each monthly file is
filtered to benchmark geography in chunks before the retained rows are concatenated and deduped.

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
This pipeline is already compliant with the filter-first combine rule: raw yearly files are
filtered to benchmark geography first, and downstream consumers combine only those filtered
artifacts.

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
| 990_givingtuesday/datamart/06_build_basic_allforms_presilver | staging/filing/givingtuesday_990_basic_allforms_presilver.parquet | Basic 990 + 990EZ + 990PF union restricted upstream to tax-year-in-scope ROI/admitted rows, with normalized keys, provenance, and admission metadata | Non-zero rows; expected key columns present; pre-Silver admission metadata present |
| 990_givingtuesday/datamart/07_build_basic_plus_combined_presilver | staging/filing/givingtuesday_990_basic_plus_combined_presilver.parquet | ROI-scoped mixed pre-Silver build; Combined backbone enriched from admitted Basic by key, unmatched admitted Basic retained | Non-zero rows; ROI admission metadata present; fill/provenance columns present |
| 990_givingtuesday/datamart/08_filter_benchmark_outputs_local | staging/filing/givingtuesday_990_basic_allforms_benchmark.parquet | Benchmark-filtered basic-only filing table with county_fips + region | Rows reduced vs pre-Silver and region populated |
| 990_givingtuesday/datamart/08_filter_benchmark_outputs_local | staging/filing/givingtuesday_990_filings_benchmark.parquet | Benchmark-filtered mixed filing table with county_fips + region | Rows reduced vs pre-Silver and region populated |
| 990_givingtuesday/datamart/08_filter_benchmark_outputs_local | raw/givingtuesday_990/datamarts/metadata/schema_snapshot_*.json | schema snapshots for the basic-only and mixed filtered artifacts | Column list matches the emitted parquet |
| 990_givingtuesday/datamart/11_verify_curated_outputs_local_s3 | raw/givingtuesday_990/datamarts/metadata/curated_output_size_verification_report.csv | local/S3 bytes for Bronze pre-Silver and Silver filtered GT artifacts | Every emitted curated artifact row has size_match=TRUE |
| 990_givingtuesday/datamart/11_verify_curated_outputs_local_s3 | raw/givingtuesday_990/datamarts/metadata/gt_pipeline_artifact_manifest.json/.csv | artifact-level bytes, signatures, row counts, and provenance summaries across GT steps 06-11 | Manifest rows exist for the emitted curated artifacts and build manifests |
| 990_givingtuesday/api/02_fetch_bmf | raw/irs_bmf/eo_<state>.csv | EIN, NAME, STREET, CITY, STATE, ZIP, etc. | One CSV per state (default: sd, mn, mt, az) |
| irs_bmf/01_fetch_bmf_release | raw/irs_bmf/eo_<state>.csv + metadata/irs_bmf_raw_manifest.csv | Raw IRS EO BMF state snapshots plus byte/source manifest | One CSV per state; manifest rows match downloaded states |
| irs_bmf/04_filter_bmf_to_benchmark_local | staging/irs_bmf/irs_bmf_combined_filtered.parquet | Combined benchmark-filtered IRS EO BMF rows after state-by-state admission, tax-year derivation, and cross-state EIN-year dedupe | Non-zero rows; `county_fips`, `region`, and `tax_year` populated |
| irs_bmf/04_filter_bmf_to_benchmark_local | staging/irs_bmf/year=YYYY/irs_bmf_benchmark_year=YYYY.parquet | Benchmark-filtered IRS EO BMF yearly parquet for analysis years `2022-2024` | Row `tax_year` matches directory year |
| irs_bmf/07_extract_analysis_variables_local | staging/irs_bmf/irs_bmf_analysis_variables.parquet | IRS EO BMF analysis-ready rowset with direct classification, financial fields, broad NTEE, proxy flags, and imputed flags | Zero duplicate `ein + tax_year` rows |
| irs_bmf/07_extract_analysis_variables_local | staging/irs_bmf/irs_bmf_analysis_geography_metrics.parquet | IRS EO BMF geography metrics by county/region, tax year, and exclusion variant | Two exclusion variants present |
| irs_bmf/07_extract_analysis_variables_local | staging/irs_bmf/irs_bmf_analysis_field_metrics.parquet | IRS EO BMF broad-NTEE representation metrics by region, tax year, and exclusion variant | Broad NTEE rows aggregate to region-year totals |
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
| nccs_990_core/05_filter_core_to_benchmark_local | staging/nccs_990/core/year=YYYY/nccs_990_core_combined_filtered_year=YYYY.parquet | combined filtered Core union parquet built only from the per-file benchmark CSVs and annotated with source context | Non-zero rows; `core_family` and `core_scope` populated |
| nccs_990_core/07_extract_analysis_variables_local | staging/nccs_990/core/nccs_990_core_analysis_variables.parquet | 2022-only row-level Core analysis dataset built only from filtered benchmark files; preserves PC/PZ overlap rows and adds source context, harmonized analysis fields, and provenance | Non-zero rows; `core_scope` populated; expected PC/PZ overlap rows retained |
| nccs_990_core/07_extract_analysis_variables_local | staging/nccs_990/core/nccs_990_core_analysis_geography_metrics.parquet | region x tax_year x exclusion-variant Core metrics | Two exclusion variants present; non-zero region rows |
| nccs_990_core/07_extract_analysis_variables_local | raw/nccs_990/core/metadata/nccs_990_core_analysis_variable_coverage.csv | analysis-variable coverage by tax_year and core_scope | Coverage rows present for mapped variables |
| nccs_990_postcard/02_download_postcard_release | raw/nccs_990/postcard/raw/snapshot_year=YYYY/snapshot_month=YYYY-MM/*.csv | official NCCS postcard monthly snapshot CSVs | Local bytes == source Content-Length |
| nccs_990_postcard/04_verify_postcard_source_local_s3 | raw/nccs_990/postcard/metadata/size_verification_snapshot_year=YYYY.csv | source/local/S3 bytes for postcard raw assets | Every required row has size_match=TRUE |
| nccs_990_postcard/05_filter_postcard_to_benchmark_local | staging/nccs_990/postcard/snapshot_year=YYYY/nccs_990_postcard_benchmark_snapshot_year=YYYY.csv | combined benchmark-county postcard output with county_fips + region + match source | Rows reduced vs raw and region populated |
| nccs_bmf/02_download_bmf_release | raw/nccs_bmf/raw/year=YYYY/*.csv | selected yearly NCCS BMF raw CSVs | Local bytes == source Content-Length |
| nccs_bmf/04_verify_bmf_source_local_s3 | raw/nccs_bmf/metadata/size_verification_start_year=2022.csv | source/local/S3 bytes for selected raw BMF assets | Every required row has size_match=TRUE |
| nccs_bmf/05_filter_bmf_to_benchmark_local | staging/nccs_bmf/year=YYYY/nccs_bmf_benchmark_year=YYYY.parquet | benchmark-county subset of each yearly BMF snapshot with county_fips + region + snapshot fields | Rows reduced vs raw and region populated |
| combined_990/01_build_combined_filtered_local | staging/combined_990/combined_990_filtered_source_union.parquet | source-preserving union table plus row provenance, harmonized fields, per-field provenance, overlay flags, diagnostics | Non-zero rows and schema metadata written |
