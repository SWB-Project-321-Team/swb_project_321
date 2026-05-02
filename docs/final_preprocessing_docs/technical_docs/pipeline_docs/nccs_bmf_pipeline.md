# NCCS BMF Pipeline

## Status

This document is the authoritative technical reference for the current NCCS BMF pipeline implemented in this repository.

It reflects the code as it exists now under `python/ingest/nccs_bmf/`, including the current analysis window of `2022-2024`, the exact-year lookup artifacts written in step 05, the row-level BMF analysis build in step 07, and the official analysis upload in step 08.

This document is intentionally detailed. It is meant to let another engineer or analyst understand:

- where the NCCS BMF data comes from
- how it is stored locally and in S3
- the exact script order and responsibilities
- how yearly raw BMF files become benchmark-filtered annual parquets
- how the final BMF analysis dataset is built from those annual outputs
- what is direct, calculated, enriched, imputed, or unavailable in the final analysis-ready package

## Pipeline At A Glance

The NCCS BMF pipeline is the repo's primary registry and classification layer for nonprofit counts, geography, subsection logic, and NTEE-based field composition analysis.

At a high level, the pipeline has these phases:

1. Discover which NCCS BMF yearly source files should be used.
2. Download the yearly raw BMF files and write selection metadata.
3. Upload the raw yearly files and release metadata to Bronze S3.
4. Strictly verify source/local/S3 byte parity for the raw yearly inputs.
5. Filter each yearly raw file to benchmark geography and write paired exact-year lookup overlays.
6. Upload the filtered yearly outputs and exact-year lookup overlays to Silver S3.
7. Build the final BMF analysis-ready rowset, geography metrics, field metrics, coverage CSV, mapping doc, and this processing doc.
8. Upload the final BMF analysis artifacts and metadata to the dedicated analysis S3 prefix.

The main artifact classes are:

- Raw data: yearly NCCS BMF source files exactly as downloaded.
- Metadata/manifests: release-selection metadata, HTML snapshots, download manifests, and verification reports.
- Silver filtered artifacts: benchmark-filtered yearly BMF parquets plus yearly exact-year lookup parquets.
- Final analysis artifacts: the row-level analysis parquet, geography metrics parquet, field metrics parquet, coverage CSV, mapping Markdown, and this processing doc.

This pipeline follows a filtered-only combine contract (combine stages use filtered inputs only):

- downstream combine stages operate only on benchmark-filtered yearly BMF artifacts
- the annual benchmark filter happens upstream in step 05
- the analysis step does not reopen the broad raw yearly files

## Documentation Checklist Coverage

- Data producer: the National Center for Charitable Statistics at the Urban Institute publishes the NCCS BMF files used by this pipeline.
- Collection method: the source is a public BMF registry dataset derived from IRS exempt-organization records; it is not a voluntary survey and does not represent complete Form 990 filings.
- Inclusion criteria and limitations: the pipeline selects representative yearly BMF files, filters each year to benchmark geography, and builds the analysis layer for `2022-2024`; the 2022 legacy file lacks a published per-file dictionary, so the package includes a header companion and documents that limitation.
- Datatype transformation: raw annual CSV files are preserved unchanged in Bronze S3; benchmark-filtered annual outputs, exact-year lookups, and analysis-ready outputs are written as Parquet after filtering and curation.
- Data cleaning: cleaning includes annual source selection, ZIP/county/region mapping, benchmark admission, exact-year lookup construction, analysis-only duplicate resolution, IRS/NCCS classification fallback enrichment, and imputed/proxy classification flags.
- Analysis data dictionary: use `docs/final_preprocessing_docs/technical_docs/analysis_variable_mappings/nccs_bmf_analysis_variable_mapping.md` plus `SWB_321_DATA_ROOT/raw/nccs_bmf/metadata/nccs_bmf_analysis_variable_coverage.csv`; raw dictionary support is under `documentation/final_preprocessing_docs/technical_docs/source_dictionaries/nccs_bmf_*` and `documentation/final_preprocessing_docs/technical_docs/source_dictionaries/irs_eo_bmf/eo-info.pdf`.

## Script Order And Responsibilities

| Step | Script | Primary role | Main inputs | Main outputs | External systems |
| ---- | ------ | ------------ | ----------- | ------------ | ---------------- |
| 01 | `01_discover_bmf_release.py` | Discover the yearly BMF assets to use for the run | Remote dataset pages and probes | `latest_release.json`, HTML snapshots | HTTP |
| 02 | `02_download_bmf_release.py` | Download the selected yearly BMF raw files | Selected yearly asset set | Raw annual BMF files | HTTP |
| 03 | `03_upload_bmf_release_to_s3.py` | Upload raw BMF files and metadata to Bronze | Local raw files and metadata | Bronze raw BMF objects | S3 |
| 04 | `04_verify_bmf_source_local_s3.py` | Verify raw bytes across source, local disk, and Bronze | Remote sizes, local files, Bronze objects | Raw verification CSV | HTTP, S3 |
| 05 | `05_filter_bmf_to_benchmark_local.py` | Filter each year to benchmark geography and write exact-year lookup overlays | Raw annual BMF files plus geography crosswalks | Filtered annual BMF parquets and exact-year lookup parquets | local files |
| 06 | `06_upload_filtered_bmf_to_s3.py` | Upload filtered annual outputs and exact-year lookups to Silver | Filtered yearly parquets and metadata | Silver filtered BMF objects | S3 |
| 07 | `07_extract_analysis_variables_local.py` | Build row-level analysis variables, geography metrics, field metrics, coverage CSV, mapping doc, and this processing doc | Filtered annual BMF parquets and exact-year lookup parquets | Official BMF analysis outputs | local files |
| 08 | `08_upload_analysis_outputs.py` | Upload official analysis outputs and verify local-vs-S3 bytes | Analysis artifacts and docs | Silver analysis objects | S3 |

## How Data Is Retrieved

## Exact Source Locations

- NCCS BMF dataset page: `https://nccs.urban.org/nccs/datasets/bmf/`
- NCCS BMF catalog page: `https://nccs.urban.org/nccs/catalogs/catalog-bmf.html`
- NCCS raw monthly BMF base URL: `https://nccsdata.s3.us-east-1.amazonaws.com/raw/bmf/`
- NCCS legacy BMF base URL: `https://nccsdata.s3.us-east-1.amazonaws.com/legacy/bmf/`

## Raw Source Dictionaries

- Current raw monthly BMF dictionary: `documentation/final_preprocessing_docs/technical_docs/source_dictionaries/irs_eo_bmf/eo-info.pdf`
- Current raw monthly header companion: `documentation/final_preprocessing_docs/technical_docs/source_dictionaries/nccs_bmf_raw_monthly/2026-03-BMF_header_dictionary.csv`
- Legacy 2022 dictionary status page: `documentation/final_preprocessing_docs/technical_docs/source_dictionaries/nccs_bmf_legacy_2022/dd_unavailable.html`
- Legacy 2022 header companion: `documentation/final_preprocessing_docs/technical_docs/source_dictionaries/nccs_bmf_legacy_2022/BMF-2022-08-501CX-NONPROFIT-PX_header_dictionary.csv`
- NCCS links the 2022 legacy BMF profile to `dd_unavailable.html`; the companion CSV documents the raw header while preserving that limitation.

### Step 01: release discovery

The BMF pipeline begins by identifying which yearly source files represent the current benchmark-analysis window.

The discovery step:

- probes the public NCCS BMF release pages
- captures HTML snapshots for reproducibility
- writes a `latest_release.json` file describing the selected yearly assets

This selection metadata is the source of truth for which annual files later steps should download and process.

### Step 02: raw yearly download

The download step materializes one raw annual BMF file for each selected analysis year.

It preserves:

- the source URL
- the selected filename
- the year represented by that file
- source byte metadata when available

The raw files are intentionally kept unchanged so the filter step can always trace back to the original annual inputs.

### Step 03 and Step 04: Bronze upload and raw verification

After download, the pipeline uploads the raw annual files and release metadata to Bronze and then verifies source/local/S3 byte parity.

This means the BMF pipeline keeps:

- local raw files
- Bronze raw copies
- a verification CSV documenting whether the local and S3 byte counts match the original source

## Where Data Is Stored

### Local directories

- Local raw BMF directory: `SWB_321_DATA_ROOT/raw/nccs_bmf/raw`
- Local metadata directory: `SWB_321_DATA_ROOT/raw/nccs_bmf/metadata`
- Local staging directory: `SWB_321_DATA_ROOT/staging/nccs_bmf`

### Filtered and analysis artifacts

- Row-level analysis parquet: `SWB_321_DATA_ROOT/staging/nccs_bmf/nccs_bmf_analysis_variables.parquet`
- Geography metrics parquet: `SWB_321_DATA_ROOT/staging/nccs_bmf/nccs_bmf_analysis_geography_metrics.parquet`
- NTEE field metrics parquet: `SWB_321_DATA_ROOT/staging/nccs_bmf/nccs_bmf_analysis_field_metrics.parquet`
- Coverage CSV: `SWB_321_DATA_ROOT/raw/nccs_bmf/metadata/nccs_bmf_analysis_variable_coverage.csv`
- Mapping doc: `docs/final_preprocessing_docs/technical_docs/analysis_variable_mappings/nccs_bmf_analysis_variable_mapping.md`
- Data-processing doc: `docs/final_preprocessing_docs/technical_docs/pipeline_docs/nccs_bmf_pipeline.md`

### S3 layout

- Silver filtered BMF prefix: `silver/nccs_bmf/`
- Silver filtered metadata prefix: `silver/nccs_bmf/metadata/`
- Official S3 analysis prefix: `silver/nccs_bmf/analysis/`
- Analysis documentation prefix: `silver/nccs_bmf/analysis/documentation`
- Analysis variable mapping prefix: `silver/nccs_bmf/analysis/variable_mappings`
- Analysis coverage prefix: `silver/nccs_bmf/analysis/quality/coverage`

## Step-By-Step Transformation

### Step 05: yearly benchmark filtering and exact-year lookup creation

Step 05 is the key upstream curation step.

For each selected BMF year, it:

1. loads the raw annual BMF file
2. filters rows to the benchmark counties and regions of interest
3. writes one benchmark-filtered yearly parquet
4. writes one exact-year lookup parquet used later for NTEE/subsection enrichment

This is a deliberate design choice. The final analysis step does not enrich classification directly from the broad raw files. Instead, the upstream yearly lookup overlays become the stable exact-year classification inputs consumed downstream.

### Step 06: Silver upload of yearly artifacts

The pipeline then uploads:

- the benchmark-filtered yearly parquets
- the exact-year lookup parquets
- the filtered-artifact metadata

This means the annual benchmark outputs are official curated Silver artifacts in their own right, not just temporary intermediates.

### Step 07: analysis-year union

The analysis step restricts scope to `2022-2024` and loads each benchmark-filtered annual parquet in that range.

It also loads the exact-year lookup parquet for the same year. This preserves a clean separation between:

- direct annual BMF values such as revenue, assets, and income
- exact-year classification overlays
- later fallback enrichment when exact-year classification is blank

### Row-level identity and direct financial extraction

The row-level analysis dataset keeps:

- `ein`
- `tax_year`
- `org_name`
- `state`
- `zip5`
- `county_fips`
- `region`

Direct financial fields are extracted from the year-aware raw BMF columns:

- `analysis_total_revenue_amount`
- `analysis_total_assets_amount`
- `analysis_total_income_amount`

The pipeline also preserves `*_source_column` provenance so the final analysis file records whether a value came from the 2022 legacy amount columns or the 2023+ amount columns.

### Analysis-only duplicate resolution

Rare duplicate `EIN + tax_year` groups are resolved only inside the final analysis rowset.

This is intentionally not an upstream rewrite of the yearly benchmark parquets. The annual benchmark artifacts remain source-faithful filtered outputs. The analysis layer resolves duplicates only so region/year totals do not double count the same organization-year.

The ranking rule prefers:

1. highest nonblank revenue
2. highest nonblank assets
3. highest nonblank income
4. stable source order

### Classification enrichment and fallback order

`analysis_ntee_code` and `analysis_subsection_code` start from the exact-year lookup overlay joined by `ein + tax_year`.

When the exact-year lookup is blank, the final analysis-ready BMF file uses this fallback order:

1. exact-year BMF
2. nearest-year BMF
3. IRS EO BMF fallback

All fills preserve:

- source family
- source variant
- source column

`analysis_calculated_ntee_broad_code` is then derived as the first letter of the final resolved NTEE code.

### Source-backed and imputed exclusion flags

The source-backed flag family is:

- `analysis_is_hospital`
- `analysis_is_university`
- `analysis_is_political_org`

These are derived from the resolved classification fields:

- hospital and university from NTEE
- political organization from subsection

The fully populated analysis-ready flag family is:

- `analysis_imputed_is_hospital`
- `analysis_imputed_is_university`
- `analysis_imputed_is_political_org`

The imputed columns prefer the source-backed value first, then conservative name-based fallback, then default `False`.

### Geography metrics

The geography metrics parquet aggregates the analysis rowset by:

- geography level
- region
- optional county FIPS
- tax year
- exclusion variant

Metrics include:

- organization counts
- unique EIN counts
- revenue, assets, and income sums
- normalized revenue and assets per nonprofit
- counts of imputed hospital, university, and political-organization flags

### Field metrics

The field metrics parquet aggregates the same rowset by:

- region
- tax year
- exclusion variant
- `analysis_calculated_ntee_broad_code`

Each row reports:

- organization count
- revenue sum
- asset sum
- share of region nonprofit count
- share of region revenue
- share of region assets

This is the table intended to support field-composition and sector-mix analysis from the stated analysis scope.

### Coverage, mapping, and upload verification

The coverage CSV reports populated counts and fill rates for the available analysis variables by year and overall.

## Coverage Evidence

- Generated coverage CSV: `SWB_321_DATA_ROOT/raw/nccs_bmf/metadata/nccs_bmf_analysis_variable_coverage.csv`
- Published S3 documentation object: `silver/nccs_bmf/analysis/documentation/nccs_bmf_pipeline.md`
- Published S3 variable mapping object: `silver/nccs_bmf/analysis/variable_mappings/nccs_bmf_analysis_variable_mapping.md`
- Published S3 coverage object: `silver/nccs_bmf/analysis/quality/coverage/nccs_bmf_analysis_variable_coverage.csv`
- Coverage CSVs are generated by step 07 and published as authoritative quality evidence.

The mapping Markdown documents which variables are:

- direct
- calculated
- enriched
- proxy
- imputed
- unavailable

The upload step performs explicit local-vs-S3 byte checks for every analysis artifact and raises if any uploaded object size does not match the local file.

## Final Analysis Outputs

The official BMF analysis package consists of:

- one row-level analysis parquet
- one geography metrics parquet
- one field metrics parquet
- one coverage CSV
- one variable mapping Markdown
- this data-processing doc

The row-level analysis dataset is the canonical BMF analysis-ready file for `2022-2024`.

## Testing And Verification

- Unit tests live in `tests/nccs_bmf/test_nccs_bmf_common.py`.
- The analysis tests cover year restriction, direct financial extraction, classification fallback behavior, duplicate handling, geography metrics, and field metrics.
- Operational verification includes raw source/local/S3 byte checks, filtered-output uploads, and analysis upload byte checks.

## Current Caveats

- BMF is a registry and classification source, not a full 990 financial-detail source.
- `analysis_total_expense_amount`, `analysis_net_asset_amount`, `analysis_calculated_surplus_amount`, `analysis_calculated_net_margin_ratio`, and `analysis_calculated_months_of_reserves` remain unavailable because the current BMF pipeline does not carry the needed expense and net-asset concepts.
- Revenue-source components such as program service revenue, contributions, and grants remain unavailable rather than being weakly backfilled from another pipeline.
- The analysis-only dedupe happens only in the final rowset; the upstream yearly benchmark artifacts remain unchanged.

## Analysis requirement alignment appendix

| analysis_requirement | source_specific_output | status | rule_or_reason |
| --- | --- | --- | --- |
| Total revenue | analysis_total_revenue_amount | direct | Year-aware direct BMF revenue field |
| Total assets | analysis_total_assets_amount | direct | Year-aware direct BMF asset field |
| Total income | analysis_total_income_amount | direct | Year-aware direct BMF income field |
| NTEE filed classification code | analysis_ntee_code | enriched | Exact-year BMF lookup joined on EIN + tax_year, then nearest-year BMF, then IRS EO BMF EIN fallback |
| Broad NTEE field classification code | analysis_calculated_ntee_broad_code | calculated | First letter of final resolved NTEE code |
| Hospital flag | analysis_is_hospital | proxy | Resolved from final NTEE code |
| University flag | analysis_is_university | proxy | Resolved from final NTEE code |
| Political organization flag | analysis_is_political_org | proxy | Resolved from final subsection code |
| Total expense | analysis_total_expense_amount | unavailable | BMF does not carry this expense field |
| Net asset | analysis_net_asset_amount | unavailable | BMF does not provide a direct net-asset field in the current pipeline |
| Program service revenue | analysis_program_service_revenue_amount | unavailable | BMF does not carry this 990-style revenue-source field |
| Total contributions | analysis_calculated_total_contributions_amount | unavailable | BMF does not carry this 990-style revenue-source field |
| Grants (total amount) | analysis_calculated_grants_total_amount | unavailable | BMF does not carry this 990-style revenue-source field |
