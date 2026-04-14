# NCCS Core Pipeline

## Status

This document is the authoritative technical reference for the current NCCS Core pipeline implemented in this repository.

It reflects the code as it exists now under `python/ingest/nccs_990_core/`, including the current `2022`-only scope, the benchmark-filtered Core outputs written in step 05, the combined filtered Core parquet, the row-level analysis build in step 07, and the official analysis upload in step 08.

This document is intentionally detailed. It is meant to let another engineer or analyst understand:

- where the Core data comes from
- how it is stored locally and in S3
- the exact script order and responsibilities
- how raw Core files become benchmark-filtered Core outputs
- how the final Core analysis-ready rowset is built from filtered Core inputs only
- what is direct, calculated, enriched, imputed, caveated, or unavailable in the final Core analysis package

## Pipeline At A Glance

The NCCS Core pipeline is now a co-equal source-specific 990 layer in the repo. It does not replace GT or efile, but it provides an additional benchmark-filtered analysis package built from the NCCS Core release.

At a high level, the pipeline has these phases:

1. Discover the latest common Core release and supporting bridge inputs.
2. Download the raw Core files, dictionaries, and bridge BMF files.
3. Upload the raw release to Bronze S3.
4. Strictly verify source/local/S3 byte parity for the raw release.
5. Filter the Core files to benchmark geography using the BMF bridge and write a combined filtered Core parquet.
6. Upload the benchmark-filtered Core outputs to Silver S3.
7. Build the final Core analysis outputs from the filtered Core inputs only.
8. Upload the final Core analysis outputs and metadata to the analysis S3 prefix.

The main artifact classes are:

- Raw data: Core source files exactly as downloaded.
- Metadata/manifests: release-selection metadata, dictionaries, bridge files, and verification reports.
- Silver filtered artifacts: benchmark-filtered Core outputs plus a combined filtered Core parquet.
- Final analysis artifacts: the row-level analysis parquet, geography metrics parquet, coverage CSV, mapping Markdown, and this processing doc.

This pipeline follows the `CODING_RULES.md` filtered-only combine contract:

- benchmark admission happens upstream in step 05
- the final analysis build reads only filtered Core outputs
- raw Core files are not directly combined in the analysis step

## Core Contract

- Year scope is `2022` only.
- The final analysis build reads only filtered Core benchmark files in staging.
- `core_scope` is the authoritative filing-context field.
- `harm_filing_form` is sparse by design and is currently populated only for reliable `PF -> 990PF` rows.
- Overlapping `PC` and `PZ` rows are intentionally preserved in the final analysis file.

## Script Order And Responsibilities

| Step | Script | Primary role | Main inputs | Main outputs | External systems |
| ---- | ------ | ------------ | ----------- | ------------ | ---------------- |
| 01 | `01_discover_core_release.py` | Discover the latest common Core release plus required bridge files | Remote release pages and bridge metadata | Release metadata and selection state | HTTP |
| 02 | `02_download_core_release.py` | Download Core raw files, dictionaries, and bridge BMF files | Release metadata and remote URLs | Raw Core files, dictionaries, and bridge inputs | HTTP |
| 03 | `03_upload_core_release_to_s3.py` | Upload the raw Core release to Bronze | Local raw release and metadata | Bronze raw Core objects | S3 |
| 04 | `04_verify_core_source_local_s3.py` | Verify source/local/S3 byte parity for the raw release | Remote sizes, local files, Bronze objects | Raw verification CSV | HTTP, S3 |
| 05 | `05_filter_core_to_benchmark_local.py` | Filter Core files to benchmark geography using the BMF bridge and write a combined filtered Core parquet | Raw Core files plus bridge BMF inputs | Filtered Core outputs and combined filtered Core parquet | local files |
| 06 | `06_upload_filtered_core_to_s3.py` | Upload benchmark-filtered Core outputs to Silver | Filtered Core outputs and metadata | Silver filtered Core objects | S3 |
| 07 | `07_extract_analysis_variables_local.py` | Build row-level analysis variables, geography metrics, coverage CSV, mapping doc, and this processing doc | Filtered Core outputs, combined filtered Core parquet, BMF lookups, IRS EO BMF lookups | Official Core analysis outputs | local files |
| 08 | `08_upload_analysis_outputs.py` | Upload analysis outputs and verify local-vs-S3 byte parity | Analysis artifacts and docs | Silver analysis objects | S3 |

## How Data Is Retrieved

### Step 01 and Step 02: release discovery and raw download

The Core pipeline starts by selecting the latest common Core release that is compatible with the benchmark-analysis workflow and then downloading:

- the raw Core data files
- supporting dictionaries
- bridge BMF files needed for benchmark geography filtering

This lets the repo preserve both the Core source data and the bridge inputs required to connect the Core release to the benchmark counties.

### Step 03 and Step 04: Bronze upload and raw verification

After download, the pipeline uploads the raw Core release to Bronze and verifies source/local/S3 byte parity.

This preserves:

- local raw Core copies
- Bronze raw Core copies
- a raw verification CSV documenting whether source, local, and S3 bytes agree

## Where Data Is Stored

### Local artifacts

- Combined filtered Core parquet: `data/321_Black_Hills_Area_Community_Foundation_2025_08/01_data/staging/nccs_990/core/year=2022/nccs_990_core_combined_filtered_year=2022.parquet`
- Row-level analysis parquet: `data/321_Black_Hills_Area_Community_Foundation_2025_08/01_data/staging/nccs_990/core/nccs_990_core_analysis_variables.parquet`
- Geography metrics parquet: `data/321_Black_Hills_Area_Community_Foundation_2025_08/01_data/staging/nccs_990/core/nccs_990_core_analysis_geography_metrics.parquet`
- Coverage CSV: `data/321_Black_Hills_Area_Community_Foundation_2025_08/01_data/raw/nccs_990/core/metadata/nccs_990_core_analysis_variable_coverage.csv`
- Mapping doc: `docs/analysis/nccs_990_core_analysis_variable_mapping.md`
- Data-processing doc: `docs/data_processing/nccs_990_core_pipeline.md`

### S3 layout

- Silver filtered Core prefix: `silver/nccs_990/core/`
- Silver filtered Core metadata prefix: `silver/nccs_990/core/metadata/`
- Official Core analysis prefix: `silver/nccs_990/core/analysis/`
- Official Core analysis metadata prefix: `silver/nccs_990/core/analysis/metadata/`

## Step-By-Step Transformation

### Step 05: benchmark filtering and combined filtered Core parquet

Step 05 is the key upstream curation step.

It:

1. reads the raw Core source files
2. uses the bridge BMF inputs to determine benchmark geography admission
3. writes benchmark-filtered Core outputs by source file
4. writes one combined filtered Core parquet after the per-file filter completes

That combined filtered Core parquet is the first place where the filtered Core files are unioned, and that union happens only after benchmark filtering.

### Step 07: analysis build from filtered Core inputs only

The analysis step prefers the combined filtered Core parquet when present and otherwise reconstructs the same filtered union from the per-file benchmark outputs.

The final analysis rowset is source-faithful with respect to the filtered Core union. It intentionally preserves overlapping `PC` and `PZ` rows instead of collapsing them to one row per EIN-year.

### Source-context columns

The final Core analysis dataset adds explicit source-context columns:

- `core_analysis_row_id`
- `core_source_file`
- `core_source_variant`
- `core_family`
- `core_scope`

These fields are part of the contract because Core is not being forced into a single simplified filing-form model.

### Identity and geography fallback

The final analysis dataset keeps:

- `ein`
- `tax_year`
- `org_name`
- `state`
- `zip5`
- `county_fips`
- `region`

When Core identity fields are sparse, the pipeline uses:

1. exact-year BMF identity
2. nearest-year BMF identity
3. IRS EO BMF identity

to backfill missing identity fields while preserving provenance and keeping `core_scope` as the authoritative Core context field.

### Direct and candidate financial extraction

The Core analysis layer extracts direct or source-faithful candidate fields for:

- total revenue
- total expense
- total assets
- net asset
- surplus
- program service revenue candidate
- contributions candidate
- gifts/grants received candidate
- grants paid candidate

Where the same concept differs across `pc`, `pz`, and `pf`, the step uses scope-aware rules rather than pretending the columns are identical across Core families.

### Draft-aligned promoted fields

The current Core analysis package also promotes these cleanly supportable draft-aligned fields:

- `analysis_program_service_revenue_amount`
- `analysis_calculated_total_contributions_amount`

These stay tied to their source-faithful candidate fields rather than being silently redefined with GT-style semantics.

### Calculated metrics

The main calculated metrics are:

- `analysis_calculated_surplus_amount`
- `analysis_calculated_net_margin_ratio`
- `analysis_calculated_cleaned_net_margin_ratio`
- `analysis_calculated_months_of_reserves`
- `analysis_calculated_ntee_broad_code`

The cleaned net-margin field exists so analysts have a more stable convenience metric while the raw ratio remains available.

### Classification enrichment

The analysis step enriches classification with the same fallback order used elsewhere in the repo when appropriate:

1. exact-year NCCS BMF
2. nearest-year NCCS BMF
3. IRS EO BMF

If all sources still fail, unresolved NTEE is explicitly marked `UNKNOWN` rather than remaining silently blank.

### Source-backed and imputed exclusion flags

The final Core analysis package includes:

- `analysis_is_hospital`
- `analysis_is_university`
- `analysis_is_political_org`
- `analysis_imputed_is_hospital`
- `analysis_imputed_is_university`
- `analysis_imputed_is_political_org`

The non-imputed flags come from Core direct fields when available and otherwise from classification-based proxy logic. The imputed fields then continue through the same conservative name-based fallback approach used elsewhere in the repo.

### Geography metrics

The geography metrics parquet aggregates the Core analysis rowset by:

- region
- tax year
- exclusion variant

Two exclusion variants are written:

- `all_rows`
- `exclude_imputed_hospital_university_political_org`

The metrics include counts, unique EIN counts, financial sums, normalized averages, and exclusion-flag counts.

### Coverage, mapping, and upload verification

The coverage CSV records populated counts and fill rates for the available Core analysis variables.

The mapping Markdown documents which variables are:

- direct
- calculated
- enriched
- imputed
- unavailable
- caveated or candidate-only

The upload step performs explicit local-vs-S3 byte checks for each analysis artifact before it accepts the upload as complete.

## Final Analysis Outputs

The official Core analysis package consists of:

- one combined filtered Core parquet
- one row-level analysis parquet
- one geography metrics parquet
- one coverage CSV
- one mapping Markdown
- this data-processing doc

This package is analogous in structure to GT and efile, while keeping Core-specific semantics visible rather than hiding them behind aggressive harmonization.

## Testing And Verification

- Unit tests live in `tests/nccs_990_core/test_nccs_990_core_common.py`.
- The analysis tests cover filtered-union loading, preserved `PC/PZ` overlaps, direct and candidate financial extraction, classification fallback behavior, cleaned net margin, and geography metrics.
- Operational verification includes raw source/local/S3 byte checks, filtered-output uploads, and analysis upload byte checks.

## Current Caveats

- Core is currently `2022` only.
- `core_scope` is the authoritative Core filing-context field; `harm_filing_form` remains sparse by design.
- `PC` and `PZ` overlap rows are intentionally preserved in the final analysis package.
- Core candidate revenue-source fields are kept source-faithful instead of being forced into GT semantics.
- Zero-expense reserve rows are made explicit rather than remaining silently null, and unresolved NTEE rows are marked `UNKNOWN`.

## Draft Alignment Appendix

| draft_variable | source_specific_output | status | rule_or_reason |
| --- | --- | --- | --- |
| Total revenue | analysis_total_revenue_amount | direct | Scope-aware direct Core revenue extraction |
| Total expense | analysis_total_expense_amount | direct | Scope-aware direct Core expense extraction |
| Total assets | analysis_total_assets_amount | direct | Scope-aware direct Core asset extraction |
| Net asset | analysis_net_asset_amount | direct_or_calculated | Direct where available; PF fallback can derive assets minus liabilities when needed |
| Surplus | analysis_calculated_surplus_amount | calculated | Scope-aware surplus rule from Core source fields |
| Net margin | analysis_calculated_net_margin_ratio | calculated | Surplus divided by positive total revenue |
| Program service revenue | analysis_program_service_revenue_amount | direct | Promoted from the Core program-service candidate field |
| Total contributions | analysis_calculated_total_contributions_amount | direct | Promoted from the Core contribution candidate field |
| NTEE filed classification code | analysis_ntee_code | enriched | Exact-year BMF, then nearest-year BMF, then IRS EO BMF fallback |
| Broad NTEE field classification code | analysis_calculated_ntee_broad_code | calculated | First letter of final resolved NTEE code |
| Political organization flag | analysis_is_political_org | proxy | Resolved from subsection classification |
