# NCCS 990 Postcard Pipeline

## Status

This document is the authoritative technical reference for the current NCCS 990 Postcard pipeline implemented in this repository.

It reflects the code as it exists now under `python/ingest/nccs_990_postcard/`, including the current retained tax-year window of `2022-2024`, the latest-snapshot benchmark derivative written in step 05, the row-level analysis extraction in step 07, and the official analysis upload in step 08.

This document is intentionally detailed. It is meant to let another engineer or analyst understand:

- where the postcard data comes from
- how it is stored locally and in S3
- the exact script order and responsibilities
- how raw monthly postcard CSVs become a retained benchmark-scoped derivative
- how the final postcard analysis package is derived from that derivative
- which variables are supported directly, enriched, imputed, or intentionally unavailable

## Pipeline At A Glance

The postcard pipeline is a latest-snapshot small-filer support layer, not a full annual financial panel.

At a high level, the pipeline has these phases:

1. Discover the latest postcard snapshot year and monthly file set.
2. Download the raw monthly postcard CSVs.
3. Upload the raw files and metadata to Bronze S3.
4. Strictly verify source/local/S3 byte parity for the raw monthly inputs.
5. Filter monthly files to benchmark geography and dedupe to one retained row per EIN in the latest snapshot derivative.
6. Upload the filtered postcard derivatives to Silver S3.
7. Build the final postcard analysis outputs for retained `2022-2024` rows.
8. Upload the final analysis artifacts and metadata to the postcard analysis S3 prefix.

The main artifact classes are:

- Raw data: monthly postcard CSV files exactly as downloaded.
- Metadata/manifests: release-selection metadata, snapshot HTML, download manifests, and verification reports.
- Silver filtered artifacts: retained postcard derivatives built from benchmark-scoped monthly rows.
- Final analysis artifacts: the row-level analysis parquet, geography metrics parquet, coverage CSV, mapping Markdown, and this processing doc.

This pipeline follows the `CODING_RULES.md` filtered-only combine contract:

- each monthly raw file is filtered chunk-by-chunk before retained rows are combined
- the latest-snapshot EIN dedupe happens only after geography admission
- the final analysis step reads only the retained filtered derivative

## Script Order And Responsibilities

| Step | Script | Primary role | Main inputs | Main outputs | External systems |
| ---- | ------ | ------------ | ----------- | ------------ | ---------------- |
| 01 | `01_discover_postcard_release.py` | Discover the latest postcard snapshot year and available monthly files | Remote HTML pages and monthly probes | `latest_release.json`, snapshot HTML | HTTP |
| 02 | `02_download_postcard_release.py` | Download the selected monthly postcard CSVs | Selected monthly URLs | Raw monthly postcard CSVs | HTTP |
| 03 | `03_upload_postcard_release_to_s3.py` | Upload raw postcard files and metadata to Bronze | Local raw monthly files and metadata | Bronze raw postcard objects | S3 |
| 04 | `04_verify_postcard_source_local_s3.py` | Verify raw postcard bytes across source, local disk, and Bronze | Remote sizes, local files, Bronze objects | Raw verification CSV | HTTP, S3 |
| 05 | `05_filter_postcard_to_benchmark_local.py` | Filter to benchmark geography and dedupe to one retained row per EIN in the latest snapshot derivative | Raw postcard CSVs plus benchmark geography reference files | Filtered postcard derivatives including the retained-tax-year parquet | local files |
| 06 | `06_upload_filtered_postcard_to_s3.py` | Upload filtered postcard derivatives to Silver | Filtered postcard outputs and manifests | Silver filtered postcard objects | S3 |
| 07 | `07_extract_analysis_variables_local.py` | Build row-level postcard analysis outputs and geography metrics for retained `2022-2024` rows | Filtered postcard derivative plus BMF/IRS classification fallbacks | Row-level analysis parquet, geography metrics parquet, coverage CSV, mapping doc, this processing doc | local files |
| 08 | `08_upload_analysis_outputs.py` | Upload official analysis outputs and metadata to the Silver analysis prefix | Analysis artifacts and docs | Silver analysis objects | S3 |

## How Data Is Retrieved

### Step 01: snapshot discovery

The postcard pipeline first probes recent months to determine:

- the current published postcard snapshot year
- which monthly files exist within that snapshot

The discovery step writes `latest_release.json` and supporting snapshot HTML so later runs can reproduce the selected monthly input set.

### Step 02: monthly raw download

The download step materializes the selected monthly postcard CSVs exactly as published.

These files are intentionally preserved as raw monthly inputs so the benchmark filter can always trace a retained postcard record back to its source month.

### Step 03 and Step 04: Bronze upload and raw verification

After download, the pipeline uploads the raw monthly files and metadata to Bronze and then verifies source/local/S3 byte parity.

This preserves:

- local raw monthly copies
- Bronze raw copies
- a verification CSV documenting whether the source, local, and S3 byte counts agree

## Where Data Is Stored

### Local directories

- Local raw monthly CSV directory: `C:\Users\eilke\Desktop\Github Repo\swb_project_321\data\321_Black_Hills_Area_Community_Foundation_2025_08\01_data\raw\nccs_990\postcard\raw`
- Local metadata directory: `C:\Users\eilke\Desktop\Github Repo\swb_project_321\data\321_Black_Hills_Area_Community_Foundation_2025_08\01_data\raw\nccs_990\postcard\metadata`
- Local staging directory: `C:\Users\eilke\Desktop\Github Repo\swb_project_321\data\321_Black_Hills_Area_Community_Foundation_2025_08\01_data\staging\nccs_990\postcard`

### Analysis artifacts

- Row-level analysis parquet: `C:\Users\eilke\Desktop\Github Repo\swb_project_321\data\321_Black_Hills_Area_Community_Foundation_2025_08\01_data\staging\nccs_990\postcard\nccs_990_postcard_analysis_variables.parquet`
- Geography metrics parquet: `C:\Users\eilke\Desktop\Github Repo\swb_project_321\data\321_Black_Hills_Area_Community_Foundation_2025_08\01_data\staging\nccs_990\postcard\nccs_990_postcard_analysis_geography_metrics.parquet`
- Coverage CSV: `C:\Users\eilke\Desktop\Github Repo\swb_project_321\data\321_Black_Hills_Area_Community_Foundation_2025_08\01_data\raw\nccs_990\postcard\metadata\nccs_990_postcard_analysis_variable_coverage.csv`
- Mapping doc: `C:\Users\eilke\Desktop\Github Repo\swb_project_321\docs\analysis\nccs_990_postcard_analysis_variable_mapping.md`
- Data-processing doc: `C:\Users\eilke\Desktop\Github Repo\swb_project_321\docs\data_processing\nccs_990_postcard_pipeline.md`

### S3 layout

- Silver filtered postcard prefix: `silver/nccs_990/postcard/`
- Silver filtered metadata prefix: `silver/nccs_990/postcard/metadata/`
- Official S3 analysis prefix: `silver/nccs_990/postcard/analysis/`
- Official S3 analysis metadata prefix: `silver/nccs_990/postcard/analysis/metadata/`

## Step-By-Step Transformation

### Step 05: benchmark filtering and retained latest-snapshot derivative

Step 05 is the key upstream curation step for postcard.

For each monthly file, it:

1. reads the monthly CSV in chunks
2. filters rows to benchmark geography before any large combined rowset is built
3. concatenates the retained benchmark-scoped rows
4. dedupes to one retained row per EIN in the latest snapshot view
5. writes the retained tax-year-window derivative consumed by the analysis step

This means the downstream analysis build never needs to reopen the broad raw monthly rowset.

### Retained row semantics

The postcard derivative is intentionally:

- benchmark-scoped
- latest-snapshot based
- one-row-per-EIN after retained dedupe

It is not a true annual filing panel. That is a central interpretive constraint for the final postcard analysis outputs.

### Step 07: analysis-year clamp and row-level analysis build

The analysis step reads the retained postcard derivative and then clamps the final analysis rows to tax years `2022-2024`.

Rows outside that tax-year window stay out of the final analysis outputs, but the latest-snapshot character of the retained derivative remains intact.

### Identity and geography extraction

The row-level analysis dataset keeps postcard identity and geography fields such as:

- `ein`
- `tax_year`
- `org_name`
- `county_fips`
- `region`
- `benchmark_match_source`
- `snapshot_month`
- `snapshot_year`

Postcard is intentionally treated as a counts-and-geography layer, not a financial-performance layer.

### Classification enrichment

`analysis_ntee_code` and `analysis_subsection_code` use the same fallback order that the repo applies to other non-BMF sources when appropriate:

1. exact-year NCCS BMF lookup by `ein + tax_year`
2. nearest-year NCCS BMF lookup by `ein`
3. IRS EO BMF lookup by `ein`

All fills preserve classification provenance fields so the final dataset records where each value came from.

### Source-backed and imputed support flags

The source-backed flag family is:

- `analysis_is_hospital`
- `analysis_is_university`
- `analysis_is_political_org`

The complete analysis-ready flag family is:

- `analysis_imputed_is_hospital`
- `analysis_imputed_is_university`
- `analysis_imputed_is_political_org`

The imputed flags use:

1. the canonical source-backed proxy
2. high-confidence name patterns
3. default `False`

Postcard also exposes direct small-filer support fields such as:

- `analysis_gross_receipts_under_25000`
- `analysis_terminated`

when they are present in the retained postcard derivative.

### Geography metrics

The postcard geography metrics parquet writes both:

- region-level rows
- county-level rows

Each record is keyed by:

- geography level
- region
- optional county FIPS
- retained tax year
- exclusion variant

Metrics include:

- filing count
- unique EIN count
- counts of imputed exclusion flags
- gross-receipts-under-25k counts
- terminated counts

### Coverage, mapping, and upload verification

The coverage CSV records fill rates for the available postcard classification and support variables by retained tax year.

Unsupported financial variables remain explicitly marked unavailable rather than being backfilled from GT or efile.

The upload step prints explicit local-vs-S3 byte comparisons for each analysis artifact and raises if any remote byte count differs from local bytes.

## Final Analysis Outputs

The official postcard analysis package consists of:

- one row-level analysis parquet
- one geography metrics parquet
- one coverage CSV
- one mapping Markdown
- this data-processing doc

This package is intended for supplemental small-filer geography, counts, and classification analysis.

## Testing And Verification

- Unit tests live in `tests/nccs_990_postcard/test_nccs_990_postcard_common.py`.
- The analysis tests cover tax-year clamping, classification fallback behavior, support-flag construction, and geography metric generation.
- Operational verification includes raw source/local/S3 byte checks, filtered-output uploads, and analysis upload byte checks.

## Current Caveats

- The postcard benchmark derivative is a latest-snapshot, one-row-per-EIN product after retained dedupe. It is not a true annual panel.
- The postcard analysis outputs are suitable for supplemental small-filer counts, geography, and classification support, not for authoritative multi-year financial trend analysis.
- Because postcard does not carry the GT/efile financial fields, revenue, expense, assets, surplus, net margin, reserves, and contribution/grant variables remain explicitly unavailable.
- The main runner currently executes sequentially rather than exposing GT-style start-step/end-step selection.

## Draft Alignment Appendix

| draft_variable | source_specific_output | status | rule_or_reason |
| --- | --- | --- | --- |
| NTEE filed classification code | analysis_ntee_code | enriched | Exact-year, nearest-year, then IRS EO BMF fallback |
| Broad NTEE field classification code | analysis_calculated_ntee_broad_code | calculated | First letter of resolved NTEE code |
| Hospital flag | analysis_is_hospital | proxy | Resolved from NTEE code |
| University flag | analysis_is_university | proxy | Resolved from NTEE code |
| Political organization flag | analysis_is_political_org | proxy | Resolved from subsection code |
| Total revenue | analysis_total_revenue_amount | unavailable | Postcard does not carry this financial-performance field |
| Total expense | analysis_total_expense_amount | unavailable | Postcard does not carry this financial-performance field |
| Net asset | analysis_net_asset_amount | unavailable | Postcard does not carry this financial-performance field |
| Program service revenue | analysis_program_service_revenue_amount | unavailable | Postcard does not carry this revenue-source field |
| Total contributions | analysis_calculated_total_contributions_amount | unavailable | Postcard does not carry this revenue-source field |
| Grants (total amount) | analysis_calculated_grants_total_amount | unavailable | Postcard does not carry this revenue-source field |
