# NCCS Efile Pipeline

## Status

This document is the authoritative technical reference for the current NCCS efile pipeline implemented in this repository.

It reflects the code as it exists now under `python/ingest/nccs_efile/`, including the current analysis window of `2022-2024`, the current filter-first annual benchmark build in step 05, the row-level analysis extraction in step 08, and the official analysis upload in step 09.

This document is intentionally detailed. It is meant to let another engineer or analyst understand:

- where the efile data comes from
- how it is stored locally and in S3
- the exact script order and responsibilities
- how raw annual CSV tables become benchmark-filtered annual filing parquets
- how the final analysis-ready efile dataset is derived from those annual outputs
- what is direct, calculated, enriched, proxy, imputed, or unavailable in the final analysis package

## Pipeline At A Glance

The NCCS efile pipeline turns published NCCS parsed-return tables into annual benchmark-filtered filing rowsets and then into official analysis outputs.

At a high level, the pipeline has these phases:

1. Discover which annual efile assets should be used.
2. Download the required annual raw tables.
3. Upload the raw files and metadata to Bronze S3.
4. Strictly verify source/local/S3 byte parity for the raw inputs.
5. Build one benchmark-filtered annual parquet per tax year.
6. Upload the annual benchmark outputs to Silver S3.
7. Optionally compare the annual efile outputs against NCCS Core for audit purposes.
8. Build the final efile analysis outputs for `2022-2024`.
9. Upload the final analysis outputs and metadata to the dedicated efile analysis S3 prefix.

The main artifact classes are:

- Raw data: annual efile CSV tables downloaded from the public NCCS release.
- Metadata/manifests: release-selection metadata, HTML snapshots, download manifests, and verification reports.
- Silver filtered artifacts: one benchmark-filtered annual filing parquet per tax year.
- Final analysis artifacts: the row-level analysis parquet, region metrics parquet, coverage CSV, mapping Markdown, and this processing doc.

This pipeline follows a filtered-only combine contract (combine stages use filtered inputs only):

- the wide table combine happens only after HEADER rows have already been admitted to benchmark geography
- filing selection happens before downstream joins to SUMMARY and Schedule A
- the final analysis step reads only the already-filtered annual benchmark parquets

## Documentation Checklist Coverage

- Data producer: the National Center for Charitable Statistics and the public NCCS efile release provide the parsed efile tables used here.
- Collection method: the source tables are derived from public electronic IRS Form 990 filing data; they are required tax filings for filing organizations, not voluntary survey responses.
- Inclusion criteria and limitations: the pipeline keeps the required HEADER, SUMMARY, and Schedule A table families for tax years `2022-2024`, admits filings through benchmark geography on HEADER rows, and keeps one selected filing per EIN-tax-year; fields not reliably present in efile remain explicitly unavailable rather than backfilled from other sources.
- Datatype transformation: raw annual CSV tables are preserved unchanged in Bronze S3; benchmark-filtered annual filing outputs and analysis-ready outputs are written as Parquet after filtering, selection, and joins.
- Data cleaning: cleaning includes HEADER-based geography filtering, ZIP/county/region mapping, deterministic filing ranking, SUMMARY and Schedule A joins for retained filings, calculated metrics, classification enrichment, and imputed/proxy support flags.
- Analysis data dictionary: use `docs/final_preprocessing_docs/technical_docs/analysis_variable_mappings/nccs_efile_analysis_variable_mapping.md` plus `SWB_321_DATA_ROOT/raw/nccs_efile/metadata/nccs_efile_analysis_variable_coverage.csv`; raw dictionary support is under `documentation/final_preprocessing_docs/technical_docs/source_dictionaries/nccs_efile/`.

## Script Order And Responsibilities

| Step | Script | Primary role | Main inputs | Main outputs | External systems |
| ---- | ------ | ------------ | ----------- | ------------ | ---------------- |
| 01 | `01_discover_efile_release.py` | Probe the public NCCS efile release and select the annual assets to use | Remote HTML pages and HEAD probes | `latest_release.json`, HTML snapshots | HTTP |
| 02 | `02_download_efile_release.py` | Download the selected annual CSV tables | Release manifest and remote table URLs | Raw annual CSV tables plus download manifest | HTTP |
| 03 | `03_upload_efile_release_to_s3.py` | Upload raw files and discovery metadata to Bronze | Local raw CSVs and metadata | Bronze raw objects and metadata objects | S3 |
| 04 | `04_verify_efile_source_local_s3.py` | Verify source/local/S3 byte parity for raw inputs | Remote sizes, local files, Bronze objects | Raw verification CSV | HTTP, S3 |
| 05 | `05_build_efile_benchmark_local.py` | Build annual benchmark-filtered efile parquets | Raw annual CSV tables, benchmark geography crosswalks | `nccs_efile_benchmark_tax_year=YYYY.parquet` and annual manifests | local files |
| 06 | `06_upload_filtered_efile_to_s3.py` | Upload annual benchmark outputs to Silver | Annual benchmark parquets and metadata | Silver annual benchmark objects | S3 |
| 07 | `07_compare_efile_vs_core_local.py` | Audit-only comparison between efile and NCCS Core | Filtered efile and filtered NCCS Core outputs | Comparison CSVs and notes | local files |
| 08 | `08_extract_analysis_variables_local.py` | Build official row-level analysis outputs for `2022-2024` | Annual benchmark parquets plus BMF/IRS classification fallbacks | Row-level analysis parquet, region metrics parquet, coverage CSV, mapping doc, this processing doc | local files |
| 09 | `09_upload_analysis_outputs.py` | Upload official analysis outputs and verify local-vs-S3 bytes | Analysis artifacts and docs | Silver analysis objects | S3 |

## How Data Is Retrieved

## Exact Source Locations

- NCCS efile dataset page: `https://nccs.urban.org/nccs/datasets/efile/`
- NCCS efile catalog page: `https://nccs.urban.org/nccs/catalogs/catalog-efile-v2_1.html`
- NCCS efile public data base URL: `https://nccs-efile.s3.us-east-1.amazonaws.com/public/efile_v2_1/`

## Raw Source Dictionaries

- NCCS efile data dictionary: `documentation/final_preprocessing_docs/technical_docs/source_dictionaries/nccs_efile/data-dictionary.html`
- Parsed variable companion CSV: `documentation/final_preprocessing_docs/technical_docs/source_dictionaries/nccs_efile/data-dictionary_variables.csv`
- These files document the public efile tables discovered from the NCCS efile catalog.

### Step 01: release discovery

The discovery step probes the NCCS efile release pages and determines which annual assets should be used for the benchmark-analysis workflow.

The required table families are:

- `F9-P00-T00-HEADER`
- `F9-P01-T00-SUMMARY`
- `SA-P01-T00-PUBLIC-CHARITY-STATUS`

The discovery step writes `latest_release.json` plus HTML snapshots so later runs can replay the exact selected asset set.

### Step 02: raw annual download

The download step materializes one CSV per required table per selected year.

The local raw layout is partitioned by:

- `tax_year=...`
- `table=...`

This lets the benchmark builder read only the tables it actually needs.

### Step 03 and Step 04: Bronze upload and raw verification

After download, the pipeline uploads the raw files and release metadata to Bronze and then verifies source/local/S3 byte parity.

This preserves:

- raw local copies
- Bronze raw copies
- a verification CSV recording whether source, local, and S3 byte counts agree

## Where Data Is Stored

### Local directories

- Local raw CSV directory: `SWB_321_DATA_ROOT/raw/nccs_efile/raw`
- Local metadata directory: `SWB_321_DATA_ROOT/raw/nccs_efile/metadata`
- Local staging directory: `SWB_321_DATA_ROOT/staging/nccs_efile`

### Analysis artifacts

- Row-level analysis parquet: `SWB_321_DATA_ROOT/staging/nccs_efile/nccs_efile_analysis_variables.parquet`
- Region metrics parquet: `SWB_321_DATA_ROOT/staging/nccs_efile/nccs_efile_analysis_geography_metrics.parquet`
- Coverage CSV: `SWB_321_DATA_ROOT/raw/nccs_efile/metadata/nccs_efile_analysis_variable_coverage.csv`
- Mapping doc: `docs/final_preprocessing_docs/technical_docs/analysis_variable_mappings/nccs_efile_analysis_variable_mapping.md`
- Data-processing doc: `docs/final_preprocessing_docs/technical_docs/pipeline_docs/nccs_efile_pipeline.md`

### S3 layout

- Silver annual benchmark prefix: `silver/nccs_efile/`
- Silver annual benchmark metadata prefix: `silver/nccs_efile/metadata/`
- Official S3 analysis prefix: `silver/nccs_efile/analysis/`
- Analysis documentation prefix: `silver/nccs_efile/analysis/documentation`
- Analysis variable mapping prefix: `silver/nccs_efile/analysis/variable_mappings`
- Analysis coverage prefix: `silver/nccs_efile/analysis/quality/coverage`

## Step-By-Step Transformation

### Step 05: annual benchmark build

Step 05 is the key upstream curation step for efile.

It uses HEADER rows to determine:

- benchmark geography admission
- the retained filing key for each EIN-year
- which rows should survive into the annual benchmark parquet before wide joins happen

Only after those retained benchmark filings are identified does the step join in:

- SUMMARY
- Schedule A public-charity support fields

This means the annual combine runs only on admitted benchmark filings rather than the broad national raw rowset.

### Annual benchmark outputs

The annual benchmark parquet harmonizes the core fields needed downstream into `harm_*` columns, including:

- identity
- geography
- revenue
- expense
- assets
- income
- filing-form context
- direct hospital and university flags when present

These annual benchmark artifacts are the stable upstream source of truth for the final analysis build.

### Step 08: analysis-year union

The analysis step restricts scope to `2022-2024` and loads the annual benchmark parquets for only those years.

It does not change row admission. It unions the already-filtered annual outputs and then derives the analysis variables from that preserved filing rowset.

The final row grain is one filing row per retained `EIN + tax_year + filing form` from the annual benchmark outputs.

### Direct amount extraction

Direct row-level variables include:

- `analysis_total_revenue_amount` from `harm_revenue_amount`
- `analysis_total_expense_amount` from `harm_expenses_amount`
- `analysis_total_assets_amount` from `harm_assets_amount`
- `analysis_calculated_surplus_amount` from `harm_income_amount`

The current pipeline also keeps `analysis_net_asset_amount` as the repo's conservative asset-based proxy field rather than claiming a guaranteed direct GAAP net-asset amount.

Blank or nonnumeric strings are converted to missing values instead of being coerced to zero.

### Calculated row-level metrics

The main calculated metrics are:

- `analysis_calculated_net_margin_ratio`
- `analysis_calculated_months_of_reserves`
- `analysis_calculated_ntee_broad_code`

The ratio fields are written only when the required denominator is positive. They remain null when the denominator is missing, zero, or negative.

### Classification enrichment

The analysis step enriches `analysis_ntee_code` and `analysis_subsection_code` with a strict fallback order:

1. exact-year NCCS BMF lookup by `ein + tax_year`
2. nearest-year NCCS BMF lookup by `ein`
3. IRS EO BMF lookup by `ein`

All resolved fills preserve:

- source family
- source variant
- source column

### Source-backed and imputed exclusion flags

The source-backed flag family is:

- `analysis_is_hospital`
- `analysis_is_university`
- `analysis_is_political_org`

The fully populated analysis-ready flag family is:

- `analysis_imputed_is_hospital`
- `analysis_imputed_is_university`
- `analysis_imputed_is_political_org`

The imputed columns prefer canonical source-backed values first, then source-backed proxy logic, then high-confidence organization-name matches, then default `False`.

### Region metrics

The region metrics parquet groups by:

- `region`
- `tax_year`
- `analysis_exclusion_variant`

Two exclusion variants are written:

- `all_rows`
- `exclude_imputed_hospital_university_political_org`

Metrics include:

- filing count
- unique EIN count
- revenue, expense, assets, and surplus sums
- normalized totals per filing
- normalized totals per unique EIN
- average net margin
- average months of reserves
- counts of imputed hospital, university, and political-organization flags

### Coverage, mapping, and upload verification

The coverage CSV records populated counts and fill rates by variable and by `tax_year + form_type` slice.

## Coverage Evidence

- Generated coverage CSV: `SWB_321_DATA_ROOT/raw/nccs_efile/metadata/nccs_efile_analysis_variable_coverage.csv`
- Published S3 documentation object: `silver/nccs_efile/analysis/documentation/nccs_efile_pipeline.md`
- Published S3 variable mapping object: `silver/nccs_efile/analysis/variable_mappings/nccs_efile_analysis_variable_mapping.md`
- Published S3 coverage object: `silver/nccs_efile/analysis/quality/coverage/nccs_efile_analysis_variable_coverage.csv`
- Coverage CSVs are generated by step 08 and published as authoritative quality evidence.

The mapping Markdown documents which variables are:

- direct
- calculated
- enriched
- proxy
- imputed
- unavailable

The upload step prints explicit local-vs-S3 byte checks for every analysis artifact and raises if any uploaded object size does not match local bytes.

## Final Analysis Outputs

The official efile analysis package consists of:

- one row-level analysis parquet
- one region metrics parquet
- one coverage CSV
- one mapping Markdown
- this data-processing doc

This package is the canonical efile analysis-ready layer for `2022-2024`.

## Variable Families

- Direct-source variables: `analysis_total_revenue_amount`, `analysis_total_expense_amount`, `analysis_total_assets_amount`, `analysis_calculated_surplus_amount`, `analysis_is_hospital`, `analysis_is_university`
- Calculated variables: `analysis_calculated_net_margin_ratio`, `analysis_calculated_months_of_reserves`, `analysis_calculated_ntee_broad_code`
- Enriched variables: `analysis_ntee_code`, `analysis_subsection_code`, and their provenance columns
- Proxy variables: `analysis_net_asset_amount`, `analysis_is_political_org`
- Imputed variables: `analysis_imputed_is_hospital`, `analysis_imputed_is_university`, `analysis_imputed_is_political_org`
- Explicitly unavailable variables remain unavailable rather than being weakly backfilled from GT

## Testing And Verification

- Unit tests live in `tests/nccs_efile/test_nccs_efile_common.py`.
- The analysis tests cover row preservation, year restriction, numeric extraction, classification fallback behavior, calculated metrics, and region aggregation.
- Operational verification includes raw source/local/S3 byte checks, annual benchmark upload flows, and analysis upload byte checks.

## Current Caveats

- The current efile benchmark layer does not expose GT-style contribution or grant component fields, so those variables remain unavailable here.
- `analysis_net_asset_amount` is still an asset-based proxy and should not be confused with a guaranteed raw net-asset field.
- The direct hospital and university fields are source-sparse because the underlying efile flags are sparse. The imputed versions are complete because they continue through proxy/name/default logic.
- The main runner currently executes sequentially rather than exposing GT-style start-step/end-step selection.

## Analysis requirement alignment appendix

| analysis_requirement | source_specific_output | status | rule_or_reason |
| --- | --- | --- | --- |
| Total revenue | analysis_total_revenue_amount | direct | Direct from `harm_revenue_amount` |
| Total expense | analysis_total_expense_amount | direct | Direct from `harm_expenses_amount` |
| Total assets | analysis_total_assets_amount | direct | Direct from `harm_assets_amount` |
| Net asset | analysis_net_asset_amount | proxy | Conservative asset-based proxy from `harm_assets_amount` |
| Net margin | analysis_calculated_net_margin_ratio | calculated | Surplus divided by positive total revenue |
| Months of reserves | analysis_calculated_months_of_reserves | calculated | (Net-asset proxy / positive total expense) * 12 |
| NTEE filed classification code | analysis_ntee_code | enriched | Exact-year, nearest-year, then IRS EO BMF fallback |
| Program service revenue | analysis_program_service_revenue_amount | unavailable | Not present in the current efile benchmark artifact |
| Total contributions | analysis_calculated_total_contributions_amount | unavailable | Contribution-component fields are not present in the current efile benchmark artifact |
| Other contributions | analysis_other_contributions_amount | unavailable | Contribution-component fields are not present in the current efile benchmark artifact |
| Grants (total amount) | analysis_calculated_grants_total_amount | unavailable | Grant-component fields are not present in the current efile benchmark artifact |
