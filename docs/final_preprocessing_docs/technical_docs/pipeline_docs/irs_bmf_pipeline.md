# IRS EO BMF Pipeline

## Status

This document is the authoritative technical reference for the current IRS EO BMF pipeline implemented in this repository.

It reflects the code as it exists now under `python/ingest/irs_bmf/`, including the current benchmark-analysis scope of `2022-2024`, the filter-first benchmark build in step 04, the row-level analysis extraction in step 07, and the official analysis upload in step 08.

This document is intentionally detailed. It is meant to let another engineer or analyst understand:

- where the IRS EO BMF data comes from
- how it is stored locally and in S3
- the exact script order and responsibilities
- how raw state CSVs become benchmark-filtered yearly outputs
- how the final IRS EO BMF analysis package is derived from those filtered outputs
- what is source-direct, fallback-enriched, proxy, imputed, or intentionally left missing

## Pipeline At A Glance

The IRS EO BMF pipeline is a registry, classification, and support-data pipeline. It is not a filing pipeline like GT, and it is not a clean yearly release pipeline like NCCS BMF.

At a high level, the pipeline has these phases:

1. Download raw state-level IRS EO BMF CSVs.
2. Upload the raw state files and metadata to Bronze S3.
3. Strictly verify source/local/S3 byte parity for raw state inputs.
4. Filter each state file to benchmark geography, derive tax year from `TAX_PERIOD`, combine only retained rows, and resolve cross-state duplicate EIN-year groups.
5. Upload the combined and yearly filtered benchmark outputs to Silver S3.
6. Verify local-vs-S3 byte parity for the filtered benchmark outputs.
7. Build the final IRS EO BMF analysis outputs from the filtered benchmark parquets only.
8. Upload the final analysis artifacts and metadata to the dedicated IRS EO BMF analysis prefix.

The main artifact classes are:

- Raw data: one IRS EO BMF CSV per state.
- Metadata/manifests: raw manifest, filter manifest, verification reports, coverage CSV, mapping Markdown, and this processing doc.
- Silver filtered artifacts: one combined filtered parquet plus one benchmark parquet per retained analysis year.
- Final analysis artifacts: the row-level analysis parquet, geography metrics parquet, field metrics parquet, coverage CSV, mapping Markdown, and this processing doc.

This pipeline follows a filtered-only combine contract (combine stages use filtered inputs only):

- raw state files are filtered to benchmark geography before any combine stage
- the final analysis step reads only the filtered benchmark outputs
- direct IRS classification is preserved while analyst-facing fallback-enriched classification remains explicit and auditable

## Documentation Checklist Coverage

- Data producer: the Internal Revenue Service produces the Exempt Organizations Business Master File extract used here.
- Collection method: this is an IRS administrative registry extract, not a voluntary survey and not a full Form 990 filing dataset.
- Inclusion criteria and limitations: the pipeline downloads the configured benchmark-state files for SD, MN, MT, and AZ, filters organizations to benchmark counties, and keeps analysis tax years `2022-2024`; the BMF does not contain full filing financial fields such as total expense, detailed contributions, or reserve measures.
- Datatype transformation: raw IRS CSV files are preserved unchanged in Bronze S3; filtered and analysis-ready outputs are written as Parquet only after benchmark filtering and analysis extraction.
- Data cleaning: cleaning includes ZIP normalization, county/region assignment, benchmark admission, tax-year derivation from `TAX_PERIOD`, cross-state duplicate EIN-year resolution, classification fallback enrichment, and explicit imputed/proxy flags.
- Analysis data dictionary: use `docs/final_preprocessing_docs/technical_docs/analysis_variable_mappings/irs_bmf_analysis_variable_mapping.md` plus `SWB_321_DATA_ROOT/raw/irs_bmf/metadata/irs_bmf_analysis_variable_coverage.csv`; the raw source dictionary is `documentation/final_preprocessing_docs/technical_docs/source_dictionaries/irs_eo_bmf/eo-info.pdf`.

## Contract

- The final analysis-ready layer is limited to tax years `2022-2024` inferred from `TAX_PERIOD`.
- Missing financial amounts remain null; they are not coerced to zero.
- `analysis_ntee_code` and `analysis_subsection_code` remain source-direct IRS EO BMF fields.
- `analysis_ntee_code_fallback_enriched` and `analysis_subsection_code_fallback_enriched` are analyst-facing enriched fields that keep IRS first, then use NCCS BMF exact-year and nearest-year fallback only when the direct IRS value is blank.

## Script Order And Responsibilities

| Step | Script | Primary role | Main inputs | Main outputs | External systems |
| ---- | ------ | ------------ | ----------- | ------------ | ---------------- |
| 01 | `01_fetch_bmf_release.py` | Download raw state CSVs and write a raw manifest | IRS EO BMF state URLs | Raw state CSVs and manifest | HTTP |
| 02 | `02_upload_bmf_release_to_s3.py` | Upload raw state CSVs and manifest to Bronze | Local raw state CSVs and manifest | Bronze raw IRS EO BMF objects | S3 |
| 03 | `03_verify_bmf_source_local_s3.py` | Verify source/local/S3 raw bytes | Source sizes, local files, Bronze objects | Raw verification CSV | HTTP, S3 |
| 04 | `04_filter_bmf_to_benchmark_local.py` | Filter state CSVs to benchmark scope, derive tax year, combine retained rows, resolve cross-state EIN-year duplicates, and write benchmark parquets | Raw state CSVs plus geography crosswalks | Combined filtered parquet, yearly filtered parquets, filter manifest | local files |
| 05 | `05_upload_filtered_bmf_to_s3.py` | Upload combined and yearly filtered benchmark outputs to Silver | Filtered benchmark outputs and metadata | Silver filtered IRS EO BMF objects | S3 |
| 06 | `06_verify_filtered_bmf_local_s3.py` | Verify local-vs-S3 bytes for filtered outputs | Local filtered outputs and Silver objects | Filtered verification CSV | S3 |
| 07 | `07_extract_analysis_variables_local.py` | Build the analysis rowset, geography metrics, field metrics, coverage CSV, mapping doc, and this processing doc | Filtered yearly benchmark parquets plus NCCS BMF lookups | Official IRS EO BMF analysis outputs | local files |
| 08 | `08_upload_analysis_outputs.py` | Upload the analysis artifacts and verify bytes | Analysis artifacts and docs | Silver analysis objects | S3 |

## How Data Is Retrieved

## Exact Source Locations

- IRS EO BMF base URL: `https://www.irs.gov/pub/irs-soi`
- Configured benchmark-state files:
  - `sd`: `https://www.irs.gov/pub/irs-soi/eo_sd.csv`
  - `mn`: `https://www.irs.gov/pub/irs-soi/eo_mn.csv`
  - `mt`: `https://www.irs.gov/pub/irs-soi/eo_mt.csv`
  - `az`: `https://www.irs.gov/pub/irs-soi/eo_az.csv`

## Raw Source Dictionaries

- Official IRS EO BMF dictionary: `documentation/final_preprocessing_docs/technical_docs/source_dictionaries/irs_eo_bmf/eo-info.pdf`
- Local client package copy: `docs/final_preprocessing_docs/technical_docs/source_dictionaries/irs_eo_bmf/eo-info.pdf`
- Applies to the raw IRS EO BMF state CSVs used by this pipeline.

### Step 01: state-file retrieval

The raw source is one IRS EO BMF state CSV per file.

The fetch step downloads the configured state files and writes a raw manifest that records:

- the state code
- the raw filename
- the source URL
- source and local byte metadata

### Step 02 and Step 03: Bronze upload and raw verification

After fetch, the pipeline uploads the raw state files and manifest to Bronze and then verifies source/local/S3 byte parity.

This preserves:

- local raw state files
- Bronze raw copies
- a verification CSV documenting whether source, local, and S3 bytes agree

## Where Data Is Stored

### Local artifacts

- Combined filtered parquet: `SWB_321_DATA_ROOT/staging/irs_bmf/irs_bmf_combined_filtered.parquet`
- Analysis parquet: `SWB_321_DATA_ROOT/staging/irs_bmf/irs_bmf_analysis_variables.parquet`
- Geography metrics parquet: `SWB_321_DATA_ROOT/staging/irs_bmf/irs_bmf_analysis_geography_metrics.parquet`
- Field metrics parquet: `SWB_321_DATA_ROOT/staging/irs_bmf/irs_bmf_analysis_field_metrics.parquet`
- Coverage CSV: `SWB_321_DATA_ROOT/raw/irs_bmf/metadata/irs_bmf_analysis_variable_coverage.csv`
- Mapping Markdown: `docs/final_preprocessing_docs/technical_docs/analysis_variable_mappings/irs_bmf_analysis_variable_mapping.md`
- Data-processing Markdown: `docs/final_preprocessing_docs/technical_docs/pipeline_docs/irs_bmf_pipeline.md`

### S3 layout

- Raw prefix: `bronze/irs990/bmf`
- Raw metadata prefix: `bronze/irs990/bmf/metadata`
- Filtered benchmark prefix: `silver/irs990/bmf`
- Filtered metadata prefix: `silver/irs990/bmf/metadata`
- Analysis prefix: `silver/irs990/bmf/analysis`
- Analysis documentation prefix: `silver/irs990/bmf/analysis/documentation`
- Analysis variable mapping prefix: `silver/irs990/bmf/analysis/variable_mappings`
- Analysis coverage prefix: `silver/irs990/bmf/analysis/quality/coverage`

## Step-By-Step Transformation

### Step 04: filter first, then combine

Step 04 is the key upstream curation step.

For each raw state file, it:

1. normalizes ZIP and state fields
2. filters rows to benchmark counties and regions
3. derives `tax_year` from `TAX_PERIOD`
4. discards rows outside the analysis-ready year scope when appropriate
5. appends only the retained benchmark rows into the combined filtered dataset

This means the final combined filtered parquet is built only from already-admitted rows.

### Cross-state duplicate EIN-year resolution

After filtering, the combined retained rowset may still contain cross-state duplicate `ein + tax_year` groups.

These are resolved upstream in the filtered benchmark build by preferring:

1. the row whose state best matches the benchmark geography state
2. the row with the richest direct field population
3. a stable lexical tie-break on source file name

The final analysis build expects zero duplicate `ein + tax_year` groups in the filtered yearly benchmark outputs.

### Filtered benchmark outputs

The pipeline writes:

- one combined filtered IRS EO BMF parquet
- one filtered yearly benchmark parquet per retained tax year
- a filter manifest describing counts by state, year derivation, and duplicate resolution

These filtered artifacts are the stable upstream inputs for the final analysis build.

### Step 07: analysis build from filtered outputs only

The analysis step reads only the filtered yearly benchmark parquets for tax years `2022-2024`.

It does not reopen the raw state CSVs.

The final row grain is one row per resolved `ein + tax_year`.

### Direct financial and classification extraction

The source-direct IRS EO BMF fields are:

- `analysis_total_revenue_amount`
- `analysis_total_assets_amount`
- `analysis_total_income_amount`
- `analysis_ntee_code`
- `analysis_subsection_code`

These fields preserve their direct IRS provenance columns. Missing source values remain null rather than being coerced to zero or overwritten.

### Analyst-facing fallback-enriched classification fields

For analysis convenience, the pipeline also builds:

- `analysis_ntee_code_fallback_enriched`
- `analysis_subsection_code_fallback_enriched`

Their fallback order is:

1. direct IRS EO BMF value
2. exact-year NCCS BMF
3. nearest-year NCCS BMF

This means the package keeps both:

- the source-direct IRS classification fields
- the more complete analyst-facing resolved classification fields

### Proxy and imputed flags

The source-backed or proxy exclusion flags are:

- `analysis_is_hospital`
- `analysis_is_university`
- `analysis_is_political_org`

These are derived from the fallback-enriched classification fields, not from the direct-only IRS fields.

The complete analysis-ready flag family is:

- `analysis_imputed_is_hospital`
- `analysis_imputed_is_university`
- `analysis_imputed_is_political_org`

The imputed fields use the same conservative name-based helper pattern used in the other source-specific analysis packages.

### Geography metrics

The geography metrics parquet aggregates the row-level analysis dataset by:

- geography level
- region
- optional county FIPS
- tax year
- exclusion variant

It includes counts, unique EIN counts, financial sums, normalized totals, and counts of imputed exclusion flags.

### Field metrics

The field metrics parquet aggregates by:

- region
- tax year
- exclusion variant
- `analysis_calculated_ntee_broad_code`

It reports organization count, financial sums, and share-of-region metrics to support field-composition analysis.

### Coverage, mapping, and upload verification

The coverage CSV records fill rates for direct IRS fields, fallback-enriched classification fields, and the proxy/imputed exclusion flags.

## Coverage Evidence

- Generated coverage CSV: `SWB_321_DATA_ROOT/raw/irs_bmf/metadata/irs_bmf_analysis_variable_coverage.csv`
- Published S3 documentation object: `silver/irs990/bmf/analysis/documentation/irs_bmf_pipeline.md`
- Published S3 variable mapping object: `silver/irs990/bmf/analysis/variable_mappings/irs_bmf_analysis_variable_mapping.md`
- Published S3 coverage object: `silver/irs990/bmf/analysis/quality/coverage/irs_bmf_analysis_variable_coverage.csv`
- Coverage CSVs are generated by step 07 and published as authoritative quality evidence.

The mapping Markdown documents which fields are:

- direct
- enriched
- calculated
- proxy
- imputed
- unavailable

The upload step verifies local-vs-S3 bytes for every analysis artifact before accepting the upload as complete.

## Final Analysis Outputs

The official IRS EO BMF analysis package consists of:

- one row-level analysis parquet
- one geography metrics parquet
- one field metrics parquet
- one coverage CSV
- one mapping Markdown
- this data-processing doc

This package is intended as a registry, geography, and classification support layer rather than a full filing-detail pipeline.

## Testing And Verification

- Unit tests live in `tests/irs_bmf/test_irs_bmf_common.py`.
- The tests cover filter-before-combine behavior, tax-year derivation, duplicate resolution, direct extraction, fallback-enriched classification, proxy/imputed flags, and upload registration.
- Operational verification includes raw source/local/S3 byte checks, filtered benchmark verification, and analysis upload byte checks.

## Current Caveats

- IRS EO BMF is a state-snapshot registry source, not a filing-level dataset.
- `tax_year` is inferred from `TAX_PERIOD`, so its temporal semantics are weaker than a true annual filing pipeline.
- Financial completeness is weaker than NCCS BMF and much weaker than GT or efile.
- `analysis_ntee_code_fallback_enriched` is the analyst-facing resolved NTEE field; `analysis_ntee_code` remains intentionally source-direct and may be sparser.

## Analysis requirement alignment appendix

| analysis_requirement | source_specific_output | status | rule_or_reason |
| --- | --- | --- | --- |
| Total revenue | analysis_total_revenue_amount | direct | Direct IRS EO BMF amount field |
| Total assets | analysis_total_assets_amount | direct | Direct IRS EO BMF amount field |
| Total income | analysis_total_income_amount | direct | Direct IRS EO BMF amount field |
| NTEE filed classification code | analysis_ntee_code | direct | Direct IRS EO BMF NTEE field |
| NTEE filed classification code (analysis-ready resolved) | analysis_ntee_code_fallback_enriched | enriched | IRS first, then NCCS BMF exact-year, then NCCS BMF nearest-year |
| Broad NTEE field classification code | analysis_calculated_ntee_broad_code | calculated | First letter of the fallback-enriched NTEE code |
| Hospital flag | analysis_is_hospital | proxy | NTEE proxy from fallback-enriched classification |
| University flag | analysis_is_university | proxy | NTEE proxy from fallback-enriched classification |
| Political organization flag | analysis_is_political_org | proxy | Subsection proxy from fallback-enriched classification |
| Total expense | analysis_total_expense_amount | unavailable | IRS EO BMF does not provide this expense concept in the current pipeline |
