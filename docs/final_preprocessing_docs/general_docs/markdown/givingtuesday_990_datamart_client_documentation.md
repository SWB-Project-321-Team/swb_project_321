# GivingTuesday 990 DataMart Client Documentation

## Background

The GivingTuesday 990 DataMart pipeline uses the public GivingTuesday DataMart catalog and selected raw DataMart CSV files to build benchmark-filtered filing outputs and a basic-only analysis dataset for `2022-2024`.

The detailed technical pipeline documentation is `docs/final_preprocessing_docs/technical_docs/pipeline_docs/givingtuesday_datamart_pipeline.md`.

## Data Provenance

- Producer: GivingTuesday.
- Source page: `https://990data.givingtuesday.org/datamarts/`.
- Public catalog API: `https://grantstory-api.gtdata.org/v2/public/678d25291eceb5d121099200/items?limit=200`.
- Per-file download URLs are catalog-derived and captured in pipeline manifests.
- Raw dictionary support comes from the exported catalog and field dictionary files.

## Collection Method

The pipeline consumes GivingTuesday DataMart files derived from public nonprofit filing data and GivingTuesday's cataloged standard-field extracts. This repo does not collect voluntary survey responses and does not build the GT analysis layer from IRS XML directly.

## Inclusion Criteria And Limitations

The pipeline selects the Combined Forms DataMart plus Basic Fields files for Forms 990, 990-EZ, and 990-PF, restricts the current analysis window to tax years `2022-2024`, and applies benchmark geography/ROI admission before heavy combine stages. The final analysis layer is GT basic-only and uses documented NCCS/IRS classification enrichment because the GT raw fields used here do not carry the required NTEE fields directly.

## Download And S3 Storage

Required raw GT CSVs and metadata are downloaded locally, uploaded unchanged to Bronze S3 under `bronze/givingtuesday_990/datamarts/raw/` and `bronze/givingtuesday_990/datamarts/metadata/`, and verified with source/local/S3 byte checks. Pre-Silver and Silver filing outputs are stored under GT Bronze/Silver filing prefixes, and analysis documentation is stored under `silver/givingtuesday_990/analysis/`.

## Datatype Transformation

Raw GT CSV files remain unchanged in Bronze S3. Later stages create a normalized Combined cache Parquet, admitted pre-Silver Parquet files, Silver benchmark Parquet files, and final analysis Parquet outputs.

## Data Cleaning

Cleaning includes catalog-driven required-file selection, raw header-based field dictionary export, low-risk field normalization, ROI key construction, benchmark geography assignment, deterministic filing deduplication, Basic/Combined curation, calculated revenue/contribution/grant fields, NCCS/IRS classification enrichment, and imputed/proxy exclusion-support flags.

## Analysis-Ready Outputs

- Row-level analysis dataset: `silver/givingtuesday_990/analysis/givingtuesday_990_basic_allforms_analysis_variables.parquet`.
- Region metrics: `silver/givingtuesday_990/analysis/givingtuesday_990_basic_allforms_analysis_region_metrics.parquet`.
- Pipeline documentation: `silver/givingtuesday_990/analysis/documentation/givingtuesday_datamart_pipeline.md`.
- Variable mapping: `silver/givingtuesday_990/analysis/variable_mappings/givingtuesday_basic_analysis_variable_mapping.md`.
- Coverage evidence: `silver/givingtuesday_990/analysis/quality/coverage/givingtuesday_990_basic_allforms_analysis_variable_coverage.csv`.

## Analysis Data Dictionary

Use `docs/final_preprocessing_docs/technical_docs/analysis_variable_mappings/givingtuesday_basic_analysis_variable_mapping.md` as the analysis-ready data dictionary. Use `docs/final_preprocessing_docs/technical_docs/quality/coverage_evidence/givingtuesday_990_basic_allforms_analysis_variable_coverage.csv` to confirm populated counts and coverage.

### Section 3 Q9 revenue-source variables (GivingTuesday)

For the analysis-plan question on revenue sources (Forms 990, 990-EZ, 990-PF), the Step 13 parquet exposes the following analysis variables. Form 990 filers can be decomposed into the IRS Part VIII Line 1 sub-channels; Form 990-EZ and Form 990-PF report only a single contributions total.

- **`analysis_total_revenue_amount`** — total revenue (form-aware).
- **`analysis_program_service_revenue_amount`** — program service revenue (990 and 990-EZ; null on typical PF rows).
- **`analysis_total_contributions_amount`** — total contributions (990 Line 1h via `TOTACASHCONT`; 990-EZ Part I Line 1 via `CONGIFGRAETC`; 990-PF Part I Line 1 via `STREACGRTOIN`); not the sum of overlapping contribution sub-lines.

For Form 990 filers, the IRS Part VIII Line 1 sub-channels are exposed as separate columns:

- **`analysis_federated_campaigns_amount`** — Line 1a (`FEDERACAMPAI`). Institutional channel.
- **`analysis_membership_dues_amount`** — Line 1b (`MEMBERDUESUE`). Likely-individual channel.
- **`analysis_fundraising_events_contributions_amount`** — Line 1c (`FUNDRAEVENTS`). Likely-individual channel.
- **`analysis_related_org_contributions_amount`** — Line 1d (`RELATEORGANI`). Institutional channel.
- **`analysis_government_grants_amount`** — Line 1e (`GOVERNGRANTS`). The only Line 1 sub-component the IRS labels as unambiguously institutional.
- **`analysis_other_contributions_amount`** — Line 1f (`ALLOOTHECONT`). The deliberately-mixed bucket: lumps individual gifts together with private foundation grants, donor-advised fund (DAF) distributions, corporate gifts, and bequests. **The IRS does not separate donor types in this line.** Cannot be split further without Schedule B.

The Section 3 Q9 plan-language "Other contributions (foundation grants etc.)" segment is published as:

- **`analysis_calculated_grants_total_amount`** — institutional-channel contributions aggregate, equal to `FEDERACAMPAI` + `RELATEORGANI` + `GOVERNGRANTS` (Form 990 Lines 1a + 1d + 1e). Form 990 only; 990-EZ / 990-PF stay null. Earlier versions of this aggregate accidentally summed `FOREGRANTOTA` and `GRANTOORORGA`, which are Form 990 Part IX (grants paid out by the filer) rather than contributions received; that mix has been removed.

**Important interpretation caveat.** Form 990 line items do not allow a clean separation of contributions from individuals versus contributions from other organizations. Lines 1a, 1d, and 1e are unambiguously institutional and Lines 1b and 1c are predominantly individual, but Line 1f is irreducibly mixed at the line-item level, and Forms 990-EZ and 990-PF expose only a single contributions total. Reading `government_grants_received` and `other_institutional_contributions` as institutional, and `individual_likely_contributions` as the closest proxy for individual giving, is defensible; reading the Line 1f mixed bucket as either pure-individual or pure-institutional is not. A cleaner individual-versus-institutional split would require Form 990 Schedule B, which is not currently in this datamart.

Component-only columns (`analysis_cash_contributions_amount`, `analysis_noncash_contributions_amount`, and the Line 1 sub-channels listed above) remain documented as diagnostics in the variable mapping.

## Supporting Raw Dictionaries

- `docs/final_preprocessing_docs/technical_docs/source_dictionaries/givingtuesday_datamarts/datamart_catalog.md`.
- `docs/final_preprocessing_docs/technical_docs/source_dictionaries/givingtuesday_datamarts/datamart_fields.md`.
- `docs/final_preprocessing_docs/technical_docs/source_dictionaries/givingtuesday_datamarts/datamart_catalog.csv`.
- `docs/final_preprocessing_docs/technical_docs/source_dictionaries/givingtuesday_datamarts/datamart_fields.csv`.
