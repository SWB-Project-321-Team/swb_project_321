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

## Supporting Raw Dictionaries

- `docs/final_preprocessing_docs/technical_docs/source_dictionaries/givingtuesday_datamarts/datamart_catalog.md`.
- `docs/final_preprocessing_docs/technical_docs/source_dictionaries/givingtuesday_datamarts/datamart_fields.md`.
- `docs/final_preprocessing_docs/technical_docs/source_dictionaries/givingtuesday_datamarts/datamart_catalog.csv`.
- `docs/final_preprocessing_docs/technical_docs/source_dictionaries/givingtuesday_datamarts/datamart_fields.csv`.
