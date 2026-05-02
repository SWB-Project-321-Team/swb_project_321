# NCCS Efile Client Documentation

## Background

The NCCS efile dataset provides parsed public efile Form 990 table data. In this project it is converted into benchmark-filtered annual filing rowsets and a row-level analysis-ready efile dataset.

The detailed technical pipeline documentation is `docs/final_preprocessing_docs/technical_docs/pipeline_docs/nccs_efile_pipeline.md`.

## Data Provenance

- Producer: National Center for Charitable Statistics public efile release.
- Dataset page: `https://nccs.urban.org/nccs/datasets/efile/`.
- Catalog page: `https://nccs.urban.org/nccs/catalogs/catalog-efile-v2_1.html`.
- Public data base: `https://nccs-efile.s3.us-east-1.amazonaws.com/public/efile_v2_1/`.
- Raw dictionary: `docs/final_preprocessing_docs/technical_docs/source_dictionaries/nccs_efile/data-dictionary.html`.

## Collection Method

The source tables are derived from public electronic IRS Form 990 filing data. They represent required tax filings for filing organizations rather than voluntary survey responses.

## Inclusion Criteria And Limitations

The pipeline includes the required HEADER, SUMMARY, and Schedule A table families for tax years `2022-2024`. Geography admission is determined from HEADER rows before the wide table join, and one selected filing is retained per EIN-tax-year. Fields not reliably present in the efile source remain unavailable rather than being weakly backfilled from other datasets.

## Download And S3 Storage

Annual raw CSV tables and discovery metadata are downloaded locally, uploaded unchanged to Bronze S3 under `bronze/nccs_efile/`, and verified with source/local/S3 byte checks. Annual benchmark outputs are stored under `silver/nccs_efile/`, and analysis documentation is stored under `silver/nccs_efile/analysis/`.

## Datatype Transformation

Raw annual CSV tables remain unchanged in Bronze S3. After benchmark filtering and selected-filing construction, annual benchmark outputs and the final analysis outputs are written as Parquet.

## Data Cleaning

Cleaning includes HEADER-based benchmark geography filtering, ZIP/county/region mapping, deterministic filing ranking, selected-filing retention, SUMMARY and Schedule A joins only for retained filings, calculated metrics, classification enrichment, and imputed/proxy exclusion-support flags.

## Analysis-Ready Outputs

- Row-level analysis dataset: `silver/nccs_efile/analysis/nccs_efile_analysis_variables.parquet`.
- Region metrics: `silver/nccs_efile/analysis/nccs_efile_analysis_geography_metrics.parquet`.
- Pipeline documentation: `silver/nccs_efile/analysis/documentation/nccs_efile_pipeline.md`.
- Variable mapping: `silver/nccs_efile/analysis/variable_mappings/nccs_efile_analysis_variable_mapping.md`.
- Coverage evidence: `silver/nccs_efile/analysis/quality/coverage/nccs_efile_analysis_variable_coverage.csv`.

## Analysis Data Dictionary

Use `docs/final_preprocessing_docs/technical_docs/analysis_variable_mappings/nccs_efile_analysis_variable_mapping.md` as the analysis-ready data dictionary. Use `docs/final_preprocessing_docs/technical_docs/quality/coverage_evidence/nccs_efile_analysis_variable_coverage.csv` to confirm populated counts and coverage.

## Supporting Raw Dictionaries

- `docs/final_preprocessing_docs/technical_docs/source_dictionaries/nccs_efile/data-dictionary.html`.
- `docs/final_preprocessing_docs/technical_docs/source_dictionaries/nccs_efile/data-dictionary_variables.csv`.
