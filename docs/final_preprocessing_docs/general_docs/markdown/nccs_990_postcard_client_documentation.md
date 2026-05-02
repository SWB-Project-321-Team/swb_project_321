# NCCS 990 Postcard Client Documentation

## Background

The NCCS 990 Postcard dataset is used as a small-filer support layer. It captures Form 990-N/e-Postcard records but does not provide the full financial detail available in Form 990, 990-EZ, or 990-PF sources.

The detailed technical pipeline documentation is `docs/final_preprocessing_docs/technical_docs/pipeline_docs/nccs_990_postcard_pipeline.md`.

## Data Provenance

- Producer: NCCS publishes the monthly postcard files; the source fields correspond to IRS Form 990-N/e-Postcard data.
- Dataset page: `https://nccs.urban.org/nccs/datasets/postcard/`.
- Raw monthly base: `https://nccsdata.s3.us-east-1.amazonaws.com/raw/e-postcard/`.
- Raw dictionary: `docs/final_preprocessing_docs/technical_docs/source_dictionaries/irs_990n_postcard/990n-data-dictionary.pdf`.

## Collection Method

Form 990-N/e-Postcard is a required IRS annual notice for eligible small exempt organizations. It is not a voluntary survey and does not contain full return-level financial fields.

## Inclusion Criteria And Limitations

The pipeline uses the latest available postcard snapshot-year monthly files, filters each monthly file to benchmark geography, keeps the latest retained row per EIN, and limits the final analysis layer to filing tax years `2022-2024`. Revenue, expense, assets, reserves, contribution, and grant variables remain unavailable because they are not present in the source.

## Download And S3 Storage

Monthly raw postcard CSVs and metadata are downloaded locally, uploaded unchanged to Bronze S3 under `bronze/nccs_990/postcard/`, and verified with source/local/S3 byte checks. Filtered retained derivatives are stored under `silver/nccs_990/postcard/`, and analysis documentation is stored under `silver/nccs_990/postcard/analysis/`.

## Datatype Transformation

Raw monthly CSV files remain unchanged in Bronze S3. After benchmark filtering and retained-row construction, filtered derivatives are written as CSV and final analysis outputs are written as Parquet.

## Data Cleaning

Cleaning includes monthly source discovery, ZIP normalization, organization-ZIP then officer-ZIP geography fallback, benchmark geography admission, latest-snapshot EIN deduplication, retained tax-year filtering, classification enrichment, and imputed/proxy exclusion-support flags.

## Analysis-Ready Outputs

- Row-level analysis dataset: `silver/nccs_990/postcard/analysis/nccs_990_postcard_analysis_variables.parquet`.
- Geography metrics: `silver/nccs_990/postcard/analysis/nccs_990_postcard_analysis_geography_metrics.parquet`.
- Pipeline documentation: `silver/nccs_990/postcard/analysis/documentation/nccs_990_postcard_pipeline.md`.
- Variable mapping: `silver/nccs_990/postcard/analysis/variable_mappings/nccs_990_postcard_analysis_variable_mapping.md`.
- Coverage evidence: `silver/nccs_990/postcard/analysis/quality/coverage/nccs_990_postcard_analysis_variable_coverage.csv`.

## Analysis Data Dictionary

Use `docs/final_preprocessing_docs/technical_docs/analysis_variable_mappings/nccs_990_postcard_analysis_variable_mapping.md` as the analysis-ready data dictionary. Use `docs/final_preprocessing_docs/technical_docs/quality/coverage_evidence/nccs_990_postcard_analysis_variable_coverage.csv` to confirm populated counts and coverage.

## Supporting Raw Dictionaries

- `docs/final_preprocessing_docs/technical_docs/source_dictionaries/irs_990n_postcard/990n-data-dictionary.pdf`.
- `docs/final_preprocessing_docs/technical_docs/source_dictionaries/irs_990n_postcard/2026-03-E-POSTCARD_header_dictionary.csv`.
