# IRS EO BMF Client Documentation

## Background

The IRS Exempt Organizations Business Master File (EO BMF) is a registry-style dataset used here to support nonprofit counts, exempt-organization classification, NTEE/subsection context, and benchmark-region analysis. It is not a full annual Form 990 filing dataset and does not contain the full financial detail available from return-level sources.

The detailed technical pipeline documentation is `docs/final_preprocessing_docs/technical_docs/pipeline_docs/irs_bmf_pipeline.md`.

## Data Provenance

- Producer: Internal Revenue Service.
- Source: IRS Statistics of Income public EO BMF state CSV extracts.
- Downloaded files: `eo_sd.csv`, `eo_mn.csv`, `eo_mt.csv`, and `eo_az.csv`.
- Source URLs: `https://www.irs.gov/pub/irs-soi/eo_{state}.csv`.
- Raw dictionary: `docs/final_preprocessing_docs/technical_docs/source_dictionaries/irs_eo_bmf/eo-info.pdf`.

## Collection Method

The EO BMF is an IRS administrative registry extract. It is not a voluntary survey and it is not a complete Form 990 filing table. The fields reflect IRS exempt-organization registry information and limited registry financial/classification fields.

## Inclusion Criteria And Limitations

The pipeline includes the configured benchmark states SD, MN, MT, and AZ, filters records to the benchmark counties, and keeps analysis tax years `2022-2024` inferred from `TAX_PERIOD`. IRS EO BMF lacks several Form 990-style fields, including total expense, detailed contribution/grant components, net assets, net margin, and months of reserves.

## Download And S3 Storage

Raw state CSV files are downloaded from IRS public URLs, stored locally under the IRS BMF raw data root, uploaded unchanged to Bronze S3 under `bronze/irs990/bmf/`, and verified with source/local/S3 byte checks. Filtered outputs are stored under `silver/irs990/bmf/`, and analysis documentation is stored under `silver/irs990/bmf/analysis/`.

## Datatype Transformation

Raw IRS CSV files remain unchanged in Bronze S3. The pipeline writes Parquet only after benchmark filtering and analysis extraction: combined filtered Parquet, yearly benchmark Parquet, row-level analysis Parquet, geography metrics Parquet, and field metrics Parquet.

## Data Cleaning

Cleaning includes ZIP normalization, county and benchmark-region assignment, benchmark-county filtering, tax-year derivation from `TAX_PERIOD`, filter-before-combine processing, cross-state duplicate EIN-year resolution, IRS-first NTEE/subsection retention, NCCS fallback enrichment for analyst-facing fields, and explicit proxy/imputed classification flags.

## Analysis-Ready Outputs

- Row-level analysis dataset: `silver/irs990/bmf/analysis/irs_bmf_analysis_variables.parquet`.
- Geography metrics: `silver/irs990/bmf/analysis/irs_bmf_analysis_geography_metrics.parquet`.
- Field metrics: `silver/irs990/bmf/analysis/irs_bmf_analysis_field_metrics.parquet`.
- Pipeline documentation: `silver/irs990/bmf/analysis/documentation/irs_bmf_pipeline.md`.
- Variable mapping: `silver/irs990/bmf/analysis/variable_mappings/irs_bmf_analysis_variable_mapping.md`.
- Coverage evidence: `silver/irs990/bmf/analysis/quality/coverage/irs_bmf_analysis_variable_coverage.csv`.

## Analysis Data Dictionary

Use `docs/final_preprocessing_docs/technical_docs/analysis_variable_mappings/irs_bmf_analysis_variable_mapping.md` as the analysis-ready data dictionary. Use `docs/final_preprocessing_docs/technical_docs/quality/coverage_evidence/irs_bmf_analysis_variable_coverage.csv` to confirm populated counts and coverage.

## Supporting Raw Dictionaries

- `docs/final_preprocessing_docs/technical_docs/source_dictionaries/irs_eo_bmf/eo-info.pdf`.
- `docs/final_preprocessing_docs/technical_docs/source_dictionaries/README.md`.
