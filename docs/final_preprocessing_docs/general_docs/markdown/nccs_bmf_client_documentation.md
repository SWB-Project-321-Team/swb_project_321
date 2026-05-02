# NCCS BMF Client Documentation

## Background

The NCCS BMF dataset is used as a nonprofit registry and classification layer for annual organization counts, geography, subsection logic, and NTEE-based field composition. In this project it complements IRS EO BMF and supports downstream classification enrichment.

The detailed technical pipeline documentation is `docs/final_preprocessing_docs/technical_docs/pipeline_docs/nccs_bmf_pipeline.md`.

## Data Provenance

- Producer: National Center for Charitable Statistics at the Urban Institute.
- Source pages: `https://nccs.urban.org/nccs/datasets/bmf/` and `https://nccs.urban.org/nccs/catalogs/catalog-bmf.html`.
- Raw monthly source base: `https://nccsdata.s3.us-east-1.amazonaws.com/raw/bmf/`.
- Legacy source base: `https://nccsdata.s3.us-east-1.amazonaws.com/legacy/bmf/`.
- Raw dictionary support: IRS EO BMF dictionary plus generated NCCS header companions.

## Collection Method

NCCS BMF is a public BMF registry dataset derived from IRS exempt-organization records. It is not a voluntary survey and it does not contain complete Form 990 filing detail.

## Inclusion Criteria And Limitations

The pipeline selects representative yearly BMF files, filters each year to benchmark geography, and builds the analysis-ready layer for `2022-2024`. The 2022 legacy file links to an unavailable profile dictionary, so this package includes a generated header companion and preserves that limitation explicitly.

## Download And S3 Storage

The selected yearly raw BMF files and release metadata are downloaded locally, uploaded unchanged to Bronze S3 under `bronze/nccs_bmf/`, and verified with local/S3 byte checks. Filtered yearly parquets and exact-year lookup files are stored under `silver/nccs_bmf/`, and analysis documentation is stored under `silver/nccs_bmf/analysis/`.

## Datatype Transformation

Raw annual CSV files remain unchanged in Bronze S3. The pipeline writes Parquet after benchmark filtering: yearly benchmark Parquet, exact-year lookup Parquet, row-level analysis Parquet, geography metrics Parquet, and field metrics Parquet.

## Data Cleaning

Cleaning includes annual source selection, ZIP normalization, county and benchmark-region assignment, benchmark filtering, exact-year lookup construction, analysis-only duplicate resolution, IRS/NCCS classification fallback enrichment, and imputed/proxy flags for hospital, university, and political-organization exclusions.

## Analysis-Ready Outputs

- Row-level analysis dataset: `silver/nccs_bmf/analysis/nccs_bmf_analysis_variables.parquet`.
- Geography metrics: `silver/nccs_bmf/analysis/nccs_bmf_analysis_geography_metrics.parquet`.
- Field metrics: `silver/nccs_bmf/analysis/nccs_bmf_analysis_field_metrics.parquet`.
- Pipeline documentation: `silver/nccs_bmf/analysis/documentation/nccs_bmf_pipeline.md`.
- Variable mapping: `silver/nccs_bmf/analysis/variable_mappings/nccs_bmf_analysis_variable_mapping.md`.
- Coverage evidence: `silver/nccs_bmf/analysis/quality/coverage/nccs_bmf_analysis_variable_coverage.csv`.

## Analysis Data Dictionary

Use `docs/final_preprocessing_docs/technical_docs/analysis_variable_mappings/nccs_bmf_analysis_variable_mapping.md` as the analysis-ready data dictionary. Use `docs/final_preprocessing_docs/technical_docs/quality/coverage_evidence/nccs_bmf_analysis_variable_coverage.csv` to confirm populated counts and coverage.

## Supporting Raw Dictionaries

- `docs/final_preprocessing_docs/technical_docs/source_dictionaries/irs_eo_bmf/eo-info.pdf`.
- `docs/final_preprocessing_docs/technical_docs/source_dictionaries/nccs_bmf_raw_monthly/2026-03-BMF_header_dictionary.csv`.
- `docs/final_preprocessing_docs/technical_docs/source_dictionaries/nccs_bmf_legacy_2022/dd_unavailable.html`.
- `docs/final_preprocessing_docs/technical_docs/source_dictionaries/nccs_bmf_legacy_2022/BMF-2022-08-501CX-NONPROFIT-PX_header_dictionary.csv`.
