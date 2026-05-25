# NCCS BMF Client Documentation

## Background

The NCCS BMF dataset is a registry-style nonprofit dataset published by the National Center for Charitable Statistics at the Urban Institute. It is based on IRS exempt-organization records and provides organization identity, address/geography fields, exempt status context, NTEE classification, subsection information, and limited amount fields.

In this project, NCCS BMF is used for annual nonprofit counts, organization geography, classification support, and project-region analysis. It also helps enrich classification fields for other datasets when those datasets do not contain a complete NTEE or subsection code. Like IRS EO BMF, this is a registry and classification source, not a full Form 990 financial-detail source.

The detailed technical pipeline documentation is `docs/final_preprocessing_docs/technical_docs/pipeline_docs/nccs_bmf_pipeline.md`.

## Data Provenance

- Producer: National Center for Charitable Statistics at the Urban Institute.
- Source pages: `https://nccs.urban.org/nccs/datasets/bmf/` and `https://nccs.urban.org/nccs/catalogs/catalog-bmf.html`.
- Raw monthly source base: `https://nccsdata.s3.us-east-1.amazonaws.com/raw/bmf/`.
- Legacy source base: `https://nccsdata.s3.us-east-1.amazonaws.com/legacy/bmf/`.
- Raw dictionary support: IRS EO BMF dictionary plus generated NCCS header companions.

## Collection Method

NCCS BMF is a public registry dataset derived from IRS exempt-organization records. It is not a voluntary survey and it does not contain complete Form 990 filing detail.

## Inclusion Criteria And Limitations

The pipeline selects representative yearly BMF files, keeps records located in the project counties and regions, and builds the analysis-ready layer for `2022-2024`.

Important limitations:

- BMF is a registry and classification source, not a complete filing-level financial source.
- Expense, net assets, surplus, net margin, months of reserves, contribution detail, and grant detail are not available in the current BMF analysis layer.
- The 2022 legacy file links to an unavailable profile dictionary, so this package includes a generated header companion and documents that limitation.
- Duplicate handling for analysis is applied in the final analysis layer; the upstream yearly project-region files remain source-faithful filtered outputs.

## Download And S3 Storage

The selected yearly raw BMF files and release metadata are downloaded locally, uploaded unchanged to Bronze S3 under `bronze/nccs_bmf/`, and checked against expected file sizes. Filtered yearly Parquet files and exact-year lookup files are stored under `silver/nccs_bmf/`, and analysis documentation is stored under `silver/nccs_bmf/analysis/`.

## Datatype Transformation

Raw annual CSV files are preserved unchanged in Bronze S3. After project-region filtering, the pipeline writes Parquet files for yearly project-region outputs, exact-year lookup files, row-level analysis outputs, geography metrics, and field metrics.

## Data Cleaning

The cleaning process keeps the raw NCCS BMF files unchanged, then creates project-specific analysis files:

- Selects the yearly source files used for the `2022-2024` analysis window.
- Standardizes EINs, ZIP codes, county FIPS codes, region labels, and year fields.
- Uses project geography reference files to keep only records located in the project counties and regions.
- Writes one filtered yearly Parquet file for each selected year.
- Creates exact-year lookup files so classification enrichment can use the right BMF year when possible.
- Combines only the retained project-region files for the final analysis layer.
- When duplicate organization-year records remain in the final analysis layer, keeps one record using documented tie-break rules.
- Builds analyst-facing NTEE and subsection fields from NCCS BMF first, then uses fallback registry information where needed.
- Creates helper flags for hospitals, universities, and political organizations based on classification and conservative name matching.
- Leaves unsupported Form 990 financial fields blank because BMF does not contain the needed source information.

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
