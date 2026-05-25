# NCCS Efile Client Documentation

## Background

The NCCS efile dataset contains parsed public electronic IRS Form 990 filing tables. It is based on required electronic filings submitted by nonprofit organizations and then published in a structured form by NCCS.

In this project, NCCS efile is used to create annual filing-level datasets for the project counties and regions. It provides organization identity, filing year, filing form, geography, revenue, expenses, assets, and selected filing context fields. It is more financially detailed than registry sources, but it does not expose every contribution or grant component needed for all revenue-source questions.

The detailed technical pipeline documentation is `docs/final_preprocessing_docs/technical_docs/pipeline_docs/nccs_efile_pipeline.md`.

## Data Provenance

- Producer: National Center for Charitable Statistics public efile release.
- Dataset page: `https://nccs.urban.org/nccs/datasets/efile/`.
- Catalog page: `https://nccs.urban.org/nccs/catalogs/catalog-efile-v2_1.html`.
- Public data base: `https://nccs-efile.s3.us-east-1.amazonaws.com/public/efile_v2_1/`.
- Raw dictionary: `docs/final_preprocessing_docs/technical_docs/source_dictionaries/nccs_efile/data-dictionary.html`.

## Collection Method

The source tables are derived from public electronic IRS Form 990 filings. They represent required tax filings for organizations that file electronically, not voluntary survey responses.

## Inclusion Criteria And Limitations

The pipeline includes the required HEADER, SUMMARY, and Schedule A table families for tax years `2022-2024`. It first determines which filings are located in the project counties and regions using HEADER rows, then joins in the additional tables needed for analysis.

Important limitations:

- The final row-level output keeps one selected filing per EIN, tax year, and filing form.
- Some source fields are sparse because not every filing has every table or field.
- The current efile analysis layer does not include GivingTuesday-style contribution and grant component fields, so those fields remain unavailable here rather than being weakly filled from another source.
- Some classification fields are supplemented from NCCS BMF and IRS EO BMF when needed.

## Download And S3 Storage

Annual raw CSV tables and discovery metadata are downloaded locally, uploaded unchanged to Bronze S3 under `bronze/nccs_efile/`, and checked against expected file sizes. Annual project-region outputs are stored under `silver/nccs_efile/`, and analysis documentation is stored under `silver/nccs_efile/analysis/`.

## Datatype Transformation

Raw annual CSV tables are preserved unchanged in Bronze S3. After filtering to the project geography and selecting the retained filings, the pipeline writes annual Parquet files and final analysis Parquet files.

## Data Cleaning

The cleaning process keeps the raw efile tables unchanged, then creates project-specific filing datasets:

- Selects the required efile tables for tax years `2022-2024`.
- Uses HEADER rows to identify organization identity, filing year, filing form, ZIP, state, county, and region.
- Standardizes EINs, ZIP codes, tax years, filing-form values, county FIPS codes, and region labels.
- Keeps only filings located in the project counties and regions before joining the wider filing tables.
- When multiple filings exist for the same organization and tax year, keeps the selected filing using documented ranking rules.
- Joins SUMMARY and Schedule A information only for retained project-region filings.
- Calculates analysis fields such as revenue, expenses, surplus, net margin, months of reserves, and asset-based support measures where source fields allow.
- Supplements missing NTEE and subsection classifications from NCCS BMF and IRS EO BMF when needed.
- Creates helper flags for hospitals, universities, and political organizations so analysts can apply consistent exclusions.
- Leaves unsupported contribution and grant fields blank because those fields are not reliably present in the current efile analysis source.

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
