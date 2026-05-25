# NCCS 990 Postcard Client Documentation

## Background

The NCCS 990 Postcard dataset contains Form 990-N/e-Postcard records. Form 990-N is a short annual notice used by eligible small tax-exempt organizations that are not required to file a full Form 990 or Form 990-EZ.

In this project, the postcard data is used as a small-filer support layer. It helps identify smaller organizations, their location, filing/tax year, termination status, and whether they reported gross receipts under the postcard threshold. It is useful for supplemental counts, geography, and classification support. It is not useful for detailed financial analysis because the postcard filing does not contain full revenue, expense, asset, contribution, or grant fields.

The detailed technical pipeline documentation is `docs/final_preprocessing_docs/technical_docs/pipeline_docs/nccs_990_postcard_pipeline.md`.

## Data Provenance

- Producer: NCCS publishes the monthly postcard files; the source fields correspond to IRS Form 990-N/e-Postcard data.
- Dataset page: `https://nccs.urban.org/nccs/datasets/postcard/`.
- Raw monthly base: `https://nccsdata.s3.us-east-1.amazonaws.com/raw/e-postcard/`.
- Raw dictionary: `docs/final_preprocessing_docs/technical_docs/source_dictionaries/irs_990n_postcard/990n-data-dictionary.pdf`.

## Collection Method

Form 990-N/e-Postcard is a required IRS annual notice for eligible small exempt organizations. It is not a voluntary survey and does not contain full return-level financial fields.

## Inclusion Criteria And Limitations

The pipeline uses the latest available monthly postcard snapshot files, keeps records located in the project counties and regions, keeps the latest retained row per EIN, and limits the final analysis layer to filing tax years `2022-2024`.

Important limitations:

- The postcard output is a latest-snapshot support file, not a true annual filing panel.
- It is intended for supplemental small-filer counts, geography, status, and classification support.
- Revenue, expenses, assets, net assets, reserves, contributions, and grants are unavailable because Form 990-N/e-Postcard does not collect those detailed financial fields.
- Some classification fields are supplemented from NCCS BMF and IRS EO BMF because postcard records do not always contain the classification detail needed for analysis.

## Download And S3 Storage

Monthly raw postcard CSVs and metadata are downloaded locally, uploaded unchanged to Bronze S3 under `bronze/nccs_990/postcard/`, and checked against expected file sizes. Filtered retained derivatives are stored under `silver/nccs_990/postcard/`, and analysis documentation is stored under `silver/nccs_990/postcard/analysis/`.

## Datatype Transformation

Raw monthly CSV files are preserved unchanged in Bronze S3. After filtering to the project geography and creating the retained latest-snapshot file, the pipeline writes filtered CSV derivatives and final analysis Parquet outputs.

## Data Cleaning

The cleaning process keeps the raw monthly postcard files unchanged, then creates a project-specific support dataset:

- Discovers the monthly postcard files available for the snapshot year.
- Standardizes EINs, ZIP codes, state values, tax years, county FIPS codes, and region labels.
- Uses organization ZIP first, then officer ZIP as a fallback when assigning geography.
- Keeps only records located in the project counties and regions.
- Combines retained records from the monthly files after project geography filtering.
- Keeps the latest retained row per EIN for the snapshot-based support file.
- Creates a derivative limited to filing tax years `2022-2024` for analysis.
- Supplements missing NTEE and subsection classifications from NCCS BMF and IRS EO BMF when needed.
- Creates helper flags for hospitals, universities, and political organizations so analysts can apply consistent exclusions.
- Preserves direct postcard support fields, such as gross receipts under the postcard threshold and termination status, when those fields are present.
- Leaves detailed financial fields blank because the postcard source does not contain them.

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
