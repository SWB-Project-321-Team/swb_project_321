# IRS EO BMF Client Documentation

## Background

The IRS Exempt Organizations Business Master File (EO BMF) is an IRS registry of tax-exempt organizations. It identifies organizations recognized by the IRS as exempt and includes organization identity, address information, exempt subsection, classification codes, ruling/status context, and limited financial fields reported in the registry.

In this project, IRS EO BMF is used to support nonprofit counts, organization classification, NTEE and subsection context, and project-region analysis. It is especially useful for registry coverage and classification, but it is not a full annual Form 990 filing dataset. It does not contain the detailed revenue, expense, contribution, grant, or net-asset information that appears in full return-level sources.

The detailed technical pipeline documentation is `docs/final_preprocessing_docs/technical_docs/pipeline_docs/irs_bmf_pipeline.md`.

## Data Provenance

- Producer: Internal Revenue Service.
- Source: IRS Statistics of Income public EO BMF state CSV extracts.
- Downloaded files: `eo_sd.csv`, `eo_mn.csv`, `eo_mt.csv`, and `eo_az.csv`.
- Source URLs: `https://www.irs.gov/pub/irs-soi/eo_{state}.csv`.
- Raw dictionary: `docs/final_preprocessing_docs/technical_docs/source_dictionaries/irs_eo_bmf/eo-info.pdf`.

## Collection Method

The EO BMF is an IRS administrative registry extract. It is not a voluntary survey and it is not a table of complete Form 990 filings. The fields reflect IRS exempt-organization registry information and limited registry financial/classification fields.

## Inclusion Criteria And Limitations

The pipeline includes the configured project states SD, MN, MT, and AZ, then keeps organizations located in the project counties and regions. The analysis-ready IRS EO BMF layer keeps tax years `2022-2024`, inferred from the IRS `TAX_PERIOD` field.

Important limitations:

- The EO BMF is a registry source, not a filing-level source.
- `tax_year` is inferred from `TAX_PERIOD`, so it should not be interpreted exactly the same way as a tax year from a filed Form 990 return.
- The source does not include many Form 990-style financial fields, including total expenses, detailed contributions, grants received, net assets, net margin, and months of reserves.
- The project adds classification helper fields for analysis, but source-direct IRS fields are preserved separately when available.

## Download And S3 Storage

Raw state CSV files are downloaded from IRS public URLs, stored locally, uploaded unchanged to Bronze S3 under `bronze/irs990/bmf/`, and checked against expected file sizes. Filtered project-region outputs are stored under `silver/irs990/bmf/`, and analysis documentation is stored under `silver/irs990/bmf/analysis/`.

## Datatype Transformation

The raw IRS CSV files are preserved unchanged in Bronze S3. After filtering to the project geography and analysis years, the pipeline writes Parquet files. Parquet is used because it is easier and faster to read in Python than large CSV files.

## Data Cleaning

The cleaning process keeps the raw IRS files unchanged, then creates project-specific analysis files:

- Standardizes EINs, state values, ZIP codes, county FIPS codes, and region labels.
- Uses the project geography reference files to keep only records located in the project counties and regions.
- Checks that the organization state is consistent with the ZIP-to-county match when that information is available.
- Derives the analysis year from the first four digits of `TAX_PERIOD`.
- Keeps records in the `2022-2024` analysis window.
- Combines the retained state files only after project geography and year filters have been applied.
- When the same EIN-year appears more than once across state files, keeps one record using documented tie-break rules that prefer the best state match and the most complete record.
- Preserves direct IRS values for revenue, assets, income, NTEE, and subsection when present.
- Supplements missing analyst-facing classification fields from NCCS BMF where appropriate, while retaining provenance for where the value came from.
- Creates helper flags for hospitals, universities, and political organizations so analysts can apply consistent exclusions.
- Leaves unsupported financial measures blank when the source does not contain enough information to calculate them.

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
