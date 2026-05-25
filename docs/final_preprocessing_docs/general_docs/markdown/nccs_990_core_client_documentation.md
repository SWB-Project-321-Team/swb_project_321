# NCCS 990 Core Client Documentation

## Background

The NCCS 990 Core dataset is a processed public filing dataset from the National Center for Charitable Statistics at the Urban Institute. It harmonizes information from IRS Form 990, Form 990-EZ, and Form 990-PF filings into standardized Core files.

In this project, NCCS 990 Core is used as a source-specific Form 990 analysis layer. It provides organization identity, filing year, filing context, revenue, expenses, assets, net assets, program service revenue, contribution candidates, and classification support. The pipeline also uses NCCS BMF bridge files to connect Core filing records to the project counties and regions.

The detailed technical pipeline documentation is `docs/final_preprocessing_docs/technical_docs/pipeline_docs/nccs_990_core_pipeline.md`.

## Data Provenance

- Producer: National Center for Charitable Statistics at the Urban Institute.
- Core catalog page: `https://nccs.urban.org/nccs/catalogs/catalog-core.html`.
- BMF bridge catalog page: `https://nccs.urban.org/nccs/catalogs/catalog-bmf.html`.
- Raw dictionaries: NCCS Core public charity, private foundation, and harmonized BMF bridge dictionaries.

## Collection Method

NCCS Core is a processed public filing dataset derived from IRS Form 990, 990-EZ, and 990-PF records. The BMF bridge files are used for geographic linkage. This is not a voluntary survey.

## Inclusion Criteria And Limitations

The current pipeline scope is tax year `2022`. It includes selected Core public charity and private foundation files and keeps records that can be connected to the project counties and regions through the BMF bridge files.

Important limitations:

- Core is currently `2022` only in this project.
- Some Core public charity files can overlap by source variant. The pipeline preserves those overlapping `PC` and `PZ` rows because they represent source-specific Core records.
- Filing-form context is not always populated in the same way across all Core files.
- Geography comes through bridge files and should be treated as a current-address approximation rather than a perfect filing-address history.
- Revenue-source fields are kept faithful to Core source fields and are not forced to match GivingTuesday contribution categories.

## Download And S3 Storage

Raw Core files, dictionaries, bridge BMF files, and release metadata are downloaded locally, uploaded unchanged to Bronze S3 under `bronze/nccs_990/core/`, and checked against expected file sizes. Filtered Core outputs are stored under `silver/nccs_990/core/`, and analysis documentation is stored under `silver/nccs_990/core/analysis/`.

## Datatype Transformation

Raw Core CSV files and dictionaries are preserved unchanged in Bronze S3. After filtering to the project geography, the pipeline writes per-file filtered CSV outputs, a combined filtered Core Parquet file, and final analysis Parquet outputs.

## Data Cleaning

The cleaning process keeps the raw Core files unchanged, then creates project-specific analysis files:

- Selects the Core files and BMF bridge files needed for tax year `2022`.
- Uses the BMF bridge files to connect filings to counties and regions.
- Keeps only records connected to the project counties and regions.
- Writes filtered outputs by source file, then creates one combined filtered Core Parquet file.
- Preserves source context fields so users can see which Core file and source variant each row came from.
- Preserves intentional `PC` and `PZ` overlap rows rather than collapsing them into one organization-year.
- Standardizes identity, tax year, state, ZIP, county, and region fields where needed.
- Calculates analysis fields such as revenue, expenses, assets, net assets, surplus, net margin, months of reserves, program service revenue, and contribution candidates where Core source fields support them.
- Supplements missing NTEE and subsection classifications from BMF and IRS EO BMF when needed.
- Creates helper flags for hospitals, universities, and political organizations so analysts can apply consistent exclusions.
- Marks missing NTEE values and zero-expense reserve cases explicitly so they are visible to analysts.

## Analysis-Ready Outputs

- Row-level analysis dataset: `silver/nccs_990/core/analysis/nccs_990_core_analysis_variables.parquet`.
- Geography metrics: `silver/nccs_990/core/analysis/nccs_990_core_analysis_geography_metrics.parquet`.
- Pipeline documentation: `silver/nccs_990/core/analysis/documentation/nccs_990_core_pipeline.md`.
- Variable mapping: `silver/nccs_990/core/analysis/variable_mappings/nccs_990_core_analysis_variable_mapping.md`.
- Coverage evidence: `silver/nccs_990/core/analysis/quality/coverage/nccs_990_core_analysis_variable_coverage.csv`.

## Analysis Data Dictionary

Use `docs/final_preprocessing_docs/technical_docs/analysis_variable_mappings/nccs_990_core_analysis_variable_mapping.md` as the analysis-ready data dictionary. Use `docs/final_preprocessing_docs/technical_docs/quality/coverage_evidence/nccs_990_core_analysis_variable_coverage.csv` to confirm populated counts and coverage.

## Supporting Raw Dictionaries

- `docs/final_preprocessing_docs/technical_docs/source_dictionaries/nccs_990_core/CORE-HRMN_dd.csv`.
- `docs/final_preprocessing_docs/technical_docs/source_dictionaries/nccs_990_core/DD-PF-HRMN-V0.csv`.
- `docs/final_preprocessing_docs/technical_docs/source_dictionaries/nccs_990_core/harmonized_data_dictionary.xlsx`.
