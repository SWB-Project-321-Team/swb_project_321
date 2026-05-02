# NCCS 990 Core Client Documentation

## Background

The NCCS 990 Core dataset is a processed public filing layer used here as a co-equal source-specific Form 990 analysis package. It provides harmonized filing fields and uses BMF bridge inputs for benchmark geography.

The detailed technical pipeline documentation is `docs/final_preprocessing_docs/technical_docs/pipeline_docs/nccs_990_core_pipeline.md`.

## Data Provenance

- Producer: National Center for Charitable Statistics at the Urban Institute.
- Core catalog page: `https://nccs.urban.org/nccs/catalogs/catalog-core.html`.
- BMF bridge catalog page: `https://nccs.urban.org/nccs/catalogs/catalog-bmf.html`.
- Raw dictionaries: NCCS Core public charity, private foundation, and harmonized BMF bridge dictionaries.

## Collection Method

NCCS Core is a processed public filing dataset derived from IRS Form 990, 990-EZ, and 990-PF filing records. The BMF bridge files are used for geographic linkage. This is not a voluntary survey.

## Inclusion Criteria And Limitations

The current pipeline scope is tax year `2022`. It includes selected Core public-charity and private-foundation files and filters records to benchmark geography through the BMF bridge. Overlapping `PC` and `PZ` rows are intentionally preserved, `harm_filing_form` is sparse by design, and bridge geography should be treated as a current-address approximation.

## Download And S3 Storage

Raw Core files, dictionaries, bridge BMF files, and release metadata are downloaded locally, uploaded unchanged to Bronze S3 under `bronze/nccs_990/core/`, and verified with source/local/S3 byte checks. Filtered Core outputs are stored under `silver/nccs_990/core/`, and analysis documentation is stored under `silver/nccs_990/core/analysis/`.

## Datatype Transformation

Raw Core CSV files and dictionaries remain unchanged in Bronze S3. After benchmark filtering, per-file filtered CSV outputs and a combined filtered Core Parquet are written. Final analysis outputs are written as Parquet.

## Data Cleaning

Cleaning includes bridge-based county assignment, benchmark geography admission, combined filtered output construction, identity and geography fallback handling, filing-context preservation, classification enrichment, calculated metrics, net-margin cleaning support, and imputed/proxy exclusion-support flags.

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
