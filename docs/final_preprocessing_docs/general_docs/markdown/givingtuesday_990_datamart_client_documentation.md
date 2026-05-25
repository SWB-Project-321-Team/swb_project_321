# GivingTuesday 990 DataMart Client Documentation

## Background

The GivingTuesday 990 DataMart is a public dataset built from IRS Form 990-family filings for U.S.-based nonprofits. GivingTuesday describes the DataMart as an openly accessible source of nonprofit Form 990 filing data. In this project, it is the main source used for detailed filing-level financial information across Forms 990, 990-EZ, and 990-PF.

The files used here include a Combined Forms DataMart and separate Basic Fields files for Forms 990, 990-EZ, and 990-PF. Together, these files provide organization identity, filing year, filing form, revenue, expenses, assets, contribution-related fields, and selected fields needed for analysis. The project converts those large raw files into smaller analysis-ready datasets for the project counties and regions.

This dataset is useful because it contains more detailed financial information than registry-style sources such as BMF. It is also important to interpret carefully: different organizations file different versions of the Form 990 family, and the IRS forms do not always separate donor types in the way analysts may want.

The detailed technical pipeline documentation is `docs/final_preprocessing_docs/technical_docs/pipeline_docs/givingtuesday_datamart_pipeline.md`.

## Data Provenance

- Producer: GivingTuesday.
- Source page: `https://990data.givingtuesday.org/datamarts/`.
- Public catalog API: `https://grantstory-api.gtdata.org/v2/public/678d25291eceb5d121099200/items?limit=200`.
- Per-file download URLs are selected from the GivingTuesday catalog and recorded in the pipeline manifests.
- Raw dictionary support comes from the exported GivingTuesday catalog and field dictionary files.

## Collection Method

The DataMart files are derived from public IRS Form 990-family filings. These are tax filings submitted by nonprofit organizations that are required to file a Form 990, 990-EZ, or 990-PF. The project does not collect survey responses and does not create the GivingTuesday analysis layer directly from IRS XML files.

## Inclusion Criteria And Limitations

The project uses selected GivingTuesday DataMart files for tax years `2022-2024`. Records are kept when they can be connected to the project counties and regions. The pipeline focuses on the Basic Fields files for Forms 990, 990-EZ, and 990-PF for the final row-level analysis dataset.

Important limitations:

- Not every nonprofit files a full Form 990. Some smaller organizations file Form 990-N/e-Postcard, some file Form 990-EZ, private foundations file Form 990-PF, and some exempt entities may not appear in these public filing files.
- The final GivingTuesday analysis dataset is based on the selected Basic Fields files. It is not a complete copy of every GivingTuesday DataMart field.
- Some contribution categories are limited by how the IRS forms are designed. For example, Form 990 includes one broad "other contributions" line that can mix individual gifts, foundation grants, donor-advised fund distributions, corporate gifts, and bequests. That line cannot be split into clean donor types from this DataMart alone.
- Forms 990-EZ and 990-PF do not report all of the same contribution subcategories that full Form 990 filers report.
- The selected GivingTuesday fields do not always include all classification fields needed for this project, so the analysis layer supplements missing classification information from NCCS BMF and IRS EO BMF registry sources.

## Download And S3 Storage

Required raw GivingTuesday CSV files and metadata are downloaded locally, uploaded unchanged to Bronze S3 under `bronze/givingtuesday_990/datamarts/raw/` and `bronze/givingtuesday_990/datamarts/metadata/`, and checked against expected file sizes. This gives the project a preserved copy of the downloaded source files.

Intermediate project-ready outputs are stored under the GivingTuesday Bronze and Silver prefixes. Final analysis documentation and analysis outputs are stored under `silver/givingtuesday_990/analysis/`.

## Datatype Transformation

The raw GivingTuesday CSV files are preserved unchanged in Bronze S3. Later pipeline steps convert the data into Parquet files, which are easier and faster to read in Python. These converted files include a normalized Combined Forms cache, pre-Silver files, Silver project-region files, and final analysis outputs.

## Data Cleaning

The cleaning process keeps the raw source files unchanged, then creates smaller project-specific files for analysis:

- Selects the required GivingTuesday DataMart files from the public catalog and records which files were used.
- Reads the raw file headers and exported catalog information so the project can document source fields.
- Standardizes key fields used across files, including EINs, filing years, filing forms, state values, ZIP codes, and project geography fields.
- Keeps records for tax years `2022-2024`.
- Keeps records that can be matched to the project counties and regions before combining large files.
- Builds project-region geography from ZIP and county reference files.
- When multiple filing records could represent the same organization, year, and form, keeps one record using documented tie-break rules.
- Combines Basic Fields and Combined Forms information only after the project-region and year filters have been applied.
- Calculates analysis fields such as total revenue, total expenses, surplus, net margin, months of reserves, and selected contribution-related measures when the source fields support them.
- Adds classification information, such as NTEE and subsection codes, from NCCS BMF and IRS EO BMF when GivingTuesday does not provide those fields directly.
- Creates helper flags for hospitals, universities, and political organizations so analysts can include or exclude those groups consistently.
- Leaves fields blank when the source does not support a reliable value, rather than filling them from a weaker source.

## Analysis-Ready Outputs

- Row-level analysis dataset: `silver/givingtuesday_990/analysis/givingtuesday_990_basic_allforms_analysis_variables.parquet`.
- Region metrics: `silver/givingtuesday_990/analysis/givingtuesday_990_basic_allforms_analysis_region_metrics.parquet`.
- Pipeline documentation: `silver/givingtuesday_990/analysis/documentation/givingtuesday_datamart_pipeline.md`.
- Variable mapping: `silver/givingtuesday_990/analysis/variable_mappings/givingtuesday_basic_analysis_variable_mapping.md`.
- Coverage evidence: `silver/givingtuesday_990/analysis/quality/coverage/givingtuesday_990_basic_allforms_analysis_variable_coverage.csv`.

## Analysis Data Dictionary

Use `docs/final_preprocessing_docs/technical_docs/analysis_variable_mappings/givingtuesday_basic_analysis_variable_mapping.md` as the analysis-ready data dictionary. Use `docs/final_preprocessing_docs/technical_docs/quality/coverage_evidence/givingtuesday_990_basic_allforms_analysis_variable_coverage.csv` to confirm populated counts and coverage.

For contribution and revenue-source analysis, the most important point is that the DataMart follows the structure of the IRS forms. Full Form 990 filers provide the most detailed contribution subcategories. Form 990-EZ and Form 990-PF filers provide less detail. Some categories, especially "other contributions" on Form 990, cannot be cleanly split into individual giving versus institutional giving without additional data that is not in this DataMart.

## Supporting Raw Dictionaries

- `docs/final_preprocessing_docs/technical_docs/source_dictionaries/givingtuesday_datamarts/datamart_catalog.md`.
- `docs/final_preprocessing_docs/technical_docs/source_dictionaries/givingtuesday_datamarts/datamart_fields.md`.
- `docs/final_preprocessing_docs/technical_docs/source_dictionaries/givingtuesday_datamarts/datamart_catalog.csv`.
- `docs/final_preprocessing_docs/technical_docs/source_dictionaries/givingtuesday_datamarts/datamart_fields.csv`.
