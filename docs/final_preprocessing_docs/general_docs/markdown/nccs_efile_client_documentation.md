# NCCS Efile Client Documentation

## Background

The NCCS efile dataset contains parsed public electronic IRS Form 990 filing tables. It is based on required electronic filings submitted by nonprofit organizations and then published in a structured form by NCCS.

In this project, NCCS efile is used to create annual filing-level datasets for the project counties and regions. It provides organization identity, filing year, filing form, geography, revenue, expenses, assets, and selected filing context fields. It is more financially detailed than registry sources, but it does not expose every contribution or grant component needed for all revenue-source questions.

## Data Provenance

- Producer: National Center for Charitable Statistics public efile release.
- Dataset page: `https://nccs.urban.org/nccs/datasets/efile/`.
- Catalog page: `https://nccs.urban.org/nccs/catalogs/catalog-efile-v2_1.html`.
- Source documentation: `https://nonprofit-open-data-collective.github.io/irs990efile/data-dictionary/data-dictionary.html`.
- Source table families used: NCCS efile HEADER, SUMMARY, and Schedule A tables for tax years `2022-2024`.

## Collection Method

The source tables are derived from public electronic IRS Form 990 filings. They represent required tax filings for organizations that file electronically, not voluntary survey responses.

## Inclusion Criteria And Limitations

The pipeline includes the required HEADER, SUMMARY, and Schedule A table families for tax years `2022-2024`. It first determines which filings are located in the project counties and regions using HEADER rows, then joins in the additional tables needed for analysis.

Important limitations:

- The final row-level output keeps one selected filing per EIN, tax year, and filing form.
- Some source fields are sparse because not every filing has every table or field.
- The current efile analysis layer does not include GivingTuesday-style contribution and grant component fields, so those fields remain unavailable here rather than being weakly filled from another source.
- Some classification fields are supplemented from NCCS BMF and IRS EO BMF when needed.

## Source Files Used

Annual raw NCCS efile CSV tables are downloaded from the public NCCS efile source pages. The project uses HEADER rows to identify project-region filings, then joins the required SUMMARY and Schedule A tables for those retained filings.

## Datatype Transformation

Raw annual CSV tables are preserved as source inputs. After filtering to the project geography and selecting the retained filings, the processing creates annual project-region files and final analysis-ready CSV deliverables.

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

## Analysis-Ready CSV Deliverables

- `nccs_efile_analysis_variables.csv`
- `nccs_efile_analysis_geography_metrics.csv`

## Analysis Data Dictionary

The deliverable package includes a separate data dictionary for the analysis-ready CSV files.
