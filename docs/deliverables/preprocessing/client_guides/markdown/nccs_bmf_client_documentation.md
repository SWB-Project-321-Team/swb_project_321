# National Center for Charitable Statistics (NCCS) Business Master File (BMF) Client Documentation

## Background

The National Center for Charitable Statistics (NCCS) Business Master File (BMF) dataset is a registry-style nonprofit dataset published by the Urban Institute. It is based on Internal Revenue Service (IRS) exempt-organization records and provides organization identity, address/geography fields, exempt status context, National Taxonomy of Exempt Entities (NTEE) classification, subsection information, and limited amount fields.

In this project, NCCS BMF is used for annual nonprofit counts, organization geography, classification support, and project-region analysis. It also helps enrich classification fields for other datasets when those datasets do not contain a complete NTEE or subsection code. Like IRS Exempt Organizations (EO) BMF, this is a registry and classification source, not a full Form 990 financial-detail source.

## Data Provenance

- Producer: National Center for Charitable Statistics at the Urban Institute.
- Source pages: `https://nccs.urban.org/nccs/datasets/bmf/` and `https://nccs.urban.org/nccs/catalogs/catalog-bmf.html`.
- Source documentation: NCCS BMF catalog documentation and IRS EO BMF field documentation.
- Source file families used: monthly NCCS BMF extracts and legacy NCCS BMF extracts covering `2022-2024` records.

## Collection Method

NCCS BMF is a public registry dataset derived from IRS exempt-organization records. It is not a voluntary survey and it does not contain complete Form 990 filing detail.

## Inclusion Criteria And Limitations

The pipeline selects representative yearly BMF files, keeps records located in the project counties and regions, and builds the analysis-ready layer for `2022-2024`.

Important limitations:

- BMF is a registry and classification source, not a complete filing-level financial source.
- Expense, net assets, surplus, net margin, months of reserves, contribution detail, and grant detail are not available in the current BMF analysis layer.
- The 2022 legacy file links to an unavailable profile dictionary, so this package includes a generated header companion and documents that limitation.
- Duplicate handling for analysis is applied in the final analysis layer; the upstream yearly project-region files remain source-faithful filtered outputs.

## Source Files Used

The project uses selected yearly NCCS BMF source files covering `2022-2024` records. Those files are downloaded from the public NCCS BMF source pages, then filtered to the project counties and benchmark regions.

## Datatype Transformation

Raw annual comma-separated values (CSV) files are preserved as source inputs. After project-region filtering, the processing creates yearly project-region outputs, exact-year lookup files, row-level analysis outputs, geography metrics, and field metrics.

## Data Cleaning

The cleaning process keeps the raw NCCS BMF files unchanged, then creates project-specific analysis files:

- Selects the yearly source files used for the `2022-2024` analysis window.
- Standardizes employer identification numbers (EINs), Zone Improvement Plan (ZIP) codes, county Federal Information Processing Standards (FIPS) codes, region labels, and year fields.
- Uses project geography reference files to keep only records located in the project counties and regions.
- Writes one filtered yearly file for each selected year.
- Creates exact-year lookup files so classification enrichment can use the right BMF year when possible.
- Combines only the retained project-region files for the final analysis layer.
- When duplicate organization-year records remain in the final analysis layer, keeps one record using documented tie-break rules.
- Builds analyst-facing NTEE and subsection fields from NCCS BMF first, then uses fallback registry information where needed.
- Creates helper flags for hospitals, universities, and political organizations based on classification and conservative name matching.
- Leaves unsupported Form 990 financial fields blank because BMF does not contain the needed source information.

## Analysis-Ready CSV Deliverables

- `nccs_bmf_analysis_variables.csv`
- `nccs_bmf_analysis_geography_metrics.csv`
- `nccs_bmf_analysis_field_metrics.csv`

## Analysis Data Dictionary

The deliverable package includes a separate data dictionary for the analysis-ready CSV files.
