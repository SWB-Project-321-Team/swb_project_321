# NCCS 990 Postcard Client Documentation

## Background

The NCCS 990 Postcard dataset contains Form 990-N/e-Postcard records. Form 990-N is a short annual notice used by eligible small tax-exempt organizations that are not required to file a full Form 990 or Form 990-EZ.

In this project, the postcard data is used as a small-filer support layer. It helps identify smaller organizations, their location, filing/tax year, termination status, and whether they reported gross receipts under the postcard threshold. It is useful for supplemental counts, geography, and classification support. It is not useful for detailed financial analysis because the postcard filing does not contain full revenue, expense, asset, contribution, or grant fields.

## Data Provenance

- Producer: NCCS publishes the monthly postcard files; the source fields correspond to IRS Form 990-N/e-Postcard data.
- Dataset page: `https://nccs.urban.org/nccs/datasets/postcard/`.
- Source documentation: `https://www.irs.gov/pub/irs-tege/990n-data-dictionary.pdf`.
- Source file family used: monthly NCCS e-Postcard CSV snapshots.

## Collection Method

Form 990-N/e-Postcard is a required IRS annual notice for eligible small exempt organizations. It is not a voluntary survey and does not contain full return-level financial fields.

## Inclusion Criteria And Limitations

The pipeline uses the latest available monthly postcard snapshot files, keeps records located in the project counties and regions, keeps the latest retained row per EIN, and limits the final analysis layer to filing tax years `2022-2024`.

Important limitations:

- The postcard output is a latest-snapshot support file, not a true annual filing panel.
- It is intended for supplemental small-filer counts, geography, status, and classification support.
- Revenue, expenses, assets, net assets, reserves, contributions, and grants are unavailable because Form 990-N/e-Postcard does not collect those detailed financial fields.
- Some classification fields are supplemented from NCCS BMF and IRS EO BMF because postcard records do not always contain the classification detail needed for analysis.

## Source Files Used

Monthly raw postcard CSVs and metadata are downloaded from the public NCCS postcard source page. The project filters the monthly snapshots to project counties and benchmark regions, then keeps the latest retained row per EIN for the support dataset.

## Datatype Transformation

Raw monthly CSV files are preserved as source inputs. After filtering to the project geography and creating the retained latest-snapshot file, the processing creates filtered derivatives and final analysis-ready CSV deliverables.

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

## Analysis-Ready CSV Deliverables

- `nccs_990_postcard_analysis_variables.csv`
- `nccs_990_postcard_analysis_geography_metrics.csv`

## Analysis Data Dictionary

The deliverable package includes a separate data dictionary for the analysis-ready CSV files.
