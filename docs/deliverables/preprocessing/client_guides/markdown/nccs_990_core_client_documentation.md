# NCCS 990 Core Client Documentation

## Background

The NCCS 990 Core dataset is a processed public filing dataset from the National Center for Charitable Statistics at the Urban Institute. It harmonizes information from IRS Form 990, Form 990-EZ, and Form 990-PF filings into standardized Core files.

In this project, NCCS 990 Core is used as a source-specific Form 990 analysis layer. It provides organization identity, filing year, filing context, revenue, expenses, assets, net assets, program service revenue, contribution candidates, and classification support. The pipeline also uses NCCS BMF bridge files to connect Core filing records to the project counties and regions.

## Data Provenance

- Producer: National Center for Charitable Statistics at the Urban Institute.
- Core catalog page: `https://nccs.urban.org/nccs/catalogs/catalog-core.html`.
- BMF bridge catalog page: `https://nccs.urban.org/nccs/catalogs/catalog-bmf.html`.
- Source documentation: NCCS Core public charity, private foundation, and harmonized BMF bridge catalog documentation.
- Source file families used: NCCS Core public charity files, NCCS Core private foundation files, and NCCS BMF bridge files for tax year `2022`.

## Collection Method

NCCS Core is a processed public filing dataset derived from IRS Form 990, 990-EZ, and 990-PF records. The BMF bridge files are used for geographic linkage. This is not a voluntary survey.

## Inclusion Criteria And Limitations

The current pipeline scope is tax year `2022`. It includes selected Core public charity and private foundation files and keeps records that can be connected to the project counties and regions through the BMF bridge files.

Important limitations:

- Core is currently `2022` only in this project because it is the only post-COVID year available in the NCCS Core files used here.
- Some Core public charity files can overlap by source variant. In plain terms, `PC` records are public charity records from the full Form 990 source files, while `PZ` records are harmonized public charity records that can include fields shared across Form 990 and Form 990-EZ source files. The pipeline preserves overlapping `PC` and `PZ` rows because they come from different NCCS source versions, so they should not be automatically collapsed as duplicates.
- Filing-form context is not always populated in the same way across all Core files.
- Geography comes through bridge files and should be treated as a current-address approximation rather than a perfect filing-address history.
- Revenue-source fields are kept faithful to Core source fields and are not forced to match GivingTuesday contribution categories.

## Source Files Used

Raw Core files, dictionaries, bridge BMF files, and release metadata are downloaded from the public NCCS Core and BMF catalog sources. The project uses the Core files and BMF bridge files needed to identify records connected to the project counties and benchmark regions.

## Datatype Transformation

Raw Core CSV files and dictionaries are preserved as source inputs. After filtering to the project geography, the processing creates per-file filtered outputs, a combined filtered Core file, and final analysis-ready CSV deliverables.

## Data Cleaning

The cleaning process keeps the raw Core files unchanged, then creates project-specific analysis files:

- Selects the Core files and BMF bridge files needed for tax year `2022`.
- Uses the BMF bridge files to connect filings to counties and regions.
- Keeps only records connected to the project counties and regions.
- Writes filtered outputs by source file, then creates one combined filtered Core file.
- Preserves source context fields so users can see which Core file and source variant each row came from.
- Preserves intentional `PC` and `PZ` overlap rows rather than collapsing them into one organization-year; these are different NCCS source versions of public charity filing records.
- Standardizes identity, tax year, state, ZIP, county, and region fields where needed.
- Calculates analysis fields such as revenue, expenses, assets, net assets, surplus, net margin, months of reserves, program service revenue, and contribution candidates where Core source fields support them.
- Supplements missing NTEE and subsection classifications from BMF and IRS EO BMF when needed.
- Creates helper flags for hospitals, universities, and political organizations so analysts can apply consistent exclusions.
- Marks missing NTEE values and zero-expense reserve cases explicitly so they are visible to analysts.

## Analysis-Ready CSV Deliverables

- `nccs_990_core_analysis_variables.csv`
- `nccs_990_core_analysis_geography_metrics.csv`

## Analysis Data Dictionary

The deliverable package includes a separate data dictionary for the analysis-ready CSV files.
