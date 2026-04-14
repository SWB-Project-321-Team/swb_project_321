# NCCS BMF Deliverable Brief

## Purpose In The Analysis

NCCS BMF is the primary census, classification, geography, and reference-total source for the current
990-family package.

Current role in the package:
- base source for organization counts and density
- base source for county and region assignment at the full-coverage layer
- base source for NTEE and subsection-driven classification work
- base source for regionwide reference totals such as revenue, assets, and income
- upstream exact-year lookup source for identity and classification enrichment

Primary current artifacts:
- `01_data/staging/nccs_bmf/year=YYYY/nccs_bmf_benchmark_year=YYYY.parquet`
- `01_data/staging/nccs_bmf/year=YYYY/nccs_bmf_exact_year_lookup_year=YYYY.parquet`

## Exact Analysis Questions Supported

- Section 2: "Is nonprofit density associated with donor participation rates across counties?"
- Section 3: "Is there a significant difference in the number and density of nonprofit organizations between Black Hills and benchmark regions?"
- Section 3: "Is there a rural and/or indigenous service gaps within the Black Hills community (compare counties)?"
- Section 3: "Is there a significant difference in total revenue between Black Hills and benchmark regions?"
- Section 3: "Is there a significant difference in total assets between Black Hills and benchmark regions?"
- Section 3: "Is there a significant difference in nonprofits' dependency on grants vs. other revenue sources between Black Hills and benchmark regions?"
- Section 3: "Are there any nonprofit fields under/over-represented in each region?"
- Section 3: "What are the best predictors of total revenue for Black Hills and each benchmark region?"
- Section 3: "How has the number of nonprofit organizations and performance changed over time (2022-2024)?"

## Variables To Supply

| Requested variable | Primary current pipeline column(s) | Availability | Notes |
|---|---|---|---|
| EIN | `EIN` in benchmark rows; `harm_ein` in exact-year lookup | available | Base organization identifier for counts and overlays. |
| Tax year | `bmf_snapshot_year` in benchmark rows; `harm_tax_year` in exact-year lookup | available | The benchmark rows are yearly snapshots from `2022-present`. |
| Organization name | `NAME` in benchmark rows; `harm_org_name` in exact-year lookup | available | Exact-year lookup is the stable overlay-friendly form. |
| State | `STATE` in benchmark rows; `harm_state` in exact-year lookup | available | Legacy 2022 and later yearly schemas differ in raw naming details. |
| ZIP | `ZIP5` in 2022; `ZIP` in 2023+; `harm_zip5` in exact-year lookup | available | Exact-year lookup is the stable harmonized representation. |
| County FIPS | `county_fips` | available | Attached upstream in the benchmark artifact. |
| Region | `region` | available | Attached upstream in the benchmark artifact. |
| Subsection or organization type | `SUBSECCD` in 2022; `SUBSECTION` in 2023+; `harm_subsection_code` in exact-year lookup | available | Supports political-organization and organization-type filtering. |
| NTEE classification | `NTEEFINAL` or `NTEE1` in 2022; `NTEE_CD` in 2023+; `harm_ntee_code` in exact-year lookup | available | Broad-letter derivation is straightforward for field-level analyses. |
| Total revenue | `CTOTREV` in 2022; `REVENUE_AMT` in 2023+ | available | Reference-total layer, not detailed filing composition. |
| Total assets | `ASSETS` in 2022; `ASSET_AMT` in 2023+ | available | Reference-total layer. |
| Income | `INCOME` in 2022; `INCOME_AMT` in 2023+ | available | Useful for trend or reference-performance proxies if the analysis team confirms the metric. |
| Exact-year overlay-ready identity and classification | `harm_org_name`, `harm_state`, `harm_zip5`, `harm_ntee_code`, `harm_subsection_code` | available | Carried in the exact-year lookup artifact with field-level source provenance. |

## What The Cleaned Deliverable Should Look Like

Recommended cleaned deliverables:
- one benchmark-filtered yearly parquet per BMF year
- one exact-year lookup parquet per BMF year
- one manifest describing the yearly benchmark outputs and the lookup outputs

Recommended use in the pooled harmonized package:
- benchmark rows define the main regionwide census-style layer
- exact-year lookup rows support exact-year identity and classification enrichment
- do not merge the two artifact types conceptually; they serve different purposes

## What The Source Cannot Reliably Support

- Detailed expense lines for organization-level financial-performance work
- Detailed revenue-source composition such as program service revenue or contribution subcomponents
- Direct hospital or university flags across the whole census without a classification proxy
- Postcard-scale small-filer detail on its own

## Harmonization Notes

- The 2022 BMF benchmark artifact uses a legacy schema with different raw field names from 2023+.
- The benchmark artifact is geography-filtered to the study regions.
- The exact-year lookup artifact is **not** a benchmark-row artifact; it exists to support exact-year
  overlay and harmonized provenance.
- BMF is the default source for:
  - organization counts
  - nonprofit density
  - NTEE broad-field grouping
  - regionwide reference totals
- If the analysis team wants small-filer sensitivity checks beyond EO-BMF, that is a separate decision
  that brings postcard into the count framework.

## Open Questions

- Should organization count and density analyses stay strictly BMF-based, or should there also be a
  `2022-present` sensitivity view that adds postcard small filers?
- What exact subsection or classification rule should be used to exclude political organizations in a
  way that is consistent across 2022 and 2023+ BMF schemas?
- For "with and without hospitals or universities" comparisons, should the package rely on BMF-based
  classification proxies, or should it use the pooled harmonized flags from the combined master layer?
