# NCCS Postcard Deliverable Brief

## Purpose In The Analysis

NCCS postcard is the small-organization supplement in the current 990-family package.

Current role in the package:
- add coverage for organizations filing `990-N`
- preserve the small-filer presence that detailed filing sources and BMF-only analyses can miss
- support the `gross receipts under threshold` concept for `tax_year >= 2022`
- do **not** act as a detailed finance source

Primary current artifacts:
- `01_data/staging/nccs_990/postcard/snapshot_year=YYYY/nccs_990_postcard_benchmark_tax_year_start=2022_snapshot_year=YYYY.parquet`
- CSV audit copy in the same folder:
  - `nccs_990_postcard_benchmark_tax_year_start=2022_snapshot_year=YYYY.csv`

## Exact Analysis Questions Supported

- Section 2: optional supplement for "Is nonprofit density associated with donor participation rates across counties?"
- Section 3: optional supplement for organization counts, density, and county service-gap views
- General pooled-package support for including `990-N` small filers in the `organization-year` universe

## Variables To Supply

| Requested variable | Primary current pipeline column(s) | Availability | Notes |
|---|---|---|---|
| EIN | `ein` | available | Base small-filer identifier. |
| Tax year | `tax_year` | available | Current downstream derivative keeps only rows where `tax_year >= 2022`. |
| Organization name | `legal_name` | available | Current benchmark derivative naming field. |
| Gross receipts under threshold | `gross_receipts_under_25000` | available | Current source for `harm_gross_receipts_under_25000_flag`. |
| Organization state | `organization_state` | available | Used first in postcard address matching when valid. |
| Organization ZIP | `organization_zip` | available | Used first in postcard address matching when valid. |
| Officer state | `officer_state` | available | Used only when organization ZIP does not yield a benchmark match. |
| Officer ZIP | `officer_zip` | available | Used only when organization ZIP does not yield a benchmark match. |
| County FIPS | `county_fips` | available | Attached upstream in the filtered derivative. |
| Region | `region` | available | Attached upstream in the filtered derivative. |
| Match-basis audit field | `benchmark_match_source` | available | Tells whether geography came from the organization or officer address path. |
| Snapshot context | `snapshot_year`, `snapshot_month` | available | Important for understanding postcard coverage semantics. |

## What The Cleaned Deliverable Should Look Like

Recommended cleaned deliverables:
- one primary postcard benchmark derivative in **Parquet** restricted to `tax_year >= 2022`
- one CSV audit copy of the same derivative
- one manifest documenting row counts and the `tax_year >= 2022` filter

Recommended use in the pooled harmonized package:
- include postcard rows in the pooled `organization-year` structure
- preserve the small-filer flag as postcard-specific semantics
- use postcard mainly to widen organization coverage, not to supply financial totals

## What The Source Cannot Reliably Support

- Total revenue, expense, asset, or income comparisons
- NTEE or subsection classification
- Detailed hospital or university flags
- Detailed revenue-source composition

## Harmonization Notes

- The active postcard meaning of "`2022-present`" is:
  - use the latest accessible published snapshot year
  - keep only rows whose `tax_year >= 2022`
- Geography precedence is already handled upstream:
  - organization ZIP first
  - officer ZIP only if organization ZIP does not produce a benchmark match
- The current postcard derivative is best treated as a coverage supplement, not as the anchor for
  finance-oriented analyses.

## Open Questions

- Should postcard be used only as a small-filer supplement in pooled coverage, or should the analysis
  team also want explicit BMF-vs-BMF-plus-postcard sensitivity views for count and density metrics?
- For public-facing tables, should postcard-derived small-filer rows be labeled explicitly so the
  audience understands why they carry no detailed finance fields?
