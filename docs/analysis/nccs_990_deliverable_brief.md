# NCCS 990 Deliverable Brief

## Purpose In The Analysis

For the current package, **NCCS 990** still means **NCCS efile as the primary multi-year source**,
but NCCS Core is now a co-equal source-specific analysis layer for `2022`.

Current role in the package:
- annual benchmark-filtered detailed filing source for published years from `2022` onward
- strong support for total revenue, expense, income, filing form, and hospital or school indicators
- comparison point against Core where needed, while Core also has its own 2022 analysis package

Primary current artifacts:
- `01_data/staging/nccs_efile/tax_year=YYYY/nccs_efile_benchmark_tax_year=YYYY.parquet`
- `01_data/staging/nccs_efile/comparison/tax_year=YYYY/efile_vs_core_*.csv`

## Exact Analysis Questions Supported

- Section 3: "Is there a difference in the revenue sources between Black Hills and benchmark regions?"
- Section 3: "Is there a difference in financial performance of nonprofit organizations between Black Hills and benchmark regions (2022-2024)?"
- Section 3 exclusion support where hospital or university flags are needed for the detailed filing subset

## Variables To Supply

| Requested variable | Primary current pipeline column(s) | Availability | Notes |
|---|---|---|---|
| EIN | `harm_ein` | available | Current annualized benchmark output is already keyed to `EIN + tax_year`. |
| Tax year | `harm_tax_year` | available | One benchmark parquet per published tax year. |
| Filing form | `harm_filing_form` | available | Current efile benchmark output includes `990` and `990EZ`. |
| Organization name | `harm_org_name` | available | Already normalized in the annualized benchmark artifact. |
| State | `harm_state` | available | Already normalized in the annualized benchmark artifact. |
| ZIP5 | `harm_zip5` | available | Already normalized in the annualized benchmark artifact. |
| County FIPS | `harm_county_fips` | available | Derived upstream from ZIP and benchmark references. |
| Region | `harm_region` | available | Benchmark region label already attached upstream. |
| Total revenue | `harm_revenue_amount` | available | Current source column in the benchmark build is `F9_01_REV_TOT_CY`. |
| Total expense | `harm_expenses_amount` | available | Current source column in the benchmark build is `F9_01_EXP_TOT_CY`. |
| Asset or net-asset-like balance | `harm_assets_amount` | available_with_caveat | Current source column is `F9_01_NAFB_ASSET_TOT_EOY`, which is closer to net-assets or fund-balance semantics than a generic total-assets label. |
| Income or surplus-deficit | `harm_income_amount` | available | Current source column is `F9_01_EXP_REV_LESS_EXP_CY`. |
| Hospital flag | `harm_is_hospital` | available | Current source column is `SA_01_PCSTAT_HOSPITAL_X`. |
| University or school flag | `harm_is_university` | available | Current source column is `SA_01_PCSTAT_SCHOOL_X`. |
| Partial-year indicator | `efile_is_partial_year` | available | Present for the latest published partial year. |

## What The Cleaned Deliverable Should Look Like

Recommended cleaned deliverable:
- one annual benchmark-filtered parquet per tax year
- one selected filing per `EIN + tax_year`
- retain the current annualized benchmark shape as the source-stage deliverable

Recommended use in the pooled harmonized package:
- provide detailed filing totals and filing-based hospital or school flags
- support the limited-number financial-performance analysis
- support total-revenue comparisons within the benchmark filing subset

## What The Source Cannot Reliably Support

- Full nonprofit census coverage across all organizations in the benchmark counties
- `990PF` coverage in the current active benchmark output
- Postcard-style small-filer coverage
- The full revenue-source breakdown named in the draft's limited-number revenue-source question,
  because the current annualized benchmark output does not expose those detailed subcomponents

## Harmonization Notes

- The annualized benchmark output already performs one-filing-per-`EIN + tax_year` selection.
- The current efile benchmark is the active NCCS 990 source for the pooled package.
- Core should still be described carefully:
  - use the `efile_vs_core_*` comparison artifacts to explain source choice where needed
  - describe Core as a co-equal source-specific analysis layer, not as the default multi-year filing layer
- The current efile path is strongest for financial-performance metrics and filing-level hospital or
  school flags, not for broad census coverage.

## Open Questions

- Should the limited-number revenue-source analysis use efile only for totals and performance metrics
  while GT carries the detailed revenue-source subcomponents?
- Is the current `harm_assets_amount` field acceptable as the "Net Asset" input in the analysis draft,
  or does the team want a stricter net-assets definition?
- Should the docs explicitly recommend a `990PF` note whenever NCCS 990 results are shown, since the
  active efile benchmark output does not carry `990PF` coverage?

## Core Comparison Note

NCCS Core now has a formal analysis package in the documentation:
- use it as a source-specific 2022 filing layer with preserved `PC` / `PZ` overlap rows
- do not define the primary pooled cleaned-data shape around Core
- do not ask the analysis team to treat Core as the default multi-year filing layer in the current package
