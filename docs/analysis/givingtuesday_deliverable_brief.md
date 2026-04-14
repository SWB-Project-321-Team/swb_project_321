# GivingTuesday Deliverable Brief

## Purpose In The Analysis

GivingTuesday is the current detailed-filing source that best supports the limited-number-organization
questions in Section 3, especially where form-level financial detail is needed.

Current role in the package:
- detailed filing support for `990`, `990EZ`, and `990PF`
- strongest current source for revenue-source subcomponents in the benchmark subset
- not the census source for all nonprofit organizations in the study regions

Primary current artifact:
- `01_data/staging/filing/givingtuesday_990_filings_benchmark.parquet`

## Exact Analysis Questions Supported

- Section 3: "Is there a difference in the revenue sources between Black Hills and benchmark regions?"
- Section 3: "Is there a difference in financial performance of nonprofit organizations between Black Hills and benchmark regions (2022-2024)?"
- Section 3 exclusion support where hospital or university flags are needed for the detailed filing subset

## Variables To Supply

| Requested variable | Primary current pipeline column(s) | Availability | Notes |
|---|---|---|---|
| EIN | `ein` | available | Base organization identifier. |
| Tax year | `tax_year` | available | Current GT benchmark coverage spans `2022-2024`. |
| Filing form | `form_type` | available | Current values include `990`, `990EZ`, and `990PF`. |
| Organization name | `FILERNAME1` | available | Current benchmark artifact carries GT-native organization naming. |
| State | `FILERUSSTATE` | available | Source-stage geography field. |
| ZIP5 | `zip5` | available | Already standardized upstream in the GT benchmark artifact. |
| County FIPS | `county_fips` | available | Benchmark county assignment already attached. |
| Region | `region` | available | Benchmark region label already attached. |
| Total revenue | `TOTREVCURYEA` for `990`; `TOTALRREVENU` for `990EZ`; `ANREEXTOREEX` for `990PF` | available | This is the current GT form-aware revenue rule. |
| Total expense | `TOTEXPCURYEA` for `990`; `TOTALEEXPENS` for `990EZ`; `ARETEREXPNSS` for `990PF` | available | This is the current GT form-aware expense rule. |
| Surplus or deficit | `analysis_total_revenue_amount - analysis_total_expense_amount` | available | Calculated in the final analysis extract from the final form-aware revenue and expense values. |
| Asset-like balance field | `TOASEOOYY` for `990`/supported `990EZ`; `ASSEOYOYY` exists for `990PF` but is not enough for net assets | available_with_caveat | GT can derive the net-asset proxy only where both assets and liabilities are present. `990PF` remains null for net asset because the current GT PF standard-fields extract does not carry a matching liability field. |
| Program service revenue | `TOTPROSERREV`, `PROGSERVREVE` | available_with_caveat | Needs one final harmonization rule across forms. |
| Cash contributions | `TOTACASHCONT` | available_with_caveat | Contribution subcomponent, not yet the final combined "total contributions" rule. |
| Noncash contributions | `NONCASCONTRI` | available_with_caveat | Contribution subcomponent, not yet the final combined "total contributions" rule. |
| Other contributions | `ALLOOTHECONT` | available_with_caveat | Contribution subcomponent, not yet the final combined "total contributions" rule. |
| Foundation grants | `FOREGRANTOTA` | available_with_caveat | Candidate field for the draft's "foundation grants" requirement. |
| Government grants | `GOVERNGRANTS` | available | Present in the current GT benchmark output. |
| Other grant-related component | `GRANTOORORGA` | available_with_caveat | Present, but final use depends on the analytic definition of "grants total amount." |
| Hospital flag | `OPERATHOSPIT` | available | Current GT source for `harm_is_hospital`. |
| University or school flag | `OPERATSCHOOL` | available | Current GT source for `harm_is_university`. |

## What The Cleaned Deliverable Should Look Like

Recommended cleaned deliverable:
- one benchmark-filtered GT parquet at the current filing or organization-year grain
- keep GT-native columns intact in the source-stage artifact
- rely on the existing upstream-derived columns already present in the benchmark output:
  - `derived_income_amount`
  - `derived_income_derivation`
  - benchmark geography columns

Recommended use in the pooled harmonized package:
- contribute detailed filing values into the pooled `organization-year` table
- remain the main current source for the limited-number revenue-source components
- preserve `form_type` so the downstream package can keep the `990PF` subset visible

## What The Source Cannot Reliably Support

- Full nonprofit census coverage across all organizations in the benchmark counties
- Small-filer coverage comparable to postcard
- A fully locked single "total contributions" field without a final analytic definition
- A guaranteed one-to-one replacement for BMF counts, density, or classification work

## Harmonization Notes

- Current GT benchmark processing already applies form-aware revenue and expense mapping:
  - `990` uses `TOTREVCURYEA` and `TOTEXPCURYEA`
  - `990EZ` uses `TOTALRREVENU` and `TOTALEEXPENS`
  - `990PF` uses PF-specific GT standard-fields `ANREEXTOREEX` and `ARETEREXPNSS`
- `derived_income_amount` is already moved upstream and should remain upstream. The final
  analysis extract also recalculates surplus from the final form-aware analysis revenue and
  expense fields so `990PF` uses the corrected PF-specific revenue and expense fields.
- GT is currently the only active detailed-filing source in the package that also includes `990PF`.
- The limited-number revenue-source analysis should treat GT as the primary current source for
  program-service, contribution, and grant subcomponents.

## Open Questions

- Should the limited-number analyses keep `990PF` inside the same pooled filing subset, or should
  `990PF` be treated as a separate comparison slice?
- What exact combination of `TOTACASHCONT`, `NONCASCONTRI`, and `ALLOOTHECONT` should count as
  "total contributions" for the analysis draft?
- GT `990PF` has an asset-like end-of-year field (`ASSEOYOYY`) but no matching liability field in
  the current standard-fields extract, so the GT net-asset proxy remains unavailable for `990PF`.
