# GivingTuesday Deliverable Brief

> **Status reviewed 2026-07-23:** source roles and field mappings reflect current local code and retained `2022-2024` artifacts. Former S3 targets are historical and inactive.

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
| Total contributions (Section 3 Q9) | Step 13 `analysis_total_contributions_amount`: `TOTACASHCONT` (`990` Line 1h); `CONGIFGRAETC` (`990EZ` Part I Line 1); `STREACGRTOIN` (`990PF` Part I Line 1) | available | Canonical total for Q9; do **not** sum `NONCASCONTRI` or `ALLOOTHECONT` into this field (they overlap Line 1h on Form 990). |
| Cash contributions | `TOTACASHCONT` | available_with_caveat | Same underlying column as Line 1h on `990`; diagnostic component alongside `analysis_total_contributions_amount`. 990-EZ total contributions use `CONGIFGRAETC` instead. |
| Noncash contributions | `NONCASCONTRI` | available_with_caveat | Line 1g disclosure (subset of Line 1h); diagnostic only—not additive into total contributions. |
| Federated campaigns (Line 1a) | `FEDERACAMPAI` → `analysis_federated_campaigns_amount` | available_with_caveat | Form 990 only. Institutional channel. Subset of Line 1h. Rolls into `analysis_calculated_grants_total_amount`. |
| Membership dues (Line 1b) | `MEMBERDUESUE` → `analysis_membership_dues_amount` | available_with_caveat | Form 990 only. Likely-individual channel for Q9 decomposition. Subset of Line 1h. |
| Fundraising-event contributions (Line 1c) | `FUNDRAEVENTS` → `analysis_fundraising_events_contributions_amount` | available_with_caveat | Form 990 only. Likely-individual channel for Q9 decomposition. Subset of Line 1h. |
| Related-organization contributions (Line 1d) | `RELATEORGANI` → `analysis_related_org_contributions_amount` | available_with_caveat | Form 990 only. Institutional channel. Subset of Line 1h. Rolls into `analysis_calculated_grants_total_amount`. |
| Government grants (Line 1e) | `GOVERNGRANTS` → `analysis_government_grants_amount` | available_with_caveat | Form 990 only. The only Line 1 sub-component the IRS labels as unambiguously institutional. |
| Other contributions, mixed bucket (Line 1f) | `ALLOOTHECONT` → `analysis_other_contributions_amount` | available_with_caveat | Form 990 only. Mixes individual gifts with private foundation grants, DAF distributions, corporate gifts, and bequests. Cannot be split without Schedule B. |
| Institutional contributions aggregate (Q9 "Other contributions (foundation grants etc.)") | `analysis_calculated_grants_total_amount` = `FEDERACAMPAI` + `RELATEORGANI` + `GOVERNGRANTS` (Form 990 Lines 1a + 1d + 1e) | available_with_caveat | Form 990 only. Closest defensible match for the plan's "foundation grants etc." concept. Earlier versions accidentally summed `FOREGRANTOTA` and `GRANTOORORGA` (Part IX grants paid out by the filer); that mix has been removed. |
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
- Inter-source reuse of GT contribution semantics without reading Step 13 column definitions (other pipelines may still name total contributions differently)
- A guaranteed one-to-one replacement for BMF counts, density, or classification work
- **A clean separation of contributions from individuals versus from other organizations.** Form 990 Part VIII Line 1f (`ALLOOTHECONT`) reports individual gifts in the same bucket as private foundation grants, DAF distributions, corporate gifts, and bequests; the IRS does not require donor-type breakdowns on Line 1. Form 990 Line 1a (federated campaigns), 1d (related-organization contributions), and 1e (government grants) can be identified as institutional, and Line 1b (membership dues) and 1c (fundraising events) are mostly individual, but Line 1f is irreducibly mixed. Form 990-EZ and Form 990-PF do not separately report any Line 1 sub-components, so the entire contributions total on those forms is undecomposable in this source. A cleaner individual-versus-institutional split would require Form 990 Schedule B, which is not currently in the GT basic datamart.

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
- GT `990PF` has an asset-like end-of-year field (`ASSEOYOYY`) but no matching liability field in
  the current standard-fields extract, so the GT net-asset proxy remains unavailable for `990PF`.

**Resolved for Section 3 Q9 (with caveats):** Total contributions for the limited-number
revenue-source analysis use Step 13 `analysis_total_contributions_amount` (form-aware 990 Line 1h / 990-EZ Part I Line 1 / PF
Part I Line 1). For Form 990 filers the contributions side is decomposed into six mutually exclusive
segments via the IRS Line 1 sub-channels: program service revenue, government grants received
(`GOVERNGRANTS`, Line 1e), other institutional contributions (`FEDERACAMPAI` + `RELATEORGANI`,
Lines 1a + 1d), likely-individual contributions (`MEMBERDUESUE` + `FUNDRAEVENTS`, Lines 1b + 1c),
the mixed Line 1f bucket (`ALLOOTHECONT`), and residual revenue. The plan-language "Other
contributions (foundation grants etc.)" segment maps to `analysis_calculated_grants_total_amount`
= `FEDERACAMPAI` + `RELATEORGANI` + `GOVERNGRANTS`. Earlier versions of that aggregate accidentally
summed `FOREGRANTOTA` and `GRANTOORORGA`, which are Form 990 Part IX (grants paid out) rather than
contributions received; that mix has been removed.

**Caveat for the client question of "individuals versus other organizations":** the IRS form
structure does not allow a clean isolation of individual giving. Line 1f mixes individuals with
foundations, DAFs, corporates, and bequests, and Form 990-EZ / 990-PF report only a single
contributions total. The current decomposition is the most informative split this source can
support; further isolation would require Schedule B or external donor data. See OQ-008 in
`990_open_questions.md` and `990_variable_analysis_crosswalk.md` (CW-027–CW-029) for the
authoritative version of this caveat.
