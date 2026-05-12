# Revenue Sources Data Lineage Audit

## Purpose

This audit traces the raw GivingTuesday 990-family fields into the harmonized variables used by the Black Hills revenue-source analysis. It was run after inspecting the raw benchmark parquet and the regenerated analysis-variable parquet.

## Raw Input Files

| Role | File |
| --- | --- |
| Raw benchmark filing source | `data/321_Black_Hills_Area_Community_Foundation_2025_08/01_data/staging/filing/givingtuesday_990_basic_allforms_benchmark.parquet` |
| Harmonized analysis variables | `data/321_Black_Hills_Area_Community_Foundation_2025_08/01_data/staging/filing/givingtuesday_990_basic_allforms_analysis_variables.parquet` |
| Revenue-source analysis output | `data/321_Black_Hills_Area_Community_Foundation_2025_08/01_data/analysis/revenue_sources_black_hills/cleaned_revenue_sources_analysis.parquet` |

The raw benchmark file has 4,380 rows and 295 columns. The regenerated analysis-variable file has the same 4,380 rows and 67 harmonized columns. Keys and row order match exactly on `ein`, `tax_year`, `form_type`, and `region`, with no duplicate keys in either file.

## Raw-To-Harmonized Variable Rules

| Harmonized variable | Form 990 raw field | Form 990-EZ raw field | Form 990-PF raw field | Notes |
| --- | --- | --- | --- | --- |
| `analysis_total_revenue_amount` | `TOTREVCURYEA` | `TOTALRREVENU` | `ANREEXTOREEX` | Total revenue. |
| `analysis_total_expense_amount` | `TOTEXPCURYEA` | `TOTALEEXPENS` | `ARETEREXPNSS` | Total expense. |
| `analysis_program_service_revenue_amount` | `TOTPROSERREV` | `PROGSERVREVE` | missing | PF has no comparable field in this extract. |
| `analysis_total_contributions_amount` | `TOTACASHCONT` | `CONGIFGRAETC` | `STREACGRTOIN` | This was corrected for 990-EZ; `TOTACASHCONT` is blank for every 990-EZ row in this local benchmark source. |
| `analysis_cash_contributions_amount` | `TOTACASHCONT` | not used | not used | Diagnostic duplicate of Form 990 total contributions only. |
| `analysis_noncash_contributions_amount` | `NONCASCONTRI` | not used | not used | Diagnostic Form 990 Line 1g disclosure; not additive to total contributions. |
| `analysis_federated_campaigns_amount` | `FEDERACAMPAI` | not used | not used | Form 990 Line 1a contribution subcomponent. |
| `analysis_membership_dues_amount` | `MEMBERDUESUE` | `MEMBERDUESUE` | missing | For 990 this is a contribution subcomponent; for 990-EZ it is a separate revenue line and stays in residual revenue for the source-mix chart. |
| `analysis_fundraising_events_contributions_amount` | `FUNDRAEVENTS` | not used | not used | Form 990 Line 1c contribution subcomponent. |
| `analysis_related_org_contributions_amount` | `RELATEORGANI` | not used | not used | Form 990 Line 1d contribution subcomponent. |
| `analysis_government_grants_amount` | `GOVERNGRANTS` | not used | not used | Form 990 Line 1e contribution subcomponent. |
| `analysis_other_contributions_amount` | `ALLOOTHECONT` | not used | not used | Form 990 Line 1f mixed contribution bucket. |
| `analysis_calculated_grants_total_amount` | `FEDERACAMPAI + RELATEORGANI + GOVERNGRANTS` | missing | missing | Institutional-channel aggregate; does not use grant-payout fields. |

## What Changed

The audit found that the previous Step 13 mapping used `TOTACASHCONT` for 990-EZ total contributions. In the local raw benchmark parquet, `TOTACASHCONT` is populated for 2,191 Form 990 rows and 0 Form 990-EZ rows. The correct 990-EZ field is `CONGIFGRAETC`, which is populated for 1,191 Form 990-EZ rows.

The extractor now maps:

- 990 total contributions: `TOTACASHCONT`
- 990-EZ total contributions: `CONGIFGRAETC`
- 990-PF total contributions: `STREACGRTOIN`

After regeneration, `analysis_total_contributions_amount` is populated for 3,560 rows rather than 2,369 rows.

## Validation Checks

| Check | Result |
| --- | --- |
| Total revenue mappings by form | 0 mismatches. |
| Program service revenue mappings for 990 and 990-EZ | 0 mismatches. |
| Total contributions mappings by form | 0 mismatches after the 990-EZ fix. |
| PF program service revenue | 512 PF rows, 0 nonmissing program service values. |
| Form 990 contribution subcomponents reconcile to total contributions | 2,191 valid Form 990 rows, 0 rows with absolute difference above $0.01. |
| Institutional aggregate equals `FEDERACAMPAI + RELATEORGANI + GOVERNGRANTS` | 0 mismatches. |
| Old grant-payout fields excluded | `FOREGRANTOTA` and `GRANTOORORGA` have nonzero raw values but are not used in the institutional-contribution aggregate. |
| Primary cleaned analysis excludes hospitals/universities/political orgs | 4,179 cleaned rows, 0 flagged rows remaining. |

## Revenue-Source Construction

The downstream revenue-source analysis builds six mutually exclusive plotting segments:

| Segment | Construction |
| --- | --- |
| `program_service_revenue` | 990/EZ program service revenue; PF missing treated as 0 only for share composition. |
| `government_grants_received` | Form 990 `GOVERNGRANTS`; 0 for EZ/PF because those forms do not expose the same subcomponent. |
| `other_institutional_contributions` | Form 990 `FEDERACAMPAI + RELATEORGANI`; 0 for EZ/PF. |
| `individual_likely_contributions` | Form 990 `MEMBERDUESUE + FUNDRAEVENTS`; 0 for EZ/PF. |
| `mixed_other_contributions` | Form 990 `ALLOOTHECONT`; full total contributions for 990-EZ and 990-PF because those forms do not expose the same Line 1 subcomponents. |
| `residual_other_revenue` | Total revenue minus the five named components; negative residuals are flagged diagnostically and clipped only for composition plots. |

This construction makes sense for the available raw data. The main limitation remains substantive rather than computational: Form 990 Line 1f and the entire 990-EZ/PF contribution totals cannot distinguish individual gifts from foundation, donor-advised fund, corporate, or other institutional gifts.
