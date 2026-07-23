# 990 Harmonized Schema Proposal

> **Status reviewed 2026-07-23:** this proposal reflects the current local repository code and retained `2022-2024` planning artifacts. It does not imply that the former project S3 bucket remains available.

## Summary

The cleaned 990-family deliverable should be one pooled **organization-year** dataset, with one row
per `EIN + tax_year`, plus source-stage companion artifacts that stay source-preserving.

This proposal mirrors the current working pipeline model:
- **BMF** is the census, classification, and reference-total layer.
- **Postcard** is the small-filer supplement for `tax_year >= 2022`.
- **GivingTuesday** and **NCCS efile** are the detailed filing layers used for the limited-number
  organization analyses.
- **NCCS Core** is now a co-equal source-specific filing layer with its own analysis package, but it
  should still not define the primary pooled cleaned-data shape because its current analysis scope is
  `2022` only and intentionally preserves overlapping `PC` / `PZ` rows.

## Proposed primary deliverable

### Grain
- One row per `organization-year`
- Primary key: `harm_ein` + `harm_tax_year`

### Proposed base shape

| Field family | Proposed harmonized columns | Why they belong in the base table |
|---|---|---|
| Identity | `harm_ein`, `harm_tax_year`, `harm_org_name` | Needed for all aggregation, joining, and review workflows. |
| Geography | `harm_state`, `harm_zip5`, `harm_county_fips`, `harm_region` | Needed for county, region, and benchmark analyses. |
| Filing context | `harm_filing_form` | Needed to define the limited-number filing subset and to preserve form scope. |
| Classification | `harm_ntee_code`, `harm_subsection_code`, `harm_is_hospital`, `harm_is_university` | Needed for field representation and exclusion rules. |
| Financial totals | `harm_revenue_amount`, `harm_expenses_amount`, `harm_assets_amount`, `harm_income_amount` | Needed for total-revenue, total-assets, and limited-number financial-performance analyses. |
| Small-filer flag | `harm_gross_receipts_under_25000_flag` | Needed to preserve postcard-only small-filer semantics. |
| Provenance | per-field provenance columns or a compact provenance summary | Needed so harmonized values stay traceable to their source family and source column. |

## Source-specific optional fields

These fields should stay available in source-stage artifacts and may be carried into a wide audit
version of the pooled table, but they are not required to exist in every analysis-ready view.

| Optional field family | Candidate current columns | Why optional |
|---|---|---|
| Detailed revenue-source components | `TOTPROSERREV`, `PROGSERVREVE`, `TOTACASHCONT`, `NONCASCONTRI`, `ALLOOTHECONT`, `FOREGRANTOTA`, `GOVERNGRANTS`, `GRANTOORORGA` | Only relevant to the limited-number filing analyses. |
| Postcard snapshot context | `snapshot_year`, `snapshot_month`, `benchmark_match_source` | Useful for audit and coverage explanation, but not needed in every pooled analysis view. |
| BMF exact-year lookup provenance | `harm_*__source_family`, `harm_*__source_variant`, `harm_*__source_column` from the exact-year lookup artifacts | Useful for audit and overlay review, but not required in a slim analysis view. |

## Pooling rule

Default behavior:
- Pool organizations together across filing types in the main `organization-year` deliverable.
- Do **not** create separate default tables by filing form.
- Handle form-specific differences through harmonization, caveat notes, and explicit missingness.

Subset behavior:
- The limited-number-organization analyses should filter from the same pooled structure using
  `harm_filing_form` and source coverage notes.
- `990PF` coverage is currently strongest through GivingTuesday, not NCCS efile.

## Required companion artifacts

The pooled table should not stand alone. It should be accompanied by:
- `990_variable_analysis_crosswalk.md` as the variable-level source-of-truth map
- source-specific deliverable briefs for GT, NCCS 990, BMF, and postcard
- `990_open_questions.md` for unresolved scope and definition choices
- source-stage filtered outputs that preserve source-native fields and file semantics

## Deliverable hierarchy

1. **Source-stage artifacts**
   - `givingtuesday_990_filings_benchmark.parquet`
   - `nccs_efile_benchmark_tax_year=YYYY.parquet`
   - `nccs_bmf_benchmark_year=YYYY.parquet`
   - `nccs_bmf_exact_year_lookup_year=YYYY.parquet`
   - `nccs_990_postcard_benchmark_tax_year_start=2022_snapshot_year=YYYY.parquet`
2. **Pooled harmonized organization-year artifact**
   - current model aligns most closely with the `combined_990_master_ein_tax_year.parquet` shape
3. **Optional slim analysis view**
   - same grain as the pooled artifact
   - only the harmonized columns actually needed for the analysis package

## Default field behaviors

- Use the current `harm_*` names as the default harmonized field vocabulary.
- Treat source-specific missingness as valid when a source truly does not supply a concept.
- Keep explicit provenance attached to harmonized values whenever the field is selected from more
  than one possible source family.
- Keep postcard and BMF-driven census coverage separate from detailed filing semantics.

## Current caveats that should remain explicit

- BMF counts and reference totals are not the same thing as detailed filing-level financial statements.
- Postcard extends small-organization coverage, but it is not a detailed finance source.
- NCCS efile is the primary current NCCS 990 source, but it does not cover `990PF` in the active
  benchmark output.
- Some limited-number variables, especially contribution and grant subcomponents, still require final
  analytic definitions before they should be locked into a single reporting field.
