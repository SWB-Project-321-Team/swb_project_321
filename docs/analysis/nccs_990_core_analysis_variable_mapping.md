# NCCS Core Analysis Variable Mapping

- Analysis output: `data/321_Black_Hills_Area_Community_Foundation_2025_08/01_data/staging/nccs_990/core/nccs_990_core_analysis_variables.parquet`
- Geography metrics output: `data/321_Black_Hills_Area_Community_Foundation_2025_08/01_data/staging/nccs_990/core/nccs_990_core_analysis_geography_metrics.parquet`
- Coverage report: `data/321_Black_Hills_Area_Community_Foundation_2025_08/01_data/raw/nccs_990/core/metadata/nccs_990_core_analysis_variable_coverage.csv`
- Scope: `2022` only
- Core filing context: `core_scope` and `core_family` are authoritative.
- Overlapping `PC` and `PZ` rows are intentionally preserved in the final analysis output.
- `harm_filing_form` is intentionally sparse and only populated when Core exposes a reliable form signal.
- `analysis_calculated_cleaned_net_margin_ratio` keeps the raw net-margin ratio but blanks rows with total revenue below `1000` or absolute ratio above `10`.
- `analysis_ntee_code` uses an explicit `UNKNOWN` sentinel when every classification source is blank.
- `analysis_calculated_months_of_reserves` writes `0` for zero-assets/zero-expense rows and `inf` for positive-assets/zero-expense rows.

## Extracted Variables

|canonical_variable|variable_role|draft_variable|source_rule|provenance_column|notes|
|---|---|---|---|---|---|
|analysis_total_revenue_amount|direct|Total revenue|Scope-aware direct Core revenue field|analysis_total_revenue_amount_source_column|PZ/PC revenue totals and PF books revenue are kept separate upstream, then harmonized here.|
|analysis_total_expense_amount|direct|Total expense|Scope-aware direct Core expense field|analysis_total_expense_amount_source_column|Expense totals come directly from the filtered Core benchmark files.|
|analysis_total_assets_amount|direct|Total assets|Scope-aware direct Core asset field|analysis_total_assets_amount_source_column|Uses EOY total assets for PC/PZ and PF book-value total assets.|
|analysis_net_asset_amount|direct|Net asset|Scope-aware direct Core net-asset field|analysis_net_asset_amount_source_column|Net-asset or fund-balance concept remains source-faithful.|
|analysis_calculated_surplus_amount|calculated|Surplus|Prefer source direct surplus where available, else revenue minus expense|analysis_calculated_surplus_amount_source_column|PZ and PF carry direct surplus-like fields; PC falls back to revenue minus expense.|
|analysis_calculated_net_margin_ratio|calculated|Net margin|analysis_calculated_surplus_amount / positive analysis_total_revenue_amount|analysis_calculated_net_margin_ratio_source_column|Null when revenue is missing, zero, or negative.|
|analysis_calculated_cleaned_net_margin_ratio|calculated|Net margin (cleaned)|analysis_calculated_net_margin_ratio when analysis_total_revenue_amount >= 1000 and abs(ratio) <= 10|analysis_calculated_cleaned_net_margin_ratio_source_column|Convenience field that suppresses tiny-denominator and extreme-ratio outliers while preserving the raw ratio separately.|
|analysis_calculated_months_of_reserves|calculated|Months of reserves|(analysis_net_asset_amount / positive analysis_total_expense_amount) * 12|analysis_calculated_months_of_reserves_source_column|Only calculated when expense is present and positive.|
|analysis_program_service_revenue_amount|direct|Program service revenue|Core program-service fields by scope|analysis_program_service_revenue_amount_source_column|Promoted draft-aligned Core program-service revenue field.|
|analysis_program_service_revenue_candidate_amount|direct|Program service revenue candidate|Core program-service candidate fields by scope|analysis_program_service_revenue_candidate_amount_source_column|Source-faithful supporting field retained alongside the promoted draft-aligned variable.|
|analysis_calculated_total_contributions_amount|direct|Total contributions|Core contribution fields by scope|analysis_calculated_total_contributions_amount_source_column|Promoted draft-aligned total-contributions field from the best available Core contribution concept.|
|analysis_contribution_candidate_amount|direct|Contribution candidate|Core contribution candidate fields by scope|analysis_contribution_candidate_amount_source_column|Source-faithful supporting field retained alongside the promoted draft-aligned variable.|
|analysis_gifts_grants_received_candidate_amount|direct|Gifts/grants received candidate|Schedule A public-support gift/grant contribution candidates when present|analysis_gifts_grants_received_candidate_amount_source_column|Kept distinct from contribution and grants-paid concepts.|
|analysis_grants_paid_candidate_amount|calculated|Grants paid candidate|Row-wise sum of Core grant-paid components, or PF contribution-paid books field|analysis_grants_paid_candidate_amount_source_column|Distinct grant-paid concept preserved for analysts.|
|analysis_ntee_code|enriched|NTEE filed classification code|Exact-year BMF, then nearest-year BMF, then IRS EO BMF EIN fallback; unresolved rows are marked UNKNOWN|analysis_ntee_code_source_column|Classification enrichment only; unresolved rows remain explicitly unknown rather than blank.|
|analysis_subsection_code|enriched|Subsection code|Exact-year BMF, then nearest-year BMF, then IRS EO BMF EIN fallback|analysis_subsection_code_source_column|Supports political-organization classification.|
|analysis_calculated_ntee_broad_code|calculated|Broad NTEE field classification code|First letter of analysis_ntee_code|analysis_calculated_ntee_broad_code_source_column|Broad field code for field-composition analyses.|
|analysis_is_hospital|proxy|Hospital flag|Direct Core flag when present, else NTEE proxy|analysis_is_hospital_source_column|Source-backed hospital flag used before name-based imputation.|
|analysis_is_university|proxy|University flag|Direct Core flag when present, else NTEE proxy|analysis_is_university_source_column|Source-backed university flag used before name-based imputation.|
|analysis_is_political_org|proxy|Political organization flag|Subsection proxy from analysis_subsection_code|analysis_is_political_org_source_column|Source-backed political proxy used before name-based imputation.|
|analysis_imputed_is_hospital|imputed|Hospital flag|Canonical source-backed flag, then high-confidence name match, then default False|analysis_imputed_is_hospital_source_column|Complete exclusion-ready hospital flag.|
|analysis_imputed_is_university|imputed|University flag|Canonical source-backed flag, then high-confidence name match, then default False|analysis_imputed_is_university_source_column|Complete exclusion-ready university flag.|
|analysis_imputed_is_political_org|imputed|Political organization flag|Canonical source-backed flag, then high-confidence name match, then default False|analysis_imputed_is_political_org_source_column|Complete exclusion-ready political flag.|

## Unavailable Variables

|canonical_variable|availability_status|draft_variable|notes|
|---|---|---|---|
|analysis_other_contributions_amount|unavailable|Other contributions|Unavailable by design.|
|analysis_calculated_grants_total_amount|unavailable|Grants total|Use the distinct candidate fields instead.|
|analysis_foundation_grants_amount|unavailable|Foundation grants|Unavailable by design.|
|analysis_government_grants_amount|unavailable|Government grants|Unavailable by design.|
