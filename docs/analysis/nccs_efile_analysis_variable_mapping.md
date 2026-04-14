# NCCS Efile Analysis Variable Mapping

- Source family: `nccs_efile`
- Analysis output: `data/321_Black_Hills_Area_Community_Foundation_2025_08/01_data/staging/nccs_efile/nccs_efile_analysis_variables.parquet`
- Region metrics output: `data/321_Black_Hills_Area_Community_Foundation_2025_08/01_data/staging/nccs_efile/nccs_efile_analysis_geography_metrics.parquet`
- Coverage report: `data/321_Black_Hills_Area_Community_Foundation_2025_08/01_data/raw/nccs_efile/metadata/nccs_efile_analysis_variable_coverage.csv`
- Scope: `2022-2024` only
- Coding rules: [`CODING_RULES.md`](../../secrets/coding_rules/CODING_RULES.md)

## Extracted Variables

|canonical_variable|variable_role|draft_variable|source_rule|provenance_column|notes|
|---|---|---|---|---|---|
|analysis_total_revenue_amount|direct|Total revenue|Direct from harm_revenue_amount|analysis_total_revenue_amount_source_column|Efile total revenue direct field.|
|analysis_total_expense_amount|direct|Total expense|Direct from harm_expenses_amount|analysis_total_expense_amount_source_column|Efile total expense direct field.|
|analysis_total_assets_amount|direct|Total assets|Direct from harm_assets_amount|analysis_total_assets_amount_source_column|Direct asset field used for the draft's total-assets questions.|
|analysis_net_asset_amount|proxy|Net asset|Asset-based proxy carried from harm_assets_amount|analysis_net_asset_amount_source_column|Conservative proxy preserved for draft financial-performance work.|
|analysis_calculated_surplus_amount|direct|Surplus|Direct from harm_income_amount, carried as the draft surplus metric|analysis_calculated_surplus_amount_source_column|Current-year revenue-less-expense amount already produced upstream.|
|analysis_calculated_net_margin_ratio|calculated|Net margin|analysis_calculated_surplus_amount / positive analysis_total_revenue_amount|analysis_calculated_net_margin_ratio_source_column|Null when revenue is missing, zero, or negative.|
|analysis_calculated_months_of_reserves|calculated|Months of reserves|(analysis_net_asset_amount / positive analysis_total_expense_amount) * 12|analysis_calculated_months_of_reserves_source_column|Uses the conservative asset-based net-asset proxy.|
|analysis_ntee_code|enriched|NTEE filed classification code|NCCS BMF exact-year, then nearest-year, then IRS EO BMF EIN fallback|analysis_ntee_code_source_column|Classification enrichment only, not a row-admission filter.|
|analysis_subsection_code|enriched|Subsection code|NCCS BMF exact-year, then nearest-year, then IRS EO BMF EIN fallback|analysis_subsection_code_source_column|Supports political-organization classification.|
|analysis_calculated_ntee_broad_code|calculated|Broad NTEE field classification code|First letter of analysis_ntee_code|analysis_calculated_ntee_broad_code_source_column|Broad field category used in field-composition analyses.|
|analysis_is_hospital|direct|Hospital flag|Direct from efile hospital flag|analysis_is_hospital_source_column|Canonical efile source-backed hospital flag.|
|analysis_is_university|direct|University flag|Direct from efile school/university flag|analysis_is_university_source_column|Canonical efile source-backed university flag.|
|analysis_is_political_org|proxy|Political organization flag|Source-backed subsection proxy|analysis_is_political_org_source_column|Canonical political-organization proxy for exclusion analyses.|

## Unavailable Variables

|canonical_variable|availability_status|draft_variable|notes|
|---|---|---|---|
|analysis_program_service_revenue_amount|unavailable|Program service revenue|Keep unavailable rather than backfilling from GT.|
|analysis_calculated_total_contributions_amount|unavailable|Total contributions|Keep unavailable rather than backfilling from GT.|
|analysis_other_contributions_amount|unavailable|Other contributions|Keep unavailable rather than backfilling from GT.|
|analysis_calculated_grants_total_amount|unavailable|Grants (total amount)|Keep unavailable rather than backfilling from GT.|

## Draft Alignment Appendix

|draft_variable|source_specific_output|status|rule_or_reason|
|---|---|---|---|
|Total revenue|analysis_total_revenue_amount|direct|Direct from harm_revenue_amount|
|Total expense|analysis_total_expense_amount|direct|Direct from harm_expenses_amount|
|Total assets|analysis_total_assets_amount|direct|Direct from harm_assets_amount|
|Net asset|analysis_net_asset_amount|proxy|Asset-based proxy carried from harm_assets_amount|
|Surplus|analysis_calculated_surplus_amount|direct|Direct from harm_income_amount, carried as the draft surplus metric|
|Net margin|analysis_calculated_net_margin_ratio|calculated|analysis_calculated_surplus_amount / positive analysis_total_revenue_amount|
|Months of reserves|analysis_calculated_months_of_reserves|calculated|(analysis_net_asset_amount / positive analysis_total_expense_amount) * 12|
|NTEE filed classification code|analysis_ntee_code|enriched|NCCS BMF exact-year, then nearest-year, then IRS EO BMF EIN fallback|
|Subsection code|analysis_subsection_code|enriched|NCCS BMF exact-year, then nearest-year, then IRS EO BMF EIN fallback|
|Broad NTEE field classification code|analysis_calculated_ntee_broad_code|calculated|First letter of analysis_ntee_code|
|Hospital flag|analysis_is_hospital|direct|Direct from efile hospital flag|
|University flag|analysis_is_university|direct|Direct from efile school/university flag|
|Political organization flag|analysis_is_political_org|proxy|Source-backed subsection proxy|
|Program service revenue|analysis_program_service_revenue_amount|unavailable|Keep unavailable rather than backfilling from GT.|
|Total contributions|analysis_calculated_total_contributions_amount|unavailable|Keep unavailable rather than backfilling from GT.|
|Other contributions|analysis_other_contributions_amount|unavailable|Keep unavailable rather than backfilling from GT.|
|Grants (total amount)|analysis_calculated_grants_total_amount|unavailable|Keep unavailable rather than backfilling from GT.|
