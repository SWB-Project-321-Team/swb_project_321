"""
Step 07: Build NCCS Core analysis outputs for the 2022-only Core benchmark layer.
"""

from __future__ import annotations

import argparse
import importlib.util
import sys
import time
from pathlib import Path
from typing import Any

import pandas as pd
from tqdm import tqdm


def _ensure_sibling_common_loaded() -> None:
    common_path = Path(__file__).with_name("common.py").resolve()
    current = sys.modules.get("common")
    current_file = getattr(current, "__file__", None)
    if current_file and Path(current_file).resolve() == common_path:
        return
    spec = importlib.util.spec_from_file_location("common", common_path)
    if spec is None or spec.loader is None:
        raise ImportError(f"Unable to load common module from {common_path}")
    module = importlib.util.module_from_spec(spec)
    sys.modules["common"] = module
    spec.loader.exec_module(module)


_ensure_sibling_common_loaded()

from common import (
    ANALYSIS_COVERAGE_PREFIX,
    ANALYSIS_DOCUMENTATION_PREFIX,
    ANALYSIS_PREFIX,
    ANALYSIS_VARIABLE_MAPPING_PREFIX,
    BMF_CATALOG_URL,
    CORE_CATALOG_URL,
    DEFAULT_S3_BUCKET,
    DEFAULT_S3_REGION,
    META_DIR,
    STAGING_DIR,
    analysis_data_processing_doc_path,
    analysis_geography_metrics_output_path,
    analysis_variable_coverage_path,
    analysis_variable_mapping_path,
    analysis_variables_output_path,
    banner,
    combined_filtered_output_path,
    load_env_from_secrets,
    print_elapsed,
)
from ingest._shared.analysis_support import (
    DEFAULT_BMF_STAGING_DIR,
    DEFAULT_IRS_BMF_RAW_DIR,
    apply_classification_fallbacks,
    apply_imputed_flag_family,
    blank_to_na,
    build_ntee_proxy_flag,
    build_political_proxy_flag,
    build_ratio_metric,
    discover_bmf_exact_year_lookup_inputs,
    discover_irs_bmf_raw_inputs,
    empty_string_series,
    load_bmf_classification_lookup,
    load_irs_bmf_classification_lookup,
    normalize_bool_text,
    normalize_ein,
    numeric_from_text,
)
from utils.paths import DATA as DATA_ROOT

CORE_ANALYSIS_TAX_YEAR = 2022
_REPO_ROOT = Path(__file__).resolve().parents[3]
CORE_CLEANED_NET_MARGIN_MIN_REVENUE = 1000.0
CORE_CLEANED_NET_MARGIN_MAX_ABS_RATIO = 10.0

AVAILABLE_VARIABLE_METADATA = [
    {"canonical_variable": "analysis_total_revenue_amount", "variable_role": "direct", "analysis_requirement": "Total revenue", "source_rule": "Scope-aware direct Core revenue field", "provenance_column": "analysis_total_revenue_amount_source_column", "notes": "PZ/PC revenue totals and PF books revenue are kept separate upstream, then harmonized here."},
    {"canonical_variable": "analysis_total_expense_amount", "variable_role": "direct", "analysis_requirement": "Total expense", "source_rule": "Scope-aware direct Core expense field", "provenance_column": "analysis_total_expense_amount_source_column", "notes": "Expense totals come directly from the filtered Core benchmark files."},
    {"canonical_variable": "analysis_total_assets_amount", "variable_role": "direct", "analysis_requirement": "Total assets", "source_rule": "Scope-aware direct Core asset field", "provenance_column": "analysis_total_assets_amount_source_column", "notes": "Uses EOY total assets for PC/PZ and PF book-value total assets."},
    {"canonical_variable": "analysis_net_asset_amount", "variable_role": "direct", "analysis_requirement": "Net asset", "source_rule": "Scope-aware direct Core net-asset field", "provenance_column": "analysis_net_asset_amount_source_column", "notes": "Net-asset or fund-balance concept remains source-faithful."},
    {"canonical_variable": "analysis_calculated_surplus_amount", "variable_role": "calculated", "analysis_requirement": "Surplus", "source_rule": "Prefer source direct surplus where available, else revenue minus expense", "provenance_column": "analysis_calculated_surplus_amount_source_column", "notes": "PZ and PF carry direct surplus-like fields; PC falls back to revenue minus expense."},
    {"canonical_variable": "analysis_calculated_net_margin_ratio", "variable_role": "calculated", "analysis_requirement": "Net margin", "source_rule": "analysis_calculated_surplus_amount / positive analysis_total_revenue_amount", "provenance_column": "analysis_calculated_net_margin_ratio_source_column", "notes": "Null when revenue is missing, zero, or negative."},
    {"canonical_variable": "analysis_calculated_cleaned_net_margin_ratio", "variable_role": "calculated", "analysis_requirement": "Net margin (cleaned)", "source_rule": f"analysis_calculated_net_margin_ratio when analysis_total_revenue_amount >= {CORE_CLEANED_NET_MARGIN_MIN_REVENUE:.0f} and abs(ratio) <= {CORE_CLEANED_NET_MARGIN_MAX_ABS_RATIO:g}", "provenance_column": "analysis_calculated_cleaned_net_margin_ratio_source_column", "notes": "Convenience field that suppresses tiny-denominator and extreme-ratio outliers while preserving the raw ratio separately."},
    {"canonical_variable": "analysis_calculated_months_of_reserves", "variable_role": "calculated", "analysis_requirement": "Months of reserves", "source_rule": "(analysis_net_asset_amount / positive analysis_total_expense_amount) * 12", "provenance_column": "analysis_calculated_months_of_reserves_source_column", "notes": "Only calculated when expense is present and positive."},
    {"canonical_variable": "analysis_program_service_revenue_amount", "variable_role": "direct", "analysis_requirement": "Program service revenue", "source_rule": "Core program-service fields by scope", "provenance_column": "analysis_program_service_revenue_amount_source_column", "notes": "Promoted requirement-aligned Core program-service revenue field."},
    {"canonical_variable": "analysis_program_service_revenue_candidate_amount", "variable_role": "direct", "analysis_requirement": "Program service revenue candidate", "source_rule": "Core program-service candidate fields by scope", "provenance_column": "analysis_program_service_revenue_candidate_amount_source_column", "notes": "Source-faithful supporting field retained alongside the promoted requirement-aligned variable."},
    {"canonical_variable": "analysis_calculated_total_contributions_amount", "variable_role": "direct", "analysis_requirement": "Total contributions", "source_rule": "Core contribution fields by scope", "provenance_column": "analysis_calculated_total_contributions_amount_source_column", "notes": "Promoted requirement-aligned total-contributions field from the best available Core contribution concept."},
    {"canonical_variable": "analysis_contribution_candidate_amount", "variable_role": "direct", "analysis_requirement": "Contribution candidate", "source_rule": "Core contribution candidate fields by scope", "provenance_column": "analysis_contribution_candidate_amount_source_column", "notes": "Source-faithful supporting field retained alongside the promoted requirement-aligned variable."},
    {"canonical_variable": "analysis_gifts_grants_received_candidate_amount", "variable_role": "direct", "analysis_requirement": "Gifts/grants received candidate", "source_rule": "Schedule A public-support gift/grant contribution candidates when present", "provenance_column": "analysis_gifts_grants_received_candidate_amount_source_column", "notes": "Kept distinct from contribution and grants-paid concepts."},
    {"canonical_variable": "analysis_grants_paid_candidate_amount", "variable_role": "calculated", "analysis_requirement": "Grants paid candidate", "source_rule": "Row-wise sum of Core grant-paid components, or PF contribution-paid books field", "provenance_column": "analysis_grants_paid_candidate_amount_source_column", "notes": "Distinct grant-paid concept preserved for analysts."},
    {"canonical_variable": "analysis_ntee_code", "variable_role": "enriched", "analysis_requirement": "NTEE filed classification code", "source_rule": "Exact-year BMF, then nearest-year BMF, then IRS EO BMF EIN fallback; unresolved rows are marked UNKNOWN", "provenance_column": "analysis_ntee_code_source_column", "notes": "Classification enrichment only; unresolved rows remain explicitly unknown rather than blank."},
    {"canonical_variable": "analysis_subsection_code", "variable_role": "enriched", "analysis_requirement": "Subsection code", "source_rule": "Exact-year BMF, then nearest-year BMF, then IRS EO BMF EIN fallback", "provenance_column": "analysis_subsection_code_source_column", "notes": "Supports political-organization classification."},
    {"canonical_variable": "analysis_calculated_ntee_broad_code", "variable_role": "calculated", "analysis_requirement": "Broad NTEE field classification code", "source_rule": "First letter of analysis_ntee_code", "provenance_column": "analysis_calculated_ntee_broad_code_source_column", "notes": "Broad field code for field-composition analyses."},
    {"canonical_variable": "analysis_is_hospital", "variable_role": "proxy", "analysis_requirement": "Hospital flag", "source_rule": "Direct Core flag when present, else NTEE proxy", "provenance_column": "analysis_is_hospital_source_column", "notes": "Source-backed hospital flag used before name-based imputation."},
    {"canonical_variable": "analysis_is_university", "variable_role": "proxy", "analysis_requirement": "University flag", "source_rule": "Direct Core flag when present, else NTEE proxy", "provenance_column": "analysis_is_university_source_column", "notes": "Source-backed university flag used before name-based imputation."},
    {"canonical_variable": "analysis_is_political_org", "variable_role": "proxy", "analysis_requirement": "Political organization flag", "source_rule": "Subsection proxy from analysis_subsection_code", "provenance_column": "analysis_is_political_org_source_column", "notes": "Source-backed political proxy used before name-based imputation."},
    {"canonical_variable": "analysis_imputed_is_hospital", "variable_role": "imputed", "analysis_requirement": "Hospital flag", "source_rule": "Canonical source-backed flag, then high-confidence name match, then default False", "provenance_column": "analysis_imputed_is_hospital_source_column", "notes": "Complete exclusion-ready hospital flag."},
    {"canonical_variable": "analysis_imputed_is_university", "variable_role": "imputed", "analysis_requirement": "University flag", "source_rule": "Canonical source-backed flag, then high-confidence name match, then default False", "provenance_column": "analysis_imputed_is_university_source_column", "notes": "Complete exclusion-ready university flag."},
    {"canonical_variable": "analysis_imputed_is_political_org", "variable_role": "imputed", "analysis_requirement": "Political organization flag", "source_rule": "Canonical source-backed flag, then high-confidence name match, then default False", "provenance_column": "analysis_imputed_is_political_org_source_column", "notes": "Complete exclusion-ready political flag."},
]

UNAVAILABLE_VARIABLES = [
    {"canonical_variable": "analysis_other_contributions_amount", "availability_status": "unavailable", "variable_role": "unavailable", "analysis_requirement": "Other contributions", "source_rule": "Core does not provide a directly aligned field in the current pipeline.", "notes": "Unavailable by design."},
    {"canonical_variable": "analysis_calculated_grants_total_amount", "availability_status": "unavailable", "variable_role": "unavailable", "analysis_requirement": "Grants total", "source_rule": "Grant-received and grant-paid concepts remain separate in Core.", "notes": "Use the distinct candidate fields instead."},
    {"canonical_variable": "analysis_foundation_grants_amount", "availability_status": "unavailable", "variable_role": "unavailable", "analysis_requirement": "Foundation grants", "source_rule": "No current direct Core mapping.", "notes": "Unavailable by design."},
    {"canonical_variable": "analysis_government_grants_amount", "availability_status": "unavailable", "variable_role": "unavailable", "analysis_requirement": "Government grants", "source_rule": "No current direct Core mapping.", "notes": "Unavailable by design."},
]


def _ensure_parent_dirs(*paths: Path) -> None:
    for path in paths:
        path.parent.mkdir(parents=True, exist_ok=True)
        print(f"[paths] Ready: {path.parent}", flush=True)


def _repo_relative_display(path: Path) -> str:
    resolved = path.resolve()
    try:
        return f"SWB_321_DATA_ROOT/{resolved.relative_to(DATA_ROOT.resolve()).as_posix()}"
    except ValueError:
        pass
    try:
        return resolved.relative_to(_REPO_ROOT).as_posix()
    except ValueError:
        return resolved.as_posix()


def _match_source_variant(path: Path) -> tuple[str, str, str]:
    stem = path.name.replace("__benchmark.csv", "")
    if "501C3-CHARITIES-PC" in stem:
        return "501c3_charities", "pc", "501c3_charities_pc"
    if "501C3-CHARITIES-PZ" in stem:
        return "501c3_charities", "pz", "501c3_charities_pz"
    if "501CE-NONPROFIT-PC" in stem:
        return "501ce_nonprofit", "pc", "501ce_nonprofit_pc"
    if "501CE-NONPROFIT-PZ" in stem:
        return "501ce_nonprofit", "pz", "501ce_nonprofit_pz"
    if "501C3-PRIVFOUND-PF" in stem:
        return "501c3_privfound", "pf", "501c3_privfound_pf"
    raise ValueError(f"Unrecognized Core benchmark file: {path.name}")


def _benchmark_files_for_year(staging_dir: Path, tax_year: int) -> list[Path]:
    year_dir = staging_dir / f"year={tax_year}"
    files = sorted(year_dir.glob("*__benchmark.csv"))
    if not files:
        raise FileNotFoundError(f"No Core benchmark files found under {year_dir}")
    return files


def _load_filtered_core_source(staging_dir: Path, tax_year: int) -> pd.DataFrame:
    combined_path = combined_filtered_output_path(staging_dir, tax_year)
    if combined_path.exists():
        print(f"[analysis] Using combined filtered Core parquet: {combined_path}", flush=True)
        combined_df = pd.read_parquet(combined_path).copy()
        if "harm_tax_year" not in combined_df.columns:
            combined_df["harm_tax_year"] = str(tax_year)
        if "core_analysis_row_id" not in combined_df.columns:
            combined_df["core_analysis_row_id"] = [
                f"{combined_df.get('core_source_file', pd.Series('combined_filtered', index=combined_df.index)).astype('string').iloc[idx]}::row={idx + 1}"
                for idx in range(len(combined_df))
            ]
        return combined_df

    benchmark_files = _benchmark_files_for_year(staging_dir, tax_year)
    print(f"[analysis] Benchmark files discovered: {len(benchmark_files)}", flush=True)
    frames: list[pd.DataFrame] = []
    for benchmark_path in tqdm(benchmark_files, desc="load core benchmark files", unit="file"):
        family, scope, variant = _match_source_variant(benchmark_path)
        frame = pd.read_csv(benchmark_path, dtype=str, low_memory=False)
        frame["core_family"] = family
        frame["core_scope"] = scope
        frame["core_source_file"] = benchmark_path.name
        frame["core_source_variant"] = variant
        frame["harm_tax_year"] = str(tax_year)
        frame["core_analysis_row_id"] = [f"{benchmark_path.name}::row={idx + 1}" for idx in range(len(frame))]
        frames.append(frame)
    return pd.concat(frames, ignore_index=True, sort=False).copy()


def _load_bmf_identity_lookup(bmf_staging_dir: Path, tax_year: int) -> pd.DataFrame:
    lookup_paths_by_year = discover_bmf_exact_year_lookup_inputs(bmf_staging_dir)
    frames: list[pd.DataFrame] = []
    for source_tax_year, lookup_path in sorted(lookup_paths_by_year.items(), key=lambda item: int(item[0])):
        frame = pd.read_parquet(
            lookup_path,
            columns=[
                "harm_ein",
                "harm_tax_year",
                "harm_org_name",
                "harm_state",
                "harm_zip5",
                "harm_org_name__source_column",
                "harm_state__source_column",
                "harm_zip5__source_column",
            ],
        ).rename(
            columns={
                "harm_ein": "ein",
                "harm_tax_year": "source_tax_year",
                "harm_org_name": "bmf_org_name",
                "harm_state": "bmf_state",
                "harm_zip5": "bmf_zip5",
                "harm_org_name__source_column": "bmf_org_name_source_column",
                "harm_state__source_column": "bmf_state_source_column",
                "harm_zip5__source_column": "bmf_zip5_source_column",
            }
        )
        frame["ein"] = frame["ein"].map(normalize_ein).astype("string")
        frame["source_tax_year"] = frame["source_tax_year"].astype("string")
        for column in ("bmf_org_name", "bmf_state", "bmf_zip5", "bmf_org_name_source_column", "bmf_state_source_column", "bmf_zip5_source_column"):
            frame[column] = blank_to_na(frame[column].astype("string"))
        frames.append(frame)
    lookup_df = pd.concat(frames, ignore_index=True)
    return lookup_df.drop_duplicates(subset=["ein", "source_tax_year"], keep="first")


def _load_irs_identity_lookup(irs_bmf_raw_dir: Path) -> pd.DataFrame:
    try:
        raw_paths = discover_irs_bmf_raw_inputs(irs_bmf_raw_dir)
    except FileNotFoundError:
        return pd.DataFrame(columns=["ein", "irs_org_name", "irs_state", "irs_zip5"])

    frames: list[pd.DataFrame] = []
    for raw_path in raw_paths:
        frame = pd.read_csv(
            raw_path,
            dtype=str,
            low_memory=False,
            usecols=lambda column_name: column_name in {"EIN", "NAME", "STATE", "ZIP"},
        )
        frame["ein"] = frame["EIN"].map(normalize_ein).astype("string")
        frame["irs_org_name"] = blank_to_na(frame["NAME"].astype("string")) if "NAME" in frame.columns else pd.Series(pd.NA, index=frame.index, dtype="string")
        frame["irs_state"] = blank_to_na(frame["STATE"].astype("string")) if "STATE" in frame.columns else pd.Series(pd.NA, index=frame.index, dtype="string")
        zip_text = frame["ZIP"].astype("string") if "ZIP" in frame.columns else pd.Series(pd.NA, index=frame.index, dtype="string")
        frame["irs_zip5"] = blank_to_na(zip_text.str.replace(r"\D", "", regex=True).str.slice(0, 5))
        frames.append(frame[["ein", "irs_org_name", "irs_state", "irs_zip5"]])
    if not frames:
        return pd.DataFrame(columns=["ein", "irs_org_name", "irs_state", "irs_zip5"])
    return pd.concat(frames, ignore_index=True).drop_duplicates(subset=["ein"], keep="first")


def _apply_identity_fallbacks(
    analysis_df: pd.DataFrame,
    *,
    source_df: pd.DataFrame,
    bmf_identity_lookup_df: pd.DataFrame,
    irs_identity_lookup_df: pd.DataFrame,
) -> None:
    source_keys = pd.DataFrame({"ein": analysis_df["harm_ein"].astype("string"), "tax_year": analysis_df["harm_tax_year"].astype("string")})

    exact_lookup = bmf_identity_lookup_df.rename(columns={"source_tax_year": "tax_year"}).drop_duplicates(subset=["ein", "tax_year"], keep="first")
    exact_join = source_keys.merge(exact_lookup, on=["ein", "tax_year"], how="left", validate="many_to_one")
    irs_join = source_keys.merge(irs_identity_lookup_df, on="ein", how="left", validate="many_to_one")

    for field_name, direct_columns, bmf_value_col, bmf_source_col, irs_value_col, irs_source_label in [
        ("harm_org_name", ["F9_00_ORG_NAME_L1", "NAME"], "bmf_org_name", "bmf_org_name_source_column", "irs_org_name", "NAME"),
        ("harm_state", ["F9_00_ORG_ADDR_STATE", "STATE"], "bmf_state", "bmf_state_source_column", "irs_state", "STATE"),
        ("harm_zip5", ["F9_00_ORG_ADDR_ZIP5", "F9_00_ORG_ADDR_ZIP_NEW", "ZIP5", "ZIP"], "bmf_zip5", "bmf_zip5_source_column", "irs_zip5", "ZIP"),
    ]:
        values, provenance = _build_text_with_fallbacks(
            source_df,
            direct_columns=direct_columns,
            fallback_series=exact_join[bmf_value_col].astype("string"),
            fallback_source_series=exact_join[bmf_source_col].astype("string"),
        )
        missing_mask = values.isna()
        if missing_mask.any():
            candidate_lookup = bmf_identity_lookup_df.loc[blank_to_na(bmf_identity_lookup_df[bmf_value_col].astype("string")).notna()].copy()
            source_missing = source_keys.loc[missing_mask, ["ein", "tax_year"]].copy()
            source_missing["row_index"] = source_missing.index
            source_missing["target_tax_year_int"] = pd.to_numeric(source_missing["tax_year"], errors="coerce")
            candidate_lookup["source_tax_year_int"] = pd.to_numeric(candidate_lookup["source_tax_year"], errors="coerce")
            merged = source_missing.merge(candidate_lookup, on="ein", how="inner", validate="many_to_many")
            if not merged.empty:
                merged["year_distance"] = (merged["target_tax_year_int"] - merged["source_tax_year_int"]).abs()
                merged = merged.sort_values(by=["row_index", "year_distance", "source_tax_year_int"], ascending=[True, True, True], kind="mergesort")
                chosen = merged.drop_duplicates(subset=["row_index"], keep="first").copy()
                values.loc[chosen["row_index"]] = chosen[bmf_value_col].astype("string").values
                provenance.loc[chosen["row_index"]] = chosen.apply(
                    lambda row: f"{row[bmf_source_col]}|nearest_year_identity_fallback_from_{row['tax_year']}_to_{row['source_tax_year']}",
                    axis=1,
                ).astype("string").values
        irs_fill_mask = values.isna() & blank_to_na(irs_join[irs_value_col].astype("string")).notna()
        values.loc[irs_fill_mask] = irs_join.loc[irs_fill_mask, irs_value_col].astype("string")
        provenance.loc[irs_fill_mask] = irs_source_label
        analysis_df[field_name] = values
        analysis_df[f"{field_name}_source_column"] = provenance


def _build_net_asset_amount(analysis_df: pd.DataFrame, source_df: pd.DataFrame) -> tuple[pd.Series, pd.Series]:
    values = pd.Series(pd.NA, index=source_df.index, dtype="Float64")
    provenance = pd.Series(pd.NA, index=source_df.index, dtype="string")
    non_pf_values, non_pf_provenance = _build_numeric_by_rule(
        source_df,
        [("pz", ["F9_01_NAFB_TOT_EOY", "F9_10_NAFB_TOT_EOY"]), ("pc", ["F9_10_NAFB_TOT_EOY"])],
        output_column="analysis_net_asset_amount_non_pf",
    )
    values.loc[non_pf_values.notna()] = non_pf_values.loc[non_pf_values.notna()]
    provenance.loc[non_pf_values.notna()] = non_pf_provenance.loc[non_pf_values.notna()]

    pf_direct = numeric_from_text(source_df.get("PF_02_NAFB_TOT_EOY_BV", pd.Series(pd.NA, index=source_df.index, dtype="string")))
    pf_direct_mask = source_df["core_scope"].astype("string").eq("pf") & pf_direct.notna()
    values.loc[pf_direct_mask] = pf_direct.loc[pf_direct_mask]
    provenance.loc[pf_direct_mask] = "PF_02_NAFB_TOT_EOY_BV"

    pf_assets = numeric_from_text(source_df.get("PF_02_ASSET_TOT_EOY_BV", pd.Series(pd.NA, index=source_df.index, dtype="string")))
    pf_liabilities = numeric_from_text(source_df.get("PF_02_LIAB_TOT_EOY_BV", pd.Series(pd.NA, index=source_df.index, dtype="string")))
    pf_derived = (pf_assets - pf_liabilities).astype("Float64")
    pf_derived_mask = source_df["core_scope"].astype("string").eq("pf") & values.isna() & pf_derived.notna()
    values.loc[pf_derived_mask] = pf_derived.loc[pf_derived_mask]
    provenance.loc[pf_derived_mask] = "PF_02_ASSET_TOT_EOY_BV - PF_02_LIAB_TOT_EOY_BV"
    print(f"[analysis] analysis_net_asset_amount: populated={int(values.notna().sum()):,}", flush=True)
    return values, provenance


def _build_text_with_fallbacks(
    source_df: pd.DataFrame,
    *,
    direct_columns: list[str],
    fallback_series: pd.Series,
    fallback_source_series: pd.Series,
) -> tuple[pd.Series, pd.Series]:
    values = pd.Series(pd.NA, index=source_df.index, dtype="string")
    provenance = pd.Series(pd.NA, index=source_df.index, dtype="string")
    for column in direct_columns:
        if column not in source_df.columns:
            continue
        candidate = blank_to_na(source_df[column].astype("string"))
        fill_mask = values.isna() & candidate.notna()
        values.loc[fill_mask] = candidate.loc[fill_mask]
        provenance.loc[fill_mask] = column
    fallback_mask = values.isna() & fallback_series.notna()
    values.loc[fallback_mask] = fallback_series.loc[fallback_mask]
    provenance.loc[fallback_mask] = fallback_source_series.loc[fallback_mask]
    return values, provenance


def _build_numeric_by_rule(
    source_df: pd.DataFrame,
    rules: list[tuple[str, list[str]]],
    *,
    output_column: str,
) -> tuple[pd.Series, pd.Series]:
    values = pd.Series(pd.NA, index=source_df.index, dtype="Float64")
    provenance = pd.Series(pd.NA, index=source_df.index, dtype="string")
    for scope, columns in rules:
        scope_mask = source_df["core_scope"].astype("string").eq(scope)
        for column in columns:
            if column not in source_df.columns:
                continue
            candidate = numeric_from_text(source_df[column])
            fill_mask = scope_mask & values.isna() & candidate.notna()
            values.loc[fill_mask] = candidate.loc[fill_mask]
            provenance.loc[fill_mask] = column
    print(f"[analysis] {output_column}: populated={int(values.notna().sum()):,}", flush=True)
    return values, provenance


def _build_sum_candidate_by_rule(
    source_df: pd.DataFrame,
    *,
    scope: str,
    columns: list[str],
    output_column: str,
) -> tuple[pd.Series, pd.Series]:
    values = pd.Series(pd.NA, index=source_df.index, dtype="Float64")
    provenance = pd.Series(pd.NA, index=source_df.index, dtype="string")
    usable_columns = [column for column in columns if column in source_df.columns]
    if usable_columns:
        numeric_parts = [numeric_from_text(source_df[column]) for column in usable_columns]
        sum_frame = pd.concat(numeric_parts, axis=1)
        sum_values = sum_frame.sum(axis=1, min_count=1).astype("Float64")
        fill_mask = source_df["core_scope"].astype("string").eq(scope) & sum_values.notna()
        values.loc[fill_mask] = sum_values.loc[fill_mask]
        provenance.loc[fill_mask] = " + ".join(usable_columns)
    print(f"[analysis] {output_column}: populated={int(values.notna().sum()):,}", flush=True)
    return values, provenance


def _build_surplus_amount(analysis_df: pd.DataFrame, source_df: pd.DataFrame) -> tuple[pd.Series, pd.Series]:
    values = pd.Series(pd.NA, index=source_df.index, dtype="Float64")
    provenance = pd.Series(pd.NA, index=source_df.index, dtype="string")
    direct_rules = [("pz", ["F9_01_EXP_REV_LESS_EXP_CY"]), ("pf", ["PF_01_EXCESS_REV_OVER_EXP_BOOKS"])]
    for scope, columns in direct_rules:
        scope_mask = source_df["core_scope"].astype("string").eq(scope)
        for column in columns:
            if column not in source_df.columns:
                continue
            candidate = numeric_from_text(source_df[column])
            fill_mask = scope_mask & values.isna() & candidate.notna()
            values.loc[fill_mask] = candidate.loc[fill_mask]
            provenance.loc[fill_mask] = column
    derived = (analysis_df["analysis_total_revenue_amount"] - analysis_df["analysis_total_expense_amount"]).astype("Float64")
    derived_mask = values.isna() & derived.notna()
    values.loc[derived_mask] = derived.loc[derived_mask]
    provenance.loc[derived_mask] = "analysis_total_revenue_amount - analysis_total_expense_amount"
    print(f"[analysis] analysis_calculated_surplus_amount: populated={int(values.notna().sum()):,}", flush=True)
    return values, provenance


def _build_broad_ntee(ntee_series: pd.Series) -> tuple[pd.Series, pd.Series]:
    normalized = blank_to_na(ntee_series).astype("string").str.upper()
    valid_mask = normalized.str.fullmatch(r"[A-Z][0-9]{2}", na=False)
    broad = normalized.where(valid_mask).str.slice(0, 1)
    broad = broad.mask(broad.fillna("").eq(""), pd.NA)
    provenance = pd.Series(pd.NA, index=ntee_series.index, dtype="string")
    provenance.loc[broad.notna()] = "analysis_ntee_code"
    return broad, provenance


def _build_cleaned_net_margin_ratio(
    analysis_df: pd.DataFrame,
    *,
    minimum_revenue: float = CORE_CLEANED_NET_MARGIN_MIN_REVENUE,
    maximum_abs_ratio: float = CORE_CLEANED_NET_MARGIN_MAX_ABS_RATIO,
) -> tuple[pd.Series, pd.Series]:
    """Blank the convenience net-margin field when revenue is too small to be stable.

    The raw net-margin ratio remains available separately. This cleaned companion field
    only suppresses the tiny-denominator cases that create extreme values from a few
    dollars of reported revenue.
    """

    values = analysis_df["analysis_calculated_net_margin_ratio"].astype("Float64").copy()
    provenance = analysis_df["analysis_calculated_net_margin_ratio_source_column"].astype("string").copy()
    revenue = pd.to_numeric(analysis_df["analysis_total_revenue_amount"], errors="coerce")
    stable_mask = revenue.ge(minimum_revenue) & values.notna() & values.abs().le(maximum_abs_ratio)
    values.loc[~stable_mask] = pd.NA
    provenance.loc[~stable_mask] = pd.NA
    provenance.loc[stable_mask] = (
        provenance.loc[stable_mask].fillna("analysis_calculated_net_margin_ratio").astype("string")
        + f"|cleaned_revenue_floor_ge_{minimum_revenue:.0f}|cleaned_abs_ratio_le_{maximum_abs_ratio:g}"
    )
    print(
        f"[analysis] analysis_calculated_cleaned_net_margin_ratio: populated={int(values.notna().sum()):,} using revenue floor >= {minimum_revenue:.0f} and abs(ratio) <= {maximum_abs_ratio:g}",
        flush=True,
    )
    return values.astype("Float64"), provenance.astype("string")


def _fill_unknown_ntee_code(analysis_df: pd.DataFrame) -> None:
    missing_mask = analysis_df["analysis_ntee_code"].isna()
    if not missing_mask.any():
        return
    analysis_df.loc[missing_mask, "analysis_ntee_code"] = "UNKNOWN"
    analysis_df.loc[missing_mask, "analysis_ntee_code_source_family"] = "imputed"
    analysis_df.loc[missing_mask, "analysis_ntee_code_source_variant"] = "missing_all_sources"
    analysis_df.loc[missing_mask, "analysis_ntee_code_source_column"] = "UNKNOWN_SENTINEL"
    print(f"[analysis] Filled unresolved NTEE rows with UNKNOWN sentinel: {int(missing_mask.sum()):,}", flush=True)


def _build_months_of_reserves(analysis_df: pd.DataFrame) -> tuple[pd.Series, pd.Series]:
    values, provenance = build_ratio_metric(
        analysis_df["analysis_net_asset_amount"] * 12,
        analysis_df["analysis_total_expense_amount"],
        provenance_label="(analysis_net_asset_amount * 12) / analysis_total_expense_amount",
        require_positive_denominator=True,
    )
    expense = pd.to_numeric(analysis_df["analysis_total_expense_amount"], errors="coerce")
    net_assets = pd.to_numeric(analysis_df["analysis_net_asset_amount"], errors="coerce")

    zero_expense_zero_assets = expense.eq(0) & net_assets.eq(0)
    if zero_expense_zero_assets.any():
        values.loc[zero_expense_zero_assets] = 0.0
        provenance.loc[zero_expense_zero_assets] = "zero_expense_and_zero_net_assets=>0"

    zero_expense_positive_assets = expense.eq(0) & net_assets.gt(0)
    if zero_expense_positive_assets.any():
        values.loc[zero_expense_positive_assets] = float("inf")
        provenance.loc[zero_expense_positive_assets] = "zero_expense_and_positive_net_assets=>inf"

    print(
        "[analysis] analysis_calculated_months_of_reserves: "
        f"finite={int(values.replace([float('inf'), float('-inf')], pd.NA).notna().sum()):,} "
        f"infinite={int(values.isin([float('inf'), float('-inf')]).sum()):,}",
        flush=True,
    )
    return values.astype("Float64"), provenance.astype("string")


def _combine_direct_and_proxy_flags(
    direct_values: pd.Series,
    direct_provenance: pd.Series,
    proxy_values: pd.Series,
    proxy_provenance: pd.Series,
) -> tuple[pd.Series, pd.Series]:
    combined = direct_values.astype("boolean").copy()
    provenance = direct_provenance.astype("string").copy()
    fill_mask = combined.isna() & proxy_values.notna()
    combined.loc[fill_mask] = proxy_values.loc[fill_mask]
    provenance.loc[fill_mask] = proxy_provenance.loc[fill_mask]
    return combined.astype("boolean"), provenance.astype("string")


def _build_region_metrics_output(analysis_df: pd.DataFrame) -> pd.DataFrame:
    scope_specs = [
        ("all_rows", pd.Series(True, index=analysis_df.index)),
        (
            "exclude_imputed_hospital_university_political_org",
            ~(
                analysis_df["analysis_imputed_is_hospital"].fillna(False).astype(bool)
                | analysis_df["analysis_imputed_is_university"].fillna(False).astype(bool)
                | analysis_df["analysis_imputed_is_political_org"].fillna(False).astype(bool)
            ),
        ),
    ]
    rows: list[dict[str, Any]] = []
    for exclusion_variant, mask in tqdm(scope_specs, desc="core geography scopes", unit="scope"):
        scoped_df = analysis_df.loc[mask].copy()
        grouped = scoped_df.groupby(["region", "tax_year"], dropna=False, sort=True)
        for (region, tax_year), group_df in grouped:
            filing_count = int(len(group_df))
            unique_ein_count = int(group_df["ein"].dropna().astype("string").nunique())
            revenue_sum = group_df["analysis_total_revenue_amount"].sum(min_count=1)
            expense_sum = group_df["analysis_total_expense_amount"].sum(min_count=1)
            assets_sum = group_df["analysis_total_assets_amount"].sum(min_count=1)
            surplus_sum = group_df["analysis_calculated_surplus_amount"].sum(min_count=1)
            rows.append(
                {
                    "region": region,
                    "tax_year": tax_year,
                    "analysis_exclusion_variant": exclusion_variant,
                    "nonprofit_count": filing_count,
                    "analysis_total_nonprofit_count": filing_count,
                    "unique_nonprofit_ein_count": unique_ein_count,
                    "analysis_total_revenue_amount_sum": revenue_sum if pd.notna(revenue_sum) else pd.NA,
                    "analysis_total_expense_amount_sum": expense_sum if pd.notna(expense_sum) else pd.NA,
                    "analysis_total_assets_amount_sum": assets_sum if pd.notna(assets_sum) else pd.NA,
                    "analysis_calculated_surplus_amount_sum": surplus_sum if pd.notna(surplus_sum) else pd.NA,
                    "analysis_program_service_revenue_amount_sum": group_df["analysis_program_service_revenue_amount"].sum(min_count=1),
                    "analysis_program_service_revenue_candidate_amount_sum": group_df["analysis_program_service_revenue_candidate_amount"].sum(min_count=1),
                    "analysis_calculated_total_contributions_amount_sum": group_df["analysis_calculated_total_contributions_amount"].sum(min_count=1),
                    "analysis_contribution_candidate_amount_sum": group_df["analysis_contribution_candidate_amount"].sum(min_count=1),
                    "analysis_gifts_grants_received_candidate_amount_sum": group_df["analysis_gifts_grants_received_candidate_amount"].sum(min_count=1),
                    "analysis_grants_paid_candidate_amount_sum": group_df["analysis_grants_paid_candidate_amount"].sum(min_count=1),
                    "analysis_calculated_normalized_total_revenue_per_nonprofit": (float(revenue_sum) / filing_count) if filing_count and pd.notna(revenue_sum) else pd.NA,
                    "analysis_calculated_normalized_total_revenue_per_unique_nonprofit": (float(revenue_sum) / unique_ein_count) if unique_ein_count and pd.notna(revenue_sum) else pd.NA,
                    "analysis_calculated_mean_net_margin_ratio": group_df["analysis_calculated_net_margin_ratio"].mean() if group_df["analysis_calculated_net_margin_ratio"].notna().any() else pd.NA,
                    "analysis_calculated_mean_cleaned_net_margin_ratio": group_df["analysis_calculated_cleaned_net_margin_ratio"].mean() if group_df["analysis_calculated_cleaned_net_margin_ratio"].notna().any() else pd.NA,
                    "analysis_calculated_mean_months_of_reserves": (
                        group_df["analysis_calculated_months_of_reserves"].replace([float("inf"), float("-inf")], pd.NA).mean()
                        if group_df["analysis_calculated_months_of_reserves"].replace([float("inf"), float("-inf")], pd.NA).notna().any()
                        else pd.NA
                    ),
                    "analysis_calculated_infinite_months_of_reserves_count": int(group_df["analysis_calculated_months_of_reserves"].isin([float("inf"), float("-inf")]).sum()),
                    "analysis_imputed_is_hospital_true_count": int(group_df["analysis_imputed_is_hospital"].fillna(False).astype(bool).sum()),
                    "analysis_imputed_is_university_true_count": int(group_df["analysis_imputed_is_university"].fillna(False).astype(bool).sum()),
                    "analysis_imputed_is_political_org_true_count": int(group_df["analysis_imputed_is_political_org"].fillna(False).astype(bool).sum()),
                }
            )
    return pd.DataFrame(rows).sort_values(["region", "tax_year", "analysis_exclusion_variant"], kind="mergesort").reset_index(drop=True)


def _build_coverage_rows(analysis_df: pd.DataFrame) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    metadata_rows = AVAILABLE_VARIABLE_METADATA + UNAVAILABLE_VARIABLES
    for (tax_year, core_scope), group_df in analysis_df.groupby(["tax_year", "core_scope"], dropna=False, sort=True):
        row_count = int(len(group_df))
        for metadata in metadata_rows:
            canonical = metadata["canonical_variable"]
            populated = int(group_df[canonical].notna().sum()) if canonical in group_df.columns else 0
            rows.append(
                {
                    "tax_year": tax_year,
                    "core_scope": core_scope,
                    "canonical_variable": canonical,
                    "variable_role": metadata["variable_role"],
                    "row_count": row_count,
                    "populated_row_count": populated,
                    "fill_rate": (populated / row_count) if row_count else 0.0,
                    "notes": metadata["notes"],
                }
            )
    return pd.DataFrame(rows)


def _write_mapping_markdown(mapping_path: Path) -> None:
    lines = [
        "# NCCS Core Analysis Variable Mapping",
        "",
        f"- Analysis output: `{_repo_relative_display(analysis_variables_output_path())}`",
        f"- Geography metrics output: `{_repo_relative_display(analysis_geography_metrics_output_path())}`",
        f"- Coverage report: `{_repo_relative_display(analysis_variable_coverage_path())}`",
        "- Scope: `2022` only",
        "- Core filing context: `core_scope` and `core_family` are authoritative.",
        "- Overlapping `PC` and `PZ` rows are intentionally preserved in the final analysis output.",
        "- `harm_filing_form` is intentionally sparse and only populated when Core exposes a reliable form signal.",
        f"- `analysis_calculated_cleaned_net_margin_ratio` keeps the raw net-margin ratio but blanks rows with total revenue below `{CORE_CLEANED_NET_MARGIN_MIN_REVENUE:.0f}` or absolute ratio above `{CORE_CLEANED_NET_MARGIN_MAX_ABS_RATIO:g}`.",
        "- `analysis_ntee_code` uses an explicit `UNKNOWN` sentinel when every classification source is blank.",
        "- `analysis_calculated_months_of_reserves` writes `0` for zero-assets/zero-expense rows and `inf` for positive-assets/zero-expense rows.",
        "",
        "## Coverage Evidence",
        "",
        f"- Generated coverage CSV: `{_repo_relative_display(analysis_variable_coverage_path())}`",
        f"- Published S3 variable mapping object: `{ANALYSIS_VARIABLE_MAPPING_PREFIX}/{analysis_variable_mapping_path().name}`",
        f"- Published S3 coverage object: `{ANALYSIS_COVERAGE_PREFIX}/{analysis_variable_coverage_path().name}`",
        "- Coverage CSVs are generated by the analysis step and published as quality evidence.",
        "",
        "## Extracted Variables",
        "",
        "|canonical_variable|variable_role|analysis_requirement|source_rule|provenance_column|notes|",
        "|---|---|---|---|---|---|",
    ]
    for row in AVAILABLE_VARIABLE_METADATA:
        lines.append(f"|{row['canonical_variable']}|{row['variable_role']}|{row['analysis_requirement']}|{row['source_rule']}|{row['provenance_column']}|{row['notes']}|")
    lines.extend(["", "## Unavailable Variables", "", "|canonical_variable|availability_status|analysis_requirement|notes|", "|---|---|---|---|"])
    for row in UNAVAILABLE_VARIABLES:
        lines.append(f"|{row['canonical_variable']}|unavailable|{row['analysis_requirement']}|{row['notes']}|")
    mapping_path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def _write_data_processing_doc(doc_path: Path) -> None:
    doc = f"""# NCCS Core Pipeline

## Status

This document is the authoritative technical reference for the current NCCS Core pipeline implemented in this repository.

It reflects the code as it exists now under `python/ingest/nccs_990_core/`, including the current `2022`-only scope, the benchmark-filtered Core outputs written in step 05, the combined filtered Core parquet, the row-level analysis build in step 07, and the official analysis upload in step 08.

This document is intentionally detailed. It is meant to let another engineer or analyst understand:

- where the Core data comes from
- how it is stored locally and in S3
- the exact script order and responsibilities
- how raw Core files become benchmark-filtered Core outputs
- how the final Core analysis-ready rowset is built from filtered Core inputs only
- what is direct, calculated, enriched, imputed, caveated, or unavailable in the final Core analysis package

## Pipeline At A Glance

The NCCS Core pipeline is now a co-equal source-specific 990 layer in the repo. It does not replace GT or efile, but it provides an additional benchmark-filtered analysis package built from the NCCS Core release.

At a high level, the pipeline has these phases:

1. Discover the latest common Core release and supporting bridge inputs.
2. Download the raw Core files, dictionaries, and bridge BMF files.
3. Upload the raw release to Bronze S3.
4. Strictly verify source/local/S3 byte parity for the raw release.
5. Filter the Core files to benchmark geography using the BMF bridge and write a combined filtered Core parquet.
6. Upload the benchmark-filtered Core outputs to Silver S3.
7. Build the final Core analysis outputs from the filtered Core inputs only.
8. Upload the final Core analysis outputs and metadata to the analysis S3 prefix.

The main artifact classes are:

- Raw data: Core source files exactly as downloaded.
- Metadata/manifests: release-selection metadata, dictionaries, bridge files, and verification reports.
- Silver filtered artifacts: benchmark-filtered Core outputs plus a combined filtered Core parquet.
- Final analysis artifacts: the row-level analysis parquet, geography metrics parquet, coverage CSV, mapping Markdown, and this processing doc.

This pipeline follows a filtered-only combine contract (combine stages use filtered inputs only):

- benchmark admission happens upstream in step 05
- the final analysis build reads only filtered Core outputs
- raw Core files are not directly combined in the analysis step

## Core Contract

- Year scope is `2022` only.
- The final analysis build reads only filtered Core benchmark files in staging.
- `core_scope` is the authoritative filing-context field.
- `harm_filing_form` is sparse by design and is currently populated only for reliable `PF -> 990PF` rows.
- Overlapping `PC` and `PZ` rows are intentionally preserved in the final analysis file.

## Script Order And Responsibilities

| Step | Script | Primary role | Main inputs | Main outputs | External systems |
| ---- | ------ | ------------ | ----------- | ------------ | ---------------- |
| 01 | `01_discover_core_release.py` | Discover the latest common Core release plus required bridge files | Remote release pages and bridge metadata | Release metadata and selection state | HTTP |
| 02 | `02_download_core_release.py` | Download Core raw files, dictionaries, and bridge BMF files | Release metadata and remote URLs | Raw Core files, dictionaries, and bridge inputs | HTTP |
| 03 | `03_upload_core_release_to_s3.py` | Upload the raw Core release to Bronze | Local raw release and metadata | Bronze raw Core objects | S3 |
| 04 | `04_verify_core_source_local_s3.py` | Verify source/local/S3 byte parity for the raw release | Remote sizes, local files, Bronze objects | Raw verification CSV | HTTP, S3 |
| 05 | `05_filter_core_to_benchmark_local.py` | Filter Core files to benchmark geography using the BMF bridge and write a combined filtered Core parquet | Raw Core files plus bridge BMF inputs | Filtered Core outputs and combined filtered Core parquet | local files |
| 06 | `06_upload_filtered_core_to_s3.py` | Upload benchmark-filtered Core outputs to Silver | Filtered Core outputs and metadata | Silver filtered Core objects | S3 |
| 07 | `07_extract_analysis_variables_local.py` | Build row-level analysis variables, geography metrics, coverage CSV, mapping doc, and this processing doc | Filtered Core outputs, combined filtered Core parquet, BMF lookups, IRS EO BMF lookups | Official Core analysis outputs | local files |
| 08 | `08_upload_analysis_outputs.py` | Upload analysis outputs and verify local-vs-S3 byte parity | Analysis artifacts and docs | Silver analysis objects | S3 |

## How Data Is Retrieved

## Exact Source Locations

- NCCS Core catalog page: `{CORE_CATALOG_URL}`
- NCCS BMF bridge catalog page: `{BMF_CATALOG_URL}`

## Raw Source Dictionaries

- NCCS Core public charity dictionary: `documentation/final_preprocessing_docs/technical_docs/source_dictionaries/nccs_990_core/CORE-HRMN_dd.csv`
- NCCS Core private foundation dictionary: `documentation/final_preprocessing_docs/technical_docs/source_dictionaries/nccs_990_core/DD-PF-HRMN-V0.csv`
- NCCS harmonized BMF bridge dictionary: `documentation/final_preprocessing_docs/technical_docs/source_dictionaries/nccs_990_core/harmonized_data_dictionary.xlsx`

### Step 01 and Step 02: release discovery and raw download

The Core pipeline starts by selecting the latest common Core release that is compatible with the benchmark-analysis workflow and then downloading:

- the raw Core data files
- supporting dictionaries
- bridge BMF files needed for benchmark geography filtering

This lets the repo preserve both the Core source data and the bridge inputs required to connect the Core release to the benchmark counties.

### Step 03 and Step 04: Bronze upload and raw verification

After download, the pipeline uploads the raw Core release to Bronze and verifies source/local/S3 byte parity.

This preserves:

- local raw Core copies
- Bronze raw Core copies
- a raw verification CSV documenting whether source, local, and S3 bytes agree

## Where Data Is Stored

### Local artifacts

- Combined filtered Core parquet: `{_repo_relative_display(combined_filtered_output_path(STAGING_DIR, CORE_ANALYSIS_TAX_YEAR))}`
- Row-level analysis parquet: `{_repo_relative_display(analysis_variables_output_path())}`
- Geography metrics parquet: `{_repo_relative_display(analysis_geography_metrics_output_path())}`
- Coverage CSV: `{_repo_relative_display(analysis_variable_coverage_path())}`
- Mapping doc: `{_repo_relative_display(analysis_variable_mapping_path())}`
- Data-processing doc: `{_repo_relative_display(analysis_data_processing_doc_path())}`

### S3 layout

- Silver filtered Core prefix: `silver/nccs_990/core/`
- Silver filtered Core metadata prefix: `silver/nccs_990/core/metadata/`
- Official Core analysis prefix: `silver/nccs_990/core/analysis/`
- Analysis documentation prefix: `{ANALYSIS_DOCUMENTATION_PREFIX}`
- Analysis variable mapping prefix: `{ANALYSIS_VARIABLE_MAPPING_PREFIX}`
- Analysis coverage prefix: `{ANALYSIS_COVERAGE_PREFIX}`

## Step-By-Step Transformation

### Step 05: benchmark filtering and combined filtered Core parquet

Step 05 is the key upstream curation step.

It:

1. reads the raw Core source files
2. uses the bridge BMF inputs to determine benchmark geography admission
3. writes benchmark-filtered Core outputs by source file
4. writes one combined filtered Core parquet after the per-file filter completes

That combined filtered Core parquet is the first place where the filtered Core files are unioned, and that union happens only after benchmark filtering.

### Step 07: analysis build from filtered Core inputs only

The analysis step prefers the combined filtered Core parquet when present and otherwise reconstructs the same filtered union from the per-file benchmark outputs.

The final analysis rowset is source-faithful with respect to the filtered Core union. It intentionally preserves overlapping `PC` and `PZ` rows instead of collapsing them to one row per EIN-year.

### Source-context columns

The final Core analysis dataset adds explicit source-context columns:

- `core_analysis_row_id`
- `core_source_file`
- `core_source_variant`
- `core_family`
- `core_scope`

These fields are part of the contract because Core is not being forced into a single simplified filing-form model.

### Identity and geography fallback

The final analysis dataset keeps:

- `ein`
- `tax_year`
- `org_name`
- `state`
- `zip5`
- `county_fips`
- `region`

When Core identity fields are sparse, the pipeline uses:

1. exact-year BMF identity
2. nearest-year BMF identity
3. IRS EO BMF identity

to backfill missing identity fields while preserving provenance and keeping `core_scope` as the authoritative Core context field.

### Direct and candidate financial extraction

The Core analysis layer extracts direct or source-faithful candidate fields for:

- total revenue
- total expense
- total assets
- net asset
- surplus
- program service revenue candidate
- contributions candidate
- gifts/grants received candidate
- grants paid candidate

Where the same concept differs across `pc`, `pz`, and `pf`, the step uses scope-aware rules rather than pretending the columns are identical across Core families.

### Requirement-aligned promoted fields

The current Core analysis package also promotes these cleanly supportable requirement-aligned fields:

- `analysis_program_service_revenue_amount`
- `analysis_calculated_total_contributions_amount`

These stay tied to their source-faithful candidate fields rather than being silently redefined with GT-style semantics.

### Calculated metrics

The main calculated metrics are:

- `analysis_calculated_surplus_amount`
- `analysis_calculated_net_margin_ratio`
- `analysis_calculated_cleaned_net_margin_ratio`
- `analysis_calculated_months_of_reserves`
- `analysis_calculated_ntee_broad_code`

The cleaned net-margin field exists so analysts have a more stable convenience metric while the raw ratio remains available.

### Classification enrichment

The analysis step enriches classification with the same fallback order used elsewhere in the repo when appropriate:

1. exact-year NCCS BMF
2. nearest-year NCCS BMF
3. IRS EO BMF

If all sources still fail, unresolved NTEE is explicitly marked `UNKNOWN` rather than remaining silently blank.

### Source-backed and imputed exclusion flags

The final Core analysis package includes:

- `analysis_is_hospital`
- `analysis_is_university`
- `analysis_is_political_org`
- `analysis_imputed_is_hospital`
- `analysis_imputed_is_university`
- `analysis_imputed_is_political_org`

The non-imputed flags come from Core direct fields when available and otherwise from classification-based proxy logic. The imputed fields then continue through the same conservative name-based fallback approach used elsewhere in the repo.

### Geography metrics

The geography metrics parquet aggregates the Core analysis rowset by:

- region
- tax year
- exclusion variant

Two exclusion variants are written:

- `all_rows`
- `exclude_imputed_hospital_university_political_org`

The metrics include counts, unique EIN counts, financial sums, normalized averages, and exclusion-flag counts.

### Coverage, mapping, and upload verification

The coverage CSV records populated counts and fill rates for the available Core analysis variables.

## Coverage Evidence

- Generated coverage CSV: `{_repo_relative_display(analysis_variable_coverage_path())}`
- Published S3 documentation object: `{ANALYSIS_DOCUMENTATION_PREFIX}/{analysis_data_processing_doc_path().name}`
- Published S3 variable mapping object: `{ANALYSIS_VARIABLE_MAPPING_PREFIX}/{analysis_variable_mapping_path().name}`
- Published S3 coverage object: `{ANALYSIS_COVERAGE_PREFIX}/{analysis_variable_coverage_path().name}`
- Coverage CSVs are generated by step 07 and published as authoritative quality evidence.

The mapping Markdown documents which variables are:

- direct
- calculated
- enriched
- imputed
- unavailable
- caveated or candidate-only

The upload step performs explicit local-vs-S3 byte checks for each analysis artifact before it accepts the upload as complete.

## Final Analysis Outputs

The official Core analysis package consists of:

- one combined filtered Core parquet
- one row-level analysis parquet
- one geography metrics parquet
- one coverage CSV
- one mapping Markdown
- this data-processing doc

This package is analogous in structure to GT and efile, while keeping Core-specific semantics visible rather than hiding them behind aggressive harmonization.

## Testing And Verification

- Unit tests live in `tests/nccs_990_core/test_nccs_990_core_common.py`.
- The analysis tests cover filtered-union loading, preserved `PC/PZ` overlaps, direct and candidate financial extraction, classification fallback behavior, cleaned net margin, and geography metrics.
- Operational verification includes raw source/local/S3 byte checks, filtered-output uploads, and analysis upload byte checks.

## Current Caveats

- Core is currently `2022` only.
- `core_scope` is the authoritative Core filing-context field; `harm_filing_form` remains sparse by design.
- `PC` and `PZ` overlap rows are intentionally preserved in the final analysis package.
- Core candidate revenue-source fields are kept source-faithful instead of being forced into GT semantics.
- Zero-expense reserve rows are made explicit rather than remaining silently null, and unresolved NTEE rows are marked `UNKNOWN`.

## Analysis requirement alignment appendix

| analysis_requirement | source_specific_output | status | rule_or_reason |
| --- | --- | --- | --- |
| Total revenue | analysis_total_revenue_amount | direct | Scope-aware direct Core revenue extraction |
| Total expense | analysis_total_expense_amount | direct | Scope-aware direct Core expense extraction |
| Total assets | analysis_total_assets_amount | direct | Scope-aware direct Core asset extraction |
| Net asset | analysis_net_asset_amount | direct_or_calculated | Direct where available; PF fallback can derive assets minus liabilities when needed |
| Surplus | analysis_calculated_surplus_amount | calculated | Scope-aware surplus rule from Core source fields |
| Net margin | analysis_calculated_net_margin_ratio | calculated | Surplus divided by positive total revenue |
| Program service revenue | analysis_program_service_revenue_amount | direct | Promoted from the Core program-service candidate field |
| Total contributions | analysis_calculated_total_contributions_amount | direct | Promoted from the Core contribution candidate field |
| NTEE filed classification code | analysis_ntee_code | enriched | Exact-year BMF, then nearest-year BMF, then IRS EO BMF fallback |
| Broad NTEE field classification code | analysis_calculated_ntee_broad_code | calculated | First letter of final resolved NTEE code |
| Political organization flag | analysis_is_political_org | proxy | Resolved from subsection classification |
"""
    doc_path.write_text(doc.strip() + "\n", encoding="utf-8")


def build_analysis_outputs(
    *,
    staging_dir: Path = STAGING_DIR,
    metadata_dir: Path = META_DIR,
    bmf_staging_dir: Path = DEFAULT_BMF_STAGING_DIR,
    irs_bmf_raw_dir: Path = DEFAULT_IRS_BMF_RAW_DIR,
    tax_year: int = CORE_ANALYSIS_TAX_YEAR,
) -> dict[str, Path]:
    analysis_path = analysis_variables_output_path(staging_dir)
    geography_path = analysis_geography_metrics_output_path(staging_dir)
    coverage_path = analysis_variable_coverage_path(metadata_dir)
    mapping_path = analysis_variable_mapping_path()
    processing_doc_path = analysis_data_processing_doc_path()
    _ensure_parent_dirs(analysis_path, geography_path, coverage_path, mapping_path, processing_doc_path)

    source_df = _load_filtered_core_source(staging_dir, tax_year)
    print(f"[analysis] Combined filtered Core rows: {len(source_df):,}", flush=True)

    source_df["harm_ein"] = source_df.get("F9_00_ORG_EIN", source_df.get("EIN2", pd.Series(pd.NA, index=source_df.index))).astype("string")
    source_df["harm_ein"] = source_df["harm_ein"].fillna(source_df.get("EIN2", pd.Series(pd.NA, index=source_df.index)).astype("string")).map(normalize_ein).astype("string")
    source_df["harm_county_fips"] = blank_to_na(source_df["county_fips"].astype("string"))
    source_df["harm_region"] = blank_to_na(source_df["region"].astype("string"))

    analysis_df = pd.DataFrame(index=source_df.index)
    analysis_df["core_analysis_row_id"] = source_df["core_analysis_row_id"].astype("string")
    analysis_df["core_source_file"] = source_df["core_source_file"].astype("string")
    analysis_df["core_source_variant"] = source_df["core_source_variant"].astype("string")
    analysis_df["core_family"] = source_df["core_family"].astype("string")
    analysis_df["core_scope"] = source_df["core_scope"].astype("string")
    analysis_df["harm_ein"] = source_df["harm_ein"].astype("string")
    analysis_df["harm_tax_year"] = source_df["harm_tax_year"].astype("string")
    analysis_df["harm_county_fips"] = source_df["harm_county_fips"].astype("string")
    analysis_df["harm_region"] = source_df["harm_region"].astype("string")

    bmf_identity_df = _load_bmf_identity_lookup(bmf_staging_dir, tax_year)
    irs_identity_df = _load_irs_identity_lookup(irs_bmf_raw_dir)
    _apply_identity_fallbacks(
        analysis_df,
        source_df=source_df,
        bmf_identity_lookup_df=bmf_identity_df,
        irs_identity_lookup_df=irs_identity_df,
    )
    analysis_df["harm_filing_form"] = pd.Series(pd.NA, index=analysis_df.index, dtype="string")
    analysis_df["harm_filing_form_source_column"] = pd.Series(pd.NA, index=analysis_df.index, dtype="string")
    pf_mask = analysis_df["core_scope"].eq("pf")
    analysis_df.loc[pf_mask, "harm_filing_form"] = "990PF"
    analysis_df.loc[pf_mask, "harm_filing_form_source_column"] = "core_scope"

    analysis_df["ein"] = analysis_df["harm_ein"]
    analysis_df["tax_year"] = analysis_df["harm_tax_year"]
    analysis_df["form_type"] = analysis_df["harm_filing_form"]
    analysis_df["org_name"] = analysis_df["harm_org_name"]
    analysis_df["state"] = analysis_df["harm_state"]
    analysis_df["zip5"] = analysis_df["harm_zip5"]
    analysis_df["county_fips"] = analysis_df["harm_county_fips"]
    analysis_df["region"] = analysis_df["harm_region"]

    analysis_df["core_overlap_ein_tax_year_group_size"] = (
        analysis_df.groupby(["ein", "tax_year"], dropna=False)["ein"].transform("size").astype("Int64")
    )

    analysis_df["analysis_total_revenue_amount"], analysis_df["analysis_total_revenue_amount_source_column"] = _build_numeric_by_rule(
        source_df, [("pz", ["F9_01_REV_TOT_CY", "F9_08_REV_TOT_TOT"]), ("pc", ["F9_08_REV_TOT_TOT"]), ("pf", ["PF_01_REV_TOT_BOOKS"])], output_column="analysis_total_revenue_amount"
    )
    analysis_df["analysis_total_expense_amount"], analysis_df["analysis_total_expense_amount_source_column"] = _build_numeric_by_rule(
        source_df, [("pz", ["F9_01_EXP_TOT_CY", "F9_09_EXP_TOT_TOT"]), ("pc", ["F9_09_EXP_TOT_TOT"]), ("pf", ["PF_01_EXP_TOT_EXP_DISBMT_BOOKS"])], output_column="analysis_total_expense_amount"
    )
    analysis_df["analysis_total_assets_amount"], analysis_df["analysis_total_assets_amount_source_column"] = _build_numeric_by_rule(
        source_df, [("pz", ["F9_10_ASSET_TOT_EOY"]), ("pc", ["F9_10_ASSET_TOT_EOY"]), ("pf", ["PF_02_ASSET_TOT_EOY_BV"])], output_column="analysis_total_assets_amount"
    )
    analysis_df["analysis_net_asset_amount"], analysis_df["analysis_net_asset_amount_source_column"] = _build_net_asset_amount(analysis_df, source_df)
    analysis_df["analysis_program_service_revenue_candidate_amount"], analysis_df["analysis_program_service_revenue_candidate_amount_source_column"] = _build_numeric_by_rule(
        source_df, [("pz", ["F9_08_REV_PROG_TOT_TOT", "F9_08_REV_PROG_TOT"]), ("pc", ["F9_08_REV_PROG_TOT_TOT", "F9_08_REV_PROG_TOT"]), ("pf", ["PF_16_REV_PROG_FEES_RLTD"])], output_column="analysis_program_service_revenue_candidate_amount"
    )
    analysis_df["analysis_program_service_revenue_amount"] = analysis_df["analysis_program_service_revenue_candidate_amount"].copy()
    analysis_df["analysis_program_service_revenue_amount_source_column"] = analysis_df["analysis_program_service_revenue_candidate_amount_source_column"].copy()
    analysis_df["analysis_contribution_candidate_amount"], analysis_df["analysis_contribution_candidate_amount_source_column"] = _build_numeric_by_rule(
        source_df, [("pz", ["F9_08_REV_CONTR_TOT", "F9_01_REV_CONTR_TOT_CY_2"]), ("pc", ["F9_08_REV_CONTR_TOT"]), ("pf", ["PF_01_REV_CONTR_REC_BOOKS"])], output_column="analysis_contribution_candidate_amount"
    )
    analysis_df["analysis_calculated_total_contributions_amount"] = analysis_df["analysis_contribution_candidate_amount"].copy()
    analysis_df["analysis_calculated_total_contributions_amount_source_column"] = analysis_df["analysis_contribution_candidate_amount_source_column"].copy()
    analysis_df["analysis_gifts_grants_received_candidate_amount"], analysis_df["analysis_gifts_grants_received_candidate_amount_source_column"] = _build_numeric_by_rule(
        source_df, [("pz", ["SA_02_PUB_GIFT_GRANT_CONTR_TOT", "SA_03_PUB_GIFT_GRANT_CONTR_TOT"]), ("pc", ["SA_02_PUB_GIFT_GRANT_CONTR_TOT", "SA_03_PUB_GIFT_GRANT_CONTR_TOT"])], output_column="analysis_gifts_grants_received_candidate_amount"
    )
    pc_pz_grants_paid, pc_pz_grants_paid_source = _build_sum_candidate_by_rule(
        source_df, scope="pc", columns=["F9_09_EXP_GRANT_US_ORG_TOT", "F9_09_EXP_GRANT_US_INDIV_TOT", "F9_09_EXP_GRANT_FRGN_TOT"], output_column="analysis_grants_paid_candidate_amount"
    )
    pz_grants_paid, pz_grants_paid_source = _build_sum_candidate_by_rule(
        source_df, scope="pz", columns=["F9_09_EXP_GRANT_US_ORG_TOT", "F9_09_EXP_GRANT_US_INDIV_TOT", "F9_09_EXP_GRANT_FRGN_TOT"], output_column="analysis_grants_paid_candidate_amount"
    )
    pf_grants_paid, pf_grants_paid_source = _build_numeric_by_rule(
        source_df, [("pf", ["PF_01_EXP_CONTR_PAID_BOOKS"])], output_column="analysis_grants_paid_candidate_amount"
    )
    analysis_df["analysis_grants_paid_candidate_amount"] = pc_pz_grants_paid.combine_first(pz_grants_paid).combine_first(pf_grants_paid)
    analysis_df["analysis_grants_paid_candidate_amount_source_column"] = pc_pz_grants_paid_source.combine_first(pz_grants_paid_source).combine_first(pf_grants_paid_source)
    negative_grants_paid_mask = analysis_df["analysis_grants_paid_candidate_amount"].lt(0)
    if negative_grants_paid_mask.any():
        analysis_df.loc[negative_grants_paid_mask, "analysis_grants_paid_candidate_amount"] = pd.NA
        analysis_df.loc[negative_grants_paid_mask, "analysis_grants_paid_candidate_amount_source_column"] = pd.NA
        print(f"[analysis] Cleared negative grants-paid rows: {int(negative_grants_paid_mask.sum()):,}", flush=True)

    analysis_df["analysis_calculated_surplus_amount"], analysis_df["analysis_calculated_surplus_amount_source_column"] = _build_surplus_amount(analysis_df, source_df)
    analysis_df["analysis_calculated_net_margin_ratio"], analysis_df["analysis_calculated_net_margin_ratio_source_column"] = build_ratio_metric(
        analysis_df["analysis_calculated_surplus_amount"], analysis_df["analysis_total_revenue_amount"], provenance_label="analysis_calculated_surplus_amount / analysis_total_revenue_amount", require_positive_denominator=True
    )
    analysis_df["analysis_calculated_cleaned_net_margin_ratio"], analysis_df["analysis_calculated_cleaned_net_margin_ratio_source_column"] = _build_cleaned_net_margin_ratio(analysis_df)
    analysis_df["analysis_calculated_months_of_reserves"], analysis_df["analysis_calculated_months_of_reserves_source_column"] = _build_months_of_reserves(analysis_df)

    bmf_lookup_df = load_bmf_classification_lookup([str(tax_year)], bmf_staging_dir=bmf_staging_dir)
    irs_lookup_df = load_irs_bmf_classification_lookup(irs_bmf_raw_dir=irs_bmf_raw_dir)
    apply_classification_fallbacks(
        analysis_df,
        pd.DataFrame({"ein": analysis_df["ein"], "tax_year": analysis_df["tax_year"], "source_state": analysis_df["state"]}),
        state_column="source_state",
        bmf_lookup_df=bmf_lookup_df,
        irs_lookup_df=irs_lookup_df,
    )
    _fill_unknown_ntee_code(analysis_df)
    analysis_df["analysis_calculated_ntee_broad_code"], analysis_df["analysis_calculated_ntee_broad_code_source_column"] = _build_broad_ntee(analysis_df["analysis_ntee_code"])

    direct_hospital = normalize_bool_text(source_df.get("F9_04_HOSPITAL_X", pd.Series(pd.NA, index=source_df.index, dtype="string")))
    direct_hospital_source = pd.Series("F9_04_HOSPITAL_X", index=source_df.index, dtype="string").mask(direct_hospital.isna(), pd.NA)
    proxy_hospital, proxy_hospital_source = build_ntee_proxy_flag(analysis_df["analysis_ntee_code"], ("E20", "E21", "E22", "E24"), provenance_label="analysis_ntee_code")
    analysis_df["analysis_is_hospital"], analysis_df["analysis_is_hospital_source_column"] = _combine_direct_and_proxy_flags(direct_hospital, direct_hospital_source, proxy_hospital, proxy_hospital_source)
    direct_university = normalize_bool_text(source_df.get("F9_04_SCHOOL_X", pd.Series(pd.NA, index=source_df.index, dtype="string")))
    direct_university_source = pd.Series("F9_04_SCHOOL_X", index=source_df.index, dtype="string").mask(direct_university.isna(), pd.NA)
    proxy_university, proxy_university_source = build_ntee_proxy_flag(analysis_df["analysis_ntee_code"], ("B40", "B41", "B42", "B43"), provenance_label="analysis_ntee_code")
    analysis_df["analysis_is_university"], analysis_df["analysis_is_university_source_column"] = _combine_direct_and_proxy_flags(direct_university, direct_university_source, proxy_university, proxy_university_source)
    analysis_df["analysis_is_political_org"], analysis_df["analysis_is_political_org_source_column"] = build_political_proxy_flag(analysis_df["analysis_subsection_code"], provenance_label="analysis_subsection_code")
    apply_imputed_flag_family(analysis_df, name_column="harm_org_name")

    analysis_df.to_parquet(analysis_path, index=False)
    _build_region_metrics_output(analysis_df).to_parquet(geography_path, index=False)
    _build_coverage_rows(analysis_df).to_csv(coverage_path, index=False)
    _write_mapping_markdown(mapping_path)
    _write_data_processing_doc(processing_doc_path)
    print(f"[analysis] Wrote row-level analysis output: {analysis_path}", flush=True)
    print(f"[analysis] Wrote geography metrics output: {geography_path}", flush=True)
    print(f"[analysis] Wrote coverage report: {coverage_path}", flush=True)
    print(f"[analysis] Wrote mapping doc: {mapping_path}", flush=True)
    print(f"[analysis] Wrote processing doc: {processing_doc_path}", flush=True)
    return {
        "analysis_variables": analysis_path,
        "analysis_geography_metrics": geography_path,
        "analysis_variable_coverage": coverage_path,
        "analysis_variable_mapping": mapping_path,
        "analysis_data_processing_doc": processing_doc_path,
    }


def main() -> None:
    parser = argparse.ArgumentParser(description="Build NCCS Core analysis outputs from filtered benchmark files.")
    parser.add_argument("--bucket", default=DEFAULT_S3_BUCKET)
    parser.add_argument("--region", default=DEFAULT_S3_REGION)
    parser.add_argument("--metadata-dir", type=Path, default=META_DIR)
    parser.add_argument("--staging-dir", type=Path, default=STAGING_DIR)
    parser.add_argument("--bmf-staging-dir", type=Path, default=DEFAULT_BMF_STAGING_DIR)
    parser.add_argument("--irs-bmf-raw-dir", type=Path, default=DEFAULT_IRS_BMF_RAW_DIR)
    parser.add_argument("--analysis-prefix", default=ANALYSIS_PREFIX)
    parser.add_argument("--analysis-documentation-prefix", default=ANALYSIS_DOCUMENTATION_PREFIX)
    parser.add_argument("--analysis-variable-mapping-prefix", default=ANALYSIS_VARIABLE_MAPPING_PREFIX)
    parser.add_argument("--analysis-coverage-prefix", default=ANALYSIS_COVERAGE_PREFIX)
    parser.add_argument("--tax-year", type=int, default=CORE_ANALYSIS_TAX_YEAR)
    args = parser.parse_args()

    start = time.perf_counter()
    banner("STEP 07 - BUILD NCCS CORE ANALYSIS OUTPUTS")
    load_env_from_secrets()
    print(f"[analysis] Bucket: {args.bucket}", flush=True)
    print(f"[analysis] Region: {args.region}", flush=True)
    print(f"[analysis] Tax year: {args.tax_year}", flush=True)
    print(f"[analysis] Analysis prefix: {args.analysis_prefix}", flush=True)
    print(f"[analysis] Analysis documentation prefix: {args.analysis_documentation_prefix}", flush=True)
    print(f"[analysis] Analysis variable mapping prefix: {args.analysis_variable_mapping_prefix}", flush=True)
    print(f"[analysis] Analysis coverage prefix: {args.analysis_coverage_prefix}", flush=True)
    build_analysis_outputs(
        staging_dir=args.staging_dir,
        metadata_dir=args.metadata_dir,
        bmf_staging_dir=args.bmf_staging_dir,
        irs_bmf_raw_dir=args.irs_bmf_raw_dir,
        tax_year=args.tax_year,
    )
    print_elapsed(start, "Step 07")


if __name__ == "__main__":
    main()
