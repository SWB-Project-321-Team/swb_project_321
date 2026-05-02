"""
Step 13: Extract GT basic-only analysis variables for the stated analysis scope.

This step intentionally reads the GT basic-only benchmark-filtered artifact and
produces a GT-only analysis-variable layer. It does not try to backfill missing
concepts from NCCS, BMF, or e-file sources, except for the explicit BMF NTEE
enrichment approved for this final GT analysis layer. That narrow exception
keeps the analysis extract source-faithful and auditable while still covering
the classification variable GT does not carry directly.

The NTEE enrichment now follows an explicit fallback chain:
1. NCCS BMF exact-year EIN + tax_year match
2. NCCS BMF nearest-year EIN fallback when the exact-year value is missing
3. IRS EO BMF EIN fallback when NCCS still does not supply a usable NTEE code

The user later asked to replace the older hospital, university, and political-
organization booleans with NTEE-derived proxy columns. The public boolean
columns therefore now follow documented NTEE prefix rules rather than the
older mixed-source flag logic.
"""

from __future__ import annotations

import argparse
import importlib.util
import re
import sys
import time
from pathlib import Path

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
    ANALYSIS_VARIABLE_MAPPING_PREFIX,
    DOCS_ANALYSIS_DIR,
    FILTERED_BASIC_ALLFORMS_PARQUET,
    GT_BASIC_ANALYSIS_REGION_METRICS_PARQUET,
    GT_BASIC_ANALYSIS_VARIABLE_COVERAGE_CSV,
    GT_BASIC_ANALYSIS_VARIABLE_MAPPING_MD,
    GT_BASIC_ANALYSIS_VARIABLES_PARQUET,
    banner,
    discover_bmf_exact_year_lookup_inputs,
    discover_irs_bmf_raw_inputs,
    ensure_dirs,
    load_env_from_secrets,
    normalize_ein,
    print_elapsed,
)
from utils.paths import DATA as DATA_ROOT


_REPO_ROOT = Path(__file__).resolve().parents[4]


def _portable_path(path: Path) -> str:
    resolved = path.resolve()
    try:
        return f"SWB_321_DATA_ROOT/{resolved.relative_to(DATA_ROOT.resolve()).as_posix()}"
    except ValueError:
        pass
    try:
        return resolved.relative_to(_REPO_ROOT).as_posix()
    except ValueError:
        return resolved.as_posix()


TRUE_FLAG_VALUES = {"1", "Y", "YES", "TRUE", "T"}
FALSE_FLAG_VALUES = {"0", "N", "NO", "FALSE", "F"}
POLITICAL_ORG_SUBSECTION_CODES = {"4", "527", "501C4", "501C527"}

FORM_AWARE_AMOUNT_SPECS = [
    {
        "output_column": "analysis_total_revenue_amount",
        "source_by_form": {
            "990": "TOTREVCURYEA",
            "990PF": "ANREEXTOREEX",
            "990EZ": "TOTALRREVENU",
        },
        "analysis_basis": "Total revenue for limited-number organization comparisons and financial-performance analysis.",
        "notes": "Keeps GT form-aware revenue precedence explicit. Form 990PF uses the PF-specific total revenue field from the GT standard-fields extract.",
    },
    {
        "output_column": "analysis_total_expense_amount",
        "source_by_form": {
            "990": "TOTEXPCURYEA",
            "990PF": "ARETEREXPNSS",
            "990EZ": "TOTALEEXPENS",
        },
        "analysis_basis": "Total expense for financial-performance analysis.",
        "notes": "Keeps GT form-aware expense precedence explicit. Form 990PF uses the PF-specific total expense field from the GT standard-fields extract.",
    },
    {
        "output_column": "analysis_program_service_revenue_amount",
        "source_by_form": {
            "990": "TOTPROSERREV",
            "990EZ": "PROGSERVREVE",
        },
        "analysis_basis": "Program service revenue for the revenue-source comparison question.",
        "notes": "GT plan says program service revenue comes from 990 and EZ. 990PF stays null here.",
    },
]

DIRECT_AMOUNT_SPECS = [
    {
        "output_column": "analysis_cash_contributions_amount",
        "source_column": "TOTACASHCONT",
        "analysis_basis": "Contribution component exposed because total contributions remain unresolved at the requirement level.",
        "notes": "Component only; does not synthesize a final total-contributions measure.",
    },
    {
        "output_column": "analysis_noncash_contributions_amount",
        "source_column": "NONCASCONTRI",
        "analysis_basis": "Contribution component exposed because total contributions remain unresolved at the requirement level.",
        "notes": "Component only; does not synthesize a final total-contributions measure.",
    },
    {
        "output_column": "analysis_other_contributions_amount",
        "source_column": "ALLOOTHECONT",
        "analysis_basis": "Contribution component exposed for other contribution lines such as foundation grants.",
        "notes": "Component only; do not treat as a final all-contributions total.",
    },
    {
        "output_column": "analysis_foundation_grants_amount",
        "source_column": "FOREGRANTOTA",
        "analysis_basis": "Grant component exposed for grants-versus-other-revenue comparisons.",
        "notes": "Component only; do not treat as a final all-grants total.",
    },
    {
        "output_column": "analysis_government_grants_amount",
        "source_column": "GOVERNGRANTS",
        "analysis_basis": "Grant component exposed for grants-versus-other-revenue comparisons.",
        "notes": "Component only; do not treat as a final all-grants total.",
    },
    {
        "output_column": "analysis_other_grant_component_amount",
        "source_column": "GRANTOORORGA",
        "analysis_basis": "Grant component exposed for grants-versus-other-revenue comparisons.",
        "notes": "Component only; do not treat as a final all-grants total.",
    },
]

DERIVED_COMPONENT_SUM_SPECS = [
    {
        "output_column": "analysis_calculated_total_contributions_amount",
        "source_columns": ["TOTACASHCONT", "NONCASCONTRI", "ALLOOTHECONT"],
        "analysis_basis": "Total contributions for the limited-number organization revenue-source comparison.",
        "notes": (
            "Derived as the row-wise sum of cash, noncash, and other contribution components. "
            "Blank components are treated as missing rather than as errors, and the total stays null when every component is blank."
        ),
    },
    {
        "output_column": "analysis_calculated_grants_total_amount",
        "source_columns": ["FOREGRANTOTA", "GOVERNGRANTS", "GRANTOORORGA"],
        "analysis_basis": "Total grants amount for the grants-versus-other-revenue comparison.",
        "notes": (
            "Derived as the row-wise sum of foundation, government, and other grant components. "
            "Blank components are treated as missing rather than as errors, and the total stays null when every component is blank."
        ),
    },
]

NTEE_PROXY_FLAG_SPECS = [
    {
        "output_column": "analysis_is_hospital",
        "positive_prefixes": ("E20", "E21", "E22", "E24"),
        "analysis_basis": "Supports with/without hospitals comparison.",
        "notes": (
            "NTEE proxy. Blank NTEE stays missing; codes beginning with E20, E21, E22, or E24 become True; "
            "other populated NTEE codes become False."
        ),
    },
    {
        "output_column": "analysis_is_university",
        "positive_prefixes": ("B40", "B41", "B42", "B43", "B50"),
        "analysis_basis": "Supports with/without universities comparison.",
        "notes": (
            "NTEE proxy. Blank NTEE stays missing; higher-education codes beginning with B40, B41, B42, B43, "
            "or B50 become True; other populated NTEE codes become False."
        ),
    },
    {
        "output_column": "analysis_is_political_org",
        "positive_prefixes": ("R40", "W24"),
        "analysis_basis": "Supports with/without political organizations comparison.",
        "notes": (
            "NTEE proxy. Blank NTEE stays missing; explicitly politics-oriented codes beginning with R40 or W24 "
            "become True; other populated NTEE codes become False."
        ),
    },
]

NAME_PROXY_FLAG_SPECS = [
    {
        "canonical_output_column": "analysis_is_hospital",
        "imputed_output_column": "analysis_imputed_is_hospital",
        "high_confidence_patterns": (
            r"\bhospital\b",
            r"\bmedical center\b",
            r"\bhealth system\b",
            r"\bgeneral hospital\b",
            r"\bspecialty hospital\b",
        ),
        "medium_confidence_patterns": (
            r"\bambulance district\b",
            r"\bambulance service\b",
            r"\bcommunity health center\b",
        ),
        "block_patterns": (
            r"\bmedical records\b",
            r"\bhealthcare\b",
            r"\bhealth care\b",
            r"\bhealth professi\b",
            r"\bclinic foundation\b",
        ),
        "analysis_basis": "Complete hospital flag for analysis workflows that require no missing values.",
        "notes": (
            "Imputed flag that prefers analysis_is_hospital, then uses only high-confidence FILERNAME1 matches, "
            "then defaults remaining unknown rows to False."
        ),
    },
    {
        "canonical_output_column": "analysis_is_university",
        "imputed_output_column": "analysis_imputed_is_university",
        "high_confidence_patterns": (
            r"\buniversity\b",
            r"\bcollegiate\b",
            r"\bcollege\b",
            r"\bDWU\b.*\bBIOLOGY DEPT\b",
            r"\bBIOLOGY DEPT\b.*\bDWU\b",
        ),
        "medium_confidence_patterns": (
            r"\bfraternity\b",
            r"\bsorority\b",
            r"\bdepartment of\b.*\b(?:university|college)\b",
            r"\bdept of\b.*\b(?:university|college)\b",
        ),
        "block_patterns": (),
        "analysis_basis": "Complete university flag for analysis workflows that require no missing values.",
        "notes": (
            "Imputed flag that prefers analysis_is_university, then uses only high-confidence FILERNAME1 matches, "
            "then defaults remaining unknown rows to False."
        ),
    },
    {
        "canonical_output_column": "analysis_is_political_org",
        "imputed_output_column": "analysis_imputed_is_political_org",
        "high_confidence_patterns": (
            r"\bvoters?\b",
            r"\bcampaign\b",
            r"\bdemocratic\b",
            r"\brepublican\b",
            r"\bpolitical\b",
            r"\bparty\b",
            r"\bballot\b",
            r"\bpac\b",
            r"\belection\b",
        ),
        "medium_confidence_patterns": (
            r"\bcommittee\b",
            r"\baction\b",
            r"\bvoices?\b",
            r"\bamerica\b",
        ),
        "block_patterns": (),
        "analysis_basis": "Complete political-organization flag for analysis workflows that require no missing values.",
        "notes": (
            "Imputed flag that prefers analysis_is_political_org, then uses only high-confidence FILERNAME1 matches, "
            "then defaults remaining unknown rows to False."
        ),
    },
]

UNRESOLVED_CONCEPTS: list[dict[str, str]] = []


def _empty_string_series(index: pd.Index) -> pd.Series:
    """Return an empty pandas string series with the requested index."""
    return pd.Series(pd.NA, index=index, dtype="string")


def _blank_to_na(series: pd.Series) -> pd.Series:
    """Convert blank-like text to pandas NA while preserving string dtype."""
    text = series.astype("string")
    return text.mask(text.fillna("").str.strip().eq(""), pd.NA)


def _numeric_from_text(series: pd.Series) -> pd.Series:
    """Parse GT source text amounts into nullable numeric values for analysis use."""
    cleaned = (
        series.astype("string")
        .fillna("")
        .str.strip()
        .str.replace(",", "", regex=False)
        .str.replace("$", "", regex=False)
        .str.replace(r"^\((.*)\)$", r"-\1", regex=True)
    )
    cleaned = cleaned.mask(cleaned.eq(""), pd.NA)
    return pd.to_numeric(cleaned, errors="coerce").astype("Float64")


def _normalize_flag(series: pd.Series) -> pd.Series:
    """
    Normalize GT hospital/university indicators into nullable booleans.

    The source files can use slightly different flag conventions. We preserve
    missing values, honor explicit negative markers, and treat any other
    nonblank marker as a positive indicator so the exclusion flags remain usable
    without guessing across multiple downstream steps.
    """
    text = series.astype("string").fillna("").str.strip().str.upper()
    output = pd.Series(pd.NA, index=series.index, dtype="boolean")
    output.loc[text.isin(FALSE_FLAG_VALUES)] = False
    output.loc[text.isin(TRUE_FLAG_VALUES)] = True
    other_nonblank = text.ne("") & ~text.isin(FALSE_FLAG_VALUES) & ~text.isin(TRUE_FLAG_VALUES)
    output.loc[other_nonblank] = True
    return output


def _normalize_subsection_code(series: pd.Series) -> pd.Series:
    """Normalize subsection-style codes so cross-source comparisons stay stable."""
    return (
        series.astype("string")
        .fillna("")
        .str.strip()
        .str.upper()
        .str.replace(r"[^A-Z0-9]", "", regex=True)
        .mask(lambda values: values.eq(""), pd.NA)
    )


def _normalize_political_org_flag(series: pd.Series) -> pd.Series:
    """
    Normalize subsection classification into a nullable political-org boolean.

    We keep the same nullable-boolean convention used for the hospital and
    university flags: blank classification stays missing, recognized political
    codes become True, and other populated subsection codes become False.
    """
    normalized = _normalize_subsection_code(series)
    output = pd.Series(pd.NA, index=series.index, dtype="boolean")
    output.loc[normalized.notna()] = False
    output.loc[normalized.isin(POLITICAL_ORG_SUBSECTION_CODES)] = True
    return output


def _build_ntee_proxy_flag(ntee_series: pd.Series, positive_prefixes: tuple[str, ...]) -> tuple[pd.Series, pd.Series]:
    """
    Build one nullable boolean proxy from the resolved NTEE classification.

    The user asked to replace the older source-specific boolean columns with
    NTEE-based proxies. We therefore drive the public analysis flags from the
    final resolved `analysis_ntee_code` field so the rule is consistent across
    forms and across fallback sources. Blank NTEE stays missing, configured
    prefixes become True, and other populated NTEE values become False.
    """
    normalized = _blank_to_na(ntee_series).astype("string").str.upper()
    output = pd.Series(pd.NA, index=ntee_series.index, dtype="boolean")
    has_ntee = normalized.notna()
    output.loc[has_ntee] = False
    positive_mask = pd.Series(False, index=ntee_series.index)
    for prefix in positive_prefixes:
        positive_mask = positive_mask | normalized.fillna("").str.startswith(prefix)
    output.loc[positive_mask] = True
    provenance = pd.Series(pd.NA, index=ntee_series.index, dtype="string")
    provenance.loc[has_ntee] = "analysis_ntee_code"
    return output, provenance


def _build_name_proxy_flag(
    name_series: pd.Series,
    canonical_series: pd.Series,
    high_confidence_patterns: tuple[str, ...],
    medium_confidence_patterns: tuple[str, ...],
    block_patterns: tuple[str, ...],
) -> tuple[pd.Series, pd.Series, pd.Series, pd.Series]:
    """
    Build a positive-only name-based fallback for rows still missing a flag.

    This helper intentionally does less than the NTEE proxy. It only marks a
    row True when a keyword pattern appears in the organization name and the
    canonical NTEE-based flag is still missing. It also records confidence so
    only clearly strong matches are allowed to flow into the filled analysis
    columns. It never manufactures a False value from a non-match because the
    absence of a keyword in a name is not strong evidence of the opposite
    classification.
    """
    text = name_series.astype("string").fillna("")
    missing_canonical = canonical_series.isna()
    blocked = pd.Series(False, index=name_series.index)
    for pattern in block_patterns:
        blocked = blocked | text.str.contains(pattern, case=False, regex=True)

    high_match = pd.Series(False, index=name_series.index)
    for pattern in high_confidence_patterns:
        high_match = high_match | text.str.contains(pattern, case=False, regex=True)

    medium_match = pd.Series(False, index=name_series.index)
    for pattern in medium_confidence_patterns:
        medium_match = medium_match | text.str.contains(pattern, case=False, regex=True)

    high_match = high_match & ~blocked
    medium_match = medium_match & ~blocked & ~high_match

    proxy_values = pd.Series(pd.NA, index=name_series.index, dtype="boolean")
    proxy_values.loc[missing_canonical & (high_match | medium_match)] = True

    proxy_source = pd.Series(pd.NA, index=name_series.index, dtype="string")
    proxy_source.loc[missing_canonical & (high_match | medium_match)] = "FILERNAME1"

    proxy_confidence = pd.Series(pd.NA, index=name_series.index, dtype="string")
    proxy_confidence.loc[missing_canonical & high_match] = "high"
    proxy_confidence.loc[missing_canonical & medium_match] = "medium"

    filled_values = canonical_series.astype("boolean").copy()
    high_fill_mask = filled_values.isna() & proxy_confidence.eq("high")
    filled_values.loc[high_fill_mask] = True
    return proxy_values, proxy_source, proxy_confidence, filled_values


def _series_has_value(series: pd.Series) -> pd.Series:
    """
    Return a boolean mask for populated values with blank-string handling.

    Coverage reporting should count only meaningful populated values. For text
    fields such as NTEE codes, blank strings should be treated the same as
    missing values instead of inflating fill rates.
    """
    if pd.api.types.is_string_dtype(series.dtype) or series.dtype == object:
        return series.fillna("").astype(str).str.strip().ne("")
    return series.notna()


def _build_form_aware_amount(
    source_df: pd.DataFrame,
    *,
    form_series: pd.Series,
    source_by_form: dict[str, str],
) -> tuple[pd.Series, pd.Series]:
    """
    Build one GT amount column whose source depends on the filing form type.

    We emit both the numeric analysis value and a row-level provenance column so
    later harmonization work can see exactly which raw GT field fed each row.
    """
    value_series = pd.Series(pd.NA, index=source_df.index, dtype="Float64")
    provenance_series = _empty_string_series(source_df.index)
    for form_type, source_column in source_by_form.items():
        if source_column not in source_df.columns:
            continue
        form_mask = form_series.eq(form_type)
        if not form_mask.any():
            continue
        value_series.loc[form_mask] = _numeric_from_text(source_df.loc[form_mask, source_column])
        provenance_series.loc[form_mask] = source_column
    provenance_series = provenance_series.mask(provenance_series.fillna("").eq(""), pd.NA)
    return value_series, provenance_series


def _build_direct_amount(source_df: pd.DataFrame, source_column: str) -> tuple[pd.Series, pd.Series]:
    """Build one numeric GT amount plus its row-level source-column provenance."""
    if source_column not in source_df.columns:
        return pd.Series(pd.NA, index=source_df.index, dtype="Float64"), _empty_string_series(source_df.index)
    value_series = _numeric_from_text(source_df[source_column])
    provenance_series = pd.Series(source_column, index=source_df.index, dtype="string")
    provenance_series = provenance_series.mask(value_series.isna(), pd.NA)
    return value_series, provenance_series


def _build_component_sum(source_df: pd.DataFrame, source_columns: list[str]) -> tuple[pd.Series, pd.Series]:
    """
    Build one additive GT metric from several component columns plus provenance.

    This helper is intentionally explicit about blank handling because the
    analysis asks for combined contribution/grant totals while the GT
    source exposes those concepts as separate component fields. We treat blank
    components as missing pieces of the sum, not as hard failures, and only
    emit a null total when every component is blank.
    """
    present_columns = [column for column in source_columns if column in source_df.columns]
    if not present_columns:
        return pd.Series(pd.NA, index=source_df.index, dtype="Float64"), _empty_string_series(source_df.index)

    component_values = pd.DataFrame(index=source_df.index)
    component_present_mask = pd.DataFrame(index=source_df.index)
    for column_name in present_columns:
        raw_series = _blank_to_na(source_df[column_name])
        component_values[column_name] = _numeric_from_text(raw_series)
        component_present_mask[column_name] = raw_series.notna()

    any_component_present = component_present_mask.any(axis=1)
    summed_values = component_values.fillna(0).sum(axis=1).astype("Float64")
    summed_values = summed_values.where(any_component_present, pd.NA)

    provenance_label = "derived:" + "+".join(present_columns)
    provenance_series = pd.Series(provenance_label, index=source_df.index, dtype="string")
    provenance_series = provenance_series.mask(~any_component_present, pd.NA)
    return summed_values, provenance_series


def _build_ratio_metric(
    numerator: pd.Series,
    denominator: pd.Series,
    *,
    provenance_label: str,
    require_positive_denominator: bool = False,
) -> tuple[pd.Series, pd.Series]:
    """
    Build one nullable ratio metric plus its provenance label.

    This keeps ratio handling explicit and consistent across row-level GT
    analysis metrics. Ratios are only emitted where both numerator and
    denominator are available and the denominator is nonzero.
    """
    ratio_values = pd.Series(pd.NA, index=numerator.index, dtype="Float64")
    valid_mask = numerator.notna() & denominator.notna() & denominator.ne(0)
    if require_positive_denominator:
        valid_mask = valid_mask & denominator.gt(0)
    ratio_values.loc[valid_mask] = (numerator.loc[valid_mask] / denominator.loc[valid_mask]).astype("Float64")
    provenance = pd.Series(pd.NA, index=numerator.index, dtype="string")
    provenance.loc[valid_mask] = provenance_label
    return ratio_values, provenance


def _build_grants_share_quality_flag(
    grants_total: pd.Series,
    total_revenue: pd.Series,
    ratio: pd.Series,
) -> pd.Series:
    """
    Classify row-level grants-share usability for downstream analysis.

    This keeps the main ratio numeric while making the main edge cases explicit:
    missing components, nonpositive denominators, and ratios above 1 that may
    indicate source inconsistency or atypical reporting structure.
    """
    quality = pd.Series(pd.NA, index=ratio.index, dtype="string")
    grants_present = grants_total.notna()
    revenue_present = total_revenue.notna()
    quality.loc[~grants_present | ~revenue_present] = "missing_component"
    quality.loc[grants_present & revenue_present & total_revenue.le(0)] = "nonpositive_revenue"
    quality.loc[ratio.notna() & ratio.le(1)] = "valid_0_to_1"
    quality.loc[ratio.notna() & ratio.gt(1)] = "gt_1_source_anomaly"
    return quality


def _build_region_metrics_output(analysis_df: pd.DataFrame) -> pd.DataFrame:
    """
    Build one region/tax-year aggregate table for analysis-team normalization work.

    This output is intentionally derived from the final row-level GT analysis
    parquet so the region metrics inherit the fully documented row-level
    contracts, NTEE enrichment, and imputed exclusion flags from this step.
    """
    scope_specs = [
        {
            "analysis_exclusion_variant": "all_rows",
            "row_mask": pd.Series(True, index=analysis_df.index),
        },
        {
            "analysis_exclusion_variant": "exclude_imputed_hospital_university_political_org",
            "row_mask": ~(
                analysis_df["analysis_imputed_is_hospital"].fillna(False).astype(bool)
                | analysis_df["analysis_imputed_is_university"].fillna(False).astype(bool)
                | analysis_df["analysis_imputed_is_political_org"].fillna(False).astype(bool)
            ),
        },
    ]

    all_region_year_pairs = (
        analysis_df[["region", "tax_year"]]
        .drop_duplicates()
        .sort_values(["region", "tax_year"], kind="mergesort")
        .itertuples(index=False, name=None)
    )
    all_region_year_pairs = list(all_region_year_pairs)

    region_rows: list[dict[str, object]] = []
    for spec in tqdm(scope_specs, desc="region metric scopes", unit="scope"):
        scoped_df = analysis_df.loc[spec["row_mask"]].copy()
        grouped = {
            (region, tax_year): group_df
            for (region, tax_year), group_df in scoped_df.groupby(["region", "tax_year"], dropna=False, sort=True)
        }
        for region, tax_year in all_region_year_pairs:
            group_df = grouped.get((region, tax_year), scoped_df.iloc[0:0].copy())
            filing_count = int(len(group_df))
            unique_ein_count = int(group_df["ein"].dropna().astype("string").nunique())
            total_revenue_sum = group_df["analysis_total_revenue_amount"].sum(min_count=1)
            total_grants_sum = group_df["analysis_calculated_grants_total_amount"].sum(min_count=1)
            total_net_asset_sum = group_df["analysis_calculated_net_asset_amount"].sum(min_count=1)
            total_revenue_sum = total_revenue_sum if pd.notna(total_revenue_sum) else pd.NA
            total_grants_sum = total_grants_sum if pd.notna(total_grants_sum) else pd.NA
            total_net_asset_sum = total_net_asset_sum if pd.notna(total_net_asset_sum) else pd.NA

            normalized_revenue = pd.NA
            normalized_net_asset = pd.NA
            normalized_revenue_per_unique_ein = pd.NA
            normalized_net_asset_per_unique_ein = pd.NA
            grants_share = pd.NA
            cleaned_grants_share = pd.NA
            grants_share_valid_count = int(group_df["analysis_grants_share_of_revenue_quality_flag"].eq("valid_0_to_1").sum())
            grants_share_gt_1_count = int(group_df["analysis_grants_share_of_revenue_quality_flag"].eq("gt_1_source_anomaly").sum())
            if filing_count > 0 and pd.notna(total_revenue_sum):
                normalized_revenue = float(total_revenue_sum) / filing_count
            if filing_count > 0 and pd.notna(total_net_asset_sum):
                normalized_net_asset = float(total_net_asset_sum) / filing_count
            if unique_ein_count > 0 and pd.notna(total_revenue_sum):
                normalized_revenue_per_unique_ein = float(total_revenue_sum) / unique_ein_count
            if unique_ein_count > 0 and pd.notna(total_net_asset_sum):
                normalized_net_asset_per_unique_ein = float(total_net_asset_sum) / unique_ein_count
            if pd.notna(total_revenue_sum) and float(total_revenue_sum) != 0 and pd.notna(total_grants_sum):
                grants_share = float(total_grants_sum) / float(total_revenue_sum)
            cleaned_ratio_rows = group_df.loc[
                group_df["analysis_total_revenue_amount"].notna()
                & group_df["analysis_total_revenue_amount"].gt(0)
                & group_df["analysis_calculated_grants_total_amount"].notna()
            ].copy()
            if not cleaned_ratio_rows.empty:
                cleaned_revenue_sum = cleaned_ratio_rows["analysis_total_revenue_amount"].sum(min_count=1)
                cleaned_grants_sum = cleaned_ratio_rows["analysis_calculated_grants_total_amount"].sum(min_count=1)
                if pd.notna(cleaned_revenue_sum) and float(cleaned_revenue_sum) != 0 and pd.notna(cleaned_grants_sum):
                    cleaned_grants_share = float(cleaned_grants_sum) / float(cleaned_revenue_sum)

            region_rows.append(
                {
                    "region": region,
                    "tax_year": tax_year,
                    "analysis_exclusion_variant": spec["analysis_exclusion_variant"],
                    "nonprofit_filing_count": filing_count,
                    "unique_nonprofit_ein_count": unique_ein_count,
                    "analysis_total_revenue_amount_sum": total_revenue_sum,
                    "analysis_calculated_grants_total_amount_sum": total_grants_sum,
                    "analysis_calculated_net_asset_amount_sum": total_net_asset_sum,
                    "analysis_calculated_normalized_total_revenue_per_nonprofit": normalized_revenue,
                    "analysis_calculated_normalized_net_asset_per_nonprofit": normalized_net_asset,
                    "analysis_calculated_normalized_total_revenue_per_unique_nonprofit": normalized_revenue_per_unique_ein,
                    "analysis_calculated_normalized_net_asset_per_unique_nonprofit": normalized_net_asset_per_unique_ein,
                    "analysis_calculated_region_grants_share_of_revenue_ratio": grants_share,
                    "analysis_calculated_cleaned_region_grants_share_of_revenue_ratio": cleaned_grants_share,
                    "analysis_grants_share_valid_row_count": grants_share_valid_count,
                    "analysis_grants_share_gt_1_row_count": grants_share_gt_1_count,
                    "analysis_imputed_is_hospital_true_count": int(group_df["analysis_imputed_is_hospital"].fillna(False).astype(bool).sum()),
                    "analysis_imputed_is_university_true_count": int(group_df["analysis_imputed_is_university"].fillna(False).astype(bool).sum()),
                    "analysis_imputed_is_political_org_true_count": int(group_df["analysis_imputed_is_political_org"].fillna(False).astype(bool).sum()),
                }
            )

    region_metrics_df = pd.DataFrame(region_rows).sort_values(
        ["region", "tax_year", "analysis_exclusion_variant"],
        kind="mergesort",
    ).reset_index(drop=True)
    return region_metrics_df


def _build_flag(source_df: pd.DataFrame, source_column: str) -> tuple[pd.Series, pd.Series]:
    """Build one normalized GT boolean flag plus its row-level source provenance."""
    if source_column not in source_df.columns:
        return pd.Series(pd.NA, index=source_df.index, dtype="boolean"), _empty_string_series(source_df.index)
    value_series = _normalize_flag(source_df[source_column])
    provenance_series = pd.Series(source_column, index=source_df.index, dtype="string")
    provenance_series = provenance_series.mask(value_series.isna(), pd.NA)
    return value_series, provenance_series


def _load_bmf_ntee_lookup(required_tax_years: list[str]) -> pd.DataFrame:
    """
    Load NCCS BMF NTEE lookup rows needed for exact and nearest-year fallback.

    Exact-year enrichment still requires one lookup file per GT tax year. We
    additionally load every discovered staged NCCS BMF lookup year so missing
    exact-year NTEE values can fall back to the nearest available year by EIN
    before we resort to the IRS EO BMF fallback. This preserves the requested
    source-preference order while keeping the fallback inputs explicit.
    """
    lookup_paths_by_year = discover_bmf_exact_year_lookup_inputs()
    print("[analysis] Discovered NCCS BMF exact-year lookup files:", flush=True)
    for tax_year, path in lookup_paths_by_year.items():
        print(f"[analysis]   tax_year={tax_year} lookup={path}", flush=True)

    missing_years = [tax_year for tax_year in required_tax_years if tax_year not in lookup_paths_by_year]
    if missing_years:
        raise FileNotFoundError(
            "Missing NCCS BMF exact-year lookup files for GT tax years: "
            + ", ".join(missing_years)
        )

    frames: list[pd.DataFrame] = []
    available_tax_years = sorted(lookup_paths_by_year.keys(), key=int)
    for tax_year in tqdm(available_tax_years, desc="load BMF NTEE lookups", unit="year"):
        lookup_path = lookup_paths_by_year[tax_year]
        frame = pd.read_parquet(
            lookup_path,
            columns=[
                "harm_ein",
                "harm_tax_year",
                "harm_ntee_code",
                "harm_ntee_code__source_family",
                "harm_ntee_code__source_variant",
                "harm_ntee_code__source_column",
                "harm_subsection_code",
                "harm_subsection_code__source_family",
                "harm_subsection_code__source_variant",
                "harm_subsection_code__source_column",
            ],
        )
        frame = frame.rename(
            columns={
                "harm_ein": "ein",
                "harm_tax_year": "source_tax_year",
                "harm_ntee_code": "analysis_ntee_code",
                "harm_ntee_code__source_family": "analysis_ntee_code_source_family",
                "harm_ntee_code__source_variant": "analysis_ntee_code_source_variant",
                "harm_ntee_code__source_column": "analysis_ntee_code_source_column",
                "harm_subsection_code": "analysis_subsection_code",
                "harm_subsection_code__source_family": "analysis_subsection_code_source_family",
                "harm_subsection_code__source_variant": "analysis_subsection_code_source_variant",
                "harm_subsection_code__source_column": "analysis_subsection_code_source_column",
            }
        )
        frame["ein"] = frame["ein"].map(normalize_ein).astype("string")
        frame["source_tax_year"] = frame["source_tax_year"].astype("string")
        frame["analysis_ntee_code"] = _blank_to_na(frame["analysis_ntee_code"])
        frame["analysis_ntee_code_source_family"] = _blank_to_na(frame["analysis_ntee_code_source_family"])
        frame["analysis_ntee_code_source_variant"] = _blank_to_na(frame["analysis_ntee_code_source_variant"])
        frame["analysis_ntee_code_source_column"] = _blank_to_na(frame["analysis_ntee_code_source_column"])
        frame["analysis_subsection_code"] = _normalize_subsection_code(frame["analysis_subsection_code"])
        frame["analysis_subsection_code_source_family"] = _blank_to_na(frame["analysis_subsection_code_source_family"])
        frame["analysis_subsection_code_source_variant"] = _blank_to_na(frame["analysis_subsection_code_source_variant"])
        frame["analysis_subsection_code_source_column"] = _blank_to_na(frame["analysis_subsection_code_source_column"])
        frames.append(frame)

    lookup_df = pd.concat(frames, ignore_index=True)
    lookup_df = lookup_df.drop_duplicates(subset=["ein", "source_tax_year"], keep="first")
    return lookup_df


def _load_irs_bmf_ntee_lookup() -> pd.DataFrame:
    """
    Load raw IRS EO BMF NTEE rows keyed by EIN for the final fallback layer.

    This fallback exists only to rescue GT NTEE gaps after NCCS exact-year and
    nearest-year attempts both fail. We preserve the IRS source file name so the
    final output still makes it obvious when the classification did not come
    from NCCS BMF.
    """
    try:
        raw_paths = discover_irs_bmf_raw_inputs()
    except FileNotFoundError as exc:
        print(f"[analysis] IRS EO BMF fallback unavailable: {exc}", flush=True)
        return pd.DataFrame(
            columns=[
                "ein",
                "analysis_ntee_code",
                "analysis_ntee_code_source_family",
                "analysis_ntee_code_source_variant",
                "analysis_ntee_code_source_column",
                "analysis_subsection_code",
                "analysis_subsection_code_source_family",
                "analysis_subsection_code_source_variant",
                "analysis_subsection_code_source_column",
                "irs_bmf_state",
            ]
        )
    print("[analysis] Discovered IRS EO BMF raw files:", flush=True)
    for path in raw_paths:
        print(f"[analysis]   raw_file={path}", flush=True)

    frames: list[pd.DataFrame] = []
    for raw_path in tqdm(raw_paths, desc="load IRS EO BMF fallback files", unit="file"):
        frame = pd.read_csv(
            raw_path,
            dtype=str,
            low_memory=False,
            usecols=lambda column_name: column_name in {"EIN", "STATE", "NTEE_CD", "SUBSECTION", "NAME"},
        )
        if "EIN" not in frame.columns or "NTEE_CD" not in frame.columns:
            print(
                f"[analysis] Skipping IRS EO BMF file without EIN/NTEE_CD columns: {raw_path}",
                flush=True,
            )
            continue
        frame["ein"] = frame["EIN"].map(normalize_ein).astype("string")
        frame["analysis_ntee_code"] = _blank_to_na(frame["NTEE_CD"])
        frame["analysis_ntee_code_source_family"] = pd.Series("irs_bmf", index=frame.index, dtype="string")
        frame["analysis_ntee_code_source_variant"] = pd.Series(
            f"{raw_path.name}|ein_fallback",
            index=frame.index,
            dtype="string",
        )
        frame["analysis_ntee_code_source_column"] = pd.Series("NTEE_CD", index=frame.index, dtype="string")
        frame["analysis_subsection_code"] = _normalize_subsection_code(
            frame["SUBSECTION"] if "SUBSECTION" in frame.columns else pd.Series(pd.NA, index=frame.index, dtype="string")
        )
        frame["analysis_subsection_code_source_family"] = pd.Series("irs_bmf", index=frame.index, dtype="string")
        frame["analysis_subsection_code_source_variant"] = pd.Series(
            f"{raw_path.name}|ein_fallback",
            index=frame.index,
            dtype="string",
        )
        frame["analysis_subsection_code_source_column"] = pd.Series("SUBSECTION", index=frame.index, dtype="string")
        frame["irs_bmf_state"] = _blank_to_na(frame["STATE"].astype("string") if "STATE" in frame.columns else pd.Series(pd.NA, index=frame.index, dtype="string"))
        frames.append(
            frame[
                [
                    "ein",
                    "analysis_ntee_code",
                    "analysis_ntee_code_source_family",
                    "analysis_ntee_code_source_variant",
                    "analysis_ntee_code_source_column",
                    "analysis_subsection_code",
                    "analysis_subsection_code_source_family",
                    "analysis_subsection_code_source_variant",
                    "analysis_subsection_code_source_column",
                    "irs_bmf_state",
                ]
            ]
        )

    if not frames:
        print("[analysis] No usable IRS EO BMF fallback rows were discovered.", flush=True)
        return pd.DataFrame(
            columns=[
                "ein",
                "analysis_ntee_code",
                "analysis_ntee_code_source_family",
                "analysis_ntee_code_source_variant",
                "analysis_ntee_code_source_column",
                "analysis_subsection_code",
                "analysis_subsection_code_source_family",
                "analysis_subsection_code_source_variant",
                "analysis_subsection_code_source_column",
                "irs_bmf_state",
            ]
        )

    lookup_df = pd.concat(frames, ignore_index=True)
    lookup_df = lookup_df.loc[lookup_df["ein"].astype("string").str.len().eq(9)].copy()
    lookup_df = lookup_df.loc[
        _series_has_value(lookup_df["analysis_ntee_code"])
        | _series_has_value(lookup_df["analysis_subsection_code"])
    ].copy()
    return lookup_df


def _apply_exact_year_bmf_ntee(
    analysis_df: pd.DataFrame,
    source_df: pd.DataFrame,
    bmf_ntee_lookup_df: pd.DataFrame,
) -> tuple[pd.DataFrame, int]:
    """
    Apply the preferred exact-year NCCS BMF NTEE enrichment to GT rows.

    Exact-year matching remains the primary design because it is the cleanest
    and most auditable alignment between a GT filing tax year and the NCCS BMF
    classification overlay for that same year.
    """
    print("[analysis] Joining exact-year NCCS BMF NTEE classification onto GT rows...", flush=True)
    exact_lookup_df = (
        bmf_ntee_lookup_df.rename(columns={"source_tax_year": "tax_year"})
        .drop_duplicates(subset=["ein", "tax_year"], keep="first")
    )
    analysis_df = analysis_df.join(
        source_df[["ein", "tax_year"]]
        .astype({"ein": "string", "tax_year": "string"})
        .merge(
            exact_lookup_df,
            on=["ein", "tax_year"],
            how="left",
            validate="many_to_one",
        )
        .drop(columns=["ein", "tax_year"])
    )
    for column_name in [
        "analysis_ntee_code",
        "analysis_ntee_code_source_family",
        "analysis_ntee_code_source_variant",
        "analysis_ntee_code_source_column",
        "analysis_subsection_code",
        "analysis_subsection_code_source_family",
        "analysis_subsection_code_source_variant",
        "analysis_subsection_code_source_column",
    ]:
        if column_name == "analysis_subsection_code":
            analysis_df[column_name] = _normalize_subsection_code(analysis_df[column_name].astype("string"))
        else:
            analysis_df[column_name] = _blank_to_na(analysis_df[column_name].astype("string"))
    exact_count = int(_series_has_value(analysis_df["analysis_ntee_code"]).sum())
    print(f"[analysis] Exact-year NCCS BMF NTEE rows populated: {exact_count:,}", flush=True)
    return analysis_df, exact_count


def _apply_nearest_year_bmf_fallback(
    analysis_df: pd.DataFrame,
    source_df: pd.DataFrame,
    bmf_ntee_lookup_df: pd.DataFrame,
) -> int:
    """
    Fill missing GT NTEE rows from the nearest NCCS BMF year for the same EIN.

    This is a documented fallback, not the primary design. We only use it when
    the exact-year NCCS BMF enrichment left the GT row without a usable NTEE
    value. Among multiple candidate years we pick the smallest absolute year
    distance, then prefer the earlier year in ties so the rule is deterministic
    and avoids future-year leakage when the distance is the same.
    """
    missing_mask = ~_series_has_value(analysis_df["analysis_ntee_code"])
    missing_count = int(missing_mask.sum())
    print(f"[analysis] Missing NTEE rows before nearest-year NCCS fallback: {missing_count:,}", flush=True)
    if missing_count == 0:
        return 0

    candidate_lookup = bmf_ntee_lookup_df.loc[_series_has_value(bmf_ntee_lookup_df["analysis_ntee_code"])].copy()
    if candidate_lookup.empty:
        print("[analysis] No nonblank NCCS BMF NTEE candidate rows available for nearest-year fallback.", flush=True)
        return 0

    gt_missing = source_df.loc[missing_mask, ["ein", "tax_year"]].copy()
    gt_missing["row_index"] = gt_missing.index
    gt_missing["gt_tax_year_int"] = pd.to_numeric(gt_missing["tax_year"], errors="coerce")

    candidate_lookup["source_tax_year_int"] = pd.to_numeric(candidate_lookup["source_tax_year"], errors="coerce")
    merged = gt_missing.merge(candidate_lookup, on="ein", how="inner", validate="many_to_many")
    if merged.empty:
        print("[analysis] No same-EIN NCCS BMF rows found for nearest-year fallback.", flush=True)
        return 0

    merged["year_distance"] = (merged["gt_tax_year_int"] - merged["source_tax_year_int"]).abs()
    merged = merged.sort_values(
        by=["row_index", "year_distance", "source_tax_year_int"],
        ascending=[True, True, True],
        kind="mergesort",
    )
    chosen = merged.drop_duplicates(subset=["row_index"], keep="first").copy()
    chosen["analysis_ntee_code_source_variant"] = chosen.apply(
        lambda row: (
            f"{row['analysis_ntee_code_source_variant']}|nearest_year_fallback_from_{row['tax_year']}_to_{row['source_tax_year']}"
        ),
        axis=1,
    )

    for row in tqdm(
        chosen.itertuples(index=False),
        total=len(chosen),
        desc="apply NCCS nearest-year NTEE fallback",
        unit="row",
    ):
        analysis_df.at[row.row_index, "analysis_ntee_code"] = row.analysis_ntee_code
        analysis_df.at[row.row_index, "analysis_ntee_code_source_family"] = row.analysis_ntee_code_source_family
        analysis_df.at[row.row_index, "analysis_ntee_code_source_variant"] = row.analysis_ntee_code_source_variant
        analysis_df.at[row.row_index, "analysis_ntee_code_source_column"] = row.analysis_ntee_code_source_column

    filled_count = int(len(chosen))
    print(f"[analysis] NCCS nearest-year fallback rows populated: {filled_count:,}", flush=True)
    return filled_count


def _apply_nearest_year_bmf_subsection_fallback(
    analysis_df: pd.DataFrame,
    source_df: pd.DataFrame,
    bmf_ntee_lookup_df: pd.DataFrame,
) -> int:
    """
    Fill missing GT subsection rows from the nearest NCCS BMF year for the same EIN.

    Political-organization classification comes from subsection codes, so it
    needs the same year-aware fallback behavior as NTEE. The nearest-year rule
    is identical: smallest absolute year distance, then earlier year in ties.
    """
    missing_mask = ~_series_has_value(analysis_df["analysis_subsection_code"])
    missing_count = int(missing_mask.sum())
    print(f"[analysis] Missing subsection rows before nearest-year NCCS fallback: {missing_count:,}", flush=True)
    if missing_count == 0:
        return 0

    candidate_lookup = bmf_ntee_lookup_df.loc[_series_has_value(bmf_ntee_lookup_df["analysis_subsection_code"])].copy()
    if candidate_lookup.empty:
        print("[analysis] No nonblank NCCS BMF subsection candidate rows available for nearest-year fallback.", flush=True)
        return 0

    gt_missing = source_df.loc[missing_mask, ["ein", "tax_year"]].copy()
    gt_missing["row_index"] = gt_missing.index
    gt_missing["gt_tax_year_int"] = pd.to_numeric(gt_missing["tax_year"], errors="coerce")

    candidate_lookup["source_tax_year_int"] = pd.to_numeric(candidate_lookup["source_tax_year"], errors="coerce")
    merged = gt_missing.merge(candidate_lookup, on="ein", how="inner", validate="many_to_many")
    if merged.empty:
        print("[analysis] No same-EIN NCCS BMF rows found for nearest-year subsection fallback.", flush=True)
        return 0

    merged["year_distance"] = (merged["gt_tax_year_int"] - merged["source_tax_year_int"]).abs()
    merged = merged.sort_values(
        by=["row_index", "year_distance", "source_tax_year_int"],
        ascending=[True, True, True],
        kind="mergesort",
    )
    chosen = merged.drop_duplicates(subset=["row_index"], keep="first").copy()
    chosen["analysis_subsection_code_source_variant"] = chosen.apply(
        lambda row: (
            f"{row['analysis_subsection_code_source_variant']}|nearest_year_fallback_from_{row['tax_year']}_to_{row['source_tax_year']}"
        ),
        axis=1,
    )

    for row in tqdm(
        chosen.itertuples(index=False),
        total=len(chosen),
        desc="apply NCCS nearest-year subsection fallback",
        unit="row",
    ):
        analysis_df.at[row.row_index, "analysis_subsection_code"] = row.analysis_subsection_code
        analysis_df.at[row.row_index, "analysis_subsection_code_source_family"] = row.analysis_subsection_code_source_family
        analysis_df.at[row.row_index, "analysis_subsection_code_source_variant"] = row.analysis_subsection_code_source_variant
        analysis_df.at[row.row_index, "analysis_subsection_code_source_column"] = row.analysis_subsection_code_source_column

    filled_count = int(len(chosen))
    print(f"[analysis] NCCS nearest-year subsection fallback rows populated: {filled_count:,}", flush=True)
    return filled_count


def _apply_irs_bmf_ein_fallback(
    analysis_df: pd.DataFrame,
    source_df: pd.DataFrame,
    irs_lookup_df: pd.DataFrame,
) -> int:
    """
    Fill remaining GT NTEE gaps from IRS EO BMF by EIN.

    This is the final fallback layer. Because the IRS EO BMF files are state
    snapshots rather than GT tax-year keyed lookups, we only use them after the
    NCCS exact-year and nearest-year steps both fail. When multiple IRS rows
    share an EIN we prefer a state match to the GT filing state and then keep
    the first deterministic file ordering from discovery.
    """
    missing_mask = ~_series_has_value(analysis_df["analysis_ntee_code"])
    missing_count = int(missing_mask.sum())
    print(f"[analysis] Missing NTEE rows before IRS EO BMF fallback: {missing_count:,}", flush=True)
    if missing_count == 0:
        return 0
    if irs_lookup_df.empty:
        print("[analysis] IRS EO BMF fallback table is empty; skipping IRS fallback.", flush=True)
        return 0

    gt_missing = source_df.loc[missing_mask, ["ein", "FILERUSSTATE"]].copy()
    gt_missing["row_index"] = gt_missing.index
    gt_missing["gt_state_norm"] = _blank_to_na(gt_missing["FILERUSSTATE"].astype("string")).fillna("").str.upper()

    candidates = gt_missing.merge(irs_lookup_df, on="ein", how="inner", validate="many_to_many")
    candidates = candidates.loc[_series_has_value(candidates["analysis_ntee_code"])].copy()
    if candidates.empty:
        print("[analysis] No IRS EO BMF EIN matches found for remaining missing GT NTEE rows.", flush=True)
        return 0

    candidates["irs_state_norm"] = _blank_to_na(candidates["irs_bmf_state"].astype("string")).fillna("").str.upper()
    candidates["state_match"] = (
        candidates["gt_state_norm"].ne("")
        & candidates["irs_state_norm"].ne("")
        & candidates["gt_state_norm"].eq(candidates["irs_state_norm"])
    )
    candidates = candidates.sort_values(
        by=["row_index", "state_match", "analysis_ntee_code_source_variant"],
        ascending=[True, False, True],
        kind="mergesort",
    )
    chosen = candidates.drop_duplicates(subset=["row_index"], keep="first").copy()

    for row in tqdm(
        chosen.itertuples(index=False),
        total=len(chosen),
        desc="apply IRS EO BMF NTEE fallback",
        unit="row",
    ):
        analysis_df.at[row.row_index, "analysis_ntee_code"] = row.analysis_ntee_code
        analysis_df.at[row.row_index, "analysis_ntee_code_source_family"] = row.analysis_ntee_code_source_family
        analysis_df.at[row.row_index, "analysis_ntee_code_source_variant"] = row.analysis_ntee_code_source_variant
        analysis_df.at[row.row_index, "analysis_ntee_code_source_column"] = row.analysis_ntee_code_source_column

    filled_count = int(len(chosen))
    print(f"[analysis] IRS EO BMF EIN fallback rows populated: {filled_count:,}", flush=True)
    return filled_count


def _apply_irs_bmf_subsection_ein_fallback(
    analysis_df: pd.DataFrame,
    source_df: pd.DataFrame,
    irs_lookup_df: pd.DataFrame,
) -> int:
    """
    Fill remaining GT subsection gaps from IRS EO BMF by EIN.

    This mirrors the NTEE fallback behavior so the political-organization
    boolean can rely on the same classification-family ordering and state-aware
    tie-break logic as the final NTEE field.
    """
    missing_mask = ~_series_has_value(analysis_df["analysis_subsection_code"])
    missing_count = int(missing_mask.sum())
    print(f"[analysis] Missing subsection rows before IRS EO BMF fallback: {missing_count:,}", flush=True)
    if missing_count == 0:
        return 0
    if irs_lookup_df.empty:
        print("[analysis] IRS EO BMF fallback table is empty; skipping subsection fallback.", flush=True)
        return 0

    gt_missing = source_df.loc[missing_mask, ["ein", "FILERUSSTATE"]].copy()
    gt_missing["row_index"] = gt_missing.index
    gt_missing["gt_state_norm"] = _blank_to_na(gt_missing["FILERUSSTATE"].astype("string")).fillna("").str.upper()

    candidates = gt_missing.merge(irs_lookup_df, on="ein", how="inner", validate="many_to_many")
    candidates = candidates.loc[_series_has_value(candidates["analysis_subsection_code"])].copy()
    if candidates.empty:
        print("[analysis] No IRS EO BMF subsection matches found for remaining missing GT rows.", flush=True)
        return 0

    candidates["irs_state_norm"] = _blank_to_na(candidates["irs_bmf_state"].astype("string")).fillna("").str.upper()
    candidates["state_match"] = (
        candidates["gt_state_norm"].ne("")
        & candidates["irs_state_norm"].ne("")
        & candidates["gt_state_norm"].eq(candidates["irs_state_norm"])
    )
    candidates = candidates.sort_values(
        by=["row_index", "state_match", "analysis_subsection_code_source_variant"],
        ascending=[True, False, True],
        kind="mergesort",
    )
    chosen = candidates.drop_duplicates(subset=["row_index"], keep="first").copy()

    for row in tqdm(
        chosen.itertuples(index=False),
        total=len(chosen),
        desc="apply IRS EO BMF subsection fallback",
        unit="row",
    ):
        analysis_df.at[row.row_index, "analysis_subsection_code"] = row.analysis_subsection_code
        analysis_df.at[row.row_index, "analysis_subsection_code_source_family"] = row.analysis_subsection_code_source_family
        analysis_df.at[row.row_index, "analysis_subsection_code_source_variant"] = row.analysis_subsection_code_source_variant
        analysis_df.at[row.row_index, "analysis_subsection_code_source_column"] = row.analysis_subsection_code_source_column

    filled_count = int(len(chosen))
    print(f"[analysis] IRS EO BMF subsection fallback rows populated: {filled_count:,}", flush=True)
    return filled_count


def _build_calculated_net_asset(source_df: pd.DataFrame, form_series: pd.Series) -> tuple[pd.Series, pd.Series]:
    """
    Build a partial GT net-asset proxy from raw asset and liability fields.

    The raw GivingTuesday files support a defensible calculation for Form 990
    and for the subset of Form 990EZ rows that carry both assets and
    liabilities. The GT basic-only artifact does not carry a matching
    liability-side field for Form 990PF, so those rows intentionally remain
    null instead of forcing a misleading cross-form formula.
    """
    value_series = pd.Series(pd.NA, index=source_df.index, dtype="Float64")
    provenance_series = _empty_string_series(source_df.index)

    asset_990 = _numeric_from_text(source_df["TOASEOOYY"]) if "TOASEOOYY" in source_df.columns else pd.Series(pd.NA, index=source_df.index, dtype="Float64")
    liability_990 = _numeric_from_text(source_df["TOLIEOOYY"]) if "TOLIEOOYY" in source_df.columns else pd.Series(pd.NA, index=source_df.index, dtype="Float64")
    supported_mask = form_series.isin(["990", "990EZ"]) & asset_990.notna() & liability_990.notna()

    value_series.loc[supported_mask] = (asset_990.loc[supported_mask] - liability_990.loc[supported_mask]).astype("Float64")
    provenance_series.loc[supported_mask] = "derived:TOASEOOYY-minus-TOLIEOOYY"
    provenance_series = provenance_series.mask(provenance_series.fillna("").eq(""), pd.NA)
    return value_series, provenance_series


def _write_mapping_markdown(
    *,
    mapping_path: Path,
    source_path: Path,
    coverage_path: Path,
    metric_specs: list[dict[str, str]],
    unresolved_concepts: list[dict[str, str]],
) -> None:
    """Write the GT analysis variable mapping document."""
    source_display = _portable_path(source_path)
    coverage_display = _portable_path(coverage_path)
    lines = [
        "# GivingTuesday Basic-Only Analysis Variable Mapping",
        "",
        f"- Source artifact: `{source_display}`",
        f"- Coverage report: `{coverage_display}`",
        "",
        "## Coverage Evidence",
        "",
        f"- Generated coverage CSV: `{coverage_display}`",
        f"- Published S3 variable mapping object: `{ANALYSIS_VARIABLE_MAPPING_PREFIX}/{GT_BASIC_ANALYSIS_VARIABLE_MAPPING_MD.name}`",
        f"- Published S3 coverage object: `{ANALYSIS_COVERAGE_PREFIX}/{GT_BASIC_ANALYSIS_VARIABLE_COVERAGE_CSV.name}`",
        "- Coverage CSVs are generated by the analysis step and published as quality evidence.",
        "",
        "## Analysis Data Dictionary",
        "",
        "This file is the analysis-ready data dictionary for the GivingTuesday basic-only analysis dataset. "
        "It lists each canonical analysis variable, the source rule used to populate it, the provenance column when applicable, "
        "the analysis basis, and source-specific limitations.",
        "",
        "## Extracted Variables",
        "",
        "|canonical_variable|source_rule|provenance_column|analysis_basis|notes|",
        "|---|---|---|---|---|",
    ]
    for spec in metric_specs:
        lines.append(
            "|"
            + "|".join(
                [
                    spec["output_column"],
                    spec["source_rule"],
                    spec["provenance_column"],
                    spec["analysis_basis"],
                    spec["notes"],
                ]
            )
            + "|"
        )
    if unresolved_concepts:
        lines.extend(
            [
                "",
                "## Client Caveats",
                "",
                "|concept|status|reason|related_fields|",
                "|---|---|---|---|",
            ]
        )
        for concept in unresolved_concepts:
            lines.append(
                "|"
                + "|".join(
                    [
                        concept["concept"],
                        concept["status"],
                        concept["reason"],
                        concept["related_fields"],
                    ]
                )
                + "|"
            )
    mapping_path.parent.mkdir(parents=True, exist_ok=True)
    mapping_path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def main() -> None:
    parser = argparse.ArgumentParser(description="Extract GT basic-only analysis variables from the filtered benchmark artifact.")
    parser.add_argument("--input", default=str(FILTERED_BASIC_ALLFORMS_PARQUET), help="GT basic-only filtered parquet input")
    parser.add_argument("--output", default=str(GT_BASIC_ANALYSIS_VARIABLES_PARQUET), help="Output parquet path for GT analysis variables")
    parser.add_argument(
        "--region-metrics-output",
        default=str(GT_BASIC_ANALYSIS_REGION_METRICS_PARQUET),
        help="Output parquet path for GT region-level analysis metrics",
    )
    parser.add_argument(
        "--coverage-csv",
        default=str(GT_BASIC_ANALYSIS_VARIABLE_COVERAGE_CSV),
        help="Coverage report CSV path",
    )
    parser.add_argument(
        "--mapping-md",
        default=str(GT_BASIC_ANALYSIS_VARIABLE_MAPPING_MD),
        help="Markdown mapping doc output path",
    )
    args = parser.parse_args()

    start = time.perf_counter()
    banner("STEP 13 - EXTRACT GT BASIC-ONLY ANALYSIS VARIABLES")
    load_env_from_secrets()
    ensure_dirs()

    input_path = Path(args.input)
    output_path = Path(args.output)
    region_metrics_output_path = Path(args.region_metrics_output)
    coverage_path = Path(args.coverage_csv)
    mapping_path = Path(args.mapping_md)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    region_metrics_output_path.parent.mkdir(parents=True, exist_ok=True)
    coverage_path.parent.mkdir(parents=True, exist_ok=True)
    mapping_path.parent.mkdir(parents=True, exist_ok=True)

    if not input_path.exists():
        raise FileNotFoundError(f"GT basic-only filtered parquet not found: {input_path}")
    print(f"[analysis] Source parquet:       {input_path}", flush=True)
    print(f"[analysis] Output parquet:       {output_path}", flush=True)
    print(f"[analysis] Region metrics out:   {region_metrics_output_path}", flush=True)
    print(f"[analysis] Coverage CSV:         {coverage_path}", flush=True)
    print(f"[analysis] Mapping Markdown:     {mapping_path}", flush=True)
    source_df = pd.read_parquet(input_path)
    source_df = source_df.copy()
    source_df["ein"] = source_df["ein"].map(normalize_ein).astype("string")
    source_df["form_type"] = source_df["form_type"].astype("string").fillna("")
    source_df["tax_year"] = source_df["tax_year"].astype("string")
    print(f"[analysis] Input rows: {len(source_df):,}", flush=True)
    print(f"[analysis] Input columns: {len(source_df.columns):,}", flush=True)

    counts_by_year_form = (
        source_df.groupby(["tax_year", "form_type"], dropna=False)
        .size()
        .reset_index(name="row_count")
        .sort_values(["tax_year", "form_type"])
    )
    print("[analysis] Counts by tax_year/form_type:", flush=True)
    for row in counts_by_year_form.itertuples(index=False):
        print(
            f"[analysis]   tax_year={row.tax_year} form_type={row.form_type} rows={int(row.row_count):,}",
            flush=True,
        )

    analysis_df = pd.DataFrame(index=source_df.index)
    required_tax_years = sorted(source_df["tax_year"].dropna().astype("string").unique().tolist())
    bmf_ntee_lookup_df = _load_bmf_ntee_lookup(required_tax_years)
    irs_bmf_lookup_df = _load_irs_bmf_ntee_lookup()
    print(f"[analysis] Loaded NCCS BMF NTEE lookup rows: {len(bmf_ntee_lookup_df):,}", flush=True)
    print(f"[analysis] Loaded IRS EO BMF fallback rows: {len(irs_bmf_lookup_df):,}", flush=True)

    # Preserve the requested identifiers and geography exactly at the same row
    # grain as the GT basic-only filtered source artifact.
    identifier_columns = ["ein", "tax_year", "form_type", "FILERNAME1", "FILERUSSTATE", "zip5", "county_fips", "region"]
    for column_name in identifier_columns:
        if column_name in source_df.columns:
            analysis_df[column_name] = source_df[column_name].astype("string")
        else:
            analysis_df[column_name] = pd.Series(pd.NA, index=source_df.index, dtype="string")

    analysis_df, exact_ntee_count = _apply_exact_year_bmf_ntee(analysis_df, source_df, bmf_ntee_lookup_df)
    nearest_ntee_fallback_count = _apply_nearest_year_bmf_fallback(analysis_df, source_df, bmf_ntee_lookup_df)
    irs_ntee_fallback_count = _apply_irs_bmf_ein_fallback(analysis_df, source_df, irs_bmf_lookup_df)
    exact_subsection_count = int(_series_has_value(analysis_df["analysis_subsection_code"]).sum())
    nearest_subsection_fallback_count = _apply_nearest_year_bmf_subsection_fallback(analysis_df, source_df, bmf_ntee_lookup_df)
    irs_subsection_fallback_count = _apply_irs_bmf_subsection_ein_fallback(analysis_df, source_df, irs_bmf_lookup_df)
    analysis_df["analysis_calculated_ntee_broad_code"] = (
        analysis_df["analysis_ntee_code"]
        .astype("string")
        .str.slice(0, 1)
        .mask(analysis_df["analysis_ntee_code"].fillna("").str.strip().eq(""), pd.NA)
    )
    analysis_df["analysis_calculated_ntee_broad_code_source_column"] = pd.Series(pd.NA, index=analysis_df.index, dtype="string")
    broad_ntee_mask = analysis_df["analysis_calculated_ntee_broad_code"].notna()
    analysis_df.loc[broad_ntee_mask, "analysis_calculated_ntee_broad_code_source_column"] = "derived:first-letter-of-analysis_ntee_code"
    remaining_missing_ntee_count = int((~_series_has_value(analysis_df["analysis_ntee_code"])).sum())
    print(
        "[analysis] Final NTEE fill summary: "
        f"exact_year={exact_ntee_count:,} "
        f"nearest_year_fallback={nearest_ntee_fallback_count:,} "
        f"irs_ein_fallback={irs_ntee_fallback_count:,} "
        f"remaining_missing={remaining_missing_ntee_count:,}",
        flush=True,
    )
    remaining_missing_subsection_count = int((~_series_has_value(analysis_df["analysis_subsection_code"])).sum())
    print(
        "[analysis] Final subsection fill summary: "
        f"exact_year={exact_subsection_count:,} "
        f"nearest_year_fallback={nearest_subsection_fallback_count:,} "
        f"irs_ein_fallback={irs_subsection_fallback_count:,} "
        f"remaining_missing={remaining_missing_subsection_count:,}",
        flush=True,
    )

    metric_specs_for_doc: list[dict[str, str]] = []

    print("[analysis] Building NTEE proxy exclusion flags...", flush=True)
    for spec in tqdm(NTEE_PROXY_FLAG_SPECS, desc="NTEE proxy flags", unit="flag"):
        values, provenance = _build_ntee_proxy_flag(analysis_df["analysis_ntee_code"], spec["positive_prefixes"])
        analysis_df[spec["output_column"]] = values
        analysis_df[f"{spec['output_column']}_source_column"] = provenance
        metric_specs_for_doc.append(
            {
                "output_column": spec["output_column"],
                "source_rule": "derived:analysis_ntee_code using prefixes " + ", ".join(spec["positive_prefixes"]),
                "provenance_column": f"{spec['output_column']}_source_column",
                "analysis_basis": spec["analysis_basis"],
                "notes": spec["notes"],
            }
        )

    print("[analysis] Building complete imputed boolean flags for analysis workflows that require no missing values...", flush=True)
    for spec in tqdm(NAME_PROXY_FLAG_SPECS, desc="name fallback flags", unit="flag"):
        proxy_values, proxy_source, proxy_confidence, _filled_values = _build_name_proxy_flag(
            source_df["FILERNAME1"],
            analysis_df[spec["canonical_output_column"]],
            spec["high_confidence_patterns"],
            spec["medium_confidence_patterns"],
            spec["block_patterns"],
        )
        imputed_values = analysis_df[spec["canonical_output_column"]].astype("boolean").copy()
        imputed_source = analysis_df[f"{spec['canonical_output_column']}_source_column"].astype("string")
        subsection_fill_count = 0
        if spec["canonical_output_column"] == "analysis_is_political_org":
            subsection_values = _normalize_political_org_flag(analysis_df["analysis_subsection_code"])
            subsection_mask = imputed_values.isna() & subsection_values.notna()
            imputed_values.loc[subsection_mask] = subsection_values.loc[subsection_mask]
            imputed_source.loc[subsection_mask] = analysis_df.loc[subsection_mask, "analysis_subsection_code_source_column"].astype("string")
            subsection_fill_count = int(subsection_mask.sum())
        high_name_mask = imputed_values.isna() & proxy_confidence.fillna("").eq("high")
        default_false_mask = imputed_values.isna() & ~proxy_confidence.fillna("").eq("high")
        imputed_values.loc[high_name_mask] = True
        imputed_values.loc[default_false_mask] = False
        imputed_source.loc[high_name_mask] = "FILERNAME1"
        imputed_source.loc[default_false_mask] = "imputed_default_false"
        analysis_df[spec["imputed_output_column"]] = imputed_values
        analysis_df[f"{spec['imputed_output_column']}_source_column"] = imputed_source
        print(
            f"[analysis]   {spec['imputed_output_column']} candidate rows="
            f"{int(proxy_values.fillna(False).astype(bool).sum()):,} "
            f"(high={int(proxy_confidence.eq('high').sum()):,}, medium={int(proxy_confidence.eq('medium').sum()):,}, subsection={subsection_fill_count:,})",
            flush=True,
        )
        metric_specs_for_doc.append(
            {
                "output_column": spec["imputed_output_column"],
                "source_rule": (
                    f"coalesce({spec['canonical_output_column']}, "
                    + ("subsection-based political-org rule, " if spec["canonical_output_column"] == "analysis_is_political_org" else "")
                    + "high-confidence FILERNAME1 match, False)"
                ),
                "provenance_column": f"{spec['imputed_output_column']}_source_column",
                "analysis_basis": spec["analysis_basis"],
                "notes": (
                    f"Fully populated analysis flag that prefers {spec['canonical_output_column']}, "
                    + (
                        "then uses subsection-based political-organization classification when the NTEE-based value is missing, "
                        if spec["canonical_output_column"] == "analysis_is_political_org"
                        else ""
                    )
                    + "then uses a high-confidence FILERNAME1 match, then defaults remaining unknown rows to False. "
                    "Medium-confidence name matches do not override the default False."
                ),
            }
        )

    form_series = source_df["form_type"]

    metric_specs_for_doc.extend(
        [
            {
                "output_column": "analysis_ntee_code",
                "source_rule": "NCCS BMF exact-year harm_ntee_code on ein + tax_year, then NCCS nearest-year EIN fallback, then IRS EO BMF EIN fallback",
                "provenance_column": "analysis_ntee_code_source_family; analysis_ntee_code_source_variant; analysis_ntee_code_source_column",
                "analysis_basis": "NTEE classification required for GT-adjacent comparisons.",
                "notes": "Approved classification enrichment only. Preferred order is NCCS exact-year, then NCCS nearest-year by EIN, then IRS EO BMF by EIN when NCCS still leaves the row blank.",
            },
            {
                "output_column": "analysis_calculated_ntee_broad_code",
                "source_rule": "derived:first letter of analysis_ntee_code",
                "provenance_column": "analysis_calculated_ntee_broad_code_source_column",
                "analysis_basis": "Broad NTEE field category required where analysis uses first-letter NTEE grouping.",
                "notes": "Derived only when the exact NCCS BMF NTEE code is present.",
            },
            {
                "output_column": "analysis_subsection_code",
                "source_rule": "NCCS BMF exact-year harm_subsection_code on ein + tax_year, then NCCS nearest-year EIN fallback, then IRS EO BMF EIN fallback",
                "provenance_column": "analysis_subsection_code_source_family; analysis_subsection_code_source_variant; analysis_subsection_code_source_column",
                "analysis_basis": "Political-organization support for exclusion comparisons.",
                "notes": "Classification support field used to derive the final political-organization boolean.",
            },
        ]
    )

    print("[analysis] Building extracted numeric metrics...", flush=True)
    for spec in tqdm(FORM_AWARE_AMOUNT_SPECS, desc="form-aware GT metrics", unit="metric"):
        values, provenance = _build_form_aware_amount(
            source_df,
            form_series=form_series,
            source_by_form=spec["source_by_form"],
        )
        analysis_df[spec["output_column"]] = values
        analysis_df[f"{spec['output_column']}_source_column"] = provenance
        source_rule = "; ".join(f"{form_type}->{source_column}" for form_type, source_column in spec["source_by_form"].items())
        metric_specs_for_doc.append(
            {
                "output_column": spec["output_column"],
                "source_rule": source_rule,
                "provenance_column": f"{spec['output_column']}_source_column",
                "analysis_basis": spec["analysis_basis"],
                "notes": spec["notes"],
            }
        )

    # Surplus is part of the net-margin formula for financial-performance analysis. Calculate it here from
    # the final form-aware revenue and expense columns rather than relying on
    # upstream `derived_income_amount`, because PF uses PF-specific revenue and
    # expense fields that the older upstream derivation did not include.
    print("[analysis] Deriving analysis_calculated_surplus_amount from final analysis revenue and expense...", flush=True)
    surplus_values = pd.Series(pd.NA, index=analysis_df.index, dtype="Float64")
    valid_surplus_mask = analysis_df["analysis_total_revenue_amount"].notna() & analysis_df["analysis_total_expense_amount"].notna()
    surplus_values.loc[valid_surplus_mask] = (
        analysis_df.loc[valid_surplus_mask, "analysis_total_revenue_amount"]
        - analysis_df.loc[valid_surplus_mask, "analysis_total_expense_amount"]
    ).astype("Float64")
    analysis_df["analysis_calculated_surplus_amount"] = surplus_values
    analysis_df["analysis_calculated_surplus_amount_source_column"] = pd.Series(pd.NA, index=analysis_df.index, dtype="string")
    analysis_df.loc[valid_surplus_mask, "analysis_calculated_surplus_amount_source_column"] = (
        "derived:analysis_total_revenue_amount-minus-analysis_total_expense_amount"
    )
    metric_specs_for_doc.append(
        {
            "output_column": "analysis_calculated_surplus_amount",
            "source_rule": "derived:analysis_total_revenue_amount - analysis_total_expense_amount",
            "provenance_column": "analysis_calculated_surplus_amount_source_column",
            "analysis_basis": "Surplus component used by the net-margin formula for financial-performance analysis.",
            "notes": "Calculated from the final form-aware analysis revenue and expense values, including PF-specific revenue and expense fields.",
        }
    )

    for spec in tqdm(DIRECT_AMOUNT_SPECS, desc="direct GT metrics", unit="metric"):
        values, provenance = _build_direct_amount(source_df, spec["source_column"])
        analysis_df[spec["output_column"]] = values
        analysis_df[f"{spec['output_column']}_source_column"] = provenance
        metric_specs_for_doc.append(
            {
                "output_column": spec["output_column"],
                "source_rule": spec["source_column"],
                "provenance_column": f"{spec['output_column']}_source_column",
                "analysis_basis": spec["analysis_basis"],
                "notes": spec["notes"],
            }
        )

    print("[analysis] Building additive GT totals from contribution and grant components...", flush=True)
    for spec in tqdm(DERIVED_COMPONENT_SUM_SPECS, desc="derived GT sums", unit="metric"):
        values, provenance = _build_component_sum(source_df, spec["source_columns"])
        analysis_df[spec["output_column"]] = values
        analysis_df[f"{spec['output_column']}_source_column"] = provenance
        metric_specs_for_doc.append(
            {
                "output_column": spec["output_column"],
                "source_rule": "derived:" + " + ".join(spec["source_columns"]),
                "provenance_column": f"{spec['output_column']}_source_column",
                "analysis_basis": spec["analysis_basis"],
                "notes": spec["notes"],
            }
        )

    # The grants-dependency question is expressed as a percentage of
    # grants in total revenue. We expose that as a row-level ratio so the
    # analysis team does not have to rebuild the basic denominator logic from
    # scratch.
    print("[analysis] Deriving analysis_calculated_grants_share_of_revenue_ratio from grants total and total revenue...", flush=True)
    grants_share_values, grants_share_provenance = _build_ratio_metric(
        analysis_df["analysis_calculated_grants_total_amount"],
        analysis_df["analysis_total_revenue_amount"],
        provenance_label="derived:analysis_calculated_grants_total_amount-divided-by-analysis_total_revenue_amount",
        require_positive_denominator=True,
    )
    analysis_df["analysis_calculated_grants_share_of_revenue_ratio"] = grants_share_values
    analysis_df["analysis_calculated_grants_share_of_revenue_ratio_source_column"] = grants_share_provenance
    analysis_df["analysis_grants_share_of_revenue_quality_flag"] = _build_grants_share_quality_flag(
        analysis_df["analysis_calculated_grants_total_amount"],
        analysis_df["analysis_total_revenue_amount"],
        analysis_df["analysis_calculated_grants_share_of_revenue_ratio"],
    )
    metric_specs_for_doc.append(
        {
            "output_column": "analysis_calculated_grants_share_of_revenue_ratio",
            "source_rule": "derived:analysis_calculated_grants_total_amount / analysis_total_revenue_amount",
            "provenance_column": "analysis_calculated_grants_share_of_revenue_ratio_source_column",
            "analysis_basis": "Percentage of grants in total revenue for the grants-versus-other-revenue analysis question.",
            "notes": "Null when total revenue is missing, zero, or negative.",
        }
    )
    metric_specs_for_doc.append(
        {
            "output_column": "analysis_grants_share_of_revenue_quality_flag",
            "source_rule": "derived:quality classification from grants total, total revenue, and grants-share ratio",
            "provenance_column": "",
            "analysis_basis": "Quality support for interpreting grants-share-of-revenue values.",
            "notes": "Values currently include valid_0_to_1, gt_1_source_anomaly, nonpositive_revenue, and missing_component.",
        }
    )

    # Net asset is calculated here as a partial GT proxy because the raw GT
    # files expose assets and liabilities for Form 990 and for part of Form
    # 990EZ, but not as one direct net-asset column. We keep the calculated
    # marker in the output name so the provenance is explicit.
    print("[analysis] Deriving analysis_calculated_net_asset_amount from GT assets and liabilities where supported...", flush=True)
    net_asset_values, net_asset_provenance = _build_calculated_net_asset(source_df, form_series)
    analysis_df["analysis_calculated_net_asset_amount"] = net_asset_values
    analysis_df["analysis_calculated_net_asset_amount_source_column"] = net_asset_provenance
    metric_specs_for_doc.append(
        {
            "output_column": "analysis_calculated_net_asset_amount",
            "source_rule": "derived:TOASEOOYY - TOLIEOOYY for 990 and supported 990EZ rows",
            "provenance_column": "analysis_calculated_net_asset_amount_source_column",
            "analysis_basis": "Net asset proxy for financial-performance analysis.",
            "notes": (
                "Partial GT calculation only. Populated for Form 990 and for the subset of Form 990EZ rows "
                "that carry both assets and liabilities. Form 990PF stays null because this GT source layer "
                "does not provide a matching liability field for a source-faithful net-asset calculation."
            ),
        }
    )

    # Months of reserves is also a calculated metric, built only where both the
    # GT net-asset proxy and the GT total-expense value are available.
    print("[analysis] Deriving analysis_calculated_months_of_reserves from calculated net assets and total expense...", flush=True)
    expense = analysis_df["analysis_total_expense_amount"]
    analysis_df["analysis_calculated_months_of_reserves"] = pd.Series(pd.NA, index=analysis_df.index, dtype="Float64")
    valid_reserve_mask = analysis_df["analysis_calculated_net_asset_amount"].notna() & expense.notna() & expense.ne(0)
    analysis_df.loc[valid_reserve_mask, "analysis_calculated_months_of_reserves"] = (
        analysis_df.loc[valid_reserve_mask, "analysis_calculated_net_asset_amount"] / expense.loc[valid_reserve_mask] * 12
    ).astype("Float64")
    analysis_df["analysis_calculated_months_of_reserves_source_column"] = pd.Series(pd.NA, index=analysis_df.index, dtype="string")
    analysis_df.loc[valid_reserve_mask, "analysis_calculated_months_of_reserves_source_column"] = (
        "derived:analysis_calculated_net_asset_amount-divided-by-analysis_total_expense_amount-times-12"
    )
    metric_specs_for_doc.append(
        {
            "output_column": "analysis_calculated_months_of_reserves",
            "source_rule": "derived:(analysis_calculated_net_asset_amount / analysis_total_expense_amount) * 12",
            "provenance_column": "analysis_calculated_months_of_reserves_source_column",
            "analysis_basis": "Months of reserves per the financial-performance method.",
            "notes": (
                "Partial GT calculation only. Populated where the GT net-asset proxy and GT total expense are both "
                "available and total expense is nonzero."
            ),
        }
    )

    # Net margin is calculated here instead of upstream because this step owns
    # the analysis-ready metric contract and must document its provenance
    # clearly. The output name intentionally includes "calculated" so analysts
    # can tell it is not a direct source variable.
    print("[analysis] Deriving analysis_calculated_net_margin_ratio from calculated surplus and total revenue...", flush=True)
    revenue = analysis_df["analysis_total_revenue_amount"]
    surplus = analysis_df["analysis_calculated_surplus_amount"]
    analysis_df["analysis_calculated_net_margin_ratio"] = pd.Series(pd.NA, index=analysis_df.index, dtype="Float64")
    valid_margin_mask = revenue.notna() & revenue.ne(0)
    analysis_df.loc[valid_margin_mask, "analysis_calculated_net_margin_ratio"] = (
        surplus.loc[valid_margin_mask] / revenue.loc[valid_margin_mask]
    ).astype("Float64")
    analysis_df["analysis_calculated_net_margin_ratio_source_column"] = pd.Series(
        pd.NA,
        index=analysis_df.index,
        dtype="string",
    )
    analysis_df.loc[valid_margin_mask, "analysis_calculated_net_margin_ratio_source_column"] = (
        "derived:analysis_calculated_surplus_amount-divided-by-analysis_total_revenue_amount"
    )
    metric_specs_for_doc.append(
        {
            "output_column": "analysis_calculated_net_margin_ratio",
            "source_rule": "derived:analysis_calculated_surplus_amount / analysis_total_revenue_amount",
            "provenance_column": "analysis_calculated_net_margin_ratio_source_column",
            "analysis_basis": "Net margin per the financial-performance method.",
            "notes": "Null when total revenue is missing or zero.",
        }
    )

    print("[analysis] Writing GT analysis parquet...", flush=True)
    analysis_df.to_parquet(output_path, index=False)
    print(f"[analysis] Output rows: {len(analysis_df):,}", flush=True)
    print(f"[analysis] Output columns: {len(analysis_df.columns):,}", flush=True)

    print("[analysis] Building region-level normalization metrics table...", flush=True)
    region_metrics_df = _build_region_metrics_output(analysis_df)
    region_metrics_df.to_parquet(region_metrics_output_path, index=False)
    print(f"[analysis] Region metric rows: {len(region_metrics_df):,}", flush=True)
    print(f"[analysis] Region metric columns: {len(region_metrics_df.columns):,}", flush=True)
    for row in region_metrics_df.itertuples(index=False):
        print(
            "[analysis]   region_metric "
            f"region={row.region} tax_year={row.tax_year} scope={row.analysis_exclusion_variant} "
            f"filings={int(row.nonprofit_filing_count):,}",
            flush=True,
        )

    coverage_rows: list[dict[str, object]] = []
    print("[analysis] Building coverage report by tax_year and form_type...", flush=True)
    grouped = source_df[["tax_year", "form_type"]].drop_duplicates().sort_values(["tax_year", "form_type"])
    metric_columns = [spec["output_column"] for spec in metric_specs_for_doc]
    for metric_column in tqdm(metric_columns, desc="coverage metrics", unit="metric"):
        for group_row in grouped.itertuples(index=False):
            mask = source_df["tax_year"].eq(group_row.tax_year) & source_df["form_type"].eq(group_row.form_type)
            total_rows = int(mask.sum())
            metric_series = analysis_df.loc[mask, metric_column]
            populated_rows = int(_series_has_value(metric_series).sum())
            coverage_rows.append(
                {
                    "variable_name": metric_column,
                    "tax_year": group_row.tax_year,
                    "form_type": group_row.form_type,
                    "total_rows": total_rows,
                    "populated_rows": populated_rows,
                    "fill_rate": round(populated_rows / total_rows, 6) if total_rows else None,
                    "status": "available",
                    "notes": "",
                }
            )
    for concept in UNRESOLVED_CONCEPTS:
        coverage_rows.append(
            {
                "variable_name": concept["concept"],
                "tax_year": "",
                "form_type": "",
                "total_rows": len(source_df),
                "populated_rows": "",
                "fill_rate": "",
                "status": concept["status"],
                "notes": concept["reason"],
            }
        )
    coverage_df = pd.DataFrame(coverage_rows)
    coverage_df.to_csv(coverage_path, index=False)

    print("[analysis] Fill rates by variable family:", flush=True)
    for metric_column in metric_columns:
        populated_rows = int(_series_has_value(analysis_df[metric_column]).sum())
        fill_rate = populated_rows / len(analysis_df) if len(analysis_df) else 0.0
        print(
            f"[analysis]   {metric_column}: populated_rows={populated_rows:,} fill_rate={fill_rate:.3%}",
            flush=True,
        )
    print("[analysis] NTEE coverage by tax_year:", flush=True)
    for tax_year in required_tax_years:
        tax_year_mask = source_df["tax_year"].eq(tax_year)
        matched_rows = int(_series_has_value(analysis_df.loc[tax_year_mask, "analysis_ntee_code"]).sum())
        total_rows = int(tax_year_mask.sum())
        print(
            f"[analysis]   tax_year={tax_year} ntee_matches={matched_rows:,} total_rows={total_rows:,}",
            flush=True,
        )
    print(f"[analysis] Unresolved concept count: {len(UNRESOLVED_CONCEPTS)}", flush=True)

    print("[analysis] Writing variable mapping Markdown...", flush=True)
    _write_mapping_markdown(
        mapping_path=mapping_path,
        source_path=input_path,
        coverage_path=coverage_path,
        metric_specs=metric_specs_for_doc,
        unresolved_concepts=UNRESOLVED_CONCEPTS,
    )
    print(f"[analysis] Wrote coverage CSV: {coverage_path}", flush=True)
    print(f"[analysis] Wrote mapping Markdown: {mapping_path}", flush=True)
    print_elapsed(start, "Step 13")


if __name__ == "__main__":
    main()
