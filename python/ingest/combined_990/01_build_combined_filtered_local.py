"""
Step 01: Build the combined filtered 990 union + master tables locally.
"""

from __future__ import annotations

import argparse
import json
import time
from pathlib import Path
from typing import Any

import pandas as pd
from tqdm import tqdm

from common import (
    BUILD_SUMMARY_JSON,
    COLUMN_DICTIONARY_CSV,
    COMBINED_SCHEMA_VERSION,
    DIAG_OVERLAP_BY_EIN_CSV,
    DIAG_OVERLAP_BY_EIN_TAX_YEAR_CSV,
    DIAG_OVERLAP_SUMMARY_CSV,
    FIELD_AVAILABILITY_MATRIX_CSV,
    GEOID_REFERENCE_CSV,
    HARMONIZED_COLUMNS,
    MASTER_COLUMN_DICTIONARY_CSV,
    MASTER_CONFLICT_DETAIL_CSV,
    MASTER_CONFLICT_DETAIL_PARQUET,
    MASTER_CONFLICT_SUMMARY_CSV,
    MASTER_FIELD_SELECTION_SUMMARY_CSV,
    MASTER_OUTPUT_PARQUET,
    NUMERIC_HELPER_COLUMNS,
    OUTPUT_PARQUET,
    OVERLAY_FIELDS,
    ROW_PROVENANCE_COLUMNS,
    SOURCE_INPUT_MANIFEST_CSV,
    SOURCE_PREFIXES,
    STAGING_DIR,
    banner,
    blank_to_empty,
    build_input_signature,
    column_group,
    discover_bmf_exact_year_lookup_inputs,
    discover_source_inputs,
    ensure_work_dirs,
    inputs_match_cached_build,
    load_env_from_secrets,
    normalize_ein9,
    normalize_tax_year,
    normalize_zip5,
    parse_numeric_string,
    print_elapsed,
    source_family_time_basis,
    write_csv,
    write_json,
    write_parquet_with_metadata,
)

PROVENANCE_COLUMNS = [
    f"{field}__source_family" for field in HARMONIZED_COLUMNS
] + [
    f"{field}__source_variant" for field in HARMONIZED_COLUMNS
] + [
    f"{field}__source_column" for field in HARMONIZED_COLUMNS
]

OVERLAY_OUTPUT_COLUMNS = [
    "org_match_quality",
    "bmf_overlay_applied",
    "bmf_overlay_field_count",
    "bmf_overlay_fields",
]

MASTER_GROUP_KEYS = ["harm_ein", "harm_tax_year"]
MASTER_SOURCE_TIME_BASIS_COLUMNS = [f"{field}__source_time_basis" for field in HARMONIZED_COLUMNS]
MASTER_SELECTED_ROW_COLUMNS = [
    column
    for field in HARMONIZED_COLUMNS
    for column in (
        f"{field}__selected_row_source_family",
        f"{field}__selected_row_source_variant",
        f"{field}__selected_row_time_basis",
        f"{field}__selected_row_tax_year",
        f"{field}__source_tax_year",
        f"{field}__source_year_offset",
    )
]
MASTER_GROUP_COLUMNS = [
    "master_group_row_count",
    "master_source_family_count",
    "master_source_families",
    "master_has_givingtuesday_datamart",
    "master_has_nccs_postcard",
    "master_has_nccs_efile",
    "master_has_nccs_bmf",
]
MASTER_CONFLICT_COLUMNS = [
    column
    for field in HARMONIZED_COLUMNS
    for column in (f"{field}__conflict", f"{field}__distinct_nonblank_value_count")
]
FINANCIAL_FIELDS = [
    "harm_revenue_amount",
    "harm_expenses_amount",
    "harm_assets_amount",
    "harm_income_amount",
]
IDENTITY_FIELDS = [
    "harm_org_name",
    "harm_state",
    "harm_zip5",
    "harm_county_fips",
    "harm_region",
]
CLASSIFICATION_FIELDS = [
    "harm_ntee_code",
    "harm_subsection_code",
    "harm_is_hospital",
    "harm_is_university",
]
MASTER_FINANCIAL_SEMANTIC_COLUMNS = [
    column
    for field in FINANCIAL_FIELDS
    for column in (f"{field}__is_reference_snapshot_value", f"{field}__is_filing_value")
]
MASTER_SOURCE_SUMMARY_COLUMNS = [
    "master_identity_selected_sources",
    "master_financial_selected_sources",
    "master_classification_selected_sources",
    "master_primary_identity_source",
    "master_primary_financial_source",
    "master_primary_classification_source",
    "master_has_any_selected_financial_values",
    "master_has_reference_selected_values",
    "master_has_filing_selected_values",
    "master_record_type",
    "master_has_conflicts",
    "master_conflict_field_count",
    "master_conflict_fields",
]
DEFAULT_MASTER_SOURCE_PRIORITY = [
    "givingtuesday_datamart",
    "nccs_postcard",
    "nccs_efile",
    "nccs_bmf",
]
MASTER_FIELD_SOURCE_PRIORITY = {
    "harm_org_name": ["givingtuesday_datamart", "nccs_postcard", "nccs_efile", "nccs_bmf"],
    "harm_state": ["givingtuesday_datamart", "nccs_postcard", "nccs_efile", "nccs_bmf"],
    "harm_zip5": ["givingtuesday_datamart", "nccs_postcard", "nccs_efile", "nccs_bmf"],
    "harm_county_fips": ["givingtuesday_datamart", "nccs_postcard", "nccs_efile", "nccs_bmf"],
    "harm_region": ["givingtuesday_datamart", "nccs_postcard", "nccs_efile", "nccs_bmf"],
    "harm_ntee_code": ["nccs_bmf"],
    "harm_subsection_code": ["nccs_efile", "nccs_bmf"],
    "harm_filing_form": ["givingtuesday_datamart", "nccs_postcard", "nccs_efile"],
    "harm_revenue_amount": ["givingtuesday_datamart", "nccs_efile", "nccs_bmf"],
    "harm_expenses_amount": ["givingtuesday_datamart", "nccs_efile"],
    "harm_assets_amount": ["givingtuesday_datamart", "nccs_efile", "nccs_bmf"],
    "harm_income_amount": ["givingtuesday_datamart", "nccs_efile", "nccs_bmf"],
    "harm_gross_receipts_under_25000_flag": ["nccs_postcard"],
    "harm_is_hospital": ["givingtuesday_datamart", "nccs_efile"],
    "harm_is_university": ["givingtuesday_datamart", "nccs_efile"],
}
MASTER_DOMAIN_PRIORITY = {
    "identity": ["givingtuesday_datamart", "nccs_postcard", "nccs_efile", "nccs_bmf"],
    "financial": ["givingtuesday_datamart", "nccs_efile", "nccs_bmf"],
    "classification": ["nccs_efile", "nccs_bmf", "givingtuesday_datamart"],
}
MASTER_DOMAIN_FIELDS = {
    "identity": IDENTITY_FIELDS,
    "financial": FINANCIAL_FIELDS,
    "classification": CLASSIFICATION_FIELDS,
}


def _read_source_dataframe(path: Path, input_format: str) -> pd.DataFrame:
    """Read one filtered source file into a DataFrame."""
    if input_format == "parquet":
        return pd.read_parquet(path)
    return pd.read_csv(path, dtype=str, low_memory=False)


def _stringify_frame(df: pd.DataFrame) -> pd.DataFrame:
    """Convert a source frame to string-preserving values with blanks for nulls."""
    out = df.copy()
    for column in out.columns:
        out[column] = out[column].map(blank_to_empty)
    return out


def _ensure_base_columns(out: pd.DataFrame) -> pd.DataFrame:
    """Ensure the shared combined-schema columns exist before native columns are added."""
    for column in ROW_PROVENANCE_COLUMNS + HARMONIZED_COLUMNS + PROVENANCE_COLUMNS + OVERLAY_OUTPUT_COLUMNS:
        if column not in out.columns:
            out[column] = ""
    return out


def _set_harmonized(
    out: pd.DataFrame,
    *,
    field: str,
    values: pd.Series,
    source_family: str,
    source_variant: str,
    source_columns: str | pd.Series,
) -> None:
    """Set one harmonized field and its provenance triplet."""
    value_series = values.map(blank_to_empty)
    if isinstance(source_columns, pd.Series):
        source_column_series = source_columns.map(blank_to_empty)
    else:
        source_column_series = pd.Series([blank_to_empty(source_columns)] * len(out), index=out.index)
    nonblank = value_series.str.strip().ne("")
    out[field] = value_series
    out[f"{field}__source_family"] = nonblank.map(lambda flag: source_family if flag else "")
    out[f"{field}__source_variant"] = nonblank.map(lambda flag: source_variant if flag else "")
    out[f"{field}__source_column"] = source_column_series.where(nonblank, "")


def _source_native_frame(df: pd.DataFrame, prefix: str) -> pd.DataFrame:
    """Return the source-native columns with the required family prefix."""
    out = _stringify_frame(df)
    out.columns = [f"{prefix}{column}" for column in out.columns]
    return out


def _row_numbers(df: pd.DataFrame) -> pd.Series:
    """Return 1-based row numbers for one source file."""
    return pd.Series(range(1, len(df) + 1), index=df.index)


def _normalize_boolean_flags(series: pd.Series) -> pd.Series:
    """
    Normalize source-specific boolean-ish flags to lowercase `true` / `false`.

    These two harmonized flag fields are intentionally stricter than other
    harmonized strings because downstream analysis needs a stable cross-source
    boolean convention. Unexpected nonblank tokens are treated as missing.
    """
    truthy = {"1", "y", "yes", "true", "t", "x"}
    falsey = {"0", "n", "no", "false", "f"}

    def _normalize_one(value: Any) -> str:
        raw = blank_to_empty(value).strip()
        if not raw:
            return ""
        lowered = raw.lower()
        if lowered in truthy:
            return "true"
        if lowered in falsey:
            return "false"
        return ""

    return series.map(_normalize_one)


def _pick_gt_value(df: pd.DataFrame, primary: str, fallback: str) -> tuple[pd.Series, pd.Series]:
    """Choose the first nonblank value between two GivingTuesday columns and track provenance."""
    primary_values = df[primary].map(blank_to_empty)
    fallback_values = df[fallback].map(blank_to_empty)
    use_primary = primary_values.str.strip().ne("")
    values = primary_values.where(use_primary, fallback_values)
    provenance = pd.Series([primary] * len(df), index=df.index).where(use_primary, fallback)
    provenance = provenance.where(values.str.strip().ne(""), "")
    return values, provenance


def _gt_column_values(df: pd.DataFrame, column_name: str) -> pd.Series:
    """Return one GT column as blank-normalized strings, tolerating older fixtures without the column."""
    if column_name not in df.columns:
        return pd.Series([""] * len(df), index=df.index)
    return df[column_name].map(blank_to_empty)


def _transform_givingtuesday(source, df: pd.DataFrame) -> tuple[pd.DataFrame, dict[str, Any]]:
    """Normalize the GivingTuesday benchmark parquet into the source-union schema."""
    out = pd.DataFrame(index=df.index)
    out = _ensure_base_columns(out)
    out["row_source_family"] = source.source_family
    out["row_source_variant"] = source.source_variant
    out["row_source_file_name"] = source.input_path.name
    out["row_source_file_path"] = str(source.input_path)
    out["row_source_row_number"] = _row_numbers(df)
    out["row_source_time_basis"] = "tax_year"
    out["row_source_snapshot_year"] = ""
    out["row_source_snapshot_month"] = ""

    _set_harmonized(out, field="harm_ein", values=df["ein"].map(normalize_ein9), source_family=source.source_family, source_variant=source.source_variant, source_columns="ein")
    _set_harmonized(out, field="harm_tax_year", values=df["tax_year"].map(normalize_tax_year), source_family=source.source_family, source_variant=source.source_variant, source_columns="tax_year")
    _set_harmonized(out, field="harm_filing_form", values=df["form_type"].map(blank_to_empty), source_family=source.source_family, source_variant=source.source_variant, source_columns="form_type")
    _set_harmonized(out, field="harm_org_name", values=df["FILERNAME1"].map(blank_to_empty), source_family=source.source_family, source_variant=source.source_variant, source_columns="FILERNAME1")
    _set_harmonized(out, field="harm_state", values=df["FILERUSSTATE"].map(blank_to_empty), source_family=source.source_family, source_variant=source.source_variant, source_columns="FILERUSSTATE")
    zip_values, zip_source = _pick_gt_value(df, "zip5", "FILERUSZIP")
    _set_harmonized(out, field="harm_zip5", values=zip_values.map(normalize_zip5), source_family=source.source_family, source_variant=source.source_variant, source_columns=zip_source)
    _set_harmonized(out, field="harm_county_fips", values=df["county_fips"].map(blank_to_empty), source_family=source.source_family, source_variant=source.source_variant, source_columns="county_fips")
    _set_harmonized(out, field="harm_region", values=df["region"].map(blank_to_empty), source_family=source.source_family, source_variant=source.source_variant, source_columns="region")

    form_type = df["form_type"].map(blank_to_empty)
    # Form 990PF uses PF-specific GT standard-field names for total revenue
    # and total expense. Keep the union harmonization aligned with the GT
    # analysis layer so PF rows are not silently blanked by the 990 mappings.
    revenue_values = (
        _gt_column_values(df, "TOTALRREVENU")
        .where(form_type.eq("990EZ"), _gt_column_values(df, "TOTREVCURYEA"))
        .where(~form_type.eq("990PF"), _gt_column_values(df, "ANREEXTOREEX"))
    )
    revenue_source = (
        pd.Series(["TOTALRREVENU"] * len(df), index=df.index)
        .where(form_type.eq("990EZ"), "TOTREVCURYEA")
        .where(~form_type.eq("990PF"), "ANREEXTOREEX")
    )
    revenue_source = revenue_source.where(revenue_values.str.strip().ne(""), "")
    _set_harmonized(out, field="harm_revenue_amount", values=revenue_values, source_family=source.source_family, source_variant=source.source_variant, source_columns=revenue_source)

    expense_values = (
        _gt_column_values(df, "TOTALEEXPENS")
        .where(form_type.eq("990EZ"), _gt_column_values(df, "TOTEXPCURYEA"))
        .where(~form_type.eq("990PF"), _gt_column_values(df, "ARETEREXPNSS"))
    )
    expense_source = (
        pd.Series(["TOTALEEXPENS"] * len(df), index=df.index)
        .where(form_type.eq("990EZ"), "TOTEXPCURYEA")
        .where(~form_type.eq("990PF"), "ARETEREXPNSS")
    )
    expense_source = expense_source.where(expense_values.str.strip().ne(""), "")
    _set_harmonized(out, field="harm_expenses_amount", values=expense_values, source_family=source.source_family, source_variant=source.source_variant, source_columns=expense_source)

    asset_values = df["TOASEOOYY"].map(blank_to_empty).where(df["form_type"].map(blank_to_empty).ne("990PF"), df["ASSEOYOYY"].map(blank_to_empty))
    asset_source = pd.Series(["TOASEOOYY"] * len(df), index=df.index).where(df["form_type"].map(blank_to_empty).ne("990PF"), "ASSEOYOYY")
    asset_source = asset_source.where(asset_values.str.strip().ne(""), "")
    _set_harmonized(out, field="harm_assets_amount", values=asset_values, source_family=source.source_family, source_variant=source.source_variant, source_columns=asset_source)

    _set_harmonized(
        out,
        field="harm_income_amount",
        values=df["derived_income_amount"].map(blank_to_empty),
        source_family=source.source_family,
        source_variant=source.source_variant,
        source_columns=df["derived_income_derivation"].map(blank_to_empty),
    )
    _set_harmonized(out, field="harm_gross_receipts_under_25000_flag", values=pd.Series([""] * len(df), index=df.index), source_family=source.source_family, source_variant=source.source_variant, source_columns="")
    _set_harmonized(out, field="harm_is_hospital", values=_normalize_boolean_flags(df["OPERATHOSPIT"]), source_family=source.source_family, source_variant=source.source_variant, source_columns="OPERATHOSPIT")
    _set_harmonized(out, field="harm_is_university", values=_normalize_boolean_flags(df["OPERATSCHOOL"]), source_family=source.source_family, source_variant=source.source_variant, source_columns="OPERATSCHOOL")

    out["org_match_quality"] = out["harm_ein"].map(lambda value: "exact_source_ein" if blank_to_empty(value).strip() else "missing_ein")
    native = _source_native_frame(df, source.native_prefix)
    final = pd.concat([out, native], axis=1)
    return final, {
        **source.to_manifest_dict(),
        "input_rows": len(df),
        "output_rows_written": len(final),
        "min_tax_year": out["harm_tax_year"].replace("", pd.NA).dropna().min() if len(out) else "",
        "max_tax_year": out["harm_tax_year"].replace("", pd.NA).dropna().max() if len(out) else "",
        "load_status": "ok",
    }


def _transform_postcard(source, df: pd.DataFrame) -> tuple[pd.DataFrame, dict[str, Any]]:
    """Normalize the NCCS postcard filtered CSV into the source-union schema."""
    out = pd.DataFrame(index=df.index)
    out = _ensure_base_columns(out)
    out["row_source_family"] = source.source_family
    out["row_source_variant"] = source.source_variant
    out["row_source_file_name"] = source.input_path.name
    out["row_source_file_path"] = str(source.input_path)
    out["row_source_row_number"] = _row_numbers(df)
    out["row_source_time_basis"] = "tax_year_in_snapshot"
    out["row_source_snapshot_year"] = df["snapshot_year"].map(blank_to_empty)
    out["row_source_snapshot_month"] = df["snapshot_month"].map(blank_to_empty)

    _set_harmonized(out, field="harm_ein", values=df["ein"].map(normalize_ein9), source_family=source.source_family, source_variant=source.source_variant, source_columns="ein")
    _set_harmonized(out, field="harm_tax_year", values=df["tax_year"].map(normalize_tax_year), source_family=source.source_family, source_variant=source.source_variant, source_columns="tax_year")
    _set_harmonized(out, field="harm_filing_form", values=pd.Series(["990N"] * len(df), index=df.index), source_family=source.source_family, source_variant=source.source_variant, source_columns="literal:990N")
    _set_harmonized(out, field="harm_org_name", values=df["legal_name"].map(blank_to_empty), source_family=source.source_family, source_variant=source.source_variant, source_columns="legal_name")

    # The postcard benchmark filter can match a row into scope by either the
    # organization ZIP or the officer ZIP. To keep the selected state/ZIP
    # consistent with the benchmark county assignment, the harmonized address
    # fields must follow the same address basis that won the benchmark match.
    #
    # Example: when a row was admitted through `officer_zip`, carrying forward
    # `organization_zip` would make the union/master appear to have a mailing
    # address outside the benchmark county even though the in-scope match was
    # correct. We therefore prefer the benchmark-winning source first and only
    # fall back to the older "organization else officer" behavior if the
    # benchmark-match metadata is absent.
    benchmark_match_source = df.get(
        "benchmark_match_source",
        pd.Series([""] * len(df), index=df.index),
    ).map(blank_to_empty)
    use_officer_address = benchmark_match_source.str.strip().eq("officer_zip")

    state_primary = df["organization_state"].map(blank_to_empty)
    state_fallback = df["officer_state"].map(blank_to_empty)
    state_values = state_primary.where(~use_officer_address, state_fallback)
    fallback_state_values = state_primary.where(state_primary.str.strip().ne(""), state_fallback)
    state_values = state_values.where(benchmark_match_source.str.strip().ne(""), fallback_state_values)
    state_source = pd.Series(["organization_state"] * len(df), index=df.index).where(~use_officer_address, "officer_state")
    fallback_state_source = pd.Series(["organization_state"] * len(df), index=df.index).where(state_primary.str.strip().ne(""), "officer_state")
    state_source = state_source.where(benchmark_match_source.str.strip().ne(""), fallback_state_source)
    state_source = state_source.where(state_values.str.strip().ne(""), "")
    _set_harmonized(out, field="harm_state", values=state_values, source_family=source.source_family, source_variant=source.source_variant, source_columns=state_source)

    zip_primary = df["organization_zip"].map(normalize_zip5)
    zip_fallback = df["officer_zip"].map(normalize_zip5)
    zip_values = zip_primary.where(~use_officer_address, zip_fallback)
    fallback_zip_values = zip_primary.where(zip_primary.str.strip().ne(""), zip_fallback)
    zip_values = zip_values.where(benchmark_match_source.str.strip().ne(""), fallback_zip_values)
    zip_source = pd.Series(["organization_zip"] * len(df), index=df.index).where(~use_officer_address, "officer_zip")
    fallback_zip_source = pd.Series(["organization_zip"] * len(df), index=df.index).where(zip_primary.str.strip().ne(""), "officer_zip")
    zip_source = zip_source.where(benchmark_match_source.str.strip().ne(""), fallback_zip_source)
    zip_source = zip_source.where(zip_values.str.strip().ne(""), "")
    _set_harmonized(out, field="harm_zip5", values=zip_values, source_family=source.source_family, source_variant=source.source_variant, source_columns=zip_source)

    _set_harmonized(out, field="harm_county_fips", values=df["county_fips"].map(blank_to_empty), source_family=source.source_family, source_variant=source.source_variant, source_columns="county_fips")
    _set_harmonized(out, field="harm_region", values=df["region"].map(blank_to_empty), source_family=source.source_family, source_variant=source.source_variant, source_columns="region")
    _set_harmonized(out, field="harm_ntee_code", values=pd.Series([""] * len(df), index=df.index), source_family=source.source_family, source_variant=source.source_variant, source_columns="")
    _set_harmonized(out, field="harm_subsection_code", values=pd.Series([""] * len(df), index=df.index), source_family=source.source_family, source_variant=source.source_variant, source_columns="")
    _set_harmonized(out, field="harm_revenue_amount", values=pd.Series([""] * len(df), index=df.index), source_family=source.source_family, source_variant=source.source_variant, source_columns="")
    _set_harmonized(out, field="harm_expenses_amount", values=pd.Series([""] * len(df), index=df.index), source_family=source.source_family, source_variant=source.source_variant, source_columns="")
    _set_harmonized(out, field="harm_assets_amount", values=pd.Series([""] * len(df), index=df.index), source_family=source.source_family, source_variant=source.source_variant, source_columns="")
    _set_harmonized(out, field="harm_income_amount", values=pd.Series([""] * len(df), index=df.index), source_family=source.source_family, source_variant=source.source_variant, source_columns="")
    # Postcard gross-receipts flags are boolean-like source tokens, so normalize
    # them to the same lowercase true/false convention used for the other
    # harmonized boolean fields.
    _set_harmonized(
        out,
        field="harm_gross_receipts_under_25000_flag",
        values=_normalize_boolean_flags(df["gross_receipts_under_25000"]),
        source_family=source.source_family,
        source_variant=source.source_variant,
        source_columns="gross_receipts_under_25000",
    )
    _set_harmonized(out, field="harm_is_hospital", values=pd.Series([""] * len(df), index=df.index), source_family=source.source_family, source_variant=source.source_variant, source_columns="")
    _set_harmonized(out, field="harm_is_university", values=pd.Series([""] * len(df), index=df.index), source_family=source.source_family, source_variant=source.source_variant, source_columns="")

    out["org_match_quality"] = out["harm_ein"].map(lambda value: "exact_source_ein" if blank_to_empty(value).strip() else "missing_ein")
    native = _source_native_frame(df, source.native_prefix)
    final = pd.concat([out, native], axis=1)
    return final, {
        **source.to_manifest_dict(),
        "input_rows": len(df),
        "output_rows_written": len(final),
        "min_tax_year": out["harm_tax_year"].replace("", pd.NA).dropna().min() if len(out) else "",
        "max_tax_year": out["harm_tax_year"].replace("", pd.NA).dropna().max() if len(out) else "",
        "load_status": "ok",
    }


def _transform_core(source, df: pd.DataFrame) -> tuple[pd.DataFrame, dict[str, Any]]:
    """Normalize one NCCS Core filtered CSV into the source-union schema."""
    out = pd.DataFrame(index=df.index)
    out = _ensure_base_columns(out)
    out["row_source_family"] = source.source_family
    out["row_source_variant"] = source.source_variant
    out["row_source_file_name"] = source.input_path.name
    out["row_source_file_path"] = str(source.input_path)
    out["row_source_row_number"] = _row_numbers(df)
    out["row_source_time_basis"] = "tax_year"
    out["row_source_snapshot_year"] = ""
    out["row_source_snapshot_month"] = ""

    ein_primary = df["EIN2"].map(normalize_ein9)
    ein_fallback = df["F9_00_ORG_EIN"].map(normalize_ein9)
    ein_values = ein_primary.where(ein_primary.str.strip().ne(""), ein_fallback)
    ein_source = pd.Series(["EIN2"] * len(df), index=df.index).where(ein_primary.str.strip().ne(""), "F9_00_ORG_EIN")
    ein_source = ein_source.where(ein_values.str.strip().ne(""), "")
    _set_harmonized(out, field="harm_ein", values=ein_values, source_family=source.source_family, source_variant=source.source_variant, source_columns=ein_source)
    _set_harmonized(out, field="harm_tax_year", values=df["TAX_YEAR"].map(normalize_tax_year), source_family=source.source_family, source_variant=source.source_variant, source_columns="TAX_YEAR")
    _set_harmonized(out, field="harm_filing_form", values=pd.Series([""] * len(df), index=df.index), source_family=source.source_family, source_variant=source.source_variant, source_columns="")
    _set_harmonized(out, field="harm_org_name", values=pd.Series([""] * len(df), index=df.index), source_family=source.source_family, source_variant=source.source_variant, source_columns="")
    _set_harmonized(out, field="harm_state", values=pd.Series([""] * len(df), index=df.index), source_family=source.source_family, source_variant=source.source_variant, source_columns="")
    _set_harmonized(out, field="harm_zip5", values=pd.Series([""] * len(df), index=df.index), source_family=source.source_family, source_variant=source.source_variant, source_columns="")
    _set_harmonized(out, field="harm_county_fips", values=df["county_fips"].map(blank_to_empty), source_family=source.source_family, source_variant=source.source_variant, source_columns="county_fips")
    _set_harmonized(out, field="harm_region", values=df["region"].map(blank_to_empty), source_family=source.source_family, source_variant=source.source_variant, source_columns="region")
    _set_harmonized(out, field="harm_ntee_code", values=pd.Series([""] * len(df), index=df.index), source_family=source.source_family, source_variant=source.source_variant, source_columns="")
    _set_harmonized(out, field="harm_subsection_code", values=df["BMF_SUBSECTION_CODE"].map(blank_to_empty), source_family=source.source_family, source_variant=source.source_variant, source_columns="BMF_SUBSECTION_CODE")
    _set_harmonized(out, field="harm_revenue_amount", values=df["F9_08_REV_TOT_TOT"].map(blank_to_empty), source_family=source.source_family, source_variant=source.source_variant, source_columns="F9_08_REV_TOT_TOT")
    _set_harmonized(out, field="harm_expenses_amount", values=df["F9_09_EXP_TOT_TOT"].map(blank_to_empty), source_family=source.source_family, source_variant=source.source_variant, source_columns="F9_09_EXP_TOT_TOT")
    _set_harmonized(out, field="harm_assets_amount", values=df["F9_10_ASSET_TOT_EOY"].map(blank_to_empty), source_family=source.source_family, source_variant=source.source_variant, source_columns="F9_10_ASSET_TOT_EOY")
    _set_harmonized(out, field="harm_income_amount", values=pd.Series([""] * len(df), index=df.index), source_family=source.source_family, source_variant=source.source_variant, source_columns="")
    _set_harmonized(out, field="harm_gross_receipts_under_25000_flag", values=pd.Series([""] * len(df), index=df.index), source_family=source.source_family, source_variant=source.source_variant, source_columns="")
    _set_harmonized(out, field="harm_is_hospital", values=_normalize_boolean_flags(df["F9_04_HOSPITAL_X"]), source_family=source.source_family, source_variant=source.source_variant, source_columns="F9_04_HOSPITAL_X")
    _set_harmonized(out, field="harm_is_university", values=_normalize_boolean_flags(df["F9_04_SCHOOL_X"]), source_family=source.source_family, source_variant=source.source_variant, source_columns="F9_04_SCHOOL_X")

    out["org_match_quality"] = out["harm_ein"].map(lambda value: "normalized_core_ein2" if blank_to_empty(value).strip() else "missing_ein")
    native = _source_native_frame(df, source.native_prefix)
    final = pd.concat([out, native], axis=1)
    return final, {
        **source.to_manifest_dict(),
        "input_rows": len(df),
        "output_rows_written": len(final),
        "min_tax_year": out["harm_tax_year"].replace("", pd.NA).dropna().min() if len(out) else "",
        "max_tax_year": out["harm_tax_year"].replace("", pd.NA).dropna().max() if len(out) else "",
        "load_status": "ok",
    }


def _transform_efile(source, df: pd.DataFrame) -> tuple[pd.DataFrame, dict[str, Any]]:
    """Normalize one annualized NCCS efile benchmark parquet into the source-union schema."""
    out = pd.DataFrame(index=df.index)
    out = _ensure_base_columns(out)
    out["row_source_family"] = source.source_family
    out["row_source_variant"] = source.source_variant
    out["row_source_file_name"] = source.input_path.name
    out["row_source_file_path"] = str(source.input_path)
    out["row_source_row_number"] = _row_numbers(df)
    out["row_source_time_basis"] = "tax_year"
    out["row_source_snapshot_year"] = ""
    out["row_source_snapshot_month"] = ""

    _set_harmonized(out, field="harm_ein", values=df["harm_ein"].map(normalize_ein9), source_family=source.source_family, source_variant=source.source_variant, source_columns="F9_00_ORG_EIN")
    _set_harmonized(out, field="harm_tax_year", values=df["harm_tax_year"].map(normalize_tax_year), source_family=source.source_family, source_variant=source.source_variant, source_columns="F9_00_TAX_YEAR")
    _set_harmonized(out, field="harm_filing_form", values=df["harm_filing_form"].map(blank_to_empty), source_family=source.source_family, source_variant=source.source_variant, source_columns="F9_00_RETURN_TYPE")
    _set_harmonized(out, field="harm_org_name", values=df["harm_org_name"].map(blank_to_empty), source_family=source.source_family, source_variant=source.source_variant, source_columns="F9_00_ORG_NAME_L1")
    _set_harmonized(out, field="harm_state", values=df["harm_state"].map(blank_to_empty), source_family=source.source_family, source_variant=source.source_variant, source_columns="F9_00_ORG_ADDR_STATE")
    _set_harmonized(out, field="harm_zip5", values=df["harm_zip5"].map(normalize_zip5), source_family=source.source_family, source_variant=source.source_variant, source_columns="F9_00_ORG_ADDR_ZIP")
    _set_harmonized(out, field="harm_county_fips", values=df["harm_county_fips"].map(blank_to_empty), source_family=source.source_family, source_variant=source.source_variant, source_columns="county_fips")
    _set_harmonized(out, field="harm_region", values=df["harm_region"].map(blank_to_empty), source_family=source.source_family, source_variant=source.source_variant, source_columns="region")
    _set_harmonized(out, field="harm_ntee_code", values=pd.Series([""] * len(df), index=df.index), source_family=source.source_family, source_variant=source.source_variant, source_columns="")
    _set_harmonized(out, field="harm_subsection_code", values=pd.Series([""] * len(df), index=df.index), source_family=source.source_family, source_variant=source.source_variant, source_columns="")
    _set_harmonized(out, field="harm_revenue_amount", values=df["harm_revenue_amount"].map(blank_to_empty), source_family=source.source_family, source_variant=source.source_variant, source_columns="F9_01_REV_TOT_CY")
    _set_harmonized(out, field="harm_expenses_amount", values=df["harm_expenses_amount"].map(blank_to_empty), source_family=source.source_family, source_variant=source.source_variant, source_columns="F9_01_EXP_TOT_CY")
    _set_harmonized(out, field="harm_assets_amount", values=df["harm_assets_amount"].map(blank_to_empty), source_family=source.source_family, source_variant=source.source_variant, source_columns="F9_01_NAFB_ASSET_TOT_EOY")
    _set_harmonized(out, field="harm_income_amount", values=df["harm_income_amount"].map(blank_to_empty), source_family=source.source_family, source_variant=source.source_variant, source_columns="F9_01_EXP_REV_LESS_EXP_CY")
    _set_harmonized(out, field="harm_gross_receipts_under_25000_flag", values=pd.Series([""] * len(df), index=df.index), source_family=source.source_family, source_variant=source.source_variant, source_columns="")
    _set_harmonized(out, field="harm_is_hospital", values=df["harm_is_hospital"].map(blank_to_empty), source_family=source.source_family, source_variant=source.source_variant, source_columns="SA_01_PCSTAT_HOSPITAL_X")
    _set_harmonized(out, field="harm_is_university", values=df["harm_is_university"].map(blank_to_empty), source_family=source.source_family, source_variant=source.source_variant, source_columns="SA_01_PCSTAT_SCHOOL_X")

    out["org_match_quality"] = out["harm_ein"].map(lambda value: "exact_source_ein" if blank_to_empty(value).strip() else "missing_ein")
    native = _source_native_frame(df, source.native_prefix)
    final = pd.concat([out, native], axis=1)
    return final, {
        **source.to_manifest_dict(),
        "input_rows": len(df),
        "output_rows_written": len(final),
        "min_tax_year": out["harm_tax_year"].replace("", pd.NA).dropna().min() if len(out) else "",
        "max_tax_year": out["harm_tax_year"].replace("", pd.NA).dropna().max() if len(out) else "",
        "load_status": "ok",
    }


def _transform_bmf(source, df: pd.DataFrame) -> tuple[pd.DataFrame, dict[str, Any]]:
    """Normalize one yearly NCCS BMF filtered parquet into the source-union schema."""
    out = pd.DataFrame(index=df.index)
    out = _ensure_base_columns(out)
    out["row_source_family"] = source.source_family
    out["row_source_variant"] = source.source_variant
    out["row_source_file_name"] = source.input_path.name
    out["row_source_file_path"] = str(source.input_path)
    out["row_source_row_number"] = _row_numbers(df)
    out["row_source_time_basis"] = "reference_snapshot_year"
    out["row_source_snapshot_year"] = df["bmf_snapshot_year"].map(blank_to_empty)
    out["row_source_snapshot_month"] = df["bmf_snapshot_month"].map(blank_to_empty)

    is_legacy = "ZIP5" in df.columns
    harm_tax_year = df["bmf_snapshot_year"].map(normalize_tax_year)
    _set_harmonized(out, field="harm_ein", values=df["EIN"].map(normalize_ein9), source_family=source.source_family, source_variant=source.source_variant, source_columns="EIN")
    _set_harmonized(out, field="harm_tax_year", values=harm_tax_year, source_family=source.source_family, source_variant=source.source_variant, source_columns="bmf_snapshot_year")
    _set_harmonized(out, field="harm_filing_form", values=pd.Series([""] * len(df), index=df.index), source_family=source.source_family, source_variant=source.source_variant, source_columns="")
    _set_harmonized(out, field="harm_org_name", values=df["NAME"].map(blank_to_empty), source_family=source.source_family, source_variant=source.source_variant, source_columns="NAME")
    _set_harmonized(out, field="harm_state", values=df["STATE"].map(blank_to_empty), source_family=source.source_family, source_variant=source.source_variant, source_columns="STATE")
    zip_column = "ZIP5" if is_legacy else "ZIP"
    _set_harmonized(out, field="harm_zip5", values=df[zip_column].map(normalize_zip5), source_family=source.source_family, source_variant=source.source_variant, source_columns=zip_column)
    _set_harmonized(out, field="harm_county_fips", values=df["county_fips"].map(blank_to_empty), source_family=source.source_family, source_variant=source.source_variant, source_columns="county_fips")
    _set_harmonized(out, field="harm_region", values=df["region"].map(blank_to_empty), source_family=source.source_family, source_variant=source.source_variant, source_columns="region")

    subsection_col = "SUBSECCD" if is_legacy else "SUBSECTION"
    ntee_col = "NTEEFINAL" if is_legacy else "NTEE_CD"
    asset_col = "ASSETS" if is_legacy else "ASSET_AMT"
    income_col = "INCOME" if is_legacy else "INCOME_AMT"
    revenue_col = "CTOTREV" if is_legacy else "REVENUE_AMT"
    _set_harmonized(out, field="harm_ntee_code", values=df[ntee_col].map(blank_to_empty), source_family=source.source_family, source_variant=source.source_variant, source_columns=ntee_col)
    _set_harmonized(out, field="harm_subsection_code", values=df[subsection_col].map(blank_to_empty), source_family=source.source_family, source_variant=source.source_variant, source_columns=subsection_col)
    _set_harmonized(out, field="harm_revenue_amount", values=df[revenue_col].map(blank_to_empty), source_family=source.source_family, source_variant=source.source_variant, source_columns=revenue_col)
    _set_harmonized(out, field="harm_expenses_amount", values=pd.Series([""] * len(df), index=df.index), source_family=source.source_family, source_variant=source.source_variant, source_columns="")
    _set_harmonized(out, field="harm_assets_amount", values=df[asset_col].map(blank_to_empty), source_family=source.source_family, source_variant=source.source_variant, source_columns=asset_col)
    _set_harmonized(out, field="harm_income_amount", values=df[income_col].map(blank_to_empty), source_family=source.source_family, source_variant=source.source_variant, source_columns=income_col)
    _set_harmonized(out, field="harm_gross_receipts_under_25000_flag", values=pd.Series([""] * len(df), index=df.index), source_family=source.source_family, source_variant=source.source_variant, source_columns="")
    _set_harmonized(out, field="harm_is_hospital", values=pd.Series([""] * len(df), index=df.index), source_family=source.source_family, source_variant=source.source_variant, source_columns="")
    _set_harmonized(out, field="harm_is_university", values=pd.Series([""] * len(df), index=df.index), source_family=source.source_family, source_variant=source.source_variant, source_columns="")

    out["org_match_quality"] = out["harm_ein"].map(lambda value: "exact_source_ein" if blank_to_empty(value).strip() else "missing_ein")
    native = _source_native_frame(df, source.native_prefix)
    final = pd.concat([out, native], axis=1)
    return final, {
        **source.to_manifest_dict(),
        "input_rows": len(df),
        "output_rows_written": len(final),
        "min_tax_year": out["harm_tax_year"].replace("", pd.NA).dropna().min() if len(out) else "",
        "max_tax_year": out["harm_tax_year"].replace("", pd.NA).dropna().max() if len(out) else "",
        "load_status": "ok",
    }


def _transform_source(source, df: pd.DataFrame) -> tuple[pd.DataFrame, dict[str, Any]]:
    """Dispatch one discovered source input to the correct source-specific transformer."""
    if source.source_family == "givingtuesday_datamart":
        return _transform_givingtuesday(source, df)
    if source.source_family == "nccs_postcard":
        return _transform_postcard(source, df)
    if source.source_family == "nccs_efile":
        return _transform_efile(source, df)
    if source.source_family == "nccs_core":
        return _transform_core(source, df)
    if source.source_family == "nccs_bmf":
        return _transform_bmf(source, df)
    raise ValueError(f"Unsupported source_family: {source.source_family}")


def _load_bmf_exact_year_overlay_lookup(
    lookup_paths: dict[str, Path],
    *,
    needed_eins_by_year: dict[str, set[str]],
) -> pd.DataFrame:
    """
    Load only the exact-year BMF lookup rows that the current union build can use.

    The upstream BMF lookup artifacts are intentionally broad because they are
    exact-year enrichment tables, not benchmark-filtered row sets. Loading every
    row from every lookup parquet is correct but wasteful. The combined build
    already knows which `EIN + tax_year` keys are present in the union frame, so
    we narrow the parquet reads to those EINs before concatenating the lookup.

    We still keep a safe fallback path:
    - first try parquet predicate pushdown on `harm_ein in <needed_eins>`
    - if that fails for any engine-specific reason, fall back to reading the
      year file and filtering in-memory
    """
    required_columns = ["harm_ein", "harm_tax_year"]
    for field in OVERLAY_FIELDS:
        required_columns.extend(
            [
                field,
                f"{field}__source_family",
                f"{field}__source_variant",
                f"{field}__source_column",
            ]
        )
    frames: list[pd.DataFrame] = []
    loaded_rows = 0
    for year, path in tqdm(sorted(lookup_paths.items(), key=lambda item: int(item[0])), desc="load exact-year bmf lookups", unit="file"):
        needed_eins = sorted(needed_eins_by_year.get(year, set()))
        if not needed_eins:
            print(f"[overlay] Skipping exact-year BMF lookup for {year}: no union EINs require this year.", flush=True)
            continue

        print(
            f"[overlay] Loading exact-year BMF lookup for {year}: {path} | needed_eins={len(needed_eins):,}",
            flush=True,
        )
        try:
            frame = pd.read_parquet(
                path,
                columns=required_columns,
                filters=[("harm_ein", "in", needed_eins)],
            ).fillna("")
        except Exception as exc:
            print(
                f"[overlay] Predicate pushdown failed for {path.name}; falling back to in-memory filtering. "
                f"Reason: {exc}",
                flush=True,
            )
            frame = pd.read_parquet(path, columns=required_columns).fillna("")
            frame = frame[frame["harm_ein"].isin(needed_eins)].copy()

        print(f"[overlay] Loaded candidate lookup rows for {year}: {len(frame):,}", flush=True)
        loaded_rows += int(len(frame))
        frames.append(frame)
    if not frames:
        raise ValueError("No exact-year BMF lookup files were supplied for overlay.")
    combined_lookup = pd.concat(frames, ignore_index=True, sort=False).fillna("")
    combined_lookup = combined_lookup.drop_duplicates(subset=["harm_ein", "harm_tax_year"], keep="first")
    print(f"[overlay] Exact-year BMF lookup candidate rows loaded: {loaded_rows:,}", flush=True)
    print(f"[overlay] Exact-year BMF lookup rows after de-duplication: {len(combined_lookup):,}", flush=True)
    return combined_lookup


def _apply_bmf_overlay(df: pd.DataFrame, *, lookup_paths: dict[str, Path]) -> tuple[pd.DataFrame, dict[str, Any]]:
    """Apply the year-aware exact-year BMF overlay to non-BMF rows."""
    needed_eins_by_year: dict[str, set[str]] = {}
    candidate_rows = df.loc[
        df["row_source_family"].ne("nccs_bmf")
        & df["harm_ein"].map(blank_to_empty).str.strip().ne("")
        & df["harm_tax_year"].map(blank_to_empty).str.strip().ne(""),
        ["harm_ein", "harm_tax_year"],
    ].drop_duplicates()
    for row in candidate_rows.itertuples(index=False):
        needed_eins_by_year.setdefault(blank_to_empty(row.harm_tax_year), set()).add(blank_to_empty(row.harm_ein))
    print(
        f"[overlay] Exact-year BMF overlay candidates: {len(candidate_rows):,} unique EIN+tax_year keys across "
        f"{len(needed_eins_by_year):,} tax years.",
        flush=True,
    )
    bmf_lookup = _load_bmf_exact_year_overlay_lookup(lookup_paths, needed_eins_by_year=needed_eins_by_year)
    renamed_lookup = bmf_lookup.rename(columns={column: f"{column}__bmf_lookup" for column in bmf_lookup.columns if column not in ("harm_ein", "harm_tax_year")})

    merged = df.merge(renamed_lookup, on=["harm_ein", "harm_tax_year"], how="left")
    overlay_flags: dict[str, pd.Series] = {}
    non_bmf = merged["row_source_family"].ne("nccs_bmf")
    for field in OVERLAY_FIELDS:
        lookup_value_col = f"{field}__bmf_lookup"
        lookup_family_col = f"{field}__source_family__bmf_lookup"
        lookup_variant_col = f"{field}__source_variant__bmf_lookup"
        lookup_column_col = f"{field}__source_column__bmf_lookup"
        cond = (
            non_bmf
            & merged[field].map(blank_to_empty).str.strip().eq("")
            & merged[lookup_value_col].map(blank_to_empty).str.strip().ne("")
        )
        merged[field] = merged[field].where(~cond, merged[lookup_value_col])
        merged[f"{field}__source_family"] = merged[f"{field}__source_family"].where(~cond, merged[lookup_family_col])
        merged[f"{field}__source_variant"] = merged[f"{field}__source_variant"].where(~cond, merged[lookup_variant_col])
        merged[f"{field}__source_column"] = merged[f"{field}__source_column"].where(~cond, merged[lookup_column_col])
        overlay_flags[field] = cond

    merged["bmf_overlay_applied"] = pd.Series(False, index=merged.index)
    merged["bmf_overlay_field_count"] = 0
    for field, cond in overlay_flags.items():
        merged["bmf_overlay_applied"] = merged["bmf_overlay_applied"] | cond
        merged["bmf_overlay_field_count"] = merged["bmf_overlay_field_count"] + cond.astype(int)
    merged["bmf_overlay_fields"] = pd.Series("", index=merged.index)
    if overlay_flags:
        merged["bmf_overlay_fields"] = pd.Series(
            [
                ";".join(field for field, cond in overlay_flags.items() if bool(cond.iloc[idx]))
                for idx in range(len(merged))
            ],
            index=merged.index,
        )

    drop_columns = [column for column in merged.columns if column.endswith("__bmf_lookup")]
    overlay_stats = {
        "bmf_exact_lookup_file_count": len(lookup_paths),
        "bmf_exact_lookup_row_count": int(len(bmf_lookup)),
        "bmf_overlay_row_count": int(merged["bmf_overlay_applied"].sum()),
        "bmf_overlay_field_fill_counts": {field: int(cond.sum()) for field, cond in overlay_flags.items()},
    }
    return merged.drop(columns=drop_columns), overlay_stats


def _apply_numeric_helpers(df: pd.DataFrame) -> pd.DataFrame:
    """Add parsed numeric helper columns for the top-line harmonized financial fields."""
    mapping = {
        "harm_revenue_amount": ("harm_revenue_amount_num", "harm_revenue_amount_num_parse_ok"),
        "harm_expenses_amount": ("harm_expenses_amount_num", "harm_expenses_amount_num_parse_ok"),
        "harm_assets_amount": ("harm_assets_amount_num", "harm_assets_amount_num_parse_ok"),
        "harm_income_amount": ("harm_income_amount_num", "harm_income_amount_num_parse_ok"),
    }
    new_columns: dict[str, pd.Series] = {}
    for source_column, (value_column, ok_column) in mapping.items():
        parsed = df[source_column].map(parse_numeric_string)
        new_columns[value_column] = parsed.map(lambda item: item[0])
        new_columns[ok_column] = parsed.map(lambda item: item[1])
    return df.assign(**new_columns)


def _field_priority(field: str) -> list[str]:
    """Return the source-family precedence order for one master-file harmonized field."""
    return MASTER_FIELD_SOURCE_PRIORITY.get(field, DEFAULT_MASTER_SOURCE_PRIORITY)


def _ordered_output_columns(df: pd.DataFrame) -> list[str]:
    """Return the canonical output column order for the final union table."""
    provenance_columns: list[str] = []
    for field in HARMONIZED_COLUMNS:
        provenance_columns.extend(
            [
                f"{field}__source_family",
                f"{field}__source_variant",
                f"{field}__source_column",
            ]
        )
    native_columns = sorted(
        column
        for column in df.columns
        if any(column.startswith(prefix) for prefix in SOURCE_PREFIXES.values())
    )
    ordered = ROW_PROVENANCE_COLUMNS + HARMONIZED_COLUMNS + provenance_columns + OVERLAY_OUTPUT_COLUMNS + NUMERIC_HELPER_COLUMNS + native_columns
    extras = [column for column in df.columns if column not in ordered]
    return ordered + extras


def _master_ordered_output_columns(df: pd.DataFrame) -> list[str]:
    """Return the canonical column order for the organization-year master parquet."""
    provenance_columns: list[str] = []
    for field in HARMONIZED_COLUMNS:
        provenance_columns.extend(
            [
                f"{field}__source_family",
                f"{field}__source_variant",
                f"{field}__source_column",
                f"{field}__source_time_basis",
                f"{field}__selected_row_source_family",
                f"{field}__selected_row_source_variant",
                f"{field}__selected_row_time_basis",
                f"{field}__selected_row_tax_year",
                f"{field}__source_tax_year",
                f"{field}__source_year_offset",
            ]
        )
    ordered = (
        HARMONIZED_COLUMNS
        + provenance_columns
        + MASTER_FINANCIAL_SEMANTIC_COLUMNS
        + MASTER_GROUP_COLUMNS
        + MASTER_SOURCE_SUMMARY_COLUMNS
        + MASTER_CONFLICT_COLUMNS
        + NUMERIC_HELPER_COLUMNS
    )
    extras = [column for column in df.columns if column not in ordered]
    return ordered + extras


def _group_value_series(grouped: pd.core.groupby.generic.DataFrameGroupBy, series: pd.Series, name: str) -> pd.DataFrame:
    """Materialize one group-level series as a keyed DataFrame for deterministic merges."""
    return grouped.apply(lambda group: series.loc[group.index].iloc[0], include_groups=False).reset_index(name=name)


def _build_master_group_diagnostics(union_df: pd.DataFrame) -> tuple[pd.DataFrame, dict[str, int], int]:
    """
    Build one-row-per-organization-year group diagnostics from the finalized union frame.

    The master file only includes rows with nonblank `harm_ein + harm_tax_year`.
    Current data already satisfies that requirement, but we still count excluded rows
    so future builds surface any upstream key regressions in the build summary.
    """
    key_mask = (
        union_df["harm_ein"].map(blank_to_empty).str.strip().ne("")
        & union_df["harm_tax_year"].map(blank_to_empty).str.strip().ne("")
    )
    excluded_blank_key_row_count = int((~key_mask).sum())
    keyed_df = union_df.loc[key_mask].copy()

    if keyed_df.empty:
        raise ValueError("Cannot build organization-year master because no union rows have nonblank harm_ein + harm_tax_year.")

    grouped = keyed_df.groupby(MASTER_GROUP_KEYS, dropna=False, sort=True)
    master_df = grouped.size().reset_index(name="master_group_row_count")

    print(f"[master] Rows eligible for grouping: {len(keyed_df):,}", flush=True)
    print(f"[master] Rows excluded due to blank key: {excluded_blank_key_row_count:,}", flush=True)
    print(f"[master] Organization-year groups: {len(master_df):,}", flush=True)

    family_strings = grouped["row_source_family"].agg(lambda values: ";".join(sorted({blank_to_empty(value) for value in values if blank_to_empty(value).strip()})))
    master_df = master_df.merge(family_strings.reset_index(name="master_source_families"), on=MASTER_GROUP_KEYS, how="left")
    master_df["master_source_family_count"] = master_df["master_source_families"].map(
        lambda value: len([item for item in blank_to_empty(value).split(";") if item])
    )

    family_presence_counts: dict[str, int] = {}
    for family in DEFAULT_MASTER_SOURCE_PRIORITY:
        presence = grouped["row_source_family"].agg(lambda values, target=family: any(blank_to_empty(value) == target for value in values))
        column_name = f"master_has_{family}"
        family_frame = presence.reset_index(name=column_name)
        master_df = master_df.merge(family_frame, on=MASTER_GROUP_KEYS, how="left")
        master_df[column_name] = master_df[column_name].fillna(False)
        family_presence_counts[family] = int(master_df[column_name].sum())

    return master_df, family_presence_counts, excluded_blank_key_row_count


def _initialize_master_selection_columns(master_df: pd.DataFrame) -> pd.DataFrame:
    """
    Pre-create master selection columns before per-field merges.

    The master build adds a large, predictable block of per-field provenance and
    conflict columns. Creating them up front avoids repeated DataFrame inserts
    inside the field loop, which keeps the builder faster and the runtime output
    cleaner during tests and full builds.
    """
    out = master_df.copy()
    new_columns: dict[str, Any] = {}
    for field in HARMONIZED_COLUMNS:
        if field not in out.columns:
            new_columns[field] = ""
        new_columns[f"{field}__source_family"] = ""
        new_columns[f"{field}__source_variant"] = ""
        new_columns[f"{field}__source_column"] = ""
        new_columns[f"{field}__source_time_basis"] = ""
        new_columns[f"{field}__selected_row_source_family"] = ""
        new_columns[f"{field}__selected_row_source_variant"] = ""
        new_columns[f"{field}__selected_row_time_basis"] = ""
        new_columns[f"{field}__selected_row_tax_year"] = ""
        new_columns[f"{field}__source_tax_year"] = ""
        new_columns[f"{field}__source_year_offset"] = pd.Series([pd.NA] * len(out), index=out.index, dtype="Int64")
        new_columns[f"{field}__conflict"] = False
        new_columns[f"{field}__distinct_nonblank_value_count"] = 0
    return pd.concat([out, pd.DataFrame(new_columns, index=out.index)], axis=1)


def _boolean_flag_from_family(source_family: Any, *, target_family: str) -> str:
    """Return lowercase true/false strings for per-field semantic flags."""
    family = blank_to_empty(source_family).strip()
    if not family:
        return ""
    return "true" if family == target_family else "false"


def _sorted_sources_for_domain(master_row: pd.Series, fields: list[str], priority: list[str]) -> list[str]:
    """Return the unique selected source families used in one semantic domain."""
    seen = {
        blank_to_empty(master_row.get(f"{field}__source_family", "")).strip()
        for field in fields
        if blank_to_empty(master_row.get(field, "")).strip()
    }
    return [family for family in priority if family in seen]


def _build_candidate_summary_json(candidates: pd.DataFrame, *, field: str) -> str:
    """Serialize one conflicted field's candidate rows into compact JSON."""
    if candidates.empty:
        return "[]"

    def _summarize(group: pd.DataFrame) -> pd.Series:
        return pd.Series(
            {
                "row_count": int(len(group)),
            }
        )

    summary = (
        candidates.groupby(
            [
                field,
                f"{field}__source_family",
                f"{field}__source_variant",
                f"{field}__source_column",
                f"{field}__source_time_basis",
                "row_source_time_basis",
                f"{field}__source_tax_year",
                "row_source_variant",
                "row_source_file_name",
                "row_source_row_number",
            ],
            dropna=False,
        )
        .apply(_summarize, include_groups=False)
        .reset_index()
    )
    summary = summary.sort_values(
        [
            field,
            f"{field}__source_family",
            f"{field}__source_variant",
            f"{field}__source_time_basis",
            "row_source_time_basis",
            f"{field}__source_tax_year",
            "row_source_variant",
            "row_source_file_name",
            "row_source_row_number",
        ],
        kind="stable",
    )

    rows = []
    for _, row in summary.iterrows():
        rows.append(
            {
                "value": blank_to_empty(row[field]),
                "source_family": blank_to_empty(row[f"{field}__source_family"]),
                "source_variant": blank_to_empty(row[f"{field}__source_variant"]),
                "source_column": blank_to_empty(row[f"{field}__source_column"]),
                "source_time_basis": blank_to_empty(row[f"{field}__source_time_basis"]),
                "source_tax_year": blank_to_empty(row[f"{field}__source_tax_year"]),
                "selected_row_time_basis": blank_to_empty(row["row_source_time_basis"]),
                "row_count": int(row["row_count"]),
            }
        )
    return json.dumps(rows, separators=(",", ":"), ensure_ascii=True)


def _build_master_field(
    union_df: pd.DataFrame,
    master_df: pd.DataFrame,
    *,
    field: str,
) -> tuple[pd.DataFrame, list[dict[str, Any]], dict[str, Any], list[dict[str, Any]]]:
    """
    Select one winning value per organization-year for one harmonized field.

    The master file is field-priority based, so each harmonized field is selected
    independently from the finalized union frame. This keeps the union parquet as
    the audit artifact while producing one deterministic organization-year row.
    """
    field_start = time.perf_counter()
    group_keys = MASTER_GROUP_KEYS
    priority = _field_priority(field)
    priority_rank = {family: idx for idx, family in enumerate(priority)}
    family_column = f"{field}__source_family"
    variant_column = f"{field}__source_variant"
    source_column_column = f"{field}__source_column"
    time_basis_column = f"{field}__source_time_basis"
    selected_row_family_column = f"{field}__selected_row_source_family"
    selected_row_variant_column = f"{field}__selected_row_source_variant"
    selected_row_time_basis_column = f"{field}__selected_row_time_basis"
    selected_row_tax_year_column = f"{field}__selected_row_tax_year"
    source_tax_year_column = f"{field}__source_tax_year"
    source_year_offset_column = f"{field}__source_year_offset"
    conflict_column = f"{field}__conflict"
    distinct_count_column = f"{field}__distinct_nonblank_value_count"

    print(f"[master] Selecting field: {field}", flush=True)
    print(f"[master] Priority: {' > '.join(priority)}", flush=True)

    working_columns = group_keys + [
        family_column,
        variant_column,
        source_column_column,
        "row_source_family",
        "row_source_variant",
        "row_source_file_name",
        "row_source_row_number",
        "row_source_time_basis",
    ]
    if field not in group_keys:
        working_columns.insert(len(group_keys), field)
    working = union_df[working_columns].copy()
    working[field] = working[field].map(blank_to_empty)
    working[family_column] = working[family_column].map(blank_to_empty)
    working[variant_column] = working[variant_column].map(blank_to_empty)
    working[source_column_column] = working[source_column_column].map(blank_to_empty)
    working["row_source_family"] = working["row_source_family"].map(blank_to_empty)
    working["row_source_variant"] = working["row_source_variant"].map(blank_to_empty)
    working["row_source_file_name"] = working["row_source_file_name"].map(blank_to_empty)
    working["row_source_time_basis"] = working["row_source_time_basis"].map(blank_to_empty)
    working[selected_row_tax_year_column] = working["harm_tax_year"].map(blank_to_empty)
    working[source_tax_year_column] = working["harm_tax_year"].map(blank_to_empty)
    working[time_basis_column] = working[family_column].map(source_family_time_basis)
    working[source_year_offset_column] = 0
    candidates = working[working[field].str.strip().ne("")].copy()

    if candidates.empty:
        master_df[field] = master_df[field] if field in master_df.columns else ""
        master_df[family_column] = ""
        master_df[variant_column] = ""
        master_df[source_column_column] = ""
        master_df[time_basis_column] = ""
        master_df[selected_row_family_column] = ""
        master_df[selected_row_variant_column] = ""
        master_df[selected_row_time_basis_column] = ""
        master_df[selected_row_tax_year_column] = ""
        master_df[source_tax_year_column] = ""
        master_df[source_year_offset_column] = pd.Series([pd.NA] * len(master_df), dtype="Int64")
        master_df[conflict_column] = False
        master_df[distinct_count_column] = 0
        summary_rows = [
            {
                "harmonized_field": field,
                "selected_source_family": "",
                "selected_source_time_basis": "",
                "selected_row_count": int(len(master_df)),
            }
        ]
        conflict_summary = {
            "harmonized_field": field,
            "master_row_count": int(len(master_df)),
            "rows_with_selected_value": 0,
            "conflict_row_count": 0,
            "max_distinct_nonblank_value_count": 0,
        }
        print(f"[master] Field {field} has no nonblank candidates across any organization-year group.", flush=True)
        print_elapsed(field_start, f"master field {field}")
        return master_df, summary_rows, conflict_summary, []

    candidates["_family_rank"] = candidates["row_source_family"].map(lambda value: priority_rank.get(blank_to_empty(value), len(priority_rank)))
    candidates["_variant_sort"] = candidates["row_source_variant"].map(blank_to_empty)
    candidates["_file_sort"] = candidates["row_source_file_name"].map(blank_to_empty)
    candidates["_row_number_sort"] = pd.to_numeric(candidates["row_source_row_number"], errors="coerce").fillna(10**12).astype("int64")

    candidates = candidates.sort_values(
        group_keys + ["_family_rank", "_variant_sort", "_file_sort", "_row_number_sort"],
        ascending=[True, True, True, True, True, True],
        kind="stable",
    )

    winners = candidates.drop_duplicates(subset=group_keys, keep="first").copy()
    winner_columns = group_keys + [
        family_column,
        variant_column,
        source_column_column,
        time_basis_column,
        "row_source_family",
        "row_source_variant",
        "row_source_time_basis",
        selected_row_tax_year_column,
        source_tax_year_column,
        source_year_offset_column,
    ]
    if field not in MASTER_GROUP_KEYS:
        winner_columns.insert(len(group_keys), field)
    winners = winners[winner_columns].rename(
        columns={
            "row_source_family": selected_row_family_column,
            "row_source_variant": selected_row_variant_column,
            "row_source_time_basis": selected_row_time_basis_column,
        }
    )

    existing_winner_columns = [column for column in winners.columns if column not in group_keys and column in master_df.columns]
    if existing_winner_columns:
        master_df = master_df.drop(columns=existing_winner_columns)
    master_df = master_df.merge(winners, on=group_keys, how="left")
    if field not in MASTER_GROUP_KEYS:
        master_df[field] = master_df[field].fillna("")
    master_df[family_column] = master_df[family_column].fillna("")
    master_df[variant_column] = master_df[variant_column].fillna("")
    master_df[source_column_column] = master_df[source_column_column].fillna("")
    master_df[time_basis_column] = master_df[time_basis_column].fillna("")
    master_df[selected_row_family_column] = master_df[selected_row_family_column].fillna("")
    master_df[selected_row_variant_column] = master_df[selected_row_variant_column].fillna("")
    master_df[selected_row_time_basis_column] = master_df[selected_row_time_basis_column].fillna("")
    master_df[selected_row_tax_year_column] = master_df[selected_row_tax_year_column].fillna("")
    master_df[source_tax_year_column] = master_df[source_tax_year_column].fillna("")
    master_df[source_year_offset_column] = master_df[source_year_offset_column].astype("Int64")

    distinct_counts = candidates.groupby(group_keys, dropna=False)[field].nunique().reset_index(name=distinct_count_column)
    if distinct_count_column in master_df.columns:
        master_df = master_df.drop(columns=[distinct_count_column])
    master_df = master_df.merge(distinct_counts, on=group_keys, how="left")
    master_df[distinct_count_column] = master_df[distinct_count_column].fillna(0).astype(int)
    master_df[conflict_column] = master_df[distinct_count_column] > 1

    detail_rows: list[dict[str, Any]] = []
    conflicted_keys = master_df.loc[master_df[conflict_column], group_keys]
    if not conflicted_keys.empty:
        conflicted_candidates = candidates.merge(conflicted_keys.drop_duplicates(), on=group_keys, how="inner")
        selected_lookup = master_df.loc[
            master_df[conflict_column],
            group_keys + [field, family_column, variant_column, time_basis_column, distinct_count_column],
        ]
        for _, selected in selected_lookup.iterrows():
            mask = (
                conflicted_candidates["harm_ein"].eq(selected["harm_ein"])
                & conflicted_candidates["harm_tax_year"].eq(selected["harm_tax_year"])
            )
            group_candidates = conflicted_candidates.loc[mask].copy()
            detail_rows.append(
                {
                    "harm_ein": selected["harm_ein"],
                    "harm_tax_year": selected["harm_tax_year"],
                    "field_name": field,
                    "selected_value": blank_to_empty(selected[field]),
                    "selected_source_family": blank_to_empty(selected[family_column]),
                    "selected_source_variant": blank_to_empty(selected[variant_column]),
                    "selected_source_time_basis": blank_to_empty(selected[time_basis_column]),
                    "distinct_nonblank_value_count": int(selected[distinct_count_column]),
                    "candidate_summary_json": _build_candidate_summary_json(group_candidates, field=field),
                }
            )

    selected_source_counts = (
        master_df.groupby([family_column, time_basis_column], dropna=False)
        .size()
        .reset_index(name="selected_row_count")
        .rename(columns={family_column: "selected_source_family", time_basis_column: "selected_source_time_basis"})
    )
    selected_source_counts.insert(0, "harmonized_field", field)
    summary_rows = selected_source_counts.to_dict("records")

    conflict_summary = {
        "harmonized_field": field,
        "master_row_count": int(len(master_df)),
        "rows_with_selected_value": int(master_df[family_column].map(blank_to_empty).str.strip().ne("").sum()),
        "conflict_row_count": int(master_df[conflict_column].sum()),
        "max_distinct_nonblank_value_count": int(master_df[distinct_count_column].max()),
    }
    print(
        f"[master] Selected rows for {field}: {conflict_summary['rows_with_selected_value']:,} | "
        f"conflicts: {conflict_summary['conflict_row_count']:,}",
        flush=True,
    )
    print_elapsed(field_start, f"master field {field}")
    return master_df, summary_rows, conflict_summary, detail_rows


def _add_master_financial_semantics(master_df: pd.DataFrame) -> pd.DataFrame:
    """Annotate selected master financial fields as filing-based or reference-snapshot values."""
    print("[master] Annotating financial semantics for selected master values.", flush=True)
    master_df = master_df.copy()
    for field in FINANCIAL_FIELDS:
        family_column = f"{field}__source_family"
        reference_column = f"{field}__is_reference_snapshot_value"
        filing_column = f"{field}__is_filing_value"
        source_family = master_df[family_column].map(blank_to_empty)
        master_df[reference_column] = source_family.map(lambda value: _boolean_flag_from_family(value, target_family="nccs_bmf"))
        master_df[filing_column] = source_family.map(
            lambda value: ""
            if not blank_to_empty(value).strip()
            else ("true" if blank_to_empty(value) in {"givingtuesday_datamart", "nccs_efile"} else "false")
        )

    master_df["master_has_reference_selected_values"] = master_df[[f"{field}__source_family" for field in FINANCIAL_FIELDS]].apply(
        lambda row: any(blank_to_empty(value) == "nccs_bmf" for value in row),
        axis=1,
    )
    master_df["master_has_filing_selected_values"] = master_df[[f"{field}__source_family" for field in FINANCIAL_FIELDS]].apply(
        lambda row: any(blank_to_empty(value) in {"givingtuesday_datamart", "nccs_efile"} for value in row),
        axis=1,
    )
    master_df["master_has_any_selected_financial_values"] = master_df[FINANCIAL_FIELDS].apply(
        lambda row: any(blank_to_empty(value).strip() for value in row),
        axis=1,
    )

    def _record_type(row: pd.Series) -> str:
        has_any_financial = bool(row["master_has_any_selected_financial_values"])
        has_reference = bool(row["master_has_reference_selected_values"])
        has_filing = bool(row["master_has_filing_selected_values"])
        if not has_any_financial:
            return "nonfinancial_only"
        if has_reference and has_filing:
            return "mixed"
        if has_reference:
            return "reference_only"
        return "filing_only"

    master_df["master_record_type"] = master_df.apply(_record_type, axis=1)
    return master_df


def _add_master_source_summaries(master_df: pd.DataFrame) -> pd.DataFrame:
    """Add analyst-facing source summaries derived from the selected master fields."""
    print("[master] Building top-level source summary columns for master rows.", flush=True)
    master_df = master_df.copy()

    for domain_name, fields in MASTER_DOMAIN_FIELDS.items():
        selected_sources_column = f"master_{domain_name}_selected_sources"
        primary_source_column = f"master_primary_{domain_name}_source"
        priority = MASTER_DOMAIN_PRIORITY[domain_name]
        selected_lists = master_df.apply(lambda row, domain_fields=fields, order=priority: _sorted_sources_for_domain(row, domain_fields, order), axis=1)
        master_df[selected_sources_column] = selected_lists.map(lambda items: ";".join(items))
        master_df[primary_source_column] = selected_lists.map(lambda items: items[0] if items else "")

    conflict_columns = [f"{field}__conflict" for field in HARMONIZED_COLUMNS]
    master_df["master_conflict_field_count"] = master_df[conflict_columns].sum(axis=1).astype(int)
    master_df["master_has_conflicts"] = master_df["master_conflict_field_count"] > 0
    master_df["master_conflict_fields"] = master_df[conflict_columns].apply(
        lambda row: ";".join(field.replace("__conflict", "") for field, flag in row.items() if bool(flag)),
        axis=1,
    )
    return master_df


def _build_master_from_union(
    union_df: pd.DataFrame,
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame, dict[str, Any]]:
    """Build the one-row-per-organization-year master parquet from the finalized union frame."""
    master_start = time.perf_counter()
    print("[master] Building organization-year master from finalized union frame.", flush=True)
    master_df, family_presence_counts, excluded_blank_key_row_count = _build_master_group_diagnostics(union_df)
    master_df = _initialize_master_selection_columns(master_df)

    field_selection_rows: list[dict[str, Any]] = []
    conflict_summary_rows: list[dict[str, Any]] = []
    conflict_detail_rows: list[dict[str, Any]] = []
    for field in tqdm(HARMONIZED_COLUMNS, desc="master fields", unit="field"):
        master_df, field_summary_rows, conflict_summary, detail_rows = _build_master_field(union_df, master_df, field=field)
        field_selection_rows.extend(field_summary_rows)
        conflict_summary_rows.append(conflict_summary)
        conflict_detail_rows.extend(detail_rows)

    master_df = _add_master_financial_semantics(master_df)
    master_df = _add_master_source_summaries(master_df)
    master_df = _apply_numeric_helpers(master_df)
    master_df = master_df.reindex(columns=_master_ordered_output_columns(master_df))

    build_stats = {
        "grouped_row_count": int(len(union_df) - excluded_blank_key_row_count),
        "excluded_blank_key_row_count": excluded_blank_key_row_count,
        "master_source_family_participation_counts": family_presence_counts,
        "master_record_type_counts": master_df["master_record_type"].value_counts(dropna=False).to_dict(),
    }
    print(f"[master] Wrote master rows in memory: {len(master_df):,}", flush=True)
    print_elapsed(master_start, "master build")
    return (
        master_df,
        pd.DataFrame(field_selection_rows),
        pd.DataFrame(conflict_summary_rows),
        pd.DataFrame(conflict_detail_rows),
        build_stats,
    )


def _build_overlap_by_ein(df: pd.DataFrame) -> pd.DataFrame:
    """Build one-row-per-EIN overlap diagnostics across source families."""
    rows = []
    subset = df[df["harm_ein"].map(blank_to_empty).str.strip().ne("")].copy()
    families = sorted(df["row_source_family"].dropna().unique())
    for harm_ein, group in subset.groupby("harm_ein", dropna=False):
        row: dict[str, Any] = {"harm_ein": harm_ein}
        source_count = 0
        for family in families:
            count = int((group["row_source_family"] == family).sum())
            row[f"has_{family}"] = count > 0
            row[f"{family}_row_count"] = count
            if count > 0:
                source_count += 1
        tax_years = group["harm_tax_year"].replace("", pd.NA).dropna()
        row["min_tax_year"] = tax_years.min() if not tax_years.empty else ""
        row["max_tax_year"] = tax_years.max() if not tax_years.empty else ""
        row["source_count"] = source_count
        rows.append(row)
    return pd.DataFrame(rows)


def _build_overlap_by_ein_tax_year(df: pd.DataFrame) -> pd.DataFrame:
    """Build one-row-per-EIN-plus-tax-year overlap diagnostics across source families."""
    rows = []
    subset = df[
        df["harm_ein"].map(blank_to_empty).str.strip().ne("")
        & df["harm_tax_year"].map(blank_to_empty).str.strip().ne("")
    ].copy()
    families = sorted(df["row_source_family"].dropna().unique())
    for (harm_ein, harm_tax_year), group in subset.groupby(["harm_ein", "harm_tax_year"], dropna=False):
        row: dict[str, Any] = {"harm_ein": harm_ein, "harm_tax_year": harm_tax_year}
        source_count = 0
        for family in families:
            count = int((group["row_source_family"] == family).sum())
            row[f"has_{family}"] = count > 0
            row[f"{family}_row_count"] = count
            if count > 0:
                source_count += 1
        row["source_count"] = source_count
        rows.append(row)
    return pd.DataFrame(rows)


def _build_overlap_summary(overlap_df: pd.DataFrame, grain: str) -> pd.DataFrame:
    """Aggregate overlap diagnostics by source-combination signature."""
    if overlap_df.empty:
        return pd.DataFrame(columns=["grain", "source_combination", "group_count"])
    family_flag_columns = sorted(column for column in overlap_df.columns if column.startswith("has_"))
    working = overlap_df.copy()
    working["source_combination"] = working[family_flag_columns].apply(
        lambda row: ";".join(column.replace("has_", "") for column, flag in row.items() if bool(flag)),
        axis=1,
    )
    summary = (
        working.groupby("source_combination", dropna=False)
        .size()
        .reset_index(name="group_count")
        .sort_values(["group_count", "source_combination"], ascending=[False, True])
    )
    summary.insert(0, "grain", grain)
    return summary


def _example_source_column(df: pd.DataFrame, column_name: str, source_family: str) -> str:
    """Return a simple source-column example for field-availability reporting."""
    if column_name in HARMONIZED_COLUMNS:
        provenance_column = f"{column_name}__source_column"
        subset = df.loc[df["row_source_family"] == source_family, provenance_column].map(blank_to_empty)
        subset = subset[subset.str.strip().ne("")]
        return subset.iloc[0] if not subset.empty else ""
    for prefix in SOURCE_PREFIXES.values():
        if column_name.startswith(prefix):
            return column_name[len(prefix):]
    return ""


def _build_field_availability_matrix(df: pd.DataFrame) -> pd.DataFrame:
    """Build the output-column x source-family fill-rate matrix."""
    families = sorted(df["row_source_family"].dropna().unique())
    rows = []
    for column_name in tqdm(df.columns, desc="availability columns", unit="column"):
        for family in families:
            subset = df.loc[df["row_source_family"] == family, column_name]
            rows_total = int(len(subset))
            nonnull = subset.map(blank_to_empty).str.strip().ne("")
            rows_nonnull = int(nonnull.sum())
            distinct_nonnull = int(subset.map(blank_to_empty)[nonnull].nunique())
            rows.append(
                {
                    "output_column": column_name,
                    "source_family": family,
                    "rows_total": rows_total,
                    "rows_nonnull": rows_nonnull,
                    "fill_rate": (rows_nonnull / rows_total) if rows_total else 0.0,
                    "distinct_nonnull_count": distinct_nonnull,
                    "column_group": column_group(column_name),
                    "example_source_column": _example_source_column(df, column_name, family),
                }
            )
    return pd.DataFrame(rows)


def _build_column_dictionary(df: pd.DataFrame) -> pd.DataFrame:
    """Build one-row-per-output-column metadata for the combined schema."""
    rows = []
    for column_name in df.columns:
        source_family = ""
        source_variant = ""
        original_column = ""
        is_harmonized = column_name in HARMONIZED_COLUMNS
        is_derived = column_group(column_name) != "source_native"
        for family, prefix in SOURCE_PREFIXES.items():
            if column_name.startswith(prefix):
                source_family = family
                original_column = column_name[len(prefix):]
                break
        if column_name in ROW_PROVENANCE_COLUMNS:
            notes = "Row-level source provenance."
        elif column_name in HARMONIZED_COLUMNS:
            notes = "Harmonized field populated from source-specific mappings and optional year-aware BMF overlay."
        elif "__source_" in column_name:
            notes = "Per-harmonized-field provenance."
        elif column_name in OVERLAY_OUTPUT_COLUMNS:
            notes = "Overlay or match-quality diagnostic."
        elif column_name in NUMERIC_HELPER_COLUMNS:
            notes = "Parsed numeric helper derived from a harmonized string amount."
        elif source_family:
            notes = "Source-native column preserved with source-family prefix."
        else:
            notes = "Derived output column."
        rows.append(
            {
                "output_column": column_name,
                "column_group": column_group(column_name),
                "source_family": source_family,
                "source_variant": source_variant,
                "original_column": original_column,
                "is_harmonized": is_harmonized,
                "is_derived": is_derived,
                "schema_version": COMBINED_SCHEMA_VERSION,
                "notes": notes,
            }
        )
    return pd.DataFrame(rows)


def _build_master_column_dictionary(df: pd.DataFrame) -> pd.DataFrame:
    """Build one-row-per-output-column metadata for the organization-year master schema."""
    rows = []
    for column_name in df.columns:
        source_family = ""
        original_column = ""
        is_harmonized = column_name in HARMONIZED_COLUMNS
        is_derived = column_group(column_name) != "harmonized"
        if column_name in HARMONIZED_COLUMNS:
            notes = "Master-selected harmonized field at organization-year grain."
        elif column_name.endswith("__source_family") or column_name.endswith("__source_variant") or column_name.endswith("__source_column"):
            notes = "Per-field value-origin provenance for the selected master value."
        elif column_name.endswith("__source_time_basis"):
            notes = "Canonical time-basis label for the selected master value's source family."
        elif (
            column_name.endswith("__selected_row_source_family")
            or column_name.endswith("__selected_row_source_variant")
            or column_name.endswith("__selected_row_time_basis")
            or column_name.endswith("__selected_row_tax_year")
        ):
            notes = "Selected donor-row provenance for the union row that supplied the chosen master value."
        elif column_name.endswith("__source_tax_year") or column_name.endswith("__source_year_offset"):
            notes = "Selected-value year provenance used to distinguish exact-year vs cross-year fills."
        elif column_name.endswith("__is_reference_snapshot_value") or column_name.endswith("__is_filing_value"):
            notes = "Per-field semantic flag showing whether the selected financial value came from BMF or a filing source."
        elif column_name in MASTER_GROUP_COLUMNS:
            notes = "Organization-year group membership diagnostic."
        elif column_name in MASTER_SOURCE_SUMMARY_COLUMNS:
            notes = "Analyst-facing summary of selected sources, record type, or conflict status."
        elif column_name.endswith("__conflict"):
            notes = "True when multiple distinct nonblank values existed in the organization-year group."
        elif column_name.endswith("__distinct_nonblank_value_count"):
            notes = "Count of distinct nonblank candidate values seen in the organization-year group."
        elif column_name in NUMERIC_HELPER_COLUMNS:
            notes = "Parsed numeric helper recomputed from the selected master string amount."
        else:
            notes = "Derived master output column."
        rows.append(
            {
                "output_column": column_name,
                "column_group": column_group(column_name),
                "source_family": source_family,
                "source_variant": "",
                "original_column": original_column,
                "is_harmonized": is_harmonized,
                "is_derived": is_derived,
                "schema_version": COMBINED_SCHEMA_VERSION,
                "notes": notes,
            }
        )
    return pd.DataFrame(rows)


def main() -> None:
    parser = argparse.ArgumentParser(description="Build the combined filtered 990 union + master tables locally.")
    parser.add_argument("--staging-dir", type=Path, default=STAGING_DIR, help="Local staging output directory")
    parser.add_argument("--output", type=Path, default=OUTPUT_PARQUET, help="Combined union parquet path")
    parser.add_argument("--master-output", type=Path, default=MASTER_OUTPUT_PARQUET, help="Combined organization-year master parquet path")
    parser.add_argument("--compression", default="zstd", help="Parquet compression codec (default: zstd)")
    parser.add_argument("--skip-if-unchanged", action=argparse.BooleanOptionalAction, default=True, help="Skip rebuild when the discovered inputs match the cached build summary")
    parser.add_argument("--force-rebuild", action="store_true", help="Rebuild even when the cached input signature matches")
    args = parser.parse_args()

    start = time.perf_counter()
    banner("STEP 01 - BUILD COMBINED FILTERED 990 UNION + MASTER LOCAL")
    load_env_from_secrets()
    ensure_work_dirs(args.staging_dir)

    print(f"[build] Union output parquet: {args.output}", flush=True)
    print(f"[build] Master output parquet: {args.master_output}", flush=True)
    print(f"[build] Compression: {args.compression}", flush=True)
    print(f"[build] Skip if unchanged: {args.skip_if_unchanged}", flush=True)
    print(f"[build] Force rebuild: {args.force_rebuild}", flush=True)

    inputs = discover_source_inputs()
    bmf_lookup_inputs = discover_bmf_exact_year_lookup_inputs()
    for source in inputs:
        print(f"[build] Input: {source.source_family} | {source.source_variant} | {source.input_path}", flush=True)
    for year, path in bmf_lookup_inputs.items():
        print(f"[build] Auxiliary exact-year BMF lookup: {year} | {path}", flush=True)
    input_signature = build_input_signature(inputs, auxiliary_paths={f"bmf_exact_lookup_{year}": path for year, path in bmf_lookup_inputs.items()})

    if args.skip_if_unchanged and not args.force_rebuild and inputs_match_cached_build(
        BUILD_SUMMARY_JSON,
        input_signature,
        args.output,
        args.master_output,
    ):
        print("[build] Inputs match cached build summary and both output parquets already exist; skipping rebuild.", flush=True)
        print_elapsed(start, "Step 01")
        return

    source_frames: list[pd.DataFrame] = []
    manifest_rows: list[dict[str, Any]] = []
    for source in tqdm(inputs, desc="load source inputs", unit="file"):
        file_start = time.perf_counter()
        print(f"[build] Loading {source.input_path}", flush=True)
        raw_df = _read_source_dataframe(source.input_path, source.input_format)
        print(f"[build] Loaded rows: {len(raw_df):,} | columns: {len(raw_df.columns):,}", flush=True)
        normalized_df, manifest_row = _transform_source(source, raw_df)
        source_frames.append(normalized_df)
        manifest_rows.append(manifest_row)
        print_elapsed(file_start, f"normalize {source.input_path.name}")

    combined_df = pd.concat(source_frames, ignore_index=True, sort=False).fillna("")
    print(f"[build] Combined row count before overlay: {len(combined_df):,}", flush=True)
    combined_df, overlay_stats = _apply_bmf_overlay(combined_df, lookup_paths=bmf_lookup_inputs)
    combined_df = _apply_numeric_helpers(combined_df)
    combined_df = combined_df.reindex(columns=_ordered_output_columns(combined_df))
    print(f"[build] Combined row count after overlay and numeric helpers: {len(combined_df):,}", flush=True)

    overlap_ein_df = _build_overlap_by_ein(combined_df)
    overlap_ein_tax_year_df = _build_overlap_by_ein_tax_year(combined_df)
    overlap_summary_df = pd.concat(
        [
            _build_overlap_summary(overlap_ein_df, "ein"),
            _build_overlap_summary(overlap_ein_tax_year_df, "ein_tax_year"),
        ],
        ignore_index=True,
        sort=False,
    )
    field_availability_df = _build_field_availability_matrix(combined_df)
    column_dictionary_df = _build_column_dictionary(combined_df)
    (
        master_df,
        master_field_selection_df,
        master_conflict_summary_df,
        master_conflict_detail_df,
        master_build_stats,
    ) = _build_master_from_union(combined_df)
    master_column_dictionary_df = _build_master_column_dictionary(master_df)

    write_parquet_with_metadata(combined_df, args.output, schema_version=COMBINED_SCHEMA_VERSION, compression=args.compression)
    write_parquet_with_metadata(master_df, args.master_output, schema_version=COMBINED_SCHEMA_VERSION, compression=args.compression)
    write_csv(SOURCE_INPUT_MANIFEST_CSV, manifest_rows, list(manifest_rows[0].keys()))
    column_dictionary_df.to_csv(COLUMN_DICTIONARY_CSV, index=False)
    field_availability_df.to_csv(FIELD_AVAILABILITY_MATRIX_CSV, index=False)
    overlap_ein_df.to_csv(DIAG_OVERLAP_BY_EIN_CSV, index=False)
    overlap_ein_tax_year_df.to_csv(DIAG_OVERLAP_BY_EIN_TAX_YEAR_CSV, index=False)
    overlap_summary_df.to_csv(DIAG_OVERLAP_SUMMARY_CSV, index=False)
    master_column_dictionary_df.to_csv(MASTER_COLUMN_DICTIONARY_CSV, index=False)
    master_field_selection_df.to_csv(MASTER_FIELD_SELECTION_SUMMARY_CSV, index=False)
    master_conflict_summary_df.to_csv(MASTER_CONFLICT_SUMMARY_CSV, index=False)
    master_conflict_detail_df.to_csv(MASTER_CONFLICT_DETAIL_CSV, index=False)
    write_parquet_with_metadata(
        master_conflict_detail_df,
        MASTER_CONFLICT_DETAIL_PARQUET,
        schema_version=COMBINED_SCHEMA_VERSION,
        compression=args.compression,
    )

    build_summary = {
        "combined_schema_version": COMBINED_SCHEMA_VERSION,
        "created_at_utc": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "union_output_parquet": str(args.output),
        "master_output_parquet": str(args.master_output),
        "union_row_count": int(len(combined_df)),
        "union_column_count": int(len(combined_df.columns)),
        "row_count": int(len(combined_df)),
        "column_count": int(len(combined_df.columns)),
        "master_row_count": int(len(master_df)),
        "master_column_count": int(len(master_df.columns)),
        "input_signature": input_signature,
        "source_row_counts": combined_df["row_source_family"].value_counts(dropna=False).to_dict(),
        "grouped_row_count": master_build_stats["grouped_row_count"],
        "excluded_blank_key_row_count": master_build_stats["excluded_blank_key_row_count"],
        "master_source_family_participation_counts": master_build_stats["master_source_family_participation_counts"],
        "bmf_exact_lookup_file_count": overlay_stats["bmf_exact_lookup_file_count"],
        "bmf_exact_lookup_row_count": overlay_stats["bmf_exact_lookup_row_count"],
        "bmf_overlay_row_count": overlay_stats["bmf_overlay_row_count"],
        "bmf_overlay_field_fill_counts": overlay_stats["bmf_overlay_field_fill_counts"],
        "master_record_type_counts": master_build_stats["master_record_type_counts"],
    }
    write_json(BUILD_SUMMARY_JSON, build_summary)

    print(f"[build] Wrote union parquet: {args.output}", flush=True)
    print(f"[build] Wrote master parquet: {args.master_output}", flush=True)
    print(f"[build] Wrote source manifest: {SOURCE_INPUT_MANIFEST_CSV}", flush=True)
    print(f"[build] Wrote column dictionary: {COLUMN_DICTIONARY_CSV}", flush=True)
    print(f"[build] Wrote master column dictionary: {MASTER_COLUMN_DICTIONARY_CSV}", flush=True)
    print(f"[build] Wrote field availability matrix: {FIELD_AVAILABILITY_MATRIX_CSV}", flush=True)
    print(f"[build] Wrote overlap diagnostics: {DIAG_OVERLAP_BY_EIN_CSV}, {DIAG_OVERLAP_BY_EIN_TAX_YEAR_CSV}, {DIAG_OVERLAP_SUMMARY_CSV}", flush=True)
    print(f"[build] Wrote master selection summary: {MASTER_FIELD_SELECTION_SUMMARY_CSV}", flush=True)
    print(f"[build] Wrote master conflict summary: {MASTER_CONFLICT_SUMMARY_CSV}", flush=True)
    print(f"[build] Wrote master conflict detail: {MASTER_CONFLICT_DETAIL_CSV}", flush=True)
    print(f"[build] Wrote master conflict detail parquet: {MASTER_CONFLICT_DETAIL_PARQUET}", flush=True)
    print(f"[build] Union row count: {len(combined_df):,}", flush=True)
    print(f"[build] Master row count: {len(master_df):,}", flush=True)
    print_elapsed(start, "Step 01")


if __name__ == "__main__":
    main()
