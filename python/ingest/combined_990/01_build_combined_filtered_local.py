"""
Step 01: Build the combined filtered 990 source-union table locally.
"""

from __future__ import annotations

import argparse
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
    HARMONIZED_COLUMNS,
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
    discover_source_inputs,
    ensure_work_dirs,
    inputs_match_cached_build,
    load_env_from_secrets,
    normalize_ein9,
    normalize_tax_year,
    normalize_zip5,
    parse_numeric_string,
    print_elapsed,
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


def _boolish(series: pd.Series) -> pd.Series:
    """Normalize truthy-ish flags to stable string output."""
    return series.map(blank_to_empty)


def _pick_gt_value(df: pd.DataFrame, primary: str, fallback: str) -> tuple[pd.Series, pd.Series]:
    """Choose the first nonblank value between two GivingTuesday columns and track provenance."""
    primary_values = df[primary].map(blank_to_empty)
    fallback_values = df[fallback].map(blank_to_empty)
    use_primary = primary_values.str.strip().ne("")
    values = primary_values.where(use_primary, fallback_values)
    provenance = pd.Series([primary] * len(df), index=df.index).where(use_primary, fallback)
    provenance = provenance.where(values.str.strip().ne(""), "")
    return values, provenance


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

    revenue_values = df["TOTREVCURYEA"].map(blank_to_empty).where(df["form_type"].map(blank_to_empty).ne("990EZ"), df["TOTALRREVENU"].map(blank_to_empty))
    revenue_source = pd.Series(["TOTREVCURYEA"] * len(df), index=df.index).where(df["form_type"].map(blank_to_empty).ne("990EZ"), "TOTALRREVENU")
    revenue_source = revenue_source.where(revenue_values.str.strip().ne(""), "")
    _set_harmonized(out, field="harm_revenue_amount", values=revenue_values, source_family=source.source_family, source_variant=source.source_variant, source_columns=revenue_source)

    expense_values = df["TOTEXPCURYEA"].map(blank_to_empty).where(df["form_type"].map(blank_to_empty).ne("990EZ"), df["TOTALEEXPENS"].map(blank_to_empty))
    expense_source = pd.Series(["TOTEXPCURYEA"] * len(df), index=df.index).where(df["form_type"].map(blank_to_empty).ne("990EZ"), "TOTALEEXPENS")
    expense_source = expense_source.where(expense_values.str.strip().ne(""), "")
    _set_harmonized(out, field="harm_expenses_amount", values=expense_values, source_family=source.source_family, source_variant=source.source_variant, source_columns=expense_source)

    asset_values = df["TOASEOOYY"].map(blank_to_empty).where(df["form_type"].map(blank_to_empty).ne("990PF"), df["ASSEOYOYY"].map(blank_to_empty))
    asset_source = pd.Series(["TOASEOOYY"] * len(df), index=df.index).where(df["form_type"].map(blank_to_empty).ne("990PF"), "ASSEOYOYY")
    asset_source = asset_source.where(asset_values.str.strip().ne(""), "")
    _set_harmonized(out, field="harm_assets_amount", values=asset_values, source_family=source.source_family, source_variant=source.source_variant, source_columns=asset_source)

    _set_harmonized(out, field="harm_income_amount", values=pd.Series([""] * len(df), index=df.index), source_family=source.source_family, source_variant=source.source_variant, source_columns="")
    _set_harmonized(out, field="harm_gross_receipts_under_25000_flag", values=pd.Series([""] * len(df), index=df.index), source_family=source.source_family, source_variant=source.source_variant, source_columns="")
    _set_harmonized(out, field="harm_is_hospital", values=_boolish(df["OPERATHOSPIT"]), source_family=source.source_family, source_variant=source.source_variant, source_columns="OPERATHOSPIT")
    _set_harmonized(out, field="harm_is_university", values=_boolish(df["OPERATSCHOOL"]), source_family=source.source_family, source_variant=source.source_variant, source_columns="OPERATSCHOOL")

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

    state_primary = df["organization_state"].map(blank_to_empty)
    state_fallback = df["officer_state"].map(blank_to_empty)
    state_values = state_primary.where(state_primary.str.strip().ne(""), state_fallback)
    state_source = pd.Series(["organization_state"] * len(df), index=df.index).where(state_primary.str.strip().ne(""), "officer_state")
    state_source = state_source.where(state_values.str.strip().ne(""), "")
    _set_harmonized(out, field="harm_state", values=state_values, source_family=source.source_family, source_variant=source.source_variant, source_columns=state_source)

    zip_primary = df["organization_zip"].map(normalize_zip5)
    zip_fallback = df["officer_zip"].map(normalize_zip5)
    zip_values = zip_primary.where(zip_primary.str.strip().ne(""), zip_fallback)
    zip_source = pd.Series(["organization_zip"] * len(df), index=df.index).where(zip_primary.str.strip().ne(""), "officer_zip")
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
    _set_harmonized(out, field="harm_gross_receipts_under_25000_flag", values=df["gross_receipts_under_25000"].map(blank_to_empty), source_family=source.source_family, source_variant=source.source_variant, source_columns="gross_receipts_under_25000")
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
    _set_harmonized(out, field="harm_is_hospital", values=df["F9_04_HOSPITAL_X"].map(blank_to_empty), source_family=source.source_family, source_variant=source.source_variant, source_columns="F9_04_HOSPITAL_X")
    _set_harmonized(out, field="harm_is_university", values=df["F9_04_SCHOOL_X"].map(blank_to_empty), source_family=source.source_family, source_variant=source.source_variant, source_columns="F9_04_SCHOOL_X")

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
    if source.source_family == "nccs_core":
        return _transform_core(source, df)
    if source.source_family == "nccs_bmf":
        return _transform_bmf(source, df)
    raise ValueError(f"Unsupported source_family: {source.source_family}")


def _apply_bmf_overlay(df: pd.DataFrame) -> pd.DataFrame:
    """Apply the limited year-aware BMF overlay to non-BMF rows."""
    bmf_rows = df[df["row_source_family"] == "nccs_bmf"].copy()
    lookup_columns = ["harm_ein", "harm_tax_year"]
    for field in OVERLAY_FIELDS:
        lookup_columns.extend(
            [
                field,
                f"{field}__source_family",
                f"{field}__source_variant",
                f"{field}__source_column",
            ]
        )
    bmf_lookup = bmf_rows[lookup_columns].drop_duplicates(subset=["harm_ein", "harm_tax_year"], keep="first")
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
    return merged.drop(columns=drop_columns)


def _apply_numeric_helpers(df: pd.DataFrame) -> pd.DataFrame:
    """Add parsed numeric helper columns for the top-line harmonized financial fields."""
    mapping = {
        "harm_revenue_amount": ("harm_revenue_amount_num", "harm_revenue_amount_num_parse_ok"),
        "harm_expenses_amount": ("harm_expenses_amount_num", "harm_expenses_amount_num_parse_ok"),
        "harm_assets_amount": ("harm_assets_amount_num", "harm_assets_amount_num_parse_ok"),
        "harm_income_amount": ("harm_income_amount_num", "harm_income_amount_num_parse_ok"),
    }
    for source_column, (value_column, ok_column) in mapping.items():
        parsed = df[source_column].map(parse_numeric_string)
        df[value_column] = parsed.map(lambda item: item[0])
        df[ok_column] = parsed.map(lambda item: item[1])
    return df


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


def main() -> None:
    parser = argparse.ArgumentParser(description="Build the combined filtered 990 source-union table locally.")
    parser.add_argument("--staging-dir", type=Path, default=STAGING_DIR, help="Local staging output directory")
    parser.add_argument("--output", type=Path, default=OUTPUT_PARQUET, help="Combined union parquet path")
    parser.add_argument("--compression", default="zstd", help="Parquet compression codec (default: zstd)")
    parser.add_argument("--skip-if-unchanged", action=argparse.BooleanOptionalAction, default=True, help="Skip rebuild when the discovered inputs match the cached build summary")
    parser.add_argument("--force-rebuild", action="store_true", help="Rebuild even when the cached input signature matches")
    args = parser.parse_args()

    start = time.perf_counter()
    banner("STEP 01 - BUILD COMBINED FILTERED 990 SOURCE UNION LOCAL")
    load_env_from_secrets()
    ensure_work_dirs(args.staging_dir)

    print(f"[build] Output parquet: {args.output}", flush=True)
    print(f"[build] Compression: {args.compression}", flush=True)
    print(f"[build] Skip if unchanged: {args.skip_if_unchanged}", flush=True)
    print(f"[build] Force rebuild: {args.force_rebuild}", flush=True)

    inputs = discover_source_inputs()
    for source in inputs:
        print(f"[build] Input: {source.source_family} | {source.source_variant} | {source.input_path}", flush=True)
    input_signature = build_input_signature(inputs)

    if args.skip_if_unchanged and not args.force_rebuild and inputs_match_cached_build(BUILD_SUMMARY_JSON, input_signature, args.output):
        print("[build] Inputs match cached build summary and output parquet already exists; skipping rebuild.", flush=True)
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
    combined_df = _apply_bmf_overlay(combined_df)
    combined_df = _apply_numeric_helpers(combined_df)
    combined_df = combined_df.reindex(columns=_ordered_output_columns(combined_df))

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

    write_parquet_with_metadata(combined_df, args.output, schema_version=COMBINED_SCHEMA_VERSION, compression=args.compression)
    write_csv(SOURCE_INPUT_MANIFEST_CSV, manifest_rows, list(manifest_rows[0].keys()))
    column_dictionary_df.to_csv(COLUMN_DICTIONARY_CSV, index=False)
    field_availability_df.to_csv(FIELD_AVAILABILITY_MATRIX_CSV, index=False)
    overlap_ein_df.to_csv(DIAG_OVERLAP_BY_EIN_CSV, index=False)
    overlap_ein_tax_year_df.to_csv(DIAG_OVERLAP_BY_EIN_TAX_YEAR_CSV, index=False)
    overlap_summary_df.to_csv(DIAG_OVERLAP_SUMMARY_CSV, index=False)

    build_summary = {
        "combined_schema_version": COMBINED_SCHEMA_VERSION,
        "created_at_utc": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "output_parquet": str(args.output),
        "row_count": int(len(combined_df)),
        "column_count": int(len(combined_df.columns)),
        "input_signature": input_signature,
        "source_row_counts": combined_df["row_source_family"].value_counts(dropna=False).to_dict(),
    }
    write_json(BUILD_SUMMARY_JSON, build_summary)

    print(f"[build] Wrote combined parquet: {args.output}", flush=True)
    print(f"[build] Wrote source manifest: {SOURCE_INPUT_MANIFEST_CSV}", flush=True)
    print(f"[build] Wrote column dictionary: {COLUMN_DICTIONARY_CSV}", flush=True)
    print(f"[build] Wrote field availability matrix: {FIELD_AVAILABILITY_MATRIX_CSV}", flush=True)
    print(f"[build] Wrote overlap diagnostics: {DIAG_OVERLAP_BY_EIN_CSV}, {DIAG_OVERLAP_BY_EIN_TAX_YEAR_CSV}, {DIAG_OVERLAP_SUMMARY_CSV}", flush=True)
    print(f"[build] Row count: {len(combined_df):,}", flush=True)
    print_elapsed(start, "Step 01")


if __name__ == "__main__":
    main()
