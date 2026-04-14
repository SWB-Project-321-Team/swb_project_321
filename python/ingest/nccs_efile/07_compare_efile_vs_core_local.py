"""
Step 07: Compare annualized NCCS efile benchmark outputs against current NCCS Core benchmark outputs.
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
    COMPARISON_SILVER_PREFIX,
    DEFAULT_S3_BUCKET,
    DEFAULT_S3_REGION,
    META_DIR,
    START_YEAR_DEFAULT,
    STAGING_DIR,
    banner,
    comparison_conflicts_path,
    comparison_dir,
    comparison_fill_rates_path,
    comparison_row_counts_path,
    comparison_s3_key,
    comparison_summary_path,
    grouped_assets_by_year,
    guess_content_type,
    load_env_from_secrets,
    normalize_ein9,
    normalize_tax_year,
    print_elapsed,
    resolve_release_and_write_metadata,
    should_skip_upload,
    upload_file_with_progress,
)

COMPARABLE_FIELDS = [
    "harm_org_name",
    "harm_state",
    "harm_zip5",
    "harm_county_fips",
    "harm_region",
    "harm_revenue_amount",
    "harm_expenses_amount",
    "harm_assets_amount",
    "harm_income_amount",
    "harm_is_hospital",
    "harm_is_university",
]


def _blank_to_empty(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, float) and pd.isna(value):
        return ""
    text = str(value)
    return "" if text.lower() == "nan" else text


def _normalize_boolish(series: pd.Series) -> pd.Series:
    # NCCS efile checkbox indicators use `X` for a checked / true value.
    truthy = {"1", "y", "yes", "true", "t", "x"}
    falsey = {"0", "n", "no", "false", "f"}

    def _normalize_one(value: Any) -> str:
        raw = _blank_to_empty(value).strip().lower()
        if not raw:
            return ""
        if raw in truthy:
            return "true"
        if raw in falsey:
            return "false"
        return ""

    return series.map(_normalize_one)


def _core_patterns() -> list[str]:
    return [
        "*501C3-CHARITIES-PC*__benchmark.csv",
        "*501C3-CHARITIES-PZ*__benchmark.csv",
        "*501CE-NONPROFIT-PC*__benchmark.csv",
        "*501CE-NONPROFIT-PZ*__benchmark.csv",
    ]


def _load_core_year(core_root: Path, tax_year: int) -> pd.DataFrame:
    year_dir = core_root / f"year={tax_year}"
    if not year_dir.exists():
        raise FileNotFoundError(f"Core year directory not found: {year_dir}")

    frames: list[pd.DataFrame] = []
    for pattern in _core_patterns():
        matches = sorted(year_dir.glob(pattern))
        if not matches:
            raise FileNotFoundError(f"Expected Core benchmark file matching {pattern} in {year_dir}")
        frame = pd.read_csv(matches[0], dtype=str, low_memory=False).fillna("")
        frames.append(frame)

    core_df = pd.concat(frames, ignore_index=True)
    ein_primary = core_df["EIN2"].map(normalize_ein9) if "EIN2" in core_df.columns else pd.Series([""] * len(core_df))
    ein_fallback = core_df["F9_00_ORG_EIN"].map(normalize_ein9) if "F9_00_ORG_EIN" in core_df.columns else pd.Series([""] * len(core_df))
    harm_ein = ein_primary.where(ein_primary.str.strip().ne(""), ein_fallback)
    comparable = pd.DataFrame(
        {
            "harm_ein": harm_ein,
            "harm_tax_year": core_df["TAX_YEAR"].map(normalize_tax_year) if "TAX_YEAR" in core_df.columns else "",
            "harm_org_name": "",
            "harm_state": "",
            "harm_zip5": "",
            "harm_county_fips": core_df["county_fips"].map(_blank_to_empty) if "county_fips" in core_df.columns else "",
            "harm_region": core_df["region"].map(_blank_to_empty) if "region" in core_df.columns else "",
            "harm_revenue_amount": core_df["F9_08_REV_TOT_TOT"].map(_blank_to_empty) if "F9_08_REV_TOT_TOT" in core_df.columns else "",
            "harm_expenses_amount": core_df["F9_09_EXP_TOT_TOT"].map(_blank_to_empty) if "F9_09_EXP_TOT_TOT" in core_df.columns else "",
            "harm_assets_amount": core_df["F9_10_ASSET_TOT_EOY"].map(_blank_to_empty) if "F9_10_ASSET_TOT_EOY" in core_df.columns else "",
            "harm_income_amount": "",
            "harm_is_hospital": _normalize_boolish(core_df["F9_04_HOSPITAL_X"]) if "F9_04_HOSPITAL_X" in core_df.columns else "",
            "harm_is_university": _normalize_boolish(core_df["F9_04_SCHOOL_X"]) if "F9_04_SCHOOL_X" in core_df.columns else "",
        }
    )
    comparable = comparable.sort_values(["harm_ein", "harm_tax_year"], kind="mergesort")
    comparable = comparable.drop_duplicates(subset=["harm_ein", "harm_tax_year"], keep="first")
    return comparable


def _load_efile_year(staging_root: Path, tax_year: int) -> pd.DataFrame:
    path = staging_root / f"tax_year={tax_year}" / f"nccs_efile_benchmark_tax_year={tax_year}.parquet"
    if not path.exists():
        raise FileNotFoundError(f"Efile benchmark parquet not found: {path}. Run step 05 first.")
    efile_df = pd.read_parquet(path).fillna("")
    comparable = pd.DataFrame(
        {
            "harm_ein": efile_df["F9_00_ORG_EIN"].map(normalize_ein9) if "F9_00_ORG_EIN" in efile_df.columns else efile_df["ORG_EIN"].map(normalize_ein9),
            "harm_tax_year": efile_df["F9_00_TAX_YEAR"].map(normalize_tax_year) if "F9_00_TAX_YEAR" in efile_df.columns else efile_df["TAX_YEAR"].map(normalize_tax_year),
            "harm_org_name": efile_df["F9_00_ORG_NAME_L1"].map(_blank_to_empty) if "F9_00_ORG_NAME_L1" in efile_df.columns else "",
            "harm_state": efile_df["F9_00_ORG_ADDR_STATE"].map(_blank_to_empty) if "F9_00_ORG_ADDR_STATE" in efile_df.columns else "",
            "harm_zip5": efile_df["F9_00_ORG_ADDR_ZIP"].map(_blank_to_empty) if "F9_00_ORG_ADDR_ZIP" in efile_df.columns else "",
            "harm_county_fips": efile_df["county_fips"].map(_blank_to_empty),
            "harm_region": efile_df["region"].map(_blank_to_empty),
            "harm_revenue_amount": efile_df["F9_01_REV_TOT_CY"].map(_blank_to_empty) if "F9_01_REV_TOT_CY" in efile_df.columns else "",
            "harm_expenses_amount": efile_df["F9_01_EXP_TOT_CY"].map(_blank_to_empty) if "F9_01_EXP_TOT_CY" in efile_df.columns else "",
            "harm_assets_amount": efile_df["F9_01_NAFB_ASSET_TOT_EOY"].map(_blank_to_empty) if "F9_01_NAFB_ASSET_TOT_EOY" in efile_df.columns else "",
            "harm_income_amount": efile_df["F9_01_EXP_REV_LESS_EXP_CY"].map(_blank_to_empty) if "F9_01_EXP_REV_LESS_EXP_CY" in efile_df.columns else "",
            "harm_is_hospital": _normalize_boolish(efile_df["SA_01_PCSTAT_HOSPITAL_X"]) if "SA_01_PCSTAT_HOSPITAL_X" in efile_df.columns else "",
            "harm_is_university": _normalize_boolish(efile_df["SA_01_PCSTAT_SCHOOL_X"]) if "SA_01_PCSTAT_SCHOOL_X" in efile_df.columns else "",
        }
    )
    comparable = comparable.sort_values(["harm_ein", "harm_tax_year"], kind="mergesort")
    comparable = comparable.drop_duplicates(subset=["harm_ein", "harm_tax_year"], keep="first")
    return comparable


def _row_counts_frame(tax_year: int, efile_df: pd.DataFrame, core_df: pd.DataFrame) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    for dataset_name, df in (("nccs_efile", efile_df), ("nccs_core", core_df)):
        rows.append({"tax_year": tax_year, "metric_type": "dataset_total_row_count", "dataset": dataset_name, "harm_region": "", "value": int(len(df))})
        grouped = df.groupby("harm_region", dropna=False).size().reset_index(name="value")
        for _, row in grouped.iterrows():
            rows.append(
                {
                    "tax_year": tax_year,
                    "metric_type": "dataset_region_row_count",
                    "dataset": dataset_name,
                    "harm_region": _blank_to_empty(row["harm_region"]),
                    "value": int(row["value"]),
                }
            )

    efile_eins = set(efile_df["harm_ein"].map(_blank_to_empty)) - {""}
    core_eins = set(core_df["harm_ein"].map(_blank_to_empty)) - {""}
    efile_keys = set(zip(efile_df["harm_ein"].map(_blank_to_empty), efile_df["harm_tax_year"].map(_blank_to_empty))) - {("", "")}
    core_keys = set(zip(core_df["harm_ein"].map(_blank_to_empty), core_df["harm_tax_year"].map(_blank_to_empty))) - {("", "")}
    rows.extend(
        [
            {"tax_year": tax_year, "metric_type": "overlap_ein_count", "dataset": "efile_vs_core", "harm_region": "", "value": len(efile_eins & core_eins)},
            {"tax_year": tax_year, "metric_type": "overlap_ein_tax_year_count", "dataset": "efile_vs_core", "harm_region": "", "value": len(efile_keys & core_keys)},
        ]
    )
    return pd.DataFrame(rows)


def _fill_rates_frame(tax_year: int, efile_df: pd.DataFrame, core_df: pd.DataFrame) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    for dataset_name, df in (("nccs_efile", efile_df), ("nccs_core", core_df)):
        rows_total = int(len(df))
        for field_name in tqdm(COMPARABLE_FIELDS, desc=f"fill rates {tax_year}", unit="field", leave=False):
            nonblank_mask = df[field_name].map(_blank_to_empty).str.strip().ne("")
            rows_nonblank = int(nonblank_mask.sum())
            rows.append(
                {
                    "tax_year": tax_year,
                    "dataset": dataset_name,
                    "field_name": field_name,
                    "rows_total": rows_total,
                    "rows_nonblank": rows_nonblank,
                    "fill_rate": (rows_nonblank / rows_total) if rows_total else 0.0,
                }
            )
    return pd.DataFrame(rows)


def _conflicts_frame(tax_year: int, efile_df: pd.DataFrame, core_df: pd.DataFrame) -> pd.DataFrame:
    joined = efile_df.merge(core_df, on=["harm_ein", "harm_tax_year"], how="inner", suffixes=("_efile", "_core"))
    rows: list[dict[str, Any]] = []
    for field_name in COMPARABLE_FIELDS:
        efile_values = joined[f"{field_name}_efile"].map(_blank_to_empty)
        core_values = joined[f"{field_name}_core"].map(_blank_to_empty)
        both_nonblank = efile_values.str.strip().ne("") & core_values.str.strip().ne("")
        equal_nonblank = both_nonblank & efile_values.eq(core_values)
        conflict_nonblank = both_nonblank & efile_values.ne(core_values)
        rows.append(
            {
                "tax_year": tax_year,
                "field_name": field_name,
                "shared_ein_tax_year_count": int(len(joined)),
                "both_nonblank_count": int(both_nonblank.sum()),
                "equal_nonblank_count": int(equal_nonblank.sum()),
                "conflict_count": int(conflict_nonblank.sum()),
            }
        )
    return pd.DataFrame(rows)


def _summary_payload(tax_year: int, row_counts_df: pd.DataFrame, fill_rates_df: pd.DataFrame, conflicts_df: pd.DataFrame) -> dict[str, Any]:
    counts = {
        f"{row['metric_type']}::{row['dataset']}::{row['harm_region']}": int(row["value"])
        for _, row in row_counts_df.iterrows()
        if row["metric_type"] != "dataset_region_row_count"
    }
    fill_lookup = {
        (row["dataset"], row["field_name"]): float(row["fill_rate"])
        for _, row in fill_rates_df.iterrows()
    }
    shared_count = int(conflicts_df["shared_ein_tax_year_count"].max()) if not conflicts_df.empty else 0
    conflict_counts = {row["field_name"]: int(row["conflict_count"]) for _, row in conflicts_df.iterrows()}
    efile_row_count = counts.get("dataset_total_row_count::nccs_efile::", 0)
    core_row_count = counts.get("dataset_total_row_count::nccs_core::", 0)
    overlap_key_count = counts.get("overlap_ein_tax_year_count::efile_vs_core::", 0)
    return {
        "tax_year": tax_year,
        "efile_row_count": efile_row_count,
        "core_row_count": core_row_count,
        "overlap_ein_count": counts.get("overlap_ein_count::efile_vs_core::", 0),
        "overlap_ein_tax_year_count": overlap_key_count,
        "efile_has_all_core_ein_tax_years": bool(core_row_count and overlap_key_count == core_row_count),
        "efile_row_count_ge_core": bool(efile_row_count >= core_row_count),
        "shared_ein_tax_year_count": shared_count,
        "conflict_counts": conflict_counts,
        "efile_fill_rates": {field: fill_lookup.get(("nccs_efile", field), 0.0) for field in COMPARABLE_FIELDS},
        "core_fill_rates": {field: fill_lookup.get(("nccs_core", field), 0.0) for field in COMPARABLE_FIELDS},
        "replacement_assessment": (
            "efile benchmark comparison artifacts generated; review overlap and fill-rate metrics before relying on year-specific replacement decisions."
        ),
    }


def main() -> None:
    parser = argparse.ArgumentParser(description="Compare annualized NCCS efile benchmark outputs against current Core benchmark outputs.")
    parser.add_argument("--start-year", type=int, default=START_YEAR_DEFAULT, help="First tax year to include (default: 2022)")
    parser.add_argument("--metadata-dir", type=Path, default=META_DIR, help="Local efile metadata directory")
    parser.add_argument("--staging-dir", type=Path, default=STAGING_DIR, help="Local efile staging root")
    parser.add_argument("--core-staging-root", type=Path, default=STAGING_DIR.parent / "nccs_990" / "core", help="Local NCCS Core staging root used for comparison")
    parser.add_argument("--bucket", default=DEFAULT_S3_BUCKET, help="Target S3 bucket")
    parser.add_argument("--region", default=DEFAULT_S3_REGION, help="Target S3 region")
    parser.add_argument("--comparison-prefix", default=COMPARISON_SILVER_PREFIX, help="S3 comparison prefix")
    parser.add_argument("--overwrite", action="store_true", help="Upload even when the S3 object already matches local bytes")
    args = parser.parse_args()

    start = time.perf_counter()
    banner("STEP 07 - COMPARE NCCS EFILE VS CORE")
    load_env_from_secrets()

    print(f"[compare] Start year: {args.start_year}", flush=True)
    print(f"[compare] Efile staging root: {args.staging_dir}", flush=True)
    print(f"[compare] Core staging root: {args.core_staging_root}", flush=True)
    print(f"[compare] Bucket: {args.bucket}", flush=True)
    print(f"[compare] Region: {args.region}", flush=True)
    print(f"[compare] Comparison prefix: {args.comparison_prefix}", flush=True)

    release = resolve_release_and_write_metadata(args.metadata_dir, start_year=args.start_year)
    efile_years = sorted(grouped_assets_by_year(release))
    overlap_years = [tax_year for tax_year in efile_years if (args.core_staging_root / f"year={tax_year}").exists()]
    if not overlap_years:
        raise FileNotFoundError(f"No overlapping efile/core comparison years found under {args.core_staging_root}.")

    print(f"[compare] Overlapping comparison years: {overlap_years}", flush=True)
    for tax_year in tqdm(overlap_years, desc="compare years", unit="year"):
        year_start = time.perf_counter()
        print(f"[compare] Building comparison outputs for tax_year={tax_year}", flush=True)
        local_dir = comparison_dir(args.staging_dir, tax_year)
        local_dir.mkdir(parents=True, exist_ok=True)

        efile_df = _load_efile_year(args.staging_dir, tax_year)
        core_df = _load_core_year(args.core_staging_root, tax_year)

        row_counts_df = _row_counts_frame(tax_year, efile_df, core_df)
        fill_rates_df = _fill_rates_frame(tax_year, efile_df, core_df)
        conflicts_df = _conflicts_frame(tax_year, efile_df, core_df)
        summary_payload = _summary_payload(tax_year, row_counts_df, fill_rates_df, conflicts_df)

        row_counts_path = comparison_row_counts_path(args.staging_dir, tax_year)
        fill_rates_path = comparison_fill_rates_path(args.staging_dir, tax_year)
        conflicts_path = comparison_conflicts_path(args.staging_dir, tax_year)
        summary_path = comparison_summary_path(args.staging_dir, tax_year)

        row_counts_df.to_csv(row_counts_path, index=False)
        fill_rates_df.to_csv(fill_rates_path, index=False)
        conflicts_df.to_csv(conflicts_path, index=False)
        summary_path.write_text(json.dumps(summary_payload, indent=2, sort_keys=True), encoding="utf-8")

        print(
            f"[compare] tax_year={tax_year} | efile_rows={len(efile_df):,} | core_rows={len(core_df):,} | "
            f"shared_keys={summary_payload['overlap_ein_tax_year_count']:,}",
            flush=True,
        )

        for local_path in (row_counts_path, fill_rates_path, conflicts_path, summary_path):
            s3_key = comparison_s3_key(args.comparison_prefix, tax_year, local_path.name)
            print(f"[compare] Uploading {local_path.name} -> s3://{args.bucket}/{s3_key}", flush=True)
            if should_skip_upload(local_path, args.bucket, s3_key, args.region, args.overwrite):
                print(f"[compare] Skip unchanged S3 object: s3://{args.bucket}/{s3_key}", flush=True)
                continue
            upload_file_with_progress(
                local_path,
                args.bucket,
                s3_key,
                args.region,
                extra_args={"ContentType": guess_content_type(local_path)},
            )
        print_elapsed(year_start, f"compare tax_year={tax_year}")

    print_elapsed(start, "Step 07")


if __name__ == "__main__":
    main()
