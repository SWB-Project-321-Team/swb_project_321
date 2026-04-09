"""
Step 04: Filter raw IRS EO BMF state files to benchmark scope before combining.
"""

from __future__ import annotations

import argparse
import time
from pathlib import Path
from typing import Any

import pandas as pd
from tqdm import tqdm

from common import (
    ANALYSIS_TAX_YEAR_MAX,
    ANALYSIS_TAX_YEAR_MIN,
    FILTER_MANIFEST_PATH,
    GEOID_REFERENCE_CSV,
    META_DIR,
    RAW_DIR,
    STAGING_DIR,
    ZIP_TO_COUNTY_CSV,
    banner,
    benchmark_reference_maps,
    combined_filtered_output_path,
    derive_tax_year_series,
    discover_raw_state_files,
    ensure_work_dirs,
    legacy_filtered_output_path,
    load_env_from_secrets,
    normalize_state_series,
    normalize_zip_series,
    normalized_direct_field_count,
    print_elapsed,
    state_code_from_path,
    yearly_filtered_output_path,
)
from ingest._shared.analysis_support import numeric_from_text, normalize_ein


STANDARDIZED_COLUMNS = [
    "ein",
    "org_name",
    "state",
    "zip5",
    "county_fips",
    "region",
    "tax_period",
    "tax_year",
    "raw_ntee_code",
    "raw_subsection_code",
    "raw_total_revenue_amount",
    "raw_total_assets_amount",
    "raw_total_income_amount",
    "raw_ntee_code_source_column",
    "raw_subsection_code_source_column",
    "raw_total_revenue_amount_source_column",
    "raw_total_assets_amount_source_column",
    "raw_total_income_amount_source_column",
    "source_state_code",
    "source_state_file",
    "source_row_number",
    "benchmark_match_source",
    "tax_year_derivation_basis",
    "state_matches_benchmark_zip",
]


def _blank_to_na(series: pd.Series) -> pd.Series:
    text = series.astype("string").fillna("").str.strip()
    return text.mask(text.eq(""), pd.NA)


def _standardize_state_frame(
    raw_df: pd.DataFrame,
    *,
    source_path: Path,
    benchmark_zip_map: dict[str, str],
    geoid_to_region: dict[str, str] | None,
    benchmark_zip_state_map: dict[str, str] | None,
) -> tuple[pd.DataFrame, dict[str, Any]]:
    """
    Standardize one raw IRS EO BMF state file into the admitted benchmark shape.

    The goal here is to fix scope and dedupe as close to the source as possible:
    rows are admitted by benchmark ZIP/county first, then tax year is derived,
    then the surviving rows are prepared for the cross-state combine.
    """
    frame = raw_df.copy()
    frame["source_row_number"] = range(len(frame))
    frame["source_state_code"] = state_code_from_path(source_path).upper()
    frame["source_state_file"] = source_path.name
    frame["benchmark_match_source"] = "zip_to_county_benchmark_zip_map"

    frame["zip5"] = normalize_zip_series(frame["ZIP"])
    frame["county_fips"] = frame["zip5"].map(benchmark_zip_map).astype("string")
    frame["region"] = frame["county_fips"].map(geoid_to_region).astype("string") if geoid_to_region else pd.Series(pd.NA, index=frame.index, dtype="string")
    benchmark_state = frame["zip5"].map(benchmark_zip_state_map).astype("string") if benchmark_zip_state_map else pd.Series(pd.NA, index=frame.index, dtype="string")
    raw_state = normalize_state_series(frame["STATE"]) if "STATE" in frame.columns else pd.Series(pd.NA, index=frame.index, dtype="string")
    frame["state_matches_benchmark_zip"] = (
        raw_state.notna() & benchmark_state.notna() & raw_state.eq(benchmark_state)
    ).astype("boolean")

    rows_after_zip_geography_filter = int(frame["county_fips"].notna().sum())
    state_admitted_mask = frame["county_fips"].notna()
    if benchmark_zip_state_map is not None:
        state_validation_mask = raw_state.isna() | benchmark_state.isna() | raw_state.eq(benchmark_state)
        state_mismatch_dropped_count = int((frame["county_fips"].notna() & ~state_validation_mask).sum())
        state_admitted_mask &= state_validation_mask
    else:
        state_mismatch_dropped_count = 0

    frame = frame.loc[state_admitted_mask].copy()
    frame["state"] = raw_state.loc[frame.index].combine_first(benchmark_state.loc[frame.index]).astype("string")
    frame["org_name"] = _blank_to_na(frame["NAME"])
    frame["ein"] = frame["EIN"].map(normalize_ein).astype("string")
    invalid_ein_dropped_count = int(frame["ein"].fillna("").eq("").sum())
    frame = frame.loc[frame["ein"].fillna("").ne("")].copy()

    frame["tax_period"] = _blank_to_na(frame["TAX_PERIOD"])
    frame["tax_year"] = derive_tax_year_series(frame["tax_period"])
    frame["tax_year_derivation_basis"] = pd.Series("TAX_PERIOD[:4]", index=frame.index, dtype="string")
    frame.loc[frame["tax_year"].isna(), "tax_year_derivation_basis"] = pd.NA
    missing_tax_year_dropped_count = int(frame["tax_year"].isna().sum())
    in_range_mask = frame["tax_year"].isin([str(year) for year in range(ANALYSIS_TAX_YEAR_MIN, ANALYSIS_TAX_YEAR_MAX + 1)])
    out_of_range_tax_year_dropped_count = int((frame["tax_year"].notna() & ~in_range_mask).sum())
    frame = frame.loc[in_range_mask].copy()

    frame["raw_ntee_code"] = _blank_to_na(frame["NTEE_CD"]).str.upper()
    frame["raw_subsection_code"] = _blank_to_na(frame["SUBSECTION"]).str.upper()
    frame["raw_total_revenue_amount"] = numeric_from_text(frame["REVENUE_AMT"])
    frame["raw_total_assets_amount"] = numeric_from_text(frame["ASSET_AMT"])
    frame["raw_total_income_amount"] = numeric_from_text(frame["INCOME_AMT"])
    frame["raw_ntee_code_source_column"] = pd.Series("NTEE_CD", index=frame.index, dtype="string")
    frame.loc[frame["raw_ntee_code"].isna(), "raw_ntee_code_source_column"] = pd.NA
    frame["raw_subsection_code_source_column"] = pd.Series("SUBSECTION", index=frame.index, dtype="string")
    frame.loc[frame["raw_subsection_code"].isna(), "raw_subsection_code_source_column"] = pd.NA
    frame["raw_total_revenue_amount_source_column"] = pd.Series("REVENUE_AMT", index=frame.index, dtype="string")
    frame.loc[frame["raw_total_revenue_amount"].isna(), "raw_total_revenue_amount_source_column"] = pd.NA
    frame["raw_total_assets_amount_source_column"] = pd.Series("ASSET_AMT", index=frame.index, dtype="string")
    frame.loc[frame["raw_total_assets_amount"].isna(), "raw_total_assets_amount_source_column"] = pd.NA
    frame["raw_total_income_amount_source_column"] = pd.Series("INCOME_AMT", index=frame.index, dtype="string")
    frame.loc[frame["raw_total_income_amount"].isna(), "raw_total_income_amount_source_column"] = pd.NA

    standardized = frame[STANDARDIZED_COLUMNS].copy()
    stats = {
        "artifact_type": "state_file",
        "source_state_code": state_code_from_path(source_path).lower(),
        "source_state_file": source_path.name,
        "input_row_count": int(len(raw_df)),
        "rows_after_zip_geography_filter": rows_after_zip_geography_filter,
        "state_mismatch_dropped_count": state_mismatch_dropped_count,
        "invalid_ein_dropped_count": invalid_ein_dropped_count,
        "missing_tax_year_dropped_count": missing_tax_year_dropped_count,
        "out_of_range_tax_year_dropped_count": out_of_range_tax_year_dropped_count,
        "admitted_analysis_scope_row_count": int(len(standardized)),
        "duplicate_group_count_before_resolution": pd.NA,
        "duplicate_rows_dropped_count": pd.NA,
        "output_row_count": int(len(standardized)),
    }
    return standardized, stats


def _resolve_cross_state_duplicates(filtered_df: pd.DataFrame) -> tuple[pd.DataFrame, int, int]:
    """Resolve duplicate EIN-year rows after state files have already been filtered to benchmark scope."""
    duplicate_group_count = int((filtered_df.groupby(["ein", "tax_year"]).size() > 1).sum())
    if duplicate_group_count == 0:
        return filtered_df.copy(), 0, 0

    sort_df = filtered_df.copy()
    sort_df["__state_match_rank"] = sort_df["state_matches_benchmark_zip"].fillna(False).astype(bool).astype(int)
    sort_df["__direct_field_count"] = normalized_direct_field_count(sort_df)
    sort_df = sort_df.sort_values(
        by=["ein", "tax_year", "__state_match_rank", "__direct_field_count", "source_state_file", "source_row_number"],
        ascending=[True, True, False, False, True, True],
        kind="mergesort",
    )
    deduped = sort_df.drop_duplicates(subset=["ein", "tax_year"], keep="first").copy()
    duplicate_rows_dropped_count = int(len(sort_df) - len(deduped))
    deduped.drop(columns=["__state_match_rank", "__direct_field_count"], inplace=True)
    return deduped.reset_index(drop=True), duplicate_group_count, duplicate_rows_dropped_count


def build_filtered_outputs(
    *,
    raw_dir: Path = RAW_DIR,
    metadata_dir: Path = META_DIR,
    staging_dir: Path = STAGING_DIR,
    geoid_reference_path: Path = GEOID_REFERENCE_CSV,
    zip_to_county_path: Path = ZIP_TO_COUNTY_CSV,
) -> dict[str, Any]:
    """Build the canonical filtered IRS EO BMF benchmark outputs."""
    ensure_work_dirs(raw_dir=raw_dir, metadata_dir=metadata_dir, staging_dir=staging_dir)
    raw_paths = discover_raw_state_files(raw_dir)
    if not raw_paths:
        raise FileNotFoundError(f"No raw IRS EO BMF state files found under {raw_dir}")

    maps = benchmark_reference_maps(geoid_reference_path, zip_to_county_path)
    state_frames: list[pd.DataFrame] = []
    manifest_rows: list[dict[str, Any]] = []

    print(f"[filter] Raw state files discovered: {len(raw_paths)}", flush=True)
    for raw_path in tqdm(raw_paths, desc="filter IRS EO BMF states", unit="state"):
        print(f"[filter] Processing {raw_path}", flush=True)
        raw_df = pd.read_csv(raw_path, dtype=str, low_memory=False)
        standardized, stats = _standardize_state_frame(
            raw_df,
            source_path=raw_path,
            benchmark_zip_map=maps["benchmark_zip_map"],
            geoid_to_region=maps["geoid_to_region"],
            benchmark_zip_state_map=maps["benchmark_zip_state_map"],
        )
        print(
            "[filter] "
            f"{raw_path.name}: input={stats['input_row_count']:,}, "
            f"zip_admitted={stats['rows_after_zip_geography_filter']:,}, "
            f"in_scope={stats['admitted_analysis_scope_row_count']:,}",
            flush=True,
        )
        state_frames.append(standardized)
        manifest_rows.append(stats)

    combined = pd.concat(state_frames, ignore_index=True) if state_frames else pd.DataFrame(columns=STANDARDIZED_COLUMNS)
    print(f"[filter] Combined filtered rows before duplicate resolution: {len(combined):,}", flush=True)
    deduped, duplicate_group_count, duplicate_rows_dropped_count = _resolve_cross_state_duplicates(combined)
    print(
        f"[filter] Cross-state duplicate groups resolved: {duplicate_group_count:,}; "
        f"rows dropped: {duplicate_rows_dropped_count:,}",
        flush=True,
    )

    combined_output = combined_filtered_output_path(staging_dir)
    combined_output.parent.mkdir(parents=True, exist_ok=True)
    deduped.sort_values(["tax_year", "region", "county_fips", "ein"], kind="mergesort").to_parquet(combined_output, index=False)
    legacy_output = legacy_filtered_output_path()
    legacy_output.parent.mkdir(parents=True, exist_ok=True)
    deduped.to_parquet(legacy_output, index=False)
    print(f"[filter] Wrote combined filtered parquet: {combined_output}", flush=True)
    print(f"[filter] Wrote legacy compatibility parquet: {legacy_output}", flush=True)

    year_rows: list[dict[str, Any]] = []
    for year in range(ANALYSIS_TAX_YEAR_MIN, ANALYSIS_TAX_YEAR_MAX + 1):
        year_df = deduped.loc[deduped["tax_year"].astype("string").eq(str(year))].copy()
        output_path = yearly_filtered_output_path(year, staging_dir)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        year_df.to_parquet(output_path, index=False)
        print(f"[filter] Wrote year={year} benchmark parquet with {len(year_df):,} rows: {output_path}", flush=True)
        year_rows.append(
            {
                "artifact_type": "benchmark_year",
                "source_state_code": pd.NA,
                "source_state_file": pd.NA,
                "input_row_count": pd.NA,
                "rows_after_zip_geography_filter": pd.NA,
                "state_mismatch_dropped_count": pd.NA,
                "invalid_ein_dropped_count": pd.NA,
                "missing_tax_year_dropped_count": pd.NA,
                "out_of_range_tax_year_dropped_count": pd.NA,
                "admitted_analysis_scope_row_count": pd.NA,
                "duplicate_group_count_before_resolution": pd.NA,
                "duplicate_rows_dropped_count": pd.NA,
                "output_row_count": int(len(year_df)),
                "tax_year": str(year),
            }
        )

    manifest_rows.append(
        {
            "artifact_type": "combined_filtered",
            "source_state_code": pd.NA,
            "source_state_file": pd.NA,
            "input_row_count": int(len(combined)),
            "rows_after_zip_geography_filter": pd.NA,
            "state_mismatch_dropped_count": pd.NA,
            "invalid_ein_dropped_count": pd.NA,
            "missing_tax_year_dropped_count": pd.NA,
            "out_of_range_tax_year_dropped_count": pd.NA,
            "admitted_analysis_scope_row_count": int(len(combined)),
            "duplicate_group_count_before_resolution": duplicate_group_count,
            "duplicate_rows_dropped_count": duplicate_rows_dropped_count,
            "output_row_count": int(len(deduped)),
            "tax_year": pd.NA,
        }
    )
    manifest_rows.extend(year_rows)
    manifest_df = pd.DataFrame(manifest_rows)
    manifest_path = metadata_dir / FILTER_MANIFEST_PATH.name
    manifest_df.to_csv(manifest_path, index=False)
    print(f"[filter] Wrote filter manifest: {manifest_path}", flush=True)

    return {
        "combined_row_count_before_resolution": int(len(combined)),
        "combined_row_count_after_resolution": int(len(deduped)),
        "duplicate_group_count_before_resolution": duplicate_group_count,
        "duplicate_rows_dropped_count": duplicate_rows_dropped_count,
        "filter_manifest_path": str(manifest_path),
        "combined_output_path": str(combined_output),
        "legacy_output_path": str(legacy_output),
    }


def main() -> None:
    parser = argparse.ArgumentParser(description="Filter raw IRS EO BMF state files to benchmark scope.")
    parser.add_argument("--raw-dir", type=Path, default=RAW_DIR, help="Local raw IRS EO BMF directory")
    parser.add_argument("--metadata-dir", type=Path, default=META_DIR, help="Local IRS EO BMF metadata directory")
    parser.add_argument("--staging-dir", type=Path, default=STAGING_DIR, help="Local IRS EO BMF staging directory")
    parser.add_argument("--geoid-reference", type=Path, default=GEOID_REFERENCE_CSV, help="Benchmark GEOID reference CSV")
    parser.add_argument("--zip-to-county", type=Path, default=ZIP_TO_COUNTY_CSV, help="ZIP-to-county crosswalk CSV")
    args = parser.parse_args()

    start = time.perf_counter()
    banner("STEP 04 - FILTER IRS EO BMF TO BENCHMARK LOCAL")
    load_env_from_secrets()
    print(f"[config] Analysis tax-year window: {ANALYSIS_TAX_YEAR_MIN}-{ANALYSIS_TAX_YEAR_MAX}", flush=True)
    build_filtered_outputs(
        raw_dir=args.raw_dir,
        metadata_dir=args.metadata_dir,
        staging_dir=args.staging_dir,
        geoid_reference_path=args.geoid_reference,
        zip_to_county_path=args.zip_to_county,
    )
    print_elapsed(start, "Step 04")


if __name__ == "__main__":
    main()
