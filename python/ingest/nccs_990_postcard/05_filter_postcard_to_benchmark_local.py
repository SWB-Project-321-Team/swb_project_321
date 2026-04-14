"""
Step 05: Filter the NCCS e-Postcard monthly snapshots to benchmark counties and combine them.
"""

from __future__ import annotations

import argparse
import json
import time
from pathlib import Path

from common import (
    GEOID_REFERENCE_CSV,
    META_DIR,
    POSTCARD_TAX_YEAR_START_DEFAULT,
    POSTCARD_RAW_DIR,
    SILVER_PREFIX,
    STAGING_DIR,
    ZIP_TO_COUNTY_CSV,
    banner,
    build_tax_year_window_derivative,
    filter_manifest_path,
    filter_postcard_year_to_benchmark,
    filtered_output_path,
    filtered_tax_year_window_manifest_path,
    filtered_tax_year_window_output_path,
    filtered_tax_year_window_parquet_path,
    filtered_s3_key,
    load_geoid_state_map,
    load_env_from_secrets,
    load_geoid_reference_set,
    load_zip_to_county_map,
    local_asset_path,
    print_elapsed,
    resolve_release_and_write_metadata,
    selected_assets,
    snapshot_staging_dir,
    write_csv,
)


def main() -> None:
    parser = argparse.ArgumentParser(description="Filter NCCS e-Postcard CSVs to benchmark counties.")
    parser.add_argument("--snapshot-year", default="latest", help="Snapshot year or 'latest' (default: latest)")
    parser.add_argument(
        "--snapshot-months",
        default="all",
        help="Snapshot months to include: 'all' or comma-separated MM / YYYY-MM values",
    )
    parser.add_argument("--postcard-raw-dir", type=Path, default=POSTCARD_RAW_DIR, help="Local postcard raw root directory")
    parser.add_argument("--metadata-dir", type=Path, default=META_DIR, help="Local metadata directory")
    parser.add_argument("--staging-dir", type=Path, default=STAGING_DIR, help="Local staging root directory")
    parser.add_argument("--silver-prefix", default=SILVER_PREFIX, help="S3 silver prefix recorded in the manifest")
    parser.add_argument("--geoid-reference", type=Path, default=GEOID_REFERENCE_CSV, help="Benchmark GEOID reference CSV")
    parser.add_argument("--zip-to-county", type=Path, default=ZIP_TO_COUNTY_CSV, help="ZIP to county FIPS crosswalk")
    parser.add_argument("--chunk-size", type=int, default=100_000, help="Chunk size for streamed postcard filtering")
    parser.add_argument("--tax-year-start", type=int, default=POSTCARD_TAX_YEAR_START_DEFAULT, help="Minimum filing tax year for the secondary postcard derivative")
    args = parser.parse_args()

    start = time.perf_counter()
    banner("STEP 05 - FILTER NCCS E-POSTCARD TO BENCHMARK COUNTIES")
    load_env_from_secrets()

    print(f"[filter] Requested snapshot year: {args.snapshot_year}", flush=True)
    print(f"[filter] Requested snapshot months: {args.snapshot_months}", flush=True)
    print(f"[filter] Postcard raw root: {args.postcard_raw_dir}", flush=True)
    print(f"[filter] Metadata directory: {args.metadata_dir}", flush=True)
    print(f"[filter] Staging root: {args.staging_dir}", flush=True)
    print(f"[filter] GEOID reference: {args.geoid_reference}", flush=True)
    print(f"[filter] ZIP-to-county: {args.zip_to_county}", flush=True)
    print(f"[filter] Chunk size: {args.chunk_size}", flush=True)
    print(f"[filter] Secondary tax-year window start: {args.tax_year_start}", flush=True)

    release = resolve_release_and_write_metadata(
        args.snapshot_year,
        args.metadata_dir,
        snapshot_months_arg=args.snapshot_months,
    )
    snapshot_year = int(release["snapshot_year"])
    available_months = list(release["available_snapshot_months"])
    assets = selected_assets(release)
    release_staging_dir = snapshot_staging_dir(args.staging_dir, snapshot_year)
    release_staging_dir.mkdir(parents=True, exist_ok=True)

    geoid_reference_set, geoid_to_region = load_geoid_reference_set(args.geoid_reference)
    geoid_to_state = load_geoid_state_map(args.geoid_reference)
    zip_to_county = load_zip_to_county_map(args.zip_to_county)
    print(f"[filter] Benchmark counties loaded: {len(geoid_reference_set)}", flush=True)
    print(f"[filter] ZIP crosswalk rows loaded: {len(zip_to_county)}", flush=True)
    print(f"[filter] Benchmark state mappings loaded: {len(geoid_to_state)}", flush=True)
    print(f"[filter] Snapshot months to combine: {', '.join(available_months)}", flush=True)

    output_path = filtered_output_path(args.staging_dir, snapshot_year)
    file_start = time.perf_counter()
    result = filter_postcard_year_to_benchmark(
        assets,
        args.postcard_raw_dir,
        args.metadata_dir,
        output_path,
        geoid_reference_set,
        geoid_to_region,
        geoid_to_state,
        zip_to_county,
        chunk_size=args.chunk_size,
    )
    output_bytes = output_path.stat().st_size
    print(
        f"[filter] Wrote {output_path} | input_rows={result['input_row_count']:,} | "
        f"matched_rows={result['matched_row_count']:,} | output_rows={result['output_row_count']:,} | "
        f"deduped_eins={result['deduped_ein_count']:,}",
        flush=True,
    )
    print_elapsed(file_start, f"filter postcard snapshot_year={snapshot_year}")

    s3_key = filtered_s3_key(args.silver_prefix, snapshot_year, output_path.name)
    source_csvs = ";".join(str(local_asset_path(args.postcard_raw_dir, args.metadata_dir, asset)) for asset in assets)
    manifest_rows = [
        {
            "snapshot_year": snapshot_year,
            "available_snapshot_months": ",".join(available_months),
            "source_csvs": source_csvs,
            "input_row_count": result["input_row_count"],
            "matched_row_count": result["matched_row_count"],
            "output_row_count": result["output_row_count"],
            "deduped_ein_count": result["deduped_ein_count"],
            "matched_county_fips_count": result["matched_county_fips_count"],
            "zip_match_source_counts": json.dumps(result["zip_match_source_counts"], sort_keys=True),
            "state_mismatch_rejected_row_count": result["state_mismatch_rejected_row_count"],
            "geoid_reference_path": str(args.geoid_reference),
            "zip_to_county_path": str(args.zip_to_county),
            "local_filtered_path": str(output_path),
            "local_filtered_bytes": output_bytes,
            "s3_bucket": "",
            "s3_key": s3_key,
            "s3_bytes": "",
            "size_match": "",
        }
    ]

    manifest_path = filter_manifest_path(args.staging_dir, snapshot_year)
    fieldnames = [
        "snapshot_year",
        "available_snapshot_months",
        "source_csvs",
        "input_row_count",
        "matched_row_count",
        "output_row_count",
        "deduped_ein_count",
        "matched_county_fips_count",
        "zip_match_source_counts",
        "state_mismatch_rejected_row_count",
        "geoid_reference_path",
        "zip_to_county_path",
        "local_filtered_path",
        "local_filtered_bytes",
        "s3_bucket",
        "s3_key",
        "s3_bytes",
        "size_match",
    ]
    write_csv(manifest_path, manifest_rows, fieldnames)

    print("[filter] Building the secondary postcard derivative restricted to the requested tax-year window.", flush=True)
    window_output_path = filtered_tax_year_window_output_path(args.staging_dir, snapshot_year, args.tax_year_start)
    window_parquet_path = filtered_tax_year_window_parquet_path(args.staging_dir, snapshot_year, args.tax_year_start)
    window_start = time.perf_counter()
    window_stats = build_tax_year_window_derivative(
        output_path,
        window_output_path,
        output_parquet_path=window_parquet_path,
        tax_year_start=args.tax_year_start,
    )
    window_output_bytes = window_output_path.stat().st_size
    window_parquet_bytes = window_parquet_path.stat().st_size
    print(
        f"[filter] Wrote {window_output_path} and {window_parquet_path} | combined_rows={window_stats['input_row_count']:,} | "
        f"window_rows={window_stats['output_row_count']:,} | min_tax_year={window_stats['min_tax_year_in_output'] or '<blank>'} | "
        f"max_tax_year={window_stats['max_tax_year_in_output'] or '<blank>'}",
        flush=True,
    )
    print_elapsed(window_start, f"derive postcard tax_year>={args.tax_year_start}")

    window_s3_key = filtered_s3_key(args.silver_prefix, snapshot_year, window_output_path.name)
    window_manifest_rows = [
        {
            "snapshot_year": snapshot_year,
            "available_snapshot_months": ",".join(available_months),
            "tax_year_start": args.tax_year_start,
            "min_tax_year_in_output": window_stats["min_tax_year_in_output"],
            "max_tax_year_in_output": window_stats["max_tax_year_in_output"],
            "source_combined_csv": str(output_path),
            "combined_snapshot_output_row_count": result["output_row_count"],
            "window_output_row_count": window_stats["output_row_count"],
            "local_filtered_csv_path": str(window_output_path),
            "local_filtered_csv_bytes": window_output_bytes,
            "local_filtered_parquet_path": str(window_parquet_path),
            "local_filtered_parquet_bytes": window_parquet_bytes,
            "geoid_reference_path": str(args.geoid_reference),
            "zip_to_county_path": str(args.zip_to_county),
            "s3_bucket": "",
            "s3_key": window_s3_key,
            "s3_bytes": "",
            "size_match": "",
        }
    ]
    window_manifest_path = filtered_tax_year_window_manifest_path(args.staging_dir, snapshot_year, args.tax_year_start)
    window_fieldnames = [
        "snapshot_year",
        "available_snapshot_months",
        "tax_year_start",
        "min_tax_year_in_output",
        "max_tax_year_in_output",
        "source_combined_csv",
        "combined_snapshot_output_row_count",
        "window_output_row_count",
        "local_filtered_csv_path",
        "local_filtered_csv_bytes",
        "local_filtered_parquet_path",
        "local_filtered_parquet_bytes",
        "geoid_reference_path",
        "zip_to_county_path",
        "s3_bucket",
        "s3_key",
        "s3_bytes",
        "size_match",
    ]
    write_csv(window_manifest_path, window_manifest_rows, window_fieldnames)
    print(f"[filter] Filtered outputs written: {len(manifest_rows) + len(window_manifest_rows)}", flush=True)
    print_elapsed(start, "Step 05")


if __name__ == "__main__":
    main()
