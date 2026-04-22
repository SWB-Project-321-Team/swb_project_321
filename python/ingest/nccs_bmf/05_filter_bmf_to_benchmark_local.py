"""
Step 05: Filter the NCCS BMF yearly raw assets to benchmark counties and write local staging Parquets.
"""

from __future__ import annotations

import argparse
import time
from pathlib import Path

from common import (
    BMF_RAW_DIR,
    GEOID_REFERENCE_CSV,
    META_DIR,
    SILVER_PREFIX,
    STAGING_DIR,
    START_YEAR_DEFAULT,
    ZIP_TO_COUNTY_CSV,
    banner,
    build_bmf_exact_year_lookup,
    filter_bmf_file_to_benchmark,
    filter_manifest_path,
    exact_year_lookup_output_path,
    filtered_output_path,
    filtered_s3_key,
    load_env_from_secrets,
    local_asset_path,
    print_elapsed,
    resolve_release_and_write_metadata,
    selected_assets,
    write_csv,
)


def main() -> None:
    parser = argparse.ArgumentParser(description="Filter the NCCS BMF yearly raw assets to benchmark counties.")
    parser.add_argument("--start-year", type=int, default=START_YEAR_DEFAULT, help="First year to include (default: 2022)")
    parser.add_argument("--raw-dir", type=Path, default=BMF_RAW_DIR, help="Local BMF raw root directory")
    parser.add_argument("--metadata-dir", type=Path, default=META_DIR, help="Local metadata directory")
    parser.add_argument("--staging-dir", type=Path, default=STAGING_DIR, help="Local staging root directory")
    parser.add_argument("--silver-prefix", default=SILVER_PREFIX, help="S3 silver prefix recorded in the manifest")
    parser.add_argument("--geoid-reference", type=Path, default=GEOID_REFERENCE_CSV, help="Benchmark GEOID reference CSV")
    parser.add_argument("--zip-to-county", type=Path, default=ZIP_TO_COUNTY_CSV, help="ZIP-to-county FIPS crosswalk")
    args = parser.parse_args()

    start = time.perf_counter()
    banner("STEP 05 - FILTER NCCS BMF TO BENCHMARK COUNTIES")
    load_env_from_secrets()

    print(f"[filter] Start year: {args.start_year}", flush=True)
    print(f"[filter] Raw root: {args.raw_dir}", flush=True)
    print(f"[filter] Metadata directory: {args.metadata_dir}", flush=True)
    print(f"[filter] Staging root: {args.staging_dir}", flush=True)
    print(f"[filter] GEOID reference: {args.geoid_reference}", flush=True)
    print(f"[filter] ZIP-to-county: {args.zip_to_county}", flush=True)

    release = resolve_release_and_write_metadata(args.metadata_dir, start_year=args.start_year)
    manifest_rows: list[dict[str, object]] = []

    for asset in selected_assets(release):
        local_source_path = local_asset_path(args.raw_dir, args.metadata_dir, asset)
        if not local_source_path.exists():
            raise FileNotFoundError(f"Local BMF file not found: {local_source_path}. Run step 02 first.")

        output_path = filtered_output_path(args.staging_dir, int(asset["snapshot_year"]))
        lookup_output_path = exact_year_lookup_output_path(args.staging_dir, int(asset["snapshot_year"]))
        s3_key = filtered_s3_key(args.silver_prefix, int(asset["snapshot_year"]), output_path.name)
        lookup_s3_key = filtered_s3_key(args.silver_prefix, int(asset["snapshot_year"]), lookup_output_path.name)
        print(f"[filter] Source file: {local_source_path}", flush=True)
        print(f"[filter] Output file: {output_path}", flush=True)
        print(f"[lookup] Output file: {lookup_output_path}", flush=True)

        file_start = time.perf_counter()
        result = filter_bmf_file_to_benchmark(
            asset=asset,
            local_bmf_path=local_source_path,
            output_path=output_path,
            geoid_reference_path=args.geoid_reference,
            zip_to_county_path=args.zip_to_county,
        )
        output_bytes = output_path.stat().st_size
        lookup_result = build_bmf_exact_year_lookup(
            asset=asset,
            local_bmf_path=local_source_path,
            output_path=lookup_output_path,
        )
        lookup_bytes = lookup_output_path.stat().st_size
        print(
            f"[filter] Wrote {output_path} | input_rows={result['input_row_count']:,} | "
            f"rows_after_zip_geography={result['rows_after_zip_geography_filter']:,} | "
            f"output_rows={result['output_row_count']:,} | matched_counties={result['matched_county_fips_count']:,} | "
            f"state_mismatch_dropped={result['state_mismatch_dropped_count']:,}",
            flush=True,
        )
        print_elapsed(file_start, f"filter {local_source_path.name}")

        manifest_rows.append(
            {
                "snapshot_year": asset["snapshot_year"],
                "snapshot_month": asset["snapshot_month"],
                "source_period": asset["source_period"],
                "year_basis": asset["year_basis"],
                "source_csv": str(local_source_path),
                "input_row_count": result["input_row_count"],
                "rows_after_zip_geography_filter": result["rows_after_zip_geography_filter"],
                "output_row_count": result["output_row_count"],
                "matched_county_fips_count": result["matched_county_fips_count"],
                "schema_variant": result["schema_variant"],
                "zip_column_used": result["zip_column_used"],
                "state_validation_applied": result["state_validation_applied"],
                "state_mismatch_dropped_count": result["state_mismatch_dropped_count"],
                "geoid_reference_path": result["geoid_reference_path"],
                "zip_to_county_path": result["zip_to_county_path"],
                "local_filtered_path": str(output_path),
                "local_filtered_bytes": output_bytes,
                "lookup_output_row_count": lookup_result["lookup_output_row_count"],
                "lookup_duplicate_group_count": lookup_result["lookup_duplicate_group_count"],
                "lookup_source_variant": lookup_result["lookup_source_variant"],
                "local_lookup_path": str(lookup_output_path),
                "local_lookup_bytes": lookup_bytes,
                "s3_bucket": "",
                "s3_key": s3_key,
                "lookup_s3_key": lookup_s3_key,
                "s3_bytes": "",
                "size_match": "",
            }
        )

    manifest_path = filter_manifest_path(args.staging_dir, args.start_year)
    fieldnames = [
        "snapshot_year",
        "snapshot_month",
        "source_period",
        "year_basis",
        "source_csv",
        "input_row_count",
        "rows_after_zip_geography_filter",
        "output_row_count",
        "matched_county_fips_count",
        "schema_variant",
        "zip_column_used",
        "state_validation_applied",
        "state_mismatch_dropped_count",
        "geoid_reference_path",
        "zip_to_county_path",
        "local_filtered_path",
        "local_filtered_bytes",
        "lookup_output_row_count",
        "lookup_duplicate_group_count",
        "lookup_source_variant",
        "local_lookup_path",
        "local_lookup_bytes",
        "s3_bucket",
        "s3_key",
        "lookup_s3_key",
        "s3_bytes",
        "size_match",
    ]
    write_csv(manifest_path, manifest_rows, fieldnames)
    print(f"[filter] Filtered outputs written: {len(manifest_rows)}", flush=True)
    print_elapsed(start, "Step 05")


if __name__ == "__main__":
    main()
