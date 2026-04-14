"""
Step 05: Build compact annual NCCS efile benchmark Parquets locally.
"""

from __future__ import annotations

import argparse
import time
from pathlib import Path

from common import (
    EFILE_RAW_DIR,
    GEOID_REFERENCE_CSV,
    META_DIR,
    SILVER_PREFIX,
    STAGING_DIR,
    START_YEAR_DEFAULT,
    ZIP_TO_COUNTY_CSV,
    banner,
    build_efile_year_to_benchmark,
    filter_manifest_path,
    filtered_output_path,
    filtered_s3_key,
    grouped_assets_by_year,
    load_env_from_secrets,
    local_asset_path,
    print_elapsed,
    resolve_release_and_write_metadata,
    write_csv,
)


def main() -> None:
    parser = argparse.ArgumentParser(description="Build compact annual NCCS efile benchmark Parquets locally.")
    parser.add_argument("--start-year", type=int, default=START_YEAR_DEFAULT, help="First tax year to include (default: 2022)")
    parser.add_argument("--raw-dir", type=Path, default=EFILE_RAW_DIR, help="Local efile raw root directory")
    parser.add_argument("--metadata-dir", type=Path, default=META_DIR, help="Local metadata directory")
    parser.add_argument("--staging-dir", type=Path, default=STAGING_DIR, help="Local staging root directory")
    parser.add_argument("--silver-prefix", default=SILVER_PREFIX, help="S3 silver prefix recorded in the manifest")
    parser.add_argument("--geoid-reference", type=Path, default=GEOID_REFERENCE_CSV, help="Benchmark GEOID reference CSV")
    parser.add_argument("--zip-to-county", type=Path, default=ZIP_TO_COUNTY_CSV, help="ZIP-to-county FIPS crosswalk")
    args = parser.parse_args()

    start = time.perf_counter()
    banner("STEP 05 - BUILD NCCS EFILE BENCHMARK PARQUETS")
    load_env_from_secrets()

    print(f"[build] Start year: {args.start_year}", flush=True)
    print(f"[build] Raw root: {args.raw_dir}", flush=True)
    print(f"[build] Metadata directory: {args.metadata_dir}", flush=True)
    print(f"[build] Staging root: {args.staging_dir}", flush=True)
    print(f"[build] GEOID reference: {args.geoid_reference}", flush=True)
    print(f"[build] ZIP-to-county: {args.zip_to_county}", flush=True)

    release = resolve_release_and_write_metadata(args.metadata_dir, start_year=args.start_year)
    grouped_assets = grouped_assets_by_year(release)

    for tax_year, year_assets in grouped_assets.items():
        output_path = filtered_output_path(args.staging_dir, tax_year)
        print(f"[build] Tax year: {tax_year}", flush=True)
        print(f"[build] Output file: {output_path}", flush=True)
        file_start = time.perf_counter()
        result = build_efile_year_to_benchmark(
            tax_year=tax_year,
            year_assets=year_assets,
            raw_dir=args.raw_dir,
            output_path=output_path,
            geoid_reference_path=args.geoid_reference,
            zip_to_county_path=args.zip_to_county,
        )
        output_bytes = output_path.stat().st_size
        source_csvs = ";".join(str(local_asset_path(args.raw_dir, args.metadata_dir, asset)) for asset in year_assets)
        manifest_path = filter_manifest_path(args.staging_dir, tax_year)
        manifest_rows = [
            {
                "tax_year": tax_year,
                "is_partial_year": result["is_partial_year"],
                "source_csvs": source_csvs,
                "input_row_count": result["input_row_count"],
                "header_form_row_count": result["header_form_row_count"],
                "benchmark_admitted_row_count": result["benchmark_admitted_row_count"],
                "joined_row_count": result["joined_row_count"],
                "duplicate_group_count": result["duplicate_group_count"],
                "post_dedupe_row_count": result["post_dedupe_row_count"],
                "output_row_count": result["output_row_count"],
                "matched_county_fips_count": result["matched_county_fips_count"],
                "geoid_reference_path": str(args.geoid_reference),
                "zip_to_county_path": str(args.zip_to_county),
                "local_filtered_path": str(output_path),
                "local_filtered_bytes": output_bytes,
                "s3_bucket": "",
                "s3_key": filtered_s3_key(args.silver_prefix, tax_year, output_path.name),
                "s3_bytes": "",
                "size_match": "",
            }
        ]
        fieldnames = [
            "tax_year",
            "is_partial_year",
            "source_csvs",
            "input_row_count",
            "header_form_row_count",
            "benchmark_admitted_row_count",
            "joined_row_count",
            "duplicate_group_count",
            "post_dedupe_row_count",
            "output_row_count",
            "matched_county_fips_count",
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
        print(
            f"[build] Wrote {output_path} | input_rows={result['input_row_count']:,} | "
            f"form_rows={result['header_form_row_count']:,} | "
            f"benchmark_admitted_rows={result['benchmark_admitted_row_count']:,} | "
            f"joined_rows={result['joined_row_count']:,} | post_dedupe_rows={result['post_dedupe_row_count']:,} | "
            f"output_rows={result['output_row_count']:,}",
            flush=True,
        )
        print_elapsed(file_start, f"build efile benchmark tax_year={tax_year}")

    print(f"[build] Benchmark years written: {len(grouped_assets)}", flush=True)
    print_elapsed(start, "Step 05")


if __name__ == "__main__":
    main()
