"""
Step 05: Filter the NCCS Core raw CSVs to benchmark counties and write local staging CSVs.
"""

from __future__ import annotations

import argparse
import time
from pathlib import Path

from common import (
    BRIDGE_BMF_DIR,
    CORE_RAW_DIR,
    GEOID_REFERENCE_CSV,
    META_DIR,
    SILVER_PREFIX,
    STAGING_DIR,
    ZIP_TO_COUNTY_CSV,
    banner,
    filter_core_file_to_benchmark,
    filter_manifest_path,
    filtered_output_path,
    filtered_s3_key,
    load_env_from_secrets,
    load_geoid_reference_set,
    load_zip_to_county_map,
    local_asset_path,
    prepare_bmf_bridge_dataframe,
    print_elapsed,
    resolve_release_and_write_metadata,
    selected_bmf_assets,
    selected_core_csv_assets,
    write_csv,
    year_staging_dir,
)


def main() -> None:
    parser = argparse.ArgumentParser(description="Filter NCCS Core CSVs to benchmark counties.")
    parser.add_argument("--year", default="latest_common", help="Core release year or 'latest_common' (default: latest_common)")
    parser.add_argument("--core-raw-dir", type=Path, default=CORE_RAW_DIR, help="Local Core raw root directory")
    parser.add_argument("--bridge-dir", type=Path, default=BRIDGE_BMF_DIR, help="Local Unified BMF bridge directory")
    parser.add_argument("--metadata-dir", type=Path, default=META_DIR, help="Local metadata directory")
    parser.add_argument("--staging-dir", type=Path, default=STAGING_DIR, help="Local staging root directory")
    parser.add_argument("--silver-prefix", default=SILVER_PREFIX, help="S3 silver prefix recorded in the manifest")
    parser.add_argument("--geoid-reference", type=Path, default=GEOID_REFERENCE_CSV, help="Benchmark GEOID reference CSV")
    parser.add_argument("--zip-to-county", type=Path, default=ZIP_TO_COUNTY_CSV, help="ZIP to county FIPS crosswalk")
    parser.add_argument(
        "--source-types",
        default="all",
        help="Optional comma-separated Core CSV asset types to filter (default: all selected Core CSVs)",
    )
    parser.add_argument("--benchmark-states", default=None, help="Optional comma-separated benchmark state override")
    parser.add_argument("--chunk-size", type=int, default=100_000, help="Chunk size for streamed Core CSV filtering")
    args = parser.parse_args()

    start = time.perf_counter()
    banner("STEP 05 - FILTER NCCS CORE CSVs TO BENCHMARK COUNTIES")
    load_env_from_secrets()

    print(f"[filter] Requested year: {args.year}", flush=True)
    print(f"[filter] Core raw root: {args.core_raw_dir}", flush=True)
    print(f"[filter] Bridge root: {args.bridge_dir}", flush=True)
    print(f"[filter] Metadata directory: {args.metadata_dir}", flush=True)
    print(f"[filter] Staging root: {args.staging_dir}", flush=True)
    print(f"[filter] GEOID reference: {args.geoid_reference}", flush=True)
    print(f"[filter] ZIP-to-county: {args.zip_to_county}", flush=True)
    print(f"[filter] Source types: {args.source_types}", flush=True)
    print(f"[filter] Chunk size: {args.chunk_size}", flush=True)

    release = resolve_release_and_write_metadata(
        args.year,
        args.metadata_dir,
        geoid_reference_path=args.geoid_reference,
        benchmark_states_arg=args.benchmark_states,
    )
    tax_year = int(release["tax_year"])
    release_staging_dir = year_staging_dir(args.staging_dir, tax_year)
    release_staging_dir.mkdir(parents=True, exist_ok=True)

    geoid_reference_set, geoid_to_region = load_geoid_reference_set(args.geoid_reference)
    zip_to_county = load_zip_to_county_map(args.zip_to_county)
    print(f"[filter] Benchmark counties loaded: {len(geoid_reference_set)}", flush=True)
    print(f"[filter] ZIP crosswalk rows loaded: {len(zip_to_county)}", flush=True)

    local_bmf_paths: list[Path] = []
    for asset in selected_bmf_assets(release):
        local_path = local_asset_path(args.core_raw_dir, args.bridge_dir, args.metadata_dir, asset)
        if not local_path.exists():
            raise FileNotFoundError(f"Unified BMF bridge file not found: {local_path}. Run step 02 first.")
        print(f"[filter] Using Unified BMF bridge file: {local_path}", flush=True)
        local_bmf_paths.append(local_path)

    bridge_start = time.perf_counter()
    bridge_df = prepare_bmf_bridge_dataframe(local_bmf_paths, geoid_reference_set, geoid_to_region, zip_to_county)
    print(f"[filter] Prepared bridge rows: {len(bridge_df):,}", flush=True)
    print_elapsed(bridge_start, "prepare Unified BMF bridge")

    manifest_rows: list[dict[str, object]] = []
    for asset in selected_core_csv_assets(release, args.source_types):
        local_source_path = local_asset_path(args.core_raw_dir, args.bridge_dir, args.metadata_dir, asset)
        if not local_source_path.exists():
            raise FileNotFoundError(f"Local Core file not found: {local_source_path}. Run step 02 first.")

        output_path = filtered_output_path(args.staging_dir, tax_year, str(asset["filename"]))
        s3_key = filtered_s3_key(args.silver_prefix, tax_year, output_path.name)
        print(f"[filter] Source file: {local_source_path}", flush=True)
        print(f"[filter] Output file: {output_path}", flush=True)

        file_start = time.perf_counter()
        input_row_count, output_row_count, matched_counties = filter_core_file_to_benchmark(
            local_source_path,
            output_path,
            bridge_df,
            chunk_size=args.chunk_size,
        )
        output_bytes = output_path.stat().st_size
        print(
            f"[filter] Wrote {output_path} | input_rows={input_row_count:,} | "
            f"output_rows={output_row_count:,} | matched_counties={matched_counties:,}",
            flush=True,
        )
        print_elapsed(file_start, f"filter {local_source_path.name}")

        manifest_rows.append(
            {
                "tax_year": tax_year,
                "source_type": asset["asset_type"],
                "source_csv": str(local_source_path),
                "input_row_count": input_row_count,
                "output_row_count": output_row_count,
                "matched_county_fips_count": matched_counties,
                "geoid_reference_path": str(args.geoid_reference),
                "zip_to_county_path": str(args.zip_to_county),
                "bridge_state_count": len(local_bmf_paths),
                "bridge_rows": len(bridge_df),
                "local_filtered_path": str(output_path),
                "local_filtered_bytes": output_bytes,
                "s3_bucket": "",
                "s3_key": s3_key,
                "s3_bytes": "",
                "size_match": "",
            }
        )

    manifest_path = filter_manifest_path(args.staging_dir, tax_year)
    fieldnames = [
        "tax_year",
        "source_type",
        "source_csv",
        "input_row_count",
        "output_row_count",
        "matched_county_fips_count",
        "geoid_reference_path",
        "zip_to_county_path",
        "bridge_state_count",
        "bridge_rows",
        "local_filtered_path",
        "local_filtered_bytes",
        "s3_bucket",
        "s3_key",
        "s3_bytes",
        "size_match",
    ]
    write_csv(manifest_path, manifest_rows, fieldnames)
    print(f"[filter] Filtered outputs written: {len(manifest_rows)}", flush=True)
    print_elapsed(start, "Step 05")


if __name__ == "__main__":
    main()
