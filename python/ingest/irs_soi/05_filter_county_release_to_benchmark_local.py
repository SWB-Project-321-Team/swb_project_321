"""
Step 05: Filter the county CSVs to benchmark counties and write local staging CSVs.
"""

from __future__ import annotations

import argparse
import time
from pathlib import Path

from common import (
    GEOID_REFERENCE_CSV,
    META_DIR,
    RAW_DIR,
    SILVER_PREFIX,
    STAGING_DIR,
    banner,
    filter_county_dataframe,
    filter_manifest_path,
    filtered_output_path,
    filtered_s3_key,
    load_geoid_reference_set,
    load_env_from_secrets,
    print_elapsed,
    read_irs_csv,
    resolve_release_and_write_metadata,
    select_release_assets,
    write_csv,
    year_raw_dir,
    year_staging_dir,
)


def _source_type_from_asset_type(asset_type: str) -> str:
    if asset_type == "county_csv_agi":
        return "agi"
    if asset_type == "county_csv_noagi":
        return "noagi"
    raise ValueError(f"Unsupported asset type for filtering: {asset_type}")


def main() -> None:
    parser = argparse.ArgumentParser(description="Filter IRS SOI county CSVs to benchmark counties.")
    parser.add_argument("--year", default="latest", help="County release year or 'latest' (default: latest)")
    parser.add_argument("--raw-dir", type=Path, default=RAW_DIR, help="Local raw root directory")
    parser.add_argument("--metadata-dir", type=Path, default=META_DIR, help="Local metadata directory")
    parser.add_argument("--staging-dir", type=Path, default=STAGING_DIR, help="Local staging root directory")
    parser.add_argument("--silver-prefix", default=SILVER_PREFIX, help="S3 silver prefix recorded in the manifest")
    parser.add_argument("--geoid-reference", type=Path, default=GEOID_REFERENCE_CSV, help="Benchmark GEOID reference CSV")
    parser.add_argument(
        "--source-type",
        default="both",
        choices=("agi", "noagi", "both"),
        help="Which county CSV(s) to filter (default: both)",
    )
    args = parser.parse_args()

    start = time.perf_counter()
    banner("STEP 05 - FILTER IRS SOI COUNTY CSVs TO BENCHMARK COUNTIES")
    load_env_from_secrets()

    print(f"[filter] Requested year: {args.year}", flush=True)
    print(f"[filter] Raw root: {args.raw_dir}", flush=True)
    print(f"[filter] Staging root: {args.staging_dir}", flush=True)
    print(f"[filter] GEOID reference: {args.geoid_reference}", flush=True)
    print(f"[filter] Source type: {args.source_type}", flush=True)

    release = resolve_release_and_write_metadata(args.year, args.metadata_dir)
    tax_year = int(release["tax_year"])
    release_raw_dir = year_raw_dir(args.raw_dir, tax_year)
    release_staging_dir = year_staging_dir(args.staging_dir, tax_year)
    release_staging_dir.mkdir(parents=True, exist_ok=True)
    geoid_reference_set, geoid_to_region = load_geoid_reference_set(args.geoid_reference)

    manifest_rows: list[dict[str, object]] = []
    for asset in select_release_assets(release, args.source_type):
        source_type = _source_type_from_asset_type(asset["asset_type"])
        local_source_path = release_raw_dir / asset["filename"]
        if not local_source_path.exists():
            raise FileNotFoundError(f"Raw county CSV not found: {local_source_path}. Run step 02 first.")

        print(f"[filter] Source file: {local_source_path}", flush=True)
        file_start = time.perf_counter()
        df = read_irs_csv(local_source_path)
        input_row_count = len(df)
        filtered_df, matched_counties = filter_county_dataframe(df, geoid_reference_set, geoid_to_region)
        output_path = filtered_output_path(args.staging_dir, tax_year, source_type)
        filtered_df.to_csv(output_path, index=False)
        output_row_count = len(filtered_df)
        output_bytes = output_path.stat().st_size
        s3_key = filtered_s3_key(args.silver_prefix, tax_year, output_path.name)

        print(
            f"[filter] Wrote {output_path} | input_rows={input_row_count:,} | "
            f"output_rows={output_row_count:,} | matched_counties={matched_counties}",
            flush=True,
        )
        print_elapsed(file_start, f"filter {local_source_path.name}")

        manifest_rows.append(
            {
                "tax_year": tax_year,
                "source_type": source_type,
                "source_csv": str(local_source_path),
                "input_row_count": input_row_count,
                "output_row_count": output_row_count,
                "matched_county_fips_count": matched_counties,
                "geoid_reference_path": str(args.geoid_reference),
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
