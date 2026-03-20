"""
Step 06: Upload the benchmark-filtered IRS SOI county CSVs and manifest to S3.
"""

from __future__ import annotations

import argparse
import time
from pathlib import Path

from common import (
    DEFAULT_S3_BUCKET,
    DEFAULT_S3_REGION,
    META_DIR,
    SILVER_PREFIX,
    STAGING_DIR,
    banner,
    compute_local_s3_match,
    filter_manifest_path,
    filtered_output_path,
    filtered_s3_key,
    guess_content_type,
    load_csv_rows,
    load_env_from_secrets,
    print_elapsed,
    resolve_release_and_write_metadata,
    s3_object_size,
    upload_file_with_progress,
    write_csv,
)


def _selected_source_types(source_type: str) -> list[str]:
    if source_type == "both":
        return ["agi", "noagi"]
    return [source_type]


def main() -> None:
    parser = argparse.ArgumentParser(description="Upload benchmark-filtered IRS SOI county CSVs to S3.")
    parser.add_argument("--year", default="latest", help="County release year or 'latest' (default: latest)")
    parser.add_argument("--bucket", default=DEFAULT_S3_BUCKET, help="Target S3 bucket")
    parser.add_argument("--region", default=DEFAULT_S3_REGION, help="Target S3 region")
    parser.add_argument("--metadata-dir", type=Path, default=META_DIR, help="Local metadata directory")
    parser.add_argument("--staging-dir", type=Path, default=STAGING_DIR, help="Local staging root directory")
    parser.add_argument("--silver-prefix", default=SILVER_PREFIX, help="S3 silver prefix")
    parser.add_argument(
        "--source-type",
        default="both",
        choices=("agi", "noagi", "both"),
        help="Which filtered CSV(s) to upload (default: both)",
    )
    args = parser.parse_args()

    start = time.perf_counter()
    banner("STEP 06 - UPLOAD FILTERED IRS SOI COUNTY CSVs TO S3")
    load_env_from_secrets()

    print(f"[upload] Requested year: {args.year}", flush=True)
    print(f"[upload] Bucket: {args.bucket}", flush=True)
    print(f"[upload] Region: {args.region}", flush=True)
    print(f"[upload] Silver prefix: {args.silver_prefix}", flush=True)
    print(f"[upload] Source type: {args.source_type}", flush=True)

    release = resolve_release_and_write_metadata(args.year, args.metadata_dir)
    tax_year = int(release["tax_year"])
    manifest_path = filter_manifest_path(args.staging_dir, tax_year)
    if not manifest_path.exists():
        raise FileNotFoundError(f"Filter manifest not found: {manifest_path}. Run step 05 first.")

    manifest_rows = load_csv_rows(manifest_path)
    selected_types = set(_selected_source_types(args.source_type))

    uploaded = 0
    for source_type in selected_types:
        local_path = filtered_output_path(args.staging_dir, tax_year, source_type)
        if not local_path.exists():
            raise FileNotFoundError(f"Filtered output not found: {local_path}. Run step 05 first.")
        s3_key = filtered_s3_key(args.silver_prefix, tax_year, local_path.name)
        print(f"[upload] Filtered CSV: {local_path} -> s3://{args.bucket}/{s3_key}", flush=True)
        file_start = time.perf_counter()
        upload_file_with_progress(
            local_path,
            args.bucket,
            s3_key,
            args.region,
            extra_args={"ContentType": guess_content_type(local_path)},
        )
        print_elapsed(file_start, f"upload {local_path.name}")
        uploaded += 1

        s3_bytes = s3_object_size(args.bucket, s3_key, args.region)
        local_bytes = local_path.stat().st_size
        size_match = compute_local_s3_match(local_bytes, s3_bytes)
        for row in manifest_rows:
            if row.get("source_type") == source_type:
                row["s3_bucket"] = args.bucket
                row["s3_key"] = s3_key
                row["s3_bytes"] = "" if s3_bytes is None else str(s3_bytes)
                row["size_match"] = "TRUE" if size_match else "FALSE"

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

    manifest_key = filtered_s3_key(args.silver_prefix, tax_year, manifest_path.name)
    print(f"[upload] Filter manifest: {manifest_path} -> s3://{args.bucket}/{manifest_key}", flush=True)
    upload_file_with_progress(
        manifest_path,
        args.bucket,
        manifest_key,
        args.region,
        extra_args={"ContentType": "text/csv"},
    )

    print(f"[upload] Uploaded filtered CSVs: {uploaded}", flush=True)
    print_elapsed(start, "Step 06")


if __name__ == "__main__":
    main()
