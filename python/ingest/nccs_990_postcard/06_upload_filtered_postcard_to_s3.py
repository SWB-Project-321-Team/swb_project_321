"""
Step 06: Upload the benchmark-filtered NCCS e-Postcard CSV and manifest to S3.
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
    should_skip_upload,
    upload_file_with_progress,
    write_csv,
)


def main() -> None:
    parser = argparse.ArgumentParser(description="Upload benchmark-filtered NCCS e-Postcard CSVs to S3.")
    parser.add_argument("--snapshot-year", default="latest", help="Snapshot year or 'latest' (default: latest)")
    parser.add_argument(
        "--snapshot-months",
        default="all",
        help="Snapshot months to include: 'all' or comma-separated MM / YYYY-MM values",
    )
    parser.add_argument("--bucket", default=DEFAULT_S3_BUCKET, help="Target S3 bucket")
    parser.add_argument("--region", default=DEFAULT_S3_REGION, help="Target S3 region")
    parser.add_argument("--metadata-dir", type=Path, default=META_DIR, help="Local metadata directory")
    parser.add_argument("--staging-dir", type=Path, default=STAGING_DIR, help="Local staging root directory")
    parser.add_argument("--silver-prefix", default=SILVER_PREFIX, help="S3 silver prefix")
    parser.add_argument("--overwrite", action="store_true", help="Upload even when the S3 object already matches local bytes")
    args = parser.parse_args()

    start = time.perf_counter()
    banner("STEP 06 - UPLOAD FILTERED NCCS E-POSTCARD CSV TO S3")
    load_env_from_secrets()

    print(f"[upload] Requested snapshot year: {args.snapshot_year}", flush=True)
    print(f"[upload] Requested snapshot months: {args.snapshot_months}", flush=True)
    print(f"[upload] Bucket: {args.bucket}", flush=True)
    print(f"[upload] Region: {args.region}", flush=True)
    print(f"[upload] Silver prefix: {args.silver_prefix}", flush=True)
    print(f"[upload] Overwrite: {args.overwrite}", flush=True)

    release = resolve_release_and_write_metadata(
        args.snapshot_year,
        args.metadata_dir,
        snapshot_months_arg=args.snapshot_months,
    )
    snapshot_year = int(release["snapshot_year"])
    output_path = filtered_output_path(args.staging_dir, snapshot_year)
    manifest_path = filter_manifest_path(args.staging_dir, snapshot_year)
    if not manifest_path.exists() or not output_path.exists():
        raise FileNotFoundError(f"Filtered output or manifest not found for snapshot_year={snapshot_year}. Run step 05 first.")

    output_key = filtered_s3_key(args.silver_prefix, snapshot_year, output_path.name)
    print(f"[upload] Filtered CSV: {output_path} -> s3://{args.bucket}/{output_key}", flush=True)
    if should_skip_upload(output_path, args.bucket, output_key, args.region, args.overwrite):
        print(f"[upload] Skip unchanged S3 object: s3://{args.bucket}/{output_key}", flush=True)
    else:
        file_start = time.perf_counter()
        upload_file_with_progress(
            output_path,
            args.bucket,
            output_key,
            args.region,
            extra_args={"ContentType": guess_content_type(output_path)},
        )
        print_elapsed(file_start, f"upload {output_path.name}")

    output_s3_bytes = s3_object_size(args.bucket, output_key, args.region)
    output_local_bytes = output_path.stat().st_size
    output_size_match = compute_local_s3_match(output_local_bytes, output_s3_bytes)

    manifest_rows = load_csv_rows(manifest_path)
    if manifest_rows:
        manifest_rows[0]["s3_bucket"] = args.bucket
        manifest_rows[0]["s3_key"] = output_key
        manifest_rows[0]["s3_bytes"] = "" if output_s3_bytes is None else str(output_s3_bytes)
        manifest_rows[0]["size_match"] = "TRUE" if output_size_match else "FALSE"

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

    manifest_key = filtered_s3_key(args.silver_prefix, snapshot_year, manifest_path.name)
    print(f"[upload] Filter manifest: {manifest_path} -> s3://{args.bucket}/{manifest_key}", flush=True)
    if should_skip_upload(manifest_path, args.bucket, manifest_key, args.region, args.overwrite):
        print(f"[upload] Skip unchanged manifest object: s3://{args.bucket}/{manifest_key}", flush=True)
    else:
        upload_file_with_progress(
            manifest_path,
            args.bucket,
            manifest_key,
            args.region,
            extra_args={"ContentType": "text/csv"},
        )

    print(f"[upload] Output size match: {'TRUE' if output_size_match else 'FALSE'}", flush=True)
    print_elapsed(start, "Step 06")


if __name__ == "__main__":
    main()
