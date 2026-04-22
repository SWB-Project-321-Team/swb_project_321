"""
Step 06: Upload the filtered NCCS efile annual benchmark outputs to S3.
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
    START_YEAR_DEFAULT,
    banner,
    filter_manifest_path,
    filtered_output_path,
    filtered_s3_key,
    grouped_assets_by_year,
    guess_content_type,
    load_env_from_secrets,
    print_elapsed,
    resolve_release_and_write_metadata,
    should_skip_upload,
    upload_file_with_progress,
)


def main() -> None:
    parser = argparse.ArgumentParser(description="Upload the filtered NCCS efile annual benchmark outputs to S3.")
    parser.add_argument("--start-year", type=int, default=START_YEAR_DEFAULT, help="First tax year to include (default: 2022)")
    parser.add_argument("--bucket", default=DEFAULT_S3_BUCKET, help="Target S3 bucket")
    parser.add_argument("--region", default=DEFAULT_S3_REGION, help="Target S3 region")
    parser.add_argument("--metadata-dir", type=Path, default=META_DIR, help="Local metadata directory")
    parser.add_argument("--staging-dir", type=Path, default=STAGING_DIR, help="Local staging root directory")
    parser.add_argument("--silver-prefix", default=SILVER_PREFIX, help="S3 silver prefix")
    parser.add_argument("--overwrite", action="store_true", help="Upload even when the S3 object already matches local bytes")
    args = parser.parse_args()

    start = time.perf_counter()
    banner("STEP 06 - UPLOAD FILTERED NCCS EFILE OUTPUTS TO S3")
    load_env_from_secrets()

    print(f"[upload] Start year: {args.start_year}", flush=True)
    print(f"[upload] Bucket: {args.bucket}", flush=True)
    print(f"[upload] Region: {args.region}", flush=True)
    print(f"[upload] Silver prefix: {args.silver_prefix}", flush=True)
    print(f"[upload] Overwrite: {args.overwrite}", flush=True)

    release = resolve_release_and_write_metadata(args.metadata_dir, start_year=args.start_year)
    grouped_assets = grouped_assets_by_year(release)
    uploaded_outputs = 0
    skipped_outputs = 0

    for tax_year in sorted(grouped_assets):
        output_path = filtered_output_path(args.staging_dir, tax_year)
        manifest_path = filter_manifest_path(args.staging_dir, tax_year)
        if not output_path.exists() or not manifest_path.exists():
            raise FileNotFoundError(f"Filtered output or manifest not found for tax_year={tax_year}. Run step 05 first.")

        output_key = filtered_s3_key(args.silver_prefix, tax_year, output_path.name)
        print(f"[upload] Filtered output: {output_path} -> s3://{args.bucket}/{output_key}", flush=True)
        if should_skip_upload(output_path, args.bucket, output_key, args.region, args.overwrite):
            print(f"[upload] Skip unchanged S3 object: s3://{args.bucket}/{output_key}", flush=True)
            skipped_outputs += 1
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
            uploaded_outputs += 1

        manifest_key = f"{args.silver_prefix.rstrip('/')}/tax_year={tax_year}/{manifest_path.name}"
        print(f"[upload] Filter manifest: {manifest_path} -> s3://{args.bucket}/{manifest_key}", flush=True)
        if should_skip_upload(manifest_path, args.bucket, manifest_key, args.region, args.overwrite):
            print(f"[upload] Skip unchanged S3 object: s3://{args.bucket}/{manifest_key}", flush=True)
        else:
            file_start = time.perf_counter()
            upload_file_with_progress(
                manifest_path,
                args.bucket,
                manifest_key,
                args.region,
                extra_args={"ContentType": "text/csv"},
            )
            print_elapsed(file_start, f"upload {manifest_path.name}")

    print(f"[upload] Uploaded filtered outputs: {uploaded_outputs}", flush=True)
    print(f"[upload] Skipped filtered outputs: {skipped_outputs}", flush=True)
    print_elapsed(start, "Step 06")


if __name__ == "__main__":
    main()
