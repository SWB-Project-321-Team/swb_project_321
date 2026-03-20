"""
Step 03: Upload the local IRS SOI county raw assets and metadata to S3.
"""

from __future__ import annotations

import argparse
import time
from pathlib import Path

from common import (
    DEFAULT_S3_BUCKET,
    DEFAULT_S3_REGION,
    META_DIR,
    META_PREFIX,
    RAW_DIR,
    RAW_PREFIX,
    banner,
    ensure_work_dirs,
    guess_content_type,
    load_env_from_secrets,
    meta_s3_key,
    print_elapsed,
    raw_s3_key,
    release_manifest_path,
    resolve_release_and_write_metadata,
    upload_file_with_progress,
    year_raw_dir,
)


def main() -> None:
    parser = argparse.ArgumentParser(description="Upload IRS SOI county raw assets and metadata to S3.")
    parser.add_argument("--year", default="latest", help="County release year or 'latest' (default: latest)")
    parser.add_argument("--bucket", default=DEFAULT_S3_BUCKET, help="Target S3 bucket")
    parser.add_argument("--region", default=DEFAULT_S3_REGION, help="Target S3 region")
    parser.add_argument("--raw-dir", type=Path, default=RAW_DIR, help="Local raw root directory")
    parser.add_argument("--metadata-dir", type=Path, default=META_DIR, help="Local metadata directory")
    parser.add_argument("--raw-prefix", default=RAW_PREFIX, help="S3 raw prefix")
    parser.add_argument("--meta-prefix", default=META_PREFIX, help="S3 metadata prefix")
    args = parser.parse_args()

    start = time.perf_counter()
    banner("STEP 03 - UPLOAD IRS SOI COUNTY RAW ASSETS TO S3")
    load_env_from_secrets()
    ensure_work_dirs(raw_dir=args.raw_dir, metadata_dir=args.metadata_dir)

    print(f"[upload] Requested year: {args.year}", flush=True)
    print(f"[upload] Bucket: {args.bucket}", flush=True)
    print(f"[upload] Region: {args.region}", flush=True)
    print(f"[upload] Raw prefix: {args.raw_prefix}", flush=True)
    print(f"[upload] Meta prefix: {args.meta_prefix}", flush=True)

    release = resolve_release_and_write_metadata(args.year, args.metadata_dir)
    tax_year = int(release["tax_year"])
    release_raw_dir = year_raw_dir(args.raw_dir, tax_year)
    manifest_path = release_manifest_path(args.metadata_dir, tax_year)
    latest_release_path = args.metadata_dir / "latest_release.json"

    if not manifest_path.exists():
        raise FileNotFoundError(f"Raw manifest not found: {manifest_path}. Run step 02 first.")
    if not latest_release_path.exists():
        raise FileNotFoundError(f"Release metadata not found: {latest_release_path}. Run step 01 or 02 first.")

    uploaded_raw = 0
    for asset in release["assets"]:
        local_path = release_raw_dir / asset["filename"]
        if not local_path.exists():
            raise FileNotFoundError(f"Raw asset not found locally: {local_path}. Run step 02 first.")
        s3_key = raw_s3_key(args.raw_prefix, tax_year, local_path.name)
        print(f"[upload] Raw asset: {local_path} -> s3://{args.bucket}/{s3_key}", flush=True)
        file_start = time.perf_counter()
        upload_file_with_progress(
            local_path,
            args.bucket,
            s3_key,
            args.region,
            extra_args={"ContentType": guess_content_type(local_path)},
        )
        print_elapsed(file_start, f"upload {local_path.name}")
        uploaded_raw += 1

    uploaded_meta = 0
    for local_path in (latest_release_path, manifest_path):
        s3_key = meta_s3_key(args.meta_prefix, local_path.name)
        print(f"[upload] Metadata file: {local_path} -> s3://{args.bucket}/{s3_key}", flush=True)
        file_start = time.perf_counter()
        upload_file_with_progress(
            local_path,
            args.bucket,
            s3_key,
            args.region,
            extra_args={"ContentType": guess_content_type(local_path)},
        )
        print_elapsed(file_start, f"upload {local_path.name}")
        uploaded_meta += 1

    print(f"[upload] Uploaded raw assets: {uploaded_raw}", flush=True)
    print(f"[upload] Uploaded metadata files: {uploaded_meta}", flush=True)
    print_elapsed(start, "Step 03")


if __name__ == "__main__":
    main()
