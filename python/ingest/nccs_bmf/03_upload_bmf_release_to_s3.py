"""
Step 03: Upload the local NCCS BMF raw assets and metadata to S3.
"""

from __future__ import annotations

import argparse
import time
from pathlib import Path

from common import (
    BMF_CATALOG_SNAPSHOT,
    BMF_DATASET_SNAPSHOT,
    BMF_RAW_DIR,
    DEFAULT_S3_BUCKET,
    DEFAULT_S3_REGION,
    LATEST_RELEASE_JSON,
    META_DIR,
    META_PREFIX,
    RAW_PREFIX,
    START_YEAR_DEFAULT,
    asset_s3_key,
    banner,
    ensure_work_dirs,
    guess_content_type,
    load_env_from_secrets,
    local_asset_path,
    meta_s3_key,
    print_elapsed,
    release_manifest_path,
    resolve_release_and_write_metadata,
    selected_assets,
    should_skip_upload,
    upload_file_with_progress,
)


def main() -> None:
    parser = argparse.ArgumentParser(description="Upload the local NCCS BMF raw assets and metadata to S3.")
    parser.add_argument("--start-year", type=int, default=START_YEAR_DEFAULT, help="First year to include (default: 2022)")
    parser.add_argument("--bucket", default=DEFAULT_S3_BUCKET, help="Target S3 bucket")
    parser.add_argument("--region", default=DEFAULT_S3_REGION, help="Target S3 region")
    parser.add_argument("--raw-dir", type=Path, default=BMF_RAW_DIR, help="Local BMF raw root directory")
    parser.add_argument("--metadata-dir", type=Path, default=META_DIR, help="Local metadata directory")
    parser.add_argument("--raw-prefix", default=RAW_PREFIX, help="S3 raw prefix")
    parser.add_argument("--meta-prefix", default=META_PREFIX, help="S3 metadata prefix")
    parser.add_argument("--overwrite", action="store_true", help="Upload even when the S3 object already matches local bytes")
    args = parser.parse_args()

    start = time.perf_counter()
    banner("STEP 03 - UPLOAD NCCS BMF RAW ASSETS TO S3")
    load_env_from_secrets()
    ensure_work_dirs(raw_dir=args.raw_dir, metadata_dir=args.metadata_dir)

    print(f"[upload] Start year: {args.start_year}", flush=True)
    print(f"[upload] Bucket: {args.bucket}", flush=True)
    print(f"[upload] Region: {args.region}", flush=True)
    print(f"[upload] Raw prefix: {args.raw_prefix}", flush=True)
    print(f"[upload] Meta prefix: {args.meta_prefix}", flush=True)
    print(f"[upload] Overwrite: {args.overwrite}", flush=True)

    release = resolve_release_and_write_metadata(args.metadata_dir, start_year=args.start_year)
    manifest_path = release_manifest_path(args.metadata_dir, args.start_year)
    latest_release_path = args.metadata_dir / LATEST_RELEASE_JSON.name
    dataset_snapshot_path = args.metadata_dir / BMF_DATASET_SNAPSHOT.name
    catalog_snapshot_path = args.metadata_dir / BMF_CATALOG_SNAPSHOT.name

    for required_path in (manifest_path, latest_release_path, dataset_snapshot_path, catalog_snapshot_path):
        if not required_path.exists():
            raise FileNotFoundError(f"Required metadata file not found: {required_path}. Run step 01/02 first.")

    uploaded_assets = 0
    skipped_assets = 0
    for asset in selected_assets(release):
        local_path = local_asset_path(args.raw_dir, args.metadata_dir, asset)
        if not local_path.exists():
            raise FileNotFoundError(f"Local asset not found: {local_path}. Run step 02 first.")
        s3_key = asset_s3_key(args.raw_prefix, args.meta_prefix, asset)
        print(f"[upload] Asset: {local_path} -> s3://{args.bucket}/{s3_key}", flush=True)
        if should_skip_upload(local_path, args.bucket, s3_key, args.region, args.overwrite):
            print(f"[upload] Skip unchanged S3 object: s3://{args.bucket}/{s3_key}", flush=True)
            skipped_assets += 1
            continue
        file_start = time.perf_counter()
        upload_file_with_progress(
            local_path,
            args.bucket,
            s3_key,
            args.region,
            extra_args={"ContentType": guess_content_type(local_path)},
        )
        print_elapsed(file_start, f"upload {local_path.name}")
        uploaded_assets += 1

    uploaded_meta = 0
    skipped_meta = 0
    for local_path in (latest_release_path, dataset_snapshot_path, catalog_snapshot_path, manifest_path):
        s3_key = meta_s3_key(args.meta_prefix, local_path.name)
        print(f"[upload] Metadata file: {local_path} -> s3://{args.bucket}/{s3_key}", flush=True)
        if should_skip_upload(local_path, args.bucket, s3_key, args.region, args.overwrite):
            print(f"[upload] Skip unchanged metadata object: s3://{args.bucket}/{s3_key}", flush=True)
            skipped_meta += 1
            continue
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

    print(f"[upload] Uploaded assets: {uploaded_assets}", flush=True)
    print(f"[upload] Skipped assets: {skipped_assets}", flush=True)
    print(f"[upload] Uploaded metadata files: {uploaded_meta}", flush=True)
    print(f"[upload] Skipped metadata files: {skipped_meta}", flush=True)
    print_elapsed(start, "Step 03")


if __name__ == "__main__":
    main()
