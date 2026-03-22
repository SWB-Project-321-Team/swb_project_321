"""
Step 03: Upload the local NCCS e-Postcard raw assets and metadata to S3.
"""

from __future__ import annotations

import argparse
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

from tqdm import tqdm

from common import (
    DEFAULT_S3_BUCKET,
    DEFAULT_S3_REGION,
    LATEST_RELEASE_JSON,
    META_DIR,
    META_PREFIX,
    POSTCARD_PAGE_SNAPSHOT,
    POSTCARD_RAW_DIR,
    RAW_PREFIX,
    TQDM_KW,
    UPLOAD_WORKERS,
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
    parser = argparse.ArgumentParser(description="Upload NCCS e-Postcard raw assets and metadata to S3.")
    parser.add_argument("--snapshot-year", default="latest", help="Snapshot year or 'latest' (default: latest)")
    parser.add_argument(
        "--snapshot-months",
        default="all",
        help="Snapshot months to include: 'all' or comma-separated MM / YYYY-MM values",
    )
    parser.add_argument("--bucket", default=DEFAULT_S3_BUCKET, help="Target S3 bucket")
    parser.add_argument("--region", default=DEFAULT_S3_REGION, help="Target S3 region")
    parser.add_argument("--postcard-raw-dir", type=Path, default=POSTCARD_RAW_DIR, help="Local postcard raw root directory")
    parser.add_argument("--metadata-dir", type=Path, default=META_DIR, help="Local metadata directory")
    parser.add_argument("--raw-prefix", default=RAW_PREFIX, help="S3 raw prefix")
    parser.add_argument("--meta-prefix", default=META_PREFIX, help="S3 metadata prefix")
    parser.add_argument("--overwrite", action="store_true", help="Upload even when the S3 object already matches local bytes")
    args = parser.parse_args()

    start = time.perf_counter()
    banner("STEP 03 - UPLOAD NCCS E-POSTCARD RAW ASSETS TO S3")
    load_env_from_secrets()
    ensure_work_dirs(postcard_raw_dir=args.postcard_raw_dir, metadata_dir=args.metadata_dir)

    print(f"[upload] Requested snapshot year: {args.snapshot_year}", flush=True)
    print(f"[upload] Requested snapshot months: {args.snapshot_months}", flush=True)
    print(f"[upload] Bucket: {args.bucket}", flush=True)
    print(f"[upload] Region: {args.region}", flush=True)
    print(f"[upload] Raw prefix: {args.raw_prefix}", flush=True)
    print(f"[upload] Meta prefix: {args.meta_prefix}", flush=True)
    print(f"[upload] Overwrite: {args.overwrite}", flush=True)

    release = resolve_release_and_write_metadata(
        args.snapshot_year,
        args.metadata_dir,
        snapshot_months_arg=args.snapshot_months,
    )
    snapshot_year = int(release["snapshot_year"])
    manifest_path = release_manifest_path(args.metadata_dir, snapshot_year)
    latest_release_path = args.metadata_dir / LATEST_RELEASE_JSON.name
    postcard_page_path = args.metadata_dir / POSTCARD_PAGE_SNAPSHOT.name

    for required_path in (manifest_path, latest_release_path, postcard_page_path):
        if not required_path.exists():
            raise FileNotFoundError(f"Required metadata file not found: {required_path}. Run step 01/02 first.")

    uploaded_assets = 0
    skipped_assets = 0
    uploaded_meta = 0
    skipped_meta = 0
    pending_uploads: list[tuple[int, Path, str, str]] = []
    task_counter = 0

    for asset in sorted(selected_assets(release), key=lambda item: str(item["snapshot_month"])):
        local_path = local_asset_path(args.postcard_raw_dir, args.metadata_dir, asset)
        if not local_path.exists():
            raise FileNotFoundError(f"Local asset not found: {local_path}. Run step 02 first.")
        s3_key = asset_s3_key(args.raw_prefix, args.meta_prefix, asset)
        print(f"[upload] Asset: {local_path} -> s3://{args.bucket}/{s3_key}", flush=True)
        if should_skip_upload(local_path, args.bucket, s3_key, args.region, args.overwrite):
            print(f"[upload] Skip unchanged S3 object: s3://{args.bucket}/{s3_key}", flush=True)
            skipped_assets += 1
            continue
        task_counter += 1
        pending_uploads.append((task_counter, local_path, s3_key, guess_content_type(local_path)))

    for local_path in (latest_release_path, postcard_page_path, manifest_path):
        s3_key = meta_s3_key(args.meta_prefix, local_path.name)
        print(f"[upload] Metadata file: {local_path} -> s3://{args.bucket}/{s3_key}", flush=True)
        if should_skip_upload(local_path, args.bucket, s3_key, args.region, args.overwrite):
            print(f"[upload] Skip unchanged metadata object: s3://{args.bucket}/{s3_key}", flush=True)
            skipped_meta += 1
            continue
        task_counter += 1
        pending_uploads.append((task_counter, local_path, s3_key, guess_content_type(local_path)))

    if pending_uploads:
        worker_count = min(UPLOAD_WORKERS, len(pending_uploads))
        print(f"[upload] Parallel upload workers: {worker_count}", flush=True)

        def _upload_one(position: int, local_path: Path, s3_key: str, content_type: str) -> tuple[str, str, float]:
            file_start = time.perf_counter()
            upload_file_with_progress(
                local_path,
                args.bucket,
                s3_key,
                args.region,
                extra_args={"ContentType": content_type},
                position=position,
                desc=f"upload {local_path.name}",
            )
            return str(local_path), s3_key, time.perf_counter() - file_start

        with ThreadPoolExecutor(max_workers=worker_count) as executor:
            future_to_task = {
                executor.submit(_upload_one, position, local_path, s3_key, content_type): (local_path, s3_key)
                for position, local_path, s3_key, content_type in pending_uploads
            }
            with tqdm(total=len(pending_uploads), desc="upload postcard objects", unit="file", position=0, **TQDM_KW) as overall:
                for future in as_completed(future_to_task):
                    local_path, s3_key, elapsed = future.result()
                    overall.update(1)
                    if str(s3_key).startswith(args.raw_prefix.rstrip("/") + "/"):
                        uploaded_assets += 1
                    else:
                        uploaded_meta += 1
                    print(f"[time] upload {Path(local_path).name}: {elapsed:.1f}s ({elapsed / 60:.1f} min)", flush=True)

    print(f"[upload] Uploaded assets: {uploaded_assets}", flush=True)
    print(f"[upload] Skipped assets: {skipped_assets}", flush=True)
    print(f"[upload] Uploaded metadata files: {uploaded_meta}", flush=True)
    print(f"[upload] Skipped metadata files: {skipped_meta}", flush=True)
    print_elapsed(start, "Step 03")


if __name__ == "__main__":
    main()
