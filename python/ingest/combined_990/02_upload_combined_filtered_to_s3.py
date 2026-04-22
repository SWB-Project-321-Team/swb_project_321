"""
Step 02: Upload the combined filtered 990 union + master parquets and metadata to S3.
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
    METADATA_S3_PREFIX,
    MASTER_OUTPUT_PARQUET,
    OUTPUT_PARQUET,
    SILVER_PREFIX,
    all_metadata_files,
    banner,
    combined_s3_key,
    guess_content_type,
    load_env_from_secrets,
    master_s3_key,
    metadata_s3_key,
    print_elapsed,
    print_transfer_settings,
    should_skip_upload,
    upload_file_with_progress,
    UPLOAD_WORKERS,
)


def main() -> None:
    parser = argparse.ArgumentParser(description="Upload the combined filtered 990 union + master parquets and metadata to S3.")
    parser.add_argument("--output", type=Path, default=OUTPUT_PARQUET, help="Combined union parquet path")
    parser.add_argument("--master-output", type=Path, default=MASTER_OUTPUT_PARQUET, help="Combined organization-year master parquet path")
    parser.add_argument("--bucket", default=DEFAULT_S3_BUCKET, help="Target S3 bucket")
    parser.add_argument("--region", default=DEFAULT_S3_REGION, help="Target S3 region")
    parser.add_argument("--silver-prefix", default=SILVER_PREFIX, help="S3 silver prefix for the combined parquet")
    parser.add_argument("--metadata-prefix", default=METADATA_S3_PREFIX, help="S3 metadata prefix")
    parser.add_argument("--overwrite", action="store_true", help="Upload even when the S3 object already matches local bytes")
    args = parser.parse_args()

    start = time.perf_counter()
    banner("STEP 02 - UPLOAD COMBINED FILTERED 990 UNION + MASTER TO S3")
    load_env_from_secrets()
    print_transfer_settings(label="combined-upload")

    metadata_files = all_metadata_files(include_verification=True)
    if not args.output.exists():
        raise FileNotFoundError(f"Combined union parquet not found: {args.output}. Run step 01 first.")
    if not args.master_output.exists():
        raise FileNotFoundError(f"Combined master parquet not found: {args.master_output}. Run step 01 first.")
    missing_metadata = [path for path in metadata_files if not path.exists()]
    if missing_metadata:
        missing_list = ", ".join(path.name for path in missing_metadata)
        raise FileNotFoundError(f"Combined metadata file(s) missing: {missing_list}. Run step 01 first.")

    union_key = combined_s3_key(args.silver_prefix)
    print(f"[upload] Union parquet: {args.output} -> s3://{args.bucket}/{union_key}", flush=True)
    if should_skip_upload(args.output, args.bucket, union_key, args.region, args.overwrite):
        print(f"[upload] Skip unchanged S3 object: s3://{args.bucket}/{union_key}", flush=True)
    else:
        file_start = time.perf_counter()
        upload_file_with_progress(
            args.output,
            args.bucket,
            union_key,
            args.region,
            extra_args={"ContentType": guess_content_type(args.output)},
        )
        print_elapsed(file_start, f"upload {args.output.name}")

    master_key = master_s3_key(args.silver_prefix)
    print(f"[upload] Master parquet: {args.master_output} -> s3://{args.bucket}/{master_key}", flush=True)
    if should_skip_upload(args.master_output, args.bucket, master_key, args.region, args.overwrite):
        print(f"[upload] Skip unchanged S3 object: s3://{args.bucket}/{master_key}", flush=True)
    else:
        file_start = time.perf_counter()
        upload_file_with_progress(
            args.master_output,
            args.bucket,
            master_key,
            args.region,
            extra_args={"ContentType": guess_content_type(args.master_output)},
        )
        print_elapsed(file_start, f"upload {args.master_output.name}")

    print(f"[upload] Metadata files queued: {len(metadata_files):,}", flush=True)
    worker_count = max(1, min(UPLOAD_WORKERS, len(metadata_files)))
    print(f"[upload] Metadata upload workers: {worker_count}", flush=True)

    def _upload_metadata(path: Path) -> tuple[str, bool]:
        key = metadata_s3_key(path.name, args.metadata_prefix)
        print(f"[upload] Metadata: {path} -> s3://{args.bucket}/{key}", flush=True)
        if should_skip_upload(path, args.bucket, key, args.region, args.overwrite):
            print(f"[upload] Skip unchanged S3 object: s3://{args.bucket}/{key}", flush=True)
            return path.name, False
        file_start = time.perf_counter()
        upload_file_with_progress(
            path,
            args.bucket,
            key,
            args.region,
            extra_args={"ContentType": guess_content_type(path)},
        )
        print_elapsed(file_start, f"upload {path.name}")
        return path.name, True

    uploaded_metadata = 0
    skipped_metadata = 0
    with ThreadPoolExecutor(max_workers=worker_count) as executor:
        future_to_path = {executor.submit(_upload_metadata, path): path for path in metadata_files}
        for future in tqdm(as_completed(future_to_path), total=len(future_to_path), desc="upload metadata", unit="file"):
            _, uploaded = future.result()
            if uploaded:
                uploaded_metadata += 1
            else:
                skipped_metadata += 1

    print(f"[upload] Uploaded metadata files: {uploaded_metadata}", flush=True)
    print(f"[upload] Skipped metadata files: {skipped_metadata}", flush=True)

    print_elapsed(start, "Step 02")


if __name__ == "__main__":
    main()
