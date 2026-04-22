"""
Step 05: Upload filtered IRS EO BMF benchmark outputs to S3.
"""

from __future__ import annotations

import argparse
import time
from pathlib import Path

from common import (
    ANALYSIS_TAX_YEAR_MAX,
    ANALYSIS_TAX_YEAR_MIN,
    DEFAULT_S3_BUCKET,
    DEFAULT_S3_REGION,
    FILTER_MANIFEST_PATH,
    META_DIR,
    SILVER_META_PREFIX,
    SILVER_PREFIX,
    STAGING_DIR,
    banner,
    combined_filtered_output_path,
    combined_filtered_s3_key,
    filter_manifest_s3_key,
    guess_content_type,
    legacy_filtered_output_path,
    legacy_filtered_s3_key,
    load_env_from_secrets,
    print_elapsed,
    s3_object_size,
    should_skip_upload,
    upload_file_with_progress,
    yearly_filtered_output_path,
    yearly_filtered_s3_key,
)


def _upload_one(local_path: Path, bucket: str, region: str, key: str, overwrite: bool) -> None:
    if not local_path.exists():
        raise FileNotFoundError(f"Missing filtered artifact: {local_path}")
    print(f"[upload] {local_path} -> s3://{bucket}/{key}", flush=True)
    if should_skip_upload(local_path, bucket, key, region, overwrite):
        print(f"[upload] Skip unchanged: s3://{bucket}/{key}", flush=True)
    else:
        upload_file_with_progress(
            local_path,
            bucket,
            key,
            region,
            extra_args={"ContentType": guess_content_type(local_path)},
        )
    local_size = local_path.stat().st_size
    remote_size = s3_object_size(bucket, key, region)
    print(f"[upload] Size check local={local_size:,} remote={remote_size:,} match={local_size == remote_size}", flush=True)
    if local_size != remote_size:
        raise RuntimeError(f"S3 size mismatch for {local_path} -> s3://{bucket}/{key}")


def main() -> None:
    parser = argparse.ArgumentParser(description="Upload filtered IRS EO BMF benchmark outputs to S3.")
    parser.add_argument("--bucket", default=DEFAULT_S3_BUCKET, help="Target S3 bucket")
    parser.add_argument("--region", default=DEFAULT_S3_REGION, help="Target S3 region")
    parser.add_argument("--metadata-dir", type=Path, default=META_DIR, help="Local IRS EO BMF metadata directory")
    parser.add_argument("--staging-dir", type=Path, default=STAGING_DIR, help="Local IRS EO BMF staging directory")
    parser.add_argument("--silver-prefix", default=SILVER_PREFIX, help="S3 silver prefix")
    parser.add_argument("--silver-meta-prefix", default=SILVER_META_PREFIX, help="S3 silver metadata prefix")
    parser.add_argument("--overwrite", action="store_true", help="Upload even when remote bytes already match")
    args = parser.parse_args()

    start = time.perf_counter()
    banner("STEP 05 - UPLOAD FILTERED IRS EO BMF OUTPUTS")
    load_env_from_secrets()
    uploads = [
        (combined_filtered_output_path(args.staging_dir), combined_filtered_s3_key(args.silver_prefix)),
        (legacy_filtered_output_path(), legacy_filtered_s3_key(args.silver_prefix)),
        (args.metadata_dir / FILTER_MANIFEST_PATH.name, filter_manifest_s3_key(args.silver_meta_prefix)),
    ]
    for year in range(ANALYSIS_TAX_YEAR_MIN, ANALYSIS_TAX_YEAR_MAX + 1):
        uploads.append((yearly_filtered_output_path(year, args.staging_dir), yearly_filtered_s3_key(year, args.silver_prefix)))
    for local_path, s3_key in uploads:
        _upload_one(local_path, args.bucket, args.region, s3_key, args.overwrite)
    print_elapsed(start, "Step 05")


if __name__ == "__main__":
    main()
