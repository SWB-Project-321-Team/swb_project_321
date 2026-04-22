"""
Step 02: Upload raw IRS EO BMF state CSVs and the raw manifest to S3.
"""

from __future__ import annotations

import argparse
import time
from pathlib import Path

import pandas as pd

from common import (
    DEFAULT_S3_BUCKET,
    DEFAULT_S3_REGION,
    META_DIR,
    RAW_DIR,
    RAW_MANIFEST_PATH,
    RAW_META_PREFIX,
    RAW_PREFIX,
    banner,
    discover_raw_state_files,
    guess_content_type,
    load_env_from_secrets,
    print_elapsed,
    raw_manifest_s3_key,
    raw_s3_key,
    s3_object_size,
    should_skip_upload,
    state_code_from_path,
    upload_file_with_progress,
)


def _upload_one(local_path: Path, bucket: str, region: str, key: str, overwrite: bool) -> None:
    if not local_path.exists():
        raise FileNotFoundError(f"Missing upload artifact: {local_path}")
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


def upload_raw_release(
    *,
    raw_dir: Path = RAW_DIR,
    metadata_dir: Path = META_DIR,
    bucket: str = DEFAULT_S3_BUCKET,
    region: str = DEFAULT_S3_REGION,
    raw_prefix: str = RAW_PREFIX,
    raw_meta_prefix: str = RAW_META_PREFIX,
    overwrite: bool = False,
) -> pd.DataFrame:
    """Upload all discovered raw IRS EO BMF state files and the raw manifest."""
    rows: list[dict[str, object]] = []
    raw_paths = discover_raw_state_files(raw_dir)
    if not raw_paths:
        raise FileNotFoundError(f"No raw IRS EO BMF state files found under {raw_dir}")
    for raw_path in raw_paths:
        state_code = state_code_from_path(raw_path)
        s3_key = raw_s3_key(state_code, raw_prefix)
        _upload_one(raw_path, bucket, region, s3_key, overwrite)
        rows.append({"artifact_type": "raw_state_csv", "state_code": state_code, "local_path": str(raw_path), "s3_key": s3_key})

    manifest_path = metadata_dir / RAW_MANIFEST_PATH.name
    _upload_one(manifest_path, bucket, region, raw_manifest_s3_key(raw_meta_prefix), overwrite)
    rows.append(
        {
            "artifact_type": "raw_manifest",
            "state_code": pd.NA,
            "local_path": str(manifest_path),
            "s3_key": raw_manifest_s3_key(raw_meta_prefix),
        }
    )
    return pd.DataFrame(rows)


def main() -> None:
    parser = argparse.ArgumentParser(description="Upload raw IRS EO BMF state CSVs to S3.")
    parser.add_argument("--bucket", default=DEFAULT_S3_BUCKET, help="Target S3 bucket")
    parser.add_argument("--region", default=DEFAULT_S3_REGION, help="Target S3 region")
    parser.add_argument("--raw-dir", type=Path, default=RAW_DIR, help="Local raw IRS EO BMF directory")
    parser.add_argument("--metadata-dir", type=Path, default=META_DIR, help="Local IRS EO BMF metadata directory")
    parser.add_argument("--raw-prefix", default=RAW_PREFIX, help="S3 raw prefix")
    parser.add_argument("--raw-meta-prefix", default=RAW_META_PREFIX, help="S3 raw metadata prefix")
    parser.add_argument("--overwrite", action="store_true", help="Upload even when local and remote bytes already match")
    args = parser.parse_args()

    start = time.perf_counter()
    banner("STEP 02 - UPLOAD IRS EO BMF RAW STATE FILES")
    load_env_from_secrets()
    upload_raw_release(
        raw_dir=args.raw_dir,
        metadata_dir=args.metadata_dir,
        bucket=args.bucket,
        region=args.region,
        raw_prefix=args.raw_prefix,
        raw_meta_prefix=args.raw_meta_prefix,
        overwrite=args.overwrite,
    )
    print_elapsed(start, "Step 02")


if __name__ == "__main__":
    main()
