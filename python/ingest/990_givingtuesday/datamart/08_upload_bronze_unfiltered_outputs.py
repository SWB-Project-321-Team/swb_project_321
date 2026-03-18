"""
Step 08: Upload unfiltered unified outputs to Bronze.

Uploads:
- givingtuesday_990_basic_allforms_unfiltered.parquet
- givingtuesday_990_basic_plus_combined_unfiltered.parquet
to bronze/givingtuesday_990/datamarts/unfiltered/
"""

from __future__ import annotations

import argparse
import time
from pathlib import Path

from common import (
    BASIC_ALLFORMS_PARQUET,
    BASIC_PLUS_COMBINED_PARQUET,
    BRONZE_UNFILTERED_PREFIX,
    DEFAULT_S3_BUCKET,
    DEFAULT_S3_REGION,
    banner,
    load_env_from_secrets,
    print_elapsed,
    upload_file_with_progress,
)


def _upload(path: Path, bucket: str, prefix: str, region: str) -> None:
    """Upload one parquet file to Bronze unfiltered prefix."""
    if not path.exists():
        raise FileNotFoundError(f"Missing file: {path}")
    key = f"{prefix.rstrip('/')}/{path.name}"
    print(f"[upload] {path} -> s3://{bucket}/{key}", flush=True)
    upload_file_with_progress(path, bucket, key, region, extra_args={"ContentType": "application/octet-stream"})


def main() -> None:
    parser = argparse.ArgumentParser(description="Upload unfiltered unified parquet outputs to Bronze.")
    parser.add_argument("--bucket", default=DEFAULT_S3_BUCKET, help="S3 bucket")
    parser.add_argument("--region", default=DEFAULT_S3_REGION, help="S3 region")
    parser.add_argument("--prefix", default=BRONZE_UNFILTERED_PREFIX, help="Bronze unfiltered prefix")
    parser.add_argument("--basic-allforms", default=str(BASIC_ALLFORMS_PARQUET), help="Basic allforms parquet path")
    parser.add_argument("--basic-plus-combined", default=str(BASIC_PLUS_COMBINED_PARQUET), help="Basic+Combined parquet path")
    args = parser.parse_args()

    start = time.perf_counter()
    banner("STEP 08 - UPLOAD BRONZE UNFILTERED OUTPUTS")
    load_env_from_secrets()

    _upload(Path(args.basic_allforms), args.bucket, args.prefix, args.region)
    _upload(Path(args.basic_plus_combined), args.bucket, args.prefix, args.region)
    print("[upload] Bronze unfiltered upload complete.", flush=True)
    print_elapsed(start, "Step 08")


if __name__ == "__main__":
    main()

