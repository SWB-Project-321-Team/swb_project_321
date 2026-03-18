"""
Step 10: Upload filtered Silver artifacts to S3.

Uploads:
- givingtuesday_990_filings_benchmark.parquet
- manifest_filtered.json
to silver/givingtuesday_990/filing/
"""

from __future__ import annotations

import argparse
import mimetypes
import time
from pathlib import Path

from common import (
    DEFAULT_S3_BUCKET,
    DEFAULT_S3_REGION,
    FILTERED_MANIFEST_JSON,
    FILTERED_SILVER_PARQUET,
    SILVER_PREFIX,
    banner,
    load_env_from_secrets,
    print_elapsed,
    upload_file_with_progress,
)


def _ctype(path: Path) -> str:
    """Return content type for S3 upload."""
    ct, _ = mimetypes.guess_type(str(path))
    return ct or "application/octet-stream"


def _upload(path: Path, bucket: str, prefix: str, region: str) -> None:
    """Upload one local file to Silver prefix."""
    if not path.exists():
        raise FileNotFoundError(f"Missing file: {path}")
    key = f"{prefix.rstrip('/')}/{path.name}"
    print(f"[upload] {path} -> s3://{bucket}/{key}", flush=True)
    upload_file_with_progress(path, bucket, key, region, extra_args={"ContentType": _ctype(path)})


def main() -> None:
    parser = argparse.ArgumentParser(description="Upload filtered Silver artifacts.")
    parser.add_argument("--bucket", default=DEFAULT_S3_BUCKET, help="S3 bucket")
    parser.add_argument("--region", default=DEFAULT_S3_REGION, help="S3 region")
    parser.add_argument("--prefix", default=SILVER_PREFIX, help="Silver S3 prefix")
    parser.add_argument("--filtered-parquet", default=str(FILTERED_SILVER_PARQUET), help="Filtered parquet path")
    parser.add_argument("--manifest-json", default=str(FILTERED_MANIFEST_JSON), help="Manifest JSON path")
    args = parser.parse_args()

    start = time.perf_counter()
    banner("STEP 10 - UPLOAD SILVER FILTERED OUTPUT")
    load_env_from_secrets()

    _upload(Path(args.filtered_parquet), args.bucket, args.prefix, args.region)
    _upload(Path(args.manifest_json), args.bucket, args.prefix, args.region)

    print("[upload] Silver upload complete.", flush=True)
    print_elapsed(start, "Step 10")


if __name__ == "__main__":
    main()

