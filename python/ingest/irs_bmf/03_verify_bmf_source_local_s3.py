"""
Step 03: Verify raw IRS EO BMF local and S3 bytes against the raw manifest.
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
    RAW_PREFIX,
    RAW_SIZE_VERIFICATION_PATH,
    banner,
    discover_raw_state_files,
    load_env_from_secrets,
    print_elapsed,
    raw_s3_key,
    s3_object_size,
    state_code_from_path,
)


def verify_raw_release(
    *,
    raw_dir: Path = RAW_DIR,
    metadata_dir: Path = META_DIR,
    bucket: str = DEFAULT_S3_BUCKET,
    region: str = DEFAULT_S3_REGION,
    raw_prefix: str = RAW_PREFIX,
) -> pd.DataFrame:
    """Verify source/local/S3 sizes for the raw IRS EO BMF state files."""
    manifest_path = metadata_dir / RAW_MANIFEST_PATH.name
    if not manifest_path.exists():
        raise FileNotFoundError(f"Missing raw manifest: {manifest_path}")
    manifest_df = pd.read_csv(manifest_path, dtype=str)
    manifest_by_state = {str(row["state_code"]).lower(): row for _, row in manifest_df.iterrows()}
    rows: list[dict[str, object]] = []
    for raw_path in discover_raw_state_files(raw_dir):
        state_code = state_code_from_path(raw_path)
        manifest_row = manifest_by_state.get(state_code, {})
        local_size = raw_path.stat().st_size
        remote_key = raw_s3_key(state_code, raw_prefix)
        remote_size = s3_object_size(bucket, remote_key, region)
        source_size = manifest_row.get("source_content_length_bytes", pd.NA)
        if pd.notna(source_size):
            source_size = int(source_size)
        rows.append(
            {
                "state_code": state_code,
                "source_url": manifest_row.get("source_url", pd.NA),
                "local_path": str(raw_path),
                "s3_key": remote_key,
                "source_size_bytes": source_size,
                "local_size_bytes": local_size,
                "s3_size_bytes": remote_size,
                "source_local_match": (local_size == source_size) if pd.notna(source_size) else pd.NA,
                "local_s3_match": local_size == remote_size,
            }
        )
    report_df = pd.DataFrame(rows).sort_values("state_code", kind="mergesort").reset_index(drop=True)
    output_path = metadata_dir / RAW_SIZE_VERIFICATION_PATH.name
    output_path.parent.mkdir(parents=True, exist_ok=True)
    report_df.to_csv(output_path, index=False)
    print(f"[verify] Wrote raw size verification report: {output_path}", flush=True)
    return report_df


def main() -> None:
    parser = argparse.ArgumentParser(description="Verify raw IRS EO BMF local and S3 bytes.")
    parser.add_argument("--bucket", default=DEFAULT_S3_BUCKET, help="Target S3 bucket")
    parser.add_argument("--region", default=DEFAULT_S3_REGION, help="Target S3 region")
    parser.add_argument("--raw-dir", type=Path, default=RAW_DIR, help="Local raw IRS EO BMF directory")
    parser.add_argument("--metadata-dir", type=Path, default=META_DIR, help="Local IRS EO BMF metadata directory")
    parser.add_argument("--raw-prefix", default=RAW_PREFIX, help="S3 raw prefix")
    args = parser.parse_args()

    start = time.perf_counter()
    banner("STEP 03 - VERIFY IRS EO BMF RAW LOCAL VS S3")
    load_env_from_secrets()
    verify_raw_release(
        raw_dir=args.raw_dir,
        metadata_dir=args.metadata_dir,
        bucket=args.bucket,
        region=args.region,
        raw_prefix=args.raw_prefix,
    )
    print_elapsed(start, "Step 03")


if __name__ == "__main__":
    main()
