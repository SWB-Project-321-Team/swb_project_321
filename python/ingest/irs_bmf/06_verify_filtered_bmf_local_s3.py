"""
Step 06: Verify filtered IRS EO BMF benchmark local and S3 bytes.
"""

from __future__ import annotations

import argparse
import time
from pathlib import Path

import pandas as pd

from common import (
    ANALYSIS_TAX_YEAR_MAX,
    ANALYSIS_TAX_YEAR_MIN,
    DEFAULT_S3_BUCKET,
    DEFAULT_S3_REGION,
    FILTERED_SIZE_VERIFICATION_PATH,
    META_DIR,
    SILVER_PREFIX,
    SILVER_META_PREFIX,
    STAGING_DIR,
    banner,
    combined_filtered_output_path,
    combined_filtered_s3_key,
    filter_manifest_s3_key,
    legacy_filtered_output_path,
    legacy_filtered_s3_key,
    load_env_from_secrets,
    print_elapsed,
    s3_object_size,
    yearly_filtered_output_path,
    yearly_filtered_s3_key,
)


def verify_filtered_outputs(
    *,
    metadata_dir: Path = META_DIR,
    staging_dir: Path = STAGING_DIR,
    bucket: str = DEFAULT_S3_BUCKET,
    region: str = DEFAULT_S3_REGION,
    silver_prefix: str = SILVER_PREFIX,
    silver_meta_prefix: str = SILVER_META_PREFIX,
) -> pd.DataFrame:
    """Verify local-vs-S3 bytes for the filtered IRS EO BMF artifacts."""
    artifacts: list[tuple[str, Path, str, str | None]] = [
        ("combined_filtered", combined_filtered_output_path(staging_dir), combined_filtered_s3_key(silver_prefix), None),
        ("legacy_filtered", legacy_filtered_output_path(), legacy_filtered_s3_key(silver_prefix), None),
        ("filter_manifest", metadata_dir / "irs_bmf_filter_manifest.csv", filter_manifest_s3_key(silver_meta_prefix), None),
    ]
    for year in range(ANALYSIS_TAX_YEAR_MIN, ANALYSIS_TAX_YEAR_MAX + 1):
        artifacts.append(("benchmark_year", yearly_filtered_output_path(year, staging_dir), yearly_filtered_s3_key(year, silver_prefix), str(year)))

    rows: list[dict[str, object]] = []
    for artifact_type, local_path, s3_key, tax_year in artifacts:
        if not local_path.exists():
            raise FileNotFoundError(f"Missing filtered artifact: {local_path}")
        local_size = local_path.stat().st_size
        remote_size = s3_object_size(bucket, s3_key, region)
        rows.append(
            {
                "artifact_type": artifact_type,
                "tax_year": tax_year,
                "local_path": str(local_path),
                "s3_key": s3_key,
                "local_size_bytes": local_size,
                "s3_size_bytes": remote_size,
                "local_s3_match": local_size == remote_size,
            }
        )
    report_df = pd.DataFrame(rows)
    output_path = metadata_dir / FILTERED_SIZE_VERIFICATION_PATH.name
    output_path.parent.mkdir(parents=True, exist_ok=True)
    report_df.to_csv(output_path, index=False)
    print(f"[verify] Wrote filtered size verification report: {output_path}", flush=True)
    return report_df


def main() -> None:
    parser = argparse.ArgumentParser(description="Verify filtered IRS EO BMF local and S3 bytes.")
    parser.add_argument("--bucket", default=DEFAULT_S3_BUCKET, help="Target S3 bucket")
    parser.add_argument("--region", default=DEFAULT_S3_REGION, help="Target S3 region")
    parser.add_argument("--metadata-dir", type=Path, default=META_DIR, help="Local IRS EO BMF metadata directory")
    parser.add_argument("--staging-dir", type=Path, default=STAGING_DIR, help="Local IRS EO BMF staging directory")
    parser.add_argument("--silver-prefix", default=SILVER_PREFIX, help="S3 silver prefix")
    parser.add_argument("--silver-meta-prefix", default=SILVER_META_PREFIX, help="S3 silver metadata prefix")
    args = parser.parse_args()

    start = time.perf_counter()
    banner("STEP 06 - VERIFY FILTERED IRS EO BMF LOCAL VS S3")
    load_env_from_secrets()
    verify_filtered_outputs(
        metadata_dir=args.metadata_dir,
        staging_dir=args.staging_dir,
        bucket=args.bucket,
        region=args.region,
        silver_prefix=args.silver_prefix,
        silver_meta_prefix=args.silver_meta_prefix,
    )
    print_elapsed(start, "Step 06")


if __name__ == "__main__":
    main()
