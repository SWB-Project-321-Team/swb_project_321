"""
Step 08: Upload IRS EO BMF analysis outputs to S3.
"""

from __future__ import annotations

import argparse
import time
from pathlib import Path

from common import (
    ANALYSIS_COVERAGE_PREFIX,
    ANALYSIS_DOCUMENTATION_PREFIX,
    ANALYSIS_PREFIX,
    ANALYSIS_VARIABLE_MAPPING_PREFIX,
    DEFAULT_S3_BUCKET,
    DEFAULT_S3_REGION,
    META_DIR,
    STAGING_DIR,
    analysis_data_processing_doc_path,
    analysis_field_metrics_output_path,
    analysis_geography_metrics_output_path,
    analysis_variable_coverage_path,
    analysis_variable_mapping_path,
    analysis_variables_output_path,
    banner,
    guess_content_type,
    load_env_from_secrets,
    print_elapsed,
    s3_object_size,
    should_skip_upload,
    upload_file_with_progress,
)


def _upload_one(local_path: Path, bucket: str, region: str, key: str, overwrite: bool) -> None:
    if not local_path.exists():
        raise FileNotFoundError(f"Missing analysis artifact: {local_path}")
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
    parser = argparse.ArgumentParser(description="Upload IRS EO BMF analysis outputs to S3.")
    parser.add_argument("--bucket", default=DEFAULT_S3_BUCKET, help="Target S3 bucket")
    parser.add_argument("--region", default=DEFAULT_S3_REGION, help="Target S3 region")
    parser.add_argument("--metadata-dir", type=Path, default=META_DIR, help="Local IRS EO BMF metadata directory")
    parser.add_argument("--staging-dir", type=Path, default=STAGING_DIR, help="Local IRS EO BMF staging directory")
    parser.add_argument("--analysis-prefix", default=ANALYSIS_PREFIX, help="S3 analysis prefix")
    parser.add_argument("--analysis-documentation-prefix", default=ANALYSIS_DOCUMENTATION_PREFIX, help="S3 analysis documentation prefix")
    parser.add_argument("--analysis-variable-mapping-prefix", default=ANALYSIS_VARIABLE_MAPPING_PREFIX, help="S3 analysis variable mapping prefix")
    parser.add_argument("--analysis-coverage-prefix", default=ANALYSIS_COVERAGE_PREFIX, help="S3 analysis coverage prefix")
    parser.add_argument("--overwrite", action="store_true", help="Upload even when remote bytes already match")
    args = parser.parse_args()

    start = time.perf_counter()
    banner("STEP 08 - UPLOAD IRS EO BMF ANALYSIS OUTPUTS")
    load_env_from_secrets()
    uploads = [
        (analysis_variables_output_path(args.staging_dir), f"{args.analysis_prefix.rstrip('/')}/{analysis_variables_output_path(args.staging_dir).name}"),
        (analysis_geography_metrics_output_path(args.staging_dir), f"{args.analysis_prefix.rstrip('/')}/{analysis_geography_metrics_output_path(args.staging_dir).name}"),
        (analysis_field_metrics_output_path(args.staging_dir), f"{args.analysis_prefix.rstrip('/')}/{analysis_field_metrics_output_path(args.staging_dir).name}"),
        (analysis_variable_coverage_path(args.metadata_dir), f"{args.analysis_coverage_prefix.rstrip('/')}/{analysis_variable_coverage_path(args.metadata_dir).name}"),
        (analysis_variable_mapping_path(), f"{args.analysis_variable_mapping_prefix.rstrip('/')}/{analysis_variable_mapping_path().name}"),
        (analysis_data_processing_doc_path(), f"{args.analysis_documentation_prefix.rstrip('/')}/{analysis_data_processing_doc_path().name}"),
    ]
    for local_path, s3_key in uploads:
        _upload_one(local_path, args.bucket, args.region, s3_key, args.overwrite)
    print_elapsed(start, "Step 08")


if __name__ == "__main__":
    main()
