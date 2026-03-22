"""
Step 02: Upload the combined filtered 990 source-union parquet and metadata to S3.
"""

from __future__ import annotations

import argparse
import time
from pathlib import Path

from common import (
    BUILD_SUMMARY_JSON,
    COLUMN_DICTIONARY_CSV,
    DEFAULT_S3_BUCKET,
    DEFAULT_S3_REGION,
    DIAG_OVERLAP_BY_EIN_CSV,
    DIAG_OVERLAP_BY_EIN_TAX_YEAR_CSV,
    DIAG_OVERLAP_SUMMARY_CSV,
    FIELD_AVAILABILITY_MATRIX_CSV,
    METADATA_S3_PREFIX,
    OUTPUT_PARQUET,
    SILVER_PREFIX,
    SIZE_VERIFICATION_CSV,
    SOURCE_INPUT_MANIFEST_CSV,
    banner,
    combined_s3_key,
    guess_content_type,
    load_env_from_secrets,
    metadata_s3_key,
    print_elapsed,
    should_skip_upload,
    upload_file_with_progress,
)


def main() -> None:
    parser = argparse.ArgumentParser(description="Upload the combined filtered 990 union parquet and metadata to S3.")
    parser.add_argument("--output", type=Path, default=OUTPUT_PARQUET, help="Combined union parquet path")
    parser.add_argument("--bucket", default=DEFAULT_S3_BUCKET, help="Target S3 bucket")
    parser.add_argument("--region", default=DEFAULT_S3_REGION, help="Target S3 region")
    parser.add_argument("--silver-prefix", default=SILVER_PREFIX, help="S3 silver prefix for the combined parquet")
    parser.add_argument("--metadata-prefix", default=METADATA_S3_PREFIX, help="S3 metadata prefix")
    parser.add_argument("--overwrite", action="store_true", help="Upload even when the S3 object already matches local bytes")
    args = parser.parse_args()

    start = time.perf_counter()
    banner("STEP 02 - UPLOAD COMBINED FILTERED 990 UNION TO S3")
    load_env_from_secrets()

    metadata_files = [
        SOURCE_INPUT_MANIFEST_CSV,
        COLUMN_DICTIONARY_CSV,
        FIELD_AVAILABILITY_MATRIX_CSV,
        DIAG_OVERLAP_BY_EIN_CSV,
        DIAG_OVERLAP_BY_EIN_TAX_YEAR_CSV,
        DIAG_OVERLAP_SUMMARY_CSV,
        BUILD_SUMMARY_JSON,
        SIZE_VERIFICATION_CSV,
    ]
    if not args.output.exists():
        raise FileNotFoundError(f"Combined parquet not found: {args.output}. Run step 01 first.")
    for path in metadata_files:
        if path == SIZE_VERIFICATION_CSV and not path.exists():
            continue
        if not path.exists():
            raise FileNotFoundError(f"Required metadata file not found: {path}. Run step 01 first.")

    parquet_key = combined_s3_key(args.silver_prefix)
    print(f"[upload] Parquet: {args.output} -> s3://{args.bucket}/{parquet_key}", flush=True)
    if should_skip_upload(args.output, args.bucket, parquet_key, args.region, args.overwrite):
        print(f"[upload] Skip unchanged S3 object: s3://{args.bucket}/{parquet_key}", flush=True)
    else:
        file_start = time.perf_counter()
        upload_file_with_progress(
            args.output,
            args.bucket,
            parquet_key,
            args.region,
            extra_args={"ContentType": guess_content_type(args.output)},
        )
        print_elapsed(file_start, f"upload {args.output.name}")

    for path in metadata_files:
        if not path.exists():
            continue
        key = metadata_s3_key(path.name, args.metadata_prefix)
        print(f"[upload] Metadata: {path} -> s3://{args.bucket}/{key}", flush=True)
        if should_skip_upload(path, args.bucket, key, args.region, args.overwrite):
            print(f"[upload] Skip unchanged S3 object: s3://{args.bucket}/{key}", flush=True)
            continue
        file_start = time.perf_counter()
        upload_file_with_progress(
            path,
            args.bucket,
            key,
            args.region,
            extra_args={"ContentType": guess_content_type(path)},
        )
        print_elapsed(file_start, f"upload {path.name}")

    print_elapsed(start, "Step 02")


if __name__ == "__main__":
    main()
