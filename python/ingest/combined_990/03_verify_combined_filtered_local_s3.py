"""
Step 03: Verify local/S3 byte parity for the combined filtered 990 union outputs.
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
    compute_local_s3_match,
    guess_content_type,
    load_env_from_secrets,
    metadata_s3_key,
    print_elapsed,
    s3_object_size,
    upload_file_with_progress,
)


def main() -> None:
    parser = argparse.ArgumentParser(description="Verify local/S3 byte parity for the combined filtered 990 union outputs.")
    parser.add_argument("--output", type=Path, default=OUTPUT_PARQUET, help="Combined union parquet path")
    parser.add_argument("--bucket", default=DEFAULT_S3_BUCKET, help="Target S3 bucket")
    parser.add_argument("--region", default=DEFAULT_S3_REGION, help="Target S3 region")
    parser.add_argument("--silver-prefix", default=SILVER_PREFIX, help="S3 silver prefix for the combined parquet")
    parser.add_argument("--metadata-prefix", default=METADATA_S3_PREFIX, help="S3 metadata prefix")
    args = parser.parse_args()

    start = time.perf_counter()
    banner("STEP 03 - VERIFY COMBINED FILTERED 990 LOCAL / S3 SIZE PARITY")
    load_env_from_secrets()

    files = [
        args.output,
        SOURCE_INPUT_MANIFEST_CSV,
        COLUMN_DICTIONARY_CSV,
        FIELD_AVAILABILITY_MATRIX_CSV,
        DIAG_OVERLAP_BY_EIN_CSV,
        DIAG_OVERLAP_BY_EIN_TAX_YEAR_CSV,
        DIAG_OVERLAP_SUMMARY_CSV,
        BUILD_SUMMARY_JSON,
    ]
    for path in files:
        if not path.exists():
            raise FileNotFoundError(f"Required file not found: {path}. Run steps 01 and 02 first.")

    report_rows: list[dict[str, object]] = []
    failures = 0
    for path in files:
        if path == args.output:
            s3_key = combined_s3_key(args.silver_prefix)
        else:
            s3_key = metadata_s3_key(path.name, args.metadata_prefix)
        local_bytes = path.stat().st_size
        s3_bytes = s3_object_size(args.bucket, s3_key, args.region)
        size_match = compute_local_s3_match(local_bytes, s3_bytes)
        report_rows.append(
            {
                "local_path": str(path),
                "local_bytes": local_bytes,
                "s3_bucket": args.bucket,
                "s3_key": s3_key,
                "s3_bytes": s3_bytes,
                "size_match": "TRUE" if size_match else "FALSE",
            }
        )
        print(f"[verify] {path.name}: local={local_bytes}, s3={s3_bytes}, match={'TRUE' if size_match else 'FALSE'}", flush=True)
        if not size_match:
            failures += 1

    SIZE_VERIFICATION_CSV.parent.mkdir(parents=True, exist_ok=True)
    import csv
    with open(SIZE_VERIFICATION_CSV, "w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=["local_path", "local_bytes", "s3_bucket", "s3_key", "s3_bytes", "size_match"])
        writer.writeheader()
        writer.writerows(report_rows)

    report_key = metadata_s3_key(SIZE_VERIFICATION_CSV.name, args.metadata_prefix)
    print(f"[verify] Uploading verification report to s3://{args.bucket}/{report_key}", flush=True)
    upload_file_with_progress(
        SIZE_VERIFICATION_CSV,
        args.bucket,
        report_key,
        args.region,
        extra_args={"ContentType": guess_content_type(SIZE_VERIFICATION_CSV)},
    )

    print(f"[verify] Failures: {failures}", flush=True)
    print_elapsed(start, "Step 03")
    if failures:
        raise SystemExit(f"Combined local/S3 size verification failed for {failures} file(s). See {SIZE_VERIFICATION_CSV}.")


if __name__ == "__main__":
    main()
