"""
Step 03: Verify local/S3 byte parity for the combined filtered 990 union + master outputs.
"""

from __future__ import annotations

import argparse
import csv
import time
from pathlib import Path

from tqdm import tqdm

from common import (
    DEFAULT_S3_BUCKET,
    DEFAULT_S3_REGION,
    METADATA_S3_PREFIX,
    MASTER_OUTPUT_PARQUET,
    OUTPUT_PARQUET,
    SILVER_PREFIX,
    SIZE_VERIFICATION_CSV,
    all_metadata_files,
    batch_s3_object_sizes,
    banner,
    combined_s3_key,
    compute_local_s3_match,
    guess_content_type,
    load_env_from_secrets,
    master_s3_key,
    metadata_s3_key,
    print_elapsed,
    print_transfer_settings,
    upload_file_with_progress,
    VERIFY_WORKERS,
)


def main() -> None:
    parser = argparse.ArgumentParser(description="Verify local/S3 byte parity for the combined filtered 990 union + master outputs.")
    parser.add_argument("--output", type=Path, default=OUTPUT_PARQUET, help="Combined union parquet path")
    parser.add_argument("--master-output", type=Path, default=MASTER_OUTPUT_PARQUET, help="Combined organization-year master parquet path")
    parser.add_argument("--bucket", default=DEFAULT_S3_BUCKET, help="Target S3 bucket")
    parser.add_argument("--region", default=DEFAULT_S3_REGION, help="Target S3 region")
    parser.add_argument("--silver-prefix", default=SILVER_PREFIX, help="S3 silver prefix for the combined parquet")
    parser.add_argument("--metadata-prefix", default=METADATA_S3_PREFIX, help="S3 metadata prefix")
    args = parser.parse_args()

    start = time.perf_counter()
    banner("STEP 03 - VERIFY COMBINED FILTERED 990 UNION + MASTER LOCAL / S3 SIZE PARITY")
    load_env_from_secrets()
    print_transfer_settings(label="combined-verify")

    files = [args.output, args.master_output] + all_metadata_files(include_verification=False)
    for path in files:
        if not path.exists():
            raise FileNotFoundError(f"Required file not found: {path}. Run steps 01 and 02 first.")

    print(f"[verify] Files queued: {len(files):,}", flush=True)
    worker_count = max(1, min(VERIFY_WORKERS, len(files)))
    print(f"[verify] Metadata verification workers: {worker_count}", flush=True)

    file_specs: list[tuple[Path, str, str]] = []
    for path in files:
        if path == args.output:
            file_specs.append((path, "union_parquet", combined_s3_key(args.silver_prefix)))
        elif path == args.master_output:
            file_specs.append((path, "master_parquet", master_s3_key(args.silver_prefix)))
        else:
            file_specs.append((path, "metadata", metadata_s3_key(path.name, args.metadata_prefix)))

    s3_key_map = {str(path): s3_key for path, _, s3_key in file_specs}
    print("[verify] Fetching S3 object sizes in parallel before local/S3 parity checks.", flush=True)
    s3_sizes = batch_s3_object_sizes(args.bucket, s3_key_map, args.region, workers=worker_count)

    report_rows: list[dict[str, object]] = []
    failures = 0
    for path, artifact_type, s3_key in tqdm(file_specs, desc="verify outputs", unit="file"):
        local_bytes = path.stat().st_size
        s3_bytes = s3_sizes.get(str(path))
        size_match = compute_local_s3_match(local_bytes, s3_bytes)
        report_rows.append(
            {
                "artifact_type": artifact_type,
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

    report_rows.sort(key=lambda row: (str(row["artifact_type"]), str(row["local_path"])))

    SIZE_VERIFICATION_CSV.parent.mkdir(parents=True, exist_ok=True)
    with open(SIZE_VERIFICATION_CSV, "w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=["artifact_type", "local_path", "local_bytes", "s3_bucket", "s3_key", "s3_bytes", "size_match"])
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
