"""
Step 10: Upload GT filtered parquet outputs and manifests to Silver.
"""

from __future__ import annotations

import argparse
import time

from common import (
    DEFAULT_S3_BUCKET,
    DEFAULT_S3_REGION,
    SILVER_PREFIX,
    UPLOAD_WORKERS,
    artifact_s3_key,
    banner,
    load_env_from_secrets,
    parallel_map,
    print_elapsed,
    print_parallel_summary,
    print_transfer_settings,
    stale_output_warnings,
    should_skip_upload,
    silver_filtered_artifacts,
    upload_file_with_progress,
)


def main() -> None:
    parser = argparse.ArgumentParser(description="Upload GT filtered parquet artifacts and manifests to Silver.")
    parser.add_argument("--bucket", default=DEFAULT_S3_BUCKET, help="S3 bucket")
    parser.add_argument("--region", default=DEFAULT_S3_REGION, help="S3 region")
    parser.add_argument("--prefix", default=SILVER_PREFIX, help="Silver filtered prefix")
    parser.add_argument(
        "--emit",
        default="both",
        choices=sorted(["basic", "mixed", "both"]),
        help="Which filtered outputs to upload",
    )
    parser.add_argument("--overwrite", action="store_true", help="Upload even when the remote object already matches local bytes")
    parser.add_argument("--workers", type=int, default=UPLOAD_WORKERS, help="Parallel upload worker count")
    args = parser.parse_args()

    start = time.perf_counter()
    banner("STEP 10 - UPLOAD SILVER FILTERED OUTPUTS")
    load_env_from_secrets()
    print_transfer_settings(label="gt_upload_silver")
    for warning in stale_output_warnings(args.emit):
        print(f"[upload] Warning: {warning}", flush=True)

    pending: list[tuple[object, str]] = []
    skipped = 0
    for artifact in silver_filtered_artifacts(args.emit):
        local_path = artifact.local_path
        if not local_path.exists():
            raise FileNotFoundError(f"Missing file: {local_path}")
        prefix_override = args.prefix if artifact.s3_prefix == SILVER_PREFIX else None
        s3_key = artifact_s3_key(artifact, prefix_override)
        if should_skip_upload(local_path, args.bucket, s3_key, args.region, overwrite=args.overwrite):
            skipped += 1
            print(f"[upload] Skip unchanged: s3://{args.bucket}/{s3_key}", flush=True)
            continue
        pending.append((artifact, s3_key))

    print_parallel_summary(
        label="gt_silver_upload",
        pending=len(pending),
        skipped=skipped,
        workers=min(max(1, args.workers), max(1, len(pending) if pending else 1)),
    )

    def _upload_one(task: tuple[object, str]) -> str:
        artifact, s3_key = task
        local_path = artifact.local_path
        print(f"[upload] {local_path} -> s3://{args.bucket}/{s3_key}", flush=True)
        upload_file_with_progress(
            local_path,
            args.bucket,
            s3_key,
            args.region,
            extra_args={"ContentType": artifact.content_type},
        )
        return artifact.artifact_id

    uploaded_ids = parallel_map(
        pending,
        worker_count=max(1, args.workers),
        desc="upload silver gt",
        unit="file",
        fn=_upload_one,
    ) if pending else []

    print(f"[upload] Uploaded: {len(uploaded_ids)} | Skipped unchanged: {skipped}", flush=True)
    print_elapsed(start, "Step 10")


if __name__ == "__main__":
    main()
