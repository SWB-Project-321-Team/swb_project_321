"""
Step 09: Upload GT pre-Silver parquet outputs to Bronze.

Both step-06 and step-07 outputs are now filtered/admitted pre-Silver
artifacts. They are no longer broad national "unfiltered" combine inputs.
"""

from __future__ import annotations

import argparse
import time

from common import (
    BRONZE_PRESILVER_PREFIX,
    DEFAULT_S3_BUCKET,
    DEFAULT_S3_REGION,
    UPLOAD_WORKERS,
    artifact_s3_key,
    banner,
    bronze_presilver_artifacts,
    load_env_from_secrets,
    parallel_map,
    print_elapsed,
    print_parallel_summary,
    print_transfer_settings,
    should_skip_upload,
    upload_file_with_progress,
)


def main() -> None:
    parser = argparse.ArgumentParser(description="Upload GT pre-Silver parquet artifacts to Bronze.")
    parser.add_argument("--bucket", default=DEFAULT_S3_BUCKET, help="S3 bucket")
    parser.add_argument("--region", default=DEFAULT_S3_REGION, help="S3 region")
    parser.add_argument("--prefix", default=BRONZE_PRESILVER_PREFIX, help="Bronze pre-Silver prefix")
    parser.add_argument("--overwrite", action="store_true", help="Upload even when the remote object already matches local bytes")
    parser.add_argument("--workers", type=int, default=UPLOAD_WORKERS, help="Parallel upload worker count")
    args = parser.parse_args()

    start = time.perf_counter()
    banner("STEP 09 - UPLOAD BRONZE PRE-SILVER OUTPUTS")
    load_env_from_secrets()
    print_transfer_settings(label="gt_upload_bronze")

    pending: list[tuple[object, str]] = []
    skipped = 0
    for artifact in bronze_presilver_artifacts():
        local_path = artifact.local_path
        if not local_path.exists():
            raise FileNotFoundError(f"Missing file: {local_path}")
        s3_key = artifact_s3_key(artifact, args.prefix)
        if should_skip_upload(local_path, args.bucket, s3_key, args.region, overwrite=args.overwrite):
            skipped += 1
            print(f"[upload] Skip unchanged: s3://{args.bucket}/{s3_key}", flush=True)
            continue
        pending.append((artifact, s3_key))

    print_parallel_summary(
        label="gt_bronze_upload",
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
        desc="upload bronze gt",
        unit="file",
        fn=_upload_one,
    ) if pending else []

    print(f"[upload] Uploaded: {len(uploaded_ids)} | Skipped unchanged: {skipped}", flush=True)
    print_elapsed(start, "Step 09")


if __name__ == "__main__":
    main()
