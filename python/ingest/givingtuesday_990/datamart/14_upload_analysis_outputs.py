"""
Step 14: Upload GT final analysis outputs to the dedicated analysis S3 prefix.

This step exists because the step-13 analysis artifacts are now official GT
pipeline outputs rather than local-only convenience files. The upload is kept
separate from the filing Silver upload step so the analysis artifacts can have
their own explicit S3 prefix and content-type handling.
"""

from __future__ import annotations

import argparse
import time

from common import (
    ANALYSIS_PREFIX,
    DEFAULT_S3_BUCKET,
    DEFAULT_S3_REGION,
    UPLOAD_WORKERS,
    analysis_output_artifacts,
    artifact_s3_key,
    banner,
    load_env_from_secrets,
    parallel_map,
    print_elapsed,
    print_parallel_summary,
    print_transfer_settings,
    should_skip_upload,
    upload_file_with_progress,
)


def main() -> None:
    parser = argparse.ArgumentParser(description="Upload GT analysis outputs and metadata to the analysis S3 prefix.")
    parser.add_argument("--bucket", default=DEFAULT_S3_BUCKET, help="S3 bucket")
    parser.add_argument("--region", default=DEFAULT_S3_REGION, help="S3 region")
    parser.add_argument("--prefix", default=ANALYSIS_PREFIX, help="Analysis parquet prefix")
    parser.add_argument("--overwrite", action="store_true", help="Upload even when the remote object already matches local bytes")
    parser.add_argument("--workers", type=int, default=UPLOAD_WORKERS, help="Parallel upload worker count")
    args = parser.parse_args()

    start = time.perf_counter()
    banner("STEP 14 - UPLOAD ANALYSIS OUTPUTS")
    load_env_from_secrets()
    print_transfer_settings(label="gt_upload_analysis")

    pending: list[tuple[object, str]] = []
    skipped = 0
    for artifact in analysis_output_artifacts():
        local_path = artifact.local_path
        if not local_path.exists():
            raise FileNotFoundError(f"Missing analysis artifact: {local_path}")

        # Keep the CLI prefix override scoped to the main analysis parquet prefix.
        # Documentation, variable mapping, and coverage artifacts use their
        # dedicated prefixes so the public S3 layout stays explicit.
        prefix_override = args.prefix if artifact.s3_prefix == ANALYSIS_PREFIX else None
        s3_key = artifact_s3_key(artifact, prefix_override)
        if should_skip_upload(local_path, args.bucket, s3_key, args.region, overwrite=args.overwrite):
            skipped += 1
            print(f"[upload] Skip unchanged: s3://{args.bucket}/{s3_key}", flush=True)
            continue
        pending.append((artifact, s3_key))

    print_parallel_summary(
        label="gt_analysis_upload",
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
        desc="upload gt analysis",
        unit="file",
        fn=_upload_one,
    ) if pending else []

    print(f"[upload] Uploaded: {len(uploaded_ids)} | Skipped unchanged: {skipped}", flush=True)
    print_elapsed(start, "Step 14")


if __name__ == "__main__":
    main()
