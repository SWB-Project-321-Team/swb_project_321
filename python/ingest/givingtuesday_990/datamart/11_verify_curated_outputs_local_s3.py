"""
Step 11: Verify local/S3 size parity for curated GT datamart outputs.

This includes:
- the admitted GT Basic pre-Silver Bronze parquet
- the admitted GT mixed pre-Silver Bronze parquet
- the Silver filtered GT outputs and their metadata artifacts
"""

from __future__ import annotations

import argparse
import csv
import json
import time
from pathlib import Path

from common import (
    BASIC_ALLFORMS_BUILD_MANIFEST_JSON,
    BASIC_PLUS_COMBINED_BUILD_MANIFEST_JSON,
    CURATED_SIZE_REPORT_ARTIFACT,
    CURATED_SIZE_REPORT_CSV,
    DEFAULT_S3_BUCKET,
    DEFAULT_S3_REGION,
    FILTERED_BASIC_MANIFEST_JSON,
    FILTERED_MIXED_MANIFEST_JSON,
    GT_PIPELINE_ARTIFACT_MANIFEST_CSV,
    GT_PIPELINE_ARTIFACT_MANIFEST_CSV_ARTIFACT,
    GT_PIPELINE_ARTIFACT_MANIFEST_JSON,
    GT_PIPELINE_ARTIFACT_MANIFEST_JSON_ARTIFACT,
    VERIFY_WORKERS,
    artifact_s3_key,
    banner,
    batch_s3_object_sizes,
    build_path_signature,
    curated_output_artifacts,
    load_env_from_secrets,
    pipeline_metadata_artifacts,
    print_elapsed,
    print_transfer_settings,
    read_json,
    stale_output_warnings,
    upload_file_with_progress,
    write_json,
)


def _flatten_json(value: object) -> str:
    """Serialize nested values for one CSV cell."""
    if value in (None, ""):
        return ""
    return json.dumps(value, sort_keys=True)


def main() -> None:
    parser = argparse.ArgumentParser(description="Verify local/S3 parity for GT curated output artifacts.")
    parser.add_argument("--bucket", default=DEFAULT_S3_BUCKET, help="S3 bucket")
    parser.add_argument("--region", default=DEFAULT_S3_REGION, help="S3 region")
    parser.add_argument(
        "--emit",
        default="both",
        choices=sorted(["basic", "mixed", "both"]),
        help="Which filtered outputs to include alongside the Bronze GT pre-Silver artifacts",
    )
    parser.add_argument("--report-csv", default=str(CURATED_SIZE_REPORT_CSV), help="Local verification report CSV path")
    parser.add_argument("--artifact-manifest-json", default=str(GT_PIPELINE_ARTIFACT_MANIFEST_JSON), help="Pipeline artifact manifest JSON path")
    parser.add_argument("--artifact-manifest-csv", default=str(GT_PIPELINE_ARTIFACT_MANIFEST_CSV), help="Pipeline artifact manifest CSV path")
    parser.add_argument(
        "--report-prefix",
        default=CURATED_SIZE_REPORT_ARTIFACT.s3_prefix,
        help="S3 prefix used when uploading the verification report",
    )
    parser.add_argument(
        "--verify-only-changed",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="Reuse prior verification results for unchanged artifacts and only head changed S3 objects",
    )
    parser.add_argument("--workers", type=int, default=VERIFY_WORKERS, help="Parallel S3 size-lookup worker count")
    args = parser.parse_args()

    start = time.perf_counter()
    banner("STEP 11 - VERIFY CURATED OUTPUTS LOCAL/S3")
    load_env_from_secrets()
    print_transfer_settings(label="gt_verify_curated")
    for warning in stale_output_warnings(args.emit):
        print(f"[verify] Warning: {warning}", flush=True)

    report_path = Path(args.report_csv)
    artifact_manifest_json_path = Path(args.artifact_manifest_json)
    artifact_manifest_csv_path = Path(args.artifact_manifest_csv)
    artifacts = curated_output_artifacts(args.emit)
    previous_manifest = {}
    if args.verify_only_changed and artifact_manifest_json_path.exists():
        try:
            previous_manifest = read_json(artifact_manifest_json_path)
        except Exception:
            previous_manifest = {}
    previous_artifacts = {
        row.get("artifact_id", ""): row
        for row in previous_manifest.get("artifacts", [])
        if isinstance(row, dict)
    }

    local_signature_by_artifact: dict[str, dict[str, object]] = {}
    pending_path_to_s3_key: dict[str, str] = {}
    cached_s3_sizes: dict[str, int | None] = {}
    for artifact in artifacts:
        local_path = artifact.local_path
        if not local_path.exists():
            raise FileNotFoundError(f"Missing local artifact for verification: {local_path}")
        local_signature = build_path_signature(local_path)
        local_signature_by_artifact[artifact.artifact_id] = local_signature
        prior_row = previous_artifacts.get(artifact.artifact_id, {})
        if (
            args.verify_only_changed
            and prior_row.get("local_signature") == local_signature
            and prior_row.get("size_match") is True
            and prior_row.get("s3_bytes") is not None
            and prior_row.get("s3_key") == artifact_s3_key(artifact)
            and prior_row.get("s3_bucket") == args.bucket
        ):
            cached_s3_sizes[str(local_path)] = int(prior_row.get("s3_bytes"))
            continue
        pending_path_to_s3_key[str(local_path)] = artifact_s3_key(artifact)

    s3_sizes = dict(cached_s3_sizes)
    if pending_path_to_s3_key:
        s3_sizes.update(
            batch_s3_object_sizes(
                args.bucket,
                pending_path_to_s3_key,
                args.region,
                workers=args.workers,
            )
        )

    report_rows: list[dict[str, str]] = []
    artifact_manifest_rows: list[dict[str, object]] = []
    failures = 0
    for artifact in artifacts:
        local_path = artifact.local_path
        s3_key = artifact_s3_key(artifact)
        local_bytes = local_path.stat().st_size
        s3_bytes = s3_sizes.get(str(local_path))
        is_match = s3_bytes is not None and int(local_bytes) == int(s3_bytes)
        verify_mode = "cached" if str(local_path) in cached_s3_sizes else "live"
        if not is_match:
            failures += 1
        report_rows.append(
            {
                "artifact_id": artifact.artifact_id,
                "category": artifact.category,
                "local_path": str(local_path),
                "local_bytes": str(local_bytes),
                "s3_bucket": args.bucket,
                "s3_key": s3_key,
                "s3_bytes": "" if s3_bytes is None else str(s3_bytes),
                "size_match": "TRUE" if is_match else "FALSE",
                "verify_mode": verify_mode,
            }
        )
        artifact_manifest_rows.append(
            {
                "artifact_id": artifact.artifact_id,
                "category": artifact.category,
                "local_path": str(local_path),
                "local_bytes": int(local_bytes),
                "local_signature": local_signature_by_artifact[artifact.artifact_id],
                "s3_bucket": args.bucket,
                "s3_key": s3_key,
                "s3_bytes": None if s3_bytes is None else int(s3_bytes),
                "size_match": bool(is_match),
                "verify_mode": verify_mode,
            }
        )
        print(
            f"[verify] {artifact.artifact_id} => local={local_bytes} s3={s3_bytes} match={is_match} mode={verify_mode}",
            flush=True,
        )

    report_path.parent.mkdir(parents=True, exist_ok=True)
    fields = [
        "artifact_id",
        "category",
        "local_path",
        "local_bytes",
        "s3_bucket",
        "s3_key",
        "s3_bytes",
        "size_match",
        "verify_mode",
    ]
    with open(report_path, "w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields)
        writer.writeheader()
        writer.writerows(report_rows)
    print(f"[verify] Wrote report: {report_path}", flush=True)

    manifest_sources = {
        "manifest_basic_allforms_presilver": BASIC_ALLFORMS_BUILD_MANIFEST_JSON,
        "manifest_basic_plus_combined_presilver": BASIC_PLUS_COMBINED_BUILD_MANIFEST_JSON,
        "manifest_basic_allforms_filtered": FILTERED_BASIC_MANIFEST_JSON,
        "manifest_filtered_mixed": FILTERED_MIXED_MANIFEST_JSON,
    }
    for manifest_id, manifest_path in manifest_sources.items():
        path = Path(manifest_path)
        if not path.exists():
            continue
        payload = read_json(path)
        artifact_manifest_rows.append(
            {
                "artifact_id": manifest_id,
                "category": "build_manifest",
                "local_path": str(path),
                "local_bytes": int(path.stat().st_size),
                "local_signature": build_path_signature(path),
                "s3_bucket": "",
                "s3_key": "",
                "s3_bytes": None,
                "size_match": None,
                "verify_mode": "local_only",
                "rows_output": payload.get("rows_output", ""),
                "columns_output": payload.get("columns_output", ""),
                "input_signature": payload.get("input_signature", {}),
            }
        )

    manifest_payload = {
        "created_at_utc": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "emit": args.emit,
        "verify_only_changed": bool(args.verify_only_changed),
        "artifact_count": len(artifact_manifest_rows),
        "failure_count": failures,
        "artifacts": artifact_manifest_rows,
    }
    write_json(artifact_manifest_json_path, manifest_payload)
    csv_fields = [
        "artifact_id",
        "category",
        "local_path",
        "local_bytes",
        "local_signature_json",
        "s3_bucket",
        "s3_key",
        "s3_bytes",
        "size_match",
        "verify_mode",
        "rows_output",
        "columns_output",
        "input_signature_json",
    ]
    artifact_manifest_csv_path.parent.mkdir(parents=True, exist_ok=True)
    with open(artifact_manifest_csv_path, "w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=csv_fields)
        writer.writeheader()
        for row in artifact_manifest_rows:
            writer.writerow(
                {
                    "artifact_id": row.get("artifact_id", ""),
                    "category": row.get("category", ""),
                    "local_path": row.get("local_path", ""),
                    "local_bytes": row.get("local_bytes", ""),
                    "local_signature_json": _flatten_json(row.get("local_signature")),
                    "s3_bucket": row.get("s3_bucket", ""),
                    "s3_key": row.get("s3_key", ""),
                    "s3_bytes": "" if row.get("s3_bytes") is None else row.get("s3_bytes"),
                    "size_match": "" if row.get("size_match") is None else str(bool(row.get("size_match"))).upper(),
                    "verify_mode": row.get("verify_mode", ""),
                    "rows_output": row.get("rows_output", ""),
                    "columns_output": row.get("columns_output", ""),
                    "input_signature_json": _flatten_json(row.get("input_signature")),
                }
            )
    print(f"[verify] Wrote artifact manifest JSON: {artifact_manifest_json_path}", flush=True)
    print(f"[verify] Wrote artifact manifest CSV:  {artifact_manifest_csv_path}", flush=True)

    for metadata_artifact in pipeline_metadata_artifacts():
        local_path = metadata_artifact.local_path
        upload_prefix = args.report_prefix
        s3_key = artifact_s3_key(metadata_artifact, upload_prefix)
        upload_file_with_progress(
            local_path,
            args.bucket,
            s3_key,
            args.region,
            extra_args={"ContentType": metadata_artifact.content_type},
        )
        print(f"[verify] Uploaded metadata artifact to s3://{args.bucket}/{s3_key}", flush=True)

    print(f"[verify] Failures: {failures}", flush=True)
    print_elapsed(start, "Step 11")
    if failures > 0:
        raise SystemExit(f"Curated output size verification failed for {failures} artifact(s). See {report_path}.")


if __name__ == "__main__":
    main()
