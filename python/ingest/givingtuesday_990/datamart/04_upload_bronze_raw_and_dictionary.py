"""
Step 04: Upload raw DataMart files and dictionary metadata to Bronze.

Uploads:
- raw-phase metadata -> bronze/givingtuesday_990/datamarts/metadata/
- raw/*      -> bronze/givingtuesday_990/datamarts/raw/
"""

from __future__ import annotations

import argparse
import mimetypes
import time
from pathlib import Path

from common import (
    BRONZE_META_PREFIX,
    BRONZE_RAW_PREFIX,
    DEFAULT_S3_BUCKET,
    DEFAULT_S3_REGION,
    META_DIR,
    RAW_DIR,
    RAW_UPLOAD_STATE_JSON,
    UPLOAD_WORKERS,
    banner,
    bronze_raw_metadata_files,
    build_input_signature,
    ensure_dirs,
    load_env_from_secrets,
    manifest_matches_expectation,
    parallel_map,
    print_elapsed,
    print_parallel_summary,
    print_transfer_settings,
    read_json,
    should_skip_upload,
    upload_file_with_progress,
    write_json,
)


def _content_type_for(path: Path) -> str:
    """Infer content type for S3 metadata."""
    ct, _ = mimetypes.guess_type(str(path))
    return ct or "application/octet-stream"


def _folder_files(folder: Path) -> list[Path]:
    """Return every non-recursive file in one folder."""
    files = sorted([p for p in folder.glob("*") if p.is_file()])
    print(f"[upload] Folder {folder} has {len(files)} file(s)", flush=True)
    return files


def _required_metadata_files(meta_dir: Path) -> list[Path]:
    """Return the metadata files explicitly owned by the Bronze raw upload step."""
    files = []
    for path in bronze_raw_metadata_files():
        candidate = meta_dir / path.name
        if not candidate.exists():
            raise FileNotFoundError(f"Required Bronze metadata file not found: {candidate}")
        files.append(candidate)
    print(f"[upload] Bronze metadata allowlist has {len(files)} file(s)", flush=True)
    return files


def main() -> None:
    parser = argparse.ArgumentParser(description="Upload local raw + metadata files to Bronze S3 prefixes.")
    parser.add_argument("--bucket", default=DEFAULT_S3_BUCKET, help="Target S3 bucket")
    parser.add_argument("--region", default=DEFAULT_S3_REGION, help="Target S3 region")
    parser.add_argument("--meta-dir", default=str(META_DIR), help="Local metadata directory")
    parser.add_argument("--raw-dir", default=str(RAW_DIR), help="Local raw files directory")
    parser.add_argument("--meta-prefix", default=BRONZE_META_PREFIX, help="Bronze metadata prefix")
    parser.add_argument("--raw-prefix", default=BRONZE_RAW_PREFIX, help="Bronze raw prefix")
    parser.add_argument("--state-json", default=str(RAW_UPLOAD_STATE_JSON), help="Cached raw-upload state JSON path")
    parser.add_argument("--overwrite", action="store_true", help="Upload even when remote bytes already match")
    parser.add_argument(
        "--skip-if-unchanged",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="Skip the upload pass when cached local inputs and outputs still match",
    )
    parser.add_argument("--force-rebuild", action="store_true", help="Ignore cached upload state and rerun the step")
    parser.add_argument("--workers", type=int, default=UPLOAD_WORKERS, help="Parallel upload worker count")
    args = parser.parse_args()

    start = time.perf_counter()
    banner("STEP 04 - UPLOAD BRONZE RAW + DICTIONARY")
    load_env_from_secrets()
    ensure_dirs()
    print_transfer_settings(label="gt_raw_upload")

    meta_dir = Path(args.meta_dir)
    raw_dir = Path(args.raw_dir)
    state_path = Path(args.state_json)
    if not meta_dir.exists():
        raise FileNotFoundError(f"Metadata directory not found: {meta_dir}")
    if not raw_dir.exists():
        raise FileNotFoundError(f"Raw directory not found: {raw_dir}")

    meta_files = _required_metadata_files(meta_dir)
    raw_files = _folder_files(raw_dir)
    input_signature = build_input_signature(
        {
            **{f"meta::{path.name}": path for path in meta_files},
            **{f"raw::{path.name}": path for path in raw_files},
        }
    )
    build_options = {
        "bucket": args.bucket,
        "region": args.region,
        "meta_prefix": args.meta_prefix,
        "raw_prefix": args.raw_prefix,
        "meta_file_count": len(meta_files),
        "raw_file_count": len(raw_files),
    }
    if args.skip_if_unchanged and not args.overwrite and not args.force_rebuild and manifest_matches_expectation(
        state_path,
        expected_input_signature=input_signature,
        expected_options=build_options,
        required_outputs=meta_files + raw_files,
    ):
        cached = read_json(state_path)
        print("[upload] Inputs match cached upload state; skipping step.", flush=True)
        print(f"[upload] Cached uploaded files: {cached.get('uploaded_file_count', '')}", flush=True)
        print_elapsed(start, "Step 04")
        return

    pending: list[tuple[Path, str]] = []
    skipped = 0
    for path in meta_files:
        key = f"{args.meta_prefix.rstrip('/')}/{path.name}"
        if should_skip_upload(path, args.bucket, key, args.region, overwrite=args.overwrite):
            skipped += 1
            print(f"[upload] Skip unchanged: s3://{args.bucket}/{key}", flush=True)
            continue
        pending.append((path, key))
    for path in raw_files:
        key = f"{args.raw_prefix.rstrip('/')}/{path.name}"
        if should_skip_upload(path, args.bucket, key, args.region, overwrite=args.overwrite):
            skipped += 1
            print(f"[upload] Skip unchanged: s3://{args.bucket}/{key}", flush=True)
            continue
        pending.append((path, key))

    print_parallel_summary(
        label="gt_raw_bronze_upload",
        pending=len(pending),
        skipped=skipped,
        workers=min(max(1, args.workers), max(1, len(pending) if pending else 1)),
    )

    def _upload_one(task: tuple[Path, str]) -> str:
        path, key = task
        ctype = _content_type_for(path)
        print(f"[upload] {path} -> s3://{args.bucket}/{key} ({ctype})", flush=True)
        upload_file_with_progress(path, args.bucket, key, args.region, extra_args={"ContentType": ctype})
        return str(path)

    uploaded_paths = parallel_map(
        pending,
        worker_count=max(1, args.workers),
        desc="upload gt raw+meta",
        unit="file",
        fn=_upload_one,
    ) if pending else []

    write_json(
        state_path,
        {
            "created_at_utc": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
            "input_signature": input_signature,
            "build_options": build_options,
            "uploaded_file_count": len(uploaded_paths),
            "skipped_unchanged_file_count": skipped,
        },
    )
    print(f"[upload] Wrote cached upload state: {state_path}", flush=True)
    print(f"[upload] Completed Bronze upload: metadata={len(meta_files)}, raw={len(raw_files)}", flush=True)
    print_elapsed(start, "Step 04")


if __name__ == "__main__":
    main()
