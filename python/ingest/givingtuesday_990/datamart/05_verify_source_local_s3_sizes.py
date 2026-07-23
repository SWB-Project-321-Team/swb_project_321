"""
Step 05: Strict size verification for required raw datasets.

For each required dataset:
- source bytes (Content-Length from DataMart URL)
- local bytes (downloaded file)
- S3 bytes (Bronze raw object)

All three must match, otherwise this script fails.
"""

from __future__ import annotations

import argparse
import csv
import time
from pathlib import Path

from tqdm import tqdm

from common import (
    BRONZE_META_PREFIX,
    BRONZE_RAW_PREFIX,
    CATALOG_CSV,
    DEFAULT_S3_BUCKET,
    DEFAULT_S3_REGION,
    META_DIR,
    RAW_DIR,
    RAW_VERIFY_STATE_JSON,
    SIZE_REPORT_CSV,
    banner,
    batch_s3_object_sizes,
    build_input_signature,
    ensure_dirs,
    load_catalog_rows,
    load_env_from_secrets,
    manifest_matches_expectation,
    print_elapsed,
    print_transfer_settings,
    probe_url_head,
    select_required_datasets,
    upload_file_with_progress,
    url_basename,
    read_json,
    write_json,
)


def main() -> None:
    parser = argparse.ArgumentParser(description="Verify source/local/S3 byte-size parity for required datasets.")
    parser.add_argument("--catalog-csv", default=str(CATALOG_CSV), help="Catalog CSV path")
    parser.add_argument("--raw-dir", default=str(RAW_DIR), help="Local raw data folder")
    parser.add_argument("--bucket", default=DEFAULT_S3_BUCKET, help="S3 bucket")
    parser.add_argument("--region", default=DEFAULT_S3_REGION, help="S3 region")
    parser.add_argument("--raw-prefix", default=BRONZE_RAW_PREFIX, help="Bronze raw prefix")
    parser.add_argument("--meta-prefix", default=BRONZE_META_PREFIX, help="Bronze metadata prefix")
    parser.add_argument("--report-csv", default=str(SIZE_REPORT_CSV), help="Output report CSV")
    parser.add_argument("--state-json", default=str(RAW_VERIFY_STATE_JSON), help="Cached raw-verify state JSON path")
    parser.add_argument(
        "--skip-if-unchanged",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="Skip the verify pass when cached inputs and outputs still match",
    )
    parser.add_argument("--force-rebuild", action="store_true", help="Ignore cached verify state and rerun the step")
    args = parser.parse_args()

    start = time.perf_counter()
    banner("STEP 05 - VERIFY SOURCE/LOCAL/S3 SIZE MATCH")
    load_env_from_secrets()
    ensure_dirs()
    print_transfer_settings(label="gt_verify_raw")

    raw_dir = Path(args.raw_dir)
    report_path = Path(args.report_csv)
    state_path = Path(args.state_json)

    rows = load_catalog_rows(Path(args.catalog_csv))
    required = select_required_datasets(rows)
    print(f"[verify] Required datasets: {len(required)}", flush=True)
    print(f"[verify] Skip if unchanged: {args.skip_if_unchanged}", flush=True)
    print(f"[verify] Force rebuild: {args.force_rebuild}", flush=True)

    required_local_paths = {f"raw::{url_basename(row.get('download_url', ''))}": raw_dir / url_basename(row.get("download_url", "")) for row in required}
    input_signature = build_input_signature({"catalog_csv": Path(args.catalog_csv), **required_local_paths})
    build_options = {
        "bucket": args.bucket,
        "region": args.region,
        "raw_prefix": args.raw_prefix,
        "meta_prefix": args.meta_prefix,
        "required_dataset_count": len(required),
    }
    if args.skip_if_unchanged and not args.force_rebuild and manifest_matches_expectation(
        state_path,
        expected_input_signature=input_signature,
        expected_options=build_options,
        required_outputs=[report_path],
    ):
        cached = read_json(state_path)
        print("[verify] Inputs and report match cached verify state; skipping step.", flush=True)
        print(f"[verify] Cached failure count: {cached.get('failures', '')}", flush=True)
        print_elapsed(start, "Step 05")
        return

    path_to_s3_key = {
        str(raw_dir / url_basename(row.get("download_url", ""))): f"{args.raw_prefix.rstrip('/')}/{url_basename(row.get('download_url', ''))}"
        for row in required
    }
    s3_sizes = batch_s3_object_sizes(args.bucket, path_to_s3_key, args.region)

    report_rows: list[dict[str, str]] = []
    failures = 0
    for row in tqdm(required, desc="Verify datasets", unit="dataset"):
        title = row.get("title", "")
        form = row.get("form_type", "")
        url = row.get("download_url", "")
        filename = url_basename(url)
        local_path = raw_dir / filename
        s3_key = f"{args.raw_prefix.rstrip('/')}/{filename}"

        # Source bytes from catalog if present; fallback probe.
        source_bytes = None
        src_val = row.get("source_content_length_bytes", "")
        if src_val and str(src_val).isdigit():
            source_bytes = int(src_val)
            source_status = row.get("url_status_text", "catalog_value")
        else:
            status, content_len, status_text = probe_url_head(url)
            source_bytes = content_len
            source_status = status_text if status is not None else "probe_error"

        local_bytes = local_path.stat().st_size if local_path.exists() else None
        s3_bytes = s3_sizes.get(str(local_path))

        is_match = (
            source_bytes is not None
            and local_bytes is not None
            and s3_bytes is not None
            and source_bytes == local_bytes == s3_bytes
        )
        if not is_match:
            failures += 1

        report_rows.append(
            {
                "dataset_id": row.get("dataset_id", ""),
                "title": title,
                "form_type": form,
                "download_url": url,
                "filename": filename,
                "source_bytes": "" if source_bytes is None else str(source_bytes),
                "source_status": source_status,
                "local_path": str(local_path),
                "local_bytes": "" if local_bytes is None else str(local_bytes),
                "s3_bucket": args.bucket,
                "s3_key": s3_key,
                "s3_bytes": "" if s3_bytes is None else str(s3_bytes),
                "size_match": "TRUE" if is_match else "FALSE",
            }
        )

        print(
            f"[verify] {title} [{form}] => source={source_bytes}, local={local_bytes}, s3={s3_bytes}, match={is_match}",
            flush=True,
        )

    # Write report CSV.
    report_path.parent.mkdir(parents=True, exist_ok=True)
    fields = [
        "dataset_id",
        "title",
        "form_type",
        "download_url",
        "filename",
        "source_bytes",
        "source_status",
        "local_path",
        "local_bytes",
        "s3_bucket",
        "s3_key",
        "s3_bytes",
        "size_match",
    ]
    with open(report_path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fields)
        w.writeheader()
        w.writerows(report_rows)
    print(f"[verify] Wrote size report: {report_path}", flush=True)

    # Upload report to bronze metadata immediately.
    report_s3_key = f"{args.meta_prefix.rstrip('/')}/{report_path.name}"
    upload_file_with_progress(report_path, args.bucket, report_s3_key, args.region, extra_args={"ContentType": "text/csv"})
    print(f"[verify] Uploaded report to s3://{args.bucket}/{report_s3_key}", flush=True)
    write_json(
        state_path,
        {
            "created_at_utc": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
            "input_signature": input_signature,
            "build_options": build_options,
            "failures": failures,
            "verified_dataset_count": len(required),
            "report_csv": str(report_path),
        },
    )
    print(f"[verify] Wrote cached verify state: {state_path}", flush=True)

    print(f"[verify] Failures: {failures}", flush=True)
    print_elapsed(start, "Step 05")
    if failures > 0:
        raise SystemExit(f"Strict size check failed for {failures} dataset(s). See {report_path}.")


if __name__ == "__main__":
    main()
