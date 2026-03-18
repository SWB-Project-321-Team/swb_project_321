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
    SIZE_REPORT_CSV,
    banner,
    ensure_dirs,
    load_catalog_rows,
    load_env_from_secrets,
    print_elapsed,
    probe_url_head,
    s3_object_size,
    select_required_datasets,
    upload_file_with_progress,
    url_basename,
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
    args = parser.parse_args()

    start = time.perf_counter()
    banner("STEP 05 - VERIFY SOURCE/LOCAL/S3 SIZE MATCH")
    load_env_from_secrets()
    ensure_dirs()

    raw_dir = Path(args.raw_dir)
    report_path = Path(args.report_csv)

    rows = load_catalog_rows(Path(args.catalog_csv))
    required = select_required_datasets(rows)
    print(f"[verify] Required datasets: {len(required)}", flush=True)

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
        s3_bytes = s3_object_size(args.bucket, s3_key, args.region)

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

    print(f"[verify] Failures: {failures}", flush=True)
    print_elapsed(start, "Step 05")
    if failures > 0:
        raise SystemExit(f"Strict size check failed for {failures} dataset(s). See {report_path}.")


if __name__ == "__main__":
    main()

