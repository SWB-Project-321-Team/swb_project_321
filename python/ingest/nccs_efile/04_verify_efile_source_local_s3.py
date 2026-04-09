"""
Step 04: Verify source/local/S3 byte parity for the raw NCCS efile assets.
"""

from __future__ import annotations

import argparse
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

from common import (
    DEFAULT_S3_BUCKET,
    DEFAULT_S3_REGION,
    EFILE_RAW_DIR,
    LATEST_RELEASE_JSON,
    META_DIR,
    META_PREFIX,
    RAW_PREFIX,
    START_YEAR_DEFAULT,
    apply_source_size_cache_to_release,
    asset_s3_key,
    banner,
    cache_source_size,
    compute_size_match,
    ensure_work_dirs,
    load_csv_rows,
    load_env_from_secrets,
    load_source_size_cache_from_manifest,
    local_asset_path,
    measure_remote_streamed_bytes,
    meta_s3_key,
    print_elapsed,
    release_manifest_path,
    resolve_release_and_write_metadata,
    s3_object_size,
    selected_assets,
    size_report_path,
    source_size_cache_key,
    upload_file_with_progress,
    write_csv,
    write_json,
)


def main() -> None:
    parser = argparse.ArgumentParser(description="Verify raw NCCS efile tables against source and S3 sizes.")
    parser.add_argument("--start-year", type=int, default=START_YEAR_DEFAULT, help="First tax year to include (default: 2022)")
    parser.add_argument("--bucket", default=DEFAULT_S3_BUCKET, help="Target S3 bucket")
    parser.add_argument("--region", default=DEFAULT_S3_REGION, help="Target S3 region")
    parser.add_argument("--raw-dir", type=Path, default=EFILE_RAW_DIR, help="Local efile raw root directory")
    parser.add_argument("--metadata-dir", type=Path, default=META_DIR, help="Local metadata directory")
    parser.add_argument("--raw-prefix", default=RAW_PREFIX, help="S3 raw prefix")
    parser.add_argument("--meta-prefix", default=META_PREFIX, help="S3 metadata prefix")
    args = parser.parse_args()

    start = time.perf_counter()
    banner("STEP 04 - VERIFY SOURCE / LOCAL / S3 RAW SIZE PARITY")
    load_env_from_secrets()
    ensure_work_dirs(raw_dir=args.raw_dir, metadata_dir=args.metadata_dir)

    print(f"[verify] Start year: {args.start_year}", flush=True)
    print(f"[verify] Bucket: {args.bucket}", flush=True)
    print(f"[verify] Region: {args.region}", flush=True)

    release = resolve_release_and_write_metadata(args.metadata_dir, start_year=args.start_year)
    manifest_path = release_manifest_path(args.metadata_dir, args.start_year)
    report_path = size_report_path(args.metadata_dir, args.start_year)
    latest_release_path = args.metadata_dir / LATEST_RELEASE_JSON.name

    if not manifest_path.exists():
        raise FileNotFoundError(f"Raw manifest not found: {manifest_path}. Run step 02 first.")

    manifest_rows = [{k: v for k, v in row.items()} for row in load_csv_rows(manifest_path)]
    manifest_rows_by_key = {
        source_size_cache_key(row.get("source_url", ""), row.get("source_last_modified") or None): row for row in manifest_rows
    }
    release = apply_source_size_cache_to_release(release, load_source_size_cache_from_manifest(manifest_path))

    report_rows: list[dict[str, object]] = []
    failures = 0
    assets = selected_assets(release)
    worker_count = max(1, min(4, len(assets)))
    print(f"[verify] Assets to verify: {len(assets)}", flush=True)
    print(f"[verify] Workers: {worker_count}", flush=True)

    def _verify_one(asset: dict[str, object]) -> dict[str, object]:
        local_path = local_asset_path(args.raw_dir, args.metadata_dir, asset)
        if not local_path.exists():
            raise FileNotFoundError(f"Local asset not found: {local_path}. Run step 02 first.")
        source_bytes = asset.get("source_content_length_bytes")
        source_measured = False
        if source_bytes is None:
            source_measured = True
            source_bytes = measure_remote_streamed_bytes(str(asset["source_url"]))
        local_bytes = local_path.stat().st_size
        s3_key = asset_s3_key(args.raw_prefix, args.meta_prefix, asset)
        s3_bytes = s3_object_size(args.bucket, s3_key, args.region)
        size_match = compute_size_match(int(source_bytes) if source_bytes is not None else None, local_bytes, s3_bytes)
        return {
            "asset_group": asset["asset_group"],
            "asset_type": asset["asset_type"],
            "tax_year": asset["tax_year"],
            "table_name": asset["table_name"],
            "is_partial_year": asset["is_partial_year"],
            "source_url": asset["source_url"],
            "filename": asset["filename"],
            "source_content_length_bytes": int(source_bytes) if source_bytes is not None else None,
            "source_last_modified": asset["source_last_modified"],
            "local_path": str(local_path),
            "local_bytes": local_bytes,
            "s3_bucket": args.bucket,
            "s3_key": s3_key,
            "s3_bytes": s3_bytes,
            "size_match": "TRUE" if size_match else "FALSE",
            "_source_measured": source_measured,
        }

    with ThreadPoolExecutor(max_workers=worker_count) as executor:
        future_to_asset = {executor.submit(_verify_one, asset): asset for asset in assets}
        for future in as_completed(future_to_asset):
            row = future.result()
            cache_key = source_size_cache_key(str(row["source_url"]), row["source_last_modified"] or None)

            if row["_source_measured"]:
                print(f"[verify] Measured source bytes via streamed GET for {row['filename']}.", flush=True)
                release = cache_source_size(
                    release,
                    source_url=str(row["source_url"]),
                    source_last_modified=row["source_last_modified"] or None,
                    source_content_length_bytes=int(row["source_content_length_bytes"]),
                )
                if cache_key in manifest_rows_by_key:
                    manifest_rows_by_key[cache_key]["source_content_length_bytes"] = str(row["source_content_length_bytes"])

            if cache_key in manifest_rows_by_key:
                manifest_row = manifest_rows_by_key[cache_key]
                manifest_row["local_path"] = str(row["local_path"])
                manifest_row["local_bytes"] = "" if row["local_bytes"] is None else str(row["local_bytes"])
                manifest_row["s3_bucket"] = str(row["s3_bucket"])
                manifest_row["s3_key"] = str(row["s3_key"])
                manifest_row["s3_bytes"] = "" if row["s3_bytes"] is None else str(row["s3_bytes"])
                manifest_row["size_match"] = str(row["size_match"])

            if row["size_match"] != "TRUE":
                failures += 1
            print(
                f"[verify] {row['filename']}: source={row['source_content_length_bytes']}, "
                f"local={row['local_bytes']}, s3={row['s3_bytes']}, match={row['size_match']}",
                flush=True,
            )
            report_rows.append({k: v for k, v in row.items() if not str(k).startswith("_")})

    report_rows.sort(key=lambda r: (str(r["tax_year"]), str(r["table_name"])))
    fieldnames = [
        "asset_group",
        "asset_type",
        "tax_year",
        "table_name",
        "is_partial_year",
        "source_url",
        "filename",
        "source_content_length_bytes",
        "source_last_modified",
        "local_path",
        "local_bytes",
        "s3_bucket",
        "s3_key",
        "s3_bytes",
        "size_match",
    ]
    write_csv(manifest_path, manifest_rows, fieldnames)
    write_json(latest_release_path, release)
    write_csv(report_path, report_rows, fieldnames)

    report_key = meta_s3_key(args.meta_prefix, report_path.name)
    print(f"[verify] Uploading verification report to s3://{args.bucket}/{report_key}", flush=True)
    upload_file_with_progress(report_path, args.bucket, report_key, args.region, extra_args={"ContentType": "text/csv"})

    print(f"[verify] Failures: {failures}", flush=True)
    print_elapsed(start, "Step 04")
    if failures:
        raise SystemExit(f"Raw size verification failed for {failures} asset(s). See {report_path}.")


if __name__ == "__main__":
    main()
