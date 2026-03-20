"""
Step 02: Download the discovered IRS SOI county release locally and write a raw manifest.
"""

from __future__ import annotations

import argparse
import time
from pathlib import Path

from common import (
    DEFAULT_S3_BUCKET,
    LATEST_RELEASE_JSON,
    META_DIR,
    RAW_DIR,
    RAW_PREFIX,
    banner,
    cache_source_size,
    download_with_progress,
    ensure_work_dirs,
    load_env_from_secrets,
    print_elapsed,
    raw_s3_key,
    release_manifest_path,
    resolve_release_and_write_metadata,
    write_csv,
    write_json,
    year_raw_dir,
)


def main() -> None:
    parser = argparse.ArgumentParser(description="Download the IRS SOI county release locally.")
    parser.add_argument("--year", default="latest", help="County release year or 'latest' (default: latest)")
    parser.add_argument("--bucket", default=DEFAULT_S3_BUCKET, help="Target S3 bucket recorded in the manifest")
    parser.add_argument("--raw-dir", type=Path, default=RAW_DIR, help="Local raw root directory")
    parser.add_argument("--metadata-dir", type=Path, default=META_DIR, help="Local metadata directory")
    parser.add_argument("--raw-prefix", default=RAW_PREFIX, help="S3 raw prefix recorded in the manifest")
    parser.add_argument("--overwrite", action="store_true", help="Re-download even when a matching local file already exists")
    args = parser.parse_args()

    start = time.perf_counter()
    banner("STEP 02 - DOWNLOAD IRS SOI COUNTY RELEASE")
    load_env_from_secrets()
    ensure_work_dirs(raw_dir=args.raw_dir, metadata_dir=args.metadata_dir)

    print(f"[download] Requested year: {args.year}", flush=True)
    print(f"[download] Raw root: {args.raw_dir}", flush=True)
    print(f"[download] Metadata directory: {args.metadata_dir}", flush=True)
    print(f"[download] Overwrite: {args.overwrite}", flush=True)

    release = resolve_release_and_write_metadata(args.year, args.metadata_dir)
    tax_year = int(release["tax_year"])
    release_raw_dir = year_raw_dir(args.raw_dir, tax_year)
    release_raw_dir.mkdir(parents=True, exist_ok=True)
    print(f"[download] Resolved tax year: {tax_year}", flush=True)
    print(f"[download] Year raw directory: {release_raw_dir}", flush=True)

    manifest_rows: list[dict[str, object]] = []
    for asset in release["assets"]:
        source_url = asset["source_url"]
        filename = asset["filename"]
        expected_bytes = asset["source_content_length_bytes"]
        local_path = release_raw_dir / filename
        s3_key = raw_s3_key(args.raw_prefix, tax_year, filename)

        print(f"[download] Asset: {asset['asset_type']} -> {filename}", flush=True)
        print(f"[download] Source URL: {source_url}", flush=True)
        print(f"[download] Expected bytes: {expected_bytes}", flush=True)

        local_bytes = local_path.stat().st_size if local_path.exists() else None
        local_matches_source = expected_bytes is not None and local_bytes == expected_bytes
        should_download = args.overwrite or not local_path.exists() or (expected_bytes is not None and not local_matches_source)

        if should_download:
            if local_path.exists() and not args.overwrite:
                print(
                    f"[download] Existing file size mismatch or unknown source size; refreshing {local_path.name}.",
                    flush=True,
                )
            file_start = time.perf_counter()
            local_bytes = download_with_progress(source_url, local_path, expected_bytes=expected_bytes)
            print(f"[download] Wrote {local_path} ({local_bytes} bytes)", flush=True)
            print_elapsed(file_start, f"download {local_path.name}")
        else:
            print(f"[download] Skip unchanged local file: {local_path} ({local_bytes} bytes)", flush=True)

        if expected_bytes is None:
            # IRS serves the CSVs with gzip, so header byte counts are not reliable for the
            # decoded CSV bytes we persist locally. Use the actual downloaded local bytes as the
            # source-sized value recorded in the manifest.
            expected_bytes = local_bytes

        if expected_bytes is not None:
            release = cache_source_size(
                release,
                source_url=source_url,
                source_last_modified=asset["source_last_modified"],
                source_content_length_bytes=int(expected_bytes),
            )

        if expected_bytes is not None and local_bytes != expected_bytes:
            raise RuntimeError(
                f"Downloaded size mismatch for {filename}: source={expected_bytes}, local={local_bytes}"
            )

        manifest_rows.append(
            {
                "tax_year": tax_year,
                "asset_type": asset["asset_type"],
                "source_page_url": release["year_page_url"],
                "source_url": source_url,
                "filename": filename,
                "source_content_length_bytes": expected_bytes,
                "source_last_modified": asset["source_last_modified"],
                "local_path": str(local_path),
                "local_bytes": local_bytes,
                "s3_bucket": args.bucket,
                "s3_key": s3_key,
                "s3_bytes": "",
                "size_match": "",
            }
        )

    manifest_path = release_manifest_path(args.metadata_dir, tax_year)
    fieldnames = [
        "tax_year",
        "asset_type",
        "source_page_url",
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
    write_json(args.metadata_dir / LATEST_RELEASE_JSON.name, release)
    print(f"[download] Downloaded assets: {len(manifest_rows)}", flush=True)
    print_elapsed(start, "Step 02")


if __name__ == "__main__":
    main()
