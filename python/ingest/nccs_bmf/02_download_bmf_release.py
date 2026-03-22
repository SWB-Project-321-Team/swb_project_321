"""
Step 02: Download the discovered NCCS BMF yearly raw assets locally and write a raw manifest.
"""

from __future__ import annotations

import argparse
import time
from pathlib import Path

from common import (
    BMF_RAW_DIR,
    DEFAULT_S3_BUCKET,
    LATEST_RELEASE_JSON,
    META_DIR,
    META_PREFIX,
    RAW_PREFIX,
    START_YEAR_DEFAULT,
    asset_s3_key,
    banner,
    cache_source_size,
    download_with_progress,
    ensure_work_dirs,
    load_env_from_secrets,
    local_asset_path,
    print_elapsed,
    release_manifest_path,
    resolve_release_and_write_metadata,
    selected_assets,
    write_csv,
    write_json,
)


def main() -> None:
    parser = argparse.ArgumentParser(description="Download the NCCS BMF yearly raw assets locally.")
    parser.add_argument("--start-year", type=int, default=START_YEAR_DEFAULT, help="First year to include (default: 2022)")
    parser.add_argument("--bucket", default=DEFAULT_S3_BUCKET, help="Target S3 bucket recorded in the manifest")
    parser.add_argument("--raw-dir", type=Path, default=BMF_RAW_DIR, help="Local BMF raw root directory")
    parser.add_argument("--metadata-dir", type=Path, default=META_DIR, help="Local metadata directory")
    parser.add_argument("--raw-prefix", default=RAW_PREFIX, help="S3 raw prefix recorded in the manifest")
    parser.add_argument("--meta-prefix", default=META_PREFIX, help="S3 metadata prefix recorded in the manifest")
    parser.add_argument("--overwrite", action="store_true", help="Re-download even when a matching local file already exists")
    args = parser.parse_args()

    start = time.perf_counter()
    banner("STEP 02 - DOWNLOAD NCCS BMF RAW ASSETS")
    load_env_from_secrets()
    ensure_work_dirs(raw_dir=args.raw_dir, metadata_dir=args.metadata_dir)

    print(f"[download] Start year: {args.start_year}", flush=True)
    print(f"[download] Raw root: {args.raw_dir}", flush=True)
    print(f"[download] Metadata directory: {args.metadata_dir}", flush=True)
    print(f"[download] Overwrite: {args.overwrite}", flush=True)

    release = resolve_release_and_write_metadata(args.metadata_dir, start_year=args.start_year)
    manifest_rows: list[dict[str, object]] = []
    download_count = 0
    skip_count = 0

    for asset in selected_assets(release):
        source_url = str(asset["source_url"])
        filename = str(asset["filename"])
        expected_bytes = asset.get("source_content_length_bytes")
        local_path = local_asset_path(args.raw_dir, args.metadata_dir, asset)
        s3_key = asset_s3_key(args.raw_prefix, args.meta_prefix, asset)

        print(f"[download] Asset: {filename}", flush=True)
        print(
            f"[download] snapshot_year={asset['snapshot_year']} | "
            f"snapshot_month={asset['snapshot_month'] or '<blank>'} | "
            f"source_period={asset['source_period']} | basis={asset['year_basis']}",
            flush=True,
        )
        print(f"[download] Source URL: {source_url}", flush=True)
        print(f"[download] Local target: {local_path}", flush=True)
        print(f"[download] Expected bytes: {expected_bytes}", flush=True)

        local_bytes = local_path.stat().st_size if local_path.exists() else None
        local_matches_source = expected_bytes is not None and local_bytes == expected_bytes
        should_download = args.overwrite or not local_path.exists() or (expected_bytes is not None and not local_matches_source)

        if should_download:
            file_start = time.perf_counter()
            local_bytes = download_with_progress(source_url, local_path, expected_bytes=expected_bytes)
            print(f"[download] Wrote {local_path} ({local_bytes} bytes)", flush=True)
            print_elapsed(file_start, f"download {local_path.name}")
            download_count += 1
        else:
            print(f"[download] Skip unchanged local file: {local_path} ({local_bytes} bytes)", flush=True)
            skip_count += 1

        if expected_bytes is None:
            expected_bytes = local_bytes
        if expected_bytes is not None:
            release = cache_source_size(
                release,
                source_url=source_url,
                source_last_modified=asset.get("source_last_modified") or None,
                source_content_length_bytes=int(expected_bytes),
            )
        if expected_bytes is not None and local_bytes != expected_bytes:
            raise RuntimeError(f"Downloaded size mismatch for {filename}: source={expected_bytes}, local={local_bytes}")

        manifest_rows.append(
            {
                "asset_group": asset["asset_group"],
                "asset_type": asset["asset_type"],
                "snapshot_year": asset["snapshot_year"],
                "snapshot_month": asset["snapshot_month"],
                "source_period": asset["source_period"],
                "year_basis": asset["year_basis"],
                "source_url": source_url,
                "filename": filename,
                "source_content_length_bytes": expected_bytes,
                "source_last_modified": asset.get("source_last_modified") or "",
                "local_path": str(local_path),
                "local_bytes": local_bytes,
                "s3_bucket": args.bucket,
                "s3_key": s3_key,
                "s3_bytes": "",
                "size_match": "",
            }
        )

    manifest_path = release_manifest_path(args.metadata_dir, args.start_year)
    fieldnames = [
        "asset_group",
        "asset_type",
        "snapshot_year",
        "snapshot_month",
        "source_period",
        "year_basis",
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
    print(f"[download] Downloaded assets: {download_count}", flush=True)
    print(f"[download] Skipped assets: {skip_count}", flush=True)
    print(f"[download] Manifest rows: {len(manifest_rows)}", flush=True)
    print_elapsed(start, "Step 02")


if __name__ == "__main__":
    main()
