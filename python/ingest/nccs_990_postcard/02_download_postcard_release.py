"""
Step 02: Download the discovered NCCS e-Postcard snapshots locally and write a raw manifest.
"""

from __future__ import annotations

import argparse
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

from tqdm import tqdm

from common import (
    DOWNLOAD_WORKERS,
    DEFAULT_S3_BUCKET,
    LATEST_RELEASE_JSON,
    META_DIR,
    META_PREFIX,
    POSTCARD_RAW_DIR,
    RAW_PREFIX,
    TQDM_KW,
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
    parser = argparse.ArgumentParser(description="Download the NCCS e-Postcard release locally.")
    parser.add_argument("--snapshot-year", default="latest", help="Snapshot year or 'latest' (default: latest)")
    parser.add_argument(
        "--snapshot-months",
        default="all",
        help="Snapshot months to include: 'all' or comma-separated MM / YYYY-MM values",
    )
    parser.add_argument("--bucket", default=DEFAULT_S3_BUCKET, help="Target S3 bucket recorded in the manifest")
    parser.add_argument("--postcard-raw-dir", type=Path, default=POSTCARD_RAW_DIR, help="Local postcard raw root directory")
    parser.add_argument("--metadata-dir", type=Path, default=META_DIR, help="Local metadata directory")
    parser.add_argument("--raw-prefix", default=RAW_PREFIX, help="S3 raw prefix recorded in the manifest")
    parser.add_argument("--meta-prefix", default=META_PREFIX, help="S3 metadata prefix recorded in the manifest")
    parser.add_argument("--overwrite", action="store_true", help="Re-download even when a matching local file already exists")
    args = parser.parse_args()

    start = time.perf_counter()
    banner("STEP 02 - DOWNLOAD NCCS E-POSTCARD RELEASE")
    load_env_from_secrets()
    ensure_work_dirs(postcard_raw_dir=args.postcard_raw_dir, metadata_dir=args.metadata_dir)

    print(f"[download] Requested snapshot year: {args.snapshot_year}", flush=True)
    print(f"[download] Requested snapshot months: {args.snapshot_months}", flush=True)
    print(f"[download] Postcard raw root: {args.postcard_raw_dir}", flush=True)
    print(f"[download] Metadata directory: {args.metadata_dir}", flush=True)
    print(f"[download] Overwrite: {args.overwrite}", flush=True)

    release = resolve_release_and_write_metadata(
        args.snapshot_year,
        args.metadata_dir,
        snapshot_months_arg=args.snapshot_months,
    )
    snapshot_year = int(release["snapshot_year"])
    print(f"[download] Resolved snapshot year: {snapshot_year}", flush=True)

    manifest_rows_by_month: dict[str, dict[str, object]] = {}
    download_count = 0
    skip_count = 0
    pending_downloads: list[tuple[int, dict[str, object], Path, int | None, dict[str, object]]] = []
    for task_index, asset in enumerate(sorted(selected_assets(release), key=lambda item: str(item["snapshot_month"])), start=1):
        source_url = str(asset["source_url"])
        filename = str(asset["filename"])
        expected_bytes = asset.get("source_content_length_bytes")
        local_path = local_asset_path(args.postcard_raw_dir, args.metadata_dir, asset)
        s3_key = asset_s3_key(args.raw_prefix, args.meta_prefix, asset)

        print(f"[download] Asset: {filename}", flush=True)
        print(f"[download] Snapshot month: {asset['snapshot_month']}", flush=True)
        print(f"[download] Source URL: {source_url}", flush=True)
        print(f"[download] Local target: {local_path}", flush=True)
        print(f"[download] Expected bytes: {expected_bytes}", flush=True)

        local_bytes = local_path.stat().st_size if local_path.exists() else None
        local_matches_source = expected_bytes is not None and local_bytes == expected_bytes
        should_download = args.overwrite or not local_path.exists() or (expected_bytes is not None and not local_matches_source)

        if should_download:
            if local_path.exists() and not args.overwrite:
                print(f"[download] Existing file does not match source metadata; refreshing {local_path.name}.", flush=True)
            pending_downloads.append(
                (
                    task_index,
                    asset,
                    local_path,
                    expected_bytes,
                    {
                        "asset_group": asset["asset_group"],
                        "asset_type": asset["asset_type"],
                        "snapshot_year": asset["snapshot_year"],
                        "snapshot_month": asset["snapshot_month"],
                        "source_url": source_url,
                        "filename": filename,
                        "source_content_length_bytes": expected_bytes,
                        "source_last_modified": asset.get("source_last_modified") or "",
                        "local_path": str(local_path),
                        "local_bytes": "",
                        "s3_bucket": args.bucket,
                        "s3_key": s3_key,
                        "s3_bytes": "",
                        "size_match": "",
                    },
                )
            )
        else:
            print(f"[download] Skip unchanged local file: {local_path} ({local_bytes} bytes)", flush=True)
            manifest_rows_by_month[str(asset["snapshot_month"])] = {
                "asset_group": asset["asset_group"],
                "asset_type": asset["asset_type"],
                "snapshot_year": asset["snapshot_year"],
                "snapshot_month": asset["snapshot_month"],
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
            skip_count += 1

    if pending_downloads:
        worker_count = min(DOWNLOAD_WORKERS, len(pending_downloads))
        print(f"[download] Parallel download workers: {worker_count}", flush=True)

        def _download_one(
            position: int,
            asset: dict[str, object],
            local_path: Path,
            expected_bytes: int | None,
            base_row: dict[str, object],
        ) -> tuple[dict[str, object], str, str | None, int, float]:
            file_start = time.perf_counter()
            local_bytes = download_with_progress(
                str(asset["source_url"]),
                local_path,
                expected_bytes=expected_bytes,
                position=position,
                desc=f"download {local_path.name}",
            )
            source_bytes = expected_bytes if expected_bytes is not None else local_bytes
            if source_bytes is not None and local_bytes != source_bytes:
                raise RuntimeError(
                    f"Downloaded size mismatch for {local_path.name}: source={source_bytes}, local={local_bytes}"
                )
            row = dict(base_row)
            row["source_content_length_bytes"] = source_bytes
            row["local_bytes"] = local_bytes
            return (
                row,
                str(asset["source_url"]),
                asset.get("source_last_modified") or None,
                int(source_bytes),
                time.perf_counter() - file_start,
            )

        with ThreadPoolExecutor(max_workers=worker_count) as executor:
            future_to_task = {
                executor.submit(_download_one, position, asset, local_path, expected_bytes, base_row): (asset, local_path)
                for position, asset, local_path, expected_bytes, base_row in pending_downloads
            }
            with tqdm(total=len(pending_downloads), desc="download postcard assets", unit="file", position=0, **TQDM_KW) as overall:
                for future in as_completed(future_to_task):
                    row, source_url, source_last_modified, source_bytes, elapsed = future.result()
                    release = cache_source_size(
                        release,
                        source_url=source_url,
                        source_last_modified=source_last_modified,
                        source_content_length_bytes=source_bytes,
                    )
                    manifest_rows_by_month[str(row["snapshot_month"])] = row
                    download_count += 1
                    overall.update(1)
                    print(f"[download] Wrote {row['local_path']} ({row['local_bytes']} bytes)", flush=True)
                    print(f"[time] download {row['filename']}: {elapsed:.1f}s ({elapsed / 60:.1f} min)", flush=True)

    manifest_rows = [manifest_rows_by_month[key] for key in sorted(manifest_rows_by_month)]

    manifest_path = release_manifest_path(args.metadata_dir, snapshot_year)
    fieldnames = [
        "asset_group",
        "asset_type",
        "snapshot_year",
        "snapshot_month",
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
