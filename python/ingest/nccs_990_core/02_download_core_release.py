"""
Step 02: Download the discovered NCCS Core release locally and write a raw manifest.
"""

from __future__ import annotations

import argparse
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

from tqdm import tqdm

from common import (
    BRIDGE_BMF_DIR,
    BRIDGE_PREFIX,
    CORE_RAW_DIR,
    DEFAULT_S3_BUCKET,
    DOWNLOAD_WORKERS,
    GEOID_REFERENCE_CSV,
    LATEST_RELEASE_JSON,
    META_DIR,
    META_PREFIX,
    RAW_PREFIX,
    asset_s3_key,
    banner,
    cache_source_size,
    download_with_progress,
    ensure_work_dirs,
    load_env_from_secrets,
    local_asset_path,
    print_transfer_settings,
    print_elapsed,
    release_manifest_path,
    resolve_release_and_write_metadata,
    selected_assets,
    write_csv,
    write_json,
)


def main() -> None:
    parser = argparse.ArgumentParser(description="Download the NCCS Core release locally.")
    parser.add_argument("--year", default="latest_common", help="Core release year or 'latest_common' (default: latest_common)")
    parser.add_argument("--bucket", default=DEFAULT_S3_BUCKET, help="Target S3 bucket recorded in the manifest")
    parser.add_argument("--core-raw-dir", type=Path, default=CORE_RAW_DIR, help="Local Core raw root directory")
    parser.add_argument("--bridge-dir", type=Path, default=BRIDGE_BMF_DIR, help="Local Unified BMF bridge directory")
    parser.add_argument("--metadata-dir", type=Path, default=META_DIR, help="Local metadata directory")
    parser.add_argument("--raw-prefix", default=RAW_PREFIX, help="S3 Core raw prefix recorded in the manifest")
    parser.add_argument("--bridge-prefix", default=BRIDGE_PREFIX, help="S3 Unified BMF bridge prefix recorded in the manifest")
    parser.add_argument("--meta-prefix", default=META_PREFIX, help="S3 metadata prefix recorded in the manifest")
    parser.add_argument(
        "--source-types",
        default="all",
        help="Optional comma-separated Core CSV asset types to include. Dictionaries and bridge files are always included.",
    )
    parser.add_argument("--benchmark-states", default=None, help="Optional comma-separated benchmark state override")
    parser.add_argument("--geoid-reference", type=Path, default=GEOID_REFERENCE_CSV, help="GEOID reference used for state derivation")
    parser.add_argument("--overwrite", action="store_true", help="Re-download even when a matching local file already exists")
    args = parser.parse_args()

    start = time.perf_counter()
    banner("STEP 02 - DOWNLOAD NCCS CORE RELEASE")
    load_env_from_secrets()
    ensure_work_dirs(
        core_raw_dir=args.core_raw_dir,
        bridge_dir=args.bridge_dir,
        metadata_dir=args.metadata_dir,
    )
    print_transfer_settings(label="download")

    print(f"[download] Requested year: {args.year}", flush=True)
    print(f"[download] Core raw root: {args.core_raw_dir}", flush=True)
    print(f"[download] Bridge root: {args.bridge_dir}", flush=True)
    print(f"[download] Metadata directory: {args.metadata_dir}", flush=True)
    print(f"[download] Source types: {args.source_types}", flush=True)
    print(f"[download] Overwrite: {args.overwrite}", flush=True)

    release = resolve_release_and_write_metadata(
        args.year,
        args.metadata_dir,
        geoid_reference_path=args.geoid_reference,
        benchmark_states_arg=args.benchmark_states,
    )
    tax_year = int(release["tax_year"])
    print(f"[download] Resolved tax year: {tax_year}", flush=True)

    manifest_rows: list[dict[str, object]] = []
    download_count = 0
    skip_count = 0
    pending_downloads: list[tuple[dict[str, object], Path, int | None, dict[str, object]]] = []
    for asset in selected_assets(release, args.source_types):
        source_url = str(asset["source_url"])
        filename = str(asset["filename"])
        expected_bytes = asset.get("source_content_length_bytes")
        local_path = local_asset_path(args.core_raw_dir, args.bridge_dir, args.metadata_dir, asset)
        s3_key = asset_s3_key(args.raw_prefix, args.bridge_prefix, args.meta_prefix, asset)

        print(f"[download] Asset: {asset['asset_type']} -> {filename}", flush=True)
        print(f"[download] Group: {asset['asset_group']} | Family: {asset['family']} | Scope: {asset['scope']}", flush=True)
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
                    asset,
                    local_path,
                    expected_bytes,
                    {
                        "asset_group": asset["asset_group"],
                        "asset_type": asset["asset_type"],
                        "family": asset["family"],
                        "scope": asset["scope"],
                        "year": "" if asset["year"] is None else asset["year"],
                        "year_basis": asset["year_basis"],
                        "benchmark_state": asset.get("benchmark_state") or "",
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
            manifest_rows.append(
                {
                    "asset_group": asset["asset_group"],
                    "asset_type": asset["asset_type"],
                    "family": asset["family"],
                    "scope": asset["scope"],
                    "year": "" if asset["year"] is None else asset["year"],
                    "year_basis": asset["year_basis"],
                    "benchmark_state": asset.get("benchmark_state") or "",
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
            skip_count += 1

    if pending_downloads:
        worker_count = min(DOWNLOAD_WORKERS, len(pending_downloads))
        print(f"[download] Parallel download workers: {worker_count}", flush=True)

        def _download_one(task: tuple[dict[str, object], Path, int | None, dict[str, object]]) -> tuple[dict[str, object], str, str | None, int]:
            asset, local_path, expected_bytes, base_row = task
            file_start = time.perf_counter()
            local_bytes = download_with_progress(str(asset["source_url"]), local_path, expected_bytes=expected_bytes)
            resolved_source_bytes = local_bytes if expected_bytes is None else int(expected_bytes)
            if local_bytes != resolved_source_bytes:
                raise RuntimeError(
                    f"Downloaded size mismatch for {local_path.name}: source={resolved_source_bytes}, local={local_bytes}"
                )
            row = dict(base_row)
            row["source_content_length_bytes"] = resolved_source_bytes
            row["local_bytes"] = local_bytes
            print(f"[download] Wrote {local_path} ({local_bytes} bytes)", flush=True)
            print_elapsed(file_start, f"download {local_path.name}")
            return row, str(asset["source_url"]), asset.get("source_last_modified") or None, resolved_source_bytes

        with ThreadPoolExecutor(max_workers=worker_count) as executor:
            future_to_task = {executor.submit(_download_one, task): task for task in pending_downloads}
            for future in tqdm(as_completed(future_to_task), total=len(pending_downloads), desc="download core assets", unit="file"):
                row, source_url, source_last_modified, resolved_source_bytes = future.result()
                release = cache_source_size(
                    release,
                    source_url=source_url,
                    source_last_modified=source_last_modified,
                    source_content_length_bytes=int(resolved_source_bytes),
                )
                manifest_rows.append(row)
                download_count += 1

    manifest_rows = sorted(
        manifest_rows,
        key=lambda row: (str(row["asset_group"]), str(row["family"]), str(row["scope"]), str(row["year"]), str(row["filename"])),
    )
    manifest_path = release_manifest_path(args.metadata_dir, tax_year)
    fieldnames = [
        "asset_group",
        "asset_type",
        "family",
        "scope",
        "year",
        "year_basis",
        "benchmark_state",
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
