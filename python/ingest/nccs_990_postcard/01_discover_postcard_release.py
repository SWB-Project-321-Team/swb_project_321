"""
Step 01: Discover the latest NCCS e-Postcard release and persist metadata locally.
"""

from __future__ import annotations

import argparse
import time
from pathlib import Path

from common import (
    META_DIR,
    banner,
    ensure_work_dirs,
    load_env_from_secrets,
    print_elapsed,
    resolve_release_and_write_metadata,
)


def main() -> None:
    parser = argparse.ArgumentParser(description="Discover the latest NCCS e-Postcard release metadata.")
    parser.add_argument(
        "--snapshot-year",
        default="latest",
        help="Snapshot year or 'latest' (default: latest)",
    )
    parser.add_argument(
        "--snapshot-months",
        default="all",
        help="Snapshot months to include: 'all' or comma-separated MM / YYYY-MM values",
    )
    parser.add_argument("--metadata-dir", type=Path, default=META_DIR, help="Local metadata directory")
    args = parser.parse_args()

    start = time.perf_counter()
    banner("STEP 01 - DISCOVER NCCS E-POSTCARD RELEASE")
    load_env_from_secrets()
    ensure_work_dirs(metadata_dir=args.metadata_dir)

    print(f"[discover] Requested snapshot year: {args.snapshot_year}", flush=True)
    print(f"[discover] Requested snapshot months: {args.snapshot_months}", flush=True)
    print(f"[discover] Metadata directory: {args.metadata_dir}", flush=True)

    release = resolve_release_and_write_metadata(
        args.snapshot_year,
        args.metadata_dir,
        snapshot_months_arg=args.snapshot_months,
    )

    print(f"[discover] Dataset page URL: {release['postcard_dataset_url']}", flush=True)
    print(f"[discover] Raw base URL: {release['raw_base_url']}", flush=True)
    print(f"[discover] Page-linked download URL: {release['linked_download_url']}", flush=True)
    print(f"[discover] Page-linked snapshot month: {release['linked_snapshot_month']}", flush=True)
    print(f"[discover] Resolved snapshot year: {release['snapshot_year']}", flush=True)
    print(f"[discover] Latest available snapshot month: {release['latest_snapshot_month']}", flush=True)
    print(f"[discover] Available months: {', '.join(release['available_snapshot_months'])}", flush=True)
    print(f"[discover] Assets found: {len(release['assets'])}", flush=True)

    for asset in release["assets"]:
        print(
            f"[discover] {asset['filename']} | snapshot_month={asset['snapshot_month']} | "
            f"bytes={asset['source_content_length_bytes']} | url={asset['source_url']}",
            flush=True,
        )

    print_elapsed(start, "Step 01")


if __name__ == "__main__":
    main()
