"""
Step 01: Discover the latest IRS SOI county release and persist metadata locally.

This step:
- resolves --year latest or an explicit year
- fetches the IRS landing page and year page
- extracts the 3 required county assets
- writes latest_release.json in the metadata directory
"""

from __future__ import annotations

import argparse
import time
from pathlib import Path

from common import META_DIR, banner, ensure_work_dirs, load_env_from_secrets, print_elapsed, resolve_release_and_write_metadata


def main() -> None:
    parser = argparse.ArgumentParser(description="Discover the latest IRS SOI county release metadata.")
    parser.add_argument("--year", default="latest", help="County release year or 'latest' (default: latest)")
    parser.add_argument("--metadata-dir", type=Path, default=META_DIR, help="Local metadata directory")
    args = parser.parse_args()

    start = time.perf_counter()
    banner("STEP 01 - DISCOVER IRS SOI COUNTY RELEASE")
    load_env_from_secrets()
    ensure_work_dirs(metadata_dir=args.metadata_dir)

    print(f"[discover] Requested year: {args.year}", flush=True)
    print(f"[discover] Metadata directory: {args.metadata_dir}", flush=True)
    release = resolve_release_and_write_metadata(args.year, args.metadata_dir)

    print(f"[discover] Landing page: {release['landing_page_url']}", flush=True)
    print(f"[discover] Year page: {release['year_page_url']}", flush=True)
    print(f"[discover] Resolved tax year: {release['tax_year']}", flush=True)
    print(f"[discover] IRS page last reviewed: {release['irs_page_last_reviewed']}", flush=True)
    print(f"[discover] Assets found: {len(release['assets'])}", flush=True)
    for asset in release["assets"]:
        print(
            f"[discover] {asset['asset_type']}: {asset['filename']} | "
            f"bytes={asset['source_content_length_bytes']} | url={asset['source_url']}",
            flush=True,
        )

    print_elapsed(start, "Step 01")


if __name__ == "__main__":
    main()
