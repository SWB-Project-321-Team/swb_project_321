"""
Step 01: Discover the published NCCS efile annual release selection.
"""

from __future__ import annotations

import argparse
import time
from pathlib import Path

from common import (
    META_DIR,
    START_YEAR_DEFAULT,
    banner,
    ensure_work_dirs,
    load_env_from_secrets,
    print_elapsed,
    resolve_release_and_write_metadata,
)


def main() -> None:
    parser = argparse.ArgumentParser(description="Discover the published NCCS efile annual release selection.")
    parser.add_argument("--start-year", type=int, default=START_YEAR_DEFAULT, help="First tax year to include (default: 2022)")
    parser.add_argument("--metadata-dir", type=Path, default=META_DIR, help="Local metadata directory")
    args = parser.parse_args()

    start = time.perf_counter()
    banner("STEP 01 - DISCOVER NCCS EFILE RELEASE")
    load_env_from_secrets()
    ensure_work_dirs(metadata_dir=args.metadata_dir)

    print(f"[discover] Start year: {args.start_year}", flush=True)
    print(f"[discover] Metadata directory: {args.metadata_dir}", flush=True)

    release = resolve_release_and_write_metadata(args.metadata_dir, start_year=args.start_year)
    print(f"[discover] Selected tax years: {release['selected_tax_years']}", flush=True)
    print(f"[discover] Latest published tax year: {release['latest_published_year']}", flush=True)
    print(f"[discover] Partial tax years: {release['partial_tax_years']}", flush=True)
    for asset in release["selected_assets"]:
        print(
            "[discover] "
            f"tax_year={asset['tax_year']} | table={asset['table_name']} | "
            f"partial={asset['is_partial_year']} | url={asset['source_url']}",
            flush=True,
        )
    print_elapsed(start, "Step 01")


if __name__ == "__main__":
    main()
