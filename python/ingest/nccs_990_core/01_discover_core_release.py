"""
Step 01: Discover the latest NCCS Core release and persist metadata locally.
"""

from __future__ import annotations

import argparse
import time
from pathlib import Path

from common import (
    GEOID_REFERENCE_CSV,
    META_DIR,
    banner,
    ensure_work_dirs,
    load_env_from_secrets,
    print_elapsed,
    resolve_release_and_write_metadata,
)


def main() -> None:
    parser = argparse.ArgumentParser(description="Discover the latest NCCS Core release metadata.")
    parser.add_argument("--year", default="latest_common", help="Core release year or 'latest_common' (default: latest_common)")
    parser.add_argument("--metadata-dir", type=Path, default=META_DIR, help="Local metadata directory")
    parser.add_argument("--geoid-reference", type=Path, default=GEOID_REFERENCE_CSV, help="Benchmark GEOID reference CSV")
    parser.add_argument(
        "--benchmark-states",
        default=None,
        help="Optional comma-separated benchmark state override (example: SD,MN,MT,AZ)",
    )
    args = parser.parse_args()

    start = time.perf_counter()
    banner("STEP 01 - DISCOVER NCCS CORE RELEASE")
    load_env_from_secrets()
    ensure_work_dirs(metadata_dir=args.metadata_dir)

    print(f"[discover] Requested year: {args.year}", flush=True)
    print(f"[discover] Metadata directory: {args.metadata_dir}", flush=True)
    print(f"[discover] GEOID reference: {args.geoid_reference}", flush=True)
    print(f"[discover] Benchmark states override: {args.benchmark_states or '(derived from GEOID reference)'}", flush=True)

    release = resolve_release_and_write_metadata(
        args.year,
        args.metadata_dir,
        geoid_reference_path=args.geoid_reference,
        benchmark_states_arg=args.benchmark_states,
    )

    print(f"[discover] Core catalog URL: {release['core_catalog_url']}", flush=True)
    print(f"[discover] Unified BMF catalog URL: {release['bmf_catalog_url']}", flush=True)
    print(f"[discover] Latest common year: {release['latest_common_year']}", flush=True)
    print(f"[discover] Resolved tax year: {release['tax_year']}", flush=True)
    print(f"[discover] Benchmark states: {', '.join(release['benchmark_states'])}", flush=True)
    print(f"[discover] Assets found: {len(release['assets'])}", flush=True)

    for asset in release["assets"]:
        year_display = asset["year"] if asset["year"] is not None else "-"
        state_display = asset["benchmark_state"] or "-"
        print(
            f"[discover] {asset['asset_type']}: {asset['filename']} | "
            f"group={asset['asset_group']} | year={year_display} | state={state_display} | "
            f"bytes={asset['source_content_length_bytes']} | url={asset['source_url']}",
            flush=True,
        )

    print_elapsed(start, "Step 01")


if __name__ == "__main__":
    main()
