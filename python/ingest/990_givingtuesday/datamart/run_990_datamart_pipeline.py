"""
Orchestrator: run the full DataMart pipeline in order.

Default order:
01 fetch catalog
02 export dictionary
03 download raw datamarts
04 upload bronze raw + dictionary
05 verify source/local/s3 sizes
06 build basic allforms unfiltered
07 build basic+combined unfiltered
08 upload bronze unfiltered outputs
09 filter to benchmark and write silver local
10 upload silver filtered
"""

from __future__ import annotations

import argparse
import subprocess
import sys
import time
from pathlib import Path


STEP_FILES = {
    1: "01_fetch_datamart_catalog.py",
    2: "02_export_data_dictionary.py",
    3: "03_download_raw_datamarts.py",
    4: "04_upload_bronze_raw_and_dictionary.py",
    5: "05_verify_source_local_s3_sizes.py",
    6: "06_build_basic_allforms_unfiltered.py",
    7: "07_build_basic_plus_combined_unfiltered.py",
    8: "08_upload_bronze_unfiltered_outputs.py",
    9: "09_filter_to_benchmark_and_write_silver_local.py",
    10: "10_upload_silver_filtered.py",
}


def _step_path(step_num: int) -> Path:
    return Path(__file__).resolve().parent / STEP_FILES[step_num]


def _run_step(step_num: int, common_args: dict[str, str], extra_flags: dict[str, bool]) -> None:
    """Run one step script with relevant arguments."""
    script = _step_path(step_num)
    cmd = [sys.executable, str(script)]

    # Propagate shared args to scripts that support them.
    if step_num in {4, 5, 8, 10}:
        cmd += ["--bucket", common_args["bucket"], "--region", common_args["region"]]
    if step_num == 9:
        cmd += ["--tax-year-min", common_args["tax_year_min"]]
    if step_num == 3 and extra_flags.get("overwrite_download", False):
        cmd += ["--overwrite"]
    if step_num == 2 and extra_flags.get("refresh_catalog", False):
        cmd += ["--refresh-catalog"]

    print("\n" + "-" * 88, flush=True)
    print(f"[orchestrator] STEP {step_num:02d}: {script.name}", flush=True)
    print(f"[orchestrator] Command: {' '.join(cmd)}", flush=True)
    print("-" * 88, flush=True)
    subprocess.run(cmd, check=True)


def main() -> None:
    parser = argparse.ArgumentParser(description="Run the SWB 321 GivingTuesday DataMart pipeline.")
    parser.add_argument("--start-step", type=int, default=1, help="Start step number (default: 1)")
    parser.add_argument("--end-step", type=int, default=10, help="End step number (default: 10)")
    parser.add_argument("--bucket", default="swb-321-irs990-teos", help="S3 bucket for Bronze/Silver steps")
    parser.add_argument("--region", default="us-east-2", help="S3 region for Bronze/Silver steps")
    parser.add_argument("--tax-year-min", type=int, default=2021, help="Minimum tax year for Silver filter")
    parser.add_argument("--overwrite-download", action="store_true", help="Force re-download raw files in step 03")
    parser.add_argument("--refresh-catalog", action="store_true", help="Force catalog refresh in step 02")
    args = parser.parse_args()

    if args.start_step < 1 or args.end_step > 10 or args.start_step > args.end_step:
        raise SystemExit("Invalid step range. Use 1 <= start-step <= end-step <= 10.")

    t0 = time.perf_counter()
    common_args = {
        "bucket": args.bucket,
        "region": args.region,
        "tax_year_min": str(args.tax_year_min),
    }
    flags = {
        "overwrite_download": args.overwrite_download,
        "refresh_catalog": args.refresh_catalog,
    }

    print("=" * 88, flush=True)
    print("RUN 990 DATAMART PIPELINE", flush=True)
    print(f"Step range: {args.start_step}..{args.end_step}", flush=True)
    print(f"S3 bucket/region: {args.bucket} / {args.region}", flush=True)
    print(f"Tax year min: {args.tax_year_min}", flush=True)
    print("=" * 88, flush=True)

    for step in range(args.start_step, args.end_step + 1):
        _run_step(step, common_args, flags)

    elapsed = time.perf_counter() - t0
    print("\n[orchestrator] Pipeline completed successfully.", flush=True)
    print(f"[orchestrator] Total elapsed: {elapsed:.1f}s ({elapsed / 60:.1f} min)", flush=True)


if __name__ == "__main__":
    main()

