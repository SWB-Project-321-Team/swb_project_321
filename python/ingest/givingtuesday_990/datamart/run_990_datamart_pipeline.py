"""
Orchestrator: run the full DataMart pipeline in order.

Default order:
01 fetch catalog
02 export dictionary
03 download raw datamarts
04 upload bronze raw + dictionary
05 verify source/local/s3 sizes
06 build basic allforms pre-Silver
07 build basic+combined pre-Silver
08 filter benchmark outputs local
09 upload bronze pre-Silver outputs
10 upload silver filtered outputs
11 verify curated outputs local/s3
13 extract GT analysis variables
14 upload GT analysis outputs
"""

from __future__ import annotations

import argparse
import csv
import json
import subprocess
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path


STEP_FILES = {
    1: "01_fetch_datamart_catalog.py",
    2: "02_export_data_dictionary.py",
    3: "03_download_raw_datamarts.py",
    4: "04_upload_bronze_raw_and_dictionary.py",
    5: "05_verify_source_local_s3_sizes.py",
    6: "06_build_basic_allforms_presilver.py",
    7: "07_build_basic_plus_combined_presilver.py",
    8: "08_filter_benchmark_outputs_local.py",
    9: "09_upload_bronze_presilver_outputs.py",
    10: "10_upload_silver_filtered_outputs.py",
    11: "11_verify_curated_outputs_local_s3.py",
    13: "13_extract_basic_analysis_variables_local.py",
    14: "14_upload_analysis_outputs.py",
}


def _step_path(step_num: int) -> Path:
    return Path(__file__).resolve().parent / STEP_FILES[step_num]


def _step_command(step_num: int, common_args: dict[str, str], extra_flags: dict[str, bool]) -> list[str]:
    """Build the subprocess command for one GT step."""
    script = _step_path(step_num)
    cmd = [sys.executable, str(script)]

    # Propagate shared args to scripts that support them.
    if step_num in {4, 5, 9, 10, 11, 14}:
        cmd += ["--bucket", common_args["bucket"], "--region", common_args["region"]]
    if step_num in {7, 8}:
        cmd += ["--tax-year-min", common_args["tax_year_min"]]
        cmd += ["--tax-year-max", common_args["tax_year_max"]]
    if step_num in {8, 10, 11}:
        cmd += ["--emit", common_args["emit"]]
    if step_num == 3 and extra_flags.get("overwrite_download", False):
        cmd += ["--overwrite"]
    if step_num == 2 and extra_flags.get("refresh_catalog", False):
        cmd += ["--refresh-catalog"]
    if step_num in {3, 4, 5, 6, 7, 8} and extra_flags.get("force_rebuild", False):
        cmd += ["--force-rebuild"]
    if step_num in {9, 10, 14} and extra_flags.get("overwrite_uploads", False):
        cmd += ["--overwrite"]
    return cmd


def _run_step(step_num: int, common_args: dict[str, str], extra_flags: dict[str, bool], *, mode: str = "serial") -> dict[str, object]:
    """Run one step script and return timing metadata for the orchestrator summary."""
    script = _step_path(step_num)
    cmd = _step_command(step_num, common_args, extra_flags)
    print("\n" + "-" * 88, flush=True)
    print(f"[orchestrator] STEP {step_num:02d} ({mode}): {script.name}", flush=True)
    print(f"[orchestrator] Command: {' '.join(cmd)}", flush=True)
    print("-" * 88, flush=True)
    step_start = time.perf_counter()
    subprocess.run(cmd, check=True)
    elapsed = time.perf_counter() - step_start
    print(f"[orchestrator] STEP {step_num:02d} completed in {elapsed:.1f}s", flush=True)
    return {
        "step_num": step_num,
        "script_name": script.name,
        "mode": mode,
        "elapsed_seconds": round(elapsed, 3),
    }


def _run_parallel_steps(step_nums: list[int], common_args: dict[str, str], extra_flags: dict[str, bool]) -> list[dict[str, object]]:
    """
    Run independent GT late-stage publish steps in parallel.

    Steps 09 and 10 only depend on the local build/filter outputs, so after
    step 08 completes they can safely publish Bronze and Silver artifacts in
    parallel before step 11 verifies the resulting curated outputs.
    """
    if not step_nums:
        return []
    print("\n" + "=" * 88, flush=True)
    print(f"[orchestrator] PARALLEL GROUP: {', '.join(f'{step:02d}' for step in step_nums)}", flush=True)
    print("=" * 88, flush=True)
    results: list[dict[str, object]] = []
    with ThreadPoolExecutor(max_workers=len(step_nums)) as executor:
        future_to_step = {
            executor.submit(_run_step, step_num, common_args, extra_flags, mode="parallel"): step_num
            for step_num in step_nums
        }
        for future in as_completed(future_to_step):
            results.append(future.result())
    return sorted(results, key=lambda row: int(row["step_num"]))


def main() -> None:
    parser = argparse.ArgumentParser(description="Run the SWB 321 GivingTuesday DataMart pipeline.")
    parser.add_argument("--start-step", type=int, default=1, help="Start step number (default: 1)")
    parser.add_argument("--end-step", type=int, default=14, help="End step number (default: 14)")
    parser.add_argument("--bucket", default="swb-321-irs990-teos", help="S3 bucket for Bronze/Silver steps")
    parser.add_argument("--region", default="us-east-2", help="S3 region for Bronze/Silver steps")
    parser.add_argument("--tax-year-min", type=int, default=2022, help="Minimum tax year for GT filtered outputs")
    parser.add_argument("--tax-year-max", type=int, default=2024, help="Maximum tax year for GT filtered outputs")
    parser.add_argument("--emit", choices=["basic", "mixed", "both"], default="both", help="Which filtered outputs to build/upload/verify")
    parser.add_argument("--overwrite-download", action="store_true", help="Force re-download raw files in step 03")
    parser.add_argument("--overwrite-uploads", action="store_true", help="Force re-upload Bronze/Silver curated artifacts even when bytes already match")
    parser.add_argument("--force-rebuild", action="store_true", help="Force rebuild for GT build/filter steps even when cached manifests match")
    parser.add_argument("--refresh-catalog", action="store_true", help="Force catalog refresh in step 02")
    parser.add_argument(
        "--parallel-late-stage",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="Run steps 09 and 10 in parallel when both are included in the requested step range",
    )
    args = parser.parse_args()

    if args.start_step < 1 or args.end_step > 14 or args.start_step > args.end_step:
        raise SystemExit("Invalid step range. Use 1 <= start-step <= end-step <= 14.")

    t0 = time.perf_counter()
    common_args = {
        "bucket": args.bucket,
        "region": args.region,
        "tax_year_min": str(args.tax_year_min),
        "tax_year_max": str(args.tax_year_max),
        "emit": args.emit,
    }
    flags = {
        "overwrite_download": args.overwrite_download,
        "overwrite_uploads": args.overwrite_uploads,
        "force_rebuild": args.force_rebuild,
        "refresh_catalog": args.refresh_catalog,
    }

    print("=" * 88, flush=True)
    print("RUN 990 DATAMART PIPELINE", flush=True)
    print(f"Step range: {args.start_step}..{args.end_step}", flush=True)
    print(f"S3 bucket/region: {args.bucket} / {args.region}", flush=True)
    print(f"Tax year min: {args.tax_year_min}", flush=True)
    print(f"Tax year max: {args.tax_year_max}", flush=True)
    print(f"Emit mode: {args.emit}", flush=True)
    print(f"Parallel late stage: {args.parallel_late_stage}", flush=True)
    print("=" * 88, flush=True)

    step_results: list[dict[str, object]] = []
    # The canonical GT pipeline step numbering intentionally skips step 12
    # because that script is a local audit utility rather than a standard
    # production pipeline step. Build the requested step list from the
    # registered canonical step ids instead of assuming every integer exists.
    requested_steps = [
        step_num
        for step_num in sorted(STEP_FILES)
        if args.start_step <= step_num <= args.end_step
    ]
    if not requested_steps:
        raise SystemExit("The requested step range does not include any canonical GT pipeline steps.")
    serial_prefix = [step for step in requested_steps if step < 9]
    serial_suffix = [step for step in requested_steps if step > 10]
    for step in serial_prefix:
        step_results.append(_run_step(step, common_args, flags))
    if args.parallel_late_stage and 9 in requested_steps and 10 in requested_steps:
        step_results.extend(_run_parallel_steps([9, 10], common_args, flags))
    else:
        for step in [9, 10]:
            if step in requested_steps:
                step_results.append(_run_step(step, common_args, flags))
    for step in serial_suffix:
        step_results.append(_run_step(step, common_args, flags))

    elapsed = time.perf_counter() - t0
    summary_payload = {
        "created_at_utc": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "step_range": [args.start_step, args.end_step],
        "bucket": args.bucket,
        "region": args.region,
        "tax_year_min": args.tax_year_min,
        "tax_year_max": args.tax_year_max,
        "emit": args.emit,
        "overwrite_download": bool(args.overwrite_download),
        "overwrite_uploads": bool(args.overwrite_uploads),
        "force_rebuild": bool(args.force_rebuild),
        "refresh_catalog": bool(args.refresh_catalog),
        "parallel_late_stage": bool(args.parallel_late_stage),
        "elapsed_seconds": round(elapsed, 3),
        "steps": sorted(step_results, key=lambda row: int(row["step_num"])),
    }
    try:
        from common import (  # type: ignore
            BASIC_ALLFORMS_BUILD_MANIFEST_JSON,
            BASIC_PLUS_COMBINED_BUILD_MANIFEST_JSON,
            FILTERED_BASIC_MANIFEST_JSON,
            FILTERED_MIXED_MANIFEST_JSON,
            ORCHESTRATOR_SUMMARY_JSON,
            ORCHESTRATOR_STEP_TIMINGS_CSV,
            read_json,
        )

        manifest_paths = [
            BASIC_ALLFORMS_BUILD_MANIFEST_JSON,
            BASIC_PLUS_COMBINED_BUILD_MANIFEST_JSON,
            FILTERED_BASIC_MANIFEST_JSON,
            FILTERED_MIXED_MANIFEST_JSON,
        ]
        manifest_summaries = {}
        for path in manifest_paths:
            if path.exists():
                payload = read_json(path)
                manifest_summaries[path.name] = {
                    "rows_output": payload.get("rows_output", ""),
                    "columns_output": payload.get("columns_output", ""),
                    "output_path": payload.get("output_path", ""),
                }
        summary_payload["manifests"] = manifest_summaries
        ORCHESTRATOR_STEP_TIMINGS_CSV.parent.mkdir(parents=True, exist_ok=True)
        with open(ORCHESTRATOR_STEP_TIMINGS_CSV, "w", newline="", encoding="utf-8") as handle:
            writer = csv.DictWriter(handle, fieldnames=["step_num", "script_name", "mode", "elapsed_seconds"])
            writer.writeheader()
            for row in sorted(step_results, key=lambda value: int(value["step_num"])):
                writer.writerow(row)
        summary_payload["step_timings_csv"] = str(ORCHESTRATOR_STEP_TIMINGS_CSV)
        ORCHESTRATOR_SUMMARY_JSON.parent.mkdir(parents=True, exist_ok=True)
        ORCHESTRATOR_SUMMARY_JSON.write_text(json.dumps(summary_payload, indent=2, sort_keys=True), encoding="utf-8")
        print(f"[orchestrator] Wrote summary: {ORCHESTRATOR_SUMMARY_JSON}", flush=True)
        print(f"[orchestrator] Wrote step timings CSV: {ORCHESTRATOR_STEP_TIMINGS_CSV}", flush=True)
        if manifest_summaries:
            for name, payload in manifest_summaries.items():
                print(
                    f"[orchestrator] Summary {name}: rows={payload.get('rows_output', '')} cols={payload.get('columns_output', '')}",
                    flush=True,
                )
        for row in sorted(step_results, key=lambda value: int(value["step_num"])):
            print(
                f"[orchestrator] Step {int(row['step_num']):02d} [{row['mode']}] elapsed={float(row['elapsed_seconds']):.3f}s",
                flush=True,
            )
    except Exception as exc:
        print(f"[orchestrator] Warning: unable to write run summary ({type(exc).__name__})", flush=True)
    print("\n[orchestrator] Pipeline completed successfully.", flush=True)
    print(f"[orchestrator] Total elapsed: {elapsed:.1f}s ({elapsed / 60:.1f} min)", flush=True)


if __name__ == "__main__":
    main()
