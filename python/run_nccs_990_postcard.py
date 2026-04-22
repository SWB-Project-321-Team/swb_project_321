"""
Run the NCCS e-Postcard pipeline in order: 01 -> 08.

Run from repo root:
  python python/run_nccs_990_postcard.py
"""

from __future__ import annotations

import argparse
import subprocess
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
PYTHON = REPO_ROOT / "python"
INGEST = PYTHON / "ingest" / "nccs_990_postcard"

STEP_01 = INGEST / "01_discover_postcard_release.py"
STEP_02 = INGEST / "02_download_postcard_release.py"
STEP_03 = INGEST / "03_upload_postcard_release_to_s3.py"
STEP_04 = INGEST / "04_verify_postcard_source_local_s3.py"
STEP_05 = INGEST / "05_filter_postcard_to_benchmark_local.py"
STEP_06 = INGEST / "06_upload_filtered_postcard_to_s3.py"
STEP_07 = INGEST / "07_extract_analysis_variables_local.py"
STEP_08 = INGEST / "08_upload_analysis_outputs.py"


def _append_arg(cmd: list[str], flag: str, value: object | None) -> None:
    if value is None:
        return
    cmd.extend([flag, str(value)])


def main() -> None:
    parser = argparse.ArgumentParser(description="Run the NCCS e-Postcard pipeline (01 -> 08).")
    parser.add_argument("--snapshot-year", default=None)
    parser.add_argument("--snapshot-months", default=None)
    parser.add_argument("--bucket", default=None)
    parser.add_argument("--region", default=None)
    parser.add_argument("--postcard-raw-dir", default=None)
    parser.add_argument("--metadata-dir", default=None)
    parser.add_argument("--staging-dir", default=None)
    parser.add_argument("--raw-prefix", default=None)
    parser.add_argument("--meta-prefix", default=None)
    parser.add_argument("--silver-prefix", default=None)
    parser.add_argument("--analysis-prefix", default=None)
    parser.add_argument("--analysis-meta-prefix", default=None)
    parser.add_argument("--geoid-reference", default=None)
    parser.add_argument("--zip-to-county", default=None)
    parser.add_argument("--chunk-size", default=None)
    parser.add_argument("--tax-year-start", default=None)
    parser.add_argument("--overwrite", action="store_true")
    args = parser.parse_args()

    steps: list[tuple[str, Path, list[str]]] = []

    step_01_args: list[str] = []
    for flag, value in (
        ("--snapshot-year", args.snapshot_year),
        ("--snapshot-months", args.snapshot_months),
        ("--metadata-dir", args.metadata_dir),
    ):
        _append_arg(step_01_args, flag, value)
    steps.append(("01_discover_postcard_release", STEP_01, step_01_args))

    step_02_args: list[str] = []
    for flag, value in (
        ("--snapshot-year", args.snapshot_year),
        ("--snapshot-months", args.snapshot_months),
        ("--bucket", args.bucket),
        ("--postcard-raw-dir", args.postcard_raw_dir),
        ("--metadata-dir", args.metadata_dir),
        ("--raw-prefix", args.raw_prefix),
        ("--meta-prefix", args.meta_prefix),
    ):
        _append_arg(step_02_args, flag, value)
    if args.overwrite:
        step_02_args.append("--overwrite")
    steps.append(("02_download_postcard_release", STEP_02, step_02_args))

    step_03_args: list[str] = []
    for flag, value in (
        ("--snapshot-year", args.snapshot_year),
        ("--snapshot-months", args.snapshot_months),
        ("--bucket", args.bucket),
        ("--region", args.region),
        ("--postcard-raw-dir", args.postcard_raw_dir),
        ("--metadata-dir", args.metadata_dir),
        ("--raw-prefix", args.raw_prefix),
        ("--meta-prefix", args.meta_prefix),
    ):
        _append_arg(step_03_args, flag, value)
    if args.overwrite:
        step_03_args.append("--overwrite")
    steps.append(("03_upload_postcard_release_to_s3", STEP_03, step_03_args))

    step_04_args: list[str] = []
    for flag, value in (
        ("--snapshot-year", args.snapshot_year),
        ("--snapshot-months", args.snapshot_months),
        ("--bucket", args.bucket),
        ("--region", args.region),
        ("--postcard-raw-dir", args.postcard_raw_dir),
        ("--metadata-dir", args.metadata_dir),
        ("--raw-prefix", args.raw_prefix),
        ("--meta-prefix", args.meta_prefix),
    ):
        _append_arg(step_04_args, flag, value)
    steps.append(("04_verify_postcard_source_local_s3", STEP_04, step_04_args))

    step_05_args: list[str] = []
    for flag, value in (
        ("--snapshot-year", args.snapshot_year),
        ("--snapshot-months", args.snapshot_months),
        ("--postcard-raw-dir", args.postcard_raw_dir),
        ("--metadata-dir", args.metadata_dir),
        ("--staging-dir", args.staging_dir),
        ("--silver-prefix", args.silver_prefix),
        ("--geoid-reference", args.geoid_reference),
        ("--zip-to-county", args.zip_to_county),
        ("--chunk-size", args.chunk_size),
        ("--tax-year-start", args.tax_year_start),
    ):
        _append_arg(step_05_args, flag, value)
    steps.append(("05_filter_postcard_to_benchmark_local", STEP_05, step_05_args))

    step_06_args: list[str] = []
    for flag, value in (
        ("--snapshot-year", args.snapshot_year),
        ("--snapshot-months", args.snapshot_months),
        ("--bucket", args.bucket),
        ("--region", args.region),
        ("--metadata-dir", args.metadata_dir),
        ("--staging-dir", args.staging_dir),
        ("--silver-prefix", args.silver_prefix),
        ("--tax-year-start", args.tax_year_start),
    ):
        _append_arg(step_06_args, flag, value)
    if args.overwrite:
        step_06_args.append("--overwrite")
    steps.append(("06_upload_filtered_postcard_to_s3", STEP_06, step_06_args))

    step_07_args: list[str] = []
    for flag, value in (
        ("--metadata-dir", args.metadata_dir),
        ("--staging-dir", args.staging_dir),
        ("--bucket", args.bucket),
        ("--region", args.region),
        ("--analysis-prefix", args.analysis_prefix),
        ("--analysis-meta-prefix", args.analysis_meta_prefix),
    ):
        _append_arg(step_07_args, flag, value)
    steps.append(("07_extract_analysis_variables_local", STEP_07, step_07_args))

    step_08_args: list[str] = []
    for flag, value in (
        ("--bucket", args.bucket),
        ("--region", args.region),
        ("--metadata-dir", args.metadata_dir),
        ("--staging-dir", args.staging_dir),
        ("--analysis-prefix", args.analysis_prefix),
        ("--analysis-meta-prefix", args.analysis_meta_prefix),
    ):
        _append_arg(step_08_args, flag, value)
    if args.overwrite:
        step_08_args.append("--overwrite")
    steps.append(("08_upload_analysis_outputs", STEP_08, step_08_args))

    for step_name, script_path, script_args in steps:
        if not script_path.exists():
            print(f"Script not found: {script_path}", file=sys.stderr)
            sys.exit(1)
        cmd = [sys.executable, str(script_path), *script_args]
        print(f"Running {step_name}...", flush=True)
        rc = subprocess.call(cmd, cwd=str(REPO_ROOT))
        if rc != 0:
            print(f"Step {step_name} failed with exit code {rc}", file=sys.stderr)
            sys.exit(rc)

    print("NCCS e-Postcard pipeline (01 -> 08) completed.", flush=True)


if __name__ == "__main__":
    main()
