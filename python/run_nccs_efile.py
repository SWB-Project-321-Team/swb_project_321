"""
Run the NCCS efile pipeline in order: 01 -> 09.
"""

from __future__ import annotations

import argparse
import subprocess
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
INGEST = REPO_ROOT / "python" / "ingest" / "nccs_efile"

STEP_01 = INGEST / "01_discover_efile_release.py"
STEP_02 = INGEST / "02_download_efile_release.py"
STEP_03 = INGEST / "03_upload_efile_release_to_s3.py"
STEP_04 = INGEST / "04_verify_efile_source_local_s3.py"
STEP_05 = INGEST / "05_build_efile_benchmark_local.py"
STEP_06 = INGEST / "06_upload_filtered_efile_to_s3.py"
STEP_07 = INGEST / "07_compare_efile_vs_core_local.py"
STEP_08 = INGEST / "08_extract_analysis_variables_local.py"
STEP_09 = INGEST / "09_upload_analysis_outputs.py"


def _append_arg(cmd: list[str], flag: str, value: object | None) -> None:
    if value is None:
        return
    cmd.extend([flag, str(value)])


def main() -> None:
    parser = argparse.ArgumentParser(description="Run the NCCS efile pipeline (01 -> 09).")
    parser.add_argument("--start-year", default=None)
    parser.add_argument("--bucket", default=None)
    parser.add_argument("--region", default=None)
    parser.add_argument("--raw-dir", default=None)
    parser.add_argument("--metadata-dir", default=None)
    parser.add_argument("--staging-dir", default=None)
    parser.add_argument("--raw-prefix", default=None)
    parser.add_argument("--meta-prefix", default=None)
    parser.add_argument("--silver-prefix", default=None)
    parser.add_argument("--analysis-prefix", default=None)
    parser.add_argument("--analysis-meta-prefix", default=None)
    parser.add_argument("--comparison-prefix", default=None)
    parser.add_argument("--core-staging-root", default=None)
    parser.add_argument("--geoid-reference", default=None)
    parser.add_argument("--zip-to-county", default=None)
    parser.add_argument("--overwrite", action="store_true")
    args = parser.parse_args()

    steps: list[tuple[str, Path, list[str]]] = []

    step_01_args: list[str] = []
    for flag, value in (("--start-year", args.start_year), ("--metadata-dir", args.metadata_dir)):
        _append_arg(step_01_args, flag, value)
    steps.append(("01_discover_efile_release", STEP_01, step_01_args))

    step_02_args: list[str] = []
    for flag, value in (
        ("--start-year", args.start_year),
        ("--bucket", args.bucket),
        ("--raw-dir", args.raw_dir),
        ("--metadata-dir", args.metadata_dir),
        ("--raw-prefix", args.raw_prefix),
        ("--meta-prefix", args.meta_prefix),
    ):
        _append_arg(step_02_args, flag, value)
    if args.overwrite:
        step_02_args.append("--overwrite")
    steps.append(("02_download_efile_release", STEP_02, step_02_args))

    step_03_args: list[str] = []
    for flag, value in (
        ("--start-year", args.start_year),
        ("--bucket", args.bucket),
        ("--region", args.region),
        ("--raw-dir", args.raw_dir),
        ("--metadata-dir", args.metadata_dir),
        ("--raw-prefix", args.raw_prefix),
        ("--meta-prefix", args.meta_prefix),
    ):
        _append_arg(step_03_args, flag, value)
    if args.overwrite:
        step_03_args.append("--overwrite")
    steps.append(("03_upload_efile_release_to_s3", STEP_03, step_03_args))

    step_04_args: list[str] = []
    for flag, value in (
        ("--start-year", args.start_year),
        ("--bucket", args.bucket),
        ("--region", args.region),
        ("--raw-dir", args.raw_dir),
        ("--metadata-dir", args.metadata_dir),
        ("--raw-prefix", args.raw_prefix),
        ("--meta-prefix", args.meta_prefix),
    ):
        _append_arg(step_04_args, flag, value)
    steps.append(("04_verify_efile_source_local_s3", STEP_04, step_04_args))

    step_05_args: list[str] = []
    for flag, value in (
        ("--start-year", args.start_year),
        ("--raw-dir", args.raw_dir),
        ("--metadata-dir", args.metadata_dir),
        ("--staging-dir", args.staging_dir),
        ("--silver-prefix", args.silver_prefix),
        ("--geoid-reference", args.geoid_reference),
        ("--zip-to-county", args.zip_to_county),
    ):
        _append_arg(step_05_args, flag, value)
    steps.append(("05_build_efile_benchmark_local", STEP_05, step_05_args))

    step_06_args: list[str] = []
    for flag, value in (
        ("--start-year", args.start_year),
        ("--bucket", args.bucket),
        ("--region", args.region),
        ("--metadata-dir", args.metadata_dir),
        ("--staging-dir", args.staging_dir),
        ("--silver-prefix", args.silver_prefix),
    ):
        _append_arg(step_06_args, flag, value)
    if args.overwrite:
        step_06_args.append("--overwrite")
    steps.append(("06_upload_filtered_efile_to_s3", STEP_06, step_06_args))

    step_07_args: list[str] = []
    for flag, value in (
        ("--start-year", args.start_year),
        ("--metadata-dir", args.metadata_dir),
        ("--staging-dir", args.staging_dir),
        ("--core-staging-root", args.core_staging_root),
        ("--bucket", args.bucket),
        ("--region", args.region),
        ("--comparison-prefix", args.comparison_prefix),
    ):
        _append_arg(step_07_args, flag, value)
    if args.overwrite:
        step_07_args.append("--overwrite")
    steps.append(("07_compare_efile_vs_core_local", STEP_07, step_07_args))

    step_08_args: list[str] = []
    for flag, value in (
        ("--metadata-dir", args.metadata_dir),
        ("--staging-dir", args.staging_dir),
        ("--bucket", args.bucket),
        ("--region", args.region),
        ("--analysis-prefix", args.analysis_prefix),
        ("--analysis-meta-prefix", args.analysis_meta_prefix),
    ):
        _append_arg(step_08_args, flag, value)
    steps.append(("08_extract_analysis_variables_local", STEP_08, step_08_args))

    step_09_args: list[str] = []
    for flag, value in (
        ("--bucket", args.bucket),
        ("--region", args.region),
        ("--metadata-dir", args.metadata_dir),
        ("--staging-dir", args.staging_dir),
        ("--analysis-prefix", args.analysis_prefix),
        ("--analysis-meta-prefix", args.analysis_meta_prefix),
    ):
        _append_arg(step_09_args, flag, value)
    if args.overwrite:
        step_09_args.append("--overwrite")
    steps.append(("09_upload_analysis_outputs", STEP_09, step_09_args))

    for step_name, script_path, script_args in steps:
        cmd = [sys.executable, str(script_path), *script_args]
        print(f"Running {step_name}...", flush=True)
        rc = subprocess.call(cmd, cwd=str(REPO_ROOT))
        if rc != 0:
            raise SystemExit(rc)

    print("NCCS efile pipeline (01 -> 09) completed.", flush=True)


if __name__ == "__main__":
    main()
