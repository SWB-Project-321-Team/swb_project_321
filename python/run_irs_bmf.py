"""
Run the IRS EO BMF pipeline in order: 01 -> 08.
"""

from __future__ import annotations

import argparse
import subprocess
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
PYTHON = REPO_ROOT / "python"
INGEST = PYTHON / "ingest" / "irs_bmf"

STEP_01 = INGEST / "01_fetch_bmf_release.py"
STEP_02 = INGEST / "02_upload_bmf_release_to_s3.py"
STEP_03 = INGEST / "03_verify_bmf_source_local_s3.py"
STEP_04 = INGEST / "04_filter_bmf_to_benchmark_local.py"
STEP_05 = INGEST / "05_upload_filtered_bmf_to_s3.py"
STEP_06 = INGEST / "06_verify_filtered_bmf_local_s3.py"
STEP_07 = INGEST / "07_extract_analysis_variables_local.py"
STEP_08 = INGEST / "08_upload_analysis_outputs.py"


def _append_arg(cmd: list[str], flag: str, value: object | None) -> None:
    if value is None:
        return
    cmd.extend([flag, str(value)])


def main() -> None:
    parser = argparse.ArgumentParser(description="Run the IRS EO BMF pipeline (01 -> 08).")
    parser.add_argument("--states", nargs="+", default=None)
    parser.add_argument("--bucket", default=None)
    parser.add_argument("--region", default=None)
    parser.add_argument("--raw-dir", default=None)
    parser.add_argument("--metadata-dir", default=None)
    parser.add_argument("--staging-dir", default=None)
    parser.add_argument("--raw-prefix", default=None)
    parser.add_argument("--raw-meta-prefix", default=None)
    parser.add_argument("--silver-prefix", default=None)
    parser.add_argument("--silver-meta-prefix", default=None)
    parser.add_argument("--analysis-prefix", default=None)
    parser.add_argument("--analysis-meta-prefix", default=None)
    parser.add_argument("--bmf-staging-dir", default=None)
    parser.add_argument("--geoid-reference", default=None)
    parser.add_argument("--zip-to-county", default=None)
    parser.add_argument("--overwrite", action="store_true")
    args = parser.parse_args()

    steps: list[tuple[str, Path, list[str]]] = []

    step_01_args: list[str] = []
    if args.states:
        step_01_args.extend(["--states", *args.states])
    for flag, value in (("--raw-dir", args.raw_dir), ("--metadata-dir", args.metadata_dir)):
        _append_arg(step_01_args, flag, value)
    if args.overwrite:
        step_01_args.append("--overwrite")
    steps.append(("01_fetch_bmf_release", STEP_01, step_01_args))

    step_02_args: list[str] = []
    for flag, value in (
        ("--bucket", args.bucket),
        ("--region", args.region),
        ("--raw-dir", args.raw_dir),
        ("--metadata-dir", args.metadata_dir),
        ("--raw-prefix", args.raw_prefix),
        ("--raw-meta-prefix", args.raw_meta_prefix),
    ):
        _append_arg(step_02_args, flag, value)
    if args.overwrite:
        step_02_args.append("--overwrite")
    steps.append(("02_upload_bmf_release_to_s3", STEP_02, step_02_args))

    step_03_args: list[str] = []
    for flag, value in (
        ("--bucket", args.bucket),
        ("--region", args.region),
        ("--raw-dir", args.raw_dir),
        ("--metadata-dir", args.metadata_dir),
        ("--raw-prefix", args.raw_prefix),
    ):
        _append_arg(step_03_args, flag, value)
    steps.append(("03_verify_bmf_source_local_s3", STEP_03, step_03_args))

    step_04_args: list[str] = []
    for flag, value in (
        ("--raw-dir", args.raw_dir),
        ("--metadata-dir", args.metadata_dir),
        ("--staging-dir", args.staging_dir),
        ("--geoid-reference", args.geoid_reference),
        ("--zip-to-county", args.zip_to_county),
    ):
        _append_arg(step_04_args, flag, value)
    steps.append(("04_filter_bmf_to_benchmark_local", STEP_04, step_04_args))

    step_05_args: list[str] = []
    for flag, value in (
        ("--bucket", args.bucket),
        ("--region", args.region),
        ("--metadata-dir", args.metadata_dir),
        ("--staging-dir", args.staging_dir),
        ("--silver-prefix", args.silver_prefix),
        ("--silver-meta-prefix", args.silver_meta_prefix),
    ):
        _append_arg(step_05_args, flag, value)
    if args.overwrite:
        step_05_args.append("--overwrite")
    steps.append(("05_upload_filtered_bmf_to_s3", STEP_05, step_05_args))

    step_06_args: list[str] = []
    for flag, value in (
        ("--bucket", args.bucket),
        ("--region", args.region),
        ("--metadata-dir", args.metadata_dir),
        ("--staging-dir", args.staging_dir),
        ("--silver-prefix", args.silver_prefix),
        ("--silver-meta-prefix", args.silver_meta_prefix),
    ):
        _append_arg(step_06_args, flag, value)
    steps.append(("06_verify_filtered_bmf_local_s3", STEP_06, step_06_args))

    step_07_args: list[str] = []
    for flag, value in (
        ("--bucket", args.bucket),
        ("--region", args.region),
        ("--metadata-dir", args.metadata_dir),
        ("--staging-dir", args.staging_dir),
        ("--analysis-prefix", args.analysis_prefix),
        ("--analysis-meta-prefix", args.analysis_meta_prefix),
        ("--bmf-staging-dir", args.bmf_staging_dir),
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

    print("IRS EO BMF pipeline (01 -> 08) completed.", flush=True)


if __name__ == "__main__":
    main()
