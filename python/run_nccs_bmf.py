"""
Run the NCCS BMF pipeline in order: 01 -> 06.
"""

from __future__ import annotations

import argparse
import subprocess
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
INGEST = REPO_ROOT / "python" / "ingest" / "nccs_bmf"

STEP_01 = INGEST / "01_discover_bmf_release.py"
STEP_02 = INGEST / "02_download_bmf_release.py"
STEP_03 = INGEST / "03_upload_bmf_release_to_s3.py"
STEP_04 = INGEST / "04_verify_bmf_source_local_s3.py"
STEP_05 = INGEST / "05_filter_bmf_to_benchmark_local.py"
STEP_06 = INGEST / "06_upload_filtered_bmf_to_s3.py"


def _append_arg(cmd: list[str], flag: str, value: object | None) -> None:
    if value is None:
        return
    cmd.extend([flag, str(value)])


def main() -> None:
    parser = argparse.ArgumentParser(description="Run the NCCS BMF pipeline (01 -> 06).")
    parser.add_argument("--start-year", default=None)
    parser.add_argument("--bucket", default=None)
    parser.add_argument("--region", default=None)
    parser.add_argument("--raw-dir", default=None)
    parser.add_argument("--metadata-dir", default=None)
    parser.add_argument("--staging-dir", default=None)
    parser.add_argument("--raw-prefix", default=None)
    parser.add_argument("--meta-prefix", default=None)
    parser.add_argument("--silver-prefix", default=None)
    parser.add_argument("--geoid-reference", default=None)
    parser.add_argument("--zip-to-county", default=None)
    parser.add_argument("--overwrite", action="store_true")
    args = parser.parse_args()

    steps: list[tuple[str, Path, list[str]]] = []

    step_01_args: list[str] = []
    for flag, value in (("--start-year", args.start_year), ("--metadata-dir", args.metadata_dir)):
        _append_arg(step_01_args, flag, value)
    steps.append(("01_discover_bmf_release", STEP_01, step_01_args))

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
    steps.append(("02_download_bmf_release", STEP_02, step_02_args))

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
    steps.append(("03_upload_bmf_release_to_s3", STEP_03, step_03_args))

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
    steps.append(("04_verify_bmf_source_local_s3", STEP_04, step_04_args))

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
    steps.append(("05_filter_bmf_to_benchmark_local", STEP_05, step_05_args))

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
    steps.append(("06_upload_filtered_bmf_to_s3", STEP_06, step_06_args))

    for step_name, script_path, script_args in steps:
        cmd = [sys.executable, str(script_path), *script_args]
        print(f"Running {step_name}...", flush=True)
        rc = subprocess.call(cmd, cwd=str(REPO_ROOT))
        if rc != 0:
            raise SystemExit(rc)

    print("NCCS BMF pipeline (01 -> 06) completed.", flush=True)


if __name__ == "__main__":
    main()
