"""
Run the combined filtered 990 source-union pipeline in order: 01 -> 03.
"""

from __future__ import annotations

import argparse
import subprocess
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
INGEST = REPO_ROOT / "python" / "ingest" / "combined_990"

STEP_01 = INGEST / "01_build_combined_filtered_local.py"
STEP_02 = INGEST / "02_upload_combined_filtered_to_s3.py"
STEP_03 = INGEST / "03_verify_combined_filtered_local_s3.py"


def _append_arg(cmd: list[str], flag: str, value: object | None) -> None:
    if value is None:
        return
    cmd.extend([flag, str(value)])


def main() -> None:
    parser = argparse.ArgumentParser(description="Run the combined filtered 990 source-union pipeline (01 -> 03).")
    parser.add_argument("--staging-dir", default=None)
    parser.add_argument("--output", default=None)
    parser.add_argument("--bucket", default=None)
    parser.add_argument("--region", default=None)
    parser.add_argument("--silver-prefix", default=None)
    parser.add_argument("--metadata-prefix", default=None)
    parser.add_argument("--compression", default=None)
    parser.add_argument("--skip-if-unchanged", action=argparse.BooleanOptionalAction, default=None)
    parser.add_argument("--force-rebuild", action="store_true")
    parser.add_argument("--overwrite", action="store_true")
    args = parser.parse_args()

    steps: list[tuple[str, Path, list[str]]] = []

    step_01_args: list[str] = []
    for flag, value in (
        ("--staging-dir", args.staging_dir),
        ("--output", args.output),
        ("--compression", args.compression),
    ):
        _append_arg(step_01_args, flag, value)
    if args.skip_if_unchanged is not None:
        step_01_args.append("--skip-if-unchanged" if args.skip_if_unchanged else "--no-skip-if-unchanged")
    if args.force_rebuild:
        step_01_args.append("--force-rebuild")
    steps.append(("01_build_combined_filtered_local", STEP_01, step_01_args))

    step_02_args: list[str] = []
    for flag, value in (
        ("--output", args.output),
        ("--bucket", args.bucket),
        ("--region", args.region),
        ("--silver-prefix", args.silver_prefix),
        ("--metadata-prefix", args.metadata_prefix),
    ):
        _append_arg(step_02_args, flag, value)
    if args.overwrite:
        step_02_args.append("--overwrite")
    steps.append(("02_upload_combined_filtered_to_s3", STEP_02, step_02_args))

    step_03_args: list[str] = []
    for flag, value in (
        ("--output", args.output),
        ("--bucket", args.bucket),
        ("--region", args.region),
        ("--silver-prefix", args.silver_prefix),
        ("--metadata-prefix", args.metadata_prefix),
    ):
        _append_arg(step_03_args, flag, value)
    steps.append(("03_verify_combined_filtered_local_s3", STEP_03, step_03_args))

    for step_name, script_path, script_args in steps:
        cmd = [sys.executable, str(script_path), *script_args]
        print(f"Running {step_name}...", flush=True)
        rc = subprocess.call(cmd, cwd=str(REPO_ROOT))
        if rc != 0:
            raise SystemExit(rc)

    print("Combined filtered 990 source-union pipeline (01 -> 03) completed.", flush=True)


if __name__ == "__main__":
    main()
