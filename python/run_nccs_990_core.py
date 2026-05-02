"""
Run the NCCS Core pipeline in order: 01 -> 08.

Run from repo root:
  python python/run_nccs_990_core.py
"""

from __future__ import annotations

import argparse
import subprocess
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
PYTHON = REPO_ROOT / "python"
INGEST = PYTHON / "ingest" / "nccs_990_core"

STEP_01 = INGEST / "01_discover_core_release.py"
STEP_02 = INGEST / "02_download_core_release.py"
STEP_03 = INGEST / "03_upload_core_release_to_s3.py"
STEP_04 = INGEST / "04_verify_core_source_local_s3.py"
STEP_05 = INGEST / "05_filter_core_to_benchmark_local.py"
STEP_06 = INGEST / "06_upload_filtered_core_to_s3.py"
STEP_07 = INGEST / "07_extract_analysis_variables_local.py"
STEP_08 = INGEST / "08_upload_analysis_outputs.py"


def _append_arg(cmd: list[str], flag: str, value: object | None) -> None:
    if value is None:
        return
    cmd.extend([flag, str(value)])


def main() -> None:
    parser = argparse.ArgumentParser(description="Run the NCCS Core pipeline (01 -> 08).")
    parser.add_argument("--year", default=None)
    parser.add_argument("--bucket", default=None)
    parser.add_argument("--region", default=None)
    parser.add_argument("--core-raw-dir", default=None)
    parser.add_argument("--bridge-dir", default=None)
    parser.add_argument("--metadata-dir", default=None)
    parser.add_argument("--staging-dir", default=None)
    parser.add_argument("--raw-prefix", default=None)
    parser.add_argument("--bridge-prefix", default=None)
    parser.add_argument("--meta-prefix", default=None)
    parser.add_argument("--silver-prefix", default=None)
    parser.add_argument("--analysis-prefix", default=None)
    parser.add_argument("--analysis-documentation-prefix", default=None)
    parser.add_argument("--analysis-variable-mapping-prefix", default=None)
    parser.add_argument("--analysis-coverage-prefix", default=None)
    parser.add_argument("--bmf-staging-dir", default=None)
    parser.add_argument("--irs-bmf-raw-dir", default=None)
    parser.add_argument("--geoid-reference", default=None)
    parser.add_argument("--zip-to-county", default=None)
    parser.add_argument("--benchmark-states", default=None)
    parser.add_argument("--source-types", default=None)
    parser.add_argument("--chunk-size", default=None)
    parser.add_argument("--overwrite", action="store_true")
    args = parser.parse_args()

    steps: list[tuple[str, Path, list[str]]] = []

    step_01_args: list[str] = []
    for flag, value in (
        ("--year", args.year),
        ("--metadata-dir", args.metadata_dir),
        ("--geoid-reference", args.geoid_reference),
        ("--benchmark-states", args.benchmark_states),
    ):
        _append_arg(step_01_args, flag, value)
    steps.append(("01_discover_core_release", STEP_01, step_01_args))

    step_02_args: list[str] = []
    for flag, value in (
        ("--year", args.year),
        ("--bucket", args.bucket),
        ("--core-raw-dir", args.core_raw_dir),
        ("--bridge-dir", args.bridge_dir),
        ("--metadata-dir", args.metadata_dir),
        ("--raw-prefix", args.raw_prefix),
        ("--bridge-prefix", args.bridge_prefix),
        ("--meta-prefix", args.meta_prefix),
        ("--source-types", args.source_types),
        ("--benchmark-states", args.benchmark_states),
        ("--geoid-reference", args.geoid_reference),
    ):
        _append_arg(step_02_args, flag, value)
    if args.overwrite:
        step_02_args.append("--overwrite")
    steps.append(("02_download_core_release", STEP_02, step_02_args))

    step_03_args: list[str] = []
    for flag, value in (
        ("--year", args.year),
        ("--bucket", args.bucket),
        ("--region", args.region),
        ("--core-raw-dir", args.core_raw_dir),
        ("--bridge-dir", args.bridge_dir),
        ("--metadata-dir", args.metadata_dir),
        ("--raw-prefix", args.raw_prefix),
        ("--bridge-prefix", args.bridge_prefix),
        ("--meta-prefix", args.meta_prefix),
        ("--source-types", args.source_types),
        ("--benchmark-states", args.benchmark_states),
        ("--geoid-reference", args.geoid_reference),
    ):
        _append_arg(step_03_args, flag, value)
    if args.overwrite:
        step_03_args.append("--overwrite")
    steps.append(("03_upload_core_release_to_s3", STEP_03, step_03_args))

    step_04_args: list[str] = []
    for flag, value in (
        ("--year", args.year),
        ("--bucket", args.bucket),
        ("--region", args.region),
        ("--core-raw-dir", args.core_raw_dir),
        ("--bridge-dir", args.bridge_dir),
        ("--metadata-dir", args.metadata_dir),
        ("--raw-prefix", args.raw_prefix),
        ("--bridge-prefix", args.bridge_prefix),
        ("--meta-prefix", args.meta_prefix),
        ("--source-types", args.source_types),
        ("--benchmark-states", args.benchmark_states),
        ("--geoid-reference", args.geoid_reference),
    ):
        _append_arg(step_04_args, flag, value)
    steps.append(("04_verify_core_source_local_s3", STEP_04, step_04_args))

    step_05_args: list[str] = []
    for flag, value in (
        ("--year", args.year),
        ("--core-raw-dir", args.core_raw_dir),
        ("--bridge-dir", args.bridge_dir),
        ("--metadata-dir", args.metadata_dir),
        ("--staging-dir", args.staging_dir),
        ("--silver-prefix", args.silver_prefix),
        ("--geoid-reference", args.geoid_reference),
        ("--zip-to-county", args.zip_to_county),
        ("--source-types", args.source_types),
        ("--benchmark-states", args.benchmark_states),
        ("--chunk-size", args.chunk_size),
    ):
        _append_arg(step_05_args, flag, value)
    steps.append(("05_filter_core_to_benchmark_local", STEP_05, step_05_args))

    step_06_args: list[str] = []
    for flag, value in (
        ("--year", args.year),
        ("--bucket", args.bucket),
        ("--region", args.region),
        ("--metadata-dir", args.metadata_dir),
        ("--staging-dir", args.staging_dir),
        ("--silver-prefix", args.silver_prefix),
        ("--source-types", args.source_types),
    ):
        _append_arg(step_06_args, flag, value)
    if args.overwrite:
        step_06_args.append("--overwrite")
    steps.append(("06_upload_filtered_core_to_s3", STEP_06, step_06_args))

    step_07_args: list[str] = []
    for flag, value in (
        ("--bucket", args.bucket),
        ("--region", args.region),
        ("--metadata-dir", args.metadata_dir),
        ("--staging-dir", args.staging_dir),
        ("--bmf-staging-dir", args.bmf_staging_dir),
        ("--irs-bmf-raw-dir", args.irs_bmf_raw_dir),
        ("--analysis-prefix", args.analysis_prefix),
        ("--analysis-documentation-prefix", args.analysis_documentation_prefix),
        ("--analysis-variable-mapping-prefix", args.analysis_variable_mapping_prefix),
        ("--analysis-coverage-prefix", args.analysis_coverage_prefix),
        ("--tax-year", args.year),
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
        ("--analysis-documentation-prefix", args.analysis_documentation_prefix),
        ("--analysis-variable-mapping-prefix", args.analysis_variable_mapping_prefix),
        ("--analysis-coverage-prefix", args.analysis_coverage_prefix),
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

    print("NCCS Core pipeline (01 -> 08) completed.", flush=True)


if __name__ == "__main__":
    main()
