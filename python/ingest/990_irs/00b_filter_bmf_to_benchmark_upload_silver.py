"""
Compatibility wrapper for the canonical IRS EO BMF benchmark-filter pipeline.

This preserves the old helper entrypoint while delegating benchmark filtering to:
- python/ingest/irs_bmf/04_filter_bmf_to_benchmark_local.py
- python/ingest/irs_bmf/05_upload_filtered_bmf_to_s3.py
"""

from __future__ import annotations

import argparse
import importlib.util
import shutil
import sys
from pathlib import Path

_SCRIPT_DIR = Path(__file__).resolve()
_PYTHON_DIR = _SCRIPT_DIR.parents[2]
_REPO_ROOT = _SCRIPT_DIR.parents[3]
if str(_PYTHON_DIR) not in sys.path:
    sys.path.insert(0, str(_PYTHON_DIR))

_IRS_BMF_DIR = _PYTHON_DIR / "ingest" / "irs_bmf"
if str(_IRS_BMF_DIR) not in sys.path:
    sys.path.insert(0, str(_IRS_BMF_DIR))
_COMMON_PATH = _IRS_BMF_DIR / "common.py"
_FILTER_PATH = _IRS_BMF_DIR / "04_filter_bmf_to_benchmark_local.py"
_UPLOAD_PATH = _IRS_BMF_DIR / "05_upload_filtered_bmf_to_s3.py"


def _load_module(module_name: str, script_path: Path):
    spec = importlib.util.spec_from_file_location(module_name, script_path)
    if spec is None or spec.loader is None:
        raise ImportError(f"Unable to load module from {script_path}")
    module = importlib.util.module_from_spec(spec)
    sys.modules.setdefault(module_name, module)
    spec.loader.exec_module(module)
    return module


irs_bmf_common = _load_module("irs_bmf_common_wrapper_impl", _COMMON_PATH)
filter_mod = _load_module("irs_bmf_filter_wrapper_impl", _FILTER_PATH)
upload_mod = _load_module("irs_bmf_filtered_upload_wrapper_impl", _UPLOAD_PATH)

build_benchmark_zip_map = irs_bmf_common.build_benchmark_zip_map
filter_bmf_by_geoid = irs_bmf_common.filter_bmf_by_geoid


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Compatibility wrapper: filter IRS EO BMF to benchmark counties and optionally upload to silver."
    )
    parser.add_argument("--bmf-dir", type=Path, default=irs_bmf_common.RAW_DIR)
    parser.add_argument("--geoid-reference", type=Path, default=irs_bmf_common.GEOID_REFERENCE_CSV)
    parser.add_argument("--zip-to-county", type=Path, default=irs_bmf_common.ZIP_TO_COUNTY_CSV)
    parser.add_argument("--output", type=Path, default=irs_bmf_common.legacy_filtered_output_path())
    parser.add_argument("--no-upload", action="store_true")
    parser.add_argument("--bucket", default=irs_bmf_common.DEFAULT_S3_BUCKET)
    parser.add_argument("--prefix", default=irs_bmf_common.SILVER_PREFIX)
    parser.add_argument("--region", default=irs_bmf_common.DEFAULT_S3_REGION)
    parser.add_argument("--overwrite", action="store_true")
    args = parser.parse_args()

    print("[00b_filter_bmf] Delegating to canonical irs_bmf pipeline.", flush=True)
    irs_bmf_common.banner("00B FILTER BMF WRAPPER -> STEP 04")
    irs_bmf_common.load_env_from_secrets()
    filter_mod.build_filtered_outputs(
        raw_dir=args.bmf_dir,
        metadata_dir=irs_bmf_common.META_DIR,
        staging_dir=irs_bmf_common.STAGING_DIR,
        geoid_reference_path=args.geoid_reference,
        zip_to_county_path=args.zip_to_county,
    )
    compatibility_output = irs_bmf_common.legacy_filtered_output_path()
    if args.output.resolve() != compatibility_output.resolve():
        args.output.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(compatibility_output, args.output)
        print(f"[00b_filter_bmf] Copied compatibility parquet to requested output: {args.output}", flush=True)
    if not args.no_upload:
        upload_mod.main = upload_mod.main  # keep lint quiet for loaded module use
        upload_mod._upload_one  # noqa: B018 - assert module loaded
        # Reuse the canonical upload routine so the new filtered artifacts and the old
        # compatibility key stay in sync.
        upload_mod_args = argparse.Namespace(
            bucket=args.bucket,
            region=args.region,
            metadata_dir=irs_bmf_common.META_DIR,
            staging_dir=irs_bmf_common.STAGING_DIR,
            silver_prefix=args.prefix,
            silver_meta_prefix=f"{args.prefix.rstrip('/')}/metadata",
            overwrite=args.overwrite,
        )
        for local_path, s3_key in [
            (irs_bmf_common.combined_filtered_output_path(irs_bmf_common.STAGING_DIR), irs_bmf_common.combined_filtered_s3_key(upload_mod_args.silver_prefix)),
            (irs_bmf_common.legacy_filtered_output_path(), irs_bmf_common.legacy_filtered_s3_key(upload_mod_args.silver_prefix)),
            (irs_bmf_common.META_DIR / irs_bmf_common.FILTER_MANIFEST_PATH.name, irs_bmf_common.filter_manifest_s3_key(upload_mod_args.silver_meta_prefix)),
            *[
                (
                    irs_bmf_common.yearly_filtered_output_path(year, irs_bmf_common.STAGING_DIR),
                    irs_bmf_common.yearly_filtered_s3_key(year, upload_mod_args.silver_prefix),
                )
                for year in range(irs_bmf_common.ANALYSIS_TAX_YEAR_MIN, irs_bmf_common.ANALYSIS_TAX_YEAR_MAX + 1)
            ],
        ]:
            upload_mod._upload_one(local_path, upload_mod_args.bucket, upload_mod_args.region, s3_key, upload_mod_args.overwrite)


if __name__ == "__main__":
    main()
