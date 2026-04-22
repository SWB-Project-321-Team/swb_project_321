"""
Compatibility wrapper for the canonical IRS EO BMF fetch pipeline.

This preserves the older entrypoint while delegating the actual work to:
- python/ingest/irs_bmf/01_fetch_bmf_release.py
- python/ingest/irs_bmf/02_upload_bmf_release_to_s3.py
"""

from __future__ import annotations

import argparse
import importlib.util
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
_FETCH_PATH = _IRS_BMF_DIR / "01_fetch_bmf_release.py"
_UPLOAD_PATH = _IRS_BMF_DIR / "02_upload_bmf_release_to_s3.py"


def _load_module(module_name: str, script_path: Path):
    spec = importlib.util.spec_from_file_location(module_name, script_path)
    if spec is None or spec.loader is None:
        raise ImportError(f"Unable to load module from {script_path}")
    module = importlib.util.module_from_spec(spec)
    sys.modules.setdefault(module_name, module)
    spec.loader.exec_module(module)
    return module


fetch_mod = _load_module("irs_bmf_fetch_wrapper_impl", _FETCH_PATH)
upload_mod = _load_module("irs_bmf_upload_wrapper_impl", _UPLOAD_PATH)


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Compatibility wrapper: fetch IRS EO BMF CSVs and optionally upload them to S3."
    )
    parser.add_argument("--states", nargs="+", default=fetch_mod.DEFAULT_STATE_CODES, metavar="ST")
    parser.add_argument("--upload", action="store_true", help="Also upload the fetched state files to S3")
    parser.add_argument("--bucket", default=upload_mod.DEFAULT_S3_BUCKET)
    parser.add_argument("--prefix", default=upload_mod.RAW_PREFIX)
    parser.add_argument("--region", default=upload_mod.DEFAULT_S3_REGION)
    parser.add_argument("--raw-dir", type=Path, default=fetch_mod.RAW_DIR)
    parser.add_argument("--metadata-dir", type=Path, default=fetch_mod.META_DIR)
    parser.add_argument("--overwrite", action="store_true")
    args = parser.parse_args()

    print("[00_fetch_bmf] Delegating to canonical irs_bmf pipeline.", flush=True)
    fetch_mod.banner("00 FETCH BMF WRAPPER -> STEP 01")
    fetch_mod.load_env_from_secrets()
    fetch_mod.fetch_raw_states(
        states=[state.lower().strip()[:2] for state in args.states],
        raw_dir=args.raw_dir,
        metadata_dir=args.metadata_dir,
        overwrite=args.overwrite,
    )
    if args.upload:
        upload_mod.upload_raw_release(
            raw_dir=args.raw_dir,
            metadata_dir=args.metadata_dir,
            bucket=args.bucket,
            region=args.region,
            raw_prefix=args.prefix,
            raw_meta_prefix=f"{args.prefix.rstrip('/')}/metadata",
            overwrite=args.overwrite,
        )


if __name__ == "__main__":
    main()
