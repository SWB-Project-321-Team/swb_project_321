"""
Step 01: Download raw IRS EO BMF state CSVs and write a raw manifest.
"""

from __future__ import annotations

import argparse
import time
from pathlib import Path

import pandas as pd
import requests
from tqdm import tqdm

from common import (
    DEFAULT_STATE_CODES,
    IRS_BMF_BASE_URL,
    META_DIR,
    RAW_DIR,
    RAW_MANIFEST_PATH,
    banner,
    ensure_work_dirs,
    load_env_from_secrets,
    print_elapsed,
    raw_state_path,
    state_source_url,
)


def fetch_raw_states(
    *,
    states: list[str],
    raw_dir: Path = RAW_DIR,
    metadata_dir: Path = META_DIR,
    overwrite: bool = True,
) -> pd.DataFrame:
    """Fetch the requested raw IRS EO BMF state files and persist a manifest."""
    ensure_work_dirs(raw_dir=raw_dir, metadata_dir=metadata_dir)
    rows: list[dict[str, object]] = []
    for state_code in tqdm(states, desc="download IRS EO BMF states", unit="state"):
        source_url = state_source_url(state_code)
        output_path = raw_state_path(state_code, raw_dir)
        if output_path.exists() and not overwrite:
            local_size = output_path.stat().st_size
            print(f"[fetch] Skip existing raw state file: {output_path.name}", flush=True)
            rows.append(
                {
                    "state_code": state_code,
                    "source_url": source_url,
                    "local_path": str(output_path),
                    "source_content_length_bytes": pd.NA,
                    "source_content_type": pd.NA,
                    "source_last_modified": pd.NA,
                    "local_size_bytes": local_size,
                }
            )
            continue

        print(f"[fetch] Downloading {source_url}", flush=True)
        response = requests.get(source_url, timeout=180)
        response.raise_for_status()
        output_path.parent.mkdir(parents=True, exist_ok=True)
        output_path.write_bytes(response.content)
        local_size = output_path.stat().st_size
        print(f"[fetch] Wrote {output_path} ({local_size:,} bytes)", flush=True)
        rows.append(
            {
                "state_code": state_code,
                "source_url": source_url,
                "local_path": str(output_path),
                "source_content_length_bytes": int(response.headers.get("Content-Length", "0") or 0) or pd.NA,
                "source_content_type": response.headers.get("Content-Type"),
                "source_last_modified": response.headers.get("Last-Modified"),
                "local_size_bytes": local_size,
            }
        )

    manifest_df = pd.DataFrame(rows).sort_values("state_code", kind="mergesort").reset_index(drop=True)
    RAW_MANIFEST_PATH.parent.mkdir(parents=True, exist_ok=True)
    manifest_df.to_csv(RAW_MANIFEST_PATH, index=False)
    print(f"[fetch] Wrote raw manifest: {RAW_MANIFEST_PATH}", flush=True)
    return manifest_df


def main() -> None:
    parser = argparse.ArgumentParser(description="Download raw IRS EO BMF state CSVs.")
    parser.add_argument("--states", nargs="+", default=DEFAULT_STATE_CODES, help="Lowercase state codes to fetch")
    parser.add_argument("--raw-dir", type=Path, default=RAW_DIR, help="Local raw IRS EO BMF directory")
    parser.add_argument("--metadata-dir", type=Path, default=META_DIR, help="Local IRS EO BMF metadata directory")
    parser.add_argument("--overwrite", action="store_true", help="Overwrite existing state CSVs")
    args = parser.parse_args()

    start = time.perf_counter()
    banner("STEP 01 - FETCH IRS EO BMF RAW STATE FILES")
    load_env_from_secrets()
    print(f"[config] IRS EO BMF base URL: {IRS_BMF_BASE_URL}", flush=True)
    print(f"[config] States requested: {', '.join(state.lower() for state in args.states)}", flush=True)
    print(f"[config] Raw directory: {args.raw_dir}", flush=True)
    fetch_raw_states(
        states=[state.lower().strip()[:2] for state in args.states],
        raw_dir=args.raw_dir,
        metadata_dir=args.metadata_dir,
        overwrite=args.overwrite,
    )
    print_elapsed(start, "Step 01")


if __name__ == "__main__":
    main()
