"""
Download IRS Exempt Organizations Business Master File (EO BMF) by state
and save CSVs under 01_data/raw/irs_bmf/.

Uses the official IRS download URLs (one CSV per state). For the benchmark
regions we only need SD, MN, MT, and AZ. Other states can be added via
--states.

Reads: none (downloads from IRS).
Writes: 01_data/raw/irs_bmf/eo_sd.csv, eo_mn.csv, eo_mt.csv, eo_az.csv (default)

Run from repo root: python python/ingest/990_givingtuesday/api/02_fetch_bmf.py

Source: https://www.irs.gov/charities-non-profits/exempt-organizations-business-master-file-extract-eo-bmf
"""

import argparse
import sys
from pathlib import Path

import requests

# Ensure python/ is on path so we can import utils
_SCRIPT_DIR = Path(__file__).resolve()
_PYTHON_DIR = _SCRIPT_DIR.parents[3]  # api -> 990_givingtuesday -> ingest -> python
if str(_PYTHON_DIR) not in sys.path:
    sys.path.insert(0, str(_PYTHON_DIR))
from utils.paths import DATA

BMF_DIR = DATA / "raw" / "irs_bmf"

# IRS EO BMF: one CSV per state (lowercase state abbreviation)
IRS_BMF_BASE = "https://www.irs.gov/pub/irs-soi"
# Pattern: eo_sd.csv, eo_mn.csv, etc.
DEFAULT_STATES = ["sd", "mn", "mt", "az"]


def _download_state(state: str) -> bytes:
    """Download one state BMF CSV from IRS. State = lowercase 2-letter (e.g. sd)."""
    url = f"{IRS_BMF_BASE}/eo_{state}.csv"
    r = requests.get(url, timeout=120)
    r.raise_for_status()
    return r.content


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Download IRS EO BMF CSVs by state to 01_data/raw/irs_bmf/"
    )
    parser.add_argument(
        "--states",
        nargs="+",
        default=DEFAULT_STATES,
        metavar="ST",
        help=f"State codes (lowercase, e.g. sd mn mt az). Default: {' '.join(DEFAULT_STATES)}",
    )
    args = parser.parse_args()

    BMF_DIR.mkdir(parents=True, exist_ok=True)

    for state in args.states:
        state = state.lower().strip()[:2]
        path = BMF_DIR / f"eo_{state}.csv"
        try:
            content = _download_state(state)
            path.write_bytes(content)
            print(f"  {path.name}: {len(content):,} bytes")
        except requests.RequestException as e:
            print(f"  WARNING: {path.name}: {e}")

    print(f"BMF files in {BMF_DIR}")


if __name__ == "__main__":
    main()
