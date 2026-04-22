"""
Test that local BMF files (from 00_fetch_bmf.py) match the IRS source byte-for-byte.

For each state file in 01_data/raw/irs_bmf/eo_{state}.csv, downloads the same file
from the IRS and compares content. Skips states that have no local file.

Run from repo root:
  python tests/990_irs/test_bmf_matches_irs.py
  python tests/990_irs/test_bmf_matches_irs.py --states sd mn mt az
  pytest tests/990_irs/test_bmf_matches_irs.py -v
"""

import sys
from pathlib import Path

import requests

try:
    import pytest
except ImportError:
    pytest = None

_FILE_DIR = Path(__file__).resolve().parent
_REPO_ROOT = _FILE_DIR.parent.parent
_PYTHON = _REPO_ROOT / "python"
if str(_PYTHON) not in sys.path:
    sys.path.insert(0, str(_PYTHON))
from utils.paths import DATA

BMF_DIR = DATA / "raw" / "irs_bmf"
IRS_BMF_BASE = "https://www.irs.gov/pub/irs-soi"
DEFAULT_STATES = ["sd", "mn", "mt", "az"]


def _fetch_irs_bmf(state: str, session: requests.Session | None = None) -> bytes | None:
    """Download one state BMF CSV from IRS. Returns None on error."""
    url = f"{IRS_BMF_BASE}/eo_{state}.csv"
    sess = session or requests.Session()
    try:
        r = sess.get(url, timeout=120)
        r.raise_for_status()
        return r.content
    except requests.RequestException:
        return None


def _check_state(state: str, session: requests.Session | None = None) -> tuple[bool, str]:
    """
    Compare local eo_{state}.csv to IRS. Returns (True, "") if match, else (False, reason).
    Skips (returns True with message) if local file does not exist.
    """
    path = BMF_DIR / f"eo_{state}.csv"
    if not path.exists():
        return (True, f"skip (no local file: {path.name})")

    local = path.read_bytes()
    irs_content = _fetch_irs_bmf(state, session)
    if irs_content is None:
        return (False, "failed to fetch from IRS")

    if len(local) != len(irs_content):
        return (False, f"size mismatch: local {len(local):,} vs IRS {len(irs_content):,}")

    if local != irs_content:
        return (False, "content mismatch (bytes differ)")

    return (True, f"match ({len(local):,} bytes)")


def run_checks(states: list[str] | None = None) -> tuple[bool, int]:
    """
    Check each state in BMF_DIR (or given list). Returns (all_passed, num_compared).
    If states is None, use DEFAULT_STATES. num_compared is how many local files were compared.
    """
    if states is None:
        states = DEFAULT_STATES
    session = requests.Session()
    all_ok = True
    num_compared = 0
    for state in states:
        state = state.lower().strip()[:2]
        ok, msg = _check_state(state, session)
        print(f"  [{state}] {msg}")
        if not ok:
            all_ok = False
        if "match" in msg:
            num_compared += 1
    return (all_ok, num_compared)


def test_bmf_matches_irs() -> None:
    """Pytest: assert that local BMF files match IRS source (skip missing locals)."""
    if not BMF_DIR.exists():
        pytest.skip(f"BMF directory not found: {BMF_DIR}. Run 00_fetch_bmf.py first.")
    existing = [p.stem.replace("eo_", "") for p in BMF_DIR.glob("eo_*.csv")]
    if not existing:
        pytest.skip(f"No eo_*.csv files in {BMF_DIR}. Run 00_fetch_bmf.py first.")
    ok, _ = run_checks(existing)
    assert ok, "One or more BMF files did not match IRS source"


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Verify local BMF files match IRS source")
    parser.add_argument(
        "--states",
        nargs="+",
        default=None,
        metavar="ST",
        help=f"State codes to check (default: {DEFAULT_STATES})",
    )
    args = parser.parse_args()

    print(f"[test_bmf_matches_irs] Comparing local BMF files to IRS ({IRS_BMF_BASE})")
    print(f"[test_bmf_matches_irs] Local directory: {BMF_DIR}")

    states = args.states or DEFAULT_STATES
    ok, num_compared = run_checks(states)
    if not ok:
        print("FAIL: One or more BMF files did not match the IRS source.", file=sys.stderr)
        sys.exit(1)
    if num_compared == 0:
        print("No local BMF files found to compare. Run 00_fetch_bmf.py first.")
    else:
        print(f"OK: All {num_compared} BMF file(s) match the IRS source.")
