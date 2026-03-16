"""
Test the output of build_ein_list.py (eins_in_benchmark_regions.csv).

Checks that the CSV exists, has the expected columns (EIN, optionally Region),
all EINs are 9-digit, (EIN, Region) pairs are unique (same EIN may appear in
multiple regions), and Region values (if present) are from the benchmark
clusters. Skips all tests if the output file does not exist.

Run from repo root:
  python tests/990_irs/test_ein_list_output.py
  pytest tests/990_irs/test_ein_list_output.py -v
"""

import sys
from pathlib import Path

try:
    import pandas as pd
except ImportError:
    pd = None

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

EIN_LIST_CSV = DATA / "reference" / "eins_in_benchmark_regions.csv"

# Expected region labels from GEOID reference Cluster_name (01_fetch_geoid_reference.py).
# Add or change if the benchmark set is updated.
EXPECTED_REGIONS = {"SiouxFalls", "BlackHills", "Billings", "Missoula", "Flagstaff"}


def _load_ein_list():
    """Load the EIN list CSV as strings. Returns (df, error_message). If error, df is None."""
    if not EIN_LIST_CSV.exists():
        return None, f"output file not found: {EIN_LIST_CSV}"
    if pd is None:
        return None, "pandas not installed"
    try:
        df = pd.read_csv(EIN_LIST_CSV, dtype=str)
    except Exception as e:
        return None, str(e)
    return df, None


def test_ein_list_file_exists():
    """Output file exists at DATA/reference/eins_in_benchmark_regions.csv."""
    assert EIN_LIST_CSV.exists(), f"Expected {EIN_LIST_CSV} (run build_ein_list.py first)"


def test_ein_list_has_ein_column():
    """CSV has an EIN column."""
    df, err = _load_ein_list()
    if err is not None:
        pytest.skip(err)
    ein_col = next((c for c in df.columns if c and str(c).strip().upper() == "EIN"), None)
    assert ein_col is not None, f"Missing EIN column; columns: {list(df.columns)}"


def test_ein_list_eins_are_nine_digits():
    """All EIN values are 9-digit strings (leading zeros preserved when read as string)."""
    df, err = _load_ein_list()
    if err is not None:
        pytest.skip(err)
    ein_col = next((c for c in df.columns if str(c).strip().upper() == "EIN"), df.columns[0])
    eins = df[ein_col].astype(str).str.strip()
    bad = eins[~eins.str.match(r"^\d{9}$")]
    assert bad.empty, f"EINs must be exactly 9 digits; invalid: {bad.head().tolist()}"


def test_ein_list_no_duplicate_ein_region_pairs():
    """When Region has values: no duplicate (EIN, Region) rows. Same EIN may appear in multiple regions. When Region is missing or all null, duplicate EINs are allowed."""
    df, err = _load_ein_list()
    if err is not None:
        pytest.skip(err)
    ein_col = next((c for c in df.columns if str(c).strip().upper() == "EIN"), df.columns[0])
    region_col = next((c for c in df.columns if str(c).strip().lower() == "region"), None)
    if region_col is None or df[region_col].dropna().empty:
        return  # No region: duplicate EINs allowed (we do not dedupe by EIN)
    subset = [ein_col, region_col]
    dup = df[df.duplicated(subset=subset, keep=False)]
    assert dup.empty, f"Duplicate (EIN, Region) rows: {dup[subset].drop_duplicates().head().to_dict()}"


def test_ein_list_non_empty():
    """At least one row."""
    df, err = _load_ein_list()
    if err is not None:
        pytest.skip(err)
    assert len(df) > 0, "EIN list should not be empty"


def test_ein_list_region_values_if_present():
    """If Region column exists and has values, they are from the expected benchmark clusters."""
    df, err = _load_ein_list()
    if err is not None:
        pytest.skip(err)
    region_col = next((c for c in df.columns if str(c).strip().lower() == "region"), None)
    if region_col is None:
        return
    values = df[region_col].dropna().astype(str).str.strip()
    if values.empty:
        return
    unexpected = set(values.unique()) - EXPECTED_REGIONS
    assert not unexpected, f"Unexpected Region values: {unexpected} (expected subset of {EXPECTED_REGIONS})"


def run_checks() -> tuple[bool, list[str]]:
    """
    Run all checks. Returns (all_passed, list of messages).
    Can be used when running as script (python test_ein_list_output.py).
    """
    messages = []
    if not EIN_LIST_CSV.exists():
        messages.append(f"SKIP: output file not found at {EIN_LIST_CSV}. Run build_ein_list.py first.")
        return True, messages
    if pd is None:
        messages.append("SKIP: pandas not installed.")
        return True, messages

    df, err = _load_ein_list()
    if err is not None:
        messages.append(f"FAIL: {err}")
        return False, messages

    # File exists and loaded
    ein_col = next((c for c in df.columns if str(c).strip().upper() == "EIN"), None)
    if ein_col is None:
        messages.append(f"FAIL: Missing EIN column; columns: {list(df.columns)}")
        return False, messages
    messages.append(f"Loaded {len(df):,} rows, columns: {list(df.columns)}.")

    # 9-digit EINs
    eins = df[ein_col].astype(str).str.strip()
    bad = eins[~eins.str.match(r"^\d{9}$")]
    if not bad.empty:
        messages.append(f"FAIL: EINs must be 9 digits; invalid examples: {bad.head().tolist()}")
        return False, messages
    messages.append("All EINs are 9-digit.")

    # No duplicate (EIN, Region) rows when Region has values; when Region missing or all null, duplicate EINs allowed
    region_col = next((c for c in df.columns if str(c).strip().lower() == "region"), None)
    if region_col is not None and df[region_col].notna().any():
        subset = [ein_col, region_col]
        if df.duplicated(subset=subset).any():
            n_dup = df.duplicated(subset=subset).sum()
            messages.append(f"FAIL: {n_dup} duplicate (EIN, Region) row(s).")
            return False, messages
        messages.append("No duplicate (EIN, Region) rows.")
    else:
        messages.append("No Region or Region all null; duplicate EINs allowed.")

    # Region check if present
    region_col = next((c for c in df.columns if str(c).strip().lower() == "region"), None)
    if region_col and df[region_col].notna().any():
        values = set(df[region_col].dropna().astype(str).str.strip().unique())
        unexpected = values - EXPECTED_REGIONS
        if unexpected:
            messages.append(f"FAIL: Unexpected Region values: {unexpected}")
            return False, messages
        messages.append(f"Region values OK: {sorted(values)}.")

    messages.append("All checks passed.")
    return True, messages


if __name__ == "__main__":
    ok, messages = run_checks()
    for m in messages:
        print(m)
    sys.exit(0 if ok else 1)
