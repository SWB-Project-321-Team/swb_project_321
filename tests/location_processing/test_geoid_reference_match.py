"""
Test that GEOID_reference.csv (from 01_fetch_geoid_reference.py) matches GEOID_reference.xlsx.

Compares the 18 benchmark counties: same GEOIDs, and for each GEOID the same County, State,
Cluster_ID, and Cluster_name. The CSV has an extra ZIPs column (from Census ZCTA); the xlsx
does not, so ZIPs are not compared.

Requires both files under DATA/reference/ (or SWB_321_DATA_ROOT). Skip if either file is missing.

Run from repo root: python tests/location_processing/test_geoid_reference_match.py
Or: pytest tests/location_processing/test_geoid_reference_match.py -v
"""

import sys
from pathlib import Path

import pandas as pd

try:
    import pytest
except ImportError:
    pytest = None

# Repo root and python/ on path (tests may be in tests/ or tests/subfolder/)
_FILE_DIR = Path(__file__).resolve().parent
_REPO_ROOT = _FILE_DIR.parent if _FILE_DIR.name == "tests" else _FILE_DIR.parent.parent
_PYTHON = _REPO_ROOT / "python"
if str(_PYTHON) not in sys.path:
    sys.path.insert(0, str(_PYTHON))
from utils.paths import DATA

REF_DIR = DATA / "reference"
GEOID_CSV = REF_DIR / "GEOID_reference.csv"
GEOID_XLSX = REF_DIR / "GEOID_reference.xlsx"

# Columns that must match between CSV and xlsx (xlsx has no ZIPs)
COLS_TO_MATCH = ["County", "State", "GEOID", "Cluster_ID", "Cluster_name"]


def _normalize_geoid(ser: pd.Series) -> pd.Series:
    """Normalize GEOID to 5-character string."""
    return ser.astype(str).str.strip().str.zfill(5)


def _load_and_normalize_csv() -> pd.DataFrame | None:
    if not GEOID_CSV.exists():
        return None
    df = pd.read_csv(GEOID_CSV)
    if "GEOID" not in df.columns:
        return None
    df["GEOID"] = _normalize_geoid(df["GEOID"])
    for c in ["County", "State", "Cluster_name"]:
        if c in df.columns:
            df[c] = df[c].astype(str).str.strip()
    if "Cluster_ID" in df.columns:
        df["Cluster_ID"] = pd.to_numeric(df["Cluster_ID"], errors="coerce").fillna(0).astype(int)
    return df


def _load_and_normalize_xlsx() -> pd.DataFrame | None:
    if not GEOID_XLSX.exists():
        return None
    df = pd.read_excel(GEOID_XLSX)
    if "GEOID" not in df.columns:
        return None
    df["GEOID"] = _normalize_geoid(df["GEOID"])
    for c in ["County", "State", "Cluster_name"]:
        if c in df.columns:
            df[c] = df[c].astype(str).str.strip()
    if "Cluster_ID" in df.columns:
        df["Cluster_ID"] = pd.to_numeric(df["Cluster_ID"], errors="coerce").fillna(0).astype(int)
    return df


def test_geoid_reference_csv_matches_xlsx():
    """GEOID_reference.csv (from 02 script) should match GEOID_reference.xlsx on GEOID, County, State, Cluster_ID, Cluster_name."""
    csv_df = _load_and_normalize_csv()
    xlsx_df = _load_and_normalize_xlsx()
    if csv_df is None:
        msg = f"CSV not found: {GEOID_CSV}. Run 01_fetch_geoid_reference.py or set SWB_321_DATA_ROOT."
        if pytest is not None:
            pytest.skip(msg)
        print(f"SKIP: {msg}")
        return
    if xlsx_df is None:
        msg = f"xlsx not found: {GEOID_XLSX}. Add the project GEOID reference file to DATA/reference/."
        if pytest is not None:
            pytest.skip(msg)
        print(f"SKIP: {msg}")
        return
    for c in COLS_TO_MATCH:
        if c not in csv_df.columns:
            raise AssertionError(f"CSV missing column: {c}")
        if c not in xlsx_df.columns:
            raise AssertionError(f"xlsx missing column: {c}")
    csv_geoids = set(csv_df["GEOID"])
    xlsx_geoids = set(xlsx_df["GEOID"])
    assert csv_geoids == xlsx_geoids, (
        f"GEOID set mismatch: CSV has {len(csv_geoids)}, xlsx has {len(xlsx_geoids)}. "
        f"Only in CSV: {csv_geoids - xlsx_geoids}. Only in xlsx: {xlsx_geoids - csv_geoids}."
    )
    assert len(csv_geoids) == 18, f"Expected 18 benchmark GEOIDs, got {len(csv_geoids)} in CSV."
    csv_sub = csv_df[COLS_TO_MATCH].set_index("GEOID").sort_index()
    xlsx_sub = xlsx_df[COLS_TO_MATCH].set_index("GEOID").sort_index()
    # Align and compare (both have same GEOIDs now)
    for geoid in csv_geoids:
        csv_row = csv_sub.loc[geoid]
        xlsx_row = xlsx_sub.loc[geoid]
        for col in ["County", "State", "Cluster_ID", "Cluster_name"]:
            c_val, x_val = csv_row[col], xlsx_row[col]
            assert c_val == x_val, (
                f"GEOID {geoid} {col}: CSV={c_val!r} vs xlsx={x_val!r}"
            )


if __name__ == "__main__":
    test_geoid_reference_csv_matches_xlsx()
    print("OK: GEOID_reference.csv matches GEOID_reference.xlsx")
