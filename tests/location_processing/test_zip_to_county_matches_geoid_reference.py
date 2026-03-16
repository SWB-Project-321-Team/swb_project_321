"""
Test that zip_to_county_fips.csv is consistent with GEOID_reference.csv.

Both ultimately use Census 2020 ZCTA–county data. GEOID_reference lists all ZCTAs that
overlap each of the 18 benchmark counties; zip_to_county_fips assigns each ZIP to one
county (largest overlap). This test checks:
  - All 18 GEOIDs from GEOID_reference appear in zip_to_county_fips.
  - For each GEOID, every ZIP that zip_to_county_fips assigns to that county appears
    in GEOID_reference's ZIPs column for that county (subset).

Requires both files under DATA/reference/. Skip if either file is missing.

Run from repo root: pytest tests/location_processing/test_zip_to_county_matches_geoid_reference.py -v
"""

import sys
from pathlib import Path

import pandas as pd

try:
    import pytest
except ImportError:
    pytest = None

_FILE_DIR = Path(__file__).resolve().parent
_REPO_ROOT = _FILE_DIR.parent if _FILE_DIR.name == "tests" else _FILE_DIR.parent.parent
_PYTHON = _REPO_ROOT / "python"
if str(_PYTHON) not in sys.path:
    sys.path.insert(0, str(_PYTHON))
from utils.paths import DATA

REF_DIR = DATA / "reference"
GEOID_CSV = REF_DIR / "GEOID_reference.csv"
ZIP_TO_COUNTY_CSV = REF_DIR / "zip_to_county_fips.csv"


def _normalize_geoid(ser: pd.Series) -> pd.Series:
    """Normalize GEOID/FIPS to 5-character string."""
    return ser.astype(str).str.strip().str.replace(r"\D", "", regex=True).str.zfill(5)


def _load_geoid_reference() -> pd.DataFrame | None:
    if not GEOID_CSV.exists():
        return None
    df = pd.read_csv(GEOID_CSV)
    if "GEOID" not in df.columns:
        return None
    df["GEOID"] = _normalize_geoid(df["GEOID"])
    return df


def _load_zip_to_county() -> pd.DataFrame | None:
    if not ZIP_TO_COUNTY_CSV.exists():
        return None
    df = pd.read_csv(ZIP_TO_COUNTY_CSV)
    zip_col = next((c for c in df.columns if "zip" in c.lower()), df.columns[0])
    fips_candidates = [
        c for c in df.columns
        if "fips" in c.lower() or ("county" in c.lower() and "name" not in c.lower())
    ]
    fips_col = fips_candidates[0] if fips_candidates else df.columns[1]
    df["ZIP"] = df[zip_col].astype(str).str.replace(r"\D", "", regex=True).str[:5]
    df["FIPS"] = _normalize_geoid(df[fips_col])
    return df[["ZIP", "FIPS"]].drop_duplicates().dropna(subset=["FIPS", "ZIP"])


def test_zip_to_county_matches_geoid_reference():
    """All 18 GEOIDs appear in zip_to_county_fips; for each GEOID, ZIPs in zip_to_county_fips are in GEOID_reference ZIPs."""
    ref_df = _load_geoid_reference()
    zip_df = _load_zip_to_county()
    if ref_df is None:
        msg = f"GEOID_reference.csv not found: {GEOID_CSV}. Run 02_fetch_geoid_reference.py or set SWB_321_DATA_ROOT."
        if pytest is not None:
            pytest.skip(msg)
        print(f"SKIP: {msg}")
        return
    if zip_df is None:
        msg = f"zip_to_county_fips.csv not found: {ZIP_TO_COUNTY_CSV}. Run 02_fetch_zip_to_county.py or add file to DATA/reference/."
        if pytest is not None:
            pytest.skip(msg)
        print(f"SKIP: {msg}")
        return

    ref_geoids = set(ref_df["GEOID"])
    assert len(ref_geoids) == 18, f"Expected 18 benchmark GEOIDs in GEOID_reference, got {len(ref_geoids)}."

    zip_geoids = set(zip_df["FIPS"])
    missing_in_zip = ref_geoids - zip_geoids
    assert not missing_in_zip, (
        f"GEOIDs in GEOID_reference but not in zip_to_county_fips: {missing_in_zip}. "
        "zip_to_county_fips should cover all 18 benchmark counties."
    )

    if "ZIPs" not in ref_df.columns:
        return  # no ZIPs column to compare

    # For each GEOID, ZIPs in zip_to_county_fips (for that FIPS) must be subset of GEOID_reference ZIPs for that GEOID
    ref_geoid_to_zips = {}
    for _, row in ref_df.iterrows():
        geoid = row["GEOID"]
        zips_str = row.get("ZIPs", "") or ""
        ref_geoid_to_zips[geoid] = set(z.strip() for z in str(zips_str).split(";") if z.strip())

    for geoid in ref_geoids:
        zips_in_zip_file = set(zip_df.loc[zip_df["FIPS"] == geoid, "ZIP"].astype(str).str.strip())
        ref_zips = ref_geoid_to_zips.get(geoid, set())
        extra = zips_in_zip_file - ref_zips
        assert not extra, (
            f"GEOID {geoid}: zip_to_county_fips assigns ZIPs not in GEOID_reference ZIPs: {sorted(extra)[:10]}{'...' if len(extra) > 10 else ''}. "
            "zip_to_county_fips should only assign ZIPs that overlap this county (per GEOID_reference)."
        )


if __name__ == "__main__":
    test_zip_to_county_matches_geoid_reference()
    print("OK: zip_to_county_fips.csv matches GEOID_reference.csv")
