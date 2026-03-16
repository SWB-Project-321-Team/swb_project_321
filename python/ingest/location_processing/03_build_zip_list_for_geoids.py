"""
Build a list of ZIP codes that fall in the benchmark-region counties (GEOIDs).

Uses the same ZIP→county crosswalk as build_ein_list.py: filter to counties
in GEOID_reference.csv (or xlsx) and write (ZIP, GEOID, Region) for use in donor-side
or other analyses that need "ZIPs in the 18 counties."

Reads:
  - 01_data/reference/GEOID_reference.csv  (from 01_fetch_geoid_reference.py; primary). Fallback: GEOID_reference.xlsx
  - 01_data/reference/zip_to_county_fips.csv (from 02_fetch_zip_to_county.py)

Writes:
  - 01_data/reference/zip_codes_in_benchmark_regions.csv (ZIP, GEOID, and Region if in ref)

Run from repo root: python python/ingest/location_processing/03_build_zip_list_for_geoids.py

ZIP–county source: HUD USPS Crosswalk or data.world (see docs/990/990_data_fetch_plan.md).
"""

import sys
from pathlib import Path

import pandas as pd

# Ensure python/ is on path so we can import utils
_SCRIPT_DIR = Path(__file__).resolve()
_PYTHON_DIR = _SCRIPT_DIR.parents[2]
if str(_PYTHON_DIR) not in sys.path:
    sys.path.insert(0, str(_PYTHON_DIR))
from utils.paths import DATA

REF_GEOID_XLSX = DATA / "reference" / "GEOID_reference.xlsx"
REF_GEOID_CSV = DATA / "reference" / "GEOID_reference.csv"  # from 01_fetch_geoid_reference.py
ZIP_TO_COUNTY_CSV = DATA / "reference" / "zip_to_county_fips.csv"
ZIP_LIST_CSV = DATA / "reference" / "zip_codes_in_benchmark_regions.csv"


def _load_zip_to_fips() -> pd.DataFrame:
    """Load zip_to_county_fips.csv and normalize to (ZIP, GEOID) with 5-digit GEOID; drop duplicates."""
    zip_df = pd.read_csv(ZIP_TO_COUNTY_CSV)
    zip_col = next((c for c in zip_df.columns if "zip" in c.lower()), zip_df.columns[0])
    fips_candidates = [
        c for c in zip_df.columns
        if "fips" in c.lower() or ("county" in c.lower() and "name" not in c.lower())
    ]
    fips_col = fips_candidates[0] if fips_candidates else zip_df.columns[1]
    zip_df["ZIP"] = zip_df[zip_col].astype(str).str.replace(r"\D", "", regex=True).str[:5]
    zip_df["GEOID"] = zip_df[fips_col].astype(str).str.zfill(5)
    return zip_df[["ZIP", "GEOID"]].drop_duplicates().dropna(subset=["GEOID"])


def build_zip_list() -> pd.DataFrame:
    """
    Return DataFrame of (ZIP, GEOID, Region) for all ZIPs in the benchmark counties.
    Region is included if GEOID reference has a region/cluster column.
    Reads GEOID_reference.csv (from 01) first; falls back to GEOID_reference.xlsx if CSV is missing.
    """
    # -------------------------------------------------------------------------
    # Step 1: Load GEOID reference (18 benchmark counties). Prefer CSV from 01.
    # -------------------------------------------------------------------------
    if REF_GEOID_CSV.exists():
        ref = pd.read_csv(REF_GEOID_CSV)
        print(f"[03_build_zip_list_for_geoids] Loaded GEOID reference from {REF_GEOID_CSV.name}.")
    elif REF_GEOID_XLSX.exists():
        ref = pd.read_excel(REF_GEOID_XLSX)
        print(f"[03_build_zip_list_for_geoids] Loaded GEOID reference from {REF_GEOID_XLSX.name} (CSV not found).")
    else:
        raise FileNotFoundError(
            f"GEOID reference not found at {REF_GEOID_CSV} or {REF_GEOID_XLSX}. "
            "Run 01_fetch_geoid_reference.py or provide GEOID_reference.csv or GEOID_reference.xlsx."
        )
    ref["GEOID"] = ref["GEOID"].astype(str).str.zfill(5)
    target_geoids = set(ref["GEOID"].astype(str))
    print(f"[03_build_zip_list_for_geoids] Target: {len(target_geoids)} benchmark GEOIDs.")

    # -------------------------------------------------------------------------
    # Step 2: Load ZIP → county FIPS crosswalk (from 02 or provided).
    # -------------------------------------------------------------------------
    if not ZIP_TO_COUNTY_CSV.exists():
        raise FileNotFoundError(
            f"ZIP–county file not found: {ZIP_TO_COUNTY_CSV}. "
            "Provide 01_data/reference/zip_to_county_fips.csv (see docs for HUD/data.world)."
        )
    zip_to_fips = _load_zip_to_fips()
    print(f"[03_build_zip_list_for_geoids] Loaded zip_to_county_fips: {len(zip_to_fips):,} rows.")
    # Keep only rows whose county (FIPS) is one of our 18 benchmark GEOIDs.
    in_scope = zip_to_fips[zip_to_fips["GEOID"].isin(target_geoids)].copy()
    print(f"[03_build_zip_list_for_geoids] Filtered to benchmark counties: {len(in_scope):,} (ZIP, GEOID) rows.")

    # -------------------------------------------------------------------------
    # Step 3: Attach Region (cluster name) from GEOID reference if present.
    # -------------------------------------------------------------------------
    region_col = None
    for c in ref.columns:
        if c and str(c).lower() in ("region", "cluster", "cluster_name", "benchmark_region", "area"):
            region_col = c
            break
    if region_col is None and "Region" in ref.columns:
        region_col = "Region"

    if region_col:
        geoid_to_region = ref[["GEOID", region_col]].drop_duplicates()
        in_scope = in_scope.merge(geoid_to_region, on="GEOID", how="left")
        in_scope = in_scope.rename(columns={region_col: "Region"})
    else:
        in_scope["Region"] = None

    return in_scope[["ZIP", "GEOID", "Region"]].sort_values(["GEOID", "ZIP"]).reset_index(drop=True)


def main() -> None:
    print("[03_build_zip_list_for_geoids] Starting: building ZIP list for benchmark-region counties (GEOID reference + zip_to_county_fips).")

    # -------------------------------------------------------------------------
    # Load GEOID reference and zip_to_county; filter to 18 counties; attach region; write CSV.
    # -------------------------------------------------------------------------
    df = build_zip_list()
    n_zip = df["ZIP"].nunique()
    n_geoid = df["GEOID"].nunique()

    ZIP_LIST_CSV.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(ZIP_LIST_CSV, index=False)
    print(f"[03_build_zip_list_for_geoids] Wrote {ZIP_LIST_CSV.name}: {len(df):,} rows ({n_zip:,} unique ZIPs in {n_geoid} counties).")
    print(f"[03_build_zip_list_for_geoids] Output path: {ZIP_LIST_CSV}")
    print("[03_build_zip_list_for_geoids] Done.")


if __name__ == "__main__":
    main()
