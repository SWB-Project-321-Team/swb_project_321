"""
Build a list of ZIP codes that fall in the benchmark-region counties (GEOIDs).

Uses the same ZIP→county crosswalk as build_ein_list.py: filter to counties
in GEOID_reference.xlsx and write (ZIP, GEOID, Region) for use in donor-side
or other analyses that need "ZIPs in the 18 counties."

Reads:
  - 01_data/reference/GEOID_reference.xlsx  (target county GEOIDs; optional: region/cluster column)
  - 01_data/reference/zip_to_county_fips.csv (ZIP → county FIPS; same source as build_ein_list)

Writes:
  - 01_data/reference/zip_codes_in_benchmark_regions.csv (ZIP, GEOID, and Region if in ref)

Run from repo root: python python/ingest/location_processing/build_zip_list_for_geoids.py

ZIP–county source: HUD USPS Crosswalk or data.world (see docs/990_data_fetch_plan.md).
"""

from pathlib import Path

import pandas as pd

# ── Paths (same as build_ein_list.py) ────────────────────────────────────────
BASE = Path(__file__).resolve().parent.parent.parent.parent  # location_processing -> ingest -> python -> root
DATA = BASE / "data" / "321_Black_Hills_Area_Community_Foundation_2025_08" / "01_data"

REF_GEOID_XLSX = DATA / "reference" / "GEOID_reference.xlsx"
ZIP_TO_COUNTY_CSV = DATA / "reference" / "zip_to_county_fips.csv"
ZIP_LIST_CSV = DATA / "reference" / "zip_codes_in_benchmark_regions.csv"


def _load_zip_to_fips() -> pd.DataFrame:
    """Load and normalize ZIP→FIPS from zip_to_county_fips.csv."""
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
    Region is included if GEOID_reference.xlsx has a region/cluster column.
    """
    if not REF_GEOID_XLSX.exists():
        raise FileNotFoundError(
            f"GEOID reference not found: {REF_GEOID_XLSX}. "
            "Provide 01_data/reference/GEOID_reference.xlsx."
        )
    if not ZIP_TO_COUNTY_CSV.exists():
        raise FileNotFoundError(
            f"ZIP–county file not found: {ZIP_TO_COUNTY_CSV}. "
            "Provide 01_data/reference/zip_to_county_fips.csv (see docs for HUD/data.world)."
        )

    ref = pd.read_excel(REF_GEOID_XLSX)
    ref["GEOID"] = ref["GEOID"].astype(str).str.zfill(5)
    target_geoids = set(ref["GEOID"].astype(str))

    # Optional: region/cluster column (common names)
    region_col = None
    for c in ref.columns:
        if c and str(c).lower() in ("region", "cluster", "benchmark_region", "area"):
            region_col = c
            break
    if region_col is None and "Region" in ref.columns:
        region_col = "Region"

    zip_to_fips = _load_zip_to_fips()
    # Keep only ZIPs in our counties
    in_scope = zip_to_fips[zip_to_fips["GEOID"].isin(target_geoids)].copy()

    if region_col:
        geoid_to_region = ref[["GEOID", region_col]].drop_duplicates()
        in_scope = in_scope.merge(geoid_to_region, on="GEOID", how="left")
        in_scope = in_scope.rename(columns={region_col: "Region"})
    else:
        in_scope["Region"] = None

    return in_scope[["ZIP", "GEOID", "Region"]].sort_values(["GEOID", "ZIP"]).reset_index(drop=True)


def main() -> None:
    print("Building ZIP list for benchmark-region GEOIDs...")
    df = build_zip_list()
    ZIP_LIST_CSV.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(ZIP_LIST_CSV, index=False)
    n_zip = df["ZIP"].nunique()
    n_geoid = df["GEOID"].nunique()
    print(f"Wrote {len(df)} rows ({n_zip} unique ZIPs, {n_geoid} counties) to {ZIP_LIST_CSV}")


if __name__ == "__main__":
    main()
