"""
Build the EIN list for benchmark regions: organizations whose principal address
falls in one of the 18 counties in GEOID_reference.xlsx.

Reads:
  - 01_data/reference/GEOID_reference.xlsx  (target county GEOIDs)
  - 01_data/reference/zip_to_county_fips.csv (ZIP → county FIPS)
  - 01_data/raw/irs_bmf/*.csv (IRS Business Master File by state; needs EIN, STATE, ZIP)

Writes:
  - 01_data/reference/eins_in_benchmark_regions.csv (column EIN)

Run from repo root: python python/ingest/990_givingtuesday/03_build_ein_list.py

See docs/990_data_fetch_plan.md for how to obtain BMF and zip-to-county data.
"""

from pathlib import Path

import pandas as pd

# ── Paths (repo root = 4 levels up from this file: 990_givingtuesday -> ingest -> python -> root) ───
BASE = Path(__file__).resolve().parent.parent.parent.parent
DATA = BASE / "data" / "321_Black_Hills_Area_Community_Foundation_2025_08" / "01_data"

EIN_LIST_CSV = DATA / "reference" / "eins_in_benchmark_regions.csv"
REF_GEOID_XLSX = DATA / "reference" / "GEOID_reference.xlsx"
ZIP_TO_COUNTY_CSV = DATA / "reference" / "zip_to_county_fips.csv"
BMF_DIR = DATA / "raw" / "irs_bmf"

# 2-digit state FIPS (from GEOID) -> USPS state abbreviation for BMF filter.
STATE_FIPS_TO_ABBR = {"04": "AZ", "27": "MN", "30": "MT", "46": "SD"}


def _normalize_ein(ein: str) -> str:
    """Return EIN as 9-digit string, zero-padded, no hyphen."""
    s = str(ein).strip().replace("-", "").replace(" ", "")
    return s.zfill(9) if len(s) <= 9 else s[:9]


def build_ein_list() -> pd.DataFrame:
    """
    Build EIN list by filtering BMF to the benchmark-region counties.
    Requires GEOID_reference.xlsx, zip_to_county_fips.csv, and BMF CSVs
    in 01_data/raw/irs_bmf/ with columns EIN, STATE, ZIP.
    """
    if not REF_GEOID_XLSX.exists():
        raise FileNotFoundError(
            f"GEOID reference not found: {REF_GEOID_XLSX}. "
            "Provide 01_data/reference/GEOID_reference.xlsx."
        )
    ref = pd.read_excel(REF_GEOID_XLSX)
    ref["GEOID"] = ref["GEOID"].astype(str).str.zfill(5)
    target_fips = set(ref["GEOID"].astype(str))

    state_fips_in_ref = {g[:2] for g in target_fips if len(g) >= 2}
    target_states = {STATE_FIPS_TO_ABBR[s] for s in state_fips_in_ref if s in STATE_FIPS_TO_ABBR}
    if not target_states:
        raise ValueError(
            "GEOID_reference counties did not map to known states (SD, MN, MT, AZ). "
            "Add state FIPS to STATE_FIPS_TO_ABBR in this script if using other states."
        )

    if not ZIP_TO_COUNTY_CSV.exists():
        raise FileNotFoundError(
            f"ZIP–county file not found: {ZIP_TO_COUNTY_CSV}. "
            "Provide 01_data/reference/zip_to_county_fips.csv (ZIP and county FIPS columns)."
        )
    if not BMF_DIR.exists():
        raise FileNotFoundError(
            f"BMF directory not found: {BMF_DIR}. "
            "Download IRS BMF by state and place CSVs in 01_data/raw/irs_bmf/."
        )

    zip_df = pd.read_csv(ZIP_TO_COUNTY_CSV)
    zip_col = next((c for c in zip_df.columns if "zip" in c.lower()), zip_df.columns[0])
    fips_candidates = [
        c for c in zip_df.columns
        if "fips" in c.lower() or ("county" in c.lower() and "name" not in c.lower())
    ]
    fips_col = fips_candidates[0] if fips_candidates else zip_df.columns[1]
    zip_df["ZIP"] = zip_df[zip_col].astype(str).str.replace(r"\D", "", regex=True).str[:5]
    zip_df["FIPS"] = zip_df[fips_col].astype(str).str.zfill(5)
    zip_to_fips = zip_df[["ZIP", "FIPS"]].drop_duplicates(subset=["ZIP"]).dropna(subset=["FIPS"])

    bmf_files = list(BMF_DIR.glob("*.csv"))
    if not bmf_files:
        raise FileNotFoundError(f"No CSV files in {BMF_DIR}. Download BMF by state from IRS.")

    frames = []
    for path in bmf_files:
        bmf = pd.read_csv(path, dtype=str, low_memory=False)
        for c in ["EIN", "STATE", "ZIP"]:
            if c not in bmf.columns and c.lower() in [x.lower() for x in bmf.columns]:
                bmf = bmf.rename(columns={next(x for x in bmf.columns if x.lower() == c.lower()): c})
        if "EIN" not in bmf.columns or "STATE" not in bmf.columns:
            continue
        bmf = bmf[bmf["STATE"].astype(str).str.upper().isin(target_states)]
        if "ZIP" not in bmf.columns:
            bmf["ZIP"] = ""
        bmf["ZIP"] = bmf["ZIP"].astype(str).str.replace(r"\D", "", regex=True).str[:5]
        bmf = bmf.merge(zip_to_fips, on="ZIP", how="left")
        bmf = bmf[bmf["FIPS"].isin(target_fips)]
        frames.append(bmf[["EIN"]])

    if not frames:
        raise ValueError(
            "No BMF rows matched the benchmark counties. "
            "Check BMF files (EIN, STATE, ZIP) and zip_to_county_fips coverage."
        )

    ein_df = pd.concat(frames, ignore_index=True).drop_duplicates()
    ein_df["EIN"] = ein_df["EIN"].astype(str).apply(_normalize_ein)
    return ein_df[["EIN"]]


def main() -> None:
    print("Building EIN list from GEOID reference, zip-to-county, and BMF...")
    ein_df = build_ein_list()
    EIN_LIST_CSV.parent.mkdir(parents=True, exist_ok=True)
    ein_df.to_csv(EIN_LIST_CSV, index=False)
    print(f"Wrote {len(ein_df)} EINs to {EIN_LIST_CSV}")


if __name__ == "__main__":
    main()
