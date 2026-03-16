"""
Build the EIN list for benchmark regions for the 990_irs pipeline.

Organizations whose principal address (ZIP) falls in one of the 18 benchmark
counties (from location_processing GEOID reference) are identified using BMF
and the ZIP-to-county crosswalk. Output is reference only; the pipeline filters by geography (03/04), not by this EIN list (no dependency on 990_givingtuesday).

Reads:
  - 01_data/reference/GEOID_reference.csv (from location_processing/01_fetch_geoid_reference.py).
    Fallback: GEOID_reference.xlsx.
  - 01_data/reference/zip_to_county_fips.csv (from location_processing/02_fetch_zip_to_county.py).
  - 01_data/raw/irs_bmf/*.csv (IRS EO BMF by state; from 00_fetch_bmf.py). Expected columns: EIN, STATE, ZIP.

Writes:
  - 01_data/reference/eins_in_benchmark_regions.csv (columns: EIN; optionally Region if in GEOID reference).

Run from repo root:
  python python/ingest/990_irs/build_ein_list.py
"""

import sys
from pathlib import Path

import pandas as pd

# Path setup: ensure python/ is on path for utils.paths
_SCRIPT_DIR = Path(__file__).resolve()
_PYTHON_DIR = _SCRIPT_DIR.parents[2]
if str(_PYTHON_DIR) not in sys.path:
    sys.path.insert(0, str(_PYTHON_DIR))
from utils.paths import DATA

# Output path for the EIN list (reference only; pipeline uses geography filter in 03/04).
EIN_LIST_CSV = DATA / "reference" / "eins_in_benchmark_regions.csv"
# GEOID reference: 18 benchmark counties. Prefer CSV from location_processing 01.
REF_GEOID_CSV = DATA / "reference" / "GEOID_reference.csv"
REF_GEOID_XLSX = DATA / "reference" / "GEOID_reference.xlsx"
# ZIP to county FIPS crosswalk (location_processing 02).
ZIP_TO_COUNTY_CSV = DATA / "reference" / "zip_to_county_fips.csv"
# BMF CSVs by state (990_irs 00_fetch_bmf.py).
BMF_DIR = DATA / "raw" / "irs_bmf"

# Map 2-digit state FIPS (from GEOID) to USPS abbreviation used in BMF STATE column.
# Extend this dict if benchmark regions include other states.
STATE_FIPS_TO_ABBR = {"04": "AZ", "27": "MN", "30": "MT", "46": "SD"}


def _normalize_ein(ein: str) -> str:
    """Return EIN as 9-digit string, zero-padded, digits only."""
    s = str(ein).strip().replace("-", "").replace(" ", "")
    s = "".join(c for c in s if c.isdigit())
    return s.zfill(9) if len(s) <= 9 else s[:9]


def _load_geoid_reference() -> pd.DataFrame:
    """Load GEOID reference (18 benchmark counties). Prefer CSV, fallback to XLSX."""
    if REF_GEOID_CSV.exists():
        ref = pd.read_csv(REF_GEOID_CSV)
        print(f"[build_ein_list] Loaded GEOID reference from {REF_GEOID_CSV.name}.")
    elif REF_GEOID_XLSX.exists():
        ref = pd.read_excel(REF_GEOID_XLSX)
        print(f"[build_ein_list] Loaded GEOID reference from {REF_GEOID_XLSX.name} (CSV not found).")
    else:
        raise FileNotFoundError(
            f"GEOID reference not found at {REF_GEOID_CSV} or {REF_GEOID_XLSX}. "
            "Run location_processing/01_fetch_geoid_reference.py first."
        )
    ref["GEOID"] = ref["GEOID"].astype(str).str.strip().str.zfill(5)
    return ref


def _load_zip_to_fips() -> pd.DataFrame:
    """Load zip_to_county_fips and return DataFrame with ZIP and FIPS (5-digit). One row per ZIP (primary county)."""
    if not ZIP_TO_COUNTY_CSV.exists():
        raise FileNotFoundError(
            f"ZIP-county file not found: {ZIP_TO_COUNTY_CSV}. "
            "Run location_processing/02_fetch_zip_to_county.py first."
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
    out = zip_df[["ZIP", "FIPS"]].drop_duplicates(subset=["ZIP"]).dropna(subset=["FIPS"])
    print(f"[build_ein_list] Loaded ZIP-to-county crosswalk: {len(out):,} ZIPs from {ZIP_TO_COUNTY_CSV.name}.")
    return out


def _find_region_column(ref: pd.DataFrame) -> str | None:
    """Return column name for region/cluster in GEOID reference, or None."""
    for c in ref.columns:
        if c and str(c).lower() in ("region", "cluster_name", "cluster", "benchmark_region", "area"):
            return c
    if "Region" in ref.columns:
        return "Region"
    return None


def build_ein_list() -> pd.DataFrame:
    """
    Build EIN list by filtering BMF to organizations in benchmark counties.

    Uses GEOID reference for target county FIPS, ZIP-to-county for mapping org ZIP
    to county, and BMF for EIN/STATE/ZIP. Keeps rows where org ZIP maps to a
    benchmark GEOID. Optionally attaches Region from GEOID reference.
    """
    # -------------------------------------------------------------------------
    # Step 1: Load GEOID reference (target counties).
    # -------------------------------------------------------------------------
    ref = _load_geoid_reference()
    target_fips = set(ref["GEOID"].astype(str))
    print(f"[build_ein_list] Target: {len(target_fips)} benchmark GEOIDs.")

    # Derive which states to load from BMF (first 2 digits of GEOID = state FIPS).
    state_fips_in_ref = {g[:2] for g in target_fips if len(g) >= 2}
    target_states = {STATE_FIPS_TO_ABBR[s] for s in state_fips_in_ref if s in STATE_FIPS_TO_ABBR}
    if not target_states:
        raise ValueError(
            "GEOID reference counties did not map to known states (AZ, MN, MT, SD). "
            "Add state FIPS to STATE_FIPS_TO_ABBR in this script if using other states."
        )
    print(f"[build_ein_list] BMF states to load: {sorted(target_states)}.")

    # -------------------------------------------------------------------------
    # Step 2: Load ZIP-to-county crosswalk.
    # -------------------------------------------------------------------------
    zip_to_fips = _load_zip_to_fips()

    # -------------------------------------------------------------------------
    # Step 3: Load BMF CSVs and filter to benchmark counties.
    # -------------------------------------------------------------------------
    if not BMF_DIR.exists():
        raise FileNotFoundError(
            f"BMF directory not found: {BMF_DIR}. "
            "Run 00_fetch_bmf.py to download IRS EO BMF by state."
        )
    bmf_files = sorted(BMF_DIR.glob("*.csv"))
    if not bmf_files:
        raise FileNotFoundError(f"No CSV files in {BMF_DIR}. Run 00_fetch_bmf.py first.")

    print(f"[build_ein_list] Found {len(bmf_files)} BMF file(s) in {BMF_DIR.name}.")
    frames = []
    for path in bmf_files:
        bmf = pd.read_csv(path, dtype=str, low_memory=False)
        # Normalize column names to EIN, STATE, ZIP (case-insensitive match).
        for col in ["EIN", "STATE", "ZIP"]:
            if col not in bmf.columns:
                for c in bmf.columns:
                    if c and c.upper() == col:
                        bmf = bmf.rename(columns={c: col})
                        break
        if "EIN" not in bmf.columns or "STATE" not in bmf.columns:
            print(f"[build_ein_list] Skip {path.name}: missing EIN or STATE column.")
            continue
        if "ZIP" not in bmf.columns:
            bmf["ZIP"] = ""
        bmf["ZIP"] = bmf["ZIP"].astype(str).str.replace(r"\D", "", regex=True).str[:5]
        before = len(bmf)
        bmf = bmf[bmf["STATE"].astype(str).str.upper().isin(target_states)]
        print(f"[build_ein_list] {path.name}: {len(bmf):,} rows in target states (from {before:,}).")
        bmf = bmf.merge(zip_to_fips, on="ZIP", how="left")
        bmf = bmf[bmf["FIPS"].isin(target_fips)]
        print(f"[build_ein_list] {path.name}: {len(bmf):,} rows in benchmark counties after ZIP merge.")
        frames.append(bmf[["EIN", "FIPS"]].copy())

    if not frames:
        raise ValueError(
            "No BMF rows matched the benchmark counties. "
            "Check BMF files (EIN, STATE, ZIP) and zip_to_county_fips coverage."
        )

    # -------------------------------------------------------------------------
    # Step 4: Concatenate and normalize EIN. Keep all rows (do not dedupe by EIN).
    # Same EIN can appear in multiple state files or counties; we preserve
    # one row per (EIN, Region) so an org in multiple benchmark regions is
    # represented in each. Downstream (03, 04) uses set(EIN) for filtering.
    # -------------------------------------------------------------------------
    combined = pd.concat(frames, ignore_index=True)
    combined["EIN"] = combined["EIN"].astype(str).apply(_normalize_ein)
    n_rows = len(combined)
    n_unique_eins = combined["EIN"].nunique()
    print(f"[build_ein_list] Combined {sum(len(f) for f in frames):,} rows -> {n_rows:,} rows, {n_unique_eins:,} unique EINs.")

    # -------------------------------------------------------------------------
    # Step 5: Attach Region from GEOID reference. Keep one row per (EIN, Region).
    # -------------------------------------------------------------------------
    region_col = _find_region_column(ref)
    if region_col:
        geoid_to_region = ref[["GEOID", region_col]].drop_duplicates()
        combined = combined.merge(geoid_to_region, left_on="FIPS", right_on="GEOID", how="left")
        combined = combined.rename(columns={region_col: "Region"})
        # Drop only exact duplicate (EIN, Region) pairs; same EIN can appear in multiple regions.
        n_dup = combined[["EIN", "Region"]].duplicated(subset=["EIN", "Region"]).sum()
        if n_dup > 0:
            print(f"[build_ein_list] Duplicate (EIN, Region) rows: {n_dup:,} (removed).")
        ein_df = combined[["EIN", "Region"]].drop_duplicates(subset=["EIN", "Region"])
        print(f"[build_ein_list] Attached Region from GEOID reference ({region_col}); {len(ein_df):,} EIN–region rows.")
    else:
        # No region column; keep all rows (no dedupe by EIN).
        ein_df = combined[["EIN"]].copy()
        ein_df["Region"] = None
        n_dup = ein_df.duplicated(subset=["EIN", "Region"]).sum()
        if n_dup > 0:
            print(f"[build_ein_list] Duplicate (EIN, Region) rows: {n_dup:,} (kept; no dedupe by EIN).")

    out_cols = ["EIN", "Region"] if "Region" in ein_df.columns and ein_df["Region"].notna().any() else ["EIN"]
    return ein_df[out_cols]


def main() -> None:
    print("[build_ein_list] Starting: building EIN list for benchmark regions (GEOID reference + zip_to_county + BMF).")

    ein_df = build_ein_list()

    EIN_LIST_CSV.parent.mkdir(parents=True, exist_ok=True)
    ein_df.to_csv(EIN_LIST_CSV, index=False)
    n_unique = ein_df["EIN"].nunique()
    print(f"[build_ein_list] Wrote {EIN_LIST_CSV.name}: {len(ein_df):,} rows ({n_unique:,} unique EINs).")
    print(f"[build_ein_list] Output path: {EIN_LIST_CSV}")
    print("[build_ein_list] Done.")


if __name__ == "__main__":
    main()
