"""
Download a ZIP (ZCTA) to county FIPS crosswalk and save as the project reference file.

Uses the Census Bureau 2020 ZCTA-to-County relationship file (public, no auth; pipe-delimited).
ZCTA5 is used as a proxy for 5-digit ZIP. When a ZCTA spans multiple counties,
the county with the largest overlapping land area is chosen (one row per ZIP).

Reads: none (downloads from URL).
Writes: 01_data/reference/zip_to_county_fips.csv (columns: ZIP, FIPS)

Run from repo root: python python/ingest/location_processing/02_fetch_zip_to_county.py

Alternative: To use HUD USPS Crosswalk instead (ZIP–County), download the file from
https://www.huduser.gov/portal/datasets/usps_crosswalk.html, then run with
--local path/to/downloaded.csv to normalize and copy to reference.
"""

import argparse
import sys
from pathlib import Path

import pandas as pd
import requests

# Ensure python/ is on path so we can import utils
_SCRIPT_DIR = Path(__file__).resolve()
_PYTHON_DIR = _SCRIPT_DIR.parents[2]  # location_processing -> ingest -> python
if str(_PYTHON_DIR) not in sys.path:
    sys.path.insert(0, str(_PYTHON_DIR))
from utils.paths import DATA

ZIP_TO_COUNTY_CSV = DATA / "reference" / "zip_to_county_fips.csv"

# Census 2020 ZCTA5 to County relationship (pipe-delimited)
CENSUS_ZCTA_COUNTY_URL = (
    "https://www2.census.gov/geo/docs/maps-data/data/rel2020/zcta520/"
    "tab20_zcta520_county20_natl.txt"
)


def _download_census() -> pd.DataFrame:
    """Download Census ZCTA–county relationship file (pipe-delimited); return raw DataFrame."""
    r = requests.get(CENSUS_ZCTA_COUNTY_URL, timeout=90)
    r.raise_for_status()
    text = r.content.decode("utf-8-sig").strip()
    df = pd.read_csv(pd.io.common.StringIO(text), sep="|", dtype=str, low_memory=False)
    df.columns = df.columns.str.strip()
    return df


def _census_to_zip_fips(df: pd.DataFrame) -> pd.DataFrame:
    """Convert Census ZCTA–county table to one row per ZIP; assign FIPS = county with largest overlapping land area."""
    zcta_col = next((c for c in df.columns if "GEOID" in c.upper() and "ZCTA5" in c.upper()), None)
    county_col = next((c for c in df.columns if "GEOID" in c.upper() and "COUNTY" in c.upper()), None)
    area_col = next((c for c in df.columns if "AREALAND_PART" in c.upper()), None)
    if not zcta_col or not county_col or not area_col:
        raise ValueError(
            f"Census file missing ZCTA, COUNTY GEOID, or AREALAND_PART columns. Found: {list(df.columns)[:8]}..."
        )
    zcta_vals = df[zcta_col].astype(str).str.strip()
    valid = (zcta_vals != "") & (zcta_vals != "nan") & (zcta_vals.str.len() >= 5)
    df = df.loc[valid].copy()
    df["ZIP"] = df[zcta_col].astype(str).str.strip().str[:5]
    df["FIPS"] = df[county_col].astype(str).str.strip().str.zfill(5)
    df["area"] = pd.to_numeric(df[area_col], errors="coerce").fillna(0)
    idx = df.groupby("ZIP")["area"].idxmax()
    out = df.loc[idx, ["ZIP", "FIPS"]].drop_duplicates().sort_values("ZIP").reset_index(drop=True)
    return out


def _normalize_local_csv(path: Path) -> pd.DataFrame:
    """Normalize a local CSV (e.g. from HUD) to ZIP and FIPS columns."""
    df = pd.read_csv(path, dtype=str, low_memory=False)
    zip_col = next((c for c in df.columns if "zip" in c.lower()), df.columns[0])
    fips_candidates = [
        c for c in df.columns
        if "fips" in c.lower() or ("county" in c.lower() and "name" not in c.lower())
    ]
    fips_col = fips_candidates[0] if fips_candidates else df.columns[1]
    out = pd.DataFrame()
    out["ZIP"] = df[zip_col].astype(str).str.replace(r"\D", "", regex=True).str[:5]
    out["FIPS"] = df[fips_col].astype(str).str.zfill(5)
    out = out.drop_duplicates(subset=["ZIP"]).dropna(subset=["FIPS"]).sort_values("ZIP")
    return out


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Fetch or normalize ZIP–county crosswalk to 01_data/reference/zip_to_county_fips.csv"
    )
    parser.add_argument(
        "--local",
        type=Path,
        metavar="FILE",
        help="Use a local CSV instead of Census (e.g. HUD ZIP–County). Must have zip and county/FIPS columns.",
    )
    parser.add_argument(
        "--source",
        choices=["census", "local"],
        default="census",
        help="Source: census (default) or local (requires --local FILE).",
    )
    args = parser.parse_args()

    print("[02_fetch_zip_to_county] Starting: building ZIP → county FIPS crosswalk.")

    if args.source == "local" or args.local is not None:
        # ---------------------------------------------------------------------
        # Local CSV: normalize to ZIP and FIPS columns, one row per ZIP.
        # ---------------------------------------------------------------------
        path = args.local
        if path is None:
            raise SystemExit("--local FILE is required when using a local file.")
        path = Path(path).resolve()
        if not path.exists():
            raise SystemExit(f"File not found: {path}")
        print(f"[02_fetch_zip_to_county] Using local file: {path}")
        out = _normalize_local_csv(path)
        print(f"[02_fetch_zip_to_county] Normalized to ZIP and FIPS columns: {len(out):,} rows (one per ZIP).")
    else:
        # ---------------------------------------------------------------------
        # Census: download ZCTA–county file, then pick one county per ZIP (largest overlap).
        # ---------------------------------------------------------------------
        print(f"[02_fetch_zip_to_county] Downloading Census ZCTA–county file from {CENSUS_ZCTA_COUNTY_URL}...")
        df = _download_census()
        print(f"[02_fetch_zip_to_county] Downloaded {len(df):,} raw rows. Selecting one county per ZIP (largest land overlap)...")
        out = _census_to_zip_fips(df)
        print(f"[02_fetch_zip_to_county] Result: {len(out):,} rows (one ZIP → one FIPS per row).")

    # -------------------------------------------------------------------------
    # Write crosswalk to project reference directory.
    # -------------------------------------------------------------------------
    ZIP_TO_COUNTY_CSV.parent.mkdir(parents=True, exist_ok=True)
    out.to_csv(ZIP_TO_COUNTY_CSV, index=False)
    print(f"[02_fetch_zip_to_county] Wrote {ZIP_TO_COUNTY_CSV.name} ({len(out):,} rows) to {ZIP_TO_COUNTY_CSV.parent}.")
    print("[02_fetch_zip_to_county] Done.")


if __name__ == "__main__":
    main()
