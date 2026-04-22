"""
Fetch US county GEOIDs (5-digit FIPS) from the Census Bureau API and save to CSV.

Uses the 2020 Decennial Census API (no key required). State FIPS→abbreviation
is fetched from Census state.txt; counties from the Decennial API; ZIPs (ZCTA)
from the Census ZCTA–county relationship file. Builds GEOID = state FIPS (2) + county FIPS (3).

Output columns: GEOID, County, State (abbrev), State_name, State_FIPS, County_FIPS, NAME_full, City, ZIPs (semicolon-separated).
City is the county seat (from an external county-seat list; see COUNTY_SEAT_URL).

Usage:
  All US counties (default):
    python python/ingest/location_processing/fetch_location_data.py
  Only certain states (FIPS codes):
    python python/ingest/location_processing/fetch_location_data.py --states 04 27 30 46
  Custom output path:
    python python/ingest/location_processing/fetch_location_data.py --out path/to/counties.csv

API: https://api.census.gov/data/2020/dec/pl
"""

import argparse
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed
from io import StringIO
from pathlib import Path

import pandas as pd
import requests
from tqdm import tqdm

# Ensure python/ is on path so we can import utils
_SCRIPT_DIR = Path(__file__).resolve()
_PYTHON_DIR = _SCRIPT_DIR.parents[2]
if str(_PYTHON_DIR) not in sys.path:
    sys.path.insert(0, str(_PYTHON_DIR))
from utils.paths import DATA

DEFAULT_OUT = DATA / "reference" / "locations.csv"

CENSUS_API = "https://api.census.gov/data/2020/dec/pl"
# Census state reference: STATE (FIPS) | STUSAB (USPS) | STATE_NAME | STATENS
CENSUS_STATE_REF_URL = "https://www2.census.gov/geo/docs/reference/state.txt"
# Census 2020 ZCTA5 to County (tab-delimited); ZCTA used as ZIP proxy
CENSUS_ZCTA_COUNTY_URL = (
    "https://www2.census.gov/geo/docs/maps-data/data/rel2020/zcta520/"
    "tab20_zcta520_county20_natl.txt"
)
# County seat (city) per county; GEOID = STATEFP + COUNTYFP
COUNTY_SEAT_URL = "https://raw.githubusercontent.com/LucasPuente/Data/master/County%20Seats.csv"


def _fetch_state_fips_to_abbr(session: requests.Session) -> tuple[dict[str, str], dict[str, str]]:
    """Fetch state FIPS (2-digit) to USPS abbr and to full name from Census state.txt.
    Returns (fips_to_abbr, fips_to_name)."""
    r = session.get(CENSUS_STATE_REF_URL, timeout=30)
    r.raise_for_status()
    lines = r.text.strip().splitlines()
    to_abbr = {}
    to_name = {}
    for line in lines:
        line = line.lstrip("#").strip()
        if not line or line.startswith("STATE"):
            continue
        parts = line.split("|")
        if len(parts) >= 3:
            fips = str(parts[0].strip()).zfill(2)
            abbr = parts[1].strip()
            name = parts[2].strip()
            if fips and abbr:
                to_abbr[fips] = abbr
            if fips and name:
                to_name[fips] = name
    return to_abbr, to_name


def _county_name_only(name: str) -> str:
    """e.g. 'Minnehaha County, South Dakota' -> 'Minnehaha'."""
    if not name:
        return ""
    return name.split(",")[0].replace(" County", "").strip()


def _fetch_states(session: requests.Session) -> list[str]:
    """Return list of state FIPS codes (2-digit) from Census API."""
    url = f"{CENSUS_API}?get=NAME&for=state:*"
    r = session.get(url, timeout=30)
    r.raise_for_status()
    data = r.json()
    header = data[0]
    state_idx = header.index("state")
    return [str(row[state_idx]).zfill(2) for row in data[1:]]


def _fetch_counties_for_state(state_fips: str, session: requests.Session) -> list[dict]:
    """Return list of {NAME, state, county} from Census API for one state."""
    url = f"{CENSUS_API}?get=NAME&for=county:*&in=state:{state_fips}"
    r = session.get(url, timeout=30)
    r.raise_for_status()
    data = r.json()
    header = data[0]
    return [dict(zip(header, row)) for row in data[1:]]


def _fetch_all_counties_one_request(session: requests.Session) -> list[dict]:
    """Return all US counties in one API call. Returns [] if API does not support it."""
    url = f"{CENSUS_API}?get=NAME&for=county:*&in=state:*"
    r = session.get(url, timeout=60)
    if not r.ok:
        return []
    data = r.json()
    if not data or len(data) < 2:
        return []
    header = data[0]
    return [dict(zip(header, row)) for row in data[1:]]


def _fetch_county_seats(session: requests.Session) -> pd.DataFrame:
    """Fetch county seat (city) per GEOID from external CSV. Returns DataFrame with GEOID, City (GEOID normalized)."""
    r = session.get(COUNTY_SEAT_URL, timeout=30)
    r.raise_for_status()
    text = r.content.decode("utf-8-sig").strip()
    df = pd.read_csv(StringIO(text), dtype=str)
    if "STATEFP" not in df.columns or "COUNTYFP" not in df.columns:
        raise ValueError(
            f"County seat file missing STATEFP/COUNTYFP. Check {COUNTY_SEAT_URL}."
        )
    seat_col = "County Seat" if "County Seat" in df.columns else df.columns[2]
    # Normalize to 5-digit GEOID: CSV sometimes has 4-digit state (e.g. "0042") so use int to get 2+3
    state_part = pd.to_numeric(df["STATEFP"].astype(str).str.strip(), errors="coerce").fillna(0).astype(int).astype(str).str.zfill(2)
    county_part = pd.to_numeric(df["COUNTYFP"].astype(str).str.strip(), errors="coerce").fillna(0).astype(int).astype(str).str.zfill(3)
    df["GEOID"] = (state_part + county_part).str.strip()
    out = df[["GEOID", seat_col]].rename(columns={seat_col: "City"})
    return out.drop_duplicates(subset=["GEOID"], keep="first")


def _fetch_zcta_county(session: requests.Session) -> pd.DataFrame:
    """Fetch Census ZCTA–county relationship; return DataFrame with ZIP, GEOID (both normalized)."""
    r = session.get(CENSUS_ZCTA_COUNTY_URL, timeout=90)
    r.raise_for_status()
    text = r.content.decode("utf-8-sig").strip()
    df = pd.read_csv(StringIO(text), sep="|", dtype=str, low_memory=False)
    df.columns = df.columns.str.strip()
    # Census file is pipe-delimited. GEOID_ZCTA5_20 = ZCTA (ZIP); GEOID_COUNTY_20 = 5-digit county FIPS.
    zcta_col = next((c for c in df.columns if "GEOID" in c.upper() and "ZCTA5" in c.upper()), None)
    county_col = next((c for c in df.columns if "GEOID" in c.upper() and "COUNTY" in c.upper()), None)
    if zcta_col is None or county_col is None:
        raise ValueError(
            f"Census ZCTA–county file missing ZCTA and/or COUNTY GEOID columns. "
            f"Found: {list(df.columns)[:5]}..."
        )
    # Keep only rows where ZCTA is present (file has many rows with empty ZCTA columns)
    zcta_vals = df[zcta_col].astype(str).str.strip()
    valid = (zcta_vals != "") & (zcta_vals != "nan") & (zcta_vals.str.len() >= 5)
    df = df.loc[valid].copy()
    df["ZIP"] = df[zcta_col].astype(str).str.strip().str[:5]
    df["GEOID"] = df[county_col].astype(str).str.strip().str.zfill(5)
    out = df[["ZIP", "GEOID"]].drop_duplicates().dropna(subset=["GEOID", "ZIP"])
    # Drop invalid ZIPs so groupby later is clean
    out = out.loc[out["ZIP"].str.match(r"^\d{5}$", na=False)]
    return out


def fetch_all_geoids(state_fips_list: list[str] | None = None) -> pd.DataFrame:
    """Fetch county GEOIDs from Census API. If state_fips_list is None, fetch all states."""
    with requests.Session() as session:
        state_fips_to_abbr, state_fips_to_name = _fetch_state_fips_to_abbr(session)

        if state_fips_list is None:
            # Try one API call for all counties first (avoids 50+ per-state calls)
            county_rows = _fetch_all_counties_one_request(session)
            if not county_rows:
                state_fips_list = _fetch_states(session)
                county_rows = []
                with ThreadPoolExecutor(max_workers=8) as ex:
                    future_to_fips = {
                        ex.submit(_fetch_counties_for_state, str(sf).zfill(2), session): sf
                        for sf in state_fips_list
                    }
                    for future in tqdm(as_completed(future_to_fips), total=len(future_to_fips), desc="States", unit="state"):
                        county_rows.extend(future.result())
        else:
            county_rows = []
            with ThreadPoolExecutor(max_workers=min(8, len(state_fips_list))) as ex:
                future_to_fips = {
                    ex.submit(_fetch_counties_for_state, str(sf).zfill(2), session): sf
                    for sf in state_fips_list
                }
                for future in tqdm(as_completed(future_to_fips), total=len(future_to_fips), desc="States", unit="state"):
                    county_rows.extend(future.result())

        rows = []
        for row in county_rows:
            state_fips = str(row.get("state", "")).zfill(2)
            county_fips = row.get("county", "")
            state_key = state_fips if state_fips else ""
            geoid = state_key.zfill(2) + str(county_fips).zfill(3)
            name_full = row.get("NAME", "")
            rows.append({
                "GEOID": geoid,
                "County": _county_name_only(name_full),
                "State": state_fips_to_abbr.get(state_key, state_key),
                "State_name": state_fips_to_name.get(state_key, ""),
                "State_FIPS": state_key,
                "County_FIPS": str(county_fips).zfill(3),
                "NAME_full": name_full,
            })
        df = pd.DataFrame(rows)
        # Normalize GEOID once for all downstream merges
        df["GEOID"] = df["GEOID"].astype(str).str.strip().str.zfill(5)

        # Fetch county seats and ZCTA–county in parallel (independent requests)
        print("Fetching county seats and ZCTA–county (parallel)...")
        seat_df = None
        zip_df = None
        with ThreadPoolExecutor(max_workers=2) as ex:
            f_seats = ex.submit(_fetch_county_seats, session)
            f_zcta = ex.submit(_fetch_zcta_county, session)
            try:
                seat_df = f_seats.result()
            except Exception as e:
                print(f"Warning: could not fetch county seats: {e}. City column will be empty.")
            zip_df = f_zcta.result()
        if seat_df is not None:
            df = df.merge(seat_df[["GEOID", "City"]], on="GEOID", how="left")
        else:
            df["City"] = ""

    our_geoids = set(df["GEOID"])
    zip_in_scope = zip_df.loc[zip_df["GEOID"].isin(our_geoids), ["GEOID", "ZIP"]].drop_duplicates()
    zip_list = (
        zip_in_scope.groupby("GEOID", sort=False)["ZIP"]
        .agg(lambda s: ";".join(sorted(s.unique())))
        .reset_index()
        .rename(columns={"ZIP": "ZIPs"})
    )
    df = df.merge(zip_list, on="GEOID", how="left")
    df = df[
        [
            "GEOID",
            "County",
            "State",
            "State_name",
            "State_FIPS",
            "County_FIPS",
            "NAME_full",
            "City",
            "ZIPs",
        ]
    ]
    return df


def main() -> None:
    parser = argparse.ArgumentParser(description="Fetch US county GEOIDs from Census API")
    parser.add_argument(
        "--states",
        nargs="+",
        metavar="FIPS",
        help="State FIPS codes (e.g. 04 27 30 46). If omitted, fetch all states.",
    )
    parser.add_argument(
        "--out",
        type=Path,
        default=DEFAULT_OUT,
        help=f"Output CSV path (default: {DEFAULT_OUT})",
    )
    args = parser.parse_args()

    state_list = None
    if args.states:
        state_list = [str(s).zfill(2) for s in args.states]
        print(f"Fetching counties for states: {state_list}")
    else:
        print("Fetching all states and counties from Census API...")

    df = fetch_all_geoids(state_list)
    args.out.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(args.out, index=False)
    print(f"Wrote {len(df)} rows to {args.out}")


if __name__ == "__main__":
    main()
