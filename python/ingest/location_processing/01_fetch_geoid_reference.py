"""
Fetch County, State, and ZIPs from Census for the 18 benchmark counties and write GEOID_reference.csv.

The only hardcoded inputs are: (1) the 18 GEOIDs and (2) Cluster_name for each. Everything else
(County, State, Cluster_ID, ZIPs) is derived or fetched from Census (see below).

Where to find GEOID (county FIPS) data online:
  - Census Bureau, ANSI/FIPS code lists (county and county equivalents, 2020):
    https://www.census.gov/library/reference/code-lists/ansi.html → 2020 → County and County Equivalent Entities
  - Census Bureau, all geocodes (state + county + other levels), 2020:
    https://www2.census.gov/programs-surveys/popest/geographies/2020/all-geocodes-v2020.xlsx
  - Census Bureau, Geography identifiers (GEOID) overview:
    https://www.census.gov/programs-surveys/geography/guidance/geo-identifiers.html
  - Census Data API (county list by state, no key required):
    https://api.census.gov/data/2020/dec/pl?get=NAME,state,county&for=county:*&in=state:XX
  - FCC county FIPS (plain text):
    https://transition.fcc.gov/oet/info/maps/census/fips/fips.txt
  County GEOID = 5-digit county FIPS (first 2 digits = state FIPS, last 3 = county FIPS).

Where to get the 18 GEOIDs and Cluster_name for this project (cannot be fetched from a public API):
  The list of 18 counties and their cluster (Black Hills, Sioux Falls, Billings, Flagstaff, Missoula)
  is not available from any public website or API. The sources above list all US counties but do not
  designate "benchmark regions" or assign cluster names. This geography is project/client-defined and
  must be maintained in BENCHMARK_GEOIDS below. Update the dict when the project revises the benchmark set.

Where the rest comes from (all from Census, online):
  - County name, State: Census Bureau 2020 Decennial API, https://api.census.gov/data/2020/dec/pl
  - Cluster_ID: derived in script from Cluster_name (BlackHills=0, SiouxFalls=1, etc.)
  - ZIPs: Census 2020 ZCTA-to-County file,
    https://www2.census.gov/geo/docs/maps-data/data/rel2020/zcta520/tab20_zcta520_county20_natl.txt

Output: GEOID_reference.csv (County, State, GEOID, Cluster_ID, Cluster_name, ZIPs);
        geoid_zip_codes.csv (GEOID, ZIP).

Run from repo root: python python/ingest/location_processing/01_fetch_geoid_reference.py
"""

import os
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

import pandas as pd
import requests

# Ensure python/ is on path so we can import utils
_SCRIPT_DIR = Path(__file__).resolve()
_PYTHON_DIR = _SCRIPT_DIR.parents[2]
if str(_PYTHON_DIR) not in sys.path:
    sys.path.insert(0, str(_PYTHON_DIR))
from utils.paths import DATA

REF_GEOID_CSV = DATA / "reference" / "GEOID_reference.csv"
GEOID_ZIP_CSV = DATA / "reference" / "geoid_zip_codes.csv"

# Census 2020 Decennial PL (county names by state). Optional: set CENSUS_API_KEY env for API path.
CENSUS_API = "https://api.census.gov/data/2020/dec/pl"
CENSUS_API_KEY = os.environ.get("CENSUS_API_KEY", "")
# Fallback when no API key: direct CSV of county FIPS, name, state (no key required).
COUNTY_FIPS_CSV_URL = (
    "https://raw.githubusercontent.com/kjhealy/fips-codes/master/state_and_county_fips_master.csv"
)
# Census 2020 ZCTA5 to County relationship (tab-delimited, no API key)
CENSUS_ZCTA_COUNTY_URL = (
    "https://www2.census.gov/geo/docs/maps-data/data/rel2020/zcta520/"
    "tab20_zcta520_county20_natl.txt"
)
STATE_FIPS_TO_ABBR = {"04": "AZ", "27": "MN", "30": "MT", "46": "SD"}

# Cluster_name -> Cluster_ID (order: Sioux Falls, Billings, Flagstaff, Missoula, Black Hills)
CLUSTER_NAME_TO_ID = {"SiouxFalls": 1, "Billings": 2, "Flagstaff": 3, "Missoula": 4, "BlackHills": 0}

# Minimum hardcoded input: 18 GEOIDs + Cluster_name per GEOID.
# Not available online from any API; project/client-defined (update when benchmark set changes).
# GEOID (5-digit county FIPS) -> Cluster_name
BENCHMARK_GEOIDS = {
    "46099": "SiouxFalls", "46083": "SiouxFalls", "46087": "SiouxFalls",
    "46125": "SiouxFalls", "27133": "SiouxFalls",
    "30009": "Billings", "30095": "Billings", "30111": "Billings",
    "04005": "Flagstaff",
    "30061": "Missoula", "30063": "Missoula",
    "46019": "BlackHills", "46093": "BlackHills", "46081": "BlackHills",
    "46103": "BlackHills", "46033": "BlackHills", "46102": "BlackHills",
    "46047": "BlackHills",
}


def _census_url(params: str) -> str:
    """Build Census API URL with optional API key."""
    url = f"{CENSUS_API}?{params}"
    if CENSUS_API_KEY:
        url += f"&key={CENSUS_API_KEY}"
    return url


def _fetch_counties_for_state(state_fips: str, session: requests.Session | None = None) -> list[dict]:
    """Return list of {NAME, state, county} from Census API for one state."""
    url = _census_url(f"get=NAME,state,county&for=county:*&in=state:{state_fips}")
    r = (session.get(url, timeout=30) if session is not None else requests.get(url, timeout=30))
    r.raise_for_status()
    data = r.json()
    header = data[0]
    rows = data[1:]
    return [dict(zip(header, row)) for row in rows]


def _county_name_only(name: str) -> str:
    """e.g. 'Minnehaha County, South Dakota' -> 'Minnehaha'."""
    if not name:
        return ""
    return name.split(",")[0].replace(" County", "").strip()


def _fetch_county_state_from_census(geoids: set[str], session: requests.Session) -> pd.DataFrame:
    """Fetch County and State from Census API for the given GEOIDs (parallel per state)."""
    state_fips_list = sorted({g[:2] for g in geoids if len(g) >= 2})
    all_rows = []
    with ThreadPoolExecutor(max_workers=min(4, len(state_fips_list))) as ex:
        future_to_state = {
            ex.submit(_fetch_counties_for_state, sf, session): sf for sf in state_fips_list
        }
        for future in as_completed(future_to_state):
            for row in future.result():
                state = row.get("state", "")
                county_fips = row.get("county", "")
                geoid = str(state).zfill(2) + str(county_fips).zfill(3)
                if geoid not in geoids:
                    continue
                all_rows.append({
                    "GEOID": geoid,
                    "County": _county_name_only(row.get("NAME", "")),
                    "State": STATE_FIPS_TO_ABBR.get(state, state),
                })
    return pd.DataFrame(all_rows)


def _fetch_county_state_from_csv(geoids: set[str], session: requests.Session) -> pd.DataFrame:
    """Fetch County and State from public FIPS CSV (no API key). Used when CENSUS_API_KEY is not set."""
    r = session.get(COUNTY_FIPS_CSV_URL, timeout=30)
    r.raise_for_status()
    df = pd.read_csv(pd.io.common.BytesIO(r.content), dtype=str)
    # Expected columns: fips (5-digit or state*1000+county), name, state (abbr)
    fips_col = next((c for c in df.columns if "fips" in c.lower()), df.columns[0])
    name_col = next((c for c in df.columns if "name" in c.lower()), df.columns[1])
    state_col = next((c for c in df.columns if "state" in c.lower()), df.columns[2])
    df["GEOID"] = df[fips_col].astype(str).str.strip().str.zfill(5)
    # Some CSVs use state*1000+county (e.g. 46099); others use 5-digit string
    if df["GEOID"].str.len().max() > 5:
        df["GEOID"] = df["GEOID"].str[-5:]  # last 5 digits
    df = df[df["GEOID"].isin(geoids)].copy()
    df["County"] = df[name_col].astype(str).str.replace(r"\s+County\s*$", "", regex=True).str.strip()
    df["State"] = df[state_col].astype(str).str.strip().str.upper()
    out = df[["GEOID", "County", "State"]].drop_duplicates(subset=["GEOID"], keep="first")
    # FIPS CSVs often use 46113 (Oglala Lakota); project uses 46102 (old Shannon code). Add 46102 if missing.
    missing = geoids - set(out["GEOID"])
    if "46102" in missing:
        out = pd.concat([
            out,
            pd.DataFrame([{"GEOID": "46102", "County": "Oglala Lakota", "State": "SD"}])
        ], ignore_index=True)
    return out


def _fetch_zcta_county_from_census(session: requests.Session) -> pd.DataFrame:
    """Fetch Census ZCTA–county relationship and return DataFrame with ZIP, GEOID."""
    r = session.get(CENSUS_ZCTA_COUNTY_URL, timeout=90)
    r.raise_for_status()
    # Census file is pipe-delimited
    df = pd.read_csv(pd.io.common.BytesIO(r.content), sep="|", dtype=str, low_memory=False)
    need = ["GEOID_ZCTA5_20", "GEOID_COUNTY_20"]
    missing = [c for c in need if c not in df.columns]
    if missing:
        raise ValueError(f"Census ZCTA–county file missing columns: {missing}. Check URL.")
    out = pd.DataFrame()
    out["ZIP"] = df["GEOID_ZCTA5_20"].astype(str).str.strip().str[:5]
    out["GEOID"] = df["GEOID_COUNTY_20"].astype(str).str.strip().str.zfill(5)
    return out[["ZIP", "GEOID"]].drop_duplicates().dropna(subset=["GEOID", "ZIP"])


def main() -> None:
    # -------------------------------------------------------------------------
    # Step 1: Resolve the 18 benchmark GEOIDs (hardcoded in BENCHMARK_GEOIDS).
    # -------------------------------------------------------------------------
    our_geoids = set(BENCHMARK_GEOIDS.keys())
    print(f"[01_fetch_geoid_reference] Starting: building GEOID reference for {len(our_geoids)} benchmark counties.")

    with requests.Session() as session:
        # ---------------------------------------------------------------------
        # Step 2: Fetch County name and State for each GEOID.
        #          Use Census Decennial API if CENSUS_API_KEY is set;
        #          otherwise use public FIPS CSV (no key required).
        # ---------------------------------------------------------------------
        if CENSUS_API_KEY:
            print("[01_fetch_geoid_reference] Fetching county names and state from Census Decennial API...")
            df_census = _fetch_county_state_from_census(our_geoids, session)
        else:
            print("[01_fetch_geoid_reference] Fetching county names and state from FIPS reference CSV (no API key)...")
            df_census = _fetch_county_state_from_csv(our_geoids, session)
        print(f"[01_fetch_geoid_reference] Resolved {len(df_census)} counties: {list(df_census['County'])}.")

        # ---------------------------------------------------------------------
        # Step 3: Attach Cluster_name and Cluster_ID from BENCHMARK_GEOIDS.
        # ---------------------------------------------------------------------
        cluster = [
            {"GEOID": g, "Cluster_name": name, "Cluster_ID": CLUSTER_NAME_TO_ID.get(name, 0)}
            for g, name in BENCHMARK_GEOIDS.items()
        ]
        df_cluster = pd.DataFrame(cluster)
        df = df_census.merge(df_cluster, on="GEOID", how="left")
        print("[01_fetch_geoid_reference] Attached cluster names and IDs.")

        # ---------------------------------------------------------------------
        # Step 4: Fetch Census ZCTA–county relationship; keep only rows for our GEOIDs.
        #          Build semicolon-separated ZIPs per county for GEOID_reference.csv.
        # ---------------------------------------------------------------------
        print("[01_fetch_geoid_reference] Fetching ZCTA–county relationship from Census (pipe-delimited file)...")
        zip_df = _fetch_zcta_county_from_census(session)
        print(f"[01_fetch_geoid_reference] Loaded {len(zip_df):,} ZCTA–county pairs from Census.")

    zip_in_scope = zip_df[zip_df["GEOID"].isin(our_geoids)]
    zip_list = (
        zip_in_scope.groupby("GEOID")["ZIP"]
        .apply(lambda s: ";".join(sorted(s.dropna().astype(str).unique())))
        .reset_index()
        .rename(columns={"ZIP": "ZIPs"})
    )
    df = df.merge(zip_list, on="GEOID", how="left")
    print(f"[01_fetch_geoid_reference] Mapped {len(zip_in_scope):,} (GEOID, ZIP) pairs to the 18 counties; merged ZIPs into reference.")

    # -------------------------------------------------------------------------
    # Step 5: Order columns and rows (match project xlsx order), then write CSVs.
    # -------------------------------------------------------------------------
    df = df[["County", "State", "GEOID", "Cluster_ID", "Cluster_name", "ZIPs"]]
    cluster_order = {0: 4, 1: 0, 2: 1, 3: 2, 4: 3}  # Black Hills last
    df = df.assign(_order=df["Cluster_ID"].map(cluster_order)).sort_values(
        ["_order", "GEOID"]
    ).drop(columns=["_order"]).reset_index(drop=True)

    REF_GEOID_CSV.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(REF_GEOID_CSV, index=False)
    zip_in_scope.to_csv(GEOID_ZIP_CSV, index=False)
    print(f"[01_fetch_geoid_reference] Wrote {REF_GEOID_CSV.name} ({len(df)} rows) and {GEOID_ZIP_CSV.name} ({len(zip_in_scope):,} rows).")
    print(f"[01_fetch_geoid_reference] Done. Output directory: {REF_GEOID_CSV.parent}")


if __name__ == "__main__":
    main()
