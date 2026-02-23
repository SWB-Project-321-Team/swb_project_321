"""
Fetch curated Form 990 data (Basic 120 fields) from GivingTuesday API for
organizations in the benchmark regions, tax years 2021–present.

Implements the API described at:
  https://990data.givingtuesday.org/asset-bank/

Region filter: only EINs that appear in the project EIN list (orgs in the 18
county benchmark regions). The EIN list must exist at
01_data/reference/eins_in_benchmark_regions.csv with at least an "EIN"
column (9 digits, leading zeros optional). To create that file, run the
steps in docs/990_data_fetch_plan.md (run 01_, 02_, 03_ in python/ingest/990_givingtuesday/).
Optionally, if the EIN list file is missing but BMF and zip-to-county files
are present, the script will try to build the EIN list (see _build_ein_list_from_bmf).

API (per GivingTuesday docs):
  - Base: https://990-infrastructure.gtdata.org
  - Endpoint: GET /irs-data/990basic120fields?ein=XXXXXXXXX (9 digits, no hyphen; param lowercase "ein")
  - Rate limit: configurable (REQUESTS_PER_MINUTE_LIMIT); 403 workaround: retry after cooldown, optional batch pauses.
  - Response: JSON with body.query, body.no_results, body.results

Output:
  - Raw JSON per request: 01_data/raw/givingtuesday_990/api_responses/
  - Combined CSV (2021–present only): 01_data/raw/givingtuesday_990/990_basic120_combined.csv

By default, EINs that already have a saved response file are skipped (resume).
Use --force to re-fetch all EINs from the API.

Run from repo root: python python/ingest/990_givingtuesday/04_fetch_990_givingtuesday.py
For immediate output in terminal/IDE, use: python -u python/ingest/990_givingtuesday/04_fetch_990_givingtuesday.py
"""

import argparse
import json
import random
import sys
import time
from pathlib import Path

import pandas as pd
import requests

# Unbuffered stdout so progress and errors show immediately when run from IDE/terminal.
if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(line_buffering=True)

# ── File paths (repo root = 4 levels up: 990_givingtuesday -> ingest -> python -> root) ─────────
BASE = Path(__file__).resolve().parent.parent.parent.parent
DATA = BASE / "data" / "321_Black_Hills_Area_Community_Foundation_2025_08" / "01_data"

EIN_LIST_CSV = DATA / "reference" / "eins_in_benchmark_regions.csv"
REF_GEOID_XLSX = DATA / "reference" / "GEOID_reference.xlsx"
ZIP_TO_COUNTY_CSV = DATA / "reference" / "zip_to_county_fips.csv"
BMF_DIR = DATA / "raw" / "irs_bmf"

RAW_990_DIR = DATA / "raw" / "givingtuesday_990"
API_RESPONSES_DIR = RAW_990_DIR / "api_responses"
COMBINED_CSV = RAW_990_DIR / "990_basic120_combined.csv"

API_BASE = "https://990-infrastructure.gtdata.org"
ENDPOINT = "/irs-data/990basic120fields"
# Rate and 403 workarounds (403s often start after ~50–90 requests; use conservative settings).
REQUESTS_PER_MINUTE_LIMIT = 24  # Conservative; increase only if 403s stop.
SLEEP_SECONDS = 60.0 / REQUESTS_PER_MINUTE_LIMIT + 0.15  # 60 = sec/min → seconds between requests
COOLDOWN_AFTER_403_SECONDS = 30  # 30 sec wait after 403 before retry.
MAX_403_RETRIES = 2  # Retry this many times after 403 (3 attempts total per EIN).
BATCH_SIZE = 0  # After this many API calls, long pause (0 = disabled).
BATCH_COOLDOWN_SECONDS = 600  # 10 min pause after each batch.
STUTTER_MAX_SECONDS = 2.0  # Add 0–N sec random jitter to each delay so requests aren't perfectly regular.
MAX_TRANSIENT_RETRIES = 3  # Retry 429/5xx/timeout/connection this many times with backoff.
TRANSIENT_BACKOFF_BASE_SECONDS = 5  # First backoff 5s, then 10s, 20s (exponential).
USER_AGENT = "SWB-321-990-Research/1.0 (benchmark-region 990 fetch; contact via GivingTuesday feedback if issues)"
# 2-digit state FIPS (from GEOID) -> USPS state abbreviation for BMF filter.
STATE_FIPS_TO_ABBR = {"04": "AZ", "27": "MN", "30": "MT", "46": "SD"}

# Tax years to keep (inclusive). Only rows with TAXYEAR in this range are written to combined CSV.
TAX_YEAR_MIN = 2021


def _normalize_ein(ein: str) -> str:
    """Return EIN as 9-digit string, zero-padded, no hyphen (per GivingTuesday API)."""
    s = str(ein).strip().replace("-", "").replace(" ", "")
    return s.zfill(9) if len(s) <= 9 else s[:9]


def _load_ein_list() -> pd.DataFrame:
    """Load EIN list from reference CSV. Must have column EIN (or ein)."""
    if not EIN_LIST_CSV.exists():
        return _build_ein_list_from_bmf()
    df = pd.read_csv(EIN_LIST_CSV)
    col = "EIN" if "EIN" in df.columns else "ein"
    if col not in df.columns:
        raise ValueError(f"EIN list must have column 'EIN' or 'ein': {EIN_LIST_CSV}")
    df["EIN"] = df[col].astype(str).apply(_normalize_ein)
    return df[["EIN"]].drop_duplicates()


def _build_ein_list_from_bmf() -> pd.DataFrame:
    """
    Build EIN list by filtering BMF to the 18 benchmark-region counties.
    Requires: GEOID_reference.xlsx, zip_to_county_fips.csv, and BMF CSVs
    (by state) in 01_data/raw/irs_bmf/ with columns EIN, STATE, ZIP.
    Saves the result to eins_in_benchmark_regions.csv and returns it.
    """
    if not REF_GEOID_XLSX.exists():
        raise FileNotFoundError(
            f"EIN list not found at {EIN_LIST_CSV}. Create it per docs/990_data_fetch_plan.md, "
            "or provide GEOID_reference.xlsx and BMF + zip-to-county to build it."
        )
    ref = pd.read_excel(REF_GEOID_XLSX)
    ref["GEOID"] = ref["GEOID"].astype(str).str.zfill(5)
    target_fips = set(ref["GEOID"].astype(str))
    # Only load BMF for states that appear in the reference (reduces memory and I/O).
    state_fips_in_ref = {g[:2] for g in target_fips if len(g) >= 2}
    target_states = {STATE_FIPS_TO_ABBR[s] for s in state_fips_in_ref if s in STATE_FIPS_TO_ABBR}
    if not target_states:
        raise ValueError(
            "GEOID_reference counties did not map to known states (SD, MN, MT, AZ). "
            "Add state FIPS to STATE_FIPS_TO_ABBR if using other states."
        )

    if not ZIP_TO_COUNTY_CSV.exists() or not BMF_DIR.exists():
        raise FileNotFoundError(
            f"EIN list not found at {EIN_LIST_CSV}. To build it from BMF, provide "
            f"{ZIP_TO_COUNTY_CSV} and BMF CSVs in {BMF_DIR}."
        )

    zip_df = pd.read_csv(ZIP_TO_COUNTY_CSV)
    zip_col = next((c for c in zip_df.columns if "zip" in c.lower()), zip_df.columns[0])
    fips_candidates = [c for c in zip_df.columns if "fips" in c.lower() or ("county" in c.lower() and "name" not in c.lower())]
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
        raise ValueError("No BMF rows matched the 18 counties. Check BMF files and zip-to-county.")
    ein_df = pd.concat(frames, ignore_index=True).drop_duplicates()
    ein_df["EIN"] = ein_df["EIN"].astype(str).apply(_normalize_ein)
    EIN_LIST_CSV.parent.mkdir(parents=True, exist_ok=True)
    ein_df.to_csv(EIN_LIST_CSV, index=False)
    print(f"Built EIN list from BMF: {len(ein_df)} EINs saved to {EIN_LIST_CSV}", flush=True)
    return ein_df


def _fetch_one(ein: str, session: requests.Session | None = None) -> dict:
    """GET 990 basic 120 fields for one EIN. Returns parsed JSON (statusCode, body, ...)."""
    url = f"{API_BASE}{ENDPOINT}"
    params = {"ein": _normalize_ein(ein)}
    if session is not None:
        r = session.get(url, params=params, timeout=30)
    else:
        r = requests.get(url, params=params, timeout=30)
    r.raise_for_status()
    return r.json()


def _response_path(ein: str) -> Path:
    """Path to saved raw JSON for one EIN."""
    return API_RESPONSES_DIR / f"ein_{_normalize_ein(ein)}.json"


def _load_saved_response(ein: str) -> dict | None:
    """Load existing raw JSON from disk if present. Returns None if file missing."""
    path = _response_path(ein)
    if not path.exists():
        return None
    try:
        with open(path, encoding="utf-8") as f:
            return json.load(f)
    except (json.JSONDecodeError, OSError):
        return None


def _save_raw_response(ein: str, response: dict) -> None:
    """Write raw JSON for one EIN to api_responses/ein_{ein}.json. Dir must exist."""
    with open(_response_path(ein), "w", encoding="utf-8") as f:
        json.dump(response, f, indent=2, ensure_ascii=False)


def _results_to_rows(response: dict, ein: str) -> list[dict]:
    """Extract body.results into a list of flat dicts; filter to TAXYEAR >= TAX_YEAR_MIN."""
    try:
        body = response.get("body") or response
        results = body.get("results") or []
        if isinstance(results, dict):
            results = [results]
    except Exception:
        return []
    rows = []
    for rec in results:
        if not isinstance(rec, dict):
            continue
        tax_year = rec.get("TAXYEAR") or rec.get("TAX_YEAR") or rec.get("tax_year")
        if tax_year is not None:
            try:
                y = int(tax_year)
                if y < TAX_YEAR_MIN:
                    continue
            except (TypeError, ValueError):
                pass
        rec = {str(k): v for k, v in rec.items()}
        rec["EIN"] = _normalize_ein(ein)
        rows.append(rec)
    return rows


def main() -> None:
    parser = argparse.ArgumentParser(description="Fetch Form 990 (Basic 120) from GivingTuesday API for benchmark-region EINs.")
    parser.add_argument(
        "--force",
        action="store_true",
        help="Re-fetch all EINs from the API even if a response file already exists (no resume).",
    )
    args = parser.parse_args()

    ein_df = _load_ein_list()
    eins = ein_df["EIN"].tolist()
    print(f"Loaded {len(eins)} EINs from benchmark regions.", flush=True)

    RAW_990_DIR.mkdir(parents=True, exist_ok=True)
    API_RESPONSES_DIR.mkdir(parents=True, exist_ok=True)

    if args.force:
        print("  Force rerun: will re-fetch all EINs from the API.", flush=True)
        need_fetch = len(eins)
        print(f"  {need_fetch} EINs will be requested from API.", flush=True)
    else:
        already = sum(1 for e in eins if _response_path(e).exists())
        need_fetch = len(eins) - already
        if already:
            print(f"  Resuming: {already} EINs already have saved responses; will skip API for those.", flush=True)
        if need_fetch:
            print(f"  {need_fetch} EINs need API request.", flush=True)

    all_rows = []
    n_skipped = 0
    n_fetched = 0
    n_403 = 0
    api_calls_this_batch = 0
    with requests.Session() as session:
        session.headers.update({"User-Agent": USER_AGENT, "Accept": "application/json"})
        for i, ein in enumerate(eins):
            saved = None if args.force else _load_saved_response(ein)
            if saved is not None:
                rows = _results_to_rows(saved, ein)
                all_rows.extend(rows)
                n_skipped += 1
            else:
                last_error = None
                n_403_retries = 0
                n_transient_retries = 0
                done = False
                while not done:
                    try:
                        print(f"  Requesting EIN {ein}...", flush=True)
                        resp = _fetch_one(ein, session)
                        _save_raw_response(ein, resp)
                        rows = _results_to_rows(resp, ein)
                        all_rows.extend(rows)
                        n_fetched += 1
                        api_calls_this_batch += 1
                        print(f"  Saved EIN {ein}.", flush=True)
                        delay = SLEEP_SECONDS + random.uniform(0, STUTTER_MAX_SECONDS)
                        time.sleep(delay)
                        if BATCH_SIZE and api_calls_this_batch >= BATCH_SIZE:
                            print(f"  Batch of {BATCH_SIZE} API calls done; pausing {BATCH_COOLDOWN_SECONDS // 60} min...", flush=True)
                            time.sleep(BATCH_COOLDOWN_SECONDS)
                            api_calls_this_batch = 0
                        done = True
                    except requests.HTTPError as e:
                        last_error = e
                        status = e.response.status_code if e.response is not None else None
                        if status == 403:
                            n_403 += 1
                            n_403_retries += 1
                            if n_403_retries <= MAX_403_RETRIES:
                                wait_min = COOLDOWN_AFTER_403_SECONDS // 60
                                print(f"  ERROR 403 for EIN {ein}; waiting {wait_min} min then retry ({n_403_retries}/{MAX_403_RETRIES})...", flush=True)
                                time.sleep(COOLDOWN_AFTER_403_SECONDS)
                            else:
                                print(f"  WARNING: EIN {ein}: 403 Forbidden (retries exhausted)", flush=True)
                                last_error = None
                                done = True
                        elif status in (429, 500, 502, 503) and n_transient_retries < MAX_TRANSIENT_RETRIES:
                            n_transient_retries += 1
                            backoff = TRANSIENT_BACKOFF_BASE_SECONDS * (2 ** (n_transient_retries - 1))
                            print(f"  ERROR {status} for EIN {ein}; backoff {backoff}s then retry ({n_transient_retries}/{MAX_TRANSIENT_RETRIES})...", flush=True)
                            time.sleep(backoff)
                        else:
                            if status in (429, 500, 502, 503):
                                print(f"  WARNING: EIN {ein}: {e} (transient retries exhausted)", flush=True)
                            else:
                                print(f"  WARNING: EIN {ein}: {e}", flush=True)
                            done = True
                    except (requests.ConnectionError, requests.Timeout) as e:
                        last_error = e
                        if n_transient_retries < MAX_TRANSIENT_RETRIES:
                            n_transient_retries += 1
                            backoff = TRANSIENT_BACKOFF_BASE_SECONDS * (2 ** (n_transient_retries - 1))
                            print(f"  ERROR {type(e).__name__} for EIN {ein}; backoff {backoff}s then retry ({n_transient_retries}/{MAX_TRANSIENT_RETRIES})...", flush=True)
                            time.sleep(backoff)
                        else:
                            print(f"  WARNING: EIN {ein}: {e} (retries exhausted)", flush=True)
                            done = True
                    except requests.RequestException as e:
                        last_error = e
                        print(f"  WARNING: EIN {ein}: {e}", flush=True)
                        done = True
                    except Exception as e:
                        last_error = e
                        print(f"  WARNING: EIN {ein}: {e}", flush=True)
                        done = True
            if (i + 1) % 50 == 0:
                print(f"  Progress: {i + 1}/{len(eins)} EINs (fetched {n_fetched}, skipped {n_skipped}, 403s {n_403})", flush=True)

    print(f"  Done: fetched {n_fetched}, skipped {n_skipped}, 403s {n_403} (total {len(eins)} EINs).", flush=True)

    if not all_rows:
        print("No 990 records (2021–present) returned. Check API and EIN list.", flush=True)
        return

    out = pd.DataFrame(all_rows)
    # Enforce 2021+ if TAXYEAR present
    if "TAXYEAR" in out.columns:
        out["TAXYEAR"] = pd.to_numeric(out["TAXYEAR"], errors="coerce")
        out = out[out["TAXYEAR"].ge(TAX_YEAR_MIN)]
    out.to_csv(COMBINED_CSV, index=False)
    print(f"Combined CSV ({out.shape[0]} rows, {out.shape[1]} columns) saved to:\n  {COMBINED_CSV}", flush=True)

    # Manifest for raw folder (per 990_data_fetch_plan.md)
    manifest_path = RAW_990_DIR / "README.md"
    manifest_lines = [
        "# GivingTuesday 990 raw data",
        "",
        f"- **Source:** GivingTuesday API, Basic 120 Fields",
        f"- **Endpoint:** {API_BASE}{ENDPOINT}",
        f"- **Fetch date:** " + time.strftime("%Y-%m-%d"),
        f"- **EIN list:** {EIN_LIST_CSV}",
        f"- **EINs requested:** {len(eins)}",
        f"- **Combined CSV rows (tax year >= {TAX_YEAR_MIN}):** {out.shape[0]}",
        f"- **Raw JSON responses:** {API_RESPONSES_DIR}",
        "",
        "Do not modify or overwrite files in this folder.",
    ]
    manifest_path.write_text("\n".join(manifest_lines), encoding="utf-8")
    print(f"Manifest written to {manifest_path}", flush=True)


if __name__ == "__main__":
    main()
