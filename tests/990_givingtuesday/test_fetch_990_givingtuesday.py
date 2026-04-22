"""
Test fetch: request a small amount of curated 990 data from the GivingTuesday
API to verify connectivity, response shape, and that 2021+ data are returned.

Uses a fixed list of 3 EINs (no project EIN list or data folder required).
Writes raw JSON responses to tests/990_givingtuesday/sample_990_api_output/ and a combined
sample CSV to tests/990_givingtuesday/sample_990_api_output/sample_990_basic120.csv.

Run from repo root: python tests/990_givingtuesday/test_fetch_990_givingtuesday.py
"""

import json
import time
from pathlib import Path

import requests

# Same endpoint as python/ingest/990_givingtuesday/api/04_fetch_990_givingtuesday.py
API_BASE = "https://990-infrastructure.gtdata.org"
ENDPOINT = "/irs-data/990basic120fields"
SLEEP_SECONDS = 1.1

# Small fixed set of EINs for test (public charities; used in GivingTuesday examples or well-known).
TEST_EINS = [
    "842929872",   # Example EIN from GivingTuesday API docs
    "530196605",   # Example EIN from GivingTuesday BMF docs
    "941655673",   # Additional sample
]

# Output under this test folder (keep test data out of 01_data)
_THIS_DIR = Path(__file__).resolve().parent
SAMPLE_OUTPUT_DIR = _THIS_DIR / "sample_990_api_output"
SAMPLE_CSV = SAMPLE_OUTPUT_DIR / "sample_990_basic120.csv"

TAX_YEAR_MIN = 2021


def _normalize_ein(ein: str) -> str:
    s = str(ein).strip().replace("-", "").replace(" ", "")
    return s.zfill(9) if len(s) <= 9 else s[:9]


def _fetch_one(ein: str) -> dict:
    url = f"{API_BASE}{ENDPOINT}"
    r = requests.get(url, params={"ein": _normalize_ein(ein)}, timeout=30)
    r.raise_for_status()
    return r.json()


def _results_to_rows(response: dict, ein: str) -> list[dict]:
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
                if int(tax_year) < TAX_YEAR_MIN:
                    continue
            except (TypeError, ValueError):
                pass
        rec = {str(k): v for k, v in rec.items()}
        rec["EIN"] = _normalize_ein(ein)
        rows.append(rec)
    return rows


def main() -> None:
    SAMPLE_OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    all_rows = []
    for ein in TEST_EINS:
        try:
            resp = _fetch_one(ein)
            path = SAMPLE_OUTPUT_DIR / f"ein_{_normalize_ein(ein)}.json"
            with open(path, "w", encoding="utf-8") as f:
                json.dump(resp, f, indent=2, ensure_ascii=False)
            rows = _results_to_rows(resp, ein)
            all_rows.extend(rows)
            body = resp.get("body") or resp
            n = body.get("no_results", 0)
            print(f"EIN {ein}: {n} result(s), {len(rows)} rows with tax year >= {TAX_YEAR_MIN}")
        except requests.RequestException as e:
            print(f"EIN {ein}: request failed - {e}")
        time.sleep(SLEEP_SECONDS)

    if all_rows:
        import csv
        keys = sorted({k for row in all_rows for k in row.keys()})
        with open(SAMPLE_CSV, "w", newline="", encoding="utf-8") as f:
            w = csv.DictWriter(f, fieldnames=keys, extrasaction="ignore")
            w.writeheader()
            w.writerows(all_rows)
        print(f"Sample combined CSV: {len(all_rows)} rows -> {SAMPLE_CSV}")
    else:
        print("No 2021+ records returned. Check API and EINs.")


if __name__ == "__main__":
    main()
