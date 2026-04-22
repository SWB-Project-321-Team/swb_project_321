"""
Step 01: Fetch GivingTuesday DataMart catalog metadata.

Outputs:
- datamart_catalog_raw.json (full API payload rows)

Run from repo root:
  python python/ingest/990_givingtuesday/datamart/01_fetch_datamart_catalog.py
"""

from __future__ import annotations

import argparse
import json
import time
from pathlib import Path

from common import CATALOG_API_URL, CATALOG_JSON, banner, ensure_dirs, fetch_catalog_documents, load_env_from_secrets, now_utc_iso, print_elapsed


def main() -> None:
    parser = argparse.ArgumentParser(description="Fetch DataMart catalog metadata from GivingTuesday public API.")
    parser.add_argument("--catalog-url", default=CATALOG_API_URL, help="Catalog API URL")
    parser.add_argument("--output-json", default=str(CATALOG_JSON), help="Output path for raw catalog JSON")
    args = parser.parse_args()

    start = time.perf_counter()
    banner("STEP 01 - FETCH DATAMART CATALOG")
    load_env_from_secrets()
    ensure_dirs()

    docs = fetch_catalog_documents(args.catalog_url)
    out = {
        "extracted_at_utc": now_utc_iso(),
        "catalog_url": args.catalog_url,
        "document_count": len(docs),
        "documents": docs,
    }
    out_path = Path(args.output_json)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(json.dumps(out, indent=2), encoding="utf-8")
    print(f"[catalog] Wrote raw JSON: {out_path}", flush=True)
    print_elapsed(start, "Step 01")


if __name__ == "__main__":
    main()
