"""
Step 02: Export DataMart catalog + full field dictionary to CSV/Markdown.

Outputs:
- datamart_catalog.csv
- datamart_catalog.md
- datamart_fields.csv
- datamart_fields.md

Also enriches catalog rows with URL health and Content-Length bytes.
"""

from __future__ import annotations

import argparse
import json
import time
from pathlib import Path

from tqdm import tqdm

from common import (
    CATALOG_CSV,
    CATALOG_JSON,
    CATALOG_MD,
    FIELDS_CSV,
    FIELDS_MD,
    banner,
    ensure_dirs,
    fetch_catalog_documents,
    fetch_remote_header_columns,
    flatten_catalog_documents,
    load_env_from_secrets,
    now_utc_iso,
    print_elapsed,
    probe_url_head,
    write_catalog_markdown,
    write_csv,
    write_fields_markdown,
)


def _load_or_fetch_documents(raw_json_path: Path, refresh: bool) -> list[dict]:
    """Load catalog docs from raw JSON if present; otherwise fetch from API."""
    if raw_json_path.exists() and not refresh:
        payload = json.loads(raw_json_path.read_text(encoding="utf-8"))
        docs = payload.get("documents", [])
        print(f"[catalog] Loaded {len(docs)} docs from {raw_json_path}", flush=True)
        return docs
    docs = fetch_catalog_documents()
    payload = {
        "extracted_at_utc": now_utc_iso(),
        "catalog_url": "default",
        "document_count": len(docs),
        "documents": docs,
    }
    raw_json_path.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    print(f"[catalog] Saved refreshed raw JSON: {raw_json_path}", flush=True)
    return docs


def main() -> None:
    parser = argparse.ArgumentParser(description="Export DataMart catalog and fields dictionary.")
    parser.add_argument("--raw-json", default=str(CATALOG_JSON), help="Raw catalog JSON file")
    parser.add_argument("--catalog-csv", default=str(CATALOG_CSV), help="Output catalog CSV")
    parser.add_argument("--catalog-md", default=str(CATALOG_MD), help="Output catalog markdown")
    parser.add_argument("--fields-csv", default=str(FIELDS_CSV), help="Output field dictionary CSV")
    parser.add_argument("--fields-md", default=str(FIELDS_MD), help="Output field dictionary markdown")
    parser.add_argument("--refresh-catalog", action="store_true", help="Force refresh catalog from API")
    args = parser.parse_args()

    start = time.perf_counter()
    banner("STEP 02 - EXPORT DATA DICTIONARY (CSV + MD)")
    load_env_from_secrets()
    ensure_dirs()

    raw_json_path = Path(args.raw_json)
    catalog_csv_path = Path(args.catalog_csv)
    catalog_md_path = Path(args.catalog_md)
    fields_csv_path = Path(args.fields_csv)
    fields_md_path = Path(args.fields_md)

    docs = _load_or_fetch_documents(raw_json_path, args.refresh_catalog)
    rows = flatten_catalog_documents(docs)
    print(f"[catalog] Flattened {len(rows)} row(s).", flush=True)

    # URL metadata probes (status and content-length).
    print("[catalog] Probing dataset URLs for status and source size...", flush=True)
    for row in tqdm(rows, desc="Probe URLs", unit="dataset"):
        url = row.get("download_url", "")
        if url:
            status_code, content_len, status_text = probe_url_head(url)
        else:
            status_code, content_len, status_text = (None, None, "missing_url")
        row["url_status_code"] = "" if status_code is None else str(status_code)
        row["source_content_length_bytes"] = "" if content_len is None else str(content_len)
        row["url_status_text"] = status_text

    catalog_fields = [
        "dataset_id",
        "title",
        "form_type",
        "category",
        "part",
        "size_display",
        "last_updated",
        "download_url",
        "dataset_documentation",
        "url_status_code",
        "source_content_length_bytes",
        "url_status_text",
        "source_api_id",
        "extracted_at_utc",
    ]
    write_csv(catalog_csv_path, rows, catalog_fields)
    write_catalog_markdown(catalog_md_path, rows)

    # Build full field dictionary by reading only remote headers.
    field_rows: list[dict[str, str]] = []
    print("[fields] Fetching remote header columns for all catalog datasets...", flush=True)
    for row in tqdm(rows, desc="Fetch headers", unit="dataset"):
        url = row.get("download_url", "")
        if not url:
            continue
        try:
            cols = fetch_remote_header_columns(url)
        except Exception as e:
            print(f"[fields] WARNING: header read failed for {row.get('title')} ({url}): {e}", flush=True)
            cols = []
        extracted_at = now_utc_iso()
        for idx, col in enumerate(cols, start=1):
            field_rows.append(
                {
                    "dataset_id": row.get("dataset_id", ""),
                    "title": row.get("title", ""),
                    "form_type": row.get("form_type", ""),
                    "category": row.get("category", ""),
                    "part": row.get("part", ""),
                    "download_url": url,
                    "ordinal": str(idx),
                    "column_name": col,
                    "extracted_at_utc": extracted_at,
                }
            )

    field_cols = [
        "dataset_id",
        "title",
        "form_type",
        "category",
        "part",
        "download_url",
        "ordinal",
        "column_name",
        "extracted_at_utc",
    ]
    write_csv(fields_csv_path, field_rows, field_cols)
    write_fields_markdown(fields_md_path, field_rows)

    # Friendly summary for terminal logs.
    print(f"[summary] Catalog rows: {len(rows)}", flush=True)
    print(f"[summary] Field rows:   {len(field_rows)}", flush=True)
    print_elapsed(start, "Step 02")


if __name__ == "__main__":
    main()
