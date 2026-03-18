"""
Step 03: Download required full raw DataMart files locally.

Required datasets:
- Combined Forms Datamart
- Basic Fields (990, 990EZ, 990PF)

Outputs:
- raw CSV files under 01_data/raw/givingtuesday_990/datamarts/raw/
- required_datasets_manifest.csv in metadata folder
"""

from __future__ import annotations

import argparse
import csv
import time
from pathlib import Path

from tqdm import tqdm

from common import (
    CATALOG_CSV,
    RAW_DIR,
    REQUIRED_MANIFEST_CSV,
    banner,
    download_with_progress,
    ensure_dirs,
    load_catalog_rows,
    load_env_from_secrets,
    print_elapsed,
    select_required_datasets,
    url_basename,
)


def main() -> None:
    parser = argparse.ArgumentParser(description="Download required DataMart raw CSV files.")
    parser.add_argument("--catalog-csv", default=str(CATALOG_CSV), help="Catalog CSV path from step 02")
    parser.add_argument("--raw-dir", default=str(RAW_DIR), help="Local raw output directory")
    parser.add_argument("--manifest-csv", default=str(REQUIRED_MANIFEST_CSV), help="Manifest output path")
    parser.add_argument("--overwrite", action="store_true", help="Re-download files even if local file exists")
    args = parser.parse_args()

    start = time.perf_counter()
    banner("STEP 03 - DOWNLOAD REQUIRED RAW DATAMART FILES")
    load_env_from_secrets()
    ensure_dirs()

    raw_dir = Path(args.raw_dir)
    raw_dir.mkdir(parents=True, exist_ok=True)
    manifest_path = Path(args.manifest_csv)

    rows = load_catalog_rows(Path(args.catalog_csv))
    required = select_required_datasets(rows)
    print(f"[download] Required datasets resolved: {len(required)}", flush=True)

    manifest_rows: list[dict[str, str]] = []
    for row in tqdm(required, desc="Required datasets", unit="dataset"):
        title = row.get("title", "")
        form = row.get("form_type", "")
        url = row.get("download_url", "")
        expected = row.get("source_content_length_bytes", "")
        expected_bytes = int(expected) if expected and expected.isdigit() else None
        if not url:
            raise RuntimeError(f"Missing download_url for required dataset: {title} [{form}]")
        filename = url_basename(url)
        local_path = raw_dir / filename
        print(f"\n[download] Dataset: {title} [{form}] -> {filename}", flush=True)
        print(f"[download] Source URL: {url}", flush=True)
        print(f"[download] Expected bytes: {expected_bytes if expected_bytes is not None else 'unknown'}", flush=True)

        if local_path.exists() and not args.overwrite:
            local_bytes = local_path.stat().st_size
            print(f"[download] Local file exists, skip download: {local_path} ({local_bytes} bytes)", flush=True)
        else:
            local_bytes = download_with_progress(url, local_path, expected_bytes=expected_bytes)
            print(f"[download] Completed: {local_path} ({local_bytes} bytes)", flush=True)

        # Strict local size check against source Content-Length if available.
        if expected_bytes is None:
            raise RuntimeError(
                f"Strict size validation requires source_content_length_bytes; missing for {title} [{form}] {url}"
            )
        if local_bytes != expected_bytes:
            raise RuntimeError(
                f"Size mismatch after download for {filename}: expected={expected_bytes}, local={local_bytes}"
            )
        print(f"[download] Size check OK: source == local ({local_bytes} bytes)", flush=True)

        manifest_rows.append(
            {
                "dataset_id": row.get("dataset_id", ""),
                "title": title,
                "form_type": form,
                "download_url": url,
                "filename": filename,
                "local_path": str(local_path),
                "source_content_length_bytes": str(expected_bytes),
                "local_bytes": str(local_bytes),
            }
        )

    # Write manifest.
    manifest_path.parent.mkdir(parents=True, exist_ok=True)
    fields = [
        "dataset_id",
        "title",
        "form_type",
        "download_url",
        "filename",
        "local_path",
        "source_content_length_bytes",
        "local_bytes",
    ]
    with open(manifest_path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fields)
        w.writeheader()
        w.writerows(manifest_rows)
    print(f"[manifest] Wrote required dataset manifest: {manifest_path}", flush=True)
    print_elapsed(start, "Step 03")


if __name__ == "__main__":
    main()

