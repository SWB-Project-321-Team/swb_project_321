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
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

from tqdm import tqdm

from common import (
    CATALOG_CSV,
    COMBINED_NORMALIZED_CACHE_MANIFEST_JSON,
    COMBINED_NORMALIZED_CACHE_PARQUET,
    DOWNLOAD_STATE_JSON,
    DOWNLOAD_WORKERS,
    RAW_DIR,
    REQUIRED_MANIFEST_CSV,
    banner,
    build_input_signature,
    download_with_progress,
    ensure_dirs,
    ensure_combined_normalized_cache,
    load_catalog_rows,
    load_env_from_secrets,
    manifest_matches_expectation,
    print_transfer_settings,
    print_elapsed,
    read_json,
    select_required_datasets,
    url_basename,
    write_json,
)


def main() -> None:
    parser = argparse.ArgumentParser(description="Download required DataMart raw CSV files.")
    parser.add_argument("--catalog-csv", default=str(CATALOG_CSV), help="Catalog CSV path from step 02")
    parser.add_argument("--raw-dir", default=str(RAW_DIR), help="Local raw output directory")
    parser.add_argument("--manifest-csv", default=str(REQUIRED_MANIFEST_CSV), help="Manifest output path")
    parser.add_argument("--state-json", default=str(DOWNLOAD_STATE_JSON), help="Cached download state manifest JSON path")
    parser.add_argument("--overwrite", action="store_true", help="Re-download files even if local file exists")
    parser.add_argument(
        "--skip-if-unchanged",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="Skip the download pass when the cached state and outputs still match",
    )
    parser.add_argument("--force-rebuild", action="store_true", help="Ignore cached state and rerun the step")
    args = parser.parse_args()

    start = time.perf_counter()
    banner("STEP 03 - DOWNLOAD REQUIRED RAW DATAMART FILES")
    load_env_from_secrets()
    ensure_dirs()
    print_transfer_settings(label="download")

    raw_dir = Path(args.raw_dir)
    raw_dir.mkdir(parents=True, exist_ok=True)
    manifest_path = Path(args.manifest_csv)
    state_path = Path(args.state_json)

    rows = load_catalog_rows(Path(args.catalog_csv))
    required = select_required_datasets(rows)
    print(f"[download] Required datasets resolved: {len(required)}", flush=True)
    print(f"[download] Skip if unchanged: {args.skip_if_unchanged}", flush=True)
    print(f"[download] Force rebuild: {args.force_rebuild}", flush=True)

    input_signature = build_input_signature({"catalog_csv": Path(args.catalog_csv)})
    required_outputs = [manifest_path]
    for row in required:
        url = row.get("download_url", "")
        required_outputs.append(raw_dir / url_basename(url))
    required_outputs.extend(
        [
            COMBINED_NORMALIZED_CACHE_PARQUET,
            COMBINED_NORMALIZED_CACHE_MANIFEST_JSON,
        ]
    )
    build_options = {
        "raw_dir": str(raw_dir),
        "manifest_csv": str(manifest_path),
        "required_dataset_count": len(required),
        "combined_cache_parquet": str(COMBINED_NORMALIZED_CACHE_PARQUET),
    }
    if args.skip_if_unchanged and not args.overwrite and not args.force_rebuild and manifest_matches_expectation(
        state_path,
        expected_input_signature=input_signature,
        expected_options=build_options,
        required_outputs=required_outputs,
    ):
        cached = read_json(state_path)
        print("[download] Inputs and required outputs match cached state; skipping step.", flush=True)
        print(f"[download] Cached download count: {cached.get('required_dataset_count', '')}", flush=True)
        print_elapsed(start, "Step 03")
        return

    manifest_rows: list[dict[str, str]] = []
    pending_downloads: list[tuple[dict[str, str], Path, int | None]] = []
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
            continue

        pending_downloads.append((row, local_path, expected_bytes))

    if pending_downloads:
        worker_count = min(DOWNLOAD_WORKERS, len(pending_downloads))
        print(f"[download] Parallel download workers: {worker_count}", flush=True)

        def _download_one(task: tuple[dict[str, str], Path, int | None]) -> dict[str, str]:
            row, local_path, expected_bytes = task
            title = row.get("title", "")
            form = row.get("form_type", "")
            url = row.get("download_url", "")
            filename = local_path.name
            file_start = time.perf_counter()
            local_bytes = download_with_progress(url, local_path, expected_bytes=expected_bytes)
            if expected_bytes is None:
                raise RuntimeError(
                    f"Strict size validation requires source_content_length_bytes; missing for {title} [{form}] {url}"
                )
            if local_bytes != expected_bytes:
                raise RuntimeError(
                    f"Size mismatch after download for {filename}: expected={expected_bytes}, local={local_bytes}"
                )
            print(f"[download] Completed: {local_path} ({local_bytes} bytes)", flush=True)
            print(f"[download] Size check OK: source == local ({local_bytes} bytes)", flush=True)
            print_elapsed(file_start, f"download {filename}")
            return {
                "dataset_id": row.get("dataset_id", ""),
                "title": title,
                "form_type": form,
                "download_url": url,
                "filename": filename,
                "local_path": str(local_path),
                "source_content_length_bytes": str(expected_bytes),
                "local_bytes": str(local_bytes),
            }

        with ThreadPoolExecutor(max_workers=worker_count) as executor:
            future_to_task = {executor.submit(_download_one, task): task for task in pending_downloads}
            with tqdm(total=len(pending_downloads), desc="download datamarts", unit="dataset") as overall:
                for future in as_completed(future_to_task):
                    manifest_rows.append(future.result())
                    overall.update(1)

    # Write manifest.
    manifest_rows = sorted(manifest_rows, key=lambda row: (row["title"], row["form_type"], row["filename"]))
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

    combined_row = next((row for row in required if row.get("title") == "Combined Forms Datamart"), None)
    if combined_row is None:
        raise RuntimeError("Required dataset not found: Combined Forms Datamart")
    combined_filename = url_basename(combined_row.get("download_url", ""))
    combined_path = raw_dir / combined_filename
    print(f"[download] Preparing upstream Combined parquet cache from: {combined_path}", flush=True)
    ensure_combined_normalized_cache(
        combined_csv_path=combined_path,
        output_path=COMBINED_NORMALIZED_CACHE_PARQUET,
        manifest_path=COMBINED_NORMALIZED_CACHE_MANIFEST_JSON,
        force_rebuild=args.force_rebuild,
    )

    write_json(
        state_path,
        {
            "created_at_utc": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
            "input_signature": input_signature,
            "build_options": build_options,
            "required_dataset_count": len(required),
            "downloaded_file_count": len(manifest_rows),
            "manifest_csv": str(manifest_path),
            "raw_dir": str(raw_dir),
            "combined_cache_parquet": str(COMBINED_NORMALIZED_CACHE_PARQUET),
        },
    )
    print(f"[manifest] Wrote cached state JSON: {state_path}", flush=True)
    print_elapsed(start, "Step 03")


if __name__ == "__main__":
    main()
