"""
Shared helpers for the NCCS 990-N e-Postcard ingestion pipeline.

This module intentionally mirrors the structure and runtime behavior of the
existing NCCS Core pipeline while handling the postcard source's monthly
"instantaneous snapshot" model.
"""

from __future__ import annotations

import importlib.util
import json
import os
import re
import sys
from collections import Counter
from datetime import date
from pathlib import Path
from typing import Any
from urllib.parse import urljoin

import pandas as pd
import requests
from tqdm import tqdm

_THIS_FILE = Path(__file__).resolve()
_PYTHON_DIR = _THIS_FILE.parents[2]
if str(_PYTHON_DIR) not in sys.path:
    sys.path.insert(0, str(_PYTHON_DIR))

_CORE_COMMON_PATH = _PYTHON_DIR / "ingest" / "nccs_990_core" / "common.py"
_CORE_COMMON_SPEC = importlib.util.spec_from_file_location("nccs_990_core_common", _CORE_COMMON_PATH)
if _CORE_COMMON_SPEC is None or _CORE_COMMON_SPEC.loader is None:
    raise ImportError(f"Unable to load NCCS Core common helpers from {_CORE_COMMON_PATH}")
_CORE_COMMON = importlib.util.module_from_spec(_CORE_COMMON_SPEC)
sys.modules.setdefault("nccs_990_core_common", _CORE_COMMON)
_CORE_COMMON_SPEC.loader.exec_module(_CORE_COMMON)

from utils.paths import DATA  # noqa: E402

if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(line_buffering=True)

POSTCARD_DATASET_URL = "https://nccs.urban.org/nccs/datasets/postcard/"
RAW_BASE_URL = "https://nccsdata.s3.us-east-1.amazonaws.com/raw/e-postcard/"

RAW_ROOT = DATA / "raw" / "nccs_990" / "postcard"
POSTCARD_RAW_DIR = RAW_ROOT / "raw"
META_DIR = RAW_ROOT / "metadata"
STAGING_DIR = DATA / "staging" / "nccs_990" / "postcard"

LATEST_RELEASE_JSON = META_DIR / "latest_release.json"
POSTCARD_PAGE_SNAPSHOT = META_DIR / "postcard_page.html"

DEFAULT_S3_BUCKET = _CORE_COMMON.DEFAULT_S3_BUCKET
DEFAULT_S3_REGION = _CORE_COMMON.DEFAULT_S3_REGION
RAW_PREFIX = "bronze/nccs_990/postcard/raw"
META_PREFIX = "bronze/nccs_990/postcard/metadata"
SILVER_PREFIX = "silver/nccs_990/postcard"

GEOID_REFERENCE_CSV = _CORE_COMMON.GEOID_REFERENCE_CSV
ZIP_TO_COUNTY_CSV = _CORE_COMMON.ZIP_TO_COUNTY_CSV

_TQDM_KW = getattr(_CORE_COMMON, "_TQDM_KW", {})
_DOWNLOAD_URL_REGEX = re.compile(r"(?P<month>\d{4}-\d{2})-E-POSTCARD\.csv$", re.IGNORECASE)
DOWNLOAD_WORKERS = int(os.environ.get("NCCS_POSTCARD_DOWNLOAD_WORKERS", "2"))
UPLOAD_WORKERS = int(os.environ.get("NCCS_POSTCARD_UPLOAD_WORKERS", "2"))
TQDM_KW = _TQDM_KW

banner = _CORE_COMMON.banner
print_elapsed = _CORE_COMMON.print_elapsed
load_env_from_secrets = _CORE_COMMON.load_env_from_secrets
write_json = _CORE_COMMON.write_json
read_json = _CORE_COMMON.read_json
write_text = _CORE_COMMON.write_text
write_csv = _CORE_COMMON.write_csv
load_csv_rows = _CORE_COMMON.load_csv_rows
source_size_cache_key = _CORE_COMMON.source_size_cache_key
extract_source_size_cache = _CORE_COMMON.extract_source_size_cache
load_source_size_cache_from_manifest = _CORE_COMMON.load_source_size_cache_from_manifest
apply_source_size_cache_to_release = _CORE_COMMON.apply_source_size_cache_to_release
cache_source_size = _CORE_COMMON.cache_source_size
fetch_text = _CORE_COMMON.fetch_text
collect_links = _CORE_COMMON.collect_links
guess_content_type = _CORE_COMMON.guess_content_type
measure_remote_streamed_bytes = _CORE_COMMON.measure_remote_streamed_bytes
s3_object_size = _CORE_COMMON.s3_object_size
should_skip_upload = _CORE_COMMON.should_skip_upload
compute_size_match = _CORE_COMMON.compute_size_match
compute_local_s3_match = _CORE_COMMON.compute_local_s3_match
load_geoid_reference_set = _CORE_COMMON.load_geoid_reference_set
load_zip_to_county_map = _CORE_COMMON.load_zip_to_county_map
normalize_zip5 = _CORE_COMMON.normalize_zip5
normalize_fips5 = _CORE_COMMON.normalize_fips5
normalize_ein9 = _CORE_COMMON.normalize_ein9
read_csv_flexible = _CORE_COMMON.read_csv_flexible
iter_csv_chunks = _CORE_COMMON.iter_csv_chunks
meta_s3_key = _CORE_COMMON.meta_s3_key
now_utc_iso = _CORE_COMMON.now_utc_iso


def ensure_work_dirs(
    postcard_raw_dir: Path = POSTCARD_RAW_DIR,
    metadata_dir: Path = META_DIR,
    staging_dir: Path = STAGING_DIR,
) -> None:
    """Ensure all postcard working directories exist before a script starts."""
    for path in (postcard_raw_dir, metadata_dir, staging_dir):
        path.mkdir(parents=True, exist_ok=True)
        print(f"[paths] Ready: {path}", flush=True)


def download_with_progress(
    url: str,
    output_path: Path,
    expected_bytes: int | None = None,
    timeout: int = 120,
    *,
    position: int | None = None,
    desc: str | None = None,
) -> int:
    """Download a remote file to disk with a visible byte progress bar."""
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with requests.get(url, stream=True, timeout=timeout) as response:
        response.raise_for_status()
        total = expected_bytes
        if total is None:
            header = response.headers.get("Content-Length")
            content_encoding = response.headers.get("Content-Encoding", "").lower()
            if header and header.isdigit() and content_encoding != "gzip":
                total = int(header)
        tqdm_kwargs = dict(_TQDM_KW)
        if position is not None:
            tqdm_kwargs["position"] = position
        with tqdm(
            total=total,
            unit="B",
            unit_scale=True,
            unit_divisor=1024,
            desc=desc or f"download {output_path.name}",
            leave=True,
            **tqdm_kwargs,
        ) as pbar:
            with open(output_path, "wb") as f:
                for chunk in response.iter_content(chunk_size=1024 * 1024):
                    if not chunk:
                        continue
                    f.write(chunk)
                    pbar.update(len(chunk))
    return output_path.stat().st_size


def upload_file_with_progress(
    local_path: Path,
    bucket: str,
    key: str,
    region: str,
    extra_args: dict[str, Any] | None = None,
    *,
    position: int | None = None,
    desc: str | None = None,
) -> None:
    """Upload one local file to S3 with a visible byte progress bar."""
    client = _CORE_COMMON.s3_client(region)
    size = local_path.stat().st_size
    transfer_config = _CORE_COMMON.s3_transfer_config()
    print(
        "[upload-config] "
        f"threshold={_CORE_COMMON.S3_MULTIPART_THRESHOLD_MB}MB "
        f"chunk={_CORE_COMMON.S3_MULTIPART_CHUNKSIZE_MB}MB "
        f"concurrency={_CORE_COMMON.S3_MAX_CONCURRENCY}",
        flush=True,
    )
    tqdm_kwargs = dict(_TQDM_KW)
    if position is not None:
        tqdm_kwargs["position"] = position
    with tqdm(
        total=size,
        unit="B",
        unit_scale=True,
        unit_divisor=1024,
        desc=desc or f"upload {local_path.name}",
        leave=True,
        **tqdm_kwargs,
    ) as pbar:
        callback = _CORE_COMMON._TqdmUpload(pbar)
        client.upload_file(
            str(local_path),
            bucket,
            key,
            ExtraArgs=extra_args or {},
            Callback=callback,
            Config=transfer_config,
        )


def snapshot_year_raw_dir(postcard_raw_dir: Path, snapshot_year: int) -> Path:
    """Return the year-specific postcard raw directory."""
    return postcard_raw_dir / f"snapshot_year={snapshot_year}"


def snapshot_month_raw_dir(postcard_raw_dir: Path, snapshot_year: int, snapshot_month: str) -> Path:
    """Return the month-specific postcard raw directory."""
    return snapshot_year_raw_dir(postcard_raw_dir, snapshot_year) / f"snapshot_month={snapshot_month}"


def release_manifest_path(metadata_dir: Path, snapshot_year: int) -> Path:
    """Return the raw manifest path for one discovered snapshot year."""
    return metadata_dir / f"release_manifest_snapshot_year={snapshot_year}.csv"


def size_report_path(metadata_dir: Path, snapshot_year: int) -> Path:
    """Return the source/local/S3 verification report path."""
    return metadata_dir / f"size_verification_snapshot_year={snapshot_year}.csv"


def snapshot_staging_dir(staging_dir: Path, snapshot_year: int) -> Path:
    """Return the year-specific postcard staging directory."""
    return staging_dir / f"snapshot_year={snapshot_year}"


def filtered_output_path(staging_dir: Path, snapshot_year: int) -> Path:
    """Return the combined annual benchmark postcard output path."""
    return snapshot_staging_dir(staging_dir, snapshot_year) / f"nccs_990_postcard_benchmark_snapshot_year={snapshot_year}.csv"


def filter_manifest_path(staging_dir: Path, snapshot_year: int) -> Path:
    """Return the postcard filter manifest path."""
    return snapshot_staging_dir(staging_dir, snapshot_year) / f"filter_manifest_snapshot_year={snapshot_year}.csv"


def raw_s3_key(raw_prefix: str, snapshot_year: int, snapshot_month: str, filename: str) -> str:
    """Return the S3 key for one raw postcard snapshot CSV."""
    return (
        f"{raw_prefix.rstrip('/')}/snapshot_year={snapshot_year}/"
        f"snapshot_month={snapshot_month}/{filename}"
    )


def filtered_s3_key(silver_prefix: str, snapshot_year: int, filename: str) -> str:
    """Return the S3 key for one filtered postcard output."""
    return f"{silver_prefix.rstrip('/')}/snapshot_year={snapshot_year}/{filename}"


def parse_postcard_page_html(html: str, page_url: str = POSTCARD_DATASET_URL) -> dict[str, str | None]:
    """
    Parse the NCCS postcard dataset page.

    The page's direct download link may lag behind the actual newest monthly file, so
    we use it to confirm the source family and raw base URL, then probe live months.
    """
    download_url: str | None = None
    linked_snapshot_month: str | None = None
    for link in collect_links(html):
        href = link.get("href", "")
        if not href:
            continue
        abs_url = urljoin(page_url, href)
        match = _DOWNLOAD_URL_REGEX.search(abs_url)
        if match:
            download_url = abs_url
            linked_snapshot_month = match.group("month")
            break

    if download_url is None:
        raise ValueError("NCCS postcard dataset page is missing an E-POSTCARD download link.")

    raw_base_url = download_url.rsplit("/", 1)[0] + "/"
    return {
        "download_url": download_url,
        "linked_snapshot_month": linked_snapshot_month,
        "raw_base_url": raw_base_url,
    }


def _shift_month(year: int, month: int, delta: int) -> tuple[int, int]:
    """Shift a year/month pair by a positive or negative month delta."""
    absolute = (year * 12) + (month - 1) + delta
    shifted_year, shifted_month_zero = divmod(absolute, 12)
    return shifted_year, shifted_month_zero + 1


def _snapshot_month_string(year: int, month: int) -> str:
    """Format a snapshot month as YYYY-MM."""
    return f"{year:04d}-{month:02d}"


def postcard_filename(snapshot_month: str) -> str:
    """Return the official NCCS postcard filename for one snapshot month."""
    return f"{snapshot_month}-E-POSTCARD.csv"


def postcard_source_url(snapshot_month: str, raw_base_url: str = RAW_BASE_URL) -> str:
    """Return the official NCCS postcard source URL for one snapshot month."""
    return f"{raw_base_url.rstrip('/')}/{postcard_filename(snapshot_month)}"


def _head_or_get_snapshot(url: str, timeout: int = 60) -> dict[str, Any] | None:
    """
    Probe one postcard snapshot URL.

    Missing months return None. Successful probes return the same metadata shape used
    elsewhere in the NCCS pipelines.
    """
    try:
        response = requests.head(url, allow_redirects=True, timeout=timeout)
        if response.status_code == 404:
            return None
        response.raise_for_status()
        headers = response.headers
    except requests.RequestException as exc:
        response_obj = getattr(exc, "response", None)
        if response_obj is not None and response_obj.status_code == 404:
            return None
        with requests.get(url, stream=True, timeout=timeout) as response:
            if response.status_code == 404:
                return None
            response.raise_for_status()
            headers = response.headers
            content_length = headers.get("Content-Length")
            content_encoding = headers.get("Content-Encoding", "").lower()
            return {
                "status_code": response.status_code,
                "content_length": (
                    int(content_length)
                    if content_length and content_length.isdigit() and content_encoding != "gzip"
                    else None
                ),
                "last_modified": headers.get("Last-Modified"),
                "content_type": headers.get("Content-Type"),
            }

    content_length = headers.get("Content-Length")
    content_encoding = headers.get("Content-Encoding", "").lower()
    return {
        "status_code": response.status_code,
        "content_length": (
            int(content_length)
            if content_length and content_length.isdigit() and content_encoding != "gzip"
            else None
        ),
        "last_modified": headers.get("Last-Modified"),
        "content_type": headers.get("Content-Type"),
    }


def discover_latest_available_snapshot_month(
    *,
    today: date | None = None,
    raw_base_url: str = RAW_BASE_URL,
    lookback_months: int = 60,
) -> tuple[str, dict[str, Any]]:
    """Probe backward from the current month until the newest available snapshot is found."""
    anchor = today or date.today()
    for offset in tqdm(range(lookback_months), desc="probe latest postcard month", unit="month", **_TQDM_KW):
        year, month = _shift_month(anchor.year, anchor.month, -offset)
        snapshot_month = _snapshot_month_string(year, month)
        url = postcard_source_url(snapshot_month, raw_base_url)
        meta = _head_or_get_snapshot(url)
        if meta is None:
            continue
        return snapshot_month, meta
    raise ValueError(
        f"Unable to find any postcard snapshot within the last {lookback_months} months "
        f"starting from {anchor:%Y-%m}."
    )


def parse_snapshot_months_arg(snapshot_year: int, snapshot_months_arg: str | None) -> list[str] | None:
    """Parse a comma-separated explicit snapshot month selection."""
    if snapshot_months_arg is None or str(snapshot_months_arg).strip().lower() == "all":
        return None

    pieces = [piece.strip() for piece in str(snapshot_months_arg).split(",") if piece.strip()]
    if not pieces:
        return None

    selected: list[str] = []
    invalid: list[str] = []
    for piece in pieces:
        if re.fullmatch(r"\d{2}", piece):
            month_value = f"{snapshot_year}-{piece}"
        elif re.fullmatch(r"\d{4}-\d{2}", piece):
            month_value = piece
        else:
            invalid.append(piece)
            continue

        if not month_value.startswith(f"{snapshot_year}-"):
            invalid.append(piece)
            continue

        month_number = int(month_value[-2:])
        if month_number < 1 or month_number > 12:
            invalid.append(piece)
            continue

        if month_value not in selected:
            selected.append(month_value)

    if invalid:
        raise ValueError(
            f"Invalid --snapshot-months values: {', '.join(invalid)}. "
            f"Use 'all' or a comma-separated list like '01,02,03' or '{snapshot_year}-01,{snapshot_year}-02'."
        )
    return selected


def discover_available_snapshot_months(
    *,
    snapshot_year: int,
    raw_base_url: str,
    latest_snapshot_month: str | None,
    snapshot_months_arg: str | None,
) -> list[dict[str, Any]]:
    """Return available month metadata for the requested snapshot year."""
    explicit_months = parse_snapshot_months_arg(snapshot_year, snapshot_months_arg)
    if explicit_months is not None:
        candidate_months = explicit_months
        keep_missing = False
    else:
        max_month = 12
        if latest_snapshot_month and latest_snapshot_month.startswith(f"{snapshot_year}-"):
            max_month = int(latest_snapshot_month[-2:])
        candidate_months = [_snapshot_month_string(snapshot_year, month) for month in range(1, max_month + 1)]
        keep_missing = True

    available_assets: list[dict[str, Any]] = []
    missing_months: list[str] = []
    for snapshot_month in tqdm(candidate_months, desc="probe snapshot months", unit="month", **_TQDM_KW):
        url = postcard_source_url(snapshot_month, raw_base_url)
        meta = _head_or_get_snapshot(url)
        if meta is None:
            if keep_missing:
                print(f"[discover] Snapshot month missing: {snapshot_month}", flush=True)
                continue
            missing_months.append(snapshot_month)
            continue

        filename = postcard_filename(snapshot_month)
        available_assets.append(
            {
                "asset_group": "postcard_csv",
                "asset_type": "postcard_snapshot_csv",
                "snapshot_year": snapshot_year,
                "snapshot_month": snapshot_month,
                "source_url": url,
                "filename": filename,
                "source_content_type": meta.get("content_type") or guess_content_type(Path(filename)),
                "source_content_length_bytes": meta.get("content_length"),
                "source_last_modified": meta.get("last_modified"),
            }
        )

    if missing_months:
        raise ValueError(
            f"Requested snapshot month(s) not available for snapshot_year={snapshot_year}: {', '.join(missing_months)}"
        )
    if not available_assets:
        raise ValueError(f"No postcard snapshots were found for snapshot_year={snapshot_year}.")
    return available_assets


def discover_release(
    snapshot_year_arg: str = "latest",
    snapshot_months_arg: str | None = "all",
    *,
    today: date | None = None,
) -> tuple[dict[str, Any], str]:
    """Discover the postcard release payload and return it with the fetched dataset page HTML."""
    postcard_page_html = fetch_text(POSTCARD_DATASET_URL)
    page_info = parse_postcard_page_html(postcard_page_html, POSTCARD_DATASET_URL)
    raw_base_url = page_info["raw_base_url"] or RAW_BASE_URL

    snapshot_year_arg_clean = str(snapshot_year_arg).strip().lower()
    if snapshot_year_arg_clean in ("latest", "auto"):
        latest_snapshot_month, _ = discover_latest_available_snapshot_month(today=today, raw_base_url=raw_base_url)
        snapshot_year = int(latest_snapshot_month[:4])
        snapshot_year_selection = "latest"
    else:
        snapshot_year = int(snapshot_year_arg_clean)
        latest_snapshot_month = None
        snapshot_year_selection = "explicit"

    assets = discover_available_snapshot_months(
        snapshot_year=snapshot_year,
        raw_base_url=raw_base_url,
        latest_snapshot_month=latest_snapshot_month,
        snapshot_months_arg=snapshot_months_arg,
    )

    payload = {
        "postcard_dataset_url": POSTCARD_DATASET_URL,
        "raw_base_url": raw_base_url,
        "linked_download_url": page_info["download_url"],
        "linked_snapshot_month": page_info["linked_snapshot_month"],
        "snapshot_year": snapshot_year,
        "snapshot_year_selection": snapshot_year_selection,
        "latest_snapshot_month": latest_snapshot_month,
        "available_snapshot_months": [asset["snapshot_month"] for asset in assets],
        "discovered_at_utc": now_utc_iso(),
        "assets": assets,
    }
    return payload, postcard_page_html


def resolve_release_and_write_metadata(
    snapshot_year_arg: str,
    metadata_dir: Path,
    *,
    snapshot_months_arg: str | None = "all",
) -> dict[str, Any]:
    """Discover a postcard release, persist metadata, and save the dataset page snapshot."""
    latest_path = metadata_dir / LATEST_RELEASE_JSON.name
    existing_release = read_json(latest_path)
    release, postcard_page_html = discover_release(snapshot_year_arg, snapshot_months_arg)
    release = apply_source_size_cache_to_release(release, extract_source_size_cache(existing_release))
    write_text(metadata_dir / POSTCARD_PAGE_SNAPSHOT.name, postcard_page_html)
    write_json(latest_path, release)
    return release


def selected_assets(release: dict[str, Any]) -> list[dict[str, Any]]:
    """Return all selected postcard monthly snapshot assets."""
    return list(release.get("assets", []))


def local_asset_path(postcard_raw_dir: Path, metadata_dir: Path, asset: dict[str, Any]) -> Path:
    """Return the local download path for one postcard asset."""
    return snapshot_month_raw_dir(
        postcard_raw_dir,
        int(asset["snapshot_year"]),
        str(asset["snapshot_month"]),
    ) / str(asset["filename"])


def asset_s3_key(raw_prefix: str, meta_prefix: str, asset: dict[str, Any]) -> str:
    """Return the S3 key for one selected postcard asset."""
    return raw_s3_key(
        raw_prefix,
        int(asset["snapshot_year"]),
        str(asset["snapshot_month"]),
        str(asset["filename"]),
    )


def _normalize_series_zip5(series: pd.Series) -> pd.Series:
    """Normalize a pandas Series of ZIP-like values to ZIP5 strings."""
    return series.fillna("").astype(str).map(normalize_zip5)


def _normalize_series_ein9(series: pd.Series) -> pd.Series:
    """Normalize a pandas Series of EIN-like values to 9-digit strings."""
    return series.fillna("").astype(str).map(normalize_ein9)


def filter_postcard_year_to_benchmark(
    assets: list[dict[str, Any]],
    postcard_raw_dir: Path,
    metadata_dir: Path,
    output_path: Path,
    geoid_reference_set: set[str],
    geoid_to_region: dict[str, str] | None,
    zip_to_county: dict[str, str],
    *,
    chunk_size: int = 100_000,
) -> dict[str, Any]:
    """
    Filter all monthly snapshots in one snapshot year and combine them into a single annual output.

    Postcard data already contains organization and officer ZIPs, so the benchmark geography
    match is based on ZIP -> county crosswalk rather than the Unified BMF bridge used by Core.
    """
    output_path.parent.mkdir(parents=True, exist_ok=True)
    final_frames: list[pd.DataFrame] = []
    input_row_count = 0
    matched_row_count = 0
    matched_counties: set[str] = set()
    source_counts: Counter[str] = Counter()
    original_columns: list[str] | None = None

    for asset in tqdm(assets, desc="filter postcard months", unit="file", **_TQDM_KW):
        local_source_path = local_asset_path(postcard_raw_dir, metadata_dir, asset)
        if not local_source_path.exists():
            raise FileNotFoundError(f"Local postcard file not found: {local_source_path}. Run step 02 first.")

        snapshot_month = str(asset["snapshot_month"])
        print(f"[filter] Source file: {local_source_path}", flush=True)
        header_df = read_csv_flexible(local_source_path, dtype=str, keep_default_na=False, low_memory=False, nrows=0)
        if original_columns is None:
            original_columns = list(header_df.columns)

        with tqdm(desc=f"chunks {local_source_path.name}", unit="chunk", **_TQDM_KW) as chunk_pbar:
            for chunk_index, chunk in enumerate(iter_csv_chunks(local_source_path, chunk_size=chunk_size), start=1):
                chunk_pbar.update(1)
                input_row_count += len(chunk)
                chunk = chunk.copy()

                organization_zip5 = _normalize_series_zip5(chunk.get("organization_zip", pd.Series("", index=chunk.index)))
                officer_zip5 = _normalize_series_zip5(chunk.get("officer_zip", pd.Series("", index=chunk.index)))
                county_from_org_zip = organization_zip5.map(lambda value: zip_to_county.get(value, ""))
                county_from_officer_zip = officer_zip5.map(lambda value: zip_to_county.get(value, ""))

                county_fips = county_from_org_zip.where(county_from_org_zip != "", county_from_officer_zip)
                benchmark_match_source = pd.Series("", index=chunk.index, dtype=object)
                benchmark_match_source = benchmark_match_source.mask(county_from_org_zip != "", "organization_zip")
                benchmark_match_source = benchmark_match_source.mask(
                    (county_from_org_zip == "") & (county_from_officer_zip != ""),
                    "officer_zip",
                )

                chunk["county_fips"] = county_fips.map(normalize_fips5)
                if geoid_to_region:
                    chunk["region"] = chunk["county_fips"].map(geoid_to_region).fillna("")
                else:
                    chunk["region"] = ""
                chunk["benchmark_match_source"] = benchmark_match_source.fillna("")
                chunk["snapshot_month"] = snapshot_month
                chunk["snapshot_year"] = int(asset["snapshot_year"])
                chunk["is_benchmark_county"] = chunk["county_fips"].isin(geoid_reference_set) & chunk["region"].astype(str).str.strip().ne("")

                filtered = chunk[chunk["is_benchmark_county"] == True].copy()  # noqa: E712
                if filtered.empty:
                    print(f"[filter] {local_source_path.name} chunk {chunk_index}: input={len(chunk):,} output=0", flush=True)
                    continue

                matched_row_count += len(filtered)
                matched_counties.update(filtered["county_fips"].dropna().astype(str))
                source_counts.update(filtered["benchmark_match_source"].astype(str))
                final_frames.append(
                    filtered[
                        original_columns
                        + [
                            "county_fips",
                            "region",
                            "benchmark_match_source",
                            "snapshot_month",
                            "snapshot_year",
                            "is_benchmark_county",
                        ]
                    ]
                )
                print(
                    f"[filter] {local_source_path.name} chunk {chunk_index}: input={len(chunk):,} output={len(filtered):,}",
                    flush=True,
                )

    if original_columns is None:
        raise ValueError("No postcard source columns were discovered during filtering.")

    if not final_frames:
        empty = pd.DataFrame(
            columns=original_columns
            + [
                "county_fips",
                "region",
                "benchmark_match_source",
                "snapshot_month",
                "snapshot_year",
                "is_benchmark_county",
            ]
        )
        empty.to_csv(output_path, index=False)
        return {
            "input_row_count": input_row_count,
            "matched_row_count": 0,
            "output_row_count": 0,
            "deduped_ein_count": 0,
            "matched_county_fips_count": 0,
            "zip_match_source_counts": {},
        }

    combined = pd.concat(final_frames, ignore_index=True)
    combined["__ein_norm"] = _normalize_series_ein9(combined["ein"])
    combined["__snapshot_month_sort"] = combined["snapshot_month"].astype(str)
    combined["__tax_year_sort"] = pd.to_numeric(combined.get("tax_year"), errors="coerce").fillna(-1)
    combined["__tax_period_end_sort"] = pd.to_datetime(
        combined.get("tax_period_end_date"),
        format="%m-%d-%Y",
        errors="coerce",
    )
    combined["__dedupe_key"] = combined["__ein_norm"]

    blank_mask = combined["__dedupe_key"] == ""
    if blank_mask.any():
        blank_indices = combined.index[blank_mask].tolist()
        combined.loc[blank_mask, "__dedupe_key"] = [f"__blank__{idx}" for idx in blank_indices]

    # Keep the row from the newest snapshot month, then the latest filing tax year, then the
    # latest tax period end date. Stable mergesort keeps the tie-breaking deterministic.
    combined.sort_values(
        by=["__dedupe_key", "__snapshot_month_sort", "__tax_year_sort", "__tax_period_end_sort"],
        ascending=[True, False, False, False],
        inplace=True,
        kind="mergesort",
    )
    deduped = combined.drop_duplicates(subset=["__dedupe_key"], keep="first").copy()
    deduped.sort_values(by=["region", "county_fips", "ein"], ascending=[True, True, True], inplace=True, kind="mergesort")

    final_columns = original_columns + [
        "county_fips",
        "region",
        "benchmark_match_source",
        "snapshot_month",
        "snapshot_year",
        "is_benchmark_county",
    ]
    deduped[final_columns].to_csv(output_path, index=False)

    unique_eins = deduped["ein"].astype(str).map(normalize_ein9)
    deduped_ein_count = int((unique_eins != "").sum())
    return {
        "input_row_count": input_row_count,
        "matched_row_count": matched_row_count,
        "output_row_count": len(deduped),
        "deduped_ein_count": deduped_ein_count,
        "matched_county_fips_count": len(matched_counties),
        "zip_match_source_counts": dict(source_counts),
    }
