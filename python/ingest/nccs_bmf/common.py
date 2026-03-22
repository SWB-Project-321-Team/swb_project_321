"""
Shared helpers for the NCCS BMF 2022-present ingestion pipeline.

This pipeline mirrors the existing NCCS Core and Postcard pipelines:
- step-wise CLI scripts with explicit startup banners
- raw manifest + verification report flow
- direct print() status lines and tqdm progress bars
- local raw/metadata/staging layout under DATA

The source landscape is different from the other NCCS pipelines, so discovery
logic is more opinionated:
- 2022 uses the latest available legacy BMF release in calendar year 2022
- 2023+ uses one representative raw monthly snapshot per year
- completed years prefer December when present
- the current year uses the latest currently available month
"""

from __future__ import annotations

import importlib.util
import os
import re
import sys
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Any
from urllib.parse import urljoin

import polars as pl
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

BMF_DATASET_URL = "https://nccs.urban.org/nccs/datasets/bmf/"
BMF_CATALOG_URL = "https://nccs.urban.org/nccs/catalogs/catalog-bmf.html"
RAW_MONTHLY_BASE_URL = "https://nccsdata.s3.us-east-1.amazonaws.com/raw/bmf/"
LEGACY_BASE_URL = "https://nccsdata.s3.us-east-1.amazonaws.com/legacy/bmf/"

RAW_ROOT = DATA / "raw" / "nccs_bmf"
BMF_RAW_DIR = RAW_ROOT / "raw"
META_DIR = RAW_ROOT / "metadata"
STAGING_DIR = DATA / "staging" / "nccs_bmf"

LATEST_RELEASE_JSON = META_DIR / "latest_release.json"
BMF_DATASET_SNAPSHOT = META_DIR / "dataset_bmf.html"
BMF_CATALOG_SNAPSHOT = META_DIR / "catalog_bmf.html"

DEFAULT_S3_BUCKET = _CORE_COMMON.DEFAULT_S3_BUCKET
DEFAULT_S3_REGION = _CORE_COMMON.DEFAULT_S3_REGION
RAW_PREFIX = "bronze/nccs_bmf/raw"
META_PREFIX = "bronze/nccs_bmf/metadata"
SILVER_PREFIX = "silver/nccs_bmf"

GEOID_REFERENCE_CSV = _CORE_COMMON.GEOID_REFERENCE_CSV
ZIP_TO_COUNTY_CSV = _CORE_COMMON.ZIP_TO_COUNTY_CSV

START_YEAR_DEFAULT = 2022
LOOKBACK_MONTHS_DEFAULT = 48
DOWNLOAD_WORKERS = int(os.environ.get("NCCS_BMF_DOWNLOAD_WORKERS", "2"))
UPLOAD_WORKERS = int(os.environ.get("NCCS_BMF_UPLOAD_WORKERS", "2"))
_TQDM_KW = getattr(_CORE_COMMON, "_TQDM_KW", {})

_RAW_MONTHLY_REGEX = re.compile(r"(?P<year>\d{4})-(?P<month>\d{2})-BMF\.csv$", re.IGNORECASE)
_LEGACY_REGEX = re.compile(r"BMF-(?P<year>\d{4})-(?P<month>\d{2})-501CX-NONPROFIT-PX\.csv$", re.IGNORECASE)

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
fetch_text = _CORE_COMMON.fetch_text
collect_links = _CORE_COMMON.collect_links
guess_content_type = _CORE_COMMON.guess_content_type
measure_remote_streamed_bytes = _CORE_COMMON.measure_remote_streamed_bytes
s3_object_size = _CORE_COMMON.s3_object_size
should_skip_upload = _CORE_COMMON.should_skip_upload
compute_size_match = _CORE_COMMON.compute_size_match
compute_local_s3_match = _CORE_COMMON.compute_local_s3_match
meta_s3_key = _CORE_COMMON.meta_s3_key
now_utc_iso = _CORE_COMMON.now_utc_iso
s3_client = _CORE_COMMON.s3_client
s3_transfer_config = _CORE_COMMON.s3_transfer_config


@dataclass(frozen=True)
class BMFAssetRecord:
    """One selected BMF raw asset for this run."""

    asset_group: str
    asset_type: str
    snapshot_year: int
    snapshot_month: str
    source_period: str
    source_url: str
    filename: str
    year_basis: str
    source_content_type: str
    source_content_length_bytes: int | None
    source_last_modified: str | None

    def to_dict(self) -> dict[str, Any]:
        return {
            "asset_group": self.asset_group,
            "asset_type": self.asset_type,
            "snapshot_year": self.snapshot_year,
            "snapshot_month": self.snapshot_month,
            "source_period": self.source_period,
            "source_url": self.source_url,
            "filename": self.filename,
            "year_basis": self.year_basis,
            "source_content_type": self.source_content_type,
            "source_content_length_bytes": self.source_content_length_bytes,
            "source_last_modified": self.source_last_modified,
        }


def apply_source_size_cache_to_release(
    release: dict[str, Any],
    cache_entries: dict[str, dict[str, Any]],
) -> dict[str, Any]:
    """
    Merge cache entries into the BMF release JSON and populate selected asset sizes.

    NCCS Core stores its discovered assets under `release["assets"]`, but this BMF
    pipeline stores them under `release["selected_assets"]`. This thin adapter keeps
    the same cache structure while updating the BMF-specific asset list.
    """
    release_cache = extract_source_size_cache(release)
    release_cache.update(cache_entries)

    for asset in release.get("selected_assets", []):
        if not isinstance(asset, dict):
            continue
        cache_entry = release_cache.get(source_size_cache_key(asset.get("source_url", ""), asset.get("source_last_modified")))
        if not cache_entry:
            continue
        asset["source_content_length_bytes"] = cache_entry["source_content_length_bytes"]

    release["source_size_cache"] = release_cache
    return release


def cache_source_size(
    release: dict[str, Any],
    *,
    source_url: str,
    source_last_modified: str | None,
    source_content_length_bytes: int,
) -> dict[str, Any]:
    """Persist one source-byte value into the BMF release cache and matching asset entry."""
    cache_entry = {
        "source_url": source_url,
        "source_last_modified": source_last_modified,
        "source_content_length_bytes": int(source_content_length_bytes),
        "cached_at_utc": now_utc_iso(),
    }
    key = source_size_cache_key(source_url, source_last_modified)
    return apply_source_size_cache_to_release(release, {key: cache_entry})


def ensure_work_dirs(
    raw_dir: Path = BMF_RAW_DIR,
    metadata_dir: Path = META_DIR,
    staging_dir: Path = STAGING_DIR,
) -> None:
    """Ensure the BMF pipeline's local working directories exist."""
    for path in (raw_dir, metadata_dir, staging_dir):
        path.mkdir(parents=True, exist_ok=True)
        print(f"[paths] Ready: {path}", flush=True)


def year_raw_dir(raw_dir: Path, snapshot_year: int) -> Path:
    """Return the raw-directory path for one BMF snapshot year."""
    return raw_dir / f"year={snapshot_year}"


def local_asset_path(raw_dir: Path, metadata_dir: Path, asset: dict[str, Any]) -> Path:
    """Return the local download path for one selected BMF asset."""
    return year_raw_dir(raw_dir, int(asset["snapshot_year"])) / str(asset["filename"])


def release_manifest_path(metadata_dir: Path, start_year: int = START_YEAR_DEFAULT) -> Path:
    """Return the raw-manifest path for the multi-year BMF release selection."""
    return metadata_dir / f"release_manifest_start_year={start_year}.csv"


def size_report_path(metadata_dir: Path, start_year: int = START_YEAR_DEFAULT) -> Path:
    """Return the raw verification-report path for the multi-year BMF selection."""
    return metadata_dir / f"size_verification_start_year={start_year}.csv"


def year_staging_dir(staging_dir: Path, snapshot_year: int) -> Path:
    """Return the staging directory for one filtered BMF snapshot year."""
    return staging_dir / f"year={snapshot_year}"


def filtered_output_path(staging_dir: Path, snapshot_year: int) -> Path:
    """Return the filtered parquet path for one snapshot year."""
    return year_staging_dir(staging_dir, snapshot_year) / f"nccs_bmf_benchmark_year={snapshot_year}.parquet"


def filter_manifest_path(staging_dir: Path, start_year: int = START_YEAR_DEFAULT) -> Path:
    """Return the benchmark-filter manifest path."""
    return staging_dir / f"filter_manifest_start_year={start_year}.csv"


def raw_s3_key(raw_prefix: str, snapshot_year: int, filename: str) -> str:
    """Return the S3 key for one raw BMF asset."""
    return f"{raw_prefix.rstrip('/')}/year={snapshot_year}/{filename}"


def filtered_s3_key(silver_prefix: str, snapshot_year: int, filename: str) -> str:
    """Return the S3 key for one filtered yearly BMF parquet."""
    return f"{silver_prefix.rstrip('/')}/year={snapshot_year}/{filename}"


def _shift_month(year: int, month: int, delta: int) -> tuple[int, int]:
    """Shift a YYYY-MM pair by delta months."""
    total = year * 12 + (month - 1) + delta
    return total // 12, (total % 12) + 1


def _snapshot_month_string(year: int, month: int) -> str:
    """Format a YYYY-MM snapshot identifier."""
    return f"{year:04d}-{month:02d}"


def raw_monthly_filename(snapshot_month: str) -> str:
    """Return the official NCCS raw monthly BMF filename for one month."""
    return f"{snapshot_month}-BMF.csv"


def raw_monthly_url(snapshot_month: str, raw_base_url: str = RAW_MONTHLY_BASE_URL) -> str:
    """Return the official NCCS raw monthly BMF URL for one month."""
    return f"{raw_base_url.rstrip('/')}/{raw_monthly_filename(snapshot_month)}"


def _safe_probe(url: str, timeout: int = 60) -> dict[str, Any] | None:
    """
    Probe a remote BMF URL and return metadata.

    Missing files are a normal outcome during discovery, so this wrapper returns
    None on 404 instead of surfacing an exception.
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
        try:
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
        except requests.RequestException as inner_exc:
            inner_response = getattr(inner_exc, "response", None)
            if inner_response is not None and inner_response.status_code == 404:
                return None
            raise

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


def parse_bmf_dataset_page_html(html: str, page_url: str = BMF_DATASET_URL) -> dict[str, Any]:
    """Parse the BMF dataset overview page for the public raw/legacy patterns."""
    raw_links: dict[str, str] = {}
    legacy_links: dict[str, str] = {}
    raw_base_url = RAW_MONTHLY_BASE_URL
    legacy_base_url = LEGACY_BASE_URL

    for link in collect_links(html):
        href = link.get("href", "")
        if not href:
            continue
        abs_url = urljoin(page_url, href)
        filename = Path(abs_url).name

        raw_match = _RAW_MONTHLY_REGEX.match(filename)
        if raw_match and "/raw/bmf/" in abs_url:
            raw_links[_snapshot_month_string(int(raw_match.group("year")), int(raw_match.group("month")))] = abs_url
            continue

        legacy_match = _LEGACY_REGEX.match(filename)
        if legacy_match and "/legacy/bmf/" in abs_url:
            legacy_links[filename] = abs_url

    if RAW_MONTHLY_BASE_URL.rstrip("/") in html:
        raw_base_url = RAW_MONTHLY_BASE_URL
    if LEGACY_BASE_URL.rstrip("/") in html:
        legacy_base_url = LEGACY_BASE_URL

    return {
        "raw_base_url": raw_base_url,
        "legacy_base_url": legacy_base_url,
        "raw_example_links": raw_links,
        "legacy_example_links": legacy_links,
    }


def parse_bmf_catalog_html(html: str, page_url: str = BMF_CATALOG_URL) -> dict[str, Any]:
    """Parse the BMF catalog into raw-month and legacy-file maps."""
    raw_links_by_month: dict[str, str] = {}
    legacy_links_by_period: dict[str, str] = {}

    for link in collect_links(html):
        href = link.get("href", "")
        if not href:
            continue
        abs_url = urljoin(page_url, href)
        filename = Path(abs_url).name

        raw_match = _RAW_MONTHLY_REGEX.match(filename)
        if raw_match and "/raw/bmf/" in abs_url:
            raw_links_by_month[_snapshot_month_string(int(raw_match.group("year")), int(raw_match.group("month")))] = abs_url
            continue

        legacy_match = _LEGACY_REGEX.match(filename)
        if legacy_match and "/legacy/bmf/" in abs_url:
            legacy_links_by_period[_snapshot_month_string(int(legacy_match.group("year")), int(legacy_match.group("month")))] = abs_url

    return {
        "raw_links_by_month": raw_links_by_month,
        "legacy_links_by_period": legacy_links_by_period,
    }


def discover_latest_available_raw_month(
    *,
    today: date | None = None,
    raw_base_url: str = RAW_MONTHLY_BASE_URL,
    lookback_months: int = LOOKBACK_MONTHS_DEFAULT,
) -> tuple[str, dict[str, Any]]:
    """Probe backward from the current month until the newest raw BMF monthly snapshot is found."""
    anchor = today or date.today()
    for offset in tqdm(range(lookback_months), desc="probe latest BMF month", unit="month", **_TQDM_KW):
        year, month = _shift_month(anchor.year, anchor.month, -offset)
        snapshot_month = _snapshot_month_string(year, month)
        meta = _safe_probe(raw_monthly_url(snapshot_month, raw_base_url))
        if meta is None:
            continue
        return snapshot_month, meta
    raise ValueError(
        f"Unable to find any raw BMF monthly snapshot within the last {lookback_months} months "
        f"starting from {anchor:%Y-%m}."
    )


def _select_legacy_2022_asset(legacy_links_by_period: dict[str, str]) -> tuple[str, str]:
    """Select the latest available 2022 legacy BMF asset from the catalog links."""
    legacy_2022_periods = sorted(period for period in legacy_links_by_period if period.startswith("2022-"))
    if not legacy_2022_periods:
        raise ValueError("BMF catalog did not expose any 2022 legacy BMF assets.")
    selected_period = legacy_2022_periods[-1]
    return selected_period, legacy_links_by_period[selected_period]


def _select_raw_year_period(
    year: int,
    *,
    latest_available_month: str,
    raw_base_url: str,
    timeout: int = 60,
) -> tuple[str, dict[str, Any]]:
    """
    Select the representative raw snapshot month for one year.

    Completed years prefer December when it exists so annual coverage lines up on
    year-end snapshots. The current year uses the latest currently available month.
    If a historical year lacks December, the latest available month in that year is
    used instead.
    """
    latest_year = int(latest_available_month[:4])
    if year > latest_year:
        raise ValueError(f"Requested year {year} is beyond the latest available raw BMF month {latest_available_month}.")

    candidate_months: list[int]
    if year == latest_year:
        latest_month_number = int(latest_available_month[-2:])
        candidate_months = list(range(latest_month_number, 0, -1))
    else:
        candidate_months = [12] + list(range(11, 0, -1))

    seen: set[int] = set()
    ordered_months = [month for month in candidate_months if not (month in seen or seen.add(month))]
    for month in tqdm(ordered_months, desc=f"probe BMF year {year}", unit="month", **_TQDM_KW):
        snapshot_month = _snapshot_month_string(year, month)
        meta = _safe_probe(raw_monthly_url(snapshot_month, raw_base_url), timeout=timeout)
        if meta is not None:
            return snapshot_month, meta

    raise ValueError(f"No raw BMF snapshot found for year {year}.")


def build_asset_record(
    *,
    asset_group: str,
    asset_type: str,
    snapshot_year: int,
    snapshot_month: str,
    source_period: str,
    source_url: str,
    year_basis: str,
    source_content_length_bytes: int | None = None,
    source_last_modified: str | None = None,
    source_content_type: str | None = None,
) -> BMFAssetRecord:
    """Build one BMFAssetRecord, probing metadata when it was not supplied by discovery."""
    filename = Path(source_url).name
    if source_content_length_bytes is None or source_last_modified is None or source_content_type is None:
        meta = _safe_probe(source_url)
        if meta is None:
            raise ValueError(f"Unable to probe BMF source metadata for {source_url}")
        source_content_length_bytes = meta.get("content_length")
        source_last_modified = meta.get("last_modified")
        source_content_type = meta.get("content_type")

    return BMFAssetRecord(
        asset_group=asset_group,
        asset_type=asset_type,
        snapshot_year=snapshot_year,
        snapshot_month=snapshot_month,
        source_period=source_period,
        source_url=source_url,
        filename=filename,
        year_basis=year_basis,
        source_content_type=source_content_type or guess_content_type(Path(filename)),
        source_content_length_bytes=source_content_length_bytes,
        source_last_modified=source_last_modified,
    )


def discover_release(
    *,
    start_year: int = START_YEAR_DEFAULT,
    today: date | None = None,
) -> tuple[dict[str, Any], str, str]:
    """Discover the yearly NCCS BMF snapshot set for 2022-present."""
    if start_year != 2022:
        raise ValueError("This v1 NCCS BMF pipeline is scoped to start_year=2022.")

    dataset_html = fetch_text(BMF_DATASET_URL)
    catalog_html = fetch_text(BMF_CATALOG_URL)
    dataset_info = parse_bmf_dataset_page_html(dataset_html, BMF_DATASET_URL)
    catalog_info = parse_bmf_catalog_html(catalog_html, BMF_CATALOG_URL)

    raw_base_url = str(dataset_info["raw_base_url"] or RAW_MONTHLY_BASE_URL)
    latest_raw_month, latest_raw_meta = discover_latest_available_raw_month(today=today, raw_base_url=raw_base_url)
    latest_raw_year = int(latest_raw_month[:4])

    selected_assets_list: list[BMFAssetRecord] = []

    legacy_period, legacy_url = _select_legacy_2022_asset(catalog_info["legacy_links_by_period"])
    selected_assets_list.append(
        build_asset_record(
            asset_group="legacy_bmf_csv",
            asset_type="legacy_bmf_csv",
            snapshot_year=2022,
            snapshot_month="",
            source_period=legacy_period,
            source_url=legacy_url,
            year_basis="legacy_latest_in_year",
        )
    )

    for year in range(2023, latest_raw_year + 1):
        if year == latest_raw_year:
            selected_period = latest_raw_month
            selected_meta = latest_raw_meta
            year_basis = "latest_available_month"
        else:
            selected_period, selected_meta = _select_raw_year_period(
                year,
                latest_available_month=latest_raw_month,
                raw_base_url=raw_base_url,
            )
            year_basis = "december_preferred" if selected_period.endswith("-12") else "latest_available_in_year"

        selected_assets_list.append(
            build_asset_record(
                asset_group="raw_bmf_csv",
                asset_type="raw_bmf_csv",
                snapshot_year=year,
                snapshot_month=selected_period,
                source_period=selected_period,
                source_url=raw_monthly_url(selected_period, raw_base_url),
                year_basis=year_basis,
                source_content_length_bytes=selected_meta.get("content_length"),
                source_last_modified=selected_meta.get("last_modified"),
                source_content_type=selected_meta.get("content_type"),
            )
        )

    payload = {
        "bmf_dataset_url": BMF_DATASET_URL,
        "bmf_catalog_url": BMF_CATALOG_URL,
        "raw_base_url": raw_base_url,
        "legacy_base_url": str(dataset_info["legacy_base_url"] or LEGACY_BASE_URL),
        "start_year": start_year,
        "latest_raw_month": latest_raw_month,
        "selected_snapshot_years": [asset.snapshot_year for asset in selected_assets_list],
        "selected_assets": [asset.to_dict() for asset in selected_assets_list],
        "discovered_at_utc": now_utc_iso(),
    }
    return payload, dataset_html, catalog_html


def resolve_release_and_write_metadata(
    metadata_dir: Path,
    *,
    start_year: int = START_YEAR_DEFAULT,
) -> dict[str, Any]:
    """Discover the BMF release, persist metadata JSON, and save both source-page snapshots."""
    latest_path = metadata_dir / LATEST_RELEASE_JSON.name
    existing_release = read_json(latest_path)
    release, dataset_html, catalog_html = discover_release(start_year=start_year)
    release = apply_source_size_cache_to_release(release, extract_source_size_cache(existing_release))
    write_text(metadata_dir / BMF_DATASET_SNAPSHOT.name, dataset_html)
    write_text(metadata_dir / BMF_CATALOG_SNAPSHOT.name, catalog_html)
    write_json(latest_path, release)
    return release


def selected_assets(release: dict[str, Any]) -> list[dict[str, Any]]:
    """Return the selected yearly BMF assets in snapshot-year order."""
    return sorted(release.get("selected_assets", []), key=lambda asset: (int(asset["snapshot_year"]), str(asset["source_period"])))


def asset_s3_key(raw_prefix: str, meta_prefix: str, asset: dict[str, Any]) -> str:
    """Return the S3 object key for one selected BMF raw asset."""
    return raw_s3_key(raw_prefix, int(asset["snapshot_year"]), str(asset["filename"]))


def download_with_progress(
    url: str,
    output_path: Path,
    expected_bytes: int | None = None,
    timeout: int = 120,
    *,
    position: int | None = None,
    desc: str | None = None,
) -> int:
    """Download one raw BMF asset with a visible byte progress bar."""
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
            with open(output_path, "wb") as handle:
                for chunk in response.iter_content(chunk_size=1024 * 1024):
                    if not chunk:
                        continue
                    handle.write(chunk)
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
    """Upload one local BMF file to S3 with a visible byte progress bar."""
    client = s3_client(region)
    size = local_path.stat().st_size
    transfer_config = s3_transfer_config()
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


def _blank_expr(expr: pl.Expr) -> pl.Expr:
    """True when the value is null or blank-like."""
    return expr.is_null() | expr.cast(pl.Utf8, strict=False).str.strip_chars().eq("")


def _normalize_zip5_expr(expr: pl.Expr) -> pl.Expr:
    """Normalize ZIP-like values to 5-digit ZIP codes."""
    digits = expr.cast(pl.Utf8, strict=False).fill_null("").str.replace_all(r"[^0-9]", "")
    return pl.when(digits.str.len_chars() >= 5).then(digits.str.slice(0, 5)).otherwise(pl.lit(""))


def _normalize_fips5_expr(expr: pl.Expr) -> pl.Expr:
    """Normalize FIPS/GEOID-like values to 5-digit county codes."""
    digits = expr.cast(pl.Utf8, strict=False).fill_null("").str.replace_all(r"[^0-9]", "")
    return (
        pl.when(digits.str.len_chars() == 0)
        .then(pl.lit(""))
        .when(digits.str.len_chars() < 5)
        .then(digits.str.zfill(5))
        .otherwise(digits.str.slice(0, 5))
    )


def _load_geoid_reference(path_csv: Path) -> pl.DataFrame:
    """Load the benchmark county reference and normalize county_fips + region."""
    if not path_csv.exists():
        raise FileNotFoundError(f"GEOID reference CSV not found: {path_csv}")
    ref = pl.read_csv(str(path_csv), infer_schema_length=0)
    geoid_col = next((c for c in ref.columns if "geoid" in c.lower()), None)
    if geoid_col is None:
        raise RuntimeError("GEOID reference missing GEOID column.")
    region_col = next((c for c in ref.columns if c.lower() in ("cluster_name", "region", "cluster")), None)
    if region_col is None:
        raise RuntimeError("GEOID reference missing region/cluster column.")
    return (
        ref.select(
            [
                _normalize_fips5_expr(pl.col(geoid_col)).alias("county_fips"),
                pl.col(region_col).cast(pl.Utf8, strict=False).fill_null("").alias("region"),
            ]
        )
        .filter((pl.col("county_fips").str.len_chars() == 5) & ~_blank_expr(pl.col("region")))
        .unique(subset=["county_fips"], keep="first", maintain_order=True)
    )


def _load_zip_to_fips(path_csv: Path) -> pl.DataFrame:
    """Load the ZIP-to-county crosswalk with one primary county per ZIP."""
    if not path_csv.exists():
        raise FileNotFoundError(f"ZIP-to-county CSV not found: {path_csv}")
    df = pl.read_csv(str(path_csv), infer_schema_length=0)
    zip_col = next((c for c in df.columns if "zip" in c.lower()), df.columns[0])
    fips_col = next((c for c in df.columns if "fips" in c.lower()), df.columns[1] if len(df.columns) > 1 else None)
    if fips_col is None:
        raise RuntimeError("ZIP-to-county CSV missing FIPS column.")
    return (
        df.select(
            [
                _normalize_zip5_expr(pl.col(zip_col)).alias("zip5"),
                _normalize_fips5_expr(pl.col(fips_col)).alias("county_fips"),
            ]
        )
        .filter((pl.col("zip5").str.len_chars() == 5) & (pl.col("county_fips").str.len_chars() == 5))
        .unique(subset=["zip5"], keep="first", maintain_order=True)
    )


def _bmf_schema_info(asset: dict[str, Any]) -> dict[str, str]:
    """
    Return schema-specific column names for one BMF asset.

    Raw monthly BMF and legacy BMF use different original schemas. This function
    centralizes those differences so filtering and downstream harmonization can
    stay explicit and well documented.
    """
    if str(asset["asset_group"]) == "legacy_bmf_csv":
        return {
            "variant": "legacy",
            "zip_col": "ZIP5",
            "ein_col": "EIN",
            "name_col": "NAME",
            "state_col": "STATE",
            "subsection_col": "SUBSECCD",
            "ntee_col": "NTEEFINAL",
            "asset_col": "ASSETS",
            "income_col": "INCOME",
            "revenue_col": "CTOTREV",
        }
    return {
        "variant": "raw_monthly",
        "zip_col": "ZIP",
        "ein_col": "EIN",
        "name_col": "NAME",
        "state_col": "STATE",
        "subsection_col": "SUBSECTION",
        "ntee_col": "NTEE_CD",
        "asset_col": "ASSET_AMT",
        "income_col": "INCOME_AMT",
        "revenue_col": "REVENUE_AMT",
    }


def filter_bmf_file_to_benchmark(
    *,
    asset: dict[str, Any],
    local_bmf_path: Path,
    output_path: Path,
    geoid_reference_path: Path,
    zip_to_county_path: Path,
) -> dict[str, Any]:
    """
    Filter one local BMF file to benchmark counties and write Parquet.

    The raw and legacy schemas differ, but the benchmark geography rule is the
    same: use the organization's ZIP to derive county_fips, then keep only rows
    whose county is in GEOID_reference.csv. This keeps the filtered outputs small
    enough that the downstream combined union table can work from already-filtered
    files instead of re-scanning the full raw archives.
    """
    output_path.parent.mkdir(parents=True, exist_ok=True)
    schema = _bmf_schema_info(asset)
    geoid_df = _load_geoid_reference(geoid_reference_path)
    zip_df = _load_zip_to_fips(zip_to_county_path)

    input_lf = pl.scan_csv(str(local_bmf_path), infer_schema_length=0)
    input_columns = input_lf.collect_schema().names()
    zip_col = schema["zip_col"]
    if zip_col not in input_columns:
        raise RuntimeError(f"Expected ZIP column '{zip_col}' missing from {local_bmf_path}.")

    rows_input = input_lf.select(pl.len().alias("n")).collect().item(0, 0)
    print(f"[filter] Source rows in {local_bmf_path.name}: {int(rows_input):,}", flush=True)

    filtered_lf = (
        input_lf.with_columns([_normalize_zip5_expr(pl.col(zip_col)).alias("__zip5")])
        .join(zip_df.lazy(), left_on="__zip5", right_on="zip5", how="left")
        .join(geoid_df.lazy(), on="county_fips", how="left")
        .filter(pl.col("county_fips").is_not_null() & pl.col("region").is_not_null())
        .with_columns(
            [
                pl.lit("zip_to_county").alias("benchmark_match_source"),
                pl.lit(str(asset["snapshot_year"])).alias("bmf_snapshot_year"),
                pl.lit(str(asset["snapshot_month"])).alias("bmf_snapshot_month"),
                pl.lit("True").alias("is_benchmark_county"),
            ]
        )
        .drop("__zip5")
    )

    output_columns = filtered_lf.collect_schema().names()
    print(f"[filter] Output columns for {local_bmf_path.name}: {len(output_columns)}", flush=True)
    try:
        filtered_lf.sink_parquet(str(output_path), compression="zstd")
    except Exception as exc:
        print(
            f"[filter] sink_parquet unavailable for {local_bmf_path.name} ({type(exc).__name__}); "
            "falling back to collect().write_parquet().",
            flush=True,
        )
        filtered_lf.collect().write_parquet(str(output_path), compression="zstd")

    filtered_df = pl.read_parquet(str(output_path))
    rows_output = filtered_df.height
    matched_counties = filtered_df.select(pl.col("county_fips").n_unique()).item()
    return {
        "input_row_count": int(rows_input),
        "output_row_count": int(rows_output),
        "matched_county_fips_count": int(matched_counties),
        "geoid_reference_path": str(geoid_reference_path),
        "zip_to_county_path": str(zip_to_county_path),
        "schema_variant": schema["variant"],
        "zip_column_used": zip_col,
    }
