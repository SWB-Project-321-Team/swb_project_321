"""
Shared helpers for the IRS SOI county ingestion pipeline.

This module centralizes:
- repo/data path constants
- secrets/.env loading
- IRS county page discovery and asset extraction
- local/S3 path construction
- download/upload progress helpers
- county GEOID normalization and benchmark filtering
- manifest/report writing
"""

from __future__ import annotations

import csv
import json
import mimetypes
import os
import re
import sys
import time
from dataclasses import dataclass
from html.parser import HTMLParser
from pathlib import Path
from typing import Any
from urllib.parse import urljoin, urlparse

import boto3
import pandas as pd
import requests
from boto3.s3.transfer import TransferConfig
from botocore.exceptions import ClientError
from tqdm import tqdm

_THIS_FILE = Path(__file__).resolve()
_PYTHON_DIR = _THIS_FILE.parents[2]
if str(_PYTHON_DIR) not in sys.path:
    sys.path.insert(0, str(_PYTHON_DIR))

from utils.paths import DATA, get_base  # noqa: E402

if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(line_buffering=True)

_TQDM_KW = {"file": sys.stdout} if sys.platform == "win32" else {}

LANDING_PAGE_URL = "https://www.irs.gov/statistics/soi-tax-stats-county-data"
YEAR_PAGE_URL_TEMPLATE = "https://www.irs.gov/statistics/soi-tax-stats-county-data-{year}"

RAW_ROOT = DATA / "raw" / "irs_soi" / "county"
RAW_DIR = RAW_ROOT / "raw"
META_DIR = RAW_ROOT / "metadata"
STAGING_DIR = DATA / "staging" / "irs_soi"
LATEST_RELEASE_JSON = META_DIR / "latest_release.json"

DEFAULT_S3_BUCKET = os.environ.get("IRS_990_S3_BUCKET", "swb-321-irs990-teos")
DEFAULT_S3_REGION = os.environ.get("AWS_DEFAULT_REGION", "us-east-2")
RAW_PREFIX = "bronze/irs_soi/county/raw"
META_PREFIX = "bronze/irs_soi/county/metadata"
SILVER_PREFIX = "silver/irs_soi/county"

# Use an explicit TransferConfig instead of boto3 defaults so we can tune single-file upload
# behavior. Defaults here are a bit more aggressive than the managed-transfer defaults and can
# be overridden through env vars if the network or host behaves differently.
S3_MULTIPART_THRESHOLD_MB = int(os.environ.get("IRS_SOI_S3_MULTIPART_THRESHOLD_MB", "5"))
S3_MULTIPART_CHUNKSIZE_MB = int(os.environ.get("IRS_SOI_S3_MULTIPART_CHUNKSIZE_MB", "5"))
S3_MAX_CONCURRENCY = int(os.environ.get("IRS_SOI_S3_MAX_CONCURRENCY", "16"))

GEOID_REFERENCE_CSV = DATA / "reference" / "GEOID_reference.csv"

REQUIRED_ASSET_SPECS = {
    "county_csv_agi": re.compile(r"^\d{2}incyallagi\.csv$", re.IGNORECASE),
    "county_csv_noagi": re.compile(r"^\d{2}incyallnoagi\.csv$", re.IGNORECASE),
    "users_guide_docx": re.compile(r"^\d{2}incydocguide\.docx$", re.IGNORECASE),
}
SOURCE_TYPE_TO_ASSET_TYPE = {
    "agi": "county_csv_agi",
    "noagi": "county_csv_noagi",
}


@dataclass(frozen=True)
class AssetRecord:
    """Single published IRS asset for one county release year."""

    asset_type: str
    source_url: str
    filename: str
    source_content_type: str
    source_content_length_bytes: int | None
    source_last_modified: str | None

    def to_dict(self) -> dict[str, Any]:
        """Serialize the asset into JSON/CSV-friendly primitives."""
        return {
            "asset_type": self.asset_type,
            "source_url": self.source_url,
            "filename": self.filename,
            "source_content_type": self.source_content_type,
            "source_content_length_bytes": self.source_content_length_bytes,
            "source_last_modified": self.source_last_modified,
        }


class _LinkCollector(HTMLParser):
    """Collect anchor href/text pairs from an HTML document."""

    def __init__(self) -> None:
        super().__init__()
        self.links: list[dict[str, str]] = []
        self._href: str | None = None
        self._text_parts: list[str] = []

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        if tag.lower() != "a":
            return
        attr_map = dict(attrs)
        href = attr_map.get("href")
        if href:
            self._href = href
            self._text_parts = []

    def handle_data(self, data: str) -> None:
        if self._href is not None:
            self._text_parts.append(data)

    def handle_endtag(self, tag: str) -> None:
        if tag.lower() != "a" or self._href is None:
            return
        text = " ".join("".join(self._text_parts).split())
        self.links.append({"href": self._href, "text": text})
        self._href = None
        self._text_parts = []


class _TqdmUpload:
    """boto3 callback wrapper so uploads render as tqdm progress bars."""

    def __init__(self, pbar: tqdm) -> None:
        self._pbar = pbar

    def __call__(self, bytes_amount: int) -> None:
        self._pbar.update(bytes_amount)


def now_utc_iso() -> str:
    """Return an ISO-8601 UTC timestamp."""
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())


def banner(title: str) -> None:
    """Print a visible step banner."""
    print("\n" + "=" * 88, flush=True)
    print(title, flush=True)
    print("=" * 88, flush=True)


def print_elapsed(start_ts: float, label: str) -> None:
    """Print elapsed wall-clock time for a stage or operation."""
    elapsed = time.perf_counter() - start_ts
    print(f"[time] {label}: {elapsed:.1f}s ({elapsed / 60:.1f} min)", flush=True)


def load_env_from_secrets() -> None:
    """Load repo-local secrets/.env into os.environ when present."""
    env_path = get_base() / "secrets" / ".env"
    if not env_path.exists():
        print(f"[env] No secrets file found at {env_path}; using current environment.", flush=True)
        return
    loaded = 0
    with open(env_path, encoding="utf-8-sig") as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            key, _, value = line.partition("=")
            key = key.strip()
            value = value.strip()
            if value and value[0] in ("'", '"') and value[-1] == value[0]:
                value = value[1:-1]
            if key and key not in os.environ:
                os.environ[key] = value
                loaded += 1
    print(f"[env] Loaded {loaded} key(s) from {env_path}", flush=True)


def ensure_work_dirs(raw_dir: Path = RAW_DIR, metadata_dir: Path = META_DIR, staging_dir: Path = STAGING_DIR) -> None:
    """Ensure all working directories exist before a script starts."""
    for path in (raw_dir, metadata_dir, staging_dir):
        path.mkdir(parents=True, exist_ok=True)
        print(f"[paths] Ready: {path}", flush=True)


def year_raw_dir(raw_dir: Path, tax_year: int) -> Path:
    """Return the year-specific raw directory for one release."""
    return raw_dir / f"tax_year={tax_year}"


def year_staging_dir(staging_dir: Path, tax_year: int) -> Path:
    """Return the year-specific staging directory for one release."""
    return staging_dir / f"tax_year={tax_year}"


def release_manifest_path(metadata_dir: Path, tax_year: int) -> Path:
    """Return the raw manifest path for one release year."""
    return metadata_dir / f"release_manifest_tax_year={tax_year}.csv"


def size_report_path(metadata_dir: Path, tax_year: int) -> Path:
    """Return the source/local/S3 verification report path."""
    return metadata_dir / f"size_verification_tax_year={tax_year}.csv"


def filter_manifest_path(staging_dir: Path, tax_year: int) -> Path:
    """Return the local filter manifest path for one release year."""
    return year_staging_dir(staging_dir, tax_year) / f"filter_manifest_{tax_year}.csv"


def filtered_output_path(staging_dir: Path, tax_year: int, source_type: str) -> Path:
    """Return the benchmark-filtered CSV path for AGI or no-AGI source data."""
    return year_staging_dir(staging_dir, tax_year) / f"irs_soi_county_benchmark_{source_type}_{tax_year}.csv"


def raw_s3_key(raw_prefix: str, tax_year: int, filename: str) -> str:
    """Return the raw-asset S3 object key."""
    return f"{raw_prefix.rstrip('/')}/tax_year={tax_year}/{filename}"


def meta_s3_key(meta_prefix: str, filename: str) -> str:
    """Return the metadata S3 object key."""
    return f"{meta_prefix.rstrip('/')}/{filename}"


def filtered_s3_key(silver_prefix: str, tax_year: int, filename: str) -> str:
    """Return the filtered derivative S3 object key."""
    return f"{silver_prefix.rstrip('/')}/tax_year={tax_year}/{filename}"


def basename(url: str) -> str:
    """Return the path basename of a URL."""
    return Path(urlparse(url).path).name


def guess_content_type(path: Path) -> str:
    """Infer a content type for S3 uploads from the file extension."""
    ctype, _ = mimetypes.guess_type(str(path))
    return ctype or "application/octet-stream"


def write_json(path: Path, payload: dict[str, Any]) -> None:
    """Write JSON with indentation and a stable UTF-8 encoding."""
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=False), encoding="utf-8")
    print(f"[write] JSON: {path}", flush=True)


def read_json(path: Path) -> dict[str, Any] | None:
    """Read JSON from disk if present, else return None."""
    if not path.exists():
        return None
    return json.loads(path.read_text(encoding="utf-8"))


def write_csv(path: Path, rows: list[dict[str, Any]], fieldnames: list[str]) -> None:
    """Write a CSV file with explicit field ordering."""
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)
    print(f"[write] CSV: {path}", flush=True)


def load_csv_rows(path: Path) -> list[dict[str, str]]:
    """Load CSV rows into a list of dictionaries."""
    with open(path, newline="", encoding="utf-8") as f:
        return list(csv.DictReader(f))


def source_size_cache_key(source_url: str, source_last_modified: str | None) -> str:
    """Build a stable cache key for one remote source asset version."""
    return f"{source_url}|{source_last_modified or ''}"


def _coerce_int(value: Any) -> int | None:
    """Convert digit-like values to int; otherwise return None."""
    if value is None:
        return None
    if isinstance(value, int):
        return value
    text = str(value).strip()
    if text.isdigit():
        return int(text)
    return None


def extract_source_size_cache(release: dict[str, Any] | None) -> dict[str, dict[str, Any]]:
    """Extract any cached source-byte entries from a release JSON payload."""
    cache: dict[str, dict[str, Any]] = {}
    if not release:
        return cache

    raw_cache = release.get("source_size_cache") or {}
    if isinstance(raw_cache, dict):
        for key, entry in raw_cache.items():
            if not isinstance(entry, dict):
                continue
            source_bytes = _coerce_int(entry.get("source_content_length_bytes"))
            source_url = entry.get("source_url")
            if source_bytes is None or not source_url:
                continue
            cache[str(key)] = {
                "source_url": source_url,
                "source_last_modified": entry.get("source_last_modified"),
                "source_content_length_bytes": source_bytes,
                "cached_at_utc": entry.get("cached_at_utc"),
            }

    for asset in release.get("assets", []):
        if not isinstance(asset, dict):
            continue
        source_url = asset.get("source_url")
        source_last_modified = asset.get("source_last_modified")
        source_bytes = _coerce_int(asset.get("source_content_length_bytes"))
        if not source_url or source_bytes is None:
            continue
        cache[source_size_cache_key(source_url, source_last_modified)] = {
            "source_url": source_url,
            "source_last_modified": source_last_modified,
            "source_content_length_bytes": source_bytes,
            "cached_at_utc": now_utc_iso(),
        }

    return cache


def load_source_size_cache_from_manifest(manifest_path: Path) -> dict[str, dict[str, Any]]:
    """Load source-byte cache entries from the raw manifest when available."""
    if not manifest_path.exists():
        return {}
    cache: dict[str, dict[str, Any]] = {}
    for row in load_csv_rows(manifest_path):
        source_url = row.get("source_url", "").strip()
        source_last_modified = row.get("source_last_modified") or None
        source_bytes = _coerce_int(row.get("source_content_length_bytes"))
        if not source_url or source_bytes is None:
            continue
        cache[source_size_cache_key(source_url, source_last_modified)] = {
            "source_url": source_url,
            "source_last_modified": source_last_modified,
            "source_content_length_bytes": source_bytes,
            "cached_at_utc": now_utc_iso(),
        }
    return cache


def apply_source_size_cache_to_release(
    release: dict[str, Any],
    cache_entries: dict[str, dict[str, Any]],
) -> dict[str, Any]:
    """Merge cache entries into release JSON and populate asset source bytes when missing."""
    release_cache = extract_source_size_cache(release)
    release_cache.update(cache_entries)

    for asset in release.get("assets", []):
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
    source_url: str,
    source_last_modified: str | None,
    source_content_length_bytes: int,
) -> dict[str, Any]:
    """Persist one source-byte value into the release cache and matching asset entry."""
    cache_entry = {
        "source_url": source_url,
        "source_last_modified": source_last_modified,
        "source_content_length_bytes": int(source_content_length_bytes),
        "cached_at_utc": now_utc_iso(),
    }
    key = source_size_cache_key(source_url, source_last_modified)
    release = apply_source_size_cache_to_release(release, {key: cache_entry})
    return release


def read_irs_csv(path: Path) -> pd.DataFrame:
    """Read an IRS CSV with a small encoding fallback chain."""
    last_error: Exception | None = None
    for encoding in ("utf-8-sig", "latin-1"):
        try:
            return pd.read_csv(path, dtype=str, keep_default_na=False, low_memory=False, encoding=encoding)
        except UnicodeDecodeError as exc:
            last_error = exc
    if last_error is not None:
        raise last_error
    raise RuntimeError(f"Could not read CSV: {path}")


def fetch_text(url: str, timeout: int = 60) -> str:
    """GET one HTML/text page from the IRS and return its decoded body."""
    response = requests.get(url, timeout=timeout)
    response.raise_for_status()
    return response.text


def collect_links(html: str) -> list[dict[str, str]]:
    """Extract anchor href/text pairs from an HTML page."""
    parser = _LinkCollector()
    parser.feed(html)
    return parser.links


def extract_page_last_reviewed(html: str) -> str | None:
    """Extract the IRS 'Page Last Reviewed or Updated' date when present."""
    match = re.search(r"Page Last Reviewed or Updated:\s*([0-9A-Za-z-]+)", html, flags=re.IGNORECASE)
    if not match:
        return None
    return match.group(1).strip()


def discover_latest_year_from_html(html: str) -> int:
    """Parse the landing page and return the highest linked county release year."""
    years: set[int] = set()
    for link in collect_links(html):
        href = link.get("href", "")
        text = link.get("text", "")
        href_match = re.search(r"county-data-(20\d{2})", href)
        text_match = re.fullmatch(r"20\d{2}", text)
        if href_match:
            years.add(int(href_match.group(1)))
        if text_match:
            years.add(int(text))
    if not years:
        raise ValueError("Could not discover any county release years from IRS landing page.")
    return max(years)


def _probe_with_get(url: str, timeout: int = 60) -> dict[str, Any]:
    """Fallback header probe when HEAD is unavailable or incomplete."""
    with requests.get(url, stream=True, timeout=timeout) as response:
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


def probe_url_head(url: str, timeout: int = 60) -> dict[str, Any]:
    """Probe a remote URL for size/last-modified metadata."""
    try:
        response = requests.head(url, allow_redirects=True, timeout=timeout)
        response.raise_for_status()
        headers = response.headers
        content_length = headers.get("Content-Length")
        content_encoding = headers.get("Content-Encoding", "").lower()
        result = {
            "status_code": response.status_code,
            "content_length": (
                int(content_length)
                if content_length and content_length.isdigit() and content_encoding != "gzip"
                else None
            ),
            "last_modified": headers.get("Last-Modified"),
            "content_type": headers.get("Content-Type"),
        }
        return result
    except requests.RequestException:
        return _probe_with_get(url, timeout=timeout)


def extract_assets_for_year_page(html: str, page_url: str, tax_year: int) -> list[AssetRecord]:
    """Extract the three required county asset URLs from one year page."""
    del tax_year  # year-specific validation is handled by the filename regexes
    found: dict[str, str] = {}
    for link in collect_links(html):
        href = link.get("href", "")
        if not href:
            continue
        abs_url = urljoin(page_url, href)
        filename = basename(abs_url).lower()
        for asset_type, pattern in REQUIRED_ASSET_SPECS.items():
            if pattern.match(filename):
                if asset_type in found:
                    # IRS pages sometimes repeat the exact same asset link in multiple sections.
                    # That is safe to ignore, but conflicting URLs should still fail loudly.
                    if found[asset_type] == abs_url:
                        continue
                    raise ValueError(f"Duplicate asset match for {asset_type}: {found[asset_type]} and {abs_url}")
                found[asset_type] = abs_url

    missing = [asset_type for asset_type in REQUIRED_ASSET_SPECS if asset_type not in found]
    if missing:
        raise ValueError(f"Missing required county assets on {page_url}: {', '.join(missing)}")

    assets: list[AssetRecord] = []
    for asset_type in ("county_csv_agi", "county_csv_noagi", "users_guide_docx"):
        source_url = found[asset_type]
        meta = probe_url_head(source_url)
        content_type = meta.get("content_type") or guess_content_type(Path(basename(source_url)))
        assets.append(
            AssetRecord(
                asset_type=asset_type,
                source_url=source_url,
                filename=basename(source_url),
                source_content_type=content_type,
                source_content_length_bytes=meta.get("content_length"),
                source_last_modified=meta.get("last_modified"),
            )
        )
    return assets


def discover_release(year_arg: str = "latest") -> dict[str, Any]:
    """Resolve a year argument into one discovered IRS county release payload."""
    year_arg_clean = str(year_arg).strip().lower()
    landing_html = fetch_text(LANDING_PAGE_URL)
    if year_arg_clean == "latest":
        tax_year = discover_latest_year_from_html(landing_html)
        selection_mode = "latest"
    else:
        tax_year = int(year_arg_clean)
        selection_mode = "explicit"

    year_page_url = YEAR_PAGE_URL_TEMPLATE.format(year=tax_year)
    year_html = fetch_text(year_page_url)
    assets = extract_assets_for_year_page(year_html, year_page_url, tax_year)
    payload = {
        "landing_page_url": LANDING_PAGE_URL,
        "year_page_url": year_page_url,
        "tax_year": tax_year,
        "year_selection": selection_mode,
        "irs_page_last_reviewed": extract_page_last_reviewed(year_html),
        "discovered_at_utc": now_utc_iso(),
        "assets": [asset.to_dict() for asset in assets],
    }
    return payload


def resolve_release_and_write_metadata(year_arg: str, metadata_dir: Path) -> dict[str, Any]:
    """Discover a release and persist latest_release.json under the metadata directory."""
    latest_path = metadata_dir / LATEST_RELEASE_JSON.name
    existing_release = read_json(latest_path)
    release = discover_release(year_arg)
    release = apply_source_size_cache_to_release(release, extract_source_size_cache(existing_release))
    write_json(latest_path, release)
    return release


def select_release_assets(release: dict[str, Any], source_type: str = "both") -> list[dict[str, Any]]:
    """Return the requested AGI/no-AGI assets from a discovered release."""
    if source_type not in ("agi", "noagi", "both"):
        raise ValueError("source_type must be one of: agi, noagi, both")
    assets = [asset for asset in release["assets"] if asset["asset_type"] in SOURCE_TYPE_TO_ASSET_TYPE.values()]
    if source_type == "both":
        return sorted(assets, key=lambda a: a["asset_type"])
    asset_type = SOURCE_TYPE_TO_ASSET_TYPE[source_type]
    return [asset for asset in assets if asset["asset_type"] == asset_type]


def s3_client(region: str) -> Any:
    """Build an S3 client for a specific target region."""
    return boto3.Session(region_name=region).client("s3")


def s3_transfer_config() -> TransferConfig:
    """Build the custom managed-transfer config used for SOI uploads."""
    return TransferConfig(
        multipart_threshold=S3_MULTIPART_THRESHOLD_MB * 1024 * 1024,
        multipart_chunksize=S3_MULTIPART_CHUNKSIZE_MB * 1024 * 1024,
        max_concurrency=S3_MAX_CONCURRENCY,
        use_threads=True,
    )


def download_with_progress(url: str, output_path: Path, expected_bytes: int | None = None, timeout: int = 120) -> int:
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
        with tqdm(
            total=total,
            unit="B",
            unit_scale=True,
            unit_divisor=1024,
            desc=f"download {output_path.name}",
            **_TQDM_KW,
        ) as pbar:
            with open(output_path, "wb") as f:
                for chunk in response.iter_content(chunk_size=1024 * 1024):
                    if not chunk:
                        continue
                    f.write(chunk)
                    pbar.update(len(chunk))
    return output_path.stat().st_size


def measure_remote_streamed_bytes(url: str, timeout: int = 120) -> int:
    """Stream a remote asset and return the number of decoded bytes delivered."""
    total = 0
    with requests.get(url, stream=True, timeout=timeout) as response:
        response.raise_for_status()
        for chunk in response.iter_content(chunk_size=1024 * 1024):
            if not chunk:
                continue
            total += len(chunk)
    return total


def upload_file_with_progress(
    local_path: Path,
    bucket: str,
    key: str,
    region: str,
    extra_args: dict[str, Any] | None = None,
) -> None:
    """Upload one local file to S3 with a visible byte progress bar."""
    client = s3_client(region)
    size = local_path.stat().st_size
    transfer_config = s3_transfer_config()
    print(
        "[upload-config] "
        f"threshold={S3_MULTIPART_THRESHOLD_MB}MB "
        f"chunk={S3_MULTIPART_CHUNKSIZE_MB}MB "
        f"concurrency={S3_MAX_CONCURRENCY}",
        flush=True,
    )
    with tqdm(
        total=size,
        unit="B",
        unit_scale=True,
        unit_divisor=1024,
        desc=f"upload {local_path.name}",
        **_TQDM_KW,
    ) as pbar:
        callback = _TqdmUpload(pbar)
        client.upload_file(
            str(local_path),
            bucket,
            key,
            ExtraArgs=extra_args or {},
            Callback=callback,
            Config=transfer_config,
        )


def s3_object_size(bucket: str, key: str, region: str) -> int | None:
    """Return the S3 ContentLength for one object, or None when missing."""
    client = s3_client(region)
    try:
        response = client.head_object(Bucket=bucket, Key=key)
        return int(response.get("ContentLength", 0))
    except ClientError:
        return None


def compute_size_match(source_bytes: int | None, local_bytes: int | None, s3_bytes: int | None) -> bool:
    """True when source/local/S3 sizes are all present and identical."""
    return (
        source_bytes is not None
        and local_bytes is not None
        and s3_bytes is not None
        and source_bytes == local_bytes == s3_bytes
    )


def compute_local_s3_match(local_bytes: int | None, s3_bytes: int | None) -> bool:
    """True when local and S3 sizes are both present and identical."""
    return local_bytes is not None and s3_bytes is not None and local_bytes == s3_bytes


def _find_region_column_geoid(df: pd.DataFrame) -> str | None:
    """Return the most likely region/cluster column from GEOID_reference.csv."""
    for column in df.columns:
        lowered = str(column).lower()
        if lowered in ("region", "cluster_name", "cluster", "benchmark_region", "area"):
            return column
    if "Region" in df.columns:
        return "Region"
    return None


def load_geoid_reference_set(csv_path: Path) -> tuple[set[str], dict[str, str] | None]:
    """Load benchmark county GEOIDs and their region labels from GEOID_reference.csv."""
    if not csv_path.exists():
        raise FileNotFoundError(f"GEOID reference not found: {csv_path}")
    df = pd.read_csv(csv_path)
    geoid_col = next((c for c in df.columns if "geoid" in str(c).lower()), None)
    if not geoid_col:
        raise ValueError(f"GEOID reference missing GEOID column: {csv_path}")

    geoid_norm = df[geoid_col].astype(str).str.strip().str.zfill(5)
    valid = geoid_norm.str.len() == 5
    geoid_set = set(geoid_norm[valid].unique())
    region_col = _find_region_column_geoid(df)
    geoid_to_region = None
    if region_col:
        geoid_to_region = dict(zip(geoid_norm[valid], df.loc[valid, region_col].astype(str).str.strip()))
    return (geoid_set, geoid_to_region)


def normalize_state_fips(value: Any) -> str:
    """Normalize a STATEFIPS value into a 2-digit string."""
    digits = "".join(ch for ch in str(value).strip() if ch.isdigit())
    if not digits:
        return ""
    return digits[:2].zfill(2)


def normalize_county_fips_component(value: Any) -> str:
    """Normalize a COUNTYFIPS value into a 3-digit county component."""
    digits = "".join(ch for ch in str(value).strip() if ch.isdigit())
    if not digits:
        return ""
    return digits[:3].zfill(3)


def derive_county_fips(state_fips: Any, county_fips: Any) -> str:
    """Build a 5-digit county GEOID/FIPS from STATEFIPS and COUNTYFIPS."""
    state_part = normalize_state_fips(state_fips)
    county_part = normalize_county_fips_component(county_fips)
    if len(state_part) != 2 or len(county_part) != 3:
        return ""
    return f"{state_part}{county_part}"


def filter_county_dataframe(
    df: pd.DataFrame,
    geoid_reference_set: set[str],
    geoid_to_region: dict[str, str] | None,
) -> tuple[pd.DataFrame, int]:
    """Filter county-level SOI rows to benchmark counties and append region metadata."""
    if "STATEFIPS" not in df.columns or "COUNTYFIPS" not in df.columns:
        raise ValueError("SOI county CSV must contain STATEFIPS and COUNTYFIPS columns.")

    state_norm = df["STATEFIPS"].map(normalize_state_fips)
    county_norm = df["COUNTYFIPS"].map(normalize_county_fips_component)
    county_fips = state_norm + county_norm

    # The IRS county CSV includes state-total rows with COUNTYFIPS=000. Keep them in raw files,
    # but exclude them from benchmark outputs because the benchmark geography is county-based.
    mask = (county_norm != "000") & county_fips.isin(geoid_reference_set)
    filtered = df.loc[mask].copy()
    filtered["county_fips"] = county_fips.loc[filtered.index]
    if geoid_to_region:
        filtered["region"] = filtered["county_fips"].map(geoid_to_region)
    else:
        filtered["region"] = None
    filtered["is_benchmark_county"] = True
    return (filtered, filtered["county_fips"].nunique())
