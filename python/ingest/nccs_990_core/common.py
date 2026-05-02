"""
Shared helpers for the NCCS Core 990 ingestion pipeline.
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

from ingest._shared import transfers as shared_transfers  # noqa: E402
from utils.paths import DATA, get_base  # noqa: E402

if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(line_buffering=True)

_TQDM_KW = shared_transfers.TQDM_KW

CORE_CATALOG_URL = "https://nccs.urban.org/nccs/catalogs/catalog-core.html"
BMF_CATALOG_URL = "https://nccs.urban.org/nccs/catalogs/catalog-bmf.html"

RAW_ROOT = DATA / "raw" / "nccs_990" / "core"
CORE_RAW_DIR = RAW_ROOT / "raw"
BRIDGE_BMF_DIR = RAW_ROOT / "bridge_bmf"
META_DIR = RAW_ROOT / "metadata"
STAGING_DIR = DATA / "staging" / "nccs_990" / "core"
DOCS_PACKAGE_DIR = get_base() / "docs" / "final_preprocessing_docs"
DOCS_TECHNICAL_DIR = DOCS_PACKAGE_DIR / "technical_docs"
DOCS_ANALYSIS_DIR = DOCS_TECHNICAL_DIR / "analysis_variable_mappings"
DOCS_DATA_PROCESSING_DIR = DOCS_TECHNICAL_DIR / "pipeline_docs"

LATEST_RELEASE_JSON = META_DIR / "latest_release.json"
CORE_CATALOG_SNAPSHOT = META_DIR / "catalog_core.html"
BMF_CATALOG_SNAPSHOT = META_DIR / "catalog_bmf.html"

DEFAULT_S3_BUCKET = os.environ.get("IRS_990_S3_BUCKET", "swb-321-irs990-teos")
DEFAULT_S3_REGION = os.environ.get("AWS_DEFAULT_REGION", "us-east-2")
RAW_PREFIX = "bronze/nccs_990/core/raw"
BRIDGE_PREFIX = "bronze/nccs_990/core/bridge_bmf"
META_PREFIX = "bronze/nccs_990/core/metadata"
SILVER_PREFIX = "silver/nccs_990/core"
ANALYSIS_PREFIX = f"{SILVER_PREFIX}/analysis"
ANALYSIS_DOCUMENTATION_PREFIX = f"{ANALYSIS_PREFIX}/documentation"
ANALYSIS_VARIABLE_MAPPING_PREFIX = f"{ANALYSIS_PREFIX}/variable_mappings"
ANALYSIS_COVERAGE_PREFIX = f"{ANALYSIS_PREFIX}/quality/coverage"

GEOID_REFERENCE_CSV = DATA / "reference" / "GEOID_reference.csv"
ZIP_TO_COUNTY_CSV = DATA / "reference" / "zip_to_county_fips.csv"

_TRANSFER_SETTINGS = shared_transfers.transfer_settings()
S3_MULTIPART_THRESHOLD_MB = _TRANSFER_SETTINGS.s3_multipart_threshold_mb
S3_MULTIPART_CHUNKSIZE_MB = _TRANSFER_SETTINGS.s3_multipart_chunksize_mb
S3_MAX_CONCURRENCY = _TRANSFER_SETTINGS.s3_max_concurrency
DOWNLOAD_WORKERS = shared_transfers.DOWNLOAD_WORKERS_DEFAULT
UPLOAD_WORKERS = shared_transfers.UPLOAD_WORKERS_DEFAULT
VERIFY_WORKERS = shared_transfers.VERIFY_WORKERS_DEFAULT

CORE_GROUP_SPECS = {
    "501c3_charities_pz": {
        "family": "501c3_charities",
        "scope": "pz",
        "regex": re.compile(r"^CORE-(?P<year>\d{4})-501C3-CHARITIES-PZ-HRMN\.csv$", re.IGNORECASE),
    },
    "501c3_charities_pc": {
        "family": "501c3_charities",
        "scope": "pc",
        "regex": re.compile(r"^CORE-(?P<year>\d{4})-501C3-CHARITIES-PC-HRMN\.csv$", re.IGNORECASE),
    },
    "501ce_nonprofit_pz": {
        "family": "501ce_nonprofit",
        "scope": "pz",
        "regex": re.compile(r"^CORE-(?P<year>\d{4})-501CE-NONPROFIT-PZ-HRMN\.csv$", re.IGNORECASE),
    },
    "501ce_nonprofit_pc": {
        "family": "501ce_nonprofit",
        "scope": "pc",
        "regex": re.compile(r"^CORE-(?P<year>\d{4})-501CE-NONPROFIT-PC-HRMN\.csv$", re.IGNORECASE),
    },
    "501c3_privfound_pf": {
        "family": "501c3_privfound",
        "scope": "pf",
        "regex": re.compile(r"^CORE-(?P<year>\d{4})-501C3-PRIVFOUND-PF-HRMN-V0\.csv$", re.IGNORECASE),
    },
}

CORE_DICTIONARY_SPECS = {
    "core_dictionary_pc_pz": {
        "family": "core",
        "scope": "dictionary",
        "regex": re.compile(r"^CORE-HRMN_dd\.csv$", re.IGNORECASE),
    },
    "core_dictionary_pf": {
        "family": "501c3_privfound",
        "scope": "dictionary",
        "regex": re.compile(r"^DD-PF-HRMN-V0\.csv$", re.IGNORECASE),
    },
}

BMF_DICTIONARY_SPEC = {
    "asset_type": "bmf_dictionary",
    "family": "unified_bmf",
    "scope": "dictionary",
    "regex": re.compile(r"^harmonized_data_dictionary\.xlsx$", re.IGNORECASE),
}

BMF_STATE_REGEX = re.compile(r"^(?P<state>[A-Z]{2})_BMF_V1\.1\.csv$", re.IGNORECASE)

DEFAULT_CORE_SOURCE_TYPES = list(CORE_GROUP_SPECS.keys())

STATE_FIPS_TO_ABBR = {
    "01": "AL",
    "02": "AK",
    "04": "AZ",
    "05": "AR",
    "06": "CA",
    "08": "CO",
    "09": "CT",
    "10": "DE",
    "11": "DC",
    "12": "FL",
    "13": "GA",
    "15": "HI",
    "16": "ID",
    "17": "IL",
    "18": "IN",
    "19": "IA",
    "20": "KS",
    "21": "KY",
    "22": "LA",
    "23": "ME",
    "24": "MD",
    "25": "MA",
    "26": "MI",
    "27": "MN",
    "28": "MS",
    "29": "MO",
    "30": "MT",
    "31": "NE",
    "32": "NV",
    "33": "NH",
    "34": "NJ",
    "35": "NM",
    "36": "NY",
    "37": "NC",
    "38": "ND",
    "39": "OH",
    "40": "OK",
    "41": "OR",
    "42": "PA",
    "44": "RI",
    "45": "SC",
    "46": "SD",
    "47": "TN",
    "48": "TX",
    "49": "UT",
    "50": "VT",
    "51": "VA",
    "53": "WA",
    "54": "WV",
    "55": "WI",
    "56": "WY",
    "60": "AS",
    "66": "GU",
    "69": "MP",
    "72": "PR",
    "78": "VI",
}


@dataclass(frozen=True)
class AssetRecord:
    """Single downloadable NCCS asset selected for one run."""

    asset_type: str
    asset_group: str
    family: str
    scope: str
    source_url: str
    filename: str
    year: int | None
    year_basis: str
    benchmark_state: str | None
    source_content_type: str
    source_content_length_bytes: int | None
    source_last_modified: str | None

    def to_dict(self) -> dict[str, Any]:
        return {
            "asset_type": self.asset_type,
            "asset_group": self.asset_group,
            "family": self.family,
            "scope": self.scope,
            "source_url": self.source_url,
            "filename": self.filename,
            "year": self.year,
            "year_basis": self.year_basis,
            "benchmark_state": self.benchmark_state,
            "source_content_type": self.source_content_type,
            "source_content_length_bytes": self.source_content_length_bytes,
            "source_last_modified": self.source_last_modified,
        }


class _LinkCollector(HTMLParser):
    """Collect anchor href/text pairs from HTML."""

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


print_transfer_settings = shared_transfers.print_transfer_settings
parallel_map = shared_transfers.parallel_map
batch_s3_object_sizes = shared_transfers.batch_s3_object_sizes


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


def ensure_work_dirs(
    core_raw_dir: Path = CORE_RAW_DIR,
    bridge_dir: Path = BRIDGE_BMF_DIR,
    metadata_dir: Path = META_DIR,
    staging_dir: Path = STAGING_DIR,
) -> None:
    """Ensure all NCCS working directories exist before a script starts."""
    for path in (core_raw_dir, bridge_dir, metadata_dir, staging_dir):
        path.mkdir(parents=True, exist_ok=True)
        print(f"[paths] Ready: {path}", flush=True)


def year_core_raw_dir(core_raw_dir: Path, tax_year: int) -> Path:
    """Return the year-specific Core raw directory."""
    return core_raw_dir / f"year={tax_year}"


def state_bridge_dir(bridge_dir: Path, state_abbr: str) -> Path:
    """Return the state-specific Unified BMF local directory."""
    return bridge_dir / f"state={state_abbr.upper()}"


def release_manifest_path(metadata_dir: Path, tax_year: int) -> Path:
    """Return the raw manifest path for one discovered release year."""
    return metadata_dir / f"release_manifest_year={tax_year}.csv"


def size_report_path(metadata_dir: Path, tax_year: int) -> Path:
    """Return the source/local/S3 verification report path."""
    return metadata_dir / f"size_verification_year={tax_year}.csv"


def year_staging_dir(staging_dir: Path, tax_year: int) -> Path:
    """Return the year-specific staging directory."""
    return staging_dir / f"year={tax_year}"


def filter_manifest_path(staging_dir: Path, tax_year: int) -> Path:
    """Return the local benchmark-filter manifest path."""
    return year_staging_dir(staging_dir, tax_year) / f"filter_manifest_year={tax_year}.csv"


def filtered_output_path(staging_dir: Path, tax_year: int, filename: str) -> Path:
    """Return the filtered output path for one original Core filename."""
    stem = Path(filename).stem
    return year_staging_dir(staging_dir, tax_year) / f"{stem}__benchmark.csv"


def combined_filtered_output_path(staging_dir: Path, tax_year: int) -> Path:
    """Return the combined filtered Core parquet path for one year."""
    return year_staging_dir(staging_dir, tax_year) / f"nccs_990_core_combined_filtered_year={tax_year}.parquet"


def raw_s3_key(raw_prefix: str, tax_year: int, filename: str) -> str:
    """Return the S3 key for one Core raw CSV."""
    return f"{raw_prefix.rstrip('/')}/year={tax_year}/{filename}"


def bridge_s3_key(bridge_prefix: str, state_abbr: str, filename: str) -> str:
    """Return the S3 key for one Unified BMF state CSV."""
    return f"{bridge_prefix.rstrip('/')}/state={state_abbr.upper()}/{filename}"


def meta_s3_key(meta_prefix: str, filename: str) -> str:
    """Return the S3 key for one metadata object."""
    return f"{meta_prefix.rstrip('/')}/{filename}"


def filtered_s3_key(silver_prefix: str, tax_year: int, filename: str) -> str:
    """Return the S3 key for one filtered benchmark output."""
    return f"{silver_prefix.rstrip('/')}/year={tax_year}/{filename}"


def analysis_variables_output_path(staging_dir: Path = STAGING_DIR) -> Path:
    """Return the harmonized NCCS Core analysis row-level parquet path."""
    return staging_dir / "nccs_990_core_analysis_variables.parquet"


def analysis_geography_metrics_output_path(staging_dir: Path = STAGING_DIR) -> Path:
    """Return the region-level NCCS Core analysis metrics parquet path."""
    return staging_dir / "nccs_990_core_analysis_geography_metrics.parquet"


def analysis_variable_coverage_path(metadata_dir: Path = META_DIR) -> Path:
    """Return the NCCS Core analysis coverage CSV path."""
    return metadata_dir / "nccs_990_core_analysis_variable_coverage.csv"


def analysis_variable_mapping_path() -> Path:
    """Return the NCCS Core analysis mapping Markdown path."""
    return DOCS_ANALYSIS_DIR / "nccs_990_core_analysis_variable_mapping.md"


def analysis_data_processing_doc_path() -> Path:
    """Return the NCCS Core pipeline data-processing doc path."""
    return DOCS_DATA_PROCESSING_DIR / "nccs_990_core_pipeline.md"


def analysis_s3_key(analysis_prefix: str, filename: str) -> str:
    """Return the S3 key for one NCCS Core analysis artifact."""
    return f"{analysis_prefix.rstrip('/')}/{filename}"


def analysis_auxiliary_s3_key(prefix: str, filename: str) -> str:
    """Return the S3 key for one NCCS Core analysis documentation or quality artifact."""
    return f"{prefix.rstrip('/')}/{filename}"


def basename(url: str) -> str:
    """Return the path basename of a URL."""
    return Path(urlparse(url).path).name


def guess_content_type(path: Path) -> str:
    """Infer a content type for S3 uploads from the file extension."""
    ctype, _ = mimetypes.guess_type(str(path))
    return ctype or "application/octet-stream"


def write_json(path: Path, payload: dict[str, Any]) -> None:
    """Write JSON with indentation and UTF-8 encoding."""
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=False), encoding="utf-8")
    print(f"[write] JSON: {path}", flush=True)


def read_json(path: Path) -> dict[str, Any] | None:
    """Read JSON from disk if present, else return None."""
    if not path.exists():
        return None
    return json.loads(path.read_text(encoding="utf-8"))


def write_text(path: Path, text: str) -> None:
    """Write a plain-text file to disk."""
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8")
    print(f"[write] Text: {path}", flush=True)


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


def fetch_text(url: str, timeout: int = 60) -> str:
    """GET one HTML/text page and return the decoded body."""
    response = shared_transfers.managed_requests("GET", url, timeout=timeout)
    with response:
        response.raise_for_status()
        return response.text


def collect_links(html: str) -> list[dict[str, str]]:
    """Extract anchor href/text pairs from an HTML page."""
    parser = _LinkCollector()
    parser.feed(html)
    return parser.links


def _probe_with_get(url: str, timeout: int = 60) -> dict[str, Any]:
    """Fallback header probe when HEAD is unavailable or incomplete."""
    response = shared_transfers.managed_requests("GET", url, stream=True, timeout=timeout)
    with response:
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
        response = shared_transfers.managed_requests("HEAD", url, allow_redirects=True, timeout=timeout)
        with response:
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
    except requests.RequestException:
        return _probe_with_get(url, timeout=timeout)


def parse_core_catalog_html(html: str, page_url: str = CORE_CATALOG_URL) -> dict[str, Any]:
    """Parse the NCCS Core catalog into year/group maps and dictionary URLs."""
    year_assets_by_group: dict[str, dict[int, str]] = {group: {} for group in CORE_GROUP_SPECS}
    dictionary_urls: dict[str, str] = {}

    for link in collect_links(html):
        href = link.get("href", "")
        if not href:
            continue
        abs_url = urljoin(page_url, href)
        filename = basename(abs_url)

        for asset_type, spec in CORE_DICTIONARY_SPECS.items():
            if spec["regex"].match(filename):
                dictionary_urls[asset_type] = abs_url

        for group, spec in CORE_GROUP_SPECS.items():
            match = spec["regex"].match(filename)
            if match:
                year = int(match.group("year"))
                year_assets_by_group[group][year] = abs_url

    missing_groups = [group for group, assets in year_assets_by_group.items() if not assets]
    if missing_groups:
        raise ValueError(f"NCCS Core catalog missing required asset groups: {', '.join(missing_groups)}")

    missing_dicts = [asset_type for asset_type in CORE_DICTIONARY_SPECS if asset_type not in dictionary_urls]
    if missing_dicts:
        raise ValueError(f"NCCS Core catalog missing required dictionaries: {', '.join(missing_dicts)}")

    return {
        "year_assets_by_group": year_assets_by_group,
        "dictionary_urls": dictionary_urls,
    }


def parse_bmf_catalog_html(html: str, page_url: str = BMF_CATALOG_URL) -> dict[str, Any]:
    """Parse the NCCS Unified BMF catalog into state maps and dictionary URL."""
    state_urls: dict[str, str] = {}
    dictionary_url: str | None = None

    for link in collect_links(html):
        href = link.get("href", "")
        if not href:
            continue
        abs_url = urljoin(page_url, href)
        filename = basename(abs_url)

        if BMF_DICTIONARY_SPEC["regex"].match(filename):
            dictionary_url = abs_url
            continue

        match = BMF_STATE_REGEX.match(filename)
        if match and "/harmonized/bmf/unified/" in abs_url:
            state_urls[match.group("state").upper()] = abs_url

    if dictionary_url is None:
        raise ValueError("NCCS Unified BMF catalog missing harmonized data dictionary link.")

    return {
        "state_urls": state_urls,
        "dictionary_url": dictionary_url,
    }


def latest_common_core_year(year_assets_by_group: dict[str, dict[int, str]]) -> int:
    """Return the latest year present across all required NCCS Core groups."""
    common_years: set[int] | None = None
    for assets_by_year in year_assets_by_group.values():
        years = set(assets_by_year)
        common_years = years if common_years is None else common_years & years
    if not common_years:
        raise ValueError("No common NCCS Core year exists across the required groups.")
    return max(common_years)


def normalize_state_abbreviation(value: str) -> str:
    """Normalize a state abbreviation to uppercase ASCII letters."""
    letters = "".join(ch for ch in str(value).upper().strip() if ch.isalpha())
    return letters[:2]


def parse_benchmark_states_arg(value: str | None) -> list[str] | None:
    """Parse a comma/space-separated benchmark state override string."""
    if value is None:
        return None
    pieces = re.split(r"[,\s]+", value.strip())
    states = sorted({normalize_state_abbreviation(piece) for piece in pieces if piece.strip()})
    return [state for state in states if state]


def _find_region_column_geoid(df: pd.DataFrame) -> str | None:
    """Return the most likely region/cluster column from GEOID_reference.csv."""
    for column in df.columns:
        lowered = str(column).lower()
        if lowered in ("region", "cluster_name", "cluster", "benchmark_region", "area"):
            return column
    if "Region" in df.columns:
        return "Region"
    return None


def derive_benchmark_states(geoid_reference_path: Path) -> list[str]:
    """Derive the benchmark-state abbreviation list from GEOID_reference.csv."""
    if not geoid_reference_path.exists():
        raise FileNotFoundError(f"GEOID reference not found: {geoid_reference_path}")
    ref = pd.read_csv(geoid_reference_path)
    geoid_col = next((c for c in ref.columns if "geoid" in str(c).lower()), None)
    if not geoid_col:
        raise ValueError(f"GEOID reference missing GEOID column: {geoid_reference_path}")

    state_fips = sorted({str(value).strip().zfill(5)[:2] for value in ref[geoid_col].dropna()})
    states: list[str] = []
    unknown: list[str] = []
    for state_code in state_fips:
        abbr = STATE_FIPS_TO_ABBR.get(state_code)
        if not abbr:
            unknown.append(state_code)
            continue
        states.append(abbr)
    if unknown:
        raise ValueError(f"Unknown state FIPS codes in GEOID reference: {', '.join(unknown)}")
    return states


def load_geoid_reference_set(csv_path: Path) -> tuple[set[str], dict[str, str] | None]:
    """Load benchmark county GEOIDs and their region labels from GEOID_reference.csv."""
    ref = load_geoid_reference_frame(csv_path)
    geoid_set = set(ref["county_fips"].tolist())
    geoid_to_region = dict(zip(ref["county_fips"], ref["region"]))
    return geoid_set, geoid_to_region


def load_geoid_reference_frame(csv_path: Path) -> pd.DataFrame:
    """Load benchmark county GEOIDs plus normalized region and state labels."""
    if not csv_path.exists():
        raise FileNotFoundError(f"GEOID reference not found: {csv_path}")
    df = pd.read_csv(csv_path)
    geoid_col = next((c for c in df.columns if "geoid" in str(c).lower()), None)
    if not geoid_col:
        raise ValueError(f"GEOID reference missing GEOID column: {csv_path}")

    geoid_norm = df[geoid_col].astype(str).str.strip().str.zfill(5)
    valid = geoid_norm.str.len() == 5
    region_col = _find_region_column_geoid(df)
    if region_col is None:
        raise ValueError(f"GEOID reference missing region/cluster column: {csv_path}")
    state_col = next((c for c in df.columns if str(c).lower() == "state"), None)
    state_series = (
        df.loc[valid, state_col].astype(str).str.strip().str.upper()
        if state_col is not None
        else pd.Series([""] * int(valid.sum()), index=df.index[valid], dtype=object)
    )
    ref = pd.DataFrame(
        {
            "county_fips": geoid_norm[valid],
            "region": df.loc[valid, region_col].astype(str).str.strip(),
            "benchmark_state": state_series,
        }
    )
    ref = ref[(ref["county_fips"] != "") & (ref["region"] != "")].drop_duplicates(subset=["county_fips"], keep="first").copy()
    return ref


def normalize_zip5(value: Any) -> str:
    """Normalize a ZIP-like value into a 5-digit ZIP when possible."""
    digits = "".join(ch for ch in str(value).strip() if ch.isdigit())
    if len(digits) < 5:
        return ""
    return digits[:5]


def normalize_fips5(value: Any) -> str:
    """Normalize a county/GEOID-like value into a 5-digit county FIPS."""
    digits = "".join(ch for ch in str(value).strip() if ch.isdigit())
    if not digits:
        return ""
    if len(digits) < 5:
        return digits.zfill(5)
    return digits[:5]


def normalize_ein9(value: Any) -> str:
    """Normalize an EIN-like value into a 9-digit EIN string."""
    digits = "".join(ch for ch in str(value).strip() if ch.isdigit())
    if not digits:
        return ""
    if len(digits) < 9:
        return digits.zfill(9)
    return digits[-9:]


def detect_csv_encoding(path: Path) -> str:
    """Detect a workable CSV encoding with a small fallback chain."""
    for encoding in ("utf-8-sig", "latin-1"):
        try:
            pd.read_csv(path, nrows=1, dtype=str, keep_default_na=False, low_memory=False, encoding=encoding)
            return encoding
        except UnicodeDecodeError:
            continue
    return "utf-8-sig"


def read_csv_flexible(path: Path, **kwargs: Any) -> pd.DataFrame:
    """Read a CSV using the encoding fallback chain."""
    encoding = detect_csv_encoding(path)
    return pd.read_csv(path, encoding=encoding, **kwargs)


def iter_csv_chunks(path: Path, chunk_size: int) -> Any:
    """Yield CSV chunks from a local file with stable dtype/encoding settings."""
    encoding = detect_csv_encoding(path)
    return pd.read_csv(
        path,
        dtype=str,
        keep_default_na=False,
        low_memory=False,
        encoding=encoding,
        chunksize=chunk_size,
    )


def load_zip_to_county_map(path: Path) -> dict[str, str]:
    """Load ZIP->county FIPS map from CSV into a normalized dictionary."""
    frame = load_zip_to_county_frame(path)
    out: dict[str, str] = {}
    for _, row in frame.iterrows():
        zip5 = str(row["zip5"])
        county_fips = str(row["county_fips"])
        if zip5 not in out:
            out[zip5] = county_fips
    return out


def load_zip_to_county_frame(path: Path) -> pd.DataFrame:
    """Load normalized ZIP/county pairs from the crosswalk without collapsing benchmark ZIPs."""
    if not path.exists():
        raise FileNotFoundError(f"ZIP-to-county file not found: {path}")
    df = pd.read_csv(path, dtype=str)
    zip_col = next((c for c in df.columns if "zip" in str(c).lower()), df.columns[0])
    fips_col = next((c for c in df.columns if "fips" in str(c).lower()), df.columns[1] if len(df.columns) > 1 else None)
    if fips_col is None:
        raise ValueError(f"ZIP-to-county CSV missing FIPS column: {path}")
    out = pd.DataFrame(
        {
            "zip5": df[zip_col].fillna("").astype(str).map(normalize_zip5),
            "county_fips": df[fips_col].fillna("").astype(str).map(normalize_fips5),
        }
    )
    out = out[(out["zip5"] != "") & (out["county_fips"] != "")].drop_duplicates(subset=["zip5", "county_fips"], keep="first").copy()
    return out


def build_benchmark_zip_frame(geoid_reference_path: Path, zip_to_county_path: Path) -> pd.DataFrame:
    """
    Return the explicit benchmark ZIP map derived from benchmark counties.

    This avoids relying on a generic one-ZIP/one-county collapse when the actual
    filtering question is whether a ZIP belongs to one of the benchmark counties.
    """
    geoid_ref = load_geoid_reference_frame(geoid_reference_path)
    zip_frame = load_zip_to_county_frame(zip_to_county_path)
    benchmark = zip_frame.merge(geoid_ref, on="county_fips", how="inner")
    benchmark = benchmark.drop_duplicates(subset=["zip5", "county_fips"], keep="first").copy()
    ambiguous = benchmark.groupby("zip5")["county_fips"].nunique()
    ambiguous = ambiguous[ambiguous > 1]
    if not ambiguous.empty:
        zip_list = ", ".join(sorted(ambiguous.index.tolist())[:10])
        raise ValueError(
            "Benchmark ZIP map is ambiguous: at least one ZIP maps to multiple benchmark counties. "
            f"Examples: {zip_list}"
        )
    return benchmark.drop_duplicates(subset=["zip5"], keep="first").copy()


def load_benchmark_zip_to_county_map(geoid_reference_path: Path, zip_to_county_path: Path) -> dict[str, str]:
    """Load the explicit benchmark ZIP->county map."""
    benchmark = build_benchmark_zip_frame(geoid_reference_path, zip_to_county_path)
    return dict(zip(benchmark["zip5"], benchmark["county_fips"]))


def _normalize_series_ein9(series: pd.Series) -> pd.Series:
    """Normalize a pandas Series of EIN-like values to 9-digit strings."""
    return series.fillna("").astype(str).map(normalize_ein9)


def _normalize_series_zip5(series: pd.Series) -> pd.Series:
    """Normalize a pandas Series of ZIP-like values to ZIP5 strings."""
    return series.fillna("").astype(str).map(normalize_zip5)


def _normalize_series_fips5(series: pd.Series) -> pd.Series:
    """Normalize a pandas Series of FIPS-like values to 5-digit county strings."""
    return series.fillna("").astype(str).map(normalize_fips5)


def select_source_types_arg(source_types_arg: str | None) -> list[str]:
    """Parse a comma-separated Core source-type selection string."""
    if not source_types_arg or str(source_types_arg).strip().lower() == "all":
        return DEFAULT_CORE_SOURCE_TYPES.copy()
    pieces = [piece.strip() for piece in str(source_types_arg).split(",") if piece.strip()]
    invalid = [piece for piece in pieces if piece not in CORE_GROUP_SPECS]
    if invalid:
        raise ValueError(
            f"Invalid --source-types values: {', '.join(invalid)}. "
            f"Expected one or more of: {', '.join(DEFAULT_CORE_SOURCE_TYPES)}"
        )
    selected = []
    for piece in pieces:
        if piece not in selected:
            selected.append(piece)
    return selected


def prepare_bmf_bridge_dataframe(
    local_bmf_paths: list[Path],
    geoid_reference_set: set[str],
    geoid_to_region: dict[str, str] | None,
    benchmark_zip_to_county: dict[str, str],
) -> pd.DataFrame:
    """
    Build the benchmark-county Unified BMF bridge used for Core filtering.

    The chosen precedence is deliberate:
    1. use CENSUS_BLOCK_FIPS[:5] when Unified BMF provides it
    2. fallback to F990_ORG_ADDR_ZIP -> zip_to_county_fips only when block FIPS is blank
    """
    frames: list[pd.DataFrame] = []
    for local_bmf_path in local_bmf_paths:
        df = read_csv_flexible(local_bmf_path, dtype=str, keep_default_na=False, low_memory=False)
        if "EIN2" in df.columns:
            join_source = df["EIN2"]
        elif "EIN" in df.columns:
            join_source = df["EIN"]
        else:
            raise ValueError(f"Unified BMF file missing EIN/EIN2 columns: {local_bmf_path}")

        join_ein = _normalize_series_ein9(join_source)
        block_fips = _normalize_series_fips5(df["CENSUS_BLOCK_FIPS"]) if "CENSUS_BLOCK_FIPS" in df.columns else pd.Series("", index=df.index)
        zip5 = _normalize_series_zip5(df["F990_ORG_ADDR_ZIP"]) if "F990_ORG_ADDR_ZIP" in df.columns else pd.Series("", index=df.index)
        county_from_zip = zip5.map(lambda value: benchmark_zip_to_county.get(value, ""))

        county_fips = block_fips.where(block_fips != "", county_from_zip)
        benchmark_match_source = pd.Series("", index=df.index, dtype=object)
        benchmark_match_source = benchmark_match_source.mask(block_fips != "", "census_block_fips")
        benchmark_match_source = benchmark_match_source.mask((block_fips == "") & (county_from_zip != ""), "zip_to_county")

        prepared = pd.DataFrame(
            {
                "join_ein": join_ein,
                "county_fips": county_fips,
                "benchmark_match_source": benchmark_match_source.fillna(""),
                "_org_year_last": pd.to_numeric(df.get("ORG_YEAR_LAST"), errors="coerce").fillna(-1),
                "_org_year_count": pd.to_numeric(df.get("ORG_YEAR_COUNT"), errors="coerce").fillna(-1),
                "_geocoder_score": pd.to_numeric(df.get("GEOCODER_SCORE"), errors="coerce").fillna(-1),
            }
        )
        prepared = prepared[prepared["join_ein"] != ""].copy()
        if geoid_to_region:
            prepared["region"] = prepared["county_fips"].map(geoid_to_region).fillna("")
        else:
            prepared["region"] = ""
        prepared["is_benchmark_county"] = prepared["county_fips"].isin(geoid_reference_set) & prepared["region"].astype(str).str.strip().ne("")
        frames.append(prepared)

    if not frames:
        raise ValueError("No Unified BMF dataframes were loaded for bridge preparation.")

    combined = pd.concat(frames, ignore_index=True)
    combined = combined[combined["is_benchmark_county"]].copy()
    combined.sort_values(
        by=["join_ein", "_org_year_last", "_org_year_count", "_geocoder_score"],
        ascending=[True, False, False, False],
        inplace=True,
    )
    deduped = combined.drop_duplicates(subset=["join_ein"], keep="first").copy()
    deduped["is_benchmark_county"] = True
    return deduped[
        [
            "join_ein",
            "county_fips",
            "region",
            "benchmark_match_source",
            "is_benchmark_county",
        ]
    ]


def filter_core_file_to_benchmark(
    local_core_path: Path,
    output_path: Path,
    bridge_df: pd.DataFrame,
    chunk_size: int = 100_000,
) -> tuple[int, int, int]:
    """Filter one local NCCS Core CSV to benchmark counties using the prepared BMF bridge."""
    output_path.parent.mkdir(parents=True, exist_ok=True)

    header_df = read_csv_flexible(local_core_path, dtype=str, keep_default_na=False, low_memory=False, nrows=0)
    original_columns = list(header_df.columns)
    if "EIN2" in original_columns:
        join_column = "EIN2"
    elif "F9_00_ORG_EIN" in original_columns:
        join_column = "F9_00_ORG_EIN"
    else:
        raise ValueError(f"NCCS Core file missing EIN2/F9_00_ORG_EIN join columns: {local_core_path}")

    bridge_lookup = bridge_df.copy()
    input_row_count = 0
    output_row_count = 0
    matched_counties: set[str] = set()
    wrote_header = False

    for chunk_index, chunk in enumerate(iter_csv_chunks(local_core_path, chunk_size=chunk_size), start=1):
        input_row_count += len(chunk)
        chunk = chunk.copy()
        chunk["__join_ein"] = _normalize_series_ein9(chunk[join_column])
        merged = chunk.merge(bridge_lookup, how="left", left_on="__join_ein", right_on="join_ein")
        filtered = merged[merged["is_benchmark_county"] == True].copy()  # noqa: E712
        if filtered.empty:
            print(f"[filter] {local_core_path.name} chunk {chunk_index}: input={len(chunk):,} output=0", flush=True)
            continue

        matched_counties.update(filtered["county_fips"].dropna().astype(str))
        output_row_count += len(filtered)
        filtered = filtered[original_columns + ["county_fips", "region", "benchmark_match_source", "is_benchmark_county"]]
        filtered.to_csv(output_path, mode="w" if not wrote_header else "a", header=not wrote_header, index=False)
        wrote_header = True
        print(
            f"[filter] {local_core_path.name} chunk {chunk_index}: input={len(chunk):,} output={len(filtered):,}",
            flush=True,
        )

    if not wrote_header:
        empty = pd.DataFrame(columns=original_columns + ["county_fips", "region", "benchmark_match_source", "is_benchmark_county"])
        empty.to_csv(output_path, index=False)

    return input_row_count, output_row_count, len(matched_counties)


def build_asset_record(
    *,
    asset_type: str,
    asset_group: str,
    family: str,
    scope: str,
    source_url: str,
    year: int | None,
    year_basis: str,
    benchmark_state: str | None = None,
) -> AssetRecord:
    """Build one AssetRecord with probed remote metadata."""
    filename = basename(source_url)
    meta = probe_url_head(source_url)
    content_type = meta.get("content_type") or guess_content_type(Path(filename))
    return AssetRecord(
        asset_type=asset_type,
        asset_group=asset_group,
        family=family,
        scope=scope,
        source_url=source_url,
        filename=filename,
        year=year,
        year_basis=year_basis,
        benchmark_state=benchmark_state,
        source_content_type=content_type,
        source_content_length_bytes=meta.get("content_length"),
        source_last_modified=meta.get("last_modified"),
    )


def discover_release(
    year_arg: str = "latest_common",
    geoid_reference_path: Path = GEOID_REFERENCE_CSV,
    benchmark_states_arg: str | None = None,
) -> tuple[dict[str, Any], str, str]:
    """Discover one NCCS Core release payload and return it with the fetched catalog HTML."""
    core_html = fetch_text(CORE_CATALOG_URL)
    bmf_html = fetch_text(BMF_CATALOG_URL)
    core_catalog = parse_core_catalog_html(core_html, CORE_CATALOG_URL)
    bmf_catalog = parse_bmf_catalog_html(bmf_html, BMF_CATALOG_URL)

    benchmark_states = parse_benchmark_states_arg(benchmark_states_arg)
    if benchmark_states is None:
        benchmark_states = derive_benchmark_states(geoid_reference_path)

    year_arg_clean = str(year_arg).strip().lower()
    latest_common_year = latest_common_core_year(core_catalog["year_assets_by_group"])
    if year_arg_clean in ("latest_common", "latest"):
        tax_year = latest_common_year
        year_selection = "latest_common"
    else:
        tax_year = int(year_arg_clean)
        year_selection = "explicit"

    for group, assets_by_year in core_catalog["year_assets_by_group"].items():
        if tax_year not in assets_by_year:
            raise ValueError(f"Requested NCCS Core year {tax_year} missing required asset group: {group}")

    missing_states = [state for state in benchmark_states if state not in bmf_catalog["state_urls"]]
    if missing_states:
        raise ValueError(f"NCCS Unified BMF catalog missing required benchmark states: {', '.join(missing_states)}")

    assets: list[AssetRecord] = []
    for group, spec in CORE_GROUP_SPECS.items():
        assets.append(
            build_asset_record(
                asset_type=group,
                asset_group="core_csv",
                family=spec["family"],
                scope=spec["scope"],
                source_url=core_catalog["year_assets_by_group"][group][tax_year],
                year=tax_year,
                year_basis=year_selection,
            )
        )

    for asset_type, spec in CORE_DICTIONARY_SPECS.items():
        assets.append(
            build_asset_record(
                asset_type=asset_type,
                asset_group="core_dictionary",
                family=spec["family"],
                scope=spec["scope"],
                source_url=core_catalog["dictionary_urls"][asset_type],
                year=None,
                year_basis="reference",
            )
        )

    assets.append(
        build_asset_record(
            asset_type="bmf_dictionary",
            asset_group="bmf_dictionary",
            family="unified_bmf",
            scope="dictionary",
            source_url=bmf_catalog["dictionary_url"],
            year=None,
            year_basis="reference",
        )
    )

    for state in sorted(benchmark_states):
        assets.append(
            build_asset_record(
                asset_type="bmf_state_csv",
                asset_group="bmf_state_csv",
                family="unified_bmf",
                scope="state",
                source_url=bmf_catalog["state_urls"][state],
                year=None,
                year_basis="reference",
                benchmark_state=state,
            )
        )

    payload = {
        "core_catalog_url": CORE_CATALOG_URL,
        "bmf_catalog_url": BMF_CATALOG_URL,
        "tax_year": tax_year,
        "year_selection": year_selection,
        "latest_common_year": latest_common_year,
        "benchmark_states": sorted(benchmark_states),
        "discovered_at_utc": now_utc_iso(),
        "assets": [asset.to_dict() for asset in assets],
    }
    return payload, core_html, bmf_html


def resolve_release_and_write_metadata(
    year_arg: str,
    metadata_dir: Path,
    geoid_reference_path: Path = GEOID_REFERENCE_CSV,
    benchmark_states_arg: str | None = None,
) -> dict[str, Any]:
    """Discover a release, persist latest_release.json, and save catalog page snapshots."""
    latest_path = metadata_dir / LATEST_RELEASE_JSON.name
    existing_release = read_json(latest_path)
    release, core_html, bmf_html = discover_release(year_arg, geoid_reference_path, benchmark_states_arg)
    release = apply_source_size_cache_to_release(release, extract_source_size_cache(existing_release))
    write_text(metadata_dir / CORE_CATALOG_SNAPSHOT.name, core_html)
    write_text(metadata_dir / BMF_CATALOG_SNAPSHOT.name, bmf_html)
    write_json(latest_path, release)
    return release


def selected_assets(release: dict[str, Any], source_types_arg: str | None = None) -> list[dict[str, Any]]:
    """Return all release assets, optionally restricting the Core CSV groups."""
    source_types = set(select_source_types_arg(source_types_arg))
    assets: list[dict[str, Any]] = []
    for asset in release.get("assets", []):
        if asset["asset_group"] == "core_csv" and asset["asset_type"] not in source_types:
            continue
        assets.append(asset)
    return assets


def selected_core_csv_assets(release: dict[str, Any], source_types_arg: str | None = None) -> list[dict[str, Any]]:
    """Return the selected Core raw CSV assets only."""
    return [asset for asset in selected_assets(release, source_types_arg) if asset["asset_group"] == "core_csv"]


def selected_bmf_assets(release: dict[str, Any]) -> list[dict[str, Any]]:
    """Return the selected Unified BMF state assets only."""
    return [asset for asset in release.get("assets", []) if asset["asset_group"] == "bmf_state_csv"]


def local_asset_path(
    core_raw_dir: Path,
    bridge_dir: Path,
    metadata_dir: Path,
    asset: dict[str, Any],
) -> Path:
    """Return the local download path for one selected NCCS asset."""
    asset_group = str(asset["asset_group"])
    if asset_group == "core_csv":
        return year_core_raw_dir(core_raw_dir, int(asset["year"])) / str(asset["filename"])
    if asset_group == "bmf_state_csv":
        return state_bridge_dir(bridge_dir, str(asset["benchmark_state"])) / str(asset["filename"])
    return metadata_dir / str(asset["filename"])


def asset_s3_key(
    raw_prefix: str,
    bridge_prefix: str,
    meta_prefix: str,
    asset: dict[str, Any],
) -> str:
    """Return the S3 object key for one selected NCCS asset."""
    asset_group = str(asset["asset_group"])
    if asset_group == "core_csv":
        return raw_s3_key(raw_prefix, int(asset["year"]), str(asset["filename"]))
    if asset_group == "bmf_state_csv":
        return bridge_s3_key(bridge_prefix, str(asset["benchmark_state"]), str(asset["filename"]))
    return meta_s3_key(meta_prefix, str(asset["filename"]))


def s3_client(region: str) -> Any:
    """Build an S3 client for a specific target region."""
    return shared_transfers.s3_client(region)


def s3_transfer_config() -> TransferConfig:
    """Build the custom managed-transfer config used for NCCS uploads."""
    return shared_transfers.s3_transfer_config()


def download_with_progress(url: str, output_path: Path, expected_bytes: int | None = None, timeout: int = 120) -> int:
    """Download a remote file to disk with a visible byte progress bar."""
    return shared_transfers.download_with_progress(
        url,
        output_path,
        expected_bytes=expected_bytes,
        timeout=timeout,
    )


def measure_remote_streamed_bytes(url: str, timeout: int = 120) -> int:
    """Stream a remote asset and return the number of decoded bytes delivered."""
    return shared_transfers.measure_remote_streamed_bytes(url, timeout=timeout)


def upload_file_with_progress(
    local_path: Path,
    bucket: str,
    key: str,
    region: str,
    extra_args: dict[str, Any] | None = None,
) -> None:
    """Upload one local file to S3 with a visible byte progress bar."""
    shared_transfers.upload_file_with_progress(
        local_path,
        bucket,
        key,
        region,
        extra_args=extra_args,
    )


def s3_object_size(bucket: str, key: str, region: str) -> int | None:
    """Return the S3 ContentLength for one object, or None when missing."""
    return shared_transfers.s3_object_size(bucket, key, region)


def should_skip_upload(local_path: Path, bucket: str, key: str, region: str, overwrite: bool) -> bool:
    """True when an upload can be skipped because the S3 object already matches local bytes."""
    return shared_transfers.should_skip_upload(local_path, bucket, key, region, overwrite)


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
