"""
Common utilities for the GivingTuesday DataMart pipeline.

This module centralizes:
- path/constants for local outputs and S3 prefixes
- environment loading (secrets/.env)
- catalog fetching and flattening
- required-dataset selection
- normalization helpers
- download/upload with progress bars
- simple markdown/csv writers
"""

from __future__ import annotations

import csv
import json
import os
import re
import sys
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

import boto3
import requests
from botocore.exceptions import ClientError
from tqdm import tqdm

# Ensure python/ is importable for utils.paths.
_THIS_FILE = Path(__file__).resolve()
_PYTHON_DIR = _THIS_FILE.parents[3]  # datamart -> 990_givingtuesday -> ingest -> python
if str(_PYTHON_DIR) not in sys.path:
    sys.path.insert(0, str(_PYTHON_DIR))

from utils.paths import DATA, get_base  # noqa: E402

# Keep print output live in terminal/IDE.
if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(line_buffering=True)


CATALOG_API_URL = "https://grantstory-api.gtdata.org/v2/public/678d25291eceb5d121099200/items?limit=200"

# Local directory layout.
RAW_ROOT = DATA / "raw" / "givingtuesday_990" / "datamarts"
RAW_DIR = RAW_ROOT / "raw"
META_DIR = RAW_ROOT / "metadata"
STAGING_DIR = DATA / "staging" / "filing"

# Core local files.
CATALOG_JSON = META_DIR / "datamart_catalog_raw.json"
CATALOG_CSV = META_DIR / "datamart_catalog.csv"
CATALOG_MD = META_DIR / "datamart_catalog.md"
FIELDS_CSV = META_DIR / "datamart_fields.csv"
FIELDS_MD = META_DIR / "datamart_fields.md"
SIZE_REPORT_CSV = META_DIR / "size_verification_report.csv"
REQUIRED_MANIFEST_CSV = META_DIR / "required_datasets_manifest.csv"

BASIC_ALLFORMS_PARQUET = STAGING_DIR / "givingtuesday_990_basic_allforms_unfiltered.parquet"
BASIC_PLUS_COMBINED_PARQUET = STAGING_DIR / "givingtuesday_990_basic_plus_combined_unfiltered.parquet"
FILTERED_SILVER_PARQUET = STAGING_DIR / "givingtuesday_990_filings_benchmark.parquet"
FILTERED_MANIFEST_JSON = STAGING_DIR / "manifest_filtered.json"

# S3 defaults.
DEFAULT_S3_BUCKET = os.environ.get("IRS_990_S3_BUCKET", "swb-321-irs990-teos")
DEFAULT_S3_REGION = os.environ.get("AWS_DEFAULT_REGION", "us-east-2")
BRONZE_RAW_PREFIX = "bronze/givingtuesday_990/datamarts/raw"
BRONZE_META_PREFIX = "bronze/givingtuesday_990/datamarts/metadata"
BRONZE_UNFILTERED_PREFIX = "bronze/givingtuesday_990/datamarts/unfiltered"
SILVER_PREFIX = "silver/givingtuesday_990/filing"

# Filtering references.
ZIP_TO_COUNTY_CSV = DATA / "reference" / "zip_to_county_fips.csv"
GEOID_REFERENCE_CSV = DATA / "reference" / "GEOID_reference.csv"


@dataclass(frozen=True)
class RequiredDatasetRule:
    title: str
    form_type: str | None


REQUIRED_RULES = [
    RequiredDatasetRule(title="Combined Forms Datamart", form_type=None),
    RequiredDatasetRule(title="Basic Fields", form_type="990"),
    RequiredDatasetRule(title="Basic Fields", form_type="990EZ"),
    RequiredDatasetRule(title="Basic Fields", form_type="990PF"),
]


def now_utc_iso() -> str:
    """Return UTC timestamp in ISO8601 with trailing Z."""
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def banner(title: str) -> None:
    """Print a visible script stage banner."""
    print("\n" + "=" * 88, flush=True)
    print(title, flush=True)
    print("=" * 88, flush=True)


def load_env_from_secrets() -> None:
    """Load key=value pairs from repo secrets/.env into process env if present."""
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
    print(f"[env] Loaded {loaded} keys from {env_path}", flush=True)


def ensure_dirs() -> None:
    """Ensure all working directories exist."""
    for p in (RAW_DIR, META_DIR, STAGING_DIR):
        p.mkdir(parents=True, exist_ok=True)
        print(f"[paths] Ready: {p}", flush=True)


def flatten_value(value: Any) -> str:
    """Flatten list/scalar values for CSV/markdown output."""
    if value is None:
        return ""
    if isinstance(value, list):
        return "|".join(str(x) for x in value)
    return str(value)


def normalize_ein(value: Any) -> str:
    """Normalize EIN into 9-digit string; blank for missing."""
    if value is None:
        return ""
    s = str(value).strip().replace("-", "").replace(" ", "")
    s = "".join(ch for ch in s if ch.isdigit())
    if not s:
        return ""
    if len(s) < 9:
        return s.zfill(9)
    return s[:9]


def normalize_returntype(value: Any) -> str:
    """Normalize return type into uppercase/no spaces/no hyphen."""
    if value is None:
        return ""
    s = str(value).strip().upper().replace("-", "").replace(" ", "")
    return s


def infer_form_type(returntype: Any) -> str:
    """Infer canonical form type from normalized return type."""
    rt = normalize_returntype(returntype)
    if rt.startswith("990EZ"):
        return "990EZ"
    if rt.startswith("990PF"):
        return "990PF"
    if rt.startswith("990N"):
        return "990N"
    if rt.startswith("990"):
        return "990"
    return rt or "UNKNOWN"


def normalize_tax_year(value: Any) -> str:
    """Normalize tax year to plain integer string when possible."""
    if value is None:
        return ""
    s = str(value).strip()
    if not s:
        return ""
    try:
        return str(int(float(s)))
    except ValueError:
        return s


def normalize_zip5(value: Any) -> str:
    """Normalize zip into 5-digit code where possible."""
    if value is None:
        return ""
    s = str(value)
    digits = "".join(ch for ch in s if ch.isdigit())
    if len(digits) < 5:
        return ""
    return digits[:5]


def fetch_catalog_documents(catalog_url: str = CATALOG_API_URL, timeout: int = 60) -> list[dict[str, Any]]:
    """Fetch and return catalog documents from the public GivingTuesday API."""
    print(f"[catalog] GET {catalog_url}", flush=True)
    r = requests.get(catalog_url, timeout=timeout)
    r.raise_for_status()
    payload = r.json()
    docs = payload.get("documents", [])
    print(f"[catalog] Retrieved {len(docs)} dataset records.", flush=True)
    return docs


def flatten_catalog_documents(documents: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Flatten API payload into row-oriented records for CSV/markdown."""
    rows: list[dict[str, Any]] = []
    extracted_at = now_utc_iso()
    for d in documents:
        cf = d.get("custom_fields") or {}
        row = {
            "dataset_id": d.get("_id", ""),
            "title": d.get("title", ""),
            "form_type": flatten_value(cf.get("form_type")),
            "category": flatten_value(cf.get("category")),
            "part": flatten_value(cf.get("part")),
            "size_display": flatten_value(cf.get("size")),
            "last_updated": flatten_value(cf.get("last_updated")),
            "download_url": flatten_value(cf.get("download_url")),
            "dataset_documentation": flatten_value(cf.get("dataset_documentation")),
            "source_api_id": flatten_value(d.get("_collection")),
            "extracted_at_utc": extracted_at,
        }
        rows.append(row)
    return rows


def dataset_matches_rule(row: dict[str, Any], rule: RequiredDatasetRule) -> bool:
    """Check whether a catalog row satisfies a required dataset rule."""
    title = (row.get("title") or "").strip()
    form = (row.get("form_type") or "").strip().upper()
    if title != rule.title:
        return False
    if rule.form_type is None:
        return True
    return form == rule.form_type.upper()


def select_required_datasets(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """
    Select required datasets from flattened catalog rows.

    Raises ValueError if any required rule is missing or ambiguous.
    """
    selected: list[dict[str, Any]] = []
    for rule in REQUIRED_RULES:
        matches = [r for r in rows if dataset_matches_rule(r, rule)]
        if len(matches) != 1:
            raise ValueError(
                f"Required dataset rule not resolved uniquely: title={rule.title}, form_type={rule.form_type}, matches={len(matches)}"
            )
        selected.append(matches[0])
    return selected


def url_basename(url: str) -> str:
    """Extract filename basename from URL path."""
    return Path(urlparse(url).path).name


def probe_url_head(url: str, timeout: int = 60) -> tuple[int | None, int | None, str]:
    """
    Probe URL with HEAD (fallback to GET stream) and return:
    (status_code, content_length_bytes, status_text)
    """
    try:
        r = requests.head(url, allow_redirects=True, timeout=timeout)
        status = r.status_code
        if status >= 400:
            return status, None, f"http_{status}"
        cl = r.headers.get("Content-Length")
        return status, int(cl) if cl and cl.isdigit() else None, "ok"
    except requests.RequestException:
        try:
            # Fallback to GET only for metadata probe; close immediately.
            r = requests.get(url, stream=True, allow_redirects=True, timeout=timeout)
            status = r.status_code
            cl = r.headers.get("Content-Length")
            r.close()
            if status >= 400:
                return status, None, f"http_{status}"
            return status, int(cl) if cl and cl.isdigit() else None, "ok_get_fallback"
        except requests.RequestException as e:
            return None, None, f"error:{type(e).__name__}"


def write_csv(path: Path, rows: list[dict[str, Any]], fieldnames: list[str]) -> None:
    """Write list of dict rows into CSV with explicit field order."""
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
        w.writeheader()
        for row in rows:
            w.writerow({k: flatten_value(row.get(k)) for k in fieldnames})
    print(f"[write] CSV: {path} ({len(rows)} row(s))", flush=True)


def markdown_escape(text: str) -> str:
    """Escape markdown table separators minimally."""
    return str(text).replace("|", "\\|").replace("\n", " ")


def write_catalog_markdown(path: Path, rows: list[dict[str, Any]]) -> None:
    """Write full catalog markdown table."""
    cols = [
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
        "extracted_at_utc",
    ]
    lines = [
        "# DataMart Catalog",
        "",
        f"- Generated at: {now_utc_iso()}",
        f"- Rows: {len(rows)}",
        "",
        "|" + "|".join(cols) + "|",
        "|" + "|".join(["---"] * len(cols)) + "|",
    ]
    for row in rows:
        lines.append("|" + "|".join(markdown_escape(flatten_value(row.get(c, ""))) for c in cols) + "|")
    path.write_text("\n".join(lines), encoding="utf-8")
    print(f"[write] Markdown: {path}", flush=True)


def write_fields_markdown(path: Path, field_rows: list[dict[str, Any]]) -> None:
    """Write field dictionary markdown grouped by dataset."""
    lines: list[str] = [
        "# DataMart Field Dictionary",
        "",
        f"- Generated at: {now_utc_iso()}",
        f"- Total field rows: {len(field_rows)}",
        "",
    ]
    grouped: dict[str, list[dict[str, Any]]] = {}
    for row in field_rows:
        key = f"{row.get('title', '')} [{row.get('form_type', '')}]"
        grouped.setdefault(key, []).append(row)
    for dataset_label in sorted(grouped.keys()):
        rows = sorted(grouped[dataset_label], key=lambda r: int(r.get("ordinal", 0)))
        lines.extend(
            [
                f"## {dataset_label}",
                "",
                "|ordinal|column_name|dataset_id|download_url|",
                "|---|---|---|---|",
            ]
        )
        for r in rows:
            lines.append(
                "|"
                + "|".join(
                    [
                        markdown_escape(flatten_value(r.get("ordinal", ""))),
                        markdown_escape(flatten_value(r.get("column_name", ""))),
                        markdown_escape(flatten_value(r.get("dataset_id", ""))),
                        markdown_escape(flatten_value(r.get("download_url", ""))),
                    ]
                )
                + "|"
            )
        lines.append("")
    path.write_text("\n".join(lines), encoding="utf-8")
    print(f"[write] Markdown: {path}", flush=True)


def fetch_remote_header_columns(url: str, timeout: int = 120) -> list[str]:
    """Read only the first CSV line from a remote URL and parse columns."""
    r = requests.get(url, stream=True, timeout=timeout)
    r.raise_for_status()
    header_line = None
    for line in r.iter_lines(decode_unicode=True):
        if line:
            header_line = line
            break
    r.close()
    if not header_line:
        return []
    return next(csv.reader([header_line]))


def download_with_progress(url: str, output_path: Path, expected_bytes: int | None = None, timeout: int = 120) -> int:
    """Download URL to output_path with a byte progress bar. Returns local bytes written."""
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with requests.get(url, stream=True, timeout=timeout) as r:
        r.raise_for_status()
        total = expected_bytes
        if total is None:
            cl = r.headers.get("Content-Length")
            if cl and cl.isdigit():
                total = int(cl)
        desc = f"download {output_path.name}"
        with tqdm(total=total, unit="B", unit_scale=True, unit_divisor=1024, desc=desc) as pbar:
            with open(output_path, "wb") as f:
                for chunk in r.iter_content(chunk_size=1024 * 1024):
                    if not chunk:
                        continue
                    f.write(chunk)
                    pbar.update(len(chunk))
    return output_path.stat().st_size


class _TqdmUpload:
    """tqdm callback helper for boto3 upload_file callbacks."""

    def __init__(self, pbar: tqdm):
        self.pbar = pbar
        self._seen = 0

    def __call__(self, bytes_amount: int) -> None:
        self._seen += bytes_amount
        self.pbar.update(bytes_amount)


def s3_client(region: str) -> Any:
    """Build an S3 client for a target region."""
    return boto3.Session(region_name=region).client("s3")


def upload_file_with_progress(local_path: Path, bucket: str, key: str, region: str, extra_args: dict[str, Any] | None = None) -> None:
    """Upload a local file to S3 with byte progress."""
    client = s3_client(region)
    size = local_path.stat().st_size
    desc = f"upload {local_path.name}"
    with tqdm(total=size, unit="B", unit_scale=True, unit_divisor=1024, desc=desc) as pbar:
        cb = _TqdmUpload(pbar)
        client.upload_file(str(local_path), bucket, key, ExtraArgs=extra_args or {}, Callback=cb)


def s3_object_size(bucket: str, key: str, region: str) -> int | None:
    """Return S3 object size in bytes or None if missing/error."""
    client = s3_client(region)
    try:
        obj = client.head_object(Bucket=bucket, Key=key)
        return int(obj.get("ContentLength", 0))
    except ClientError:
        return None


def safe_slug(text: str) -> str:
    """Generate a filesystem-safe slug for names if needed."""
    cleaned = re.sub(r"[^A-Za-z0-9._-]+", "_", text.strip())
    return cleaned.strip("_") or "unnamed"


def load_catalog_rows(path: Path = CATALOG_CSV) -> list[dict[str, str]]:
    """Load flattened catalog CSV rows."""
    if not path.exists():
        raise FileNotFoundError(f"Catalog CSV not found: {path}")
    with open(path, newline="", encoding="utf-8") as f:
        return list(csv.DictReader(f))


def print_elapsed(start_ts: float, label: str = "Elapsed") -> None:
    """Print elapsed wall clock time in seconds and minutes."""
    elapsed = time.perf_counter() - start_ts
    print(f"[time] {label}: {elapsed:.1f}s ({elapsed / 60:.1f} min)", flush=True)
