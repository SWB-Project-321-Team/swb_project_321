"""
Shared helpers for the NCCS efile 2022-latest-published pipeline.

This pipeline mirrors the structure and runtime behavior of the other NCCS
ingestors in the repo:
- explicit step scripts with startup banners
- raw manifest + raw verification flow
- local raw / metadata / staging directories under DATA
- direct print() status lines and tqdm progress bars

The source itself is different from NCCS Core:
- NCCS publishes efile as parsed table CSVs, not one flat annual file
- we only need a small subset of those tables to reproduce the fields that the
  combined pipeline currently consumes from Core
- the latest published wave should be treated as partial
"""

from __future__ import annotations

import importlib.util
import json
import os
import re
import sys
from dataclasses import dataclass
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

from ingest._shared import transfers as shared_transfers  # noqa: E402
_CORE_COMMON_PATH = _PYTHON_DIR / "ingest" / "nccs_990_core" / "common.py"
_CORE_COMMON_SPEC = importlib.util.spec_from_file_location("nccs_990_core_common", _CORE_COMMON_PATH)
if _CORE_COMMON_SPEC is None or _CORE_COMMON_SPEC.loader is None:
    raise ImportError(f"Unable to load NCCS Core common helpers from {_CORE_COMMON_PATH}")
_CORE_COMMON = importlib.util.module_from_spec(_CORE_COMMON_SPEC)
sys.modules.setdefault("nccs_990_core_common", _CORE_COMMON)
_CORE_COMMON_SPEC.loader.exec_module(_CORE_COMMON)

from utils.paths import DATA, get_base  # noqa: E402

if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(line_buffering=True)

EFILE_DATASET_URL = "https://nccs.urban.org/nccs/datasets/efile/"
EFILE_CATALOG_URL = "https://nccs.urban.org/nccs/catalogs/catalog-efile-v2_1.html"
EFILE_PUBLIC_BASE_URL = "https://nccs-efile.s3.us-east-1.amazonaws.com/public/efile_v2_1/"

RAW_ROOT = DATA / "raw" / "nccs_efile"
EFILE_RAW_DIR = RAW_ROOT / "raw"
META_DIR = RAW_ROOT / "metadata"
STAGING_DIR = DATA / "staging" / "nccs_efile"
DOCS_PACKAGE_DIR = get_base() / "docs" / "final_preprocessing_docs"
DOCS_TECHNICAL_DIR = DOCS_PACKAGE_DIR / "technical_docs"
DOCS_ANALYSIS_DIR = DOCS_TECHNICAL_DIR / "analysis_variable_mappings"
DOCS_DATA_PROCESSING_DIR = DOCS_TECHNICAL_DIR / "pipeline_docs"

LATEST_RELEASE_JSON = META_DIR / "latest_release.json"
EFILE_DATASET_SNAPSHOT = META_DIR / "dataset_efile.html"
EFILE_CATALOG_SNAPSHOT = META_DIR / "catalog_efile.html"

DEFAULT_S3_BUCKET = _CORE_COMMON.DEFAULT_S3_BUCKET
DEFAULT_S3_REGION = _CORE_COMMON.DEFAULT_S3_REGION
RAW_PREFIX = "bronze/nccs_efile/raw"
META_PREFIX = "bronze/nccs_efile/metadata"
SILVER_PREFIX = "silver/nccs_efile"
COMPARISON_SILVER_PREFIX = f"{SILVER_PREFIX}/comparison"
ANALYSIS_PREFIX = f"{SILVER_PREFIX}/analysis"
ANALYSIS_DOCUMENTATION_PREFIX = f"{ANALYSIS_PREFIX}/documentation"
ANALYSIS_VARIABLE_MAPPING_PREFIX = f"{ANALYSIS_PREFIX}/variable_mappings"
ANALYSIS_COVERAGE_PREFIX = f"{ANALYSIS_PREFIX}/quality/coverage"

GEOID_REFERENCE_CSV = _CORE_COMMON.GEOID_REFERENCE_CSV
ZIP_TO_COUNTY_CSV = _CORE_COMMON.ZIP_TO_COUNTY_CSV

START_YEAR_DEFAULT = 2022
ANALYSIS_TAX_YEAR_MIN = 2022
ANALYSIS_TAX_YEAR_MAX = 2024
DOWNLOAD_WORKERS = shared_transfers.DOWNLOAD_WORKERS_DEFAULT
UPLOAD_WORKERS = shared_transfers.UPLOAD_WORKERS_DEFAULT
_TQDM_KW = getattr(_CORE_COMMON, "_TQDM_KW", {})
VERIFY_WORKERS = shared_transfers.VERIFY_WORKERS_DEFAULT
print_transfer_settings = shared_transfers.print_transfer_settings
parallel_map = shared_transfers.parallel_map
batch_s3_object_sizes = shared_transfers.batch_s3_object_sizes

REQUIRED_TABLES = (
    "F9-P00-T00-HEADER",
    "F9-P01-T00-SUMMARY",
    "SA-P01-T00-PUBLIC-CHARITY-STATUS",
)
EFILE_TABLE_ORDER = list(REQUIRED_TABLES)
_EFILE_LINK_REGEX = re.compile(
    r"/public/efile_v2_1/(?P<table>[A-Z0-9-]+)-(?P<year>\d{4})\.CSV$",
    re.IGNORECASE,
)

EFILE_HEADER_COLUMNS = [
    "EIN2",
    "ORG_EIN",
    "RETURN_AMENDED_X",
    "RETURN_TIME_STAMP",
    "RETURN_TYPE",
    "TAX_PERIOD_BEGIN_DATE",
    "TAX_PERIOD_END_DATE",
    "TAX_YEAR",
    "URL",
    "F9_00_BUILD_TIME_STAMP",
    "F9_00_RETURN_TIME_STAMP",
    "F9_00_RETURN_TYPE",
    "F9_00_TAX_PERIOD_BEGIN_DATE",
    "F9_00_TAX_PERIOD_END_DATE",
    "F9_00_TAX_YEAR",
    "F9_00_RETURN_AMENDED_X",
    "F9_00_ORG_EIN",
    "F9_00_ORG_NAME_L1",
    "F9_00_ORG_ADDR_STATE",
    "F9_00_ORG_ADDR_ZIP",
]
EFILE_SUMMARY_COLUMNS = [
    "ORG_EIN",
    "TAX_YEAR",
    "URL",
    "F9_01_REV_TOT_CY",
    "F9_01_EXP_TOT_CY",
    "F9_01_NAFB_ASSET_TOT_EOY",
    "F9_01_EXP_REV_LESS_EXP_CY",
]
EFILE_SCHEDULE_A_COLUMNS = [
    "ORG_EIN",
    "TAX_YEAR",
    "URL",
    "SA_01_PCSTAT_HOSPITAL_X",
    "SA_01_PCSTAT_SCHOOL_X",
]
EFILE_BOOLEAN_TRUE = {"1", "y", "yes", "true", "t", "x"}
EFILE_BOOLEAN_FALSE = {"0", "n", "no", "false", "f"}

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
load_geoid_reference_set = _CORE_COMMON.load_geoid_reference_set
load_benchmark_zip_to_county_map = _CORE_COMMON.load_benchmark_zip_to_county_map
normalize_zip5 = _CORE_COMMON.normalize_zip5
normalize_fips5 = _CORE_COMMON.normalize_fips5
normalize_ein9 = _CORE_COMMON.normalize_ein9
meta_s3_key = _CORE_COMMON.meta_s3_key
now_utc_iso = _CORE_COMMON.now_utc_iso
download_with_progress = shared_transfers.download_with_progress
upload_file_with_progress = shared_transfers.upload_file_with_progress


def normalize_tax_year(value: Any) -> str:
    """Normalize a tax-year-like value to a 4-digit string when possible."""
    text = str(value).strip() if value is not None else ""
    if text.lower() == "nan":
        return ""
    digits = "".join(character for character in text if character.isdigit())
    if len(digits) >= 4:
        return digits[:4]
    return ""


@dataclass(frozen=True)
class EfileAssetRecord:
    """One raw efile table CSV selected for a year in this run."""

    asset_group: str
    asset_type: str
    table_name: str
    tax_year: int
    is_partial_year: bool
    source_url: str
    filename: str
    source_content_type: str
    source_content_length_bytes: int | None
    source_last_modified: str | None

    def to_dict(self) -> dict[str, Any]:
        return {
            "asset_group": self.asset_group,
            "asset_type": self.asset_type,
            "table_name": self.table_name,
            "tax_year": self.tax_year,
            "is_partial_year": self.is_partial_year,
            "source_url": self.source_url,
            "filename": self.filename,
            "source_content_type": self.source_content_type,
            "source_content_length_bytes": self.source_content_length_bytes,
            "source_last_modified": self.source_last_modified,
        }


def ensure_work_dirs(
    raw_dir: Path = EFILE_RAW_DIR,
    metadata_dir: Path = META_DIR,
    staging_dir: Path = STAGING_DIR,
) -> None:
    """Ensure the efile pipeline's local working directories exist."""
    for path in (raw_dir, metadata_dir, staging_dir):
        path.mkdir(parents=True, exist_ok=True)
        print(f"[paths] Ready: {path}", flush=True)


def year_raw_dir(raw_dir: Path, tax_year: int) -> Path:
    """Return the raw-directory path for one efile tax year."""
    return raw_dir / f"tax_year={tax_year}"


def table_raw_dir(raw_dir: Path, tax_year: int, table_name: str) -> Path:
    """Return the raw-directory path for one efile table in one tax year."""
    return year_raw_dir(raw_dir, tax_year) / f"table={table_name}"


def local_asset_path(raw_dir: Path, metadata_dir: Path, asset: dict[str, Any]) -> Path:
    """Return the local download path for one selected efile asset."""
    return table_raw_dir(raw_dir, int(asset["tax_year"]), str(asset["table_name"])) / str(asset["filename"])


def release_manifest_path(metadata_dir: Path, start_year: int = START_YEAR_DEFAULT) -> Path:
    """Return the raw-manifest path for the selected efile release."""
    return metadata_dir / f"release_manifest_start_year={start_year}.csv"


def size_report_path(metadata_dir: Path, start_year: int = START_YEAR_DEFAULT) -> Path:
    """Return the raw verification-report path for the selected efile release."""
    return metadata_dir / f"size_verification_start_year={start_year}.csv"


def year_staging_dir(staging_dir: Path, tax_year: int) -> Path:
    """Return the staging directory for one annualized efile benchmark year."""
    return staging_dir / f"tax_year={tax_year}"


def filtered_output_path(staging_dir: Path, tax_year: int) -> Path:
    """Return the annualized efile benchmark parquet path for one tax year."""
    return year_staging_dir(staging_dir, tax_year) / f"nccs_efile_benchmark_tax_year={tax_year}.parquet"


def filter_manifest_path(staging_dir: Path, tax_year: int) -> Path:
    """Return the per-year benchmark manifest path."""
    return year_staging_dir(staging_dir, tax_year) / f"filter_manifest_tax_year={tax_year}.csv"


def analysis_variables_output_path(staging_dir: Path = STAGING_DIR) -> Path:
    """Return the combined NCCS efile analysis row-level parquet path."""
    return staging_dir / "nccs_efile_analysis_variables.parquet"


def analysis_geography_metrics_output_path(staging_dir: Path = STAGING_DIR) -> Path:
    """Return the NCCS efile region-level analysis metrics parquet path."""
    return staging_dir / "nccs_efile_analysis_geography_metrics.parquet"


def analysis_variable_coverage_path(metadata_dir: Path = META_DIR) -> Path:
    """Return the NCCS efile analysis coverage CSV path."""
    return metadata_dir / "nccs_efile_analysis_variable_coverage.csv"


def analysis_variable_mapping_path() -> Path:
    """Return the NCCS efile analysis mapping Markdown path."""
    return DOCS_ANALYSIS_DIR / "nccs_efile_analysis_variable_mapping.md"


def analysis_data_processing_doc_path() -> Path:
    """Return the NCCS efile data-processing documentation path."""
    return DOCS_DATA_PROCESSING_DIR / "nccs_efile_pipeline.md"


def comparison_dir(staging_dir: Path, tax_year: int) -> Path:
    """Return the local directory for one efile-vs-core comparison year."""
    return staging_dir / "comparison" / f"tax_year={tax_year}"


def comparison_row_counts_path(staging_dir: Path, tax_year: int) -> Path:
    return comparison_dir(staging_dir, tax_year) / f"efile_vs_core_row_counts_tax_year={tax_year}.csv"


def comparison_fill_rates_path(staging_dir: Path, tax_year: int) -> Path:
    return comparison_dir(staging_dir, tax_year) / f"efile_vs_core_fill_rates_tax_year={tax_year}.csv"


def comparison_conflicts_path(staging_dir: Path, tax_year: int) -> Path:
    return comparison_dir(staging_dir, tax_year) / f"efile_vs_core_conflicts_tax_year={tax_year}.csv"


def comparison_summary_path(staging_dir: Path, tax_year: int) -> Path:
    return comparison_dir(staging_dir, tax_year) / f"efile_vs_core_summary_tax_year={tax_year}.json"


def raw_s3_key(raw_prefix: str, tax_year: int, table_name: str, filename: str) -> str:
    """Return the S3 key for one raw efile table CSV."""
    return f"{raw_prefix.rstrip('/')}/tax_year={tax_year}/table={table_name}/{filename}"


def filtered_s3_key(silver_prefix: str, tax_year: int, filename: str) -> str:
    """Return the S3 key for one annualized benchmark parquet or manifest."""
    return f"{silver_prefix.rstrip('/')}/tax_year={tax_year}/{filename}"


def comparison_s3_key(comparison_prefix: str, tax_year: int, filename: str) -> str:
    """Return the S3 key for one comparison artifact."""
    return f"{comparison_prefix.rstrip('/')}/tax_year={tax_year}/{filename}"


def asset_s3_key(raw_prefix: str, meta_prefix: str, asset: dict[str, Any]) -> str:
    """Return the S3 key for one selected raw efile asset."""
    return raw_s3_key(raw_prefix, int(asset["tax_year"]), str(asset["table_name"]), str(asset["filename"]))


def _safe_probe(url: str, timeout: int = 60) -> dict[str, Any] | None:
    """
    Probe a remote efile URL and return metadata.

    Missing files are a normal discovery outcome, so 404 returns None.
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


def parse_efile_catalog_html(html: str, page_url: str = EFILE_CATALOG_URL) -> dict[str, Any]:
    """
    Parse the public NCCS efile catalog page.

    We intentionally scrape the actual download links from the live NCCS catalog
    so discovery follows the currently published years instead of hardcoding a
    year list that can go stale.
    """
    links_by_year: dict[int, dict[str, str]] = {}
    for link in collect_links(html):
        href = link.get("href", "")
        if not href:
            continue
        abs_url = urljoin(page_url, href)
        match = _EFILE_LINK_REGEX.search(abs_url)
        if match is None:
            continue
        table_name = match.group("table").upper()
        tax_year = int(match.group("year"))
        links_by_year.setdefault(tax_year, {})[table_name] = abs_url
    return {"links_by_year": links_by_year}


def _required_assets_for_year(parsed_catalog: dict[str, Any], tax_year: int) -> dict[str, str] | None:
    """Return the required table URLs for one tax year when fully published."""
    links = parsed_catalog["links_by_year"].get(tax_year, {})
    if all(table_name in links for table_name in REQUIRED_TABLES):
        return {table_name: links[table_name] for table_name in REQUIRED_TABLES}
    return None


def discover_release(
    *,
    start_year: int = START_YEAR_DEFAULT,
    catalog_url: str = EFILE_CATALOG_URL,
    dataset_url: str = EFILE_DATASET_URL,
) -> tuple[dict[str, Any], str, str]:
    """
    Discover the usable published efile years and selected raw table assets.

    The latest published year is marked partial because NCCS explicitly states
    that the newest wave should be treated as partial.
    """
    catalog_html = fetch_text(catalog_url)
    dataset_html = fetch_text(dataset_url)
    parsed_catalog = parse_efile_catalog_html(catalog_html, catalog_url)

    available_years = sorted(
        year
        for year in parsed_catalog["links_by_year"]
        if year >= start_year and _required_assets_for_year(parsed_catalog, year) is not None
    )
    if not available_years:
        raise ValueError(f"No usable NCCS efile years >= {start_year} were found in the live catalog.")

    latest_published_year = max(available_years)
    selected_assets: list[dict[str, Any]] = []
    for tax_year in tqdm(available_years, desc="probe efile years", unit="year", **_TQDM_KW):
        table_links = _required_assets_for_year(parsed_catalog, tax_year)
        if table_links is None:
            continue
        is_partial_year = tax_year == latest_published_year
        for table_name in EFILE_TABLE_ORDER:
            source_url = table_links[table_name]
            print(f"[discover] Probing {table_name} for tax_year={tax_year}: {source_url}", flush=True)
            meta = _safe_probe(source_url)
            if meta is None:
                raise ValueError(f"Catalog listed {table_name} for {tax_year}, but the asset was not reachable: {source_url}")
            selected_assets.append(
                EfileAssetRecord(
                    asset_group="efile_csv",
                    asset_type="efile_table_csv",
                    table_name=table_name,
                    tax_year=tax_year,
                    is_partial_year=is_partial_year,
                    source_url=source_url,
                    filename=Path(source_url).name,
                    source_content_type=meta.get("content_type") or guess_content_type(source_url),
                    source_content_length_bytes=meta.get("content_length"),
                    source_last_modified=meta.get("last_modified"),
                ).to_dict()
            )

    release = {
        "efile_dataset_url": dataset_url,
        "efile_catalog_url": catalog_url,
        "required_tables": list(REQUIRED_TABLES),
        "selected_tax_years": available_years,
        "latest_published_year": latest_published_year,
        "partial_tax_years": [latest_published_year],
        "selected_assets": selected_assets,
        "discovered_at_utc": now_utc_iso(),
        "source_size_cache": {},
    }
    return release, dataset_html, catalog_html


def apply_source_size_cache_to_release(
    release: dict[str, Any],
    cache_entries: dict[str, dict[str, Any]],
) -> dict[str, Any]:
    """Merge cached source-byte values into the efile release payload."""
    release_cache = extract_source_size_cache(release)
    release_cache.update(cache_entries)

    for asset in release.get("selected_assets", []):
        if not isinstance(asset, dict):
            continue
        cache_entry = release_cache.get(source_size_cache_key(asset.get("source_url", ""), asset.get("source_last_modified")))
        if cache_entry:
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
    """Persist one source-byte value into the efile release cache and asset list."""
    cache_entry = {
        "source_url": source_url,
        "source_last_modified": source_last_modified,
        "source_content_length_bytes": int(source_content_length_bytes),
        "cached_at_utc": now_utc_iso(),
    }
    key = source_size_cache_key(source_url, source_last_modified)
    return apply_source_size_cache_to_release(release, {key: cache_entry})


def resolve_release_and_write_metadata(metadata_dir: Path, *, start_year: int = START_YEAR_DEFAULT) -> dict[str, Any]:
    """Discover the efile release, persist metadata JSON, and save page snapshots."""
    latest_path = metadata_dir / LATEST_RELEASE_JSON.name
    existing_release = read_json(latest_path)
    release, dataset_html, catalog_html = discover_release(start_year=start_year)
    release = apply_source_size_cache_to_release(release, extract_source_size_cache(existing_release))
    write_text(metadata_dir / EFILE_DATASET_SNAPSHOT.name, dataset_html)
    write_text(metadata_dir / EFILE_CATALOG_SNAPSHOT.name, catalog_html)
    write_json(latest_path, release)
    return release


def selected_assets(release: dict[str, Any]) -> list[dict[str, Any]]:
    """Return the selected efile raw assets in a stable year/table order."""
    assets = list(release.get("selected_assets", []))
    return sorted(assets, key=lambda item: (int(item["tax_year"]), EFILE_TABLE_ORDER.index(str(item["table_name"]))))


def grouped_assets_by_year(release: dict[str, Any]) -> dict[int, list[dict[str, Any]]]:
    """Group the selected raw assets by tax year."""
    grouped: dict[int, list[dict[str, Any]]] = {}
    for asset in selected_assets(release):
        grouped.setdefault(int(asset["tax_year"]), []).append(asset)
    return grouped


def _read_selected_columns(path: Path, usecols: list[str]) -> pd.DataFrame:
    """Read a CSV with a defensive column subset and string-preserving semantics."""
    header = pd.read_csv(path, nrows=0).columns.tolist()
    available = [column for column in usecols if column in header]
    if not available:
        raise ValueError(f"None of the expected columns were present in {path}. Expected one of: {usecols}")
    frame = pd.read_csv(path, usecols=available, dtype=str, low_memory=False).fillna("")
    for column in usecols:
        if column not in frame.columns:
            frame[column] = ""
    return frame[usecols]


def _normalize_boolish(series: pd.Series) -> pd.Series:
    """Normalize boolean-ish source tokens to lowercase true/false strings."""
    def _normalize_one(value: Any) -> str:
        raw = str(value or "").strip().lower()
        if not raw:
            return ""
        if raw in EFILE_BOOLEAN_TRUE:
            return "true"
        if raw in EFILE_BOOLEAN_FALSE:
            return "false"
        return ""

    return series.map(_normalize_one)


def _pick_first_nonblank(df: pd.DataFrame, primary: str, fallback: str) -> pd.Series:
    """Pick the first nonblank string between two candidate columns."""
    primary_values = df[primary].fillna("").astype(str)
    fallback_values = df[fallback].fillna("").astype(str)
    return primary_values.where(primary_values.str.strip().ne(""), fallback_values)


def build_efile_year_to_benchmark(
    *,
    tax_year: int,
    year_assets: list[dict[str, Any]],
    raw_dir: Path,
    output_path: Path,
    geoid_reference_path: Path,
    zip_to_county_path: Path,
) -> dict[str, Any]:
    """
    Build one compact annual benchmark parquet for a published efile tax year.

    The annualized output intentionally contains only the fields needed for the
    combined pipeline plus the benchmark geography and efile selection metadata.

    Per the filtered-only combine contract, benchmark admission happens upstream on HEADER rows
    before the wider SUMMARY and Schedule A table combine. HEADER carries the
    filing identity, ranking timestamps, and ZIP-based geography needed to make
    that admission decision. The downstream joins then run only on the retained
    benchmark-selected filings.
    """
    asset_by_table = {str(asset["table_name"]): asset for asset in year_assets}
    missing_tables = [table_name for table_name in REQUIRED_TABLES if table_name not in asset_by_table]
    if missing_tables:
        raise ValueError(f"Missing required efile tables for tax_year={tax_year}: {', '.join(missing_tables)}")

    header_path = local_asset_path(raw_dir, META_DIR, asset_by_table["F9-P00-T00-HEADER"])
    summary_path = local_asset_path(raw_dir, META_DIR, asset_by_table["F9-P01-T00-SUMMARY"])
    schedule_a_path = local_asset_path(raw_dir, META_DIR, asset_by_table["SA-P01-T00-PUBLIC-CHARITY-STATUS"])
    for required_path in (header_path, summary_path, schedule_a_path):
        if not required_path.exists():
            raise FileNotFoundError(f"Local efile asset not found: {required_path}. Run step 02 first.")

    print(f"[efile-build] Reading HEADER: {header_path}", flush=True)
    header_df = _read_selected_columns(header_path, EFILE_HEADER_COLUMNS)
    print(f"[efile-build] Reading SUMMARY: {summary_path}", flush=True)
    summary_df = _read_selected_columns(summary_path, EFILE_SUMMARY_COLUMNS)
    print(f"[efile-build] Reading SCHEDULE A: {schedule_a_path}", flush=True)
    schedule_a_df = _read_selected_columns(schedule_a_path, EFILE_SCHEDULE_A_COLUMNS)

    input_row_count = int(len(header_df))
    print(f"[efile-build] HEADER source rows: {input_row_count:,}", flush=True)

    header_work = header_df.copy()
    header_work["harm_ein"] = _pick_first_nonblank(header_work, "F9_00_ORG_EIN", "ORG_EIN").map(normalize_ein9)
    header_work["harm_tax_year"] = _pick_first_nonblank(header_work, "F9_00_TAX_YEAR", "TAX_YEAR")
    header_work["harm_filing_form"] = _pick_first_nonblank(header_work, "F9_00_RETURN_TYPE", "RETURN_TYPE").str.upper()
    header_work["harm_org_name"] = header_work["F9_00_ORG_NAME_L1"].fillna("").astype(str)
    header_work["harm_state"] = header_work["F9_00_ORG_ADDR_STATE"].fillna("").astype(str)
    header_work["harm_zip5"] = header_work["F9_00_ORG_ADDR_ZIP"].map(normalize_zip5)
    header_work["efile_selected_filing_url"] = header_work["URL"].fillna("").astype(str)
    header_work["efile_selected_return_amended"] = _normalize_boolish(
        _pick_first_nonblank(header_work, "F9_00_RETURN_AMENDED_X", "RETURN_AMENDED_X")
    )
    header_work["efile_selected_return_timestamp"] = _pick_first_nonblank(
        header_work,
        "F9_00_RETURN_TIME_STAMP",
        "RETURN_TIME_STAMP",
    )
    header_work["efile_selected_build_timestamp"] = header_work["F9_00_BUILD_TIME_STAMP"].fillna("").astype(str)
    header_work["efile_is_partial_year"] = (
        "true" if bool(asset_by_table["F9-P00-T00-HEADER"].get("is_partial_year")) else "false"
    )

    # Restrict to the intentionally supported filing forms before we do any
    # benchmark admission or table combine work.
    form_mask = header_work["harm_filing_form"].isin(["990", "990EZ"])
    form_rows = header_work.loc[form_mask].copy()
    header_form_row_count = int(len(form_rows))
    print(f"[efile-build] Header rows after supported-form filter: {header_form_row_count:,}", flush=True)
    if form_rows.empty:
        output_path.parent.mkdir(parents=True, exist_ok=True)
        pd.DataFrame().to_parquet(output_path, index=False)
        return {
            "input_row_count": input_row_count,
            "header_form_row_count": header_form_row_count,
            "benchmark_admitted_row_count": 0,
            "joined_row_count": 0,
            "duplicate_group_count": 0,
            "post_dedupe_row_count": 0,
            "output_row_count": 0,
            "matched_county_fips_count": 0,
            "is_partial_year": bool(asset_by_table["F9-P00-T00-HEADER"].get("is_partial_year")),
        }

    # Apply benchmark geography to HEADER rows before the wider joins. That
    # keeps the expensive combine stage on already-filtered filings.
    geoid_reference_set, geoid_to_region = load_geoid_reference_set(geoid_reference_path)
    benchmark_zip_to_county = load_benchmark_zip_to_county_map(geoid_reference_path, zip_to_county_path)
    form_rows["county_fips"] = form_rows["harm_zip5"].map(lambda value: normalize_fips5(benchmark_zip_to_county.get(value, "")))
    form_rows["region"] = form_rows["county_fips"].map(geoid_to_region).fillna("")
    form_rows["is_benchmark_county"] = (
        form_rows["county_fips"].isin(geoid_reference_set)
        & form_rows["region"].astype(str).str.strip().ne("")
    )
    benchmark_candidates = form_rows.loc[form_rows["is_benchmark_county"] == True].copy()  # noqa: E712
    benchmark_admitted_row_count = int(len(benchmark_candidates))
    print(
        f"[efile-build] Header rows admitted to benchmark geography before join: {benchmark_admitted_row_count:,}",
        flush=True,
    )
    if benchmark_candidates.empty:
        output_path.parent.mkdir(parents=True, exist_ok=True)
        pd.DataFrame().to_parquet(output_path, index=False)
        return {
            "input_row_count": input_row_count,
            "header_form_row_count": header_form_row_count,
            "benchmark_admitted_row_count": benchmark_admitted_row_count,
            "joined_row_count": 0,
            "duplicate_group_count": 0,
            "post_dedupe_row_count": 0,
            "output_row_count": 0,
            "matched_county_fips_count": 0,
            "is_partial_year": bool(asset_by_table["F9-P00-T00-HEADER"].get("is_partial_year")),
        }

    # Filing ranking still happens upstream, but now only within the already
    # admitted benchmark scope.
    benchmark_candidates["_return_timestamp_sort"] = pd.to_datetime(
        benchmark_candidates["efile_selected_return_timestamp"],
        errors="coerce",
        utc=True,
    )
    benchmark_candidates["_build_timestamp_sort"] = pd.to_datetime(
        benchmark_candidates["efile_selected_build_timestamp"],
        errors="coerce",
        utc=True,
    )
    benchmark_candidates["_amended_sort"] = benchmark_candidates["efile_selected_return_amended"].eq("true").astype(int)
    duplicate_group_count = int((benchmark_candidates.groupby(["harm_ein", "harm_tax_year"]).size() > 1).sum())
    print(
        f"[efile-build] Duplicate EIN+tax_year groups inside admitted header rows: {duplicate_group_count:,}",
        flush=True,
    )
    benchmark_candidates = benchmark_candidates.sort_values(
        by=["harm_ein", "harm_tax_year", "_return_timestamp_sort", "_amended_sort", "_build_timestamp_sort", "URL"],
        ascending=[True, True, False, False, False, True],
        kind="mergesort",
    )
    selected_headers = benchmark_candidates.drop_duplicates(subset=["harm_ein", "harm_tax_year"], keep="first").copy()
    post_dedupe_row_count = int(len(selected_headers))
    print(f"[efile-build] Selected benchmark filings after dedupe: {post_dedupe_row_count:,}", flush=True)

    # Only after benchmark admission and filing selection do we join the
    # heavier auxiliary tables. This is the actual filtered-only combine step.
    join_keys = ["URL", "ORG_EIN", "TAX_YEAR"]
    selected_headers = selected_headers.drop(
        columns=["_return_timestamp_sort", "_build_timestamp_sort", "_amended_sort"],
        errors="ignore",
    )
    merged = selected_headers.merge(summary_df, on=join_keys, how="left")
    merged = merged.merge(schedule_a_df, on=join_keys, how="left")
    merged = merged.fillna("")
    joined_row_count = int(len(merged))
    print(
        f"[efile-build] Joined benchmark-selected filings to SUMMARY/Schedule A rows: {joined_row_count:,}",
        flush=True,
    )

    merged["harm_revenue_amount"] = merged["F9_01_REV_TOT_CY"].fillna("").astype(str)
    merged["harm_expenses_amount"] = merged["F9_01_EXP_TOT_CY"].fillna("").astype(str)
    merged["harm_assets_amount"] = merged["F9_01_NAFB_ASSET_TOT_EOY"].fillna("").astype(str)
    merged["harm_income_amount"] = merged["F9_01_EXP_REV_LESS_EXP_CY"].fillna("").astype(str)
    merged["harm_is_hospital"] = _normalize_boolish(merged["SA_01_PCSTAT_HOSPITAL_X"])
    merged["harm_is_university"] = _normalize_boolish(merged["SA_01_PCSTAT_SCHOOL_X"])
    merged["harm_county_fips"] = merged["county_fips"]
    merged["harm_region"] = merged["region"]

    output_columns = [
        "harm_ein",
        "harm_tax_year",
        "harm_filing_form",
        "harm_org_name",
        "harm_state",
        "harm_zip5",
        "harm_county_fips",
        "harm_region",
        "harm_revenue_amount",
        "harm_expenses_amount",
        "harm_assets_amount",
        "harm_income_amount",
        "harm_is_hospital",
        "harm_is_university",
        "EIN2",
        "ORG_EIN",
        "RETURN_AMENDED_X",
        "RETURN_TIME_STAMP",
        "RETURN_TYPE",
        "TAX_PERIOD_BEGIN_DATE",
        "TAX_PERIOD_END_DATE",
        "TAX_YEAR",
        "URL",
        "F9_00_BUILD_TIME_STAMP",
        "F9_00_RETURN_TIME_STAMP",
        "F9_00_RETURN_TYPE",
        "F9_00_TAX_PERIOD_BEGIN_DATE",
        "F9_00_TAX_PERIOD_END_DATE",
        "F9_00_TAX_YEAR",
        "F9_00_RETURN_AMENDED_X",
        "F9_00_ORG_EIN",
        "F9_00_ORG_NAME_L1",
        "F9_00_ORG_ADDR_STATE",
        "F9_00_ORG_ADDR_ZIP",
        "F9_01_REV_TOT_CY",
        "F9_01_EXP_TOT_CY",
        "F9_01_NAFB_ASSET_TOT_EOY",
        "F9_01_EXP_REV_LESS_EXP_CY",
        "SA_01_PCSTAT_HOSPITAL_X",
        "SA_01_PCSTAT_SCHOOL_X",
        "county_fips",
        "region",
        "is_benchmark_county",
        "efile_is_partial_year",
        "efile_selected_filing_url",
        "efile_selected_return_amended",
        "efile_selected_return_timestamp",
        "efile_selected_build_timestamp",
    ]
    output_path.parent.mkdir(parents=True, exist_ok=True)
    merged[output_columns].to_parquet(output_path, index=False)
    return {
        "input_row_count": input_row_count,
        "header_form_row_count": header_form_row_count,
        "benchmark_admitted_row_count": benchmark_admitted_row_count,
        "joined_row_count": joined_row_count,
        "duplicate_group_count": duplicate_group_count,
        "post_dedupe_row_count": post_dedupe_row_count,
        "output_row_count": int(len(merged)),
        "matched_county_fips_count": int(merged["county_fips"].nunique()),
        "is_partial_year": bool(asset_by_table["F9-P00-T00-HEADER"].get("is_partial_year")),
    }
