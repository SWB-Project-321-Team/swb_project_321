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
import hashlib
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

from ingest._shared import transfers as shared_transfers  # noqa: E402
from utils.paths import DATA, get_base  # noqa: E402

# Keep print output live in terminal/IDE.
if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(line_buffering=True)


CATALOG_API_URL = "https://grantstory-api.gtdata.org/v2/public/678d25291eceb5d121099200/items?limit=200"

# Local directory layout.
RAW_ROOT = DATA / "raw" / "givingtuesday_990" / "datamarts"
RAW_DIR = RAW_ROOT / "raw"
META_DIR = RAW_ROOT / "metadata"
RAW_CACHE_DIR = RAW_ROOT / "cache"
STAGING_DIR = DATA / "staging" / "filing"
DOCS_PACKAGE_DIR = get_base() / "docs" / "final_preprocessing_docs"
DOCS_TECHNICAL_DIR = DOCS_PACKAGE_DIR / "technical_docs"
DOCS_ANALYSIS_DIR = DOCS_TECHNICAL_DIR / "analysis_variable_mappings"
DOCS_DATA_PROCESSING_DIR = DOCS_TECHNICAL_DIR / "pipeline_docs"
BMF_STAGING_DIR = DATA / "staging" / "nccs_bmf"
IRS_BMF_RAW_DIR = DATA / "raw" / "irs_bmf"

# Core local files.
CATALOG_JSON = META_DIR / "datamart_catalog_raw.json"
CATALOG_CSV = META_DIR / "datamart_catalog.csv"
CATALOG_MD = META_DIR / "datamart_catalog.md"
FIELDS_CSV = META_DIR / "datamart_fields.csv"
FIELDS_MD = META_DIR / "datamart_fields.md"
SIZE_REPORT_CSV = META_DIR / "size_verification_report.csv"
REQUIRED_MANIFEST_CSV = META_DIR / "required_datasets_manifest.csv"
DOWNLOAD_STATE_JSON = META_DIR / "download_state_manifest.json"
RAW_UPLOAD_STATE_JSON = META_DIR / "raw_bronze_upload_state.json"
RAW_VERIFY_STATE_JSON = META_DIR / "raw_size_verification_state.json"
COMBINED_NORMALIZED_CACHE_PARQUET = RAW_CACHE_DIR / "givingtuesday_990_combined_forms_normalized.parquet"
COMBINED_NORMALIZED_CACHE_MANIFEST_JSON = RAW_CACHE_DIR / "givingtuesday_990_combined_forms_normalized_manifest.json"

# The GT combine stages now operate only on filtered/admitted pre-Silver inputs.
# The canonical constants below use the current pre-Silver names directly.
BASIC_PRESILVER_PARQUET = STAGING_DIR / "givingtuesday_990_basic_allforms_presilver.parquet"
MIXED_PRESILVER_PARQUET = STAGING_DIR / "givingtuesday_990_basic_plus_combined_presilver.parquet"
BASIC_PRESILVER_BUILD_MANIFEST_JSON = STAGING_DIR / "manifest_basic_allforms_presilver.json"
MIXED_PRESILVER_BUILD_MANIFEST_JSON = STAGING_DIR / "manifest_basic_plus_combined_presilver.json"
BASIC_ALLFORMS_PARQUET = BASIC_PRESILVER_PARQUET
BASIC_PLUS_COMBINED_PARQUET = MIXED_PRESILVER_PARQUET
BASIC_ALLFORMS_BUILD_MANIFEST_JSON = BASIC_PRESILVER_BUILD_MANIFEST_JSON
BASIC_PLUS_COMBINED_BUILD_MANIFEST_JSON = MIXED_PRESILVER_BUILD_MANIFEST_JSON
FILTERED_BASIC_ALLFORMS_PARQUET = STAGING_DIR / "givingtuesday_990_basic_allforms_benchmark.parquet"
FILTERED_SILVER_PARQUET = STAGING_DIR / "givingtuesday_990_filings_benchmark.parquet"
FILTERED_BASIC_MANIFEST_JSON = STAGING_DIR / "manifest_basic_allforms_filtered.json"
FILTERED_MIXED_MANIFEST_JSON = STAGING_DIR / "manifest_filtered.json"
FILTERED_MANIFEST_JSON = FILTERED_MIXED_MANIFEST_JSON
CURATED_SIZE_REPORT_CSV = META_DIR / "curated_output_size_verification_report.csv"
FILTERED_BASIC_SCHEMA_JSON = META_DIR / "schema_snapshot_basic_allforms_filtered.json"
FILTERED_MIXED_SCHEMA_JSON = META_DIR / "schema_snapshot_filings_filtered.json"
GT_PIPELINE_ARTIFACT_MANIFEST_JSON = META_DIR / "gt_pipeline_artifact_manifest.json"
GT_PIPELINE_ARTIFACT_MANIFEST_CSV = META_DIR / "gt_pipeline_artifact_manifest.csv"
ORCHESTRATOR_SUMMARY_JSON = META_DIR / "run_990_datamart_pipeline_summary.json"
BASIC_RAW_VS_COMBINED_SUMMARY_CSV = META_DIR / "basic_raw_vs_basic_allforms_comparison_summary.csv"
BASIC_RAW_VS_COMBINED_SAMPLES_CSV = META_DIR / "basic_raw_vs_basic_allforms_comparison_samples.csv"
GT_BASIC_ANALYSIS_VARIABLES_PARQUET = STAGING_DIR / "givingtuesday_990_basic_allforms_analysis_variables.parquet"
GT_BASIC_ANALYSIS_REGION_METRICS_PARQUET = STAGING_DIR / "givingtuesday_990_basic_allforms_analysis_region_metrics.parquet"
GT_BASIC_ANALYSIS_VARIABLE_COVERAGE_CSV = META_DIR / "givingtuesday_990_basic_allforms_analysis_variable_coverage.csv"
GT_BASIC_ANALYSIS_VARIABLE_MAPPING_MD = DOCS_ANALYSIS_DIR / "givingtuesday_basic_analysis_variable_mapping.md"
GT_DATA_PROCESSING_DOC_MD = DOCS_DATA_PROCESSING_DIR / "givingtuesday_datamart_pipeline.md"
ORCHESTRATOR_STEP_TIMINGS_CSV = META_DIR / "run_990_datamart_pipeline_step_timings.csv"

# S3 defaults.
DEFAULT_S3_BUCKET = os.environ.get("IRS_990_S3_BUCKET", "swb-321-irs990-teos")
DEFAULT_S3_REGION = os.environ.get("AWS_DEFAULT_REGION", "us-east-2")
BRONZE_RAW_PREFIX = "bronze/givingtuesday_990/datamarts/raw"
BRONZE_META_PREFIX = "bronze/givingtuesday_990/datamarts/metadata"
BRONZE_PRESILVER_PREFIX = "bronze/givingtuesday_990/datamarts/presilver"
SILVER_PREFIX = "silver/givingtuesday_990/filing"
SILVER_META_PREFIX = f"{SILVER_PREFIX}/metadata"
ANALYSIS_PREFIX = "silver/givingtuesday_990/analysis"
ANALYSIS_DOCUMENTATION_PREFIX = f"{ANALYSIS_PREFIX}/documentation"
ANALYSIS_VARIABLE_MAPPING_PREFIX = f"{ANALYSIS_PREFIX}/variable_mappings"
ANALYSIS_COVERAGE_PREFIX = f"{ANALYSIS_PREFIX}/quality/coverage"

DOWNLOAD_WORKERS = shared_transfers.DOWNLOAD_WORKERS_DEFAULT
UPLOAD_WORKERS = shared_transfers.UPLOAD_WORKERS_DEFAULT
VERIFY_WORKERS = shared_transfers.VERIFY_WORKERS_DEFAULT
print_transfer_settings = shared_transfers.print_transfer_settings
parallel_map = shared_transfers.parallel_map
should_skip_upload = shared_transfers.should_skip_upload
print_parallel_summary = shared_transfers.print_parallel_summary

SIGNATURE_SAMPLE_BYTES = 64 * 1024
SIGNATURE_FULL_HASH_MAX_BYTES = 8 * 1024 * 1024

# Filtering references.
ZIP_TO_COUNTY_CSV = DATA / "reference" / "zip_to_county_fips.csv"
GEOID_REFERENCE_CSV = DATA / "reference" / "GEOID_reference.csv"


@dataclass(frozen=True)
class PipelineArtifact:
    """One local/S3 artifact owned by the GT datamart pipeline."""

    artifact_id: str
    local_path: Path
    s3_prefix: str
    content_type: str
    category: str


@dataclass(frozen=True)
class FilterTarget:
    """One benchmark-filter output target and its required inputs/metadata."""

    target_id: str
    input_path: Path
    output_artifact: PipelineArtifact
    manifest_artifact: PipelineArtifact


BASIC_PRESILVER_ARTIFACT = PipelineArtifact(
    artifact_id="basic_allforms_presilver",
    local_path=BASIC_ALLFORMS_PARQUET,
    s3_prefix=BRONZE_PRESILVER_PREFIX,
    content_type="application/octet-stream",
    category="bronze_presilver",
)

MIXED_PRESILVER_ARTIFACT = PipelineArtifact(
    artifact_id="basic_plus_combined_presilver",
    local_path=BASIC_PLUS_COMBINED_PARQUET,
    s3_prefix=BRONZE_PRESILVER_PREFIX,
    content_type="application/octet-stream",
    category="bronze_presilver",
)
FILTERED_BASIC_ALLFORMS_ARTIFACT = PipelineArtifact(
    artifact_id="basic_allforms_filtered",
    local_path=FILTERED_BASIC_ALLFORMS_PARQUET,
    s3_prefix=SILVER_PREFIX,
    content_type="application/octet-stream",
    category="silver_filtered",
)

FILTERED_MIXED_ARTIFACT = PipelineArtifact(
    artifact_id="basic_plus_combined_filtered",
    local_path=FILTERED_SILVER_PARQUET,
    s3_prefix=SILVER_PREFIX,
    content_type="application/octet-stream",
    category="silver_filtered",
)

FILTERED_BASIC_MANIFEST_ARTIFACT = PipelineArtifact(
    artifact_id="basic_allforms_filtered_manifest",
    local_path=FILTERED_BASIC_MANIFEST_JSON,
    s3_prefix=SILVER_PREFIX,
    content_type="application/json",
    category="silver_filtered_manifest",
)

FILTERED_MIXED_MANIFEST_ARTIFACT = PipelineArtifact(
    artifact_id="basic_plus_combined_filtered_manifest",
    local_path=FILTERED_MIXED_MANIFEST_JSON,
    s3_prefix=SILVER_PREFIX,
    content_type="application/json",
    category="silver_filtered_manifest",
)

FILTERED_BASIC_SCHEMA_ARTIFACT = PipelineArtifact(
    artifact_id="basic_allforms_filtered_schema_snapshot",
    local_path=FILTERED_BASIC_SCHEMA_JSON,
    s3_prefix=SILVER_META_PREFIX,
    content_type="application/json",
    category="silver_metadata",
)

FILTERED_MIXED_SCHEMA_ARTIFACT = PipelineArtifact(
    artifact_id="basic_plus_combined_filtered_schema_snapshot",
    local_path=FILTERED_MIXED_SCHEMA_JSON,
    s3_prefix=SILVER_META_PREFIX,
    content_type="application/json",
    category="silver_metadata",
)

CURATED_SIZE_REPORT_ARTIFACT = PipelineArtifact(
    artifact_id="curated_output_size_verification_report",
    local_path=CURATED_SIZE_REPORT_CSV,
    s3_prefix=SILVER_META_PREFIX,
    content_type="text/csv",
    category="silver_metadata",
)

GT_PIPELINE_ARTIFACT_MANIFEST_JSON_ARTIFACT = PipelineArtifact(
    artifact_id="gt_pipeline_artifact_manifest_json",
    local_path=GT_PIPELINE_ARTIFACT_MANIFEST_JSON,
    s3_prefix=SILVER_META_PREFIX,
    content_type="application/json",
    category="silver_metadata",
)

GT_PIPELINE_ARTIFACT_MANIFEST_CSV_ARTIFACT = PipelineArtifact(
    artifact_id="gt_pipeline_artifact_manifest_csv",
    local_path=GT_PIPELINE_ARTIFACT_MANIFEST_CSV,
    s3_prefix=SILVER_META_PREFIX,
    content_type="text/csv",
    category="silver_metadata",
)

GT_BASIC_ANALYSIS_VARIABLES_ARTIFACT = PipelineArtifact(
    artifact_id="gt_basic_analysis_variables",
    local_path=GT_BASIC_ANALYSIS_VARIABLES_PARQUET,
    s3_prefix=ANALYSIS_PREFIX,
    content_type="application/octet-stream",
    category="analysis_output",
)

GT_BASIC_ANALYSIS_REGION_METRICS_ARTIFACT = PipelineArtifact(
    artifact_id="gt_basic_analysis_region_metrics",
    local_path=GT_BASIC_ANALYSIS_REGION_METRICS_PARQUET,
    s3_prefix=ANALYSIS_PREFIX,
    content_type="application/octet-stream",
    category="analysis_output",
)

GT_BASIC_ANALYSIS_VARIABLE_COVERAGE_ARTIFACT = PipelineArtifact(
    artifact_id="gt_basic_analysis_variable_coverage",
    local_path=GT_BASIC_ANALYSIS_VARIABLE_COVERAGE_CSV,
    s3_prefix=ANALYSIS_COVERAGE_PREFIX,
    content_type="text/csv",
    category="analysis_metadata",
)

GT_BASIC_ANALYSIS_VARIABLE_MAPPING_ARTIFACT = PipelineArtifact(
    artifact_id="gt_basic_analysis_variable_mapping",
    local_path=GT_BASIC_ANALYSIS_VARIABLE_MAPPING_MD,
    s3_prefix=ANALYSIS_VARIABLE_MAPPING_PREFIX,
    content_type="text/markdown",
    category="analysis_metadata",
)

GT_DATA_PROCESSING_DOC_ARTIFACT = PipelineArtifact(
    artifact_id="gt_data_processing_doc",
    local_path=GT_DATA_PROCESSING_DOC_MD,
    s3_prefix=ANALYSIS_DOCUMENTATION_PREFIX,
    content_type="text/markdown",
    category="analysis_metadata",
)

FILTER_TARGET_BASIC = FilterTarget(
    target_id="basic",
    input_path=BASIC_ALLFORMS_PARQUET,
    output_artifact=FILTERED_BASIC_ALLFORMS_ARTIFACT,
    manifest_artifact=FILTERED_BASIC_MANIFEST_ARTIFACT,
)

FILTER_TARGET_MIXED = FilterTarget(
    target_id="mixed",
    input_path=BASIC_PLUS_COMBINED_PARQUET,
    output_artifact=FILTERED_MIXED_ARTIFACT,
    manifest_artifact=FILTERED_MIXED_MANIFEST_ARTIFACT,
)

VALID_EMIT_CHOICES = {"basic", "mixed", "both"}

NAME_LIKE_COLUMNS = [
    "FILERNAME1",
    "FILERNAME2",
    "FILERNAMECTRL",
    "OFFICERNAME",
    "OFFICERTITLE",
    "PREPARENAME",
]

STATE_CODE_COLUMNS = [
    "FILERUSSTATE",
    "FILERFORSTATE",
]

ZIP_CODE_COLUMNS = [
    "FILERUSZIP",
]

PHONE_NUMBER_COLUMNS = [
    "FILERPHONE",
]

DATE_COLUMNS = [
    "TAXPERBEGIN",
    "TAXPEREND",
    "OFFICERSIGNDATE",
    "PREPAREDATE",
]

BASIC_DEDUP_PRIORITY_COLUMNS = [
    "FILERNAME1",
    "FILERUSZIP",
    "FILERUSSTATE",
    "FILERPHONE",
    "TAXPERBEGIN",
    "TAXPEREND",
    "TOTREVCURYEA",
    "TOTEXPCURYEA",
    "TOTALRREVENU",
    "TOTALEEXPENS",
    # Form 990PF carries total revenue and total expense under PF-specific GT
    # standard-field names. Include them in the best-row score so amended or
    # duplicate PF filings are ranked with the same financial evidence that the
    # analysis layer now uses.
    "ANREEXTOREEX",
    "ARETEREXPNSS",
    "TOASEOOYY",
    "TOLIEOOYY",
    "ASSEOYOYY",
    "OFFICERNAME",
    "OFFICERTITLE",
]

# Step 06 writes an admitted/filtered Basic pre-Silver artifact before any GT
# combine stage runs. These metadata columns preserve the upstream admission
# basis so step 07 can combine only admitted rows while still keeping rescue
# behavior auditable.
GT_BASIC_PRESILVER_METADATA_COLUMNS = [
    "presilver_admitted_by_basic_geography",
    "presilver_admitted_by_combined_key",
    "presilver_admission_basis",
]

# Step 07 writes the mixed GT pre-Silver artifact as ROI-scoped content. These
# metadata columns make the mixed-stage admission basis explicit without
# changing the canonical downstream Silver mixed artifact path.
GT_MIXED_ROI_METADATA_COLUMNS = [
    "roi_admitted_by_basic",
    "roi_admitted_by_combined",
    "roi_admission_basis",
    "roi_geography_override_from_basic",
    "roi_geography_override_field_count_from_basic",
]


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
    for p in (RAW_DIR, META_DIR, RAW_CACHE_DIR, STAGING_DIR, DOCS_ANALYSIS_DIR, DOCS_DATA_PROCESSING_DIR):
        p.mkdir(parents=True, exist_ok=True)
        print(f"[paths] Ready: {p}", flush=True)


def discover_bmf_exact_year_lookup_inputs(root: Path = BMF_STAGING_DIR) -> dict[str, Path]:
    """
    Discover staged NCCS BMF exact-year lookup artifacts keyed by tax year.

    GT step 13 uses these lookup files only for the explicit NTEE enrichment
    exception. The exact-year join is strict on EIN + tax_year, so the returned
    mapping is one lookup parquet per available year.
    """
    candidates = sorted(root.glob("year=*/nccs_bmf_exact_year_lookup_year=*.parquet"))
    if not candidates:
        raise FileNotFoundError(f"No exact-year NCCS BMF lookup outputs found under {root}")

    by_year: dict[str, Path] = {}
    for path in candidates:
        snapshot_year = path.parent.name.split("=", 1)[1]
        by_year[snapshot_year] = path
    return dict(sorted(by_year.items(), key=lambda item: int(item[0])))


def discover_irs_bmf_raw_inputs(root: Path = IRS_BMF_RAW_DIR) -> list[Path]:
    """
    Discover raw IRS EO BMF CSV inputs keyed only by file path.

    GT step 13 uses these state-level IRS EO BMF files only as a final fallback
    for missing NTEE values after NCCS exact-year and nearest-year enrichment
    have both been attempted. Keeping discovery centralized here makes the raw
    IRS fallback auditable and avoids hard-coding file assumptions in the
    analysis-extract step itself.
    """
    candidates = sorted(root.glob("eo_*.csv"))
    if not candidates:
        raise FileNotFoundError(f"No IRS EO BMF raw CSV files found under {root}")
    return candidates


def write_json(path: Path, payload: dict[str, Any]) -> None:
    """Write one JSON document with stable formatting."""
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")


def read_json(path: Path) -> dict[str, Any]:
    """Read one JSON document from disk."""
    return json.loads(path.read_text(encoding="utf-8"))


def build_path_signature(path: Path) -> dict[str, Any]:
    stat = path.stat()
    signature: dict[str, Any] = {
        "path": str(path),
        "size_bytes": stat.st_size,
    }
    with open(path, "rb") as handle:
        if stat.st_size <= SIGNATURE_FULL_HASH_MAX_BYTES:
            hasher = hashlib.sha1()
            while True:
                chunk = handle.read(1024 * 1024)
                if not chunk:
                    break
                hasher.update(chunk)
            signature["hash_mode"] = "full"
            signature["content_sha1"] = hasher.hexdigest()
            return signature

        head = handle.read(SIGNATURE_SAMPLE_BYTES)
        head_hasher = hashlib.sha1()
        head_hasher.update(head)

        tail_hasher = hashlib.sha1()
        if stat.st_size > SIGNATURE_SAMPLE_BYTES:
            handle.seek(max(0, stat.st_size - SIGNATURE_SAMPLE_BYTES))
        tail_hasher.update(handle.read(SIGNATURE_SAMPLE_BYTES))

    signature["hash_mode"] = "head_tail"
    signature["head_sha1"] = head_hasher.hexdigest()
    signature["tail_sha1"] = tail_hasher.hexdigest()
    return signature


def build_input_signature(path_map: dict[str, Path]) -> dict[str, dict[str, Any]]:
    """Return a stable input-signature mapping for required files."""
    return {
        label: build_path_signature(path)
        for label, path in sorted(path_map.items())
    }


def emit_targets(emit: str) -> list[str]:
    """Normalize one emit selection into ordered target ids."""
    choice = (emit or "both").strip().lower()
    if choice not in VALID_EMIT_CHOICES:
        raise ValueError(f"Invalid emit choice: {emit}. Expected one of {sorted(VALID_EMIT_CHOICES)}")
    if choice == "both":
        return ["basic", "mixed"]
    return [choice]


def filter_targets(emit: str = "both") -> list[FilterTarget]:
    """Return the ordered benchmark-filter targets for one emit mode."""
    all_targets = {
        "basic": FILTER_TARGET_BASIC,
        "mixed": FILTER_TARGET_MIXED,
    }
    return [all_targets[target_id] for target_id in emit_targets(emit)]


def bronze_presilver_artifacts() -> list[PipelineArtifact]:
    """
    Return the GT pre-Silver parquet artifacts uploaded to Bronze.

    Both artifacts are now filtered/admitted pre-Silver stages rather than raw
    nationwide combine outputs:
    - the Basic pre-Silver parquet contains only tax-year-in-scope rows
      admitted by benchmark geography or by a Combined ROI key
    - the mixed pre-Silver parquet contains only rows admitted before the heavy
      Basic+Combined merge runs
    """
    return [
        BASIC_PRESILVER_ARTIFACT,
        MIXED_PRESILVER_ARTIFACT,
    ]
def silver_filtered_artifacts(emit: str = "both") -> list[PipelineArtifact]:
    """Return the filtered parquet/manifests uploaded to Silver."""
    artifacts: list[PipelineArtifact] = []
    for target in filter_targets(emit):
        artifacts.extend([target.output_artifact, target.manifest_artifact])
        if target.target_id == "basic":
            artifacts.append(FILTERED_BASIC_SCHEMA_ARTIFACT)
        elif target.target_id == "mixed":
            artifacts.append(FILTERED_MIXED_SCHEMA_ARTIFACT)
    return artifacts


def curated_output_artifacts(emit: str = "both") -> list[PipelineArtifact]:
    """Return every curated output artifact covered by upload/verify."""
    return bronze_presilver_artifacts() + silver_filtered_artifacts(emit)


def analysis_output_artifacts() -> list[PipelineArtifact]:
    """
    Return the final GT analysis artifacts uploaded after step 13.

    These artifacts are produced by the analysis-extraction step rather than the
    Bronze/Silver filing curation flow, so they use a separate Silver analysis
    prefix instead of the filing prefix.
    """
    return [
        GT_BASIC_ANALYSIS_VARIABLES_ARTIFACT,
        GT_BASIC_ANALYSIS_REGION_METRICS_ARTIFACT,
        GT_BASIC_ANALYSIS_VARIABLE_COVERAGE_ARTIFACT,
        GT_BASIC_ANALYSIS_VARIABLE_MAPPING_ARTIFACT,
        GT_DATA_PROCESSING_DOC_ARTIFACT,
    ]


def pipeline_metadata_artifacts() -> list[PipelineArtifact]:
    """Return GT pipeline-wide metadata artifacts written after curated verification."""
    return [
        CURATED_SIZE_REPORT_ARTIFACT,
        GT_PIPELINE_ARTIFACT_MANIFEST_JSON_ARTIFACT,
        GT_PIPELINE_ARTIFACT_MANIFEST_CSV_ARTIFACT,
    ]


def bronze_raw_metadata_files() -> list[Path]:
    """Return the raw-phase metadata files owned by the Bronze raw upload step."""
    return [
        CATALOG_JSON,
        CATALOG_CSV,
        CATALOG_MD,
        FIELDS_CSV,
        FIELDS_MD,
        REQUIRED_MANIFEST_CSV,
    ]


def artifact_s3_key(artifact: PipelineArtifact, prefix: str | None = None) -> str:
    """Return the S3 key for one registered artifact."""
    base_prefix = prefix if prefix is not None else artifact.s3_prefix
    return f"{base_prefix.rstrip('/')}/{artifact.local_path.name}"


def batch_s3_object_sizes(
    bucket: str,
    path_to_s3_key: dict[str, str],
    region: str,
    *,
    workers: int | None = None,
) -> dict[str, int | None]:
    """Resolve many GT artifact S3 sizes keyed by local-path string."""
    if not path_to_s3_key:
        return {}
    task_list = [(bucket, key) for key in path_to_s3_key.values()]
    resolved_workers = max(1, min(workers or VERIFY_WORKERS, len(task_list)))
    size_by_task = shared_transfers.batch_s3_object_sizes(
        task_list,
        region=region,
        worker_count=resolved_workers,
    )
    return {
        path: size_by_task.get((bucket, s3_key))
        for path, s3_key in path_to_s3_key.items()
    }


def manifest_matches_expectation(
    manifest_path: Path,
    *,
    expected_input_signature: dict[str, dict[str, Any]],
    expected_options: dict[str, Any],
    required_outputs: list[Path],
) -> bool:
    """True when a cached manifest matches current inputs, options, and outputs."""
    if not manifest_path.exists():
        return False
    if not all(path.exists() for path in required_outputs):
        return False
    cached = read_json(manifest_path)
    return (
        cached.get("input_signature") == expected_input_signature
        and cached.get("build_options") == expected_options
    )


def write_schema_snapshot(path: Path, *, artifact_id: str, output_path: Path, columns: list[str]) -> None:
    """Write a stable schema snapshot JSON for one filtered artifact."""
    write_json(
        path,
        {
            "artifact_id": artifact_id,
            "output_path": str(output_path),
            "column_count": len(columns),
            "columns": list(columns),
        },
    )


def expected_schema_snapshot_artifacts(emit: str = "both") -> list[PipelineArtifact]:
    """Return the schema-snapshot artifacts that should exist for one emit mode."""
    artifacts: list[PipelineArtifact] = []
    for target_id in emit_targets(emit):
        if target_id == "basic":
            artifacts.append(FILTERED_BASIC_SCHEMA_ARTIFACT)
        elif target_id == "mixed":
            artifacts.append(FILTERED_MIXED_SCHEMA_ARTIFACT)
    return artifacts


def non_emitted_filter_artifacts(emit: str) -> list[PipelineArtifact]:
    """Return filtered artifacts that will not be emitted for one run mode."""
    emitted = {artifact.artifact_id for artifact in silver_filtered_artifacts(emit)}
    all_filter_related = silver_filtered_artifacts("both")
    return [artifact for artifact in all_filter_related if artifact.artifact_id not in emitted]


def stale_output_warnings(emit: str) -> list[str]:
    """Return warnings for non-emitted filtered artifacts still present on disk."""
    warnings: list[str] = []
    for artifact in non_emitted_filter_artifacts(emit):
        if artifact.local_path.exists():
            warnings.append(
                f"Non-emitted artifact still exists locally and may be stale: {artifact.local_path}"
            )
    return warnings


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


def gt_blank_expr(expr: Any) -> Any:
    """
    Return a Polars expression that treats null/whitespace as blank.

    The GT pipeline uses this helper in multiple places so blank semantics stay
    identical across ROI filtering, validation, and downstream GT analysis
    extraction logic.
    """
    import polars as pl

    return expr.is_null() | expr.cast(pl.Utf8, strict=False).str.strip_chars().eq("")


def gt_normalize_zip5_expr(expr: Any) -> Any:
    """
    Return a Polars expression that canonicalizes ZIP codes to ZIP5.

    The ROI filters should operate on one canonical ZIP representation instead
    of letting slightly different raw ZIP text produce different geography
    outcomes across datasets or steps.
    """
    import polars as pl

    digits = expr.cast(pl.Utf8, strict=False).fill_null("").str.replace_all(r"[^0-9]", "")
    return pl.when(digits.str.len_chars() >= 5).then(digits.str.slice(0, 5)).otherwise(pl.lit(""))


def gt_normalize_fips5_expr(expr: Any) -> Any:
    """Return a Polars expression that canonicalizes county FIPS/GEOID to 5 digits."""
    import polars as pl

    digits = expr.cast(pl.Utf8, strict=False).fill_null("").str.replace_all(r"[^0-9]", "")
    return (
        pl.when(digits.str.len_chars() == 0)
        .then(pl.lit(""))
        .when(digits.str.len_chars() < 5)
        .then(digits.str.zfill(5))
        .otherwise(digits.str.slice(0, 5))
    )


def gt_normalize_tax_year_expr(expr: Any) -> Any:
    """Return a Polars expression that canonicalizes tax year text to integer-like strings."""
    import polars as pl

    raw = expr.cast(pl.Utf8, strict=False).fill_null("").str.strip_chars()
    as_float = raw.cast(pl.Float64, strict=False)
    return (
        pl.when(raw.eq(""))
        .then(pl.lit(""))
        .when(as_float.is_not_null())
        .then(as_float.cast(pl.Int64, strict=False).cast(pl.Utf8, strict=False))
        .otherwise(raw)
    )


def load_gt_geoid_reference(path_csv: Path = GEOID_REFERENCE_CSV) -> Any:
    """
    Load the benchmark-county reference with one canonical county_fips/region schema.

    This helper keeps the GT pipeline and the related nonprofit filters aligned
    on the exact same benchmark geography source and normalization rule.
    """
    import polars as pl

    if not path_csv.exists():
        raise FileNotFoundError(
            f"GEOID reference CSV not found: {path_csv}. "
            "Run python/ingest/location_processing/01_fetch_geoid_reference.py first."
        )
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
                gt_normalize_fips5_expr(pl.col(geoid_col)).alias("county_fips"),
                pl.col(region_col).cast(pl.Utf8, strict=False).fill_null("").alias("region"),
            ]
        )
        .filter((pl.col("county_fips").str.len_chars() == 5) & ~gt_blank_expr(pl.col("region")))
        .unique(subset=["county_fips"], keep="first", maintain_order=True)
    )


def load_gt_zip_to_fips(path_csv: Path = ZIP_TO_COUNTY_CSV) -> Any:
    """
    Load the ZIP-to-county crosswalk with canonical ZIP5/county_fips columns.

    Centralizing this logic prevents the GT filter and downstream analysis steps
    from drifting into slightly different ZIP normalization rules.
    """
    import polars as pl

    if not path_csv.exists():
        raise FileNotFoundError(f"ZIP-to-county file not found: {path_csv}")
    crosswalk = pl.read_csv(str(path_csv), infer_schema_length=0)
    zip_col = next((c for c in crosswalk.columns if "zip" in c.lower()), crosswalk.columns[0])
    fips_col = next(
        (c for c in crosswalk.columns if "fips" in c.lower()),
        crosswalk.columns[1] if len(crosswalk.columns) > 1 else None,
    )
    if fips_col is None:
        raise RuntimeError("ZIP-to-county CSV missing FIPS column.")
    return (
        crosswalk.select(
            [
                gt_normalize_zip5_expr(pl.col(zip_col)).alias("zip5"),
                gt_normalize_fips5_expr(pl.col(fips_col)).alias("county_fips"),
            ]
        )
        .filter((pl.col("zip5").str.len_chars() == 5) & (pl.col("county_fips").str.len_chars() == 5))
        .unique(subset=["zip5", "county_fips"], keep="first", maintain_order=True)
    )


def build_gt_roi_zip_map(zip_df: Any, geoid_df: Any) -> Any:
    """
    Build the explicit GT benchmark ZIP map from benchmark counties only.

    The pipeline intentionally rejects ambiguous ROI ZIP assignments instead of
    silently collapsing them, because an ambiguous ZIP would otherwise make the
    region filter non-auditable.
    """
    import polars as pl

    roi_zip_df = (
        zip_df.join(geoid_df, on="county_fips", how="inner")
        .select(["zip5", "county_fips", "region"])
        .unique(subset=["zip5", "county_fips", "region"], keep="first", maintain_order=True)
    )
    ambiguous = (
        roi_zip_df.group_by("zip5")
        .agg(pl.n_unique("county_fips").alias("county_count"))
        .filter(pl.col("county_count") > 1)
    )
    if ambiguous.height:
        examples = ambiguous.sort("zip5").head(10).to_dicts()
        raise RuntimeError(
            "Benchmark ZIP map is ambiguous: at least one ROI ZIP resolves to multiple benchmark counties. "
            f"Examples: {examples}"
        )
    return roi_zip_df.unique(subset=["zip5"], keep="first", maintain_order=True)


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
    response = shared_transfers.managed_requests("GET", url, stream=True, timeout=timeout)
    with response:
        response.raise_for_status()
        header_line = None
        for line in response.iter_lines(decode_unicode=True):
            if line:
                header_line = line
                break
        if not header_line:
            return []
        return next(csv.reader([header_line]))


def download_with_progress(url: str, output_path: Path, expected_bytes: int | None = None, timeout: int = 120) -> int:
    """Download URL to output_path with a byte progress bar. Returns local bytes written."""
    return shared_transfers.download_with_progress(
        url,
        output_path,
        expected_bytes=expected_bytes,
        timeout=timeout,
    )


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
    return shared_transfers.s3_client(region)


def upload_file_with_progress(local_path: Path, bucket: str, key: str, region: str, extra_args: dict[str, Any] | None = None) -> None:
    """Upload a local file to S3 with byte progress."""
    shared_transfers.upload_file_with_progress(local_path, bucket, key, region, extra_args=extra_args)


def s3_object_size(bucket: str, key: str, region: str) -> int | None:
    """Return S3 object size in bytes or None if missing/error."""
    return shared_transfers.s3_object_size(bucket, key, region)


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


def ensure_combined_normalized_cache(
    *,
    combined_csv_path: Path,
    output_path: Path = COMBINED_NORMALIZED_CACHE_PARQUET,
    manifest_path: Path = COMBINED_NORMALIZED_CACHE_MANIFEST_JSON,
    force_rebuild: bool = False,
) -> dict[str, Any]:
    """
    Materialize a cached parquet mirror of the raw GT Combined CSV.

    Step 07 is the heaviest local build stage because reparsing the full Combined
    CSV is expensive. This cache keeps the raw CSV immutable while moving the
    repeated parsing cost upstream into one reusable local parquet artifact.
    """
    import polars as pl

    if not combined_csv_path.exists():
        raise FileNotFoundError(f"Combined raw CSV not found: {combined_csv_path}")

    output_path.parent.mkdir(parents=True, exist_ok=True)
    manifest_path.parent.mkdir(parents=True, exist_ok=True)
    input_signature = build_input_signature({"combined_raw_csv": combined_csv_path})
    build_options = {
        "output_path": str(output_path),
        "script": "combined_normalized_cache",
    }
    if not force_rebuild and manifest_matches_expectation(
        manifest_path,
        expected_input_signature=input_signature,
        expected_options=build_options,
        required_outputs=[output_path],
    ):
        cached = read_json(manifest_path)
        print("[combined-cache] Raw Combined CSV matches cached parquet mirror; skipping rebuild.", flush=True)
        print(f"[combined-cache] Cached rows: {cached.get('rows_output', '')}", flush=True)
        return cached

    cache_start = time.perf_counter()
    print(f"[combined-cache] Source CSV:  {combined_csv_path}", flush=True)
    print(f"[combined-cache] Output file: {output_path}", flush=True)
    print("[combined-cache] Building normalized parquet mirror from raw Combined CSV...", flush=True)

    # infer_schema_length=0 keeps the CSV source semantically close to the raw file
    # by treating columns as strings unless a later stage explicitly derives types.
    combined_lf = pl.scan_csv(str(combined_csv_path), infer_schema_length=0)
    output_columns = combined_lf.collect_schema().names()
    try:
        combined_lf.sink_parquet(str(output_path), compression="snappy")
    except Exception as exc:
        print(
            f"[combined-cache] sink_parquet unavailable ({type(exc).__name__}); "
            "falling back to collect().write_parquet().",
            flush=True,
        )
        combined_lf.collect().write_parquet(str(output_path), compression="snappy")

    rows_output = (
        pl.scan_parquet(str(output_path))
        .select(pl.len().alias("rows_output"))
        .collect()
        .item(0, 0)
    )
    payload = {
        "created_at_utc": now_utc_iso(),
        "input_signature": input_signature,
        "build_options": build_options,
        "source_csv": str(combined_csv_path),
        "output_path": str(output_path),
        "rows_output": int(rows_output),
        "columns_output": len(output_columns),
    }
    write_json(manifest_path, payload)
    print(f"[combined-cache] Wrote manifest: {manifest_path}", flush=True)
    print_elapsed(cache_start, "build combined normalized parquet cache")
    return payload
