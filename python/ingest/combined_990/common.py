"""
Shared helpers for the combined filtered 990 union + master pipeline.

The combined pipeline now emits two artifacts:
- one output row still corresponds to one original filtered source row
- one master output row corresponds to one `EIN + tax_year`
- row-level source provenance is always explicit
- harmonized columns sit alongside source-prefixed native columns
- downstream diagnostics are built from the combined frame, not from raw data
"""

from __future__ import annotations

import hashlib
import importlib.util
import json
import os
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

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

COMBINED_SCHEMA_VERSION = "1.6.0"

# Keep the mixed/basic+combined GT filtered artifact as the canonical downstream
# input. The GT pipeline now also emits a basic-only filtered analyst artifact,
# but the combined source-union contract remains anchored to the mixed file.
GT_FILTERED_PARQUET = DATA / "staging" / "filing" / "givingtuesday_990_filings_benchmark.parquet"
POSTCARD_STAGING_ROOT = DATA / "staging" / "nccs_990" / "postcard"
EFILE_STAGING_ROOT = DATA / "staging" / "nccs_efile"
CORE_STAGING_ROOT = DATA / "staging" / "nccs_990" / "core"
BMF_STAGING_ROOT = DATA / "staging" / "nccs_bmf"
GEOID_REFERENCE_CSV = DATA / "reference" / "GEOID_reference.csv"

STAGING_DIR = DATA / "staging" / "combined_990"
OUTPUT_PARQUET = STAGING_DIR / "combined_990_filtered_source_union.parquet"
MASTER_OUTPUT_PARQUET = STAGING_DIR / "combined_990_master_ein_tax_year.parquet"
SOURCE_INPUT_MANIFEST_CSV = STAGING_DIR / "source_input_manifest.csv"
COLUMN_DICTIONARY_CSV = STAGING_DIR / "column_dictionary.csv"
FIELD_AVAILABILITY_MATRIX_CSV = STAGING_DIR / "field_availability_matrix.csv"
DIAG_OVERLAP_BY_EIN_CSV = STAGING_DIR / "diag_overlap_by_ein.csv"
DIAG_OVERLAP_BY_EIN_TAX_YEAR_CSV = STAGING_DIR / "diag_overlap_by_ein_tax_year.csv"
DIAG_OVERLAP_SUMMARY_CSV = STAGING_DIR / "diag_overlap_summary.csv"
MASTER_COLUMN_DICTIONARY_CSV = STAGING_DIR / "master_column_dictionary.csv"
MASTER_FIELD_SELECTION_SUMMARY_CSV = STAGING_DIR / "master_field_selection_summary.csv"
MASTER_CONFLICT_SUMMARY_CSV = STAGING_DIR / "master_conflict_summary.csv"
MASTER_CONFLICT_DETAIL_CSV = STAGING_DIR / "master_conflict_detail.csv"
MASTER_CONFLICT_DETAIL_PARQUET = STAGING_DIR / "master_conflict_detail.parquet"
BUILD_SUMMARY_JSON = STAGING_DIR / "build_summary.json"
SIZE_VERIFICATION_CSV = STAGING_DIR / "size_verification.csv"

DEFAULT_S3_BUCKET = _CORE_COMMON.DEFAULT_S3_BUCKET
DEFAULT_S3_REGION = _CORE_COMMON.DEFAULT_S3_REGION
SILVER_PREFIX = "silver/combined_990"
METADATA_S3_PREFIX = f"{SILVER_PREFIX}/metadata"

banner = _CORE_COMMON.banner
print_elapsed = _CORE_COMMON.print_elapsed
print_transfer_settings = _CORE_COMMON.print_transfer_settings
load_env_from_secrets = _CORE_COMMON.load_env_from_secrets
guess_content_type = _CORE_COMMON.guess_content_type
write_json = _CORE_COMMON.write_json
read_json = _CORE_COMMON.read_json
write_csv = _CORE_COMMON.write_csv
load_csv_rows = _CORE_COMMON.load_csv_rows
meta_s3_key = _CORE_COMMON.meta_s3_key
should_skip_upload = _CORE_COMMON.should_skip_upload
compute_local_s3_match = _CORE_COMMON.compute_local_s3_match
s3_object_size = _CORE_COMMON.s3_object_size
upload_file_with_progress = _CORE_COMMON.upload_file_with_progress
parallel_map = _CORE_COMMON.parallel_map
UPLOAD_WORKERS = _CORE_COMMON.UPLOAD_WORKERS
VERIFY_WORKERS = _CORE_COMMON.VERIFY_WORKERS

ROW_PROVENANCE_COLUMNS = [
    "row_source_family",
    "row_source_variant",
    "row_source_file_name",
    "row_source_file_path",
    "row_source_row_number",
    "row_source_time_basis",
    "row_source_snapshot_year",
    "row_source_snapshot_month",
]

HARMONIZED_COLUMNS = [
    "harm_ein",
    "harm_tax_year",
    "harm_filing_form",
    "harm_org_name",
    "harm_state",
    "harm_zip5",
    "harm_county_fips",
    "harm_region",
    "harm_ntee_code",
    "harm_subsection_code",
    "harm_revenue_amount",
    "harm_expenses_amount",
    "harm_assets_amount",
    "harm_income_amount",
    "harm_gross_receipts_under_25000_flag",
    "harm_is_hospital",
    "harm_is_university",
]

OVERLAY_FIELDS = [
    "harm_org_name",
    "harm_state",
    "harm_zip5",
    "harm_ntee_code",
    "harm_subsection_code",
]

NUMERIC_HELPER_COLUMNS = [
    "harm_revenue_amount_num",
    "harm_expenses_amount_num",
    "harm_assets_amount_num",
    "harm_income_amount_num",
    "harm_revenue_amount_num_parse_ok",
    "harm_expenses_amount_num_parse_ok",
    "harm_assets_amount_num_parse_ok",
    "harm_income_amount_num_parse_ok",
]

SOURCE_PREFIXES = {
    "givingtuesday_datamart": "gt__",
    "nccs_postcard": "nccs_postcard__",
    "nccs_efile": "nccs_efile__",
    "nccs_core": "nccs_core__",
    "nccs_bmf": "nccs_bmf__",
}

SOURCE_TIME_BASIS_BY_FAMILY = {
    "givingtuesday_datamart": "tax_year",
    "nccs_postcard": "tax_year_in_snapshot",
    "nccs_efile": "tax_year",
    "nccs_core": "tax_year",
    "nccs_bmf": "reference_snapshot_year",
}


@dataclass(frozen=True)
class SourceInput:
    """One filtered source file that feeds the combined union table."""

    source_family: str
    source_variant: str
    input_path: Path
    input_format: str
    snapshot_year: str
    snapshot_month: str
    native_prefix: str

    def to_manifest_dict(self) -> dict[str, str]:
        return {
            "source_family": self.source_family,
            "source_variant": self.source_variant,
            "input_path": str(self.input_path),
            "input_format": self.input_format,
            "snapshot_year": self.snapshot_year,
            "snapshot_month": self.snapshot_month,
        }


def ensure_work_dirs(staging_dir: Path = STAGING_DIR) -> None:
    """Ensure the combined pipeline's local working directory exists."""
    staging_dir.mkdir(parents=True, exist_ok=True)
    print(f"[paths] Ready: {staging_dir}", flush=True)


def combined_s3_key(silver_prefix: str = SILVER_PREFIX) -> str:
    """Return the Silver S3 key for the combined parquet."""
    return f"{silver_prefix.rstrip('/')}/{OUTPUT_PARQUET.name}"


def master_s3_key(silver_prefix: str = SILVER_PREFIX) -> str:
    """Return the Silver S3 key for the combined master parquet."""
    return f"{silver_prefix.rstrip('/')}/{MASTER_OUTPUT_PARQUET.name}"


def metadata_s3_key(filename: str, metadata_prefix: str = METADATA_S3_PREFIX) -> str:
    """Return the Silver S3 key for one combined metadata file."""
    return f"{metadata_prefix.rstrip('/')}/{filename}"


def batch_s3_object_sizes(
    bucket: str,
    path_to_s3_key: dict[str, str],
    region: str,
    *,
    workers: int | None = None,
) -> dict[str, int | None]:
    """
    Resolve S3 object sizes for combined-pipeline files using the shared helper.

    The shared transfer layer works with `(bucket, key)` tasks. The combined
    verify step is easier to read when it stays keyed by local-path string, so
    this small adapter preserves that caller-facing interface while delegating
    the actual parallel S3 head-object work to the centralized helper.
    """
    if not path_to_s3_key:
        return {}
    task_list = [(bucket, key) for key in path_to_s3_key.values()]
    resolved_workers = max(1, min(workers or VERIFY_WORKERS, len(task_list)))
    size_by_task = _CORE_COMMON.batch_s3_object_sizes(
        task_list,
        region=region,
        worker_count=resolved_workers,
    )
    return {
        path: size_by_task.get((bucket, s3_key))
        for path, s3_key in path_to_s3_key.items()
    }


def union_metadata_files(*, include_verification: bool = True) -> list[Path]:
    """Return the current union-output metadata files in a stable upload/verify order."""
    files = [
        SOURCE_INPUT_MANIFEST_CSV,
        COLUMN_DICTIONARY_CSV,
        FIELD_AVAILABILITY_MATRIX_CSV,
        DIAG_OVERLAP_BY_EIN_CSV,
        DIAG_OVERLAP_BY_EIN_TAX_YEAR_CSV,
        DIAG_OVERLAP_SUMMARY_CSV,
        BUILD_SUMMARY_JSON,
    ]
    if include_verification:
        files.append(SIZE_VERIFICATION_CSV)
    return files


def master_metadata_files() -> list[Path]:
    """Return the master-specific metadata files in a stable upload/verify order."""
    return [
        MASTER_COLUMN_DICTIONARY_CSV,
        MASTER_FIELD_SELECTION_SUMMARY_CSV,
        MASTER_CONFLICT_SUMMARY_CSV,
        MASTER_CONFLICT_DETAIL_CSV,
        MASTER_CONFLICT_DETAIL_PARQUET,
    ]


def all_metadata_files(*, include_verification: bool = True) -> list[Path]:
    """Return every combined-pipeline metadata file tracked by upload and verification."""
    return union_metadata_files(include_verification=include_verification) + master_metadata_files()


def _find_latest_postcard_input(root: Path = POSTCARD_STAGING_ROOT) -> SourceInput:
    """Find the latest postcard benchmark derivative restricted to `tax_year >= 2022`."""
    candidates = sorted(root.glob("snapshot_year=*/nccs_990_postcard_benchmark_tax_year_start=2022_snapshot_year=*.parquet"))
    if not candidates:
        raise FileNotFoundError(f"No postcard tax_year>=2022 benchmark parquet outputs found under {root}")
    selected = max(candidates, key=lambda path: int(path.parent.name.split("=", 1)[1]))
    snapshot_year = selected.parent.name.split("=", 1)[1]
    return SourceInput(
        source_family="nccs_postcard",
        source_variant=f"snapshot_year_{snapshot_year}_tax_year_start_2022",
        input_path=selected,
        input_format="parquet",
        snapshot_year=snapshot_year,
        snapshot_month="",
        native_prefix=SOURCE_PREFIXES["nccs_postcard"],
    )


def _find_efile_inputs(root: Path = EFILE_STAGING_ROOT) -> list[SourceInput]:
    """Find all annualized NCCS efile benchmark parquets from 2022 onward."""
    candidates = sorted(root.glob("tax_year=*/nccs_efile_benchmark_tax_year=*.parquet"))
    if not candidates:
        raise FileNotFoundError(f"No annualized NCCS efile benchmark outputs found under {root}")
    inputs: list[SourceInput] = []
    for path in candidates:
        tax_year = int(path.parent.name.split("=", 1)[1])
        if tax_year < 2022:
            continue
        inputs.append(
            SourceInput(
                source_family="nccs_efile",
                source_variant=f"efile_tax_year_{tax_year}",
                input_path=path,
                input_format="parquet",
                snapshot_year="",
                snapshot_month="",
                native_prefix=SOURCE_PREFIXES["nccs_efile"],
            )
        )
    if not inputs:
        raise FileNotFoundError(f"No annualized NCCS efile benchmark outputs >= 2022 found under {root}")
    return inputs


def _find_bmf_inputs(root: Path = BMF_STAGING_ROOT) -> list[SourceInput]:
    """Find the filtered yearly NCCS BMF parquet outputs from 2022-present."""
    candidates = sorted(root.glob("year=*/nccs_bmf_benchmark_year=*.parquet"))
    if not candidates:
        raise FileNotFoundError(f"No filtered NCCS BMF outputs found under {root}")

    inputs: list[SourceInput] = []
    for path in candidates:
        snapshot_year = path.parent.name.split("=", 1)[1]
        year_int = int(snapshot_year)
        if year_int < 2022:
            continue
        snapshot_month = ""
        if year_int > 2022:
            parts = path.stem.split("year=")
            # The parquet filename only carries the year, so we treat the precise
            # month as part of the row content instead of the file name.
            snapshot_month = ""
        inputs.append(
            SourceInput(
                source_family="nccs_bmf",
                source_variant=("legacy_bmf_" if year_int == 2022 else "raw_bmf_") + snapshot_year,
                input_path=path,
                input_format="parquet",
                snapshot_year=snapshot_year,
                snapshot_month=snapshot_month,
                native_prefix=SOURCE_PREFIXES["nccs_bmf"],
            )
        )
    if not inputs:
        raise FileNotFoundError(f"No NCCS BMF yearly outputs >= 2022 found under {root}")
    return inputs


def discover_bmf_exact_year_lookup_inputs(root: Path = BMF_STAGING_ROOT) -> dict[str, Path]:
    """
    Discover the upstream exact-year BMF lookup artifacts used for overlay.

    These are not source-preserving union inputs. They are supporting upstream
    enrichment artifacts that let the combined build overlay exact-year
    identity/classification fields without resorting to master-only rescue logic.
    """
    candidates = sorted(root.glob("year=*/nccs_bmf_exact_year_lookup_year=*.parquet"))
    if not candidates:
        raise FileNotFoundError(f"No exact-year NCCS BMF lookup outputs found under {root}")
    by_year: dict[str, Path] = {}
    for path in candidates:
        snapshot_year = path.parent.name.split("=", 1)[1]
        if int(snapshot_year) < 2022:
            continue
        by_year[snapshot_year] = path
    if not by_year:
        raise FileNotFoundError(f"No exact-year NCCS BMF lookup outputs >= 2022 found under {root}")
    return dict(sorted(by_year.items(), key=lambda item: int(item[0])))


def discover_source_inputs() -> list[SourceInput]:
    """Discover the filtered source files that feed the combined union table."""
    inputs: list[SourceInput] = []
    if not GT_FILTERED_PARQUET.exists():
        raise FileNotFoundError(f"GivingTuesday filtered parquet not found: {GT_FILTERED_PARQUET}")
    inputs.append(
        SourceInput(
            source_family="givingtuesday_datamart",
            source_variant="benchmark_filings",
            input_path=GT_FILTERED_PARQUET,
            input_format="parquet",
            snapshot_year="",
            snapshot_month="",
            native_prefix=SOURCE_PREFIXES["givingtuesday_datamart"],
        )
    )
    inputs.append(_find_latest_postcard_input())
    inputs.extend(_find_efile_inputs())
    inputs.extend(_find_bmf_inputs())
    return inputs


def _file_signature(path: Path) -> dict[str, Any]:
    """Return a lightweight file fingerprint for unchanged-input skipping."""
    hasher = hashlib.sha1()
    with open(path, "rb") as handle:
        hasher.update(handle.read(64 * 1024))
    stat = path.stat()
    return {
        "path": str(path),
        "size_bytes": stat.st_size,
        "mtime_ns": stat.st_mtime_ns,
        "head_sha1": hasher.hexdigest(),
    }


def build_input_signature(
    inputs: list[SourceInput],
    *,
    auxiliary_paths: dict[str, Path] | None = None,
) -> list[dict[str, Any]]:
    """Build a stable input signature list for the currently discovered inputs."""
    signature = [
        {
            "source_family": source.source_family,
            "source_variant": source.source_variant,
            "input_path": str(source.input_path),
            "signature": _file_signature(source.input_path),
        }
        for source in inputs
    ]
    if auxiliary_paths:
        for label, path in sorted(auxiliary_paths.items()):
            signature.append(
                {
                    "source_family": "auxiliary",
                    "source_variant": label,
                    "input_path": str(path),
                    "signature": _file_signature(path),
                }
            )
    return signature


def inputs_match_cached_build(
    build_summary_path: Path,
    input_signature: list[dict[str, Any]],
    output_parquet: Path = OUTPUT_PARQUET,
    master_output_parquet: Path = MASTER_OUTPUT_PARQUET,
) -> bool:
    """True when the cached build summary already matches the current discovered inputs."""
    if not build_summary_path.exists() or not output_parquet.exists() or not master_output_parquet.exists():
        return False
    cached = read_json(build_summary_path)
    return (
        cached.get("combined_schema_version") == COMBINED_SCHEMA_VERSION
        and cached.get("input_signature") == input_signature
    )


def source_family_time_basis(source_family: Any) -> str:
    """Return the canonical time-basis label for one source family."""
    return SOURCE_TIME_BASIS_BY_FAMILY.get(blank_to_empty(source_family), "")


def write_parquet_with_metadata(
    frame: pd.DataFrame,
    output_path: Path,
    *,
    schema_version: str = COMBINED_SCHEMA_VERSION,
    compression: str = "zstd",
) -> None:
    """Write the combined parquet with explicit schema-version metadata."""
    output_path.parent.mkdir(parents=True, exist_ok=True)
    table = pa.Table.from_pandas(frame, preserve_index=False)
    metadata = dict(table.schema.metadata or {})
    metadata[b"combined_schema_version"] = schema_version.encode("utf-8")
    metadata[b"created_by_pipeline"] = b"combined_990"
    table = table.cast(table.schema.with_metadata(metadata))
    pq.write_table(table, output_path, compression=compression)


def blank_to_empty(value: Any) -> str:
    """Normalize null-like values to empty strings for string-preserving output columns."""
    if value is None:
        return ""
    if isinstance(value, float) and pd.isna(value):
        return ""
    text = str(value)
    return "" if text.lower() == "nan" else text


def normalize_ein9(value: Any) -> str:
    """Normalize an EIN-like value into a 9-digit string where possible."""
    digits = "".join(ch for ch in blank_to_empty(value) if ch.isdigit())
    if not digits:
        return ""
    if len(digits) < 9:
        return digits.zfill(9)
    return digits[-9:]


def normalize_zip5(value: Any) -> str:
    """Normalize a ZIP-like value to a 5-digit ZIP code where possible."""
    digits = "".join(ch for ch in blank_to_empty(value) if ch.isdigit())
    return digits[:5] if len(digits) >= 5 else ""


def normalize_tax_year(value: Any) -> str:
    """Normalize year-like values to whole-number year strings."""
    raw = blank_to_empty(value).strip()
    if not raw:
        return ""
    try:
        return str(int(float(raw)))
    except ValueError:
        return raw


def parse_numeric_string(value: Any) -> tuple[float | None, bool]:
    """
    Parse a source-faithful numeric string into a nullable float helper value.

    Returns `(parsed_value, parse_ok)` where:
    - blank values become `(None, True)`
    - `NA` sentinel values remain source-faithful in the string column but are
      treated as missing in the numeric helper, so they become `(None, True)`
    - valid numeric strings become `(float_value, True)`
    - invalid nonblank strings become `(None, False)`
    """
    raw = blank_to_empty(value).strip()
    if not raw:
        return None, True
    if raw.upper() == "NA":
        return None, True
    cleaned = raw.replace(",", "").replace("$", "")
    if cleaned.startswith("(") and cleaned.endswith(")"):
        cleaned = "-" + cleaned[1:-1]
    try:
        return float(cleaned), True
    except ValueError:
        return None, False


def column_group(column_name: str) -> str:
    """Classify one output column for metadata reporting."""
    if column_name in ROW_PROVENANCE_COLUMNS:
        return "row_provenance"
    if column_name in HARMONIZED_COLUMNS:
        return "harmonized"
    if column_name in NUMERIC_HELPER_COLUMNS:
        return "numeric_helper"
    if (
        column_name.endswith("__selected_row_source_family")
        or column_name.endswith("__selected_row_source_variant")
        or column_name.endswith("__selected_row_time_basis")
        or column_name.endswith("__selected_row_tax_year")
        or column_name.endswith("__source_tax_year")
        or column_name.endswith("__source_year_offset")
    ):
        return "field_provenance"
    if column_name.endswith("__source_time_basis"):
        return "field_provenance"
    if column_name.endswith("__is_reference_snapshot_value") or column_name.endswith("__is_filing_value"):
        return "field_semantic_diagnostic"
    if "__source_" in column_name:
        return "field_provenance"
    if column_name in ("org_match_quality", "bmf_overlay_applied", "bmf_overlay_field_count", "bmf_overlay_fields"):
        return "overlay_diagnostic"
    if column_name.startswith("master_"):
        return "master_group_diagnostic"
    if column_name.endswith("__conflict") or column_name.endswith("__distinct_nonblank_value_count"):
        return "master_conflict_diagnostic"
    for prefix in SOURCE_PREFIXES.values():
        if column_name.startswith(prefix):
            return "source_native"
    return "derived"
