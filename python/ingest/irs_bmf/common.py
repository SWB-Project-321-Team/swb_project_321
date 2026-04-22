"""
Shared helpers for the IRS EO BMF benchmark and analysis pipeline.

This pipeline follows the same step-wise pattern as the mature NCCS ingest
families while keeping the IRS EO BMF semantics honest:
- the raw source is one state CSV per file
- benchmark admission happens before any combine stage
- tax year is inferred from TAX_PERIOD for the analysis-ready layer
- classification remains source-direct to IRS EO BMF
"""

from __future__ import annotations

import importlib.util
import sys
from pathlib import Path
from typing import Any

import pandas as pd

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

from utils.paths import DATA, get_base  # noqa: E402

DEFAULT_STATE_CODES = ["sd", "mn", "mt", "az"]
IRS_BMF_BASE_URL = "https://www.irs.gov/pub/irs-soi"

RAW_DIR = DATA / "raw" / "irs_bmf"
META_DIR = RAW_DIR / "metadata"
STAGING_DIR = DATA / "staging" / "irs_bmf"
LEGACY_STAGING_ORG_DIR = DATA / "staging" / "org"
DOCS_ANALYSIS_DIR = get_base() / "docs" / "analysis"
DOCS_DATA_PROCESSING_DIR = get_base() / "docs" / "data_processing"

RAW_MANIFEST_PATH = META_DIR / "irs_bmf_raw_manifest.csv"
RAW_SIZE_VERIFICATION_PATH = META_DIR / "irs_bmf_raw_size_verification.csv"
FILTER_MANIFEST_PATH = META_DIR / "irs_bmf_filter_manifest.csv"
FILTERED_SIZE_VERIFICATION_PATH = META_DIR / "irs_bmf_filtered_size_verification.csv"

DEFAULT_S3_BUCKET = _CORE_COMMON.DEFAULT_S3_BUCKET
DEFAULT_S3_REGION = _CORE_COMMON.DEFAULT_S3_REGION
RAW_PREFIX = "bronze/irs990/bmf"
RAW_META_PREFIX = f"{RAW_PREFIX}/metadata"
SILVER_PREFIX = "silver/irs990/bmf"
SILVER_META_PREFIX = f"{SILVER_PREFIX}/metadata"
ANALYSIS_PREFIX = f"{SILVER_PREFIX}/analysis"
ANALYSIS_META_PREFIX = f"{ANALYSIS_PREFIX}/metadata"

ANALYSIS_TAX_YEAR_MIN = 2022
ANALYSIS_TAX_YEAR_MAX = 2024

GEOID_REFERENCE_CSV = _CORE_COMMON.GEOID_REFERENCE_CSV
ZIP_TO_COUNTY_CSV = _CORE_COMMON.ZIP_TO_COUNTY_CSV

banner = _CORE_COMMON.banner
print_elapsed = _CORE_COMMON.print_elapsed
load_env_from_secrets = _CORE_COMMON.load_env_from_secrets
write_csv = _CORE_COMMON.write_csv
guess_content_type = _CORE_COMMON.guess_content_type
s3_object_size = _CORE_COMMON.s3_object_size
should_skip_upload = _CORE_COMMON.should_skip_upload
upload_file_with_progress = _CORE_COMMON.upload_file_with_progress


def ensure_work_dirs(
    raw_dir: Path = RAW_DIR,
    metadata_dir: Path = META_DIR,
    staging_dir: Path = STAGING_DIR,
) -> None:
    """Ensure the IRS EO BMF pipeline work directories exist."""
    for path in (raw_dir, metadata_dir, staging_dir, LEGACY_STAGING_ORG_DIR):
        path.mkdir(parents=True, exist_ok=True)
        print(f"[paths] Ready: {path}", flush=True)


def raw_state_path(state_code: str, raw_dir: Path = RAW_DIR) -> Path:
    """Return the local raw IRS EO BMF path for one state code."""
    return raw_dir / f"eo_{state_code.lower()}.csv"


def discover_raw_state_files(raw_dir: Path = RAW_DIR) -> list[Path]:
    """Return the raw IRS EO BMF state CSVs currently present on disk."""
    return sorted(path for path in raw_dir.glob("eo_*.csv") if path.is_file())


def state_code_from_path(path: Path) -> str:
    """Return the lowercase two-letter state code implied by an eo_<state>.csv path."""
    stem = path.stem.lower()
    return stem.split("_", 1)[1] if "_" in stem else stem[-2:]


def state_source_url(state_code: str) -> str:
    """Return the authoritative IRS EO BMF URL for one state file."""
    return f"{IRS_BMF_BASE_URL.rstrip('/')}/eo_{state_code.lower()}.csv"


def combined_filtered_output_path(staging_dir: Path = STAGING_DIR) -> Path:
    """Return the canonical combined filtered IRS EO BMF parquet path."""
    return staging_dir / "irs_bmf_combined_filtered.parquet"


def yearly_filtered_output_path(tax_year: int | str, staging_dir: Path = STAGING_DIR) -> Path:
    """Return the filtered yearly IRS EO BMF parquet path."""
    return staging_dir / f"year={tax_year}" / f"irs_bmf_benchmark_year={tax_year}.parquet"


def legacy_filtered_output_path() -> Path:
    """Return the compatibility filtered IRS EO BMF parquet path used by older tooling."""
    return LEGACY_STAGING_ORG_DIR / "irs_bmf_benchmark_counties.parquet"


def analysis_variables_output_path(staging_dir: Path = STAGING_DIR) -> Path:
    """Return the row-level IRS EO BMF analysis parquet path."""
    return staging_dir / "irs_bmf_analysis_variables.parquet"


def analysis_geography_metrics_output_path(staging_dir: Path = STAGING_DIR) -> Path:
    """Return the IRS EO BMF geography metrics parquet path."""
    return staging_dir / "irs_bmf_analysis_geography_metrics.parquet"


def analysis_field_metrics_output_path(staging_dir: Path = STAGING_DIR) -> Path:
    """Return the IRS EO BMF field metrics parquet path."""
    return staging_dir / "irs_bmf_analysis_field_metrics.parquet"


def analysis_variable_coverage_path(metadata_dir: Path = META_DIR) -> Path:
    """Return the IRS EO BMF analysis coverage CSV path."""
    return metadata_dir / "irs_bmf_analysis_variable_coverage.csv"


def analysis_variable_mapping_path() -> Path:
    """Return the IRS EO BMF analysis mapping Markdown path."""
    return DOCS_ANALYSIS_DIR / "irs_bmf_analysis_variable_mapping.md"


def analysis_data_processing_doc_path() -> Path:
    """Return the IRS EO BMF processing-documentation Markdown path."""
    return DOCS_DATA_PROCESSING_DIR / "irs_bmf_pipeline.md"


def raw_s3_key(state_code: str, raw_prefix: str = RAW_PREFIX) -> str:
    """Return the raw S3 object key for one state CSV."""
    return f"{raw_prefix.rstrip('/')}/eo_{state_code.lower()}.csv"


def raw_manifest_s3_key(raw_meta_prefix: str = RAW_META_PREFIX) -> str:
    """Return the raw-manifest S3 key."""
    return f"{raw_meta_prefix.rstrip('/')}/{RAW_MANIFEST_PATH.name}"


def raw_verification_s3_key(raw_meta_prefix: str = RAW_META_PREFIX) -> str:
    """Return the raw-size-verification S3 key."""
    return f"{raw_meta_prefix.rstrip('/')}/{RAW_SIZE_VERIFICATION_PATH.name}"


def combined_filtered_s3_key(silver_prefix: str = SILVER_PREFIX) -> str:
    """Return the combined filtered IRS EO BMF S3 key."""
    return f"{silver_prefix.rstrip('/')}/{combined_filtered_output_path().name}"


def yearly_filtered_s3_key(tax_year: int | str, silver_prefix: str = SILVER_PREFIX) -> str:
    """Return the filtered yearly IRS EO BMF S3 key."""
    filename = yearly_filtered_output_path(tax_year).name
    return f"{silver_prefix.rstrip('/')}/year={tax_year}/{filename}"


def legacy_filtered_s3_key(silver_prefix: str = SILVER_PREFIX) -> str:
    """Return the compatibility filtered IRS EO BMF S3 key."""
    return f"{silver_prefix.rstrip('/')}/bmf_benchmark_counties.parquet"


def filter_manifest_s3_key(silver_meta_prefix: str = SILVER_META_PREFIX) -> str:
    """Return the benchmark-filter manifest S3 key."""
    return f"{silver_meta_prefix.rstrip('/')}/{FILTER_MANIFEST_PATH.name}"


def filtered_verification_s3_key(silver_meta_prefix: str = SILVER_META_PREFIX) -> str:
    """Return the filtered-size-verification S3 key."""
    return f"{silver_meta_prefix.rstrip('/')}/{FILTERED_SIZE_VERIFICATION_PATH.name}"


def _normalize_zip(zip_val: object) -> str:
    """Return a 5-digit ZIP string; invalid or blank values become empty."""
    if zip_val is None or (isinstance(zip_val, float) and pd.isna(zip_val)):
        return ""
    text = str(zip_val).strip().replace(" ", "").replace("-", "")
    digits = "".join(ch for ch in text if ch.isdigit())
    return digits[:5] if len(digits) >= 5 else ""


def _normalize_state(state_val: object) -> str:
    """Normalize two-letter state text without inventing a value when blank."""
    if state_val is None or (isinstance(state_val, float) and pd.isna(state_val)):
        return ""
    return str(state_val).strip().upper()


def _derive_tax_year(tax_period_val: object) -> str:
    """
    Derive a four-digit tax year from TAX_PERIOD.

    IRS EO BMF TAX_PERIOD is often a YYYYMM integer-like text value. The final
    analysis package is intentionally limited to 2022-2024, so this helper only
    validates the leading four digits; range checks happen separately.
    """
    if tax_period_val is None or (isinstance(tax_period_val, float) and pd.isna(tax_period_val)):
        return ""
    digits = "".join(ch for ch in str(tax_period_val).strip() if ch.isdigit())
    if len(digits) < 4:
        return ""
    year = digits[:4]
    return year if year.isdigit() else ""


def load_geoid_reference(csv_path: Path = GEOID_REFERENCE_CSV) -> pd.DataFrame:
    """Load the benchmark GEOID reference with normalized county GEOIDs."""
    ref = pd.read_csv(csv_path)
    ref["GEOID"] = ref["GEOID"].astype(str).str.strip().str.zfill(5)
    return ref


def load_zip_to_geoid_pairs(csv_path: Path = ZIP_TO_COUNTY_CSV) -> pd.DataFrame:
    """Load the ZIP-to-county crosswalk as normalized ZIP/FIPS pairs."""
    df = pd.read_csv(csv_path)
    zip_col = next((c for c in df.columns if "zip" in c.lower()), df.columns[0])
    fips_candidates = [
        c
        for c in df.columns
        if "fips" in c.lower() or ("county" in c.lower() and "name" not in c.lower())
    ]
    fips_col = fips_candidates[0] if fips_candidates else df.columns[1]
    df["_zip"] = df[zip_col].astype(str).str.replace(r"\D", "", regex=True).str[:5]
    df["_geoid"] = df[fips_col].astype(str).str.strip().str.zfill(5)
    df = df[["_zip", "_geoid"]].drop_duplicates(subset=["_zip", "_geoid"], keep="first").dropna(subset=["_geoid"])
    valid = (df["_zip"].str.len() == 5) & (df["_geoid"].str.len() == 5)
    return df.loc[valid, ["_zip", "_geoid"]].copy()


def _find_region_column(ref: pd.DataFrame) -> str | None:
    """Return the reference column that holds benchmark region names, when present."""
    for col in ref.columns:
        if str(col).lower() in {"region", "cluster_name", "cluster", "benchmark_region", "area"}:
            return col
    if "Region" in ref.columns:
        return "Region"
    return None


def build_benchmark_zip_map(ref: pd.DataFrame, zip_pairs: pd.DataFrame) -> pd.DataFrame:
    """
    Build the explicit benchmark ZIP map and reject ambiguous ZIP assignments.

    The new IRS EO BMF pipeline preserves the same benchmark geography logic as
    the older helper script so we do not silently change admission rules.
    """
    state_col = next((c for c in ref.columns if str(c).lower() == "state"), None)
    keep_cols = ["GEOID"]
    if state_col is not None:
        keep_cols.append(state_col)
    benchmark = zip_pairs.merge(ref[keep_cols], left_on="_geoid", right_on="GEOID", how="inner").copy()
    benchmark = benchmark[["_zip", "_geoid"] + ([state_col] if state_col is not None else [])].drop_duplicates()
    ambiguous = benchmark.groupby("_zip")["_geoid"].nunique()
    ambiguous = ambiguous[ambiguous > 1]
    if not ambiguous.empty:
        examples = ", ".join(sorted(ambiguous.index.tolist())[:10])
        raise RuntimeError(
            "Benchmark ZIP map is ambiguous: at least one ZIP maps to multiple benchmark counties. "
            f"Examples: {examples}"
        )
    if state_col is not None:
        benchmark["_benchmark_state"] = benchmark[state_col].astype(str).str.strip().str.upper()
        benchmark = benchmark.drop(columns=[state_col])
    return benchmark.drop_duplicates(subset=["_zip"], keep="first").copy()


def filter_bmf_by_geoid(
    bmf: pd.DataFrame,
    geoid_set: set[str],
    benchmark_zip_map: dict[str, str],
    geoid_to_region: dict[str, str] | None,
    benchmark_zip_state_map: dict[str, str] | None = None,
) -> pd.DataFrame:
    """
    Filter a raw IRS EO BMF frame to benchmark counties.

    This compatibility helper intentionally mirrors the older 00b script so
    existing tests and downstream expectations stay valid while the canonical
    pipeline moves to the new irs_bmf package.
    """
    zip_col = "ZIP" if "ZIP" in bmf.columns else bmf.columns[0]
    out = bmf.copy()
    out["_zip_norm"] = out[zip_col].astype(str).apply(_normalize_zip)
    out["_geoid"] = out["_zip_norm"].map(benchmark_zip_map)
    out = out[out["_geoid"].isin(geoid_set)].copy()
    if benchmark_zip_state_map and "STATE" in out.columns:
        out["_benchmark_state"] = out["_zip_norm"].map(benchmark_zip_state_map).fillna("")
        out["_state_norm"] = out["STATE"].astype(str).str.strip().str.upper()
        out = out[
            (out["_state_norm"] == "")
            | (out["_benchmark_state"] == "")
            | (out["_state_norm"] == out["_benchmark_state"])
        ].copy()
    if geoid_to_region:
        out["Region"] = out["_geoid"].map(geoid_to_region)
    drop_cols = ["_zip_norm", "_geoid"]
    for extra_col in ("_benchmark_state", "_state_norm"):
        if extra_col in out.columns:
            drop_cols.append(extra_col)
    return out.drop(columns=drop_cols)


def benchmark_reference_maps(
    geoid_reference_path: Path = GEOID_REFERENCE_CSV,
    zip_to_county_path: Path = ZIP_TO_COUNTY_CSV,
) -> dict[str, Any]:
    """Load and package the benchmark county admission maps used in filtering."""
    ref = load_geoid_reference(geoid_reference_path)
    zip_pairs = load_zip_to_geoid_pairs(zip_to_county_path)
    benchmark_zip_df = build_benchmark_zip_map(ref, zip_pairs)
    region_col = _find_region_column(ref)
    geoid_to_region = (
        dict(zip(ref["GEOID"].astype(str), ref[region_col].astype(str).str.strip()))
        if region_col
        else None
    )
    return {
        "geoid_set": set(ref["GEOID"].astype(str)),
        "benchmark_zip_map": dict(zip(benchmark_zip_df["_zip"], benchmark_zip_df["_geoid"])),
        "benchmark_zip_state_map": (
            dict(zip(benchmark_zip_df["_zip"], benchmark_zip_df["_benchmark_state"]))
            if "_benchmark_state" in benchmark_zip_df.columns
            else None
        ),
        "geoid_to_region": geoid_to_region,
    }


def normalized_direct_field_count(frame: pd.DataFrame) -> pd.Series:
    """
    Count populated direct fields used to rank cross-state EIN-year duplicates.

    This is intentionally narrow and source-faithful: it rewards rows that carry
    more direct IRS EO BMF value rather than heuristics from later analysis.
    """
    cols = [
        "org_name",
        "state",
        "zip5",
        "raw_ntee_code",
        "raw_subsection_code",
        "raw_total_revenue_amount",
        "raw_total_assets_amount",
        "raw_total_income_amount",
    ]
    score = pd.Series(0, index=frame.index, dtype="Int64")
    for col in cols:
        if col not in frame.columns:
            continue
        if pd.api.types.is_string_dtype(frame[col].dtype) or frame[col].dtype == object:
            populated = frame[col].fillna("").astype(str).str.strip().ne("")
        else:
            populated = frame[col].notna()
        score = score.add(populated.astype("Int64"), fill_value=0)
    return score


def normalize_state_series(series: pd.Series) -> pd.Series:
    """Normalize a state column to uppercase text with blank-as-missing semantics."""
    text = series.astype("string").fillna("").str.strip().str.upper()
    return text.mask(text.eq(""), pd.NA)


def normalize_zip_series(series: pd.Series) -> pd.Series:
    """Normalize a ZIP column to nullable 5-digit strings."""
    normalized = series.astype("string").fillna("").map(_normalize_zip)
    return normalized.mask(normalized.eq(""), pd.NA).astype("string")


def derive_tax_year_series(series: pd.Series) -> pd.Series:
    """Derive nullable string tax years from a TAX_PERIOD-like series."""
    derived = series.astype("string").fillna("").map(_derive_tax_year)
    return derived.mask(derived.eq(""), pd.NA).astype("string")
