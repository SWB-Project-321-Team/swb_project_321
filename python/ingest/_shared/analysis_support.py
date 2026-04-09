"""
Shared helpers for source-specific nonprofit analysis extracts.

These helpers centralize the cross-source behavior that now appears in several
analysis layers:
- parsing string amounts into nullable numeric values
- loading NCCS BMF exact-year lookup artifacts
- loading IRS EO BMF raw fallback files
- applying the documented classification fallback order
- deriving NTEE-based proxy flags
- deriving conservative name-based imputation candidates

The goal is not to create one universal harmonized dataset. Instead, this
module keeps the repeated enrichment and proxy logic explicit, auditable, and
consistent across the new source-specific analysis steps.
"""

from __future__ import annotations

import re
import sys
from pathlib import Path

import pandas as pd
from tqdm import tqdm

_THIS_FILE = Path(__file__).resolve()
_PYTHON_DIR = _THIS_FILE.parents[2]
if str(_PYTHON_DIR) not in sys.path:
    sys.path.insert(0, str(_PYTHON_DIR))

from utils.paths import DATA  # noqa: E402

DEFAULT_BMF_STAGING_DIR = DATA / "staging" / "nccs_bmf"
DEFAULT_IRS_BMF_RAW_DIR = DATA / "raw" / "irs_bmf"

TRUE_FLAG_VALUES = {"1", "Y", "YES", "TRUE", "T", "X"}
FALSE_FLAG_VALUES = {"0", "N", "NO", "FALSE", "F"}
POLITICAL_ORG_SUBSECTION_CODES = {"4", "527", "501C4", "501C527"}

NAME_PROXY_FLAG_SPECS = [
    {
        "canonical_output_column": "analysis_is_hospital",
        "imputed_output_column": "analysis_imputed_is_hospital",
        "high_confidence_patterns": (
            r"\bhospital\b",
            r"\bmedical center\b",
            r"\bhealth system\b",
            r"\bgeneral hospital\b",
            r"\bspecialty hospital\b",
        ),
        "medium_confidence_patterns": (
            r"\bambulance district\b",
            r"\bambulance service\b",
            r"\bcommunity health center\b",
        ),
        "block_patterns": (
            r"\bmedical records\b",
            r"\bhealthcare\b",
            r"\bhealth care\b",
            r"\bhealth professi\b",
            r"\bclinic foundation\b",
        ),
    },
    {
        "canonical_output_column": "analysis_is_university",
        "imputed_output_column": "analysis_imputed_is_university",
        "high_confidence_patterns": (
            r"\buniversity\b",
            r"\bcollege\b",
            r"\bcollegiate\b",
            r"\bgraduate school\b",
            r"\bprofessional school\b",
            r"\bbiology dept\b",
            r"\bfraternity\b",
            r"\bsorority\b",
            r"\bDWU\b",
            r"\bNAU\b",
            r"\bSDSU\b",
            r"\bUSD\b",
            r"\bMSU\b",
        ),
        "medium_confidence_patterns": (
            r"\bcampus\b",
            r"\bstudent\b",
            r"\balumni\b",
            r"\bdean\b",
            r"\bdepartment\b",
        ),
        "block_patterns": (
            r"\bfire department\b",
            r"\bpolice department\b",
        ),
    },
    {
        "canonical_output_column": "analysis_is_political_org",
        "imputed_output_column": "analysis_imputed_is_political_org",
        "high_confidence_patterns": (
            r"\bvoters?\b",
            r"\bpolitical action committee\b",
            r"\bPAC\b",
            r"\belection\b",
            r"\bcampaign\b",
            r"\bballot\b",
            r"\bparty\b",
        ),
        "medium_confidence_patterns": (
            r"\baction\b",
            r"\bamerica\b",
            r"\bvoices\b",
            r"\bworking\b",
        ),
        "block_patterns": (
            r"\banimal\b",
            r"\barts\b",
            r"\bhistory\b",
            r"\bmuseum\b",
        ),
    },
]


def normalize_ein(value: object) -> str:
    """Normalize EIN-like values to a 9-digit string when possible."""
    text = "" if value is None else str(value).strip()
    digits = re.sub(r"\D", "", text)
    if not digits:
        return ""
    if len(digits) < 9:
        digits = digits.zfill(9)
    return digits[:9]


def empty_string_series(index: pd.Index) -> pd.Series:
    """Return a pandas string series of missing values."""
    return pd.Series(pd.NA, index=index, dtype="string")


def blank_to_na(series: pd.Series) -> pd.Series:
    """Treat blank-like text as missing while preserving string dtype."""
    text = series.astype("string")
    return text.mask(text.fillna("").str.strip().eq(""), pd.NA)


def numeric_from_text(series: pd.Series) -> pd.Series:
    """Parse string amounts into nullable numeric values."""
    cleaned = (
        series.astype("string")
        .fillna("")
        .str.strip()
        .str.replace(",", "", regex=False)
        .str.replace("$", "", regex=False)
        .str.replace(r"^\((.*)\)$", r"-\1", regex=True)
    )
    cleaned = cleaned.mask(cleaned.eq(""), pd.NA)
    return pd.to_numeric(cleaned, errors="coerce").astype("Float64")


def series_has_value(series: pd.Series) -> pd.Series:
    """Return a populated-value mask with blank-string handling."""
    if pd.api.types.is_string_dtype(series.dtype) or series.dtype == object:
        return series.fillna("").astype(str).str.strip().ne("")
    return series.notna()


def normalize_bool_text(series: pd.Series) -> pd.Series:
    """Normalize bool-like text into a nullable boolean series."""
    text = series.astype("string").fillna("").str.strip().str.upper()
    output = pd.Series(pd.NA, index=series.index, dtype="boolean")
    output.loc[text.isin(FALSE_FLAG_VALUES)] = False
    output.loc[text.isin(TRUE_FLAG_VALUES)] = True
    return output


def normalize_subsection_code(series: pd.Series) -> pd.Series:
    """Normalize subsection-like values for cross-source matching."""
    return (
        series.astype("string")
        .fillna("")
        .str.strip()
        .str.upper()
        .str.replace(r"[^A-Z0-9]", "", regex=True)
        .mask(lambda values: values.eq(""), pd.NA)
    )


def build_ntee_proxy_flag(
    ntee_series: pd.Series,
    positive_prefixes: tuple[str, ...],
    *,
    provenance_label: str = "analysis_ntee_code",
) -> tuple[pd.Series, pd.Series]:
    """Build one nullable boolean proxy driven by resolved NTEE codes."""
    normalized = blank_to_na(ntee_series).astype("string").str.upper()
    output = pd.Series(pd.NA, index=ntee_series.index, dtype="boolean")
    has_ntee = normalized.notna()
    output.loc[has_ntee] = False
    positive_mask = pd.Series(False, index=ntee_series.index)
    for prefix in positive_prefixes:
        positive_mask = positive_mask | normalized.fillna("").str.startswith(prefix)
    output.loc[positive_mask] = True
    provenance = pd.Series(pd.NA, index=ntee_series.index, dtype="string")
    provenance.loc[has_ntee] = provenance_label
    return output, provenance


def build_political_proxy_flag(
    subsection_series: pd.Series,
    *,
    provenance_label: str = "analysis_subsection_code",
) -> tuple[pd.Series, pd.Series]:
    """Build one nullable political-organization flag from subsection codes."""
    normalized = normalize_subsection_code(subsection_series)
    output = pd.Series(pd.NA, index=subsection_series.index, dtype="boolean")
    output.loc[normalized.notna()] = False
    output.loc[normalized.isin(POLITICAL_ORG_SUBSECTION_CODES)] = True
    provenance = pd.Series(pd.NA, index=subsection_series.index, dtype="string")
    provenance.loc[normalized.notna()] = provenance_label
    return output, provenance


def build_name_proxy_flag(
    name_series: pd.Series,
    canonical_series: pd.Series,
    high_confidence_patterns: tuple[str, ...],
    medium_confidence_patterns: tuple[str, ...],
    block_patterns: tuple[str, ...],
) -> tuple[pd.Series, pd.Series]:
    """
    Build a conservative positive-only name proxy with confidence labels.

    This helper intentionally does not infer False from a non-match. It only
    records confidence for rows still missing the canonical source-backed flag.
    """
    text = name_series.astype("string").fillna("")
    missing_canonical = canonical_series.isna()
    blocked = pd.Series(False, index=name_series.index)
    for pattern in block_patterns:
        blocked = blocked | text.str.contains(pattern, case=False, regex=True)

    high_match = pd.Series(False, index=name_series.index)
    for pattern in high_confidence_patterns:
        high_match = high_match | text.str.contains(pattern, case=False, regex=True)

    medium_match = pd.Series(False, index=name_series.index)
    for pattern in medium_confidence_patterns:
        medium_match = medium_match | text.str.contains(pattern, case=False, regex=True)

    high_match = high_match & ~blocked
    medium_match = medium_match & ~blocked & ~high_match

    proxy_values = pd.Series(pd.NA, index=name_series.index, dtype="boolean")
    proxy_values.loc[missing_canonical & (high_match | medium_match)] = True

    proxy_confidence = pd.Series(pd.NA, index=name_series.index, dtype="string")
    proxy_confidence.loc[missing_canonical & high_match] = "high"
    proxy_confidence.loc[missing_canonical & medium_match] = "medium"
    return proxy_values, proxy_confidence


def build_ratio_metric(
    numerator: pd.Series,
    denominator: pd.Series,
    *,
    provenance_label: str,
    require_positive_denominator: bool = False,
) -> tuple[pd.Series, pd.Series]:
    """Build a nullable ratio plus its row-level provenance label."""
    ratio_values = pd.Series(pd.NA, index=numerator.index, dtype="Float64")
    valid_mask = numerator.notna() & denominator.notna() & denominator.ne(0)
    if require_positive_denominator:
        valid_mask = valid_mask & denominator.gt(0)
    ratio_values.loc[valid_mask] = (numerator.loc[valid_mask] / denominator.loc[valid_mask]).astype("Float64")
    provenance = pd.Series(pd.NA, index=numerator.index, dtype="string")
    provenance.loc[valid_mask] = provenance_label
    return ratio_values, provenance


def discover_bmf_exact_year_lookup_inputs(root: Path = DEFAULT_BMF_STAGING_DIR) -> dict[str, Path]:
    """Discover staged NCCS BMF exact-year lookup parquets keyed by tax year."""
    candidates = sorted(root.glob("year=*/nccs_bmf_exact_year_lookup_year=*.parquet"))
    if not candidates:
        raise FileNotFoundError(f"No exact-year NCCS BMF lookup outputs found under {root}")

    by_year: dict[str, Path] = {}
    for path in candidates:
        snapshot_year = path.parent.name.split("=", 1)[1]
        by_year[snapshot_year] = path
    return dict(sorted(by_year.items(), key=lambda item: int(item[0])))


def discover_irs_bmf_raw_inputs(root: Path = DEFAULT_IRS_BMF_RAW_DIR) -> list[Path]:
    """Discover state-level raw IRS EO BMF CSV files."""
    candidates = sorted(root.glob("eo_*.csv"))
    if not candidates:
        raise FileNotFoundError(f"No IRS EO BMF raw CSV files found under {root}")
    return candidates


def load_bmf_classification_lookup(
    required_tax_years: list[str],
    *,
    bmf_staging_dir: Path = DEFAULT_BMF_STAGING_DIR,
) -> pd.DataFrame:
    """
    Load NCCS BMF classification lookup rows for exact and nearest-year fallback.

    We always load every discovered lookup year so the nearest-year fallback can
    use the full staged BMF range, but we still fail fast when a required exact
    year is missing entirely.
    """
    lookup_paths_by_year = discover_bmf_exact_year_lookup_inputs(bmf_staging_dir)
    print("[analysis] Discovered NCCS BMF exact-year lookup files:", flush=True)
    for tax_year, path in lookup_paths_by_year.items():
        print(f"[analysis]   tax_year={tax_year} lookup={path}", flush=True)

    missing_years = [tax_year for tax_year in required_tax_years if tax_year not in lookup_paths_by_year]
    if missing_years:
        raise FileNotFoundError(
            "Missing NCCS BMF exact-year lookup files for required tax years: "
            + ", ".join(missing_years)
        )

    frames: list[pd.DataFrame] = []
    available_tax_years = sorted(lookup_paths_by_year.keys(), key=int)
    for tax_year in tqdm(available_tax_years, desc="load BMF classification lookups", unit="year"):
        lookup_path = lookup_paths_by_year[tax_year]
        frame = pd.read_parquet(
            lookup_path,
            columns=[
                "harm_ein",
                "harm_tax_year",
                "harm_ntee_code",
                "harm_ntee_code__source_family",
                "harm_ntee_code__source_variant",
                "harm_ntee_code__source_column",
                "harm_subsection_code",
                "harm_subsection_code__source_family",
                "harm_subsection_code__source_variant",
                "harm_subsection_code__source_column",
            ],
        )
        frame = frame.rename(
            columns={
                "harm_ein": "ein",
                "harm_tax_year": "source_tax_year",
                "harm_ntee_code": "analysis_ntee_code",
                "harm_ntee_code__source_family": "analysis_ntee_code_source_family",
                "harm_ntee_code__source_variant": "analysis_ntee_code_source_variant",
                "harm_ntee_code__source_column": "analysis_ntee_code_source_column",
                "harm_subsection_code": "analysis_subsection_code",
                "harm_subsection_code__source_family": "analysis_subsection_code_source_family",
                "harm_subsection_code__source_variant": "analysis_subsection_code_source_variant",
                "harm_subsection_code__source_column": "analysis_subsection_code_source_column",
            }
        )
        frame["ein"] = frame["ein"].map(normalize_ein).astype("string")
        frame["source_tax_year"] = frame["source_tax_year"].astype("string")
        frame["analysis_ntee_code"] = blank_to_na(frame["analysis_ntee_code"])
        frame["analysis_ntee_code_source_family"] = blank_to_na(frame["analysis_ntee_code_source_family"])
        frame["analysis_ntee_code_source_variant"] = blank_to_na(frame["analysis_ntee_code_source_variant"])
        frame["analysis_ntee_code_source_column"] = blank_to_na(frame["analysis_ntee_code_source_column"])
        frame["analysis_subsection_code"] = normalize_subsection_code(frame["analysis_subsection_code"])
        frame["analysis_subsection_code_source_family"] = blank_to_na(frame["analysis_subsection_code_source_family"])
        frame["analysis_subsection_code_source_variant"] = blank_to_na(frame["analysis_subsection_code_source_variant"])
        frame["analysis_subsection_code_source_column"] = blank_to_na(frame["analysis_subsection_code_source_column"])
        frames.append(frame)

    lookup_df = pd.concat(frames, ignore_index=True)
    lookup_df = lookup_df.drop_duplicates(subset=["ein", "source_tax_year"], keep="first")
    return lookup_df


def load_irs_bmf_classification_lookup(
    *,
    irs_bmf_raw_dir: Path = DEFAULT_IRS_BMF_RAW_DIR,
) -> pd.DataFrame:
    """Load IRS EO BMF classification rows keyed by EIN for the final fallback."""
    try:
        raw_paths = discover_irs_bmf_raw_inputs(irs_bmf_raw_dir)
    except FileNotFoundError as exc:
        print(f"[analysis] IRS EO BMF fallback unavailable: {exc}", flush=True)
        return pd.DataFrame(
            columns=[
                "ein",
                "analysis_ntee_code",
                "analysis_ntee_code_source_family",
                "analysis_ntee_code_source_variant",
                "analysis_ntee_code_source_column",
                "analysis_subsection_code",
                "analysis_subsection_code_source_family",
                "analysis_subsection_code_source_variant",
                "analysis_subsection_code_source_column",
                "irs_bmf_state",
            ]
        )

    print("[analysis] Discovered IRS EO BMF raw files:", flush=True)
    for path in raw_paths:
        print(f"[analysis]   raw_file={path}", flush=True)

    frames: list[pd.DataFrame] = []
    for raw_path in tqdm(raw_paths, desc="load IRS EO BMF fallback files", unit="file"):
        frame = pd.read_csv(
            raw_path,
            dtype=str,
            low_memory=False,
            usecols=lambda column_name: column_name in {"EIN", "STATE", "NTEE_CD", "SUBSECTION", "NAME"},
        )
        if "EIN" not in frame.columns or "NTEE_CD" not in frame.columns:
            print(f"[analysis] Skipping IRS EO BMF file without EIN/NTEE_CD columns: {raw_path}", flush=True)
            continue

        frame["ein"] = frame["EIN"].map(normalize_ein).astype("string")
        frame["analysis_ntee_code"] = blank_to_na(frame["NTEE_CD"])
        frame["analysis_ntee_code_source_family"] = pd.Series("irs_bmf", index=frame.index, dtype="string")
        frame["analysis_ntee_code_source_variant"] = pd.Series(f"{raw_path.name}|ein_fallback", index=frame.index, dtype="string")
        frame["analysis_ntee_code_source_column"] = pd.Series("NTEE_CD", index=frame.index, dtype="string")
        frame["analysis_subsection_code"] = normalize_subsection_code(
            frame["SUBSECTION"] if "SUBSECTION" in frame.columns else pd.Series(pd.NA, index=frame.index, dtype="string")
        )
        frame["analysis_subsection_code_source_family"] = pd.Series("irs_bmf", index=frame.index, dtype="string")
        frame["analysis_subsection_code_source_variant"] = pd.Series(f"{raw_path.name}|ein_fallback", index=frame.index, dtype="string")
        frame["analysis_subsection_code_source_column"] = pd.Series("SUBSECTION", index=frame.index, dtype="string")
        frame["irs_bmf_state"] = blank_to_na(
            frame["STATE"].astype("string") if "STATE" in frame.columns else pd.Series(pd.NA, index=frame.index, dtype="string")
        )
        frames.append(
            frame[
                [
                    "ein",
                    "analysis_ntee_code",
                    "analysis_ntee_code_source_family",
                    "analysis_ntee_code_source_variant",
                    "analysis_ntee_code_source_column",
                    "analysis_subsection_code",
                    "analysis_subsection_code_source_family",
                    "analysis_subsection_code_source_variant",
                    "analysis_subsection_code_source_column",
                    "irs_bmf_state",
                ]
            ]
        )

    if not frames:
        print("[analysis] No usable IRS EO BMF fallback rows were discovered.", flush=True)
        return pd.DataFrame(
            columns=[
                "ein",
                "analysis_ntee_code",
                "analysis_ntee_code_source_family",
                "analysis_ntee_code_source_variant",
                "analysis_ntee_code_source_column",
                "analysis_subsection_code",
                "analysis_subsection_code_source_family",
                "analysis_subsection_code_source_variant",
                "analysis_subsection_code_source_column",
                "irs_bmf_state",
            ]
        )

    lookup_df = pd.concat(frames, ignore_index=True)
    lookup_df = lookup_df.loc[lookup_df["ein"].astype("string").str.len().eq(9)].copy()
    lookup_df = lookup_df.loc[
        series_has_value(lookup_df["analysis_ntee_code"])
        | series_has_value(lookup_df["analysis_subsection_code"])
    ].copy()
    return lookup_df


def apply_classification_fallbacks(
    analysis_df: pd.DataFrame,
    source_df: pd.DataFrame,
    *,
    state_column: str | None,
    bmf_lookup_df: pd.DataFrame,
    irs_lookup_df: pd.DataFrame,
) -> dict[str, int]:
    """
    Populate analysis NTEE/subsection fields using the documented fallback order.

    The fallback order is:
    1. exact NCCS BMF EIN + tax_year
    2. nearest-year NCCS BMF EIN fallback
    3. IRS EO BMF EIN fallback, preferring a source-state match
    """
    if "analysis_ntee_code" not in analysis_df.columns:
        analysis_df["analysis_ntee_code"] = pd.Series(pd.NA, index=analysis_df.index, dtype="string")
        analysis_df["analysis_ntee_code_source_family"] = pd.Series(pd.NA, index=analysis_df.index, dtype="string")
        analysis_df["analysis_ntee_code_source_variant"] = pd.Series(pd.NA, index=analysis_df.index, dtype="string")
        analysis_df["analysis_ntee_code_source_column"] = pd.Series(pd.NA, index=analysis_df.index, dtype="string")
    if "analysis_subsection_code" not in analysis_df.columns:
        analysis_df["analysis_subsection_code"] = pd.Series(pd.NA, index=analysis_df.index, dtype="string")
        analysis_df["analysis_subsection_code_source_family"] = pd.Series(pd.NA, index=analysis_df.index, dtype="string")
        analysis_df["analysis_subsection_code_source_variant"] = pd.Series(pd.NA, index=analysis_df.index, dtype="string")
        analysis_df["analysis_subsection_code_source_column"] = pd.Series(pd.NA, index=analysis_df.index, dtype="string")

    print("[analysis] Joining exact-year NCCS BMF classification onto source rows...", flush=True)
    exact_lookup_df = bmf_lookup_df.rename(columns={"source_tax_year": "tax_year"}).drop_duplicates(subset=["ein", "tax_year"], keep="first")
    joined = (
        source_df[["ein", "tax_year"]].astype({"ein": "string", "tax_year": "string"})
        .merge(exact_lookup_df, on=["ein", "tax_year"], how="left", validate="many_to_one")
        .drop(columns=["ein", "tax_year"])
    )
    joined = joined.reset_index(drop=True)
    for column_name in joined.columns:
        analysis_df[column_name] = joined[column_name]
    analysis_df["analysis_ntee_code"] = blank_to_na(analysis_df["analysis_ntee_code"].astype("string"))
    analysis_df["analysis_subsection_code"] = normalize_subsection_code(analysis_df["analysis_subsection_code"].astype("string"))
    exact_ntee_count = int(series_has_value(analysis_df["analysis_ntee_code"]).sum())
    exact_subsection_count = int(series_has_value(analysis_df["analysis_subsection_code"]).sum())
    print(f"[analysis] Exact-year NCCS BMF NTEE rows populated: {exact_ntee_count:,}", flush=True)
    print(f"[analysis] Exact-year NCCS BMF subsection rows populated: {exact_subsection_count:,}", flush=True)

    def _apply_nearest(field_prefix: str) -> int:
        value_col = f"{field_prefix}"
        source_family_col = f"{field_prefix}_source_family"
        source_variant_col = f"{field_prefix}_source_variant"
        source_column_col = f"{field_prefix}_source_column"
        missing_mask = ~series_has_value(analysis_df[value_col])
        missing_count = int(missing_mask.sum())
        print(f"[analysis] Missing {value_col} rows before nearest-year NCCS fallback: {missing_count:,}", flush=True)
        if missing_count == 0:
            return 0
        candidate_lookup = bmf_lookup_df.loc[series_has_value(bmf_lookup_df[value_col])].copy()
        if candidate_lookup.empty:
            print(f"[analysis] No nonblank NCCS BMF candidate rows available for {value_col} nearest-year fallback.", flush=True)
            return 0
        source_missing = source_df.loc[missing_mask, ["ein", "tax_year"]].copy()
        source_missing["row_index"] = source_missing.index
        source_missing["target_tax_year_int"] = pd.to_numeric(source_missing["tax_year"], errors="coerce")
        candidate_lookup["source_tax_year_int"] = pd.to_numeric(candidate_lookup["source_tax_year"], errors="coerce")
        merged = source_missing.merge(candidate_lookup, on="ein", how="inner", validate="many_to_many")
        if merged.empty:
            print(f"[analysis] No same-EIN NCCS BMF rows found for {value_col} nearest-year fallback.", flush=True)
            return 0
        merged["year_distance"] = (merged["target_tax_year_int"] - merged["source_tax_year_int"]).abs()
        merged = merged.sort_values(by=["row_index", "year_distance", "source_tax_year_int"], ascending=[True, True, True], kind="mergesort")
        chosen = merged.drop_duplicates(subset=["row_index"], keep="first").copy()
        chosen[source_variant_col] = chosen.apply(
            lambda row: f"{row[source_variant_col]}|nearest_year_fallback_from_{row['tax_year']}_to_{row['source_tax_year']}",
            axis=1,
        )
        for row in tqdm(chosen.itertuples(index=False), total=len(chosen), desc=f"apply {value_col} nearest-year fallback", unit="row"):
            analysis_df.at[row.row_index, value_col] = getattr(row, value_col)
            analysis_df.at[row.row_index, source_family_col] = getattr(row, source_family_col)
            analysis_df.at[row.row_index, source_variant_col] = getattr(row, source_variant_col)
            analysis_df.at[row.row_index, source_column_col] = getattr(row, source_column_col)
        filled_count = int(len(chosen))
        print(f"[analysis] NCCS nearest-year fallback rows populated for {value_col}: {filled_count:,}", flush=True)
        return filled_count

    def _apply_irs(field_prefix: str) -> int:
        value_col = f"{field_prefix}"
        source_family_col = f"{field_prefix}_source_family"
        source_variant_col = f"{field_prefix}_source_variant"
        source_column_col = f"{field_prefix}_source_column"
        missing_mask = ~series_has_value(analysis_df[value_col])
        missing_count = int(missing_mask.sum())
        print(f"[analysis] Missing {value_col} rows before IRS EO BMF fallback: {missing_count:,}", flush=True)
        if missing_count == 0:
            return 0
        if irs_lookup_df.empty:
            print(f"[analysis] IRS EO BMF fallback table is empty; skipping {value_col} fallback.", flush=True)
            return 0
        join_columns = ["ein"]
        source_missing = source_df.loc[missing_mask, join_columns].copy()
        source_missing["row_index"] = source_missing.index
        if state_column and state_column in source_df.columns:
            source_missing["source_state_norm"] = blank_to_na(source_df.loc[missing_mask, state_column].astype("string")).fillna("").str.upper()
        else:
            source_missing["source_state_norm"] = ""
        candidates = source_missing.merge(irs_lookup_df, on="ein", how="inner", validate="many_to_many")
        candidates = candidates.loc[series_has_value(candidates[value_col])].copy()
        if candidates.empty:
            print(f"[analysis] No IRS EO BMF matches found for remaining missing {value_col} rows.", flush=True)
            return 0
        candidates["irs_state_norm"] = blank_to_na(candidates["irs_bmf_state"].astype("string")).fillna("").str.upper()
        candidates["state_match"] = (
            candidates["source_state_norm"].ne("")
            & candidates["irs_state_norm"].ne("")
            & candidates["source_state_norm"].eq(candidates["irs_state_norm"])
        )
        candidates = candidates.sort_values(by=["row_index", "state_match", source_variant_col], ascending=[True, False, True], kind="mergesort")
        chosen = candidates.drop_duplicates(subset=["row_index"], keep="first").copy()
        for row in tqdm(chosen.itertuples(index=False), total=len(chosen), desc=f"apply {value_col} IRS fallback", unit="row"):
            analysis_df.at[row.row_index, value_col] = getattr(row, value_col)
            analysis_df.at[row.row_index, source_family_col] = getattr(row, source_family_col)
            analysis_df.at[row.row_index, source_variant_col] = getattr(row, source_variant_col)
            analysis_df.at[row.row_index, source_column_col] = getattr(row, source_column_col)
        filled_count = int(len(chosen))
        print(f"[analysis] IRS EO BMF fallback rows populated for {value_col}: {filled_count:,}", flush=True)
        return filled_count

    nearest_ntee_count = _apply_nearest("analysis_ntee_code")
    nearest_subsection_count = _apply_nearest("analysis_subsection_code")
    irs_ntee_count = _apply_irs("analysis_ntee_code")
    irs_subsection_count = _apply_irs("analysis_subsection_code")
    return {
        "exact_ntee_count": exact_ntee_count,
        "nearest_ntee_count": nearest_ntee_count,
        "irs_ntee_count": irs_ntee_count,
        "exact_subsection_count": exact_subsection_count,
        "nearest_subsection_count": nearest_subsection_count,
        "irs_subsection_count": irs_subsection_count,
    }


def apply_imputed_flag_family(
    analysis_df: pd.DataFrame,
    *,
    name_column: str,
) -> None:
    """
    Populate complete imputed boolean columns from canonical source-backed flags.

    The imputation rule is intentionally conservative:
    1. keep the canonical source-backed flag when available
    2. allow only high-confidence organization-name matches to force True
    3. default remaining missing values to False
    """
    name_series = analysis_df[name_column].astype("string") if name_column in analysis_df.columns else pd.Series(pd.NA, index=analysis_df.index, dtype="string")

    for spec in tqdm(NAME_PROXY_FLAG_SPECS, desc="imputed flag families", unit="flag"):
        canonical_col = spec["canonical_output_column"]
        imputed_col = spec["imputed_output_column"]
        canonical_series = analysis_df[canonical_col].astype("boolean")
        proxy_values, proxy_confidence = build_name_proxy_flag(
            name_series,
            canonical_series,
            spec["high_confidence_patterns"],
            spec["medium_confidence_patterns"],
            spec["block_patterns"],
        )

        imputed_values = canonical_series.copy()
        source_series = pd.Series(pd.NA, index=analysis_df.index, dtype="string")
        source_series.loc[canonical_series.notna()] = analysis_df.get(f"{canonical_col}_source_column", pd.Series(pd.NA, index=analysis_df.index, dtype="string")).astype("string").loc[canonical_series.notna()]

        high_name_mask = imputed_values.isna() & proxy_confidence.fillna("").eq("high")
        default_false_mask = imputed_values.isna() & ~proxy_confidence.fillna("").eq("high")
        imputed_values.loc[high_name_mask] = True
        imputed_values.loc[default_false_mask] = False
        source_series.loc[high_name_mask] = name_column
        source_series.loc[default_false_mask] = "imputed_default_false"

        analysis_df[imputed_col] = imputed_values.astype("boolean")
        analysis_df[f"{imputed_col}_source_column"] = source_series
        analysis_df[f"{imputed_col}_source_rule"] = pd.Series(
            "canonical source-backed flag, then high-confidence name match, then default False",
            index=analysis_df.index,
            dtype="string",
        )
        print(
            f"[analysis] {imputed_col}: populated={int(analysis_df[imputed_col].notna().sum()):,} "
            f"high_name={int(high_name_mask.sum()):,} default_false={int(default_false_mask.sum()):,}",
            flush=True,
        )
