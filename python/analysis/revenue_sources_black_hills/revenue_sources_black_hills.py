"""
Revenue-source comparison for Black Hills and benchmark-region nonprofits.

This script answers the project question:

    Is there a difference in the revenue sources between Black Hills and
    benchmark regions for the limited filer universe of Forms 990, 990-EZ,
    and 990-PF?

Harmonized segments follow Section 3 Q9 with an explicit donor-channel
decomposition for Form 990 filers. The six mutually exclusive segments are:

    - program_service_revenue            (990/EZ Line 2g; PF stays missing)
    - government_grants_received         (Form 990 Line 1e: GOVERNGRANTS)
    - other_institutional_contributions  (Form 990 Lines 1a + 1d:
                                          FEDERACAMPAI + RELATEORGANI)
    - individual_likely_contributions    (Form 990 Lines 1b + 1c:
                                          MEMBERDUESUE + FUNDRAEVENTS)
    - mixed_other_contributions          (Form 990 Line 1f ALLOOTHECONT;
                                          for 990-EZ / 990-PF filers, the
                                          full Line 1 / Part I Line 1 total
                                          since those forms do not separately
                                          report sub-components)
    - residual_other_revenue             (total revenue minus the five above,
                                          clipped at zero for plotting)

This decomposition is the most informative slice the IRS basic 990 family can
support for the client question of individual versus institutional giving.
Government grants (Line 1e), federated campaigns (Line 1a), and related-org
contributions (Line 1d) are unambiguously institutional. Membership dues
(Line 1b) and fundraising events (Line 1c) are predominantly but not
exclusively individual. Line 1f mixes individual gifts, private foundation
grants, donor-advised fund distributions, corporate gifts, and bequests in a
single bucket that cannot be split further without Schedule B.

The implementation is intentionally verbose. It writes intermediate tables,
prints what it is doing at each major step, and keeps the statistical helper
functions importable so tests can validate the important derivations.
"""

from __future__ import annotations

import argparse
import math
import shutil
import sys
import warnings
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import seaborn as sns
import statsmodels.api as sm
from scipy import stats
from statsmodels.formula.api import glm, ols
from statsmodels.multivariate.manova import MANOVA


# Make `python/utils/paths.py` importable when the script is executed directly
# from the repository root, matching the pattern used by existing pipeline code.
_THIS_FILE = Path(__file__).resolve()
_PYTHON_DIR = _THIS_FILE.parents[2]
if str(_PYTHON_DIR) not in sys.path:
    sys.path.insert(0, str(_PYTHON_DIR))

from utils.paths import DATA as DEFAULT_DATA_ROOT  # noqa: E402


SOURCE_COMPONENTS = [
    "program_service_revenue",
    "government_grants_received",
    "other_institutional_contributions",
    "individual_likely_contributions",
    "mixed_other_contributions",
    "residual_other_revenue",
]

SHARE_COMPONENTS = [f"{column}_share" for column in SOURCE_COMPONENTS]

AMOUNT_VARIABLES = [
    "total_revenue",
    "program_service_revenue",
    "total_contributions",
    "government_grants_received",
    "other_institutional_contributions",
    "individual_likely_contributions",
    "mixed_other_contributions",
    "calculated_institutional_contributions_total",
    "federated_campaigns",
    "membership_dues",
    "fundraising_events_contributions",
    "related_org_contributions",
    "government_grants",
    "cash_contributions",
    "noncash_contributions",
    "allo_other_contributions_line_1f",
    "residual_other_revenue",
]

PRIMARY_TEST_VARIABLES = SHARE_COMPONENTS + [f"log1p_{column}" for column in AMOUNT_VARIABLES]

REGION_LABELS = {
    "BlackHills": "Black Hills",
    "SiouxFalls": "Sioux Falls",
    "Billings": "Billings",
    "Flagstaff": "Flagstaff",
    "Missoula": "Missoula",
}

GT_ANALYSIS_RELATIVE_PATH = Path("staging") / "filing" / "givingtuesday_990_basic_allforms_analysis_variables.parquet"
CORE_ANALYSIS_RELATIVE_PATH = Path("staging") / "nccs_990" / "core" / "nccs_990_core_analysis_variables.parquet"


@dataclass(frozen=True)
class AnalysisPaths:
    """Centralized output paths so the run is easy to audit."""

    output_dir: Path
    tables_dir: Path
    figures_dir: Path
    results_dir: Path
    results_tables_dir: Path
    results_figures_dir: Path


def info(message: str) -> None:
    """Print a consistently formatted progress message."""

    print(f"[revenue-sources] {message}", flush=True)


def default_results_dir_for_output(output_dir: Path) -> Path:
    """Return the default user-facing results directory for one analysis run."""

    # Keep the polished report bundle next to this analysis code, outside the
    # data tree, so it is easy to find and review:
    # python/analysis/revenue_sources_black_hills/results/
    return _THIS_FILE.parent / "results"


def ensure_output_dirs(output_dir: Path, results_dir: Path | None = None) -> AnalysisPaths:
    """Create the output directory tree used by tables, figures, and summaries."""

    tables_dir = output_dir / "tables"
    figures_dir = output_dir / "figures"
    resolved_results_dir = results_dir or default_results_dir_for_output(output_dir)
    results_dir = resolved_results_dir
    results_tables_dir = results_dir / "tables"
    results_figures_dir = results_dir / "figures"
    tables_dir.mkdir(parents=True, exist_ok=True)
    figures_dir.mkdir(parents=True, exist_ok=True)
    results_tables_dir.mkdir(parents=True, exist_ok=True)
    results_figures_dir.mkdir(parents=True, exist_ok=True)
    info(f"Output directory ready: {output_dir}")
    info(f"Tables will be written to: {tables_dir}")
    info(f"Figures will be written to: {figures_dir}")
    info(f"Final report assets will be collected in: {results_dir}")
    return AnalysisPaths(
        output_dir=output_dir,
        tables_dir=tables_dir,
        figures_dir=figures_dir,
        results_dir=results_dir,
        results_tables_dir=results_tables_dir,
        results_figures_dir=results_figures_dir,
    )


def to_numeric(series: pd.Series) -> pd.Series:
    """Convert a source column to numeric while preserving missing values."""

    return pd.to_numeric(series, errors="coerce")


def first_existing_column(frame: pd.DataFrame, candidates: Iterable[str]) -> pd.Series:
    """Return the first available candidate column, or an all-missing series."""

    for column in candidates:
        if column in frame.columns:
            return frame[column]
    return pd.Series([pd.NA] * len(frame), index=frame.index, dtype="object")


def normalize_bool_series(series: pd.Series) -> pd.Series:
    """Normalize several boolean-ish source encodings into pandas nullable booleans."""

    true_values = {"1", "Y", "YES", "TRUE", "T"}
    false_values = {"0", "N", "NO", "FALSE", "F"}

    def _normalize(value: object) -> object:
        if pd.isna(value):
            return pd.NA
        text = str(value).strip().upper()
        if text in true_values:
            return True
        if text in false_values:
            return False
        return pd.NA

    return series.map(_normalize).astype("boolean")


def map_comparison_group(region: object) -> str:
    """Map detailed region labels into the headline Black Hills vs benchmark contrast."""

    return "Black Hills" if str(region).strip() == "BlackHills" else "Benchmark"


def load_givingtuesday_analysis(data_root: Path, years: list[int]) -> pd.DataFrame:
    """Load the primary GivingTuesday analysis-variable artifact."""

    input_path = data_root / GT_ANALYSIS_RELATIVE_PATH
    info(f"Loading primary GivingTuesday analysis file: {input_path}")
    if not input_path.exists():
        raise FileNotFoundError(f"Primary GivingTuesday analysis file not found: {input_path}")
    frame = pd.read_parquet(input_path)
    info(f"Loaded GivingTuesday rows={len(frame):,}, columns={len(frame.columns):,}")
    frame["tax_year"] = frame["tax_year"].astype(str)
    selected_years = {str(year) for year in years}
    frame = frame.loc[frame["tax_year"].isin(selected_years)].copy()
    info(f"Rows after requested-year filter {sorted(selected_years)}: {len(frame):,}")
    return frame


def prepare_givingtuesday_analysis(frame: pd.DataFrame, *, exclude_outliers: bool = False) -> pd.DataFrame:
    """
    Build the primary analysis frame at organization-year grain.

    The detailed comments in this function are deliberate. These derivations
    define the research question operationally, so they should be visible to
    anyone reviewing or reusing the script.
    """

    info("Deriving primary analysis variables from GivingTuesday fields.")
    out = pd.DataFrame(index=frame.index)
    out["ein"] = first_existing_column(frame, ["ein"]).astype(str).str.zfill(9)
    out["tax_year"] = first_existing_column(frame, ["tax_year"]).astype(str)
    out["form_type"] = first_existing_column(frame, ["form_type"]).astype(str).str.upper().str.strip()
    out["region"] = first_existing_column(frame, ["region"]).astype(str).str.strip()
    out["region_label"] = out["region"].map(REGION_LABELS).fillna(out["region"])
    out["comparison_group"] = out["region"].map(map_comparison_group)
    out["is_black_hills"] = out["comparison_group"].eq("Black Hills").astype(int)

    # Revenue and contribution fields are already extracted by the upstream GT
    # analysis layer. We still coerce them here so downstream tests and plots use
    # one consistent numeric representation.
    out["total_revenue"] = to_numeric(first_existing_column(frame, ["analysis_total_revenue_amount"]))
    out["program_service_revenue"] = to_numeric(first_existing_column(frame, ["analysis_program_service_revenue_amount"]))
    out["cash_contributions"] = to_numeric(first_existing_column(frame, ["analysis_cash_contributions_amount"]))
    out["noncash_contributions"] = to_numeric(first_existing_column(frame, ["analysis_noncash_contributions_amount"]))
    out["allo_other_contributions_line_1f"] = to_numeric(first_existing_column(frame, ["analysis_other_contributions_amount"]))
    out["federated_campaigns"] = to_numeric(first_existing_column(frame, ["analysis_federated_campaigns_amount"]))
    out["membership_dues"] = to_numeric(first_existing_column(frame, ["analysis_membership_dues_amount"]))
    out["fundraising_events_contributions"] = to_numeric(
        first_existing_column(frame, ["analysis_fundraising_events_contributions_amount"])
    )
    out["related_org_contributions"] = to_numeric(first_existing_column(frame, ["analysis_related_org_contributions_amount"]))
    out["government_grants"] = to_numeric(first_existing_column(frame, ["analysis_government_grants_amount"]))
    out["calculated_institutional_contributions_total"] = to_numeric(
        first_existing_column(frame, ["analysis_calculated_grants_total_amount"])
    )
    out["total_contributions"] = to_numeric(
        first_existing_column(frame, ["analysis_total_contributions_amount", "analysis_calculated_total_contributions_amount"])
    )

    # Form 990-PF rows do not have a comparable program-service revenue concept
    # in this source. Keeping those values missing prevents the analysis from
    # accidentally treating "not applicable" as a real zero.
    pf_mask = out["form_type"].eq("990PF")
    out.loc[pf_mask, "program_service_revenue"] = np.nan

    # Section 3 Q9 donor-channel decomposition.
    #
    # On Form 990, Part VIII Line 1 has six sub-lines (1a-1f) that reconcile to
    # Line 1h. We expose those sub-lines so the stacked-bar mix can answer the
    # client's "individuals versus other organizations" question more honestly:
    #
    #   - government_grants_received        : Line 1e (GOVERNGRANTS)
    #   - other_institutional_contributions : Line 1a + Line 1d
    #                                         (FEDERACAMPAI + RELATEORGANI)
    #   - individual_likely_contributions   : Line 1b + Line 1c
    #                                         (MEMBERDUESUE + FUNDRAEVENTS)
    #   - mixed_other_contributions         : Line 1f (ALLOOTHECONT) - a
    #                                         deliberately-mixed bucket that
    #                                         lumps individual gifts together
    #                                         with private foundation grants,
    #                                         DAF distributions, corporate
    #                                         gifts, and bequests. Form 990
    #                                         does not separate those donors.
    #
    # 990-EZ and 990-PF do not separately report Line 1 sub-components, so for
    # those filers the upstream analysis layer leaves the sub-fields null and
    # we route the entire reported Line 1 / Part I Line 1 total into the
    # mixed_other_contributions bucket. That keeps the segments mutually
    # exclusive and summing to total revenue while honestly labeling the
    # 990-EZ/990-PF contributions as undecomposable.
    is_form_990 = out["form_type"].eq("990")

    sub_components = [
        "federated_campaigns",
        "membership_dues",
        "fundraising_events_contributions",
        "related_org_contributions",
        "government_grants",
        "allo_other_contributions_line_1f",
    ]
    out["sub_component_sum"] = out[sub_components].fillna(0.0).sum(axis=1)

    out["government_grants_received"] = np.where(is_form_990, out["government_grants"].fillna(0.0), 0.0)
    out["other_institutional_contributions"] = np.where(
        is_form_990,
        out["federated_campaigns"].fillna(0.0) + out["related_org_contributions"].fillna(0.0),
        0.0,
    )
    out["individual_likely_contributions"] = np.where(
        is_form_990,
        out["membership_dues"].fillna(0.0) + out["fundraising_events_contributions"].fillna(0.0),
        0.0,
    )

    # For 990 rows, the mixed bucket is Line 1f. For 990-EZ / 990-PF, the
    # forms do not expose sub-components, so the entire Line 1 / Part I Line 1
    # total goes into the mixed bucket (it really is undecomposable).
    mixed_for_990 = out["allo_other_contributions_line_1f"].fillna(0.0)
    mixed_for_other_forms = out["total_contributions"].fillna(0.0)
    out["mixed_other_contributions"] = np.where(is_form_990, mixed_for_990, mixed_for_other_forms)

    # Diagnostic: when we trust the 990 sub-components, verify they reconcile
    # to the reported Line 1h total contributions. A meaningful gap usually
    # means the upstream extract dropped a sub-line or rounded.
    out["sub_components_sum_for_990"] = np.where(is_form_990, out["sub_component_sum"], np.nan)
    out["grants_exceed_total_contributions_flag"] = (
        is_form_990
        & out["total_contributions"].notna()
        & out["calculated_institutional_contributions_total"].notna()
        & out["calculated_institutional_contributions_total"].gt(out["total_contributions"])
    )

    # Residual captures revenue not allocated to the five named segments.
    contributions_segment_sum = (
        out["government_grants_received"]
        + out["other_institutional_contributions"]
        + out["individual_likely_contributions"]
        + out["mixed_other_contributions"]
    )
    filled_psr = out["program_service_revenue"].fillna(0.0)
    out["residual_other_revenue"] = out["total_revenue"] - filled_psr - contributions_segment_sum
    out["negative_residual_flag"] = out["residual_other_revenue"].lt(-1e-9)
    out["share_over_100_flag"] = False

    # Optional outlier flags come from the upstream NTEE/name proxy logic. If the
    # columns are missing, the script keeps all rows and explains that in output.
    out["is_hospital"] = normalize_bool_series(first_existing_column(frame, ["analysis_imputed_is_hospital", "analysis_is_hospital"]))
    out["is_university"] = normalize_bool_series(first_existing_column(frame, ["analysis_imputed_is_university", "analysis_is_university"]))
    out["is_political_org"] = normalize_bool_series(first_existing_column(frame, ["analysis_imputed_is_political_org", "analysis_is_political_org"]))
    if exclude_outliers:
        keep_mask = ~(
            out["is_hospital"].fillna(False)
            | out["is_university"].fillna(False)
            | out["is_political_org"].fillna(False)
        )
        info(f"Applying requested outlier exclusion. Rows before={len(out):,}, after={int(keep_mask.sum()):,}")
        out = out.loc[keep_mask].copy()

    # Keep only the rows that can answer the revenue-source question. A positive
    # total revenue denominator is required for interpretable shares.
    valid_revenue_mask = out["total_revenue"].notna() & out["total_revenue"].gt(0)
    valid_geo_mask = out["region"].notna() & out["region"].ne("") & out["tax_year"].notna() & out["form_type"].notna()
    before_filter = len(out)
    out = out.loc[valid_revenue_mask & valid_geo_mask].copy()
    info(f"Rows after valid geography/form/year and positive revenue filter: {len(out):,} of {before_filter:,}")

    # Shares are row-level organization-year proportions. Negative residuals are
    # retained in diagnostics but clipped to zero for composition-only plots and
    # transforms where negative components are mathematically invalid.
    for column in AMOUNT_VARIABLES:
        out[f"log1p_{column}"] = np.log1p(out[column].clip(lower=0))

    for column in SOURCE_COMPONENTS:
        share_column = f"{column}_share"
        numerator = out[column].copy()
        if column == "residual_other_revenue":
            numerator = numerator.clip(lower=0)
        out[share_column] = numerator / out["total_revenue"]

    share_sum = out[SHARE_COMPONENTS].sum(axis=1, min_count=1)
    out["source_share_sum"] = share_sum
    out["share_over_100_flag"] = share_sum.gt(1.000001)
    out["revenue_size_stratum"] = assign_revenue_size_strata(out["total_revenue"])

    info("Finished deriving revenue amounts, revenue shares, source flags, and revenue-size strata.")
    return out.reset_index(drop=True)


def assign_revenue_size_strata(total_revenue: pd.Series) -> pd.Series:
    """Assign small/medium/large strata using tertiles of positive total revenue."""

    valid = total_revenue.dropna()
    if valid.nunique() < 3:
        return pd.Series(["All"] * len(total_revenue), index=total_revenue.index, dtype="object")
    try:
        strata = pd.qcut(total_revenue, q=3, labels=["Small", "Medium", "Large"], duplicates="drop")
    except ValueError:
        return pd.Series(["All"] * len(total_revenue), index=total_revenue.index, dtype="object")
    return strata.astype("object").fillna("Unknown")


def missingness_table(frame: pd.DataFrame) -> pd.DataFrame:
    """Summarize source-field coverage by comparison group, year, form, and region."""

    info("Building missingness and field-coverage table.")
    rows: list[dict[str, object]] = []
    group_columns = ["comparison_group", "region_label", "tax_year", "form_type"]
    for keys, group in frame.groupby(group_columns, dropna=False):
        base = dict(zip(group_columns, keys, strict=False))
        for column in AMOUNT_VARIABLES + SHARE_COMPONENTS:
            series = group[column]
            rows.append(
                {
                    **base,
                    "variable": column,
                    "row_count": len(group),
                    "nonmissing_count": int(series.notna().sum()),
                    "nonzero_count": int(series.fillna(0).ne(0).sum()),
                    "missing_rate": float(series.isna().mean()) if len(group) else np.nan,
                }
            )
    return pd.DataFrame(rows)


def counts_table(frame: pd.DataFrame) -> pd.DataFrame:
    """Count rows and unique EINs across the main reporting dimensions."""

    info("Building counts table by region, group, year, and form type.")
    return (
        frame.groupby(["comparison_group", "region_label", "tax_year", "form_type"], dropna=False)
        .agg(row_count=("ein", "size"), unique_ein_count=("ein", "nunique"))
        .reset_index()
        .sort_values(["comparison_group", "region_label", "tax_year", "form_type"])
    )


def summary_statistics(frame: pd.DataFrame) -> pd.DataFrame:
    """Build descriptive summaries for amounts and shares."""

    info("Building descriptive statistics for revenue amounts and shares.")
    rows: list[dict[str, object]] = []
    group_columns = ["comparison_group", "region_label", "tax_year", "form_type"]
    for keys, group in frame.groupby(group_columns, dropna=False):
        base = dict(zip(group_columns, keys, strict=False))
        for column in AMOUNT_VARIABLES + SHARE_COMPONENTS:
            series = group[column].dropna()
            rows.append(
                {
                    **base,
                    "variable": column,
                    "n": int(series.size),
                    "sum": float(series.sum()) if series.size else np.nan,
                    "mean": float(series.mean()) if series.size else np.nan,
                    "median": float(series.median()) if series.size else np.nan,
                    "std": float(series.std(ddof=1)) if series.size > 1 else np.nan,
                    "iqr": float(series.quantile(0.75) - series.quantile(0.25)) if series.size else np.nan,
                    "p25": float(series.quantile(0.25)) if series.size else np.nan,
                    "p75": float(series.quantile(0.75)) if series.size else np.nan,
                }
            )
    return pd.DataFrame(rows)


def bootstrap_ci_difference(
    frame: pd.DataFrame,
    variable: str,
    *,
    group_column: str = "comparison_group",
    a: str = "Black Hills",
    b: str = "Benchmark",
    iterations: int = 1000,
    seed: int = 321,
    cluster_column: str | None = "ein",
) -> tuple[float, float, float]:
    """Bootstrap the mean difference (a minus b) for one variable.

    When ``cluster_column`` is provided and the column exists in ``frame``, the
    routine performs a *cluster bootstrap*: clusters (e.g. EINs) are sampled
    with replacement within each comparison group, and all org-year rows
    belonging to a sampled cluster are kept together. This preserves the
    within-cluster correlation structure that arises when the same nonprofit
    files multiple years and avoids the row-level bootstrap's tendency to
    underestimate the variance of the mean difference. If ``cluster_column``
    is None or missing, the routine falls back to a row-level bootstrap so the
    function remains usable for tests that have no clustering structure.
    """

    rng = np.random.default_rng(seed)
    work = frame[[group_column, variable]].copy()
    use_clusters = cluster_column is not None and cluster_column in frame.columns
    if use_clusters:
        work[cluster_column] = frame[cluster_column].to_numpy()
    work = work.dropna(subset=[group_column, variable])
    a_values = work.loc[work[group_column].eq(a), variable].to_numpy(dtype=float)
    b_values = work.loc[work[group_column].eq(b), variable].to_numpy(dtype=float)
    if len(a_values) == 0 or len(b_values) == 0:
        return np.nan, np.nan, np.nan
    observed = float(np.mean(a_values) - np.mean(b_values))

    if use_clusters:
        a_frame = work.loc[work[group_column].eq(a)]
        b_frame = work.loc[work[group_column].eq(b)]
        a_cluster_groups = [group[variable].to_numpy(dtype=float) for _, group in a_frame.groupby(cluster_column, sort=False)]
        b_cluster_groups = [group[variable].to_numpy(dtype=float) for _, group in b_frame.groupby(cluster_column, sort=False)]
        a_cluster_count = len(a_cluster_groups)
        b_cluster_count = len(b_cluster_groups)
        if a_cluster_count == 0 or b_cluster_count == 0:
            return observed, np.nan, np.nan
        draws = np.empty(iterations)
        for index in range(iterations):
            a_idx = rng.integers(0, a_cluster_count, size=a_cluster_count)
            b_idx = rng.integers(0, b_cluster_count, size=b_cluster_count)
            a_sample = np.concatenate([a_cluster_groups[i] for i in a_idx])
            b_sample = np.concatenate([b_cluster_groups[i] for i in b_idx])
            draws[index] = a_sample.mean() - b_sample.mean()
        lower, upper = np.percentile(draws, [2.5, 97.5])
        return observed, float(lower), float(upper)

    draws = np.empty(iterations)
    for index in range(iterations):
        a_sample = rng.choice(a_values, size=len(a_values), replace=True)
        b_sample = rng.choice(b_values, size=len(b_values), replace=True)
        draws[index] = np.mean(a_sample) - np.mean(b_sample)
    lower, upper = np.percentile(draws, [2.5, 97.5])
    return observed, float(lower), float(upper)


def aggregate_revenue_mix(frame: pd.DataFrame, group_columns: list[str]) -> pd.DataFrame:
    """
    Compute aggregate source mix by group.

    The source components in the GivingTuesday all-forms file are analytically
    useful, but in real data they are not always mutually exclusive. For that
    reason this table keeps two different share concepts:

    - `share`: the source-reported component amount divided by total revenue.
      These shares can sum above 100 percent when fields overlap.
    - `normalized_mix_share`: the component amount divided by the sum of the
      nonnegative displayed components. This is used only for 100-percent
      stacked visuals so the plot compares profiles without implying that the
      source fields reconcile perfectly to total revenue.
    """

    info(f"Computing aggregate revenue mix by {', '.join(group_columns)}.")
    grouped = frame.groupby(group_columns, dropna=False)
    rows: list[dict[str, object]] = []
    for keys, group in grouped:
        if not isinstance(keys, tuple):
            keys = (keys,)
        base = dict(zip(group_columns, keys, strict=False))
        total_revenue = group["total_revenue"].sum(skipna=True)
        for component in SOURCE_COMPONENTS:
            values = group[component]
            if component == "residual_other_revenue":
                values = values.clip(lower=0)
            amount = values.sum(skipna=True)
            rows.append(
                {
                    **base,
                    "component": component,
                    "amount": float(amount),
                    "total_revenue": float(total_revenue),
                    "share": float(amount / total_revenue) if total_revenue else np.nan,
                }
            )
    result = pd.DataFrame(rows)
    if result.empty:
        return result
    result["component_amount_sum"] = result.groupby(group_columns, dropna=False)["amount"].transform("sum")
    result["reported_component_share_sum"] = result.groupby(group_columns, dropna=False)["share"].transform("sum")
    result["normalized_mix_share"] = np.where(
        result["component_amount_sum"].gt(0),
        result["amount"] / result["component_amount_sum"],
        np.nan,
    )
    return result


def component_overlap_summary(frame: pd.DataFrame, group_columns: list[str]) -> pd.DataFrame:
    """
    Summarize rows where source components do not reconcile cleanly.

    Negative residuals and source-share totals above 100 percent are not
    necessarily code errors; they usually indicate that the component fields
    overlap or are measured on slightly different reporting concepts. Reporting
    their rate keeps the stacked-bar interpretation appropriately cautious.
    """

    info(f"Computing component-overlap diagnostics by {', '.join(group_columns)}.")
    rows: list[dict[str, object]] = []
    for keys, group in frame.groupby(group_columns, dropna=False):
        if not isinstance(keys, tuple):
            keys = (keys,)
        base = dict(zip(group_columns, keys, strict=False))
        row_count = len(group)
        negative_count = int(group["negative_residual_flag"].sum())
        over_100_count = int(group["share_over_100_flag"].sum())
        rows.append(
            {
                **base,
                "row_count": row_count,
                "negative_residual_count": negative_count,
                "negative_residual_rate": float(negative_count / row_count) if row_count else np.nan,
                "share_over_100_count": over_100_count,
                "share_over_100_rate": float(over_100_count / row_count) if row_count else np.nan,
                "mean_source_share_sum": float(group["source_share_sum"].mean()),
                "median_source_share_sum": float(group["source_share_sum"].median()),
                "max_source_share_sum": float(group["source_share_sum"].max()),
            }
        )
    return pd.DataFrame(rows)


def anova_result(frame: pd.DataFrame, variable: str, group_column: str = "comparison_group") -> dict[str, object]:
    """Run a standard one-way ANOVA for one variable."""

    groups = [group[variable].dropna().to_numpy(dtype=float) for _, group in frame.groupby(group_column)]
    groups = [group for group in groups if len(group) > 1]
    if len(groups) < 2:
        return {"test": "anova", "variable": variable, "statistic": np.nan, "p_value": np.nan, "n": int(frame[variable].notna().sum())}
    statistic, p_value = stats.f_oneway(*groups)
    return {"test": "anova", "variable": variable, "statistic": float(statistic), "p_value": float(p_value), "n": int(sum(len(group) for group in groups))}


def welch_anova_result(frame: pd.DataFrame, variable: str, group_column: str = "comparison_group") -> dict[str, object]:
    """Run Welch ANOVA for unequal variances using the standard weighted formula."""

    values_by_group = [group[variable].dropna().to_numpy(dtype=float) for _, group in frame.groupby(group_column)]
    values_by_group = [values for values in values_by_group if len(values) > 1]
    k = len(values_by_group)
    if k < 2:
        return {"test": "welch_anova", "variable": variable, "statistic": np.nan, "p_value": np.nan, "n": int(frame[variable].notna().sum())}
    n = np.array([len(values) for values in values_by_group], dtype=float)
    means = np.array([np.mean(values) for values in values_by_group], dtype=float)
    variances = np.array([np.var(values, ddof=1) for values in values_by_group], dtype=float)
    if np.any(variances <= 0):
        return {"test": "welch_anova", "variable": variable, "statistic": np.nan, "p_value": np.nan, "n": int(np.sum(n))}
    weights = n / variances
    weighted_mean = np.sum(weights * means) / np.sum(weights)
    numerator = np.sum(weights * (means - weighted_mean) ** 2) / (k - 1)
    denominator_adjustment = 1 + (2 * (k - 2) / (k**2 - 1)) * np.sum((1 / (n - 1)) * (1 - weights / np.sum(weights)) ** 2)
    statistic = numerator / denominator_adjustment
    df_num = k - 1
    df_den = (k**2 - 1) / (3 * np.sum((1 / (n - 1)) * (1 - weights / np.sum(weights)) ** 2))
    p_value = stats.f.sf(statistic, df_num, df_den)
    return {"test": "welch_anova", "variable": variable, "statistic": float(statistic), "p_value": float(p_value), "n": int(np.sum(n))}


def rank_test_result(frame: pd.DataFrame, variable: str, group_column: str = "comparison_group") -> dict[str, object]:
    """Run Mann-Whitney for two groups or Kruskal-Wallis for more groups."""

    values_by_group = [group[variable].dropna().to_numpy(dtype=float) for _, group in frame.groupby(group_column)]
    values_by_group = [values for values in values_by_group if len(values) > 0]
    if len(values_by_group) < 2:
        return {"test": "rank_test", "variable": variable, "statistic": np.nan, "p_value": np.nan, "n": int(frame[variable].notna().sum())}
    if len(values_by_group) == 2:
        statistic, p_value = stats.mannwhitneyu(values_by_group[0], values_by_group[1], alternative="two-sided")
        test_name = "mann_whitney"
    else:
        statistic, p_value = stats.kruskal(*values_by_group)
        test_name = "kruskal_wallis"
    return {"test": test_name, "variable": variable, "statistic": float(statistic), "p_value": float(p_value), "n": int(sum(len(values) for values in values_by_group))}


def permutation_test_difference(
    frame: pd.DataFrame,
    variable: str,
    *,
    group_column: str = "comparison_group",
    a: str = "Black Hills",
    b: str = "Benchmark",
    iterations: int = 2000,
    seed: int = 321,
) -> dict[str, object]:
    """Nonparametric permutation test for mean difference between two groups."""

    rng = np.random.default_rng(seed)
    work = frame.loc[frame[group_column].isin([a, b]), [group_column, variable]].dropna()
    a_values = work.loc[work[group_column].eq(a), variable].to_numpy(dtype=float)
    b_values = work.loc[work[group_column].eq(b), variable].to_numpy(dtype=float)
    if len(a_values) == 0 or len(b_values) == 0:
        return {"test": "permutation_mean_diff", "variable": variable, "statistic": np.nan, "p_value": np.nan, "n": len(work)}
    observed = float(np.mean(a_values) - np.mean(b_values))
    pooled = np.concatenate([a_values, b_values])
    n_a = len(a_values)
    extreme = 0
    for _ in range(iterations):
        permuted = rng.permutation(pooled)
        difference = np.mean(permuted[:n_a]) - np.mean(permuted[n_a:])
        if abs(difference) >= abs(observed):
            extreme += 1
    p_value = (extreme + 1) / (iterations + 1)
    return {"test": "permutation_mean_diff", "variable": variable, "statistic": observed, "p_value": float(p_value), "n": len(work)}


def effect_size_result(frame: pd.DataFrame, variable: str) -> dict[str, object]:
    """Compute practical effect sizes for Black Hills minus benchmark."""

    bh = frame.loc[frame["comparison_group"].eq("Black Hills"), variable].dropna().to_numpy(dtype=float)
    bm = frame.loc[frame["comparison_group"].eq("Benchmark"), variable].dropna().to_numpy(dtype=float)
    if len(bh) < 2 or len(bm) < 2:
        return {
            "variable": variable,
            "mean_difference": np.nan,
            "standardized_mean_difference": np.nan,
            "cliffs_delta": np.nan,
            "eta_squared": np.nan,
            "omega_squared": np.nan,
        }

    mean_difference = float(np.mean(bh) - np.mean(bm))
    pooled_sd = math.sqrt(((len(bh) - 1) * np.var(bh, ddof=1) + (len(bm) - 1) * np.var(bm, ddof=1)) / (len(bh) + len(bm) - 2))
    smd = mean_difference / pooled_sd if pooled_sd else np.nan
    cliffs = cliffs_delta(bh, bm)

    all_values = np.concatenate([bh, bm])
    group_means = [np.mean(bh), np.mean(bm)]
    grand_mean = np.mean(all_values)
    ss_between = len(bh) * (group_means[0] - grand_mean) ** 2 + len(bm) * (group_means[1] - grand_mean) ** 2
    ss_within = np.sum((bh - group_means[0]) ** 2) + np.sum((bm - group_means[1]) ** 2)
    ss_total = ss_between + ss_within
    df_between = 1
    df_within = len(all_values) - 2
    ms_within = ss_within / df_within if df_within > 0 else np.nan
    eta_squared = ss_between / ss_total if ss_total else np.nan
    omega_squared = (ss_between - df_between * ms_within) / (ss_total + ms_within) if ss_total and not np.isnan(ms_within) else np.nan

    return {
        "variable": variable,
        "mean_difference": mean_difference,
        "standardized_mean_difference": float(smd) if not np.isnan(smd) else np.nan,
        "cliffs_delta": cliffs,
        "eta_squared": float(eta_squared) if not np.isnan(eta_squared) else np.nan,
        "omega_squared": float(omega_squared) if not np.isnan(omega_squared) else np.nan,
    }


def cliffs_delta(a_values: np.ndarray, b_values: np.ndarray) -> float:
    """Compute Cliff's delta using ranks, avoiding a full pairwise matrix."""

    combined = np.concatenate([a_values, b_values])
    labels = np.concatenate([np.ones(len(a_values)), np.zeros(len(b_values))])
    order = np.argsort(combined, kind="mergesort")
    sorted_labels = labels[order]
    sorted_values = combined[order]

    greater = 0.0
    less = 0.0
    b_seen = 0
    total_b = len(b_values)
    index = 0
    while index < len(sorted_values):
        end = index + 1
        while end < len(sorted_values) and sorted_values[end] == sorted_values[index]:
            end += 1
        tie_labels = sorted_labels[index:end]
        a_in_tie = int(np.sum(tie_labels == 1))
        b_in_tie = int(np.sum(tie_labels == 0))
        greater += a_in_tie * b_seen
        less += a_in_tie * (total_b - b_seen - b_in_tie)
        b_seen += b_in_tie
        index = end
    return float((greater - less) / (len(a_values) * len(b_values)))


def fdr_bh(p_values: Iterable[float]) -> list[float]:
    """Benjamini-Hochberg FDR correction that handles missing p-values."""

    p_array = np.array([np.nan if pd.isna(value) else float(value) for value in p_values], dtype=float)
    adjusted = np.full_like(p_array, np.nan, dtype=float)
    valid_mask = ~np.isnan(p_array)
    valid = p_array[valid_mask]
    if valid.size == 0:
        return adjusted.tolist()
    order = np.argsort(valid)
    ranked = valid[order]
    m = len(ranked)
    raw_adjusted = ranked * m / np.arange(1, m + 1)
    raw_adjusted = np.minimum.accumulate(raw_adjusted[::-1])[::-1]
    raw_adjusted = np.clip(raw_adjusted, 0, 1)
    valid_adjusted = np.empty_like(raw_adjusted)
    valid_adjusted[order] = raw_adjusted
    adjusted[valid_mask] = valid_adjusted
    return adjusted.tolist()


def run_univariate_tests(
    frame: pd.DataFrame,
    variables: list[str],
    *,
    label: str,
    group_column: str = "comparison_group",
) -> pd.DataFrame:
    """Run the primary univariate test suite for a set of variables."""

    info(f"Running univariate tests for analysis frame: {label} (group={group_column})")
    rows: list[dict[str, object]] = []
    for variable in variables:
        if variable not in frame.columns:
            continue
        if frame[variable].dropna().nunique() < 2:
            continue
        test_results: list[dict[str, object]] = [
            anova_result(frame, variable, group_column=group_column),
            welch_anova_result(frame, variable, group_column=group_column),
            rank_test_result(frame, variable, group_column=group_column),
        ]
        if group_column == "comparison_group":
            test_results.append(permutation_test_difference(frame, variable))
        for result in test_results:
            result["analysis_frame"] = label
            result["group_column"] = group_column
            rows.append(result)
        if group_column == "comparison_group":
            effect = effect_size_result(frame, variable)
            for key, value in effect.items():
                if key != "variable":
                    rows.append(
                        {
                            "analysis_frame": label,
                            "group_column": group_column,
                            "test": key,
                            "variable": variable,
                            "statistic": value,
                            "p_value": np.nan,
                            "n": int(frame[variable].notna().sum()),
                        }
                    )
    results = pd.DataFrame(rows)
    if not results.empty and "p_value" in results.columns:
        primary_mask = results["test"].isin(["anova", "welch_anova", "mann_whitney", "kruskal_wallis", "permutation_mean_diff"])
        results["p_value_fdr_bh"] = np.nan
        results.loc[primary_mask, "p_value_fdr_bh"] = fdr_bh(results.loc[primary_mask, "p_value"])
    return results


def run_regressions(frame: pd.DataFrame, variables: list[str]) -> pd.DataFrame:
    """Run OLS and logistic-presence models with year and form-type controls."""

    info("Running regression models with tax-year and form-type controls.")
    rows: list[dict[str, object]] = []
    for variable in variables:
        if variable not in frame.columns or frame[variable].dropna().nunique() < 2:
            continue
        model_frame = frame[["ein", "is_black_hills", "tax_year", "form_type", variable]].dropna().copy()
        if model_frame["is_black_hills"].nunique() < 2 or len(model_frame) < 10:
            continue
        formula = f"{variable} ~ is_black_hills + C(tax_year) + C(form_type)"
        try:
            model = ols(formula, data=model_frame).fit(cov_type="cluster", cov_kwds={"groups": model_frame["ein"]})
            rows.append(
                {
                    "model": "ols_clustered_by_ein",
                    "outcome": variable,
                    "term": "is_black_hills",
                    "estimate": float(model.params.get("is_black_hills", np.nan)),
                    "std_error": float(model.bse.get("is_black_hills", np.nan)),
                    "p_value": float(model.pvalues.get("is_black_hills", np.nan)),
                    "n": int(model.nobs),
                }
            )
        except Exception as exc:  # pragma: no cover - defensive for singular designs in real data
            rows.append({"model": "ols_clustered_by_ein", "outcome": variable, "term": "is_black_hills", "estimate": np.nan, "std_error": np.nan, "p_value": np.nan, "n": len(model_frame), "error": str(exc)})

        presence_column = f"{variable}_present"
        model_frame[presence_column] = model_frame[variable].gt(0).astype(int)
        if model_frame[presence_column].nunique() < 2:
            continue
        try:
            with warnings.catch_warnings(record=True) as caught_warnings:
                warnings.simplefilter("always")
                logistic_model = glm(
                    f"{presence_column} ~ is_black_hills + C(tax_year) + C(form_type)",
                    data=model_frame,
                    family=sm.families.Binomial(),
                ).fit(
                    maxiter=200,
                    cov_type="cluster",
                    cov_kwds={"groups": model_frame["ein"].to_numpy()},
                )
            warning_messages = "; ".join(str(warning.message) for warning in caught_warnings)
            rows.append(
                {
                    "model": "logistic_presence_clustered_by_ein",
                    "outcome": presence_column,
                    "term": "is_black_hills",
                    "estimate": float(logistic_model.params.get("is_black_hills", np.nan)),
                    "std_error": float(logistic_model.bse.get("is_black_hills", np.nan)),
                    "p_value": float(logistic_model.pvalues.get("is_black_hills", np.nan)),
                    "n": int(logistic_model.nobs),
                    "warning": warning_messages,
                }
            )
        except Exception as exc:  # pragma: no cover - defensive for separation in real data
            rows.append({"model": "logistic_presence_clustered_by_ein", "outcome": presence_column, "term": "is_black_hills", "estimate": np.nan, "std_error": np.nan, "p_value": np.nan, "n": len(model_frame), "error": str(exc)})
    results = pd.DataFrame(rows)
    if not results.empty:
        results["p_value_fdr_bh"] = fdr_bh(results["p_value"])
    return results


def composition_matrix(frame: pd.DataFrame, components: list[str] = SOURCE_COMPONENTS) -> pd.DataFrame:
    """Build a nonnegative composition matrix that sums to one per row."""

    matrix = frame[components].copy()
    for component in components:
        if component == "residual_other_revenue":
            matrix[component] = matrix[component].clip(lower=0)
        matrix[component] = matrix[component].fillna(0).clip(lower=0)
    row_sum = matrix.sum(axis=1)
    valid = row_sum.gt(0)
    matrix = matrix.loc[valid].div(row_sum.loc[valid], axis=0)
    matrix.index = frame.loc[valid].index
    return matrix


def clr_transform(composition: pd.DataFrame, pseudocount: float = 1e-6) -> pd.DataFrame:
    """Centered log-ratio transform for compositional revenue shares."""

    adjusted = composition + pseudocount
    adjusted = adjusted.div(adjusted.sum(axis=1), axis=0)
    logged = np.log(adjusted)
    return logged.sub(logged.mean(axis=1), axis=0)


def alr_transform(
    composition: pd.DataFrame,
    *,
    denominator: str = "residual_other_revenue",
    pseudocount: float = 1e-6,
) -> pd.DataFrame:
    """
    Additive log-ratio transform for MANOVA-style models.

    CLR-transformed columns sum to zero by construction, which can make MANOVA
    singular in year-specific or otherwise thin frames. ALR keeps k-1
    independent transformed outcomes by comparing each component with one
    denominator component.
    """

    if denominator not in composition.columns:
        denominator = composition.columns[-1]
    adjusted = composition + pseudocount
    adjusted = adjusted.div(adjusted.sum(axis=1), axis=0)
    denominator_values = adjusted[denominator]
    transformed = {
        f"alr_{component}": np.log(adjusted[component] / denominator_values)
        for component in adjusted.columns
        if component != denominator
    }
    return pd.DataFrame(transformed, index=composition.index)


def permanova_two_group(
    frame: pd.DataFrame,
    *,
    group_column: str = "comparison_group",
    iterations: int = 999,
    seed: int = 321,
) -> dict[str, object]:
    """PERMANOVA-style permutation test on Euclidean distances in CLR space."""

    composition = composition_matrix(frame)
    if composition.empty:
        return {"test": "permanova_clr", "statistic": np.nan, "p_value": np.nan, "n": 0}
    groups = frame.loc[composition.index, group_column].astype(str)
    if groups.nunique() != 2:
        return {"test": "permanova_clr", "statistic": np.nan, "p_value": np.nan, "n": len(composition)}
    transformed = clr_transform(composition).to_numpy(dtype=float)
    labels = groups.to_numpy()
    observed = pseudo_f_statistic(transformed, labels)
    rng = np.random.default_rng(seed)
    extreme = 0
    for _ in range(iterations):
        permuted = rng.permutation(labels)
        stat_value = pseudo_f_statistic(transformed, permuted)
        if stat_value >= observed:
            extreme += 1
    p_value = (extreme + 1) / (iterations + 1)
    return {"test": "permanova_clr", "statistic": float(observed), "p_value": float(p_value), "n": len(composition)}


def pseudo_f_statistic(values: np.ndarray, labels: np.ndarray) -> float:
    """Compute a between-group over within-group pseudo-F statistic."""

    grand = values.mean(axis=0)
    groups = np.unique(labels)
    ss_between = 0.0
    ss_within = 0.0
    for group in groups:
        group_values = values[labels == group]
        centroid = group_values.mean(axis=0)
        ss_between += len(group_values) * np.sum((centroid - grand) ** 2)
        ss_within += np.sum((group_values - centroid) ** 2)
    df_between = len(groups) - 1
    df_within = len(values) - len(groups)
    if df_between <= 0 or df_within <= 0 or ss_within == 0:
        return np.nan
    return float((ss_between / df_between) / (ss_within / df_within))


def manova_result(frame: pd.DataFrame) -> dict[str, object]:
    """Run a MANOVA-style overall mix test on ALR-transformed source composition."""

    composition = composition_matrix(frame)
    if composition.empty:
        return {"test": "manova_alr", "statistic": np.nan, "p_value": np.nan, "n": 0}
    transformed = alr_transform(composition)
    model_frame = pd.concat([transformed, frame.loc[composition.index, ["comparison_group", "tax_year", "form_type"]]], axis=1).dropna()
    if model_frame["comparison_group"].nunique() < 2 or len(model_frame) < 10:
        return {"test": "manova_alr", "statistic": np.nan, "p_value": np.nan, "n": len(model_frame)}
    outcomes = " + ".join(transformed.columns)
    rhs_terms = ["comparison_group"]
    if model_frame["tax_year"].nunique(dropna=True) > 1:
        rhs_terms.append("C(tax_year)")
    if model_frame["form_type"].nunique(dropna=True) > 1:
        rhs_terms.append("C(form_type)")
    formula = f"{outcomes} ~ {' + '.join(rhs_terms)}"
    try:
        result = MANOVA.from_formula(formula, data=model_frame).mv_test()
        stat_table = result.results["comparison_group"]["stat"]
        pillai_row = stat_table.loc["Pillai's trace"]
        return {"test": "manova_alr_pillai", "statistic": float(pillai_row["Value"]), "p_value": float(pillai_row["Pr > F"]), "n": len(model_frame)}
    except Exception as exc:  # pragma: no cover - defensive for singular real-data designs
        return {"test": "manova_alr_pillai", "statistic": np.nan, "p_value": np.nan, "n": len(model_frame), "error": str(exc)}


def run_multivariate_tests(frame: pd.DataFrame, *, label: str) -> pd.DataFrame:
    """Run full revenue-mix tests for one analysis frame."""

    info(f"Running compositional and multivariate tests for: {label}")
    rows = [permanova_two_group(frame), manova_result(frame)]
    for row in rows:
        row["analysis_frame"] = label
    results = pd.DataFrame(rows)
    if not results.empty:
        results["p_value_fdr_bh"] = fdr_bh(results["p_value"])
    return results


def concentration_metrics(frame: pd.DataFrame, group_columns: list[str]) -> pd.DataFrame:
    """Calculate revenue concentration metrics by region or comparison group."""

    info(f"Computing concentration metrics by {', '.join(group_columns)}.")
    rows: list[dict[str, object]] = []
    for keys, group in frame.groupby(group_columns, dropna=False):
        if not isinstance(keys, tuple):
            keys = (keys,)
        base = dict(zip(group_columns, keys, strict=False))
        revenue_by_ein = group.groupby("ein", dropna=False)["total_revenue"].sum().clip(lower=0)
        total = revenue_by_ein.sum()
        top5 = revenue_by_ein.sort_values(ascending=False).head(5).sum()
        shares = revenue_by_ein / total if total else revenue_by_ein * np.nan
        rows.append(
            {
                **base,
                "organization_count": int(revenue_by_ein.size),
                "total_revenue": float(total),
                "gini_total_revenue": gini(revenue_by_ein.to_numpy(dtype=float)),
                "hhi_total_revenue": float(np.sum(shares**2)) if total else np.nan,
                "top5_revenue_share": float(top5 / total) if total else np.nan,
            }
        )
    return pd.DataFrame(rows)


def gini(values: np.ndarray) -> float:
    """Gini coefficient for nonnegative values."""

    values = values[~np.isnan(values)]
    values = values[values >= 0]
    if values.size == 0:
        return np.nan
    if np.all(values == 0):
        return 0.0
    sorted_values = np.sort(values)
    n = len(sorted_values)
    cumulative = np.cumsum(sorted_values)
    return float((n + 1 - 2 * np.sum(cumulative) / cumulative[-1]) / n)


def negative_residual_diagnostics(frame: pd.DataFrame) -> pd.DataFrame:
    """Return rows where derived residuals or shares look implausible."""

    info("Collecting negative residual and over-100-percent share diagnostics.")
    columns = [
        "ein",
        "tax_year",
        "form_type",
        "region_label",
        "comparison_group",
        "total_revenue",
        "program_service_revenue",
        "total_contributions",
        "residual_other_revenue",
        "source_share_sum",
        "negative_residual_flag",
        "share_over_100_flag",
    ]
    return frame.loc[frame["negative_residual_flag"] | frame["share_over_100_flag"], columns].copy()


def load_core_sensitivity(data_root: Path) -> pd.DataFrame | None:
    """Load the NCCS Core sensitivity artifact if it is available."""

    core_path = data_root / CORE_ANALYSIS_RELATIVE_PATH
    info(f"Checking NCCS Core sensitivity file: {core_path}")
    if not core_path.exists():
        info("NCCS Core sensitivity file is not available; skipping Core sensitivity.")
        return None
    frame = pd.read_parquet(core_path)
    info(f"Loaded NCCS Core rows={len(frame):,}, columns={len(frame.columns):,}")
    out = pd.DataFrame(index=frame.index)
    out["source"] = "nccs_core_2022"
    out["ein"] = first_existing_column(frame, ["ein", "harm_ein"]).astype(str).str.zfill(9)
    out["tax_year"] = first_existing_column(frame, ["tax_year", "harm_tax_year"]).astype(str)
    out["form_type"] = first_existing_column(frame, ["form_type", "harm_filing_form"]).astype(str).str.upper().str.strip()
    out["region"] = first_existing_column(frame, ["region", "harm_region"]).astype(str).str.strip()
    out["region_label"] = out["region"].map(REGION_LABELS).fillna(out["region"])
    out["comparison_group"] = out["region"].map(map_comparison_group)
    out["total_revenue"] = to_numeric(first_existing_column(frame, ["analysis_total_revenue_amount"]))
    out["program_service_revenue"] = to_numeric(first_existing_column(frame, ["analysis_program_service_revenue_amount", "analysis_program_service_revenue_candidate_amount"]))
    out["total_contributions"] = to_numeric(
        first_existing_column(
            frame,
            ["analysis_total_contributions_amount", "analysis_calculated_total_contributions_amount", "analysis_contribution_candidate_amount"],
        )
    )
    out = out.loc[out["tax_year"].eq("2022") & out["total_revenue"].notna() & out["total_revenue"].gt(0)].copy()
    for column in ["program_service_revenue", "total_contributions"]:
        out[f"{column}_share"] = out[column] / out["total_revenue"]
    info(f"NCCS Core sensitivity rows after 2022 positive-revenue filter: {len(out):,}")
    return out.reset_index(drop=True)


def write_table(frame: pd.DataFrame, path: Path) -> None:
    """Write a CSV table with a progress message."""

    frame.to_csv(path, index=False)
    info(f"Saved table: {path} ({len(frame):,} rows)")


def write_parquet(frame: pd.DataFrame, path: Path) -> None:
    """Write a parquet file with a progress message."""

    frame.to_parquet(path, index=False)
    info(f"Saved parquet: {path} ({len(frame):,} rows)")


def save_stacked_bar(mix: pd.DataFrame, index_column: str, output_path: Path, title: str) -> None:
    """
    Save a normalized 100-percent stacked bar chart.

    The plotted value is `normalized_mix_share`, not the raw share of total
    revenue, because the component fields can overlap in the source data.
    """

    value_column = "normalized_mix_share" if "normalized_mix_share" in mix.columns else "share"
    pivot = mix.pivot_table(index=index_column, columns="component", values=value_column, aggfunc="sum").fillna(0)
    pivot = pivot[[component for component in SOURCE_COMPONENTS if component in pivot.columns]]
    ax = pivot.plot(kind="bar", stacked=True, figsize=(11, 6), colormap="tab20")
    ax.set_title(title)
    ax.set_ylabel("Normalized component share")
    ax.set_xlabel("")
    ax.legend(title="Revenue source", bbox_to_anchor=(1.02, 1), loc="upper left")
    ax.yaxis.set_major_formatter(lambda value, _: f"{value:.0%}")
    plt.tight_layout()
    plt.savefig(output_path, dpi=200)
    plt.close()
    info(f"Saved figure: {output_path}")


INDIVIDUAL_FOCUS_COMPONENTS = [
    "institutional_clear",
    "individual_narrow",
    "line_1f_mixed",
]

INDIVIDUAL_FOCUS_SHARES = [
    "institutional_clear_share_of_contrib",
    "individual_narrow_share_of_contrib",
    "individual_broad_share_of_contrib",
    "line_1f_mixed_share_of_contrib",
]


def prepare_individual_contributions_focus(frame: pd.DataFrame) -> pd.DataFrame:
    """Build the Form-990-only frame for the focused individual-vs-institutional comparison.

    The primary analysis divides every revenue segment by total revenue, which
    mixes "where do contributions come from" with "how big is program service
    revenue". For the focused individual question, we want a cleaner contrast,
    so this frame restricts to:

    1. Form 990 filers (the only ones with Line 1 sub-components).
    2. Rows with positive total contributions (so shares are interpretable).

    The analysis then computes channel amounts and four channel-share metrics
    expressed as shares of total contributions (not total revenue). The four
    channels are:

    - ``institutional_clear``: Line 1a + Line 1d + Line 1e (federated campaigns,
      related-org contributions, government grants). Unambiguously institutional.
    - ``individual_narrow``: Line 1b + Line 1c (membership dues + fundraising
      events). A *strict lower bound* on individual giving - these channels are
      predominantly but not exclusively individual.
    - ``individual_broad``: ``individual_narrow`` + Line 1f. An *upper bound*
      that treats the entire mixed Line 1f bucket as if it were individual
      giving. The truth lies between the two bounds; reporting both makes the
      decomposition uncertainty visible.
    - ``line_1f_mixed``: Line 1f alone (the deliberately-mixed bucket). Useful
      as a separate diagnostic.
    """

    info("Preparing Form-990-only individual-contributions focus frame.")
    is_form_990 = frame["form_type"].eq("990")
    has_contributions = frame["total_contributions"].notna() & frame["total_contributions"].gt(0)
    out = frame.loc[is_form_990 & has_contributions].copy()
    if out.empty:
        return out

    out["institutional_clear"] = (
        out["federated_campaigns"].fillna(0.0)
        + out["related_org_contributions"].fillna(0.0)
        + out["government_grants"].fillna(0.0)
    )
    out["individual_narrow"] = (
        out["membership_dues"].fillna(0.0)
        + out["fundraising_events_contributions"].fillna(0.0)
    )
    out["line_1f_mixed"] = out["allo_other_contributions_line_1f"].fillna(0.0)
    out["individual_broad"] = out["individual_narrow"] + out["line_1f_mixed"]

    denominator = out["total_contributions"].replace(0, np.nan)
    out["institutional_clear_share_of_contrib"] = out["institutional_clear"] / denominator
    out["individual_narrow_share_of_contrib"] = out["individual_narrow"] / denominator
    out["individual_broad_share_of_contrib"] = out["individual_broad"] / denominator
    out["line_1f_mixed_share_of_contrib"] = out["line_1f_mixed"] / denominator

    return out


def aggregate_individual_focus_mix(frame: pd.DataFrame, group_columns: list[str]) -> pd.DataFrame:
    """Aggregate the institutional / individual-narrow / Line-1f channel amounts.

    Returns long-format share-of-total-contributions for the three mutually
    exclusive channels (institutional_clear, individual_narrow, line_1f_mixed).
    The "individual_broad" upper-bound metric is *not* included here because
    individual_broad = individual_narrow + line_1f_mixed; including all three
    would double-count Line 1f.
    """

    if frame.empty:
        return pd.DataFrame(columns=group_columns + ["component", "amount", "share_of_contrib"])
    rows: list[dict[str, object]] = []
    for keys, group in frame.groupby(group_columns, dropna=False):
        if not isinstance(keys, tuple):
            keys = (keys,)
        total_contributions = float(group["total_contributions"].sum())
        for component in INDIVIDUAL_FOCUS_COMPONENTS:
            amount = float(group[component].fillna(0.0).sum())
            share = amount / total_contributions if total_contributions > 0 else np.nan
            row = {column: value for column, value in zip(group_columns, keys)}
            row.update({
                "component": component,
                "amount": amount,
                "share_of_contrib": share,
            })
            rows.append(row)
    return pd.DataFrame(rows)


def run_individual_focus_tests(frame: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Run the focused tests on the individual-contributions metrics.

    Returns ``(univariate_tests, bootstrap_cis)``. The univariate tests run
    Welch ANOVA for both group definitions (five regions and BH vs benchmark)
    plus permutation tests on the BH-vs-benchmark mean difference. The
    bootstrap CIs use the EIN cluster bootstrap.
    """

    if frame.empty:
        return pd.DataFrame(), pd.DataFrame()

    info("Running focused individual-contributions Welch ANOVA and permutation tests.")
    five_regions = run_univariate_tests(
        frame,
        INDIVIDUAL_FOCUS_SHARES,
        label="individual_focus_five_regions_form_990_only",
        group_column="region_label",
    )
    bh_vs_bm = run_univariate_tests(
        frame,
        INDIVIDUAL_FOCUS_SHARES,
        label="individual_focus_bh_vs_benchmark_form_990_only",
        group_column="comparison_group",
    )
    univariate = pd.concat([five_regions, bh_vs_bm], ignore_index=True)

    info("Running EIN cluster-bootstrap CIs for the focused individual-contributions metrics.")
    bootstrap_rows: list[dict[str, object]] = []
    for variable in INDIVIDUAL_FOCUS_SHARES:
        observed, lower, upper = bootstrap_ci_difference(frame, variable)
        bootstrap_rows.append({"variable": variable, "mean_difference": observed, "ci_lower": lower, "ci_upper": upper})
    bootstrap = pd.DataFrame(bootstrap_rows)
    return univariate, bootstrap


def save_individual_focus_chart(
    mix: pd.DataFrame,
    output_path: Path,
    title: str,
) -> None:
    """Render a 100% stacked bar chart of the three focused contribution channels.

    Uses share_of_contrib (each bar sums to 100% of total contributions for
    that group) so individuals-versus-institutional differences in
    *contribution mix* are visible without being diluted by program-service
    revenue.
    """

    if mix.empty:
        return
    pivot = mix.pivot(index="comparison_group", columns="component", values="share_of_contrib")
    pivot = pivot[[col for col in INDIVIDUAL_FOCUS_COMPONENTS if col in pivot.columns]]
    plt.figure(figsize=(10, 5))
    bottom = np.zeros(len(pivot.index))
    palette = sns.color_palette("Set2", n_colors=pivot.shape[1])
    for color, component in zip(palette, pivot.columns):
        values = pivot[component].fillna(0).to_numpy()
        plt.bar(pivot.index.astype(str), values, bottom=bottom, label=component, color=color)
        bottom = bottom + values
    plt.gca().yaxis.set_major_formatter(lambda value, _: f"{value:.0%}")
    plt.ylabel("Share of total contributions")
    plt.title(title)
    plt.legend(title="Channel", bbox_to_anchor=(1.02, 1.0), loc="upper left", borderaxespad=0)
    plt.tight_layout()
    plt.savefig(output_path, dpi=200, bbox_inches="tight")
    plt.close()
    info(f"Saved figure: {output_path}")


def save_individual_focus_year_chart(
    mix: pd.DataFrame,
    output_path: Path,
    title: str,
) -> None:
    """Stacked-bar chart of the three focused contribution channels by group and tax year."""

    if mix.empty:
        return
    work = mix.copy()
    work["group_year"] = (
        work["comparison_group"].astype(str) + " " + work["tax_year"].astype(str)
    )
    pivot = work.pivot(index="group_year", columns="component", values="share_of_contrib")
    pivot = pivot[[col for col in INDIVIDUAL_FOCUS_COMPONENTS if col in pivot.columns]]
    pivot = pivot.sort_index()
    plt.figure(figsize=(12, 5))
    bottom = np.zeros(len(pivot.index))
    palette = sns.color_palette("Set2", n_colors=pivot.shape[1])
    for color, component in zip(palette, pivot.columns):
        values = pivot[component].fillna(0).to_numpy()
        plt.bar(pivot.index.astype(str), values, bottom=bottom, label=component, color=color)
        bottom = bottom + values
    plt.gca().yaxis.set_major_formatter(lambda value, _: f"{value:.0%}")
    plt.ylabel("Share of total contributions")
    plt.title(title)
    plt.xticks(rotation=20, ha="right")
    plt.legend(title="Channel", bbox_to_anchor=(1.02, 1.0), loc="upper left", borderaxespad=0)
    plt.tight_layout()
    plt.savefig(output_path, dpi=200, bbox_inches="tight")
    plt.close()
    info(f"Saved figure: {output_path}")


def create_figures(frame: pd.DataFrame, tables: dict[str, pd.DataFrame], figures_dir: Path) -> None:
    """Create all planned figures from the prepared analysis frame."""

    info("Creating revenue-source visualizations.")
    sns.set_theme(style="whitegrid")

    save_stacked_bar(
        tables["mix_by_group"],
        "comparison_group",
        figures_dir / "stacked_revenue_mix_black_hills_vs_benchmark.png",
        "Normalized reported component mix: Black Hills vs pooled benchmarks",
    )

    by_year = tables["mix_by_group_year"].copy()
    by_year["group_year"] = by_year["comparison_group"] + " " + by_year["tax_year"].astype(str)
    save_stacked_bar(
        by_year,
        "group_year",
        figures_dir / "stacked_revenue_mix_by_year.png",
        "Normalized reported component mix by comparison group and tax year",
    )

    save_stacked_bar(
        tables["mix_by_region"],
        "region_label",
        figures_dir / "stacked_revenue_mix_by_region.png",
        "Normalized reported component mix by region",
    )

    by_form = tables["mix_by_group_form"].copy()
    by_form["group_form"] = by_form["comparison_group"] + " " + by_form["form_type"].astype(str)
    save_stacked_bar(
        by_form,
        "group_form",
        figures_dir / "stacked_revenue_mix_by_form_type.png",
        "Normalized reported component mix by comparison group and form type",
    )

    plot_frame = frame.melt(
        id_vars=["comparison_group"],
        value_vars=SHARE_COMPONENTS,
        var_name="source_share",
        value_name="share",
    ).dropna()
    plt.figure(figsize=(12, 6))
    sns.violinplot(data=plot_frame, x="source_share", y="share", hue="comparison_group", cut=0, inner="quartile")
    plt.xticks(rotation=25, ha="right")
    plt.gca().yaxis.set_major_formatter(lambda value, _: f"{value:.0%}")
    plt.title("Organization-level revenue-source shares")
    plt.tight_layout()
    output_path = figures_dir / "violin_source_shares_by_group.png"
    plt.savefig(output_path, dpi=200)
    plt.close()
    info(f"Saved figure: {output_path}")

    plt.figure(figsize=(10, 6))
    sns.histplot(data=frame, x="total_revenue", hue="comparison_group", log_scale=True, element="step", stat="density", common_norm=False)
    plt.title("Total revenue distribution, log scale")
    plt.tight_layout()
    output_path = figures_dir / "diagnostic_total_revenue_distribution.png"
    plt.savefig(output_path, dpi=200)
    plt.close()
    info(f"Saved figure: {output_path}")

    diagnostic = negative_residual_diagnostics(frame)
    plt.figure(figsize=(9, 5))
    if diagnostic.empty:
        plt.text(0.5, 0.5, "No negative residual or over-100-percent share rows detected", ha="center", va="center")
        plt.axis("off")
    else:
        sns.scatterplot(data=diagnostic, x="total_revenue", y="residual_other_revenue", hue="comparison_group")
        plt.xscale("log")
        plt.axhline(0, color="black", linewidth=1)
    plt.title("Residual revenue diagnostics")
    plt.tight_layout()
    output_path = figures_dir / "diagnostic_residual_revenue.png"
    plt.savefig(output_path, dpi=200)
    plt.close()
    info(f"Saved figure: {output_path}")

    concentration = tables["concentration_by_region"].sort_values("top5_revenue_share", ascending=False)
    plt.figure(figsize=(10, 5))
    sns.barplot(data=concentration, x="region_label", y="top5_revenue_share")
    plt.gca().yaxis.set_major_formatter(lambda value, _: f"{value:.0%}")
    plt.title("Top 5 organizations' share of total revenue by region")
    plt.tight_layout()
    output_path = figures_dir / "concentration_top5_revenue_share_by_region.png"
    plt.savefig(output_path, dpi=200)
    plt.close()
    info(f"Saved figure: {output_path}")

    heatmap_data = tables["mix_by_group_year"].pivot_table(
        index="component",
        columns=["comparison_group", "tax_year"],
        values="share",
        aggfunc="sum",
    )
    plt.figure(figsize=(12, 5))
    sns.heatmap(heatmap_data, annot=True, fmt=".1%", cmap="vlag", center=0)
    plt.title("Reported component share of total revenue by group and year")
    plt.tight_layout()
    output_path = figures_dir / "heatmap_revenue_source_share_by_year.png"
    plt.savefig(output_path, dpi=200)
    plt.close()
    info(f"Saved figure: {output_path}")


def build_markdown_summary(
    frame: pd.DataFrame,
    stats_results: pd.DataFrame,
    multivariate_results: pd.DataFrame,
    by_year_results: pd.DataFrame,
    concentration: pd.DataFrame,
    output_path: Path,
) -> None:
    """Write a concise executive summary for the analysis run."""

    info("Writing Markdown methods and interpretation summary.")
    lines: list[str] = [
        "# Revenue Sources: Black Hills vs Benchmark Regions",
        "",
        "## Methods",
        "",
        "- Primary source: GivingTuesday 990 basic all-forms analysis variables.",
        "- Universe: Forms 990, 990-EZ, and 990-PF with positive total revenue in the selected years, excluding hospitals, universities, and political organizations for client-peer comparability.",
        "- Headline statistical tests: five-region Welch ANOVA on revenue-source shares (Section 3 convention), plus Black Hills versus pooled benchmarks as follow-up.",
        "- Program service revenue is interpreted for Forms 990 and 990-EZ; 990-PF is kept missing for that component.",
        "- Total contributions use GT `analysis_total_contributions_amount` (990 Line 1h via TOTACASHCONT; 990-EZ Part I Line 1 via CONGIFGRAETC; 990-PF Part I Line 1 via STREACGRTOIN).",
        "- For Form 990 filers, the contributions total is decomposed into Line 1 sub-channels: government grants (1e), other institutional channels (1a federated + 1d related-org), likely-individual channels (1b membership dues + 1c fundraising events), and the unidentifiable Line 1f bucket that lumps individual gifts with private foundation grants, DAF distributions, corporate gifts, and bequests.",
        "- 990-EZ and 990-PF do not separately report Line 1 sub-components, so for those filers the entire reported contributions total is routed into the unidentifiable mixed bucket.",
        "- Reported segment shares divide harmonized segment amounts by total revenue. Residual captures remaining revenue.",
        "- Statistical tests include ANOVA, Welch ANOVA, rank tests, permutation tests, EIN-clustered OLS, EIN-clustered logistic presence models, compositional PERMANOVA-style tests, MANOVA-style tests, EIN-cluster bootstrap confidence intervals, a one-row-per-EIN independence sensitivity, and concentration metrics.",
        "- Year-by-year hypothesis tests are reported separately so 2022, 2023, and 2024 can be interpreted without relying only on pooled results.",
        "",
        "## Coverage",
        "",
        f"- Analytic rows: {len(frame):,}",
        f"- Unique EINs: {frame['ein'].nunique():,}",
        f"- Excluded hospital/university/political org rows: {int(frame.attrs.get('excluded_org_type_row_count', 0)):,}",
        f"- Years: {', '.join(sorted(frame['tax_year'].astype(str).unique()))}",
        f"- Regions: {', '.join(sorted(frame['region_label'].astype(str).unique()))}",
        "",
        "## Headline Statistical Signals",
        "",
    ]

    for variable in SHARE_COMPONENTS:
        subset = stats_results.loc[
            stats_results["variable"].eq(variable)
            & stats_results["test"].eq("welch_anova")
            & stats_results["analysis_frame"].eq("primary_all_years_five_regions")
        ]
        if subset.empty:
            continue
        row = subset.iloc[0]
        bh_mean = frame.loc[frame["comparison_group"].eq("Black Hills"), variable].mean()
        bm_mean = frame.loc[frame["comparison_group"].eq("Benchmark"), variable].mean()
        direction = "higher" if bh_mean > bm_mean else "lower"
        lines.append(
            f"- `{variable}`: Black Hills mean is {direction} than benchmarks "
            f"({bh_mean:.1%} vs {bm_mean:.1%}); five-region Welch p={row['p_value']:.4g}, FDR p={row.get('p_value_fdr_bh', np.nan):.4g}."
        )

    if not multivariate_results.empty:
        lines.extend(["", "## Overall Revenue-Mix Tests", ""])
        for row in multivariate_results.itertuples(index=False):
            lines.append(f"- `{row.test}`: statistic={row.statistic:.4g}, p={row.p_value:.4g}, n={row.n}.")

    if not by_year_results.empty:
        lines.extend(["", "## Year-by-Year Tests", ""])
        year_rows = by_year_results.loc[
            by_year_results["test"].eq("welch_anova")
            & by_year_results["variable"].isin(SHARE_COMPONENTS)
            & by_year_results["analysis_frame"].str.endswith("_five_regions", na=False)
        ].copy()
        for row in year_rows.itertuples(index=False):
            year_label = (
                str(row.analysis_frame).replace("year_", "").replace("_five_regions", "")
            )
            lines.append(
                f"- {year_label} `{row.variable}`: Welch p={row.p_value:.4g}, "
                f"FDR p={getattr(row, 'p_value_fdr_bh', np.nan):.4g}, n={row.n}."
            )

    lines.extend(["", "## Concentration", ""])
    for row in concentration.itertuples(index=False):
        label = getattr(row, "comparison_group", "")
        lines.append(
            f"- {label}: Gini={row.gini_total_revenue:.3f}, HHI={row.hhi_total_revenue:.3f}, "
            f"top-5 revenue share={row.top5_revenue_share:.1%}."
        )

    lines.extend(
        [
            "",
            "## Caveats",
            "",
            "- 2024 filing data may be incomplete, so the script also writes a 2022-2023 sensitivity frame.",
            "- Residual other revenue is derived from available components and should be interpreted as an accounting diagnostic, not a source-reported line item.",
            "- Some rows still show diagnostic overlap when upstream totals are missing or the institutional aggregate exceeds reported total contributions.",
            "- Form 990 Line 1 sub-channels are exposed individually as diagnostics; the institutional headline segments use Lines 1a + 1d (`other_institutional_contributions`) and Line 1e (`government_grants_received`).",
            "- Form 990 Line 1f (`mixed_other_contributions`) cannot be split into individual versus institutional giving without Schedule B, which is not in the GT basic datamart. 990-EZ and 990-PF route their entire contributions total into this bucket because those forms do not separately report Line 1 sub-components.",
        ]
    )

    output_path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    info(f"Saved Markdown summary: {output_path}")


def _percent(value: object) -> str:
    """Format a numeric value as a percentage for report tables."""

    if pd.isna(value):
        return ""
    return f"{float(value):.1%}"


def _number(value: object, digits: int = 3) -> str:
    """Format a numeric value for report tables."""

    if pd.isna(value):
        return ""
    return f"{float(value):.{digits}g}"


def _markdown_table(frame: pd.DataFrame, columns: list[str], rename: dict[str, str] | None = None) -> str:
    """Return a compact Markdown table without adding a heavyweight dependency."""

    if frame.empty:
        return "_No rows available._"
    work = frame.loc[:, columns].copy()
    if rename:
        work = work.rename(columns=rename)
    headers = list(work.columns)
    lines = [
        "| " + " | ".join(headers) + " |",
        "| " + " | ".join(["---"] * len(headers)) + " |",
    ]
    for row in work.itertuples(index=False):
        lines.append("| " + " | ".join("" if pd.isna(value) else str(value) for value in row) + " |")
    return "\n".join(lines)


def collect_results_artifacts(paths: AnalysisPaths, table_names: list[str], figure_names: list[str]) -> None:
    """
    Copy the most useful tables and charts into a dedicated results folder.

    The main analysis directory keeps all intermediate outputs. The `results`
    folder is the user-facing bundle with the report, selected CSV results, and
    graph files referenced from the Markdown report.
    """

    info("Collecting selected tables and charts into the results folder.")
    for table_name in table_names:
        source = paths.tables_dir / table_name
        if source.exists():
            shutil.copy2(source, paths.results_tables_dir / table_name)
            info(f"Copied result table: {paths.results_tables_dir / table_name}")
    for figure_name in figure_names:
        source = paths.figures_dir / figure_name
        if source.exists():
            shutil.copy2(source, paths.results_figures_dir / figure_name)
            info(f"Copied result figure: {paths.results_figures_dir / figure_name}")


def write_detailed_results_report(
    *,
    analysis: pd.DataFrame,
    tables: dict[str, pd.DataFrame],
    univariate_results: pd.DataFrame,
    regression_results: pd.DataFrame,
    multivariate_results: pd.DataFrame,
    by_year_univariate_results: pd.DataFrame,
    by_year_multivariate_results: pd.DataFrame,
    bootstrap_results: pd.DataFrame,
    individual_focus_frame: pd.DataFrame,
    individual_focus_mix: pd.DataFrame,
    individual_focus_univariate: pd.DataFrame,
    individual_focus_bootstrap: pd.DataFrame,
    individual_focus_by_year_univariate: pd.DataFrame,
    individual_focus_by_year_bootstrap: pd.DataFrame,
    paths: AnalysisPaths,
) -> None:
    """
    Write the final user-facing report with embedded charts.

    The report intentionally repeats the headline findings in plain language and
    then links to the exact charts/tables in the same `results` folder. This is
    the file to open first when reviewing the completed run.
    """

    info("Writing detailed Markdown report with embedded charts.")
    report_path = paths.results_dir / "revenue_sources_black_hills_results.md"

    mix = tables["mix_by_group"].copy()
    raw_mix = mix.copy()
    reported_share_sum = (
        raw_mix.groupby("comparison_group", dropna=False)["share"]
        .sum()
        .reset_index(name="reported_component_share_sum")
    )
    reported_share_sum["reported_component_share_sum"] = reported_share_sum["reported_component_share_sum"].map(_percent)
    mix["share"] = mix["share"].map(_percent)
    if "normalized_mix_share" in mix.columns:
        mix["normalized_mix_share"] = mix["normalized_mix_share"].map(_percent)
    else:
        mix["normalized_mix_share"] = ""
    mix["amount"] = mix["amount"].map(lambda value: f"${float(value):,.0f}" if not pd.isna(value) else "")
    mix_report = mix[["comparison_group", "component", "amount", "share", "normalized_mix_share"]].rename(
        columns={
            "comparison_group": "Group",
            "component": "Revenue source",
            "amount": "Amount",
            "share": "Reported share of total revenue",
            "normalized_mix_share": "Normalized chart share",
        }
    )

    primary_five_raw = univariate_results.loc[
        univariate_results["analysis_frame"].eq("primary_all_years_five_regions")
        & univariate_results["test"].eq("welch_anova")
        & univariate_results["variable"].isin(SHARE_COMPONENTS)
    ].copy()
    primary_bm_raw = univariate_results.loc[
        univariate_results["analysis_frame"].eq("primary_all_years_bh_vs_benchmark")
        & univariate_results["test"].isin(["welch_anova", "permutation_mean_diff"])
        & univariate_results["variable"].isin(SHARE_COMPONENTS)
    ].copy()

    # Level-variable Welch ANOVA. The Section 3 Q9 plan lists "Total revenue",
    # "Program service revenue", "Total contributions", and "Other contributions
    # (foundation grants etc.)" as variables, so we surface the five-region
    # ANOVA on the log1p-transformed level fields here in addition to the share
    # comparisons above.
    level_variables_for_report = [
        "log1p_total_revenue",
        "log1p_total_contributions",
        "log1p_program_service_revenue",
        "log1p_government_grants_received",
        "log1p_other_institutional_contributions",
        "log1p_individual_likely_contributions",
        "log1p_mixed_other_contributions",
        "log1p_calculated_institutional_contributions_total",
    ]
    primary_levels_raw = univariate_results.loc[
        univariate_results["analysis_frame"].eq("primary_all_years_five_regions")
        & univariate_results["test"].eq("welch_anova")
        & univariate_results["variable"].isin(level_variables_for_report)
    ].copy()

    # Independence sensitivity: one-row-per-EIN re-run of the share-variable
    # Welch ANOVAs. If the headline pattern holds when each EIN contributes a
    # single observation, the repeated-filer structure is not driving the
    # primary p-values.
    one_per_ein_raw = univariate_results.loc[
        univariate_results["analysis_frame"].isin(
            ["sensitivity_one_row_per_ein_five_regions", "sensitivity_one_row_per_ein_bh_vs_benchmark"]
        )
        & univariate_results["test"].isin(["welch_anova", "permutation_mean_diff"])
        & univariate_results["variable"].isin(SHARE_COMPONENTS)
    ].copy()

    full_universe_raw = univariate_results.loc[
        univariate_results["analysis_frame"].isin(
            ["sensitivity_including_outliers_five_regions", "sensitivity_including_outliers_bh_vs_benchmark"]
        )
        & univariate_results["test"].isin(["welch_anova", "permutation_mean_diff"])
        & univariate_results["variable"].isin(SHARE_COMPONENTS)
    ].copy()

    def _format_univariate_table(frame: pd.DataFrame) -> pd.DataFrame:
        if frame.empty:
            return frame
        out = frame.copy()
        out["p_value"] = out["p_value"].map(lambda value: _number(value, 4))
        out["p_value_fdr_bh"] = out["p_value_fdr_bh"].map(lambda value: _number(value, 4))
        out["statistic"] = out["statistic"].map(lambda value: _number(value, 4))
        return out

    primary_five = _format_univariate_table(primary_five_raw)
    primary_bm = _format_univariate_table(primary_bm_raw)
    primary_levels = _format_univariate_table(primary_levels_raw)
    one_per_ein = _format_univariate_table(one_per_ein_raw)
    if not one_per_ein.empty:
        one_per_ein["analysis_frame"] = one_per_ein["analysis_frame"].str.replace(
            "sensitivity_one_row_per_ein_", "", regex=False
        )
    full_universe = _format_univariate_table(full_universe_raw)
    if not full_universe.empty:
        full_universe["analysis_frame"] = full_universe["analysis_frame"].str.replace(
            "sensitivity_including_outliers_", "", regex=False
        )

    # Individual contributions focus: format the share-of-contributions mix
    # table and the share-of-contributions Welch ANOVA table for the report.
    if individual_focus_mix is None or individual_focus_mix.empty:
        focus_mix_table = pd.DataFrame()
        focus_n_orgs = 0
        focus_n_eins = 0
    else:
        focus_mix_table = individual_focus_mix.copy()
        focus_mix_table["amount"] = focus_mix_table["amount"].map(
            lambda value: f"${float(value):,.0f}" if not pd.isna(value) else ""
        )
        focus_mix_table["share_of_contrib"] = focus_mix_table["share_of_contrib"].map(_percent)
        focus_mix_table = focus_mix_table[["comparison_group", "component", "amount", "share_of_contrib"]].rename(
            columns={
                "comparison_group": "Group",
                "component": "Channel",
                "amount": "Amount",
                "share_of_contrib": "Share of total contributions",
            }
        )
        focus_n_orgs = int(individual_focus_frame["ein"].nunique()) if individual_focus_frame is not None else 0
        focus_n_eins = focus_n_orgs

    if individual_focus_univariate is None or individual_focus_univariate.empty:
        focus_anova = pd.DataFrame()
    else:
        focus_anova_raw = individual_focus_univariate.loc[
            individual_focus_univariate["test"].isin(["welch_anova", "permutation_mean_diff"])
            & individual_focus_univariate["variable"].isin(INDIVIDUAL_FOCUS_SHARES)
        ].copy()
        focus_anova = _format_univariate_table(focus_anova_raw)
        if not focus_anova.empty:
            focus_anova["analysis_frame"] = focus_anova["analysis_frame"].str.replace(
                "individual_focus_", "", regex=False
            ).str.replace("_form_990_only", "", regex=False)

    if individual_focus_bootstrap is None or individual_focus_bootstrap.empty:
        focus_bootstrap = pd.DataFrame()
    else:
        focus_bootstrap = individual_focus_bootstrap.copy()
        for column in ["mean_difference", "ci_lower", "ci_upper"]:
            focus_bootstrap[column] = focus_bootstrap[column].map(_percent)

    # Year-by-year focused tables for the report. Filter to the Welch ANOVA
    # row for each share metric so the table mirrors the existing year-by-year
    # primary table format.
    if individual_focus_by_year_univariate is None or individual_focus_by_year_univariate.empty:
        focus_by_year = pd.DataFrame()
    else:
        focus_by_year_raw = individual_focus_by_year_univariate.loc[
            individual_focus_by_year_univariate["test"].eq("welch_anova")
            & individual_focus_by_year_univariate["variable"].isin(INDIVIDUAL_FOCUS_SHARES)
            & individual_focus_by_year_univariate["analysis_frame"].str.endswith(
                "_bh_vs_benchmark", na=False
            )
        ].copy()
        focus_by_year = _format_univariate_table(focus_by_year_raw)
        if not focus_by_year.empty:
            focus_by_year["tax_year"] = (
                focus_by_year["analysis_frame"]
                .str.replace("individual_focus_year_", "", regex=False)
                .str.replace("_bh_vs_benchmark", "", regex=False)
            )

    if individual_focus_by_year_bootstrap is None or individual_focus_by_year_bootstrap.empty:
        focus_bootstrap_by_year = pd.DataFrame()
    else:
        focus_bootstrap_by_year = individual_focus_by_year_bootstrap.copy()
        for column in ["mean_difference", "ci_lower", "ci_upper"]:
            focus_bootstrap_by_year[column] = focus_bootstrap_by_year[column].map(_percent)

    bootstrap = bootstrap_results.copy()
    for column in ["mean_difference", "ci_lower", "ci_upper"]:
        bootstrap[column] = bootstrap[column].map(_percent)

    if {"test", "variable"}.issubset(by_year_univariate_results.columns):
        by_year = by_year_univariate_results.loc[
            by_year_univariate_results["test"].eq("welch_anova")
            & by_year_univariate_results["variable"].isin(SHARE_COMPONENTS)
            & by_year_univariate_results["analysis_frame"].str.endswith("_five_regions", na=False)
        ].copy()
    else:
        by_year = pd.DataFrame()
    if not by_year.empty:
        by_year["tax_year"] = (
            by_year["analysis_frame"].str.replace(r"^year_", "", regex=True).str.replace("_five_regions$", "", regex=True)
        )
        by_year["p_value"] = by_year["p_value"].map(lambda value: _number(value, 4))
        by_year["p_value_fdr_bh"] = by_year["p_value_fdr_bh"].map(lambda value: _number(value, 4))
        by_year["statistic"] = by_year["statistic"].map(lambda value: _number(value, 4))

    multivariate_parts = [
        frame
        for frame in [multivariate_results, by_year_multivariate_results]
        if not frame.empty and {"test", "p_value", "statistic"}.issubset(frame.columns)
    ]
    multivariate = pd.concat(multivariate_parts, ignore_index=True) if multivariate_parts else pd.DataFrame()
    if not multivariate.empty:
        multivariate["p_value"] = multivariate["p_value"].map(lambda value: _number(value, 4))
        multivariate["p_value_fdr_bh"] = multivariate["p_value_fdr_bh"].map(lambda value: _number(value, 4))
        multivariate["statistic"] = multivariate["statistic"].map(lambda value: _number(value, 4))

    concentration = tables["concentration_by_group"].copy()
    concentration["total_revenue"] = concentration["total_revenue"].map(lambda value: f"${float(value):,.0f}")
    concentration["gini_total_revenue"] = concentration["gini_total_revenue"].map(lambda value: _number(value, 3))
    concentration["hhi_total_revenue"] = concentration["hhi_total_revenue"].map(lambda value: _number(value, 3))
    concentration["top5_revenue_share"] = concentration["top5_revenue_share"].map(_percent)

    overlap = tables.get("component_overlap_by_group", pd.DataFrame()).copy()
    if not overlap.empty:
        for column in ["negative_residual_rate", "share_over_100_rate", "mean_source_share_sum", "median_source_share_sum", "max_source_share_sum"]:
            if column in overlap.columns:
                overlap[column] = overlap[column].map(_percent)

    if {"term", "estimate", "p_value"}.issubset(regression_results.columns):
        regression = regression_results.loc[regression_results["term"].eq("is_black_hills")].copy()
    else:
        regression = pd.DataFrame()
    if not regression.empty:
        regression["estimate"] = regression["estimate"].map(lambda value: _number(value, 4))
        regression["std_error"] = regression["std_error"].map(lambda value: _number(value, 4))
        regression["p_value"] = regression["p_value"].map(lambda value: _number(value, 4))
        regression["p_value_fdr_bh"] = regression["p_value_fdr_bh"].map(lambda value: _number(value, 4))
    warning_count = 0
    if "warning" in regression_results.columns:
        warning_count = int(regression_results["warning"].fillna("").astype(str).str.len().gt(0).sum())

    lines = [
        "# Revenue Sources Analysis Results",
        "",
        "## Question",
        "",
        "Is there a difference in revenue sources between Black Hills nonprofit organizations and nonprofit organizations in the benchmark regions?",
        "",
        "## Analysis Performed",
        "",
        "The analysis uses the GivingTuesday Form 990 basic all-forms analysis dataset as the primary source because it covers Form 990, Form 990-EZ, and Form 990-PF and contains the requested revenue-source fields. The primary comparison excludes hospitals, universities, and political organizations so the benchmark regions better reflect the client-peer universe; the full Form 990/990-EZ/990-PF universe is retained as a sensitivity check. NCCS Core is used separately as a 2022 sensitivity check where comparable fields exist.",
        "",
        "The script filtered to positive total revenue, valid region/year/form records, and the comparable peer universe, then derived six mutually exclusive revenue-source segments aligned with Section 3 Q9. For Form 990 filers (the bulk of the analytical sample) the contribution side of total revenue is decomposed into the IRS Part VIII Line 1 sub-channels:",
        "",
        "1. **program_service_revenue** - Form 990 / 990-EZ Line 2g program service revenue (Form 990-PF stays missing; that form lacks an equivalent concept).",
        "2. **government_grants_received** - Form 990 Line 1e (`GOVERNGRANTS`). The only Line 1 sub-component the IRS labels unambiguously as institutional.",
        "3. **other_institutional_contributions** - Form 990 Line 1a + Line 1d (`FEDERACAMPAI` + `RELATEORGANI`). Federated campaigns (e.g. United Way) and related-organization transfers are also institutional channels.",
        "4. **individual_likely_contributions** - Form 990 Line 1b + Line 1c (`MEMBERDUESUE` + `FUNDRAEVENTS`). Membership dues and fundraising-event proceeds are predominantly but not exclusively individual.",
        "5. **mixed_other_contributions** - Form 990 Line 1f (`ALLOOTHECONT`). The IRS lumps individual gifts together with private foundation grants, donor-advised fund distributions, corporate gifts, and bequests in this single line and does not separate the donor types. For 990-EZ and 990-PF filers, which do not separately report Line 1 sub-components, the entire reported Line 1 / Part I Line 1 contributions total is routed into this bucket so the segments still partition reported revenue.",
        "6. **residual_other_revenue** - total revenue minus the five segments above; clipped at zero for plotting.",
        "",
        "Total contributions used as the contribution-side denominator are the GT canonical field `analysis_total_contributions_amount` (Line 1h via `TOTACASHCONT` for 990; Part I Line 1 via `CONGIFGRAETC` for 990-EZ; Part I Line 1 via `STREACGRTOIN` for 990-PF). The institutional aggregate (`analysis_calculated_grants_total_amount` = Line 1a + 1d + 1e on Form 990) is exposed for diagnostics; earlier versions of that aggregate accidentally included Form 990 Part IX grants paid out (`FOREGRANTOTA`, `GRANTOORORGA`) and have been corrected.",
        "",
        "**Interpretation caveat for the client question.** This decomposition is the most informative split the IRS basic 990 family can support for distinguishing individual donors from other organizations. It cannot fully isolate individual giving because Line 1f mixes individuals, foundations, DAFs, corporates, and bequests, and Form 990-EZ / 990-PF expose only a single contributions total. Reading `government_grants_received` and `other_institutional_contributions` as institutional, and `individual_likely_contributions` as the closest proxy for individual giving, is defensible; reading `mixed_other_contributions` as either pure-individual or pure-institutional is not.",
        "",
        "The statistical analysis includes descriptive summaries, ANOVA, Welch ANOVA, rank tests, permutation tests, FDR-adjusted p-values, effect sizes, OLS models with EIN-clustered standard errors, EIN-clustered logistic presence models, compositional PERMANOVA-style tests, MANOVA-style tests, year-by-year tests, form-type sensitivity tests, revenue-size sensitivity tests, a full-universe sensitivity that includes the excluded organization types, a one-row-per-EIN independence sensitivity, EIN-cluster bootstrap confidence intervals, and concentration metrics. Where the same nonprofit appears in multiple tax years, inference uses cluster-robust adjustments rather than treating org-years as independent.",
        "",
        "## Coverage",
        "",
        f"- Analytic organization-year rows: {len(analysis):,}",
        f"- Unique EINs: {analysis['ein'].nunique():,}",
        f"- Excluded hospital/university/political org rows: {int(analysis.attrs.get('excluded_org_type_row_count', 0)):,} "
        f"({int(analysis.attrs.get('excluded_org_type_ein_count', 0)):,} EINs) from the full valid universe of "
        f"{int(analysis.attrs.get('full_universe_row_count', len(analysis))):,} rows",
        f"- Tax years: {', '.join(sorted(analysis['tax_year'].astype(str).unique()))}",
        f"- Regions: {', '.join(sorted(analysis['region_label'].astype(str).unique()))}",
        f"- Rows flagged for negative residual or over-100-percent source shares: {len(tables['negative_residual_diagnostics']):,}",
        "",
        "## Headline Findings",
        "",
        "The clearest headline comparison is the five-region Welch ANOVA on organization-level revenue-source shares (Section 3 convention). Follow-up Black Hills versus pooled benchmark tests (including permutation tests on the mean difference) appear in the secondary table below.",
        "",
        "The aggregate-dollar view can differ from organization-level tests because benchmark-region revenue is often concentrated among a small number of very large organizations. Read stacked bars and concentration diagnostics together.",
        "",
        "## Aggregate Reported Component Mix",
        "",
        "The table below shows both the reported component share of total revenue and the normalized share used in the stacked charts. With the Q9 donor-channel decomposition, reported segment shares should sum to roughly 100 percent within rounding; larger deviations indicate upstream overlap or missing contribution totals on specific rows.",
        "",
        _markdown_table(mix_report, list(mix_report.columns)),
        "",
        "Reported component share sums by group:",
        "",
        _markdown_table(
            reported_share_sum,
            ["comparison_group", "reported_component_share_sum"],
            {
                "comparison_group": "Group",
                "reported_component_share_sum": "Reported component shares summed",
            },
        ),
        "",
        "![Normalized reported component mix: Black Hills vs benchmarks](figures/stacked_revenue_mix_black_hills_vs_benchmark.png)",
        "",
        "![Normalized reported component mix by year](figures/stacked_revenue_mix_by_year.png)",
        "",
        "![Normalized reported component mix by region](figures/stacked_revenue_mix_by_region.png)",
        "",
        "![Normalized reported component mix by form type](figures/stacked_revenue_mix_by_form_type.png)",
        "",
        "## Primary Statistical Tests (five regions, Welch ANOVA)",
        "",
        _markdown_table(
            primary_five,
            ["test", "variable", "statistic", "p_value", "p_value_fdr_bh", "n"],
            {
                "test": "Test",
                "variable": "Variable",
                "statistic": "Statistic",
                "p_value": "P-value",
                "p_value_fdr_bh": "FDR p-value",
                "n": "N",
            },
        ),
        "",
        "## Black Hills vs pooled benchmarks (follow-up)",
        "",
        _markdown_table(
            primary_bm,
            ["test", "variable", "statistic", "p_value", "p_value_fdr_bh", "n"],
            {
                "test": "Test",
                "variable": "Variable",
                "statistic": "Statistic",
                "p_value": "P-value",
                "p_value_fdr_bh": "FDR p-value",
                "n": "N",
            },
        ),
        "",
        "## Level-Variable Comparisons (five regions, Welch ANOVA on log1p)",
        "",
        "The Section 3 Q9 plan lists Total revenue, Program service revenue, Total contributions, and "
        "Other contributions (foundation grants etc.) as variables of interest. The table below tests "
        "those variables on the level (log1p-transformed) in addition to the share comparisons above. "
        "Level differences reflect organization-size differences across regions; share differences "
        "reflect revenue-mix differences. Both views are needed for a complete answer.",
        "",
        _markdown_table(
            primary_levels,
            ["test", "variable", "statistic", "p_value", "p_value_fdr_bh", "n"],
            {
                "test": "Test",
                "variable": "Variable",
                "statistic": "Statistic",
                "p_value": "P-value",
                "p_value_fdr_bh": "FDR p-value",
                "n": "N",
            },
        ),
        "",
        "## Independence Sensitivity (one row per EIN, most recent year)",
        "",
        "Each EIN contributes at most three filings (2022-2024). The pooled tests above treat each "
        "org-year as an independent observation; this sensitivity restricts the analysis to one row "
        "per EIN (most recent reported year) so that the test's independence assumption is satisfied "
        "exactly. If the direction and significance of the share comparisons survive this restriction, "
        "repeated filings are not driving the headline result.",
        "",
        _markdown_table(
            one_per_ein,
            ["analysis_frame", "test", "variable", "statistic", "p_value", "p_value_fdr_bh", "n"],
            {
                "analysis_frame": "Comparison",
                "test": "Test",
                "variable": "Variable",
                "statistic": "Statistic",
                "p_value": "P-value",
                "p_value_fdr_bh": "FDR p-value",
                "n": "N",
            },
        ),
        "",
        "## Full-Universe Sensitivity (including hospitals, universities, and political orgs)",
        "",
        "The primary results exclude hospitals, universities, and political organizations for client-peer "
        "comparability. This sensitivity reruns the main share tests on the full valid Form 990/990-EZ/990-PF "
        "universe so readers can see whether that comparability choice changes the substantive conclusion.",
        "",
        _markdown_table(
            full_universe,
            ["analysis_frame", "test", "variable", "statistic", "p_value", "p_value_fdr_bh", "n"],
            {
                "analysis_frame": "Comparison",
                "test": "Test",
                "variable": "Variable",
                "statistic": "Statistic",
                "p_value": "P-value",
                "p_value_fdr_bh": "FDR p-value",
                "n": "N",
            },
        ),
        "",
        "## Bootstrap Confidence Intervals (cluster-bootstrap by EIN)",
        "",
        "These rows show Black Hills minus benchmark mean differences in organization-level revenue-source shares. "
        "The bootstrap resamples EINs (not org-years) so that within-EIN correlation is preserved; the resulting "
        "confidence intervals are wider than a row-level bootstrap and reflect the actual organization-level uncertainty.",
        "",
        _markdown_table(
            bootstrap,
            ["variable", "mean_difference", "ci_lower", "ci_upper"],
            {
                "variable": "Variable",
                "mean_difference": "Mean difference",
                "ci_lower": "95% CI lower",
                "ci_upper": "95% CI upper",
            },
        ),
        "",
        "![Organization-level source shares](figures/violin_source_shares_by_group.png)",
        "",
        "## Individual Contributions Focus (Form 990 only, share of total contributions)",
        "",
        f"This section answers the client question more directly: of the contributions a nonprofit "
        f"reports, what fraction comes from likely-individual donors versus from other organizations? "
        f"It is restricted to Form 990 filers ({focus_n_orgs:,} EINs in this run) because 990-EZ and "
        f"990-PF do not separately report Line 1 sub-channels, and to organization-years with positive "
        f"total contributions so the denominator is meaningful. Shares are share of total contributions "
        f"(not of total revenue), which removes program-service revenue as a confounder.",
        "",
        "Channels:",
        "",
        "- **institutional_clear** - Lines 1a + 1d + 1e (federated campaigns + related-org "
        "contributions + government grants). Unambiguously institutional.",
        "- **individual_narrow** - Lines 1b + 1c (membership dues + fundraising-event "
        "contributions). A *strict lower bound* on individual giving.",
        "- **line_1f_mixed** - Line 1f (`ALLOOTHECONT`). Mixes individuals with foundations, "
        "DAF distributions, corporate gifts, and bequests. Cannot be split further without "
        "Schedule B.",
        "- **individual_broad** - `individual_narrow` + `line_1f_mixed`. An *upper bound* that "
        "treats Line 1f as if it were entirely individual giving. The truth lies between the two "
        "bounds.",
        "",
        "### Aggregate channel mix (share of total contributions, Form 990 only)",
        "",
        _markdown_table(
            focus_mix_table,
            ["Group", "Channel", "Amount", "Share of total contributions"],
            {},
        ),
        "",
        "![Form 990 contribution channels by group](figures/individual_focus_contributions_mix.png)",
        "",
        "### Welch ANOVA on share-of-contribution metrics (Form 990 only)",
        "",
        _markdown_table(
            focus_anova,
            ["analysis_frame", "test", "variable", "statistic", "p_value", "p_value_fdr_bh", "n"],
            {
                "analysis_frame": "Comparison",
                "test": "Test",
                "variable": "Variable",
                "statistic": "Statistic",
                "p_value": "P-value",
                "p_value_fdr_bh": "FDR p-value",
                "n": "N",
            },
        ),
        "",
        "### EIN cluster-bootstrap CIs on Black Hills minus benchmark (Form 990 only)",
        "",
        _markdown_table(
            focus_bootstrap,
            ["variable", "mean_difference", "ci_lower", "ci_upper"],
            {
                "variable": "Variable",
                "mean_difference": "Mean difference",
                "ci_lower": "95% CI lower",
                "ci_upper": "95% CI upper",
            },
        ),
        "",
        "Interpretation guide. If the institutional-clear share differs significantly between BH "
        "and benchmarks but the individual_narrow and individual_broad shares do not, the "
        "individual-versus-institutional gap is being driven by institutional channels (e.g. "
        "government grants), not by individual giving. If the individual_broad bound differs but "
        "individual_narrow does not, the gap is concentrated in Line 1f, which is the bucket we "
        "cannot decompose without Schedule B and should be flagged for follow-up.",
        "",
        "### Year-by-Year Focused Comparisons (Form 990 only, BH vs pooled benchmarks)",
        "",
        "The pooled tests above can hide year-to-year changes. The Welch ANOVA below isolates "
        "Black Hills versus pooled benchmark for each share-of-contributions metric within each "
        "tax year, so a single year of unusual filings cannot drive a multi-year conclusion.",
        "",
        _markdown_table(
            focus_by_year,
            ["tax_year", "variable", "statistic", "p_value", "p_value_fdr_bh", "n"],
            {
                "tax_year": "Tax year",
                "variable": "Variable",
                "statistic": "Welch statistic",
                "p_value": "P-value",
                "p_value_fdr_bh": "FDR p-value",
                "n": "N",
            },
        ),
        "",
        "Cluster-bootstrap 95% confidence intervals on Black Hills minus benchmark, by tax year:",
        "",
        _markdown_table(
            focus_bootstrap_by_year,
            ["tax_year", "variable", "mean_difference", "ci_lower", "ci_upper"],
            {
                "tax_year": "Tax year",
                "variable": "Variable",
                "mean_difference": "Mean difference",
                "ci_lower": "95% CI lower",
                "ci_upper": "95% CI upper",
            },
        ),
        "",
        "![Form 990 contribution channels by group and year](figures/individual_focus_contributions_mix_by_year.png)",
        "",
        "## Year-by-Year Comparisons",
        "",
        _markdown_table(
            by_year,
            ["tax_year", "variable", "statistic", "p_value", "p_value_fdr_bh", "n"],
            {
                "tax_year": "Tax year",
                "variable": "Variable",
                "statistic": "Welch statistic",
                "p_value": "P-value",
                "p_value_fdr_bh": "FDR p-value",
                "n": "N",
            },
        ),
        "",
        "## Overall Revenue-Mix Tests",
        "",
        _markdown_table(
            multivariate,
            ["analysis_frame", "test", "statistic", "p_value", "p_value_fdr_bh", "n"],
            {
                "analysis_frame": "Analysis frame",
                "test": "Test",
                "statistic": "Statistic",
                "p_value": "P-value",
                "p_value_fdr_bh": "FDR p-value",
                "n": "N",
            },
        ),
        "",
        "## Regression Results",
        "",
        _markdown_table(
            regression,
            ["model", "outcome", "estimate", "std_error", "p_value", "p_value_fdr_bh", "n"],
            {
                "model": "Model",
                "outcome": "Outcome",
                "estimate": "Black Hills estimate",
                "std_error": "Std. error",
                "p_value": "P-value",
                "p_value_fdr_bh": "FDR p-value",
                "n": "N",
            },
        ),
        "",
        f"Auxiliary regression warnings captured: {warning_count}. These are retained in `tables/statistical_tests_regression.csv` and do not change the descriptive tables or univariate tests.",
        "",
        "## Concentration Diagnostics",
        "",
        _markdown_table(
            concentration,
            ["comparison_group", "organization_count", "total_revenue", "gini_total_revenue", "hhi_total_revenue", "top5_revenue_share"],
            {
                "comparison_group": "Group",
                "organization_count": "Organizations",
                "total_revenue": "Total revenue",
                "gini_total_revenue": "Gini",
                "hhi_total_revenue": "HHI",
                "top5_revenue_share": "Top 5 share",
            },
        ),
        "",
        "![Top 5 revenue concentration by region](figures/concentration_top5_revenue_share_by_region.png)",
        "",
        "## Diagnostics",
        "",
        "Residual other revenue is a derived category, so rows with negative residuals or shares above 100 percent are reported in `tables/negative_residual_diagnostics.csv`. These rows should be reviewed before using residual revenue as a substantive category. The summary below shows how common those overlap/reconciliation flags are by comparison group.",
        "",
        _markdown_table(
            overlap,
            ["comparison_group", "row_count", "negative_residual_count", "negative_residual_rate", "share_over_100_count", "share_over_100_rate", "mean_source_share_sum"],
            {
                "comparison_group": "Group",
                "row_count": "Rows",
                "negative_residual_count": "Negative residual rows",
                "negative_residual_rate": "Negative residual rate",
                "share_over_100_count": "Over-100 rows",
                "share_over_100_rate": "Over-100 rate",
                "mean_source_share_sum": "Mean source-share sum",
            },
        ),
        "",
        "![Total revenue distribution](figures/diagnostic_total_revenue_distribution.png)",
        "",
        "![Residual revenue diagnostics](figures/diagnostic_residual_revenue.png)",
        "",
        "![Revenue-source share heatmap](figures/heatmap_revenue_source_share_by_year.png)",
        "",
        "## Bottom Line",
        "",
        "Review the five-region Welch ANOVA table and follow-up Black Hills versus benchmark tests. Aggregate dollar mixes and concentration charts provide complementary context when organization-level p-values are noisy.",
        "",
        "## Key Files in This Results Folder",
        "",
        "- `tables/mix_by_group.csv`: aggregate reported component mix by Black Hills versus benchmarks",
        "- `tables/statistical_tests_univariate.csv`: pooled and sensitivity univariate tests",
        "- `tables/statistical_tests_by_year_univariate.csv`: year-by-year hypothesis tests",
        "- `tables/statistical_tests_multivariate.csv`: pooled compositional and multivariate tests",
        "- `tables/concentration_by_group.csv`: Gini, HHI, and top-5 concentration metrics",
        "- `tables/component_overlap_by_group.csv`: group-level reconciliation diagnostics for overlapping components",
        "- `tables/negative_residual_diagnostics.csv`: rows needing residual/share diagnostics",
    ]
    report_path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    info(f"Saved detailed results report: {report_path}")


def run_analysis(
    *,
    data_root: Path,
    output_dir: Path,
    results_dir: Path | None,
    years: list[int],
    include_sensitivity: bool,
    exclude_outliers: bool,
) -> None:
    """Run the full revenue-source analysis workflow."""

    info("Starting revenue-source analysis.")
    paths = ensure_output_dirs(output_dir, results_dir=results_dir)
    gt_raw = load_givingtuesday_analysis(data_root, years)
    full_universe_analysis: pd.DataFrame | None = None
    if exclude_outliers:
        full_universe_analysis = prepare_givingtuesday_analysis(gt_raw, exclude_outliers=False)
        outlier_mask = (
            full_universe_analysis["is_hospital"].fillna(False)
            | full_universe_analysis["is_university"].fillna(False)
            | full_universe_analysis["is_political_org"].fillna(False)
        )
        analysis = full_universe_analysis.loc[~outlier_mask].copy().reset_index(drop=True)
        analysis.attrs["excluded_org_type_row_count"] = int(outlier_mask.sum())
        analysis.attrs["excluded_org_type_ein_count"] = int(full_universe_analysis.loc[outlier_mask, "ein"].nunique())
        analysis.attrs["full_universe_row_count"] = int(len(full_universe_analysis))
        analysis.attrs["full_universe_ein_count"] = int(full_universe_analysis["ein"].nunique())
        info(
            "Primary comparable-universe exclusion applied. "
            f"Rows before={len(full_universe_analysis):,}, after={len(analysis):,}, "
            f"excluded rows={int(outlier_mask.sum()):,}."
        )
    else:
        analysis = prepare_givingtuesday_analysis(gt_raw, exclude_outliers=False)
        analysis.attrs["excluded_org_type_row_count"] = 0
        analysis.attrs["excluded_org_type_ein_count"] = 0
        analysis.attrs["full_universe_row_count"] = int(len(analysis))
        analysis.attrs["full_universe_ein_count"] = int(analysis["ein"].nunique())

    write_parquet(analysis, paths.output_dir / "cleaned_revenue_sources_analysis.parquet")
    write_table(analysis, paths.output_dir / "cleaned_revenue_sources_analysis.csv")

    tables: dict[str, pd.DataFrame] = {}
    tables["counts"] = counts_table(analysis)
    tables["missingness"] = missingness_table(analysis)
    tables["summary_statistics"] = summary_statistics(analysis)
    tables["mix_by_group"] = aggregate_revenue_mix(analysis, ["comparison_group"])
    tables["mix_by_group_year"] = aggregate_revenue_mix(analysis, ["comparison_group", "tax_year"])
    tables["mix_by_region"] = aggregate_revenue_mix(analysis, ["region_label"])
    tables["mix_by_group_form"] = aggregate_revenue_mix(analysis, ["comparison_group", "form_type"])
    tables["concentration_by_group"] = concentration_metrics(analysis, ["comparison_group"])
    tables["concentration_by_region"] = concentration_metrics(analysis, ["region_label"])
    tables["negative_residual_diagnostics"] = negative_residual_diagnostics(analysis)
    tables["component_overlap_by_group"] = component_overlap_summary(analysis, ["comparison_group"])
    tables["component_overlap_by_group_year"] = component_overlap_summary(analysis, ["comparison_group", "tax_year"])
    tables["component_overlap_by_form"] = component_overlap_summary(analysis, ["comparison_group", "form_type"])

    for name, table in tables.items():
        write_table(table, paths.tables_dir / f"{name}.csv")

    primary_results_five = run_univariate_tests(
        analysis, PRIMARY_TEST_VARIABLES, label="primary_all_years_five_regions", group_column="region_label"
    )
    primary_results_benchmark = run_univariate_tests(
        analysis, PRIMARY_TEST_VARIABLES, label="primary_all_years_bh_vs_benchmark", group_column="comparison_group"
    )
    primary_results = pd.concat([primary_results_five, primary_results_benchmark], ignore_index=True)
    regression_results = run_regressions(
        analysis,
        SHARE_COMPONENTS
        + [
            f"log1p_{column}"
            for column in [
                "total_revenue",
                "total_contributions",
                "program_service_revenue",
                "government_grants_received",
                "other_institutional_contributions",
                "individual_likely_contributions",
                "mixed_other_contributions",
            ]
        ],
    )
    multivariate_results = run_multivariate_tests(analysis, label="primary_all_years")

    # Focused individual-contributions analysis.
    #
    # The primary share variables divide every segment by *total revenue*,
    # which mixes revenue-mix differences with program-service-revenue
    # differences. For the client question "are individual donors versus
    # other organizations the dominant source of contributions in BH",
    # the cleaner contrast is share-of-total-contributions on a 990-only
    # frame. This block runs that focused analysis as a supplement.
    individual_focus_frame = prepare_individual_contributions_focus(analysis)
    individual_focus_mix = aggregate_individual_focus_mix(
        individual_focus_frame, ["comparison_group"]
    )
    individual_focus_mix_by_region = aggregate_individual_focus_mix(
        individual_focus_frame, ["region_label"]
    )
    individual_focus_mix_by_year = aggregate_individual_focus_mix(
        individual_focus_frame, ["comparison_group", "tax_year"]
    )
    individual_focus_univariate, individual_focus_bootstrap = run_individual_focus_tests(
        individual_focus_frame
    )

    # Year-by-year focused tests so the client can see whether the
    # institutional vs individual gap is stable across 2022, 2023, 2024 or
    # whether it is driven by a single filing year.
    individual_focus_by_year_frames: list[pd.DataFrame] = []
    individual_focus_by_year_bootstrap_rows: list[dict[str, object]] = []
    if not individual_focus_frame.empty:
        for tax_year, year_frame in individual_focus_frame.groupby("tax_year"):
            if (
                len(year_frame) < 10
                or year_frame["comparison_group"].nunique() < 2
                or year_frame["region_label"].nunique() < 2
            ):
                continue
            info(f"Running year-by-year individual-focus tests for tax_year={tax_year}.")
            individual_focus_by_year_frames.append(
                run_univariate_tests(
                    year_frame,
                    INDIVIDUAL_FOCUS_SHARES,
                    label=f"individual_focus_year_{tax_year}_five_regions",
                    group_column="region_label",
                )
            )
            individual_focus_by_year_frames.append(
                run_univariate_tests(
                    year_frame,
                    INDIVIDUAL_FOCUS_SHARES,
                    label=f"individual_focus_year_{tax_year}_bh_vs_benchmark",
                    group_column="comparison_group",
                )
            )
            for variable in INDIVIDUAL_FOCUS_SHARES:
                observed, lower, upper = bootstrap_ci_difference(year_frame, variable)
                individual_focus_by_year_bootstrap_rows.append({
                    "tax_year": str(tax_year),
                    "variable": variable,
                    "mean_difference": observed,
                    "ci_lower": lower,
                    "ci_upper": upper,
                })
    individual_focus_by_year_univariate = (
        pd.concat(individual_focus_by_year_frames, ignore_index=True)
        if individual_focus_by_year_frames
        else pd.DataFrame()
    )
    individual_focus_by_year_bootstrap = pd.DataFrame(individual_focus_by_year_bootstrap_rows)

    # Year-by-year tests are kept separate from the pooled headline tests. This
    # is important because 2024 filing coverage may be incomplete, and because a
    # pooled result can hide one-year changes in the direction or size of the
    # Black Hills versus benchmark difference.
    info("Running full year-by-year hypothesis tests.")
    by_year_univariate_frames: list[pd.DataFrame] = []
    by_year_multivariate_frames: list[pd.DataFrame] = []
    for tax_year, year_frame in analysis.groupby("tax_year"):
        if len(year_frame) < 10 or year_frame["comparison_group"].nunique() < 2 or year_frame["region_label"].nunique() < 2:
            info(f"Skipping year {tax_year}: insufficient rows, regions, or comparison group.")
            continue
        info(f"Running year-by-year tests for tax_year={tax_year} with rows={len(year_frame):,}.")
        by_year_univariate_frames.append(
            run_univariate_tests(
                year_frame,
                PRIMARY_TEST_VARIABLES,
                label=f"year_{tax_year}_five_regions",
                group_column="region_label",
            )
        )
        by_year_univariate_frames.append(
            run_univariate_tests(
                year_frame,
                PRIMARY_TEST_VARIABLES,
                label=f"year_{tax_year}_bh_vs_benchmark",
                group_column="comparison_group",
            )
        )
        by_year_multivariate_frames.append(run_multivariate_tests(year_frame, label=f"year_{tax_year}"))
    by_year_univariate_results = (
        pd.concat(by_year_univariate_frames, ignore_index=True)
        if by_year_univariate_frames
        else pd.DataFrame()
    )
    by_year_multivariate_results = (
        pd.concat(by_year_multivariate_frames, ignore_index=True)
        if by_year_multivariate_frames
        else pd.DataFrame()
    )

    sensitivity_results: list[pd.DataFrame] = []
    if include_sensitivity:
        info("Running requested sensitivity analyses.")

        # Independence sensitivity: keep only the most recent organization-year
        # per EIN. The pooled tests treat each org-year as an independent
        # observation, but the typical EIN files in two of the three years
        # (~1.94 obs/EIN). Reducing to one row per EIN removes within-EIN
        # autocorrelation entirely; if the headline pattern survives this
        # restriction, the repeated-EIN issue is not driving the result.
        one_per_ein = (
            analysis.sort_values(["ein", "tax_year"])
            .drop_duplicates(subset=["ein"], keep="last")
            .copy()
        )
        if (
            len(one_per_ein) >= 10
            and one_per_ein["comparison_group"].nunique() == 2
            and one_per_ein["region_label"].nunique() >= 2
        ):
            info(f"Running one-row-per-EIN sensitivity with rows={len(one_per_ein):,} (was {len(analysis):,}).")
            sensitivity_results.append(
                run_univariate_tests(
                    one_per_ein,
                    PRIMARY_TEST_VARIABLES,
                    label="sensitivity_one_row_per_ein_five_regions",
                    group_column="region_label",
                )
            )
            sensitivity_results.append(
                run_univariate_tests(
                    one_per_ein,
                    PRIMARY_TEST_VARIABLES,
                    label="sensitivity_one_row_per_ein_bh_vs_benchmark",
                    group_column="comparison_group",
                )
            )
            sensitivity_results.append(run_multivariate_tests(one_per_ein, label="sensitivity_one_row_per_ein"))

        year_sensitivity = analysis.loc[analysis["tax_year"].isin(["2022", "2023"])].copy()
        if not year_sensitivity.empty:
            sensitivity_results.append(
                run_univariate_tests(
                    year_sensitivity,
                    PRIMARY_TEST_VARIABLES,
                    label="sensitivity_2022_2023_only_five_regions",
                    group_column="region_label",
                )
            )
            sensitivity_results.append(
                run_univariate_tests(
                    year_sensitivity,
                    PRIMARY_TEST_VARIABLES,
                    label="sensitivity_2022_2023_only_bh_vs_benchmark",
                    group_column="comparison_group",
                )
            )
            sensitivity_results.append(run_multivariate_tests(year_sensitivity, label="sensitivity_2022_2023_only"))

        for form_type, form_frame in analysis.groupby("form_type"):
            if len(form_frame) >= 10 and form_frame["comparison_group"].nunique() == 2 and form_frame["region_label"].nunique() >= 2:
                sensitivity_results.append(
                    run_univariate_tests(
                        form_frame,
                        PRIMARY_TEST_VARIABLES,
                        label=f"sensitivity_form_{form_type}_five_regions",
                        group_column="region_label",
                    )
                )
                sensitivity_results.append(
                    run_univariate_tests(
                        form_frame,
                        PRIMARY_TEST_VARIABLES,
                        label=f"sensitivity_form_{form_type}_bh_vs_benchmark",
                        group_column="comparison_group",
                    )
                )

        for stratum, stratum_frame in analysis.groupby("revenue_size_stratum"):
            if len(stratum_frame) >= 10 and stratum_frame["comparison_group"].nunique() == 2 and stratum_frame["region_label"].nunique() >= 2:
                sensitivity_results.append(
                    run_univariate_tests(
                        stratum_frame,
                        PRIMARY_TEST_VARIABLES,
                        label=f"sensitivity_revenue_size_{stratum}_five_regions",
                        group_column="region_label",
                    )
                )
                sensitivity_results.append(
                    run_univariate_tests(
                        stratum_frame,
                        PRIMARY_TEST_VARIABLES,
                        label=f"sensitivity_revenue_size_{stratum}_bh_vs_benchmark",
                        group_column="comparison_group",
                    )
                )

        if exclude_outliers and full_universe_analysis is not None:
            if (
                len(full_universe_analysis) >= 10
                and full_universe_analysis["comparison_group"].nunique() == 2
                and full_universe_analysis["region_label"].nunique() >= 2
            ):
                info("Running full-universe sensitivity that includes hospitals, universities, and political organizations.")
                sensitivity_results.append(
                    run_univariate_tests(
                        full_universe_analysis,
                        PRIMARY_TEST_VARIABLES,
                        label="sensitivity_including_outliers_five_regions",
                        group_column="region_label",
                    )
                )
                sensitivity_results.append(
                    run_univariate_tests(
                        full_universe_analysis,
                        PRIMARY_TEST_VARIABLES,
                        label="sensitivity_including_outliers_bh_vs_benchmark",
                        group_column="comparison_group",
                    )
                )
        else:
            no_outlier = analysis.loc[
                ~(
                    analysis["is_hospital"].fillna(False)
                    | analysis["is_university"].fillna(False)
                    | analysis["is_political_org"].fillna(False)
                )
            ].copy()
            if len(no_outlier) >= 10 and no_outlier["comparison_group"].nunique() == 2 and no_outlier["region_label"].nunique() >= 2:
                sensitivity_results.append(
                    run_univariate_tests(
                        no_outlier,
                        PRIMARY_TEST_VARIABLES,
                        label="sensitivity_excluding_outliers_five_regions",
                        group_column="region_label",
                    )
                )
                sensitivity_results.append(
                    run_univariate_tests(
                        no_outlier,
                        PRIMARY_TEST_VARIABLES,
                        label="sensitivity_excluding_outliers_bh_vs_benchmark",
                        group_column="comparison_group",
                    )
                )

        core = load_core_sensitivity(data_root)
        if core is not None and not core.empty:
            core_results = run_univariate_tests(core, ["program_service_revenue_share", "total_contributions_share"], label="sensitivity_nccs_core_2022")
            sensitivity_results.append(core_results)
            write_table(core, paths.tables_dir / "nccs_core_2022_sensitivity_cleaned.csv")

    all_univariate_parts = [primary_results]
    if not by_year_univariate_results.empty:
        all_univariate_parts.append(by_year_univariate_results)
    all_univariate_parts.extend(sensitivity_results)
    all_stats = pd.concat(all_univariate_parts, ignore_index=True)
    write_table(all_stats, paths.tables_dir / "statistical_tests_univariate.csv")
    write_table(by_year_univariate_results, paths.tables_dir / "statistical_tests_by_year_univariate.csv")
    write_table(regression_results, paths.tables_dir / "statistical_tests_regression.csv")
    write_table(multivariate_results, paths.tables_dir / "statistical_tests_multivariate.csv")
    write_table(by_year_multivariate_results, paths.tables_dir / "statistical_tests_by_year_multivariate.csv")

    bootstrap_rows = []
    for variable in SHARE_COMPONENTS:
        observed, lower, upper = bootstrap_ci_difference(analysis, variable)
        bootstrap_rows.append({"variable": variable, "mean_difference": observed, "ci_lower": lower, "ci_upper": upper})
    bootstrap_results = pd.DataFrame(bootstrap_rows)
    write_table(bootstrap_results, paths.tables_dir / "bootstrap_mean_difference_ci.csv")

    write_table(individual_focus_mix, paths.tables_dir / "individual_focus_mix_by_group.csv")
    write_table(individual_focus_mix_by_region, paths.tables_dir / "individual_focus_mix_by_region.csv")
    write_table(individual_focus_mix_by_year, paths.tables_dir / "individual_focus_mix_by_year.csv")
    write_table(individual_focus_univariate, paths.tables_dir / "individual_focus_statistical_tests.csv")
    write_table(individual_focus_by_year_univariate, paths.tables_dir / "individual_focus_statistical_tests_by_year.csv")
    write_table(individual_focus_bootstrap, paths.tables_dir / "individual_focus_bootstrap_ci.csv")
    write_table(individual_focus_by_year_bootstrap, paths.tables_dir / "individual_focus_bootstrap_ci_by_year.csv")

    create_figures(analysis, tables, paths.figures_dir)
    save_individual_focus_chart(
        individual_focus_mix,
        paths.figures_dir / "individual_focus_contributions_mix.png",
        "Form-990 contribution channels: Black Hills vs pooled benchmarks",
    )
    save_individual_focus_year_chart(
        individual_focus_mix_by_year,
        paths.figures_dir / "individual_focus_contributions_mix_by_year.png",
        "Form-990 contribution channels by group and tax year",
    )
    collect_results_artifacts(
        paths,
        table_names=[
            "counts.csv",
            "mix_by_group.csv",
            "mix_by_group_year.csv",
            "mix_by_region.csv",
            "mix_by_group_form.csv",
            "statistical_tests_univariate.csv",
            "statistical_tests_by_year_univariate.csv",
            "statistical_tests_regression.csv",
            "statistical_tests_multivariate.csv",
            "statistical_tests_by_year_multivariate.csv",
            "bootstrap_mean_difference_ci.csv",
            "concentration_by_group.csv",
            "concentration_by_region.csv",
            "negative_residual_diagnostics.csv",
            "component_overlap_by_group.csv",
            "component_overlap_by_group_year.csv",
            "component_overlap_by_form.csv",
            "individual_focus_mix_by_group.csv",
            "individual_focus_mix_by_region.csv",
            "individual_focus_mix_by_year.csv",
            "individual_focus_statistical_tests.csv",
            "individual_focus_statistical_tests_by_year.csv",
            "individual_focus_bootstrap_ci.csv",
            "individual_focus_bootstrap_ci_by_year.csv",
        ],
        figure_names=[
            "stacked_revenue_mix_black_hills_vs_benchmark.png",
            "stacked_revenue_mix_by_year.png",
            "stacked_revenue_mix_by_region.png",
            "stacked_revenue_mix_by_form_type.png",
            "violin_source_shares_by_group.png",
            "diagnostic_total_revenue_distribution.png",
            "diagnostic_residual_revenue.png",
            "concentration_top5_revenue_share_by_region.png",
            "heatmap_revenue_source_share_by_year.png",
            "individual_focus_contributions_mix.png",
            "individual_focus_contributions_mix_by_year.png",
        ],
    )
    write_detailed_results_report(
        analysis=analysis,
        tables=tables,
        univariate_results=all_stats,
        regression_results=regression_results,
        multivariate_results=multivariate_results,
        by_year_univariate_results=by_year_univariate_results,
        by_year_multivariate_results=by_year_multivariate_results,
        bootstrap_results=bootstrap_results,
        individual_focus_frame=individual_focus_frame,
        individual_focus_mix=individual_focus_mix,
        individual_focus_univariate=individual_focus_univariate,
        individual_focus_bootstrap=individual_focus_bootstrap,
        individual_focus_by_year_univariate=individual_focus_by_year_univariate,
        individual_focus_by_year_bootstrap=individual_focus_by_year_bootstrap,
        paths=paths,
    )
    build_markdown_summary(
        analysis,
        all_stats,
        multivariate_results,
        by_year_univariate_results,
        tables["concentration_by_group"],
        paths.output_dir / "revenue_sources_methods_results_summary.md",
    )
    info("Revenue-source analysis complete.")
    info(f"Review outputs in: {paths.output_dir}")


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    """Parse command-line options."""

    parser = argparse.ArgumentParser(description="Analyze nonprofit revenue-source differences between Black Hills and benchmark regions.")
    parser.add_argument("--data-root", type=Path, default=DEFAULT_DATA_ROOT, help="Project 01_data root. Defaults to python.utils.paths.DATA.")
    parser.add_argument("--output-dir", type=Path, default=None, help="Output directory. Defaults to DATA/analysis/revenue_sources_black_hills.")
    parser.add_argument("--results-dir", type=Path, default=None, help="User-facing results bundle directory. Defaults to python/analysis/revenue_sources_black_hills/results.")
    parser.add_argument("--years", type=int, nargs="+", default=[2022, 2023, 2024], help="Tax years to include.")
    parser.add_argument("--include-sensitivity", action=argparse.BooleanOptionalAction, default=True, help="Run sensitivity analyses.")
    parser.add_argument(
        "--exclude-outliers",
        action=argparse.BooleanOptionalAction,
        default=True,
        help=(
            "Exclude hospital, university, and political-organization proxy rows "
            "from the primary frame for client-peer comparability. Use "
            "--no-exclude-outliers to make the full valid universe primary."
        ),
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> None:
    """CLI entry point."""

    args = parse_args(argv)
    data_root = args.data_root.resolve()
    output_dir = (args.output_dir or (data_root / "analysis" / "revenue_sources_black_hills")).resolve()
    run_analysis(
        data_root=data_root,
        output_dir=output_dir,
        results_dir=args.results_dir.resolve() if args.results_dir else None,
        years=args.years,
        include_sensitivity=args.include_sensitivity,
        exclude_outliers=args.exclude_outliers,
    )


if __name__ == "__main__":
    main()
