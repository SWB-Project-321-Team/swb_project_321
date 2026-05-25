"""
Assumption diagnostics for the Black Hills revenue-source analysis.

The companion script ``revenue_sources_black_hills.py`` runs the substantive
tests for Section 3 Q9: Kruskal-Wallis five-region raw-dollar tests,
Mann-Whitney U Black Hills-vs-benchmark raw-dollar tests, permutation
mean-difference checks, supplemental share/compositional tests, OLS with
EIN-clustered standard errors, EIN-clustered logistic presence models,
EIN-cluster bootstrap confidence intervals, PERMANOVA-style tests on
CLR-transformed shares, MANOVA on ALR-transformed shares, log-dollar checks,
and a one-row-per-EIN independence sensitivity. Each
of those tests has assumptions. This script empirically checks the assumptions
on the cleaned analysis frame so the user can see where the caveats are, how
severe they are, and whether the headline finding survives them.

The script writes:

- ``results/assumptions_check_report.md``  - human-readable findings
- ``results/tables/assumptions_check_details.csv`` - machine-readable table
- ``results/sample_size_diagnostics_report.md`` - 2022 presentation sample-size findings
- ``results/tables/client_2022_sample_size_diagnostics.csv`` - pairwise 2022 diagnostics

Each row in the CSV has columns ``test``, ``assumption``, ``variable``,
``group``, ``statistic``, ``p_value``, ``status``, and ``notes``. ``status``
is one of ``OK``, ``CAVEAT``, or ``VIOLATED``. CAVEAT means the assumption is
formally violated or creates an interpretation caveat but the analysis script
already mitigates it (e.g. with cluster-robust SEs, rank tests,
distribution-free permutation, composition-aware tests, or sensitivity runs);
VIOLATED means there is no mitigation in place and the result should be
interpreted with care.

Run from the repository root:

    python python/analysis/revenue_sources_black_hills/assumptions_check.py

It uses the cleaned parquet written by the main analysis. If the parquet is
missing, the script will tell you to run the main analysis first.
"""

from __future__ import annotations

import argparse
import math
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable

import numpy as np
import pandas as pd
from scipy import stats

_THIS_FILE = Path(__file__).resolve()
_PYTHON_DIR = _THIS_FILE.parents[2]
if str(_PYTHON_DIR) not in sys.path:
    sys.path.insert(0, str(_PYTHON_DIR))

from utils.paths import DATA as DEFAULT_DATA_ROOT  # noqa: E402

from analysis.revenue_sources_black_hills.revenue_sources_black_hills import (  # noqa: E402
    CLIENT_PRESENTATION_VARIABLES,
    CLIENT_RAW_LEVEL_VARIABLES,
    CLIENT_RAW_DISPLAY_LABELS,
    PERMUTATION_ITERATIONS_2022,
    REGION_ORDER,
    SHARE_COMPONENTS,
    SOURCE_COMPONENTS,
    composition_matrix,
)


# ---------------------------------------------------------------------------
# Logging and IO helpers
# ---------------------------------------------------------------------------


def info(message: str) -> None:
    """Print a tagged status line so the script's output is easy to scan."""

    print(f"[assumptions-check] {message}", flush=True)


def _round(value: float | int | np.floating | None, digits: int = 4) -> float | None:
    """Round a numeric value while passing missing values through unchanged."""

    if value is None or (isinstance(value, float) and (np.isnan(value) or np.isinf(value))):
        return None
    return float(np.round(float(value), digits))


@dataclass
class AssumptionsPaths:
    """Paths the script reads from and writes to."""

    cleaned_parquet: Path
    report_md: Path
    details_csv: Path
    sample_size_report_md: Path
    sample_size_csv: Path
    pairwise_2022_summary_csv: Path


def resolve_paths(
    data_root: Path,
    output_dir: Path | None,
    results_dir: Path | None,
) -> AssumptionsPaths:
    """Resolve input and output paths in a single place."""

    output_dir = (output_dir or (data_root / "analysis" / "revenue_sources_black_hills")).resolve()
    results_dir = (
        results_dir
        or _THIS_FILE.parent / "results"
    ).resolve()
    cleaned_parquet = output_dir / "cleaned_revenue_sources_analysis.parquet"
    report_md = results_dir / "assumptions_check_report.md"
    details_csv = results_dir / "tables" / "assumptions_check_details.csv"
    sample_size_report_md = results_dir / "sample_size_diagnostics_report.md"
    sample_size_csv = results_dir / "tables" / "client_2022_sample_size_diagnostics.csv"
    pairwise_2022_summary_csv = results_dir / "tables" / "client_2022_pairwise_presentation_summary.csv"
    return AssumptionsPaths(
        cleaned_parquet=cleaned_parquet,
        report_md=report_md,
        details_csv=details_csv,
        sample_size_report_md=sample_size_report_md,
        sample_size_csv=sample_size_csv,
        pairwise_2022_summary_csv=pairwise_2022_summary_csv,
    )


# ---------------------------------------------------------------------------
# Status-row helper
# ---------------------------------------------------------------------------


def make_row(
    *,
    test: str,
    assumption: str,
    variable: str = "",
    group: str = "",
    statistic: float | None = None,
    p_value: float | None = None,
    status: str,
    notes: str = "",
) -> dict[str, object]:
    """Build a single row for the assumptions-check details CSV."""

    return {
        "test": test,
        "assumption": assumption,
        "variable": variable,
        "group": group,
        "statistic": _round(statistic),
        "p_value": _round(p_value),
        "status": status,
        "notes": notes,
    }


# ---------------------------------------------------------------------------
# Independence (repeated EINs)
# ---------------------------------------------------------------------------


def check_independence(frame: pd.DataFrame) -> list[dict[str, object]]:
    """Quantify the repeated-EIN problem and the within-EIN correlation (ICC).

    All pooled tests in the analysis treat each org-year as an independent
    observation. The actual data has the same EIN filing in multiple years,
    so org-years are not independent. We report:

    1. Average filings per EIN (raw repeated-measures index).
    2. Intraclass correlation (ICC1) for each share variable, defined as the
       between-EIN variance divided by total variance. ICC > 0 means the
       residuals are correlated within EIN.
    """

    info("Checking independence (repeated-EIN structure).")
    rows: list[dict[str, object]] = []

    n_rows = len(frame)
    n_eins = frame["ein"].nunique()
    obs_per_ein = n_rows / max(n_eins, 1)
    rows.append(
        make_row(
            test="all_pooled_tests",
            assumption="independence_of_observations",
            statistic=obs_per_ein,
            # Independence is formally violated because EINs file in multiple years,
            # but the analysis design covers every test that uses pooled org-years
            # (cluster-robust SEs for OLS/logistic, cluster bootstrap for CIs,
            # one-row-per-EIN sensitivity for rank tests / PERMANOVA / secondary
            # Welch tests). Treat as CAVEAT, not blocker.
            status="CAVEAT" if obs_per_ein > 1.05 else "OK",
            notes=(
                f"{n_rows:,} org-years from {n_eins:,} EINs ({obs_per_ein:.2f} filings/EIN). "
                "Mitigated by EIN-clustered OLS, EIN-clustered logistic, EIN-cluster "
                "bootstrap CIs, and a one-row-per-EIN sensitivity in the main script. "
                "The rank and permutation tests are row-level tests, so the "
                "one-row-per-EIN sensitivity is the cluster-aware check for those results."
            ),
        )
    )

    # ICC1 per share variable using a one-way random-effects ANOVA decomposition.
    for variable in SHARE_COMPONENTS:
        if variable not in frame.columns:
            continue
        sub = frame[["ein", variable]].dropna()
        if sub["ein"].nunique() < 2 or len(sub) <= sub["ein"].nunique():
            continue
        try:
            grand_mean = sub[variable].mean()
            group_means = sub.groupby("ein")[variable].mean()
            group_sizes = sub.groupby("ein")[variable].size()
            ss_between = float((group_sizes * (group_means - grand_mean) ** 2).sum())
            ss_within = float(
                sub.groupby("ein")[variable]
                .apply(lambda x: float(((x - x.mean()) ** 2).sum()))
                .sum()
            )
            k = float(sub["ein"].nunique())
            n = float(len(sub))
            df_between = k - 1
            df_within = n - k
            ms_between = ss_between / df_between if df_between > 0 else np.nan
            ms_within = ss_within / df_within if df_within > 0 else np.nan
            n_bar = float(group_sizes.mean())
            denominator = ms_between + (n_bar - 1) * ms_within
            if not np.isfinite(denominator) or denominator <= 0:
                continue
            icc1 = (ms_between - ms_within) / denominator
            icc1 = max(min(icc1, 1.0), -1.0)
        except Exception:  # pragma: no cover - defensive for degenerate slices
            continue
        # Even a high ICC is mitigated by the analysis design (cluster-robust SEs,
        # cluster bootstrap, one-row-per-EIN sensitivity). The status here records
        # how strong the within-EIN correlation is, not whether the analysis
        # responds to it. Reserve VIOLATED for un-mitigated assumption failures
        # so the summary count is meaningful.
        if icc1 >= 0.02:
            status = "CAVEAT"
        else:
            status = "OK"
        rows.append(
            make_row(
                test="rank_share_tests",
                assumption="independence_of_observations",
                variable=variable,
                statistic=icc1,
                status=status,
                notes=(
                    "ICC1 from one-way random-effects variance decomposition by EIN. ICC near zero means org-years "
                    "behave independently for this share; larger ICC means the same EIN tends to "
                    "report similar shares across years. The share tests are rank/permutation "
                    "tests, so the one-row-per-EIN sensitivity and EIN-cluster bootstrap are "
                    "the relevant cluster-aware checks."
                ),
            )
        )
    return rows


# ---------------------------------------------------------------------------
# Rank-test sample size requirements
# ---------------------------------------------------------------------------


def check_rank_test_sample_sizes(frame: pd.DataFrame) -> list[dict[str, object]]:
    """Check that the rank tests have enough observations in each group.

    Kruskal-Wallis and Mann-Whitney U do not require normally distributed
    outcomes, but they do require meaningful group sizes. This check makes the
    sample-size assumption explicit for both the five-region and direct
    Black Hills-vs-benchmark raw-dollar tests, with supplemental share tests.
    """

    info("Checking rank-test sample sizes.")
    rows: list[dict[str, object]] = []
    for variable in CLIENT_RAW_LEVEL_VARIABLES + SHARE_COMPONENTS:
        if variable not in frame.columns:
            continue
        is_raw = variable in CLIENT_RAW_LEVEL_VARIABLES
        test_suffix = "raw_dollars" if is_raw else "share"

        region_counts = frame.dropna(subset=[variable]).groupby("region_label")[variable].size()
        if not region_counts.empty:
            min_count = int(region_counts.min())
            if min_count >= 20:
                status = "OK"
            elif min_count >= 5:
                status = "CAVEAT"
            else:
                status = "VIOLATED"
            rows.append(
                make_row(
                    test=f"kruskal_wallis_{test_suffix}",
                    assumption="sufficient_group_size",
                    variable=variable,
                    group="region_label",
                    statistic=min_count,
                    status=status,
                    notes=(
                        "Smallest five-region group size for this variable. Kruskal-Wallis is "
                        "distribution-free but needs enough observations in each region for the "
                        "rank approximation to be reliable."
                    ),
                )
            )

        comparison_counts = frame.dropna(subset=[variable]).groupby("comparison_group")[variable].size()
        if not comparison_counts.empty:
            min_count = int(comparison_counts.min())
            if min_count >= 20:
                status = "OK"
            elif min_count >= 5:
                status = "CAVEAT"
            else:
                status = "VIOLATED"
            rows.append(
                make_row(
                    test=f"mann_whitney_{test_suffix}",
                    assumption="sufficient_group_size",
                    variable=variable,
                    group="comparison_group",
                    statistic=min_count,
                    status=status,
                    notes=(
                        "Smaller Black Hills/benchmark sample size for this variable. Mann-Whitney "
                        "U is distribution-free but needs enough observations in both groups for "
                        "the asymptotic p-value to be reliable."
                    ),
                )
            )
    return rows


# ---------------------------------------------------------------------------
# 2022 presentation sample-size diagnostics
# ---------------------------------------------------------------------------


def _log10_combinations(n: int, k: int) -> float:
    """Return log10(n choose k) without materializing a huge integer."""

    if n < 0 or k < 0 or k > n:
        return np.nan
    return float(
        (math.lgamma(n + 1) - math.lgamma(k + 1) - math.lgamma(n - k + 1))
        / math.log(10)
    )


def _load_pairwise_2022_summary(path: Path) -> pd.DataFrame:
    """Load the generated 2022 pairwise slide summary when it is available."""

    if not path.exists():
        return pd.DataFrame()
    summary = pd.read_csv(path)
    merge_columns = [
        "variable",
        "benchmark_region",
        "median_difference",
        "median_difference_ci_low",
        "median_difference_ci_high",
        "permutation_p_value",
        "fdr_p_value",
        "significant_before_fdr",
        "significant_after_fdr",
    ]
    return summary[[column for column in merge_columns if column in summary.columns]].copy()


def _sample_size_concern_level(min_positive_n: int, harmonic_positive_n: float, tie_fraction: float) -> str:
    """Classify whether sample size is likely to limit the positive-only test."""

    if min_positive_n < 2:
        return "invalid"
    if min_positive_n < 10 or harmonic_positive_n < 20:
        return "high"
    if min_positive_n < 20 or harmonic_positive_n < 40 or tie_fraction >= 0.50:
        return "moderate"
    return "low"


def build_2022_sample_size_diagnostics(
    frame: pd.DataFrame,
    pairwise_summary: pd.DataFrame | None = None,
) -> pd.DataFrame:
    """Build pairwise sample-size diagnostics for the 2022 client deck.

    The deck's source slides use positive-only medians and permutation tests.
    This diagnostic table checks whether each Black Hills-vs-benchmark
    comparison has enough positive reporters for that conditional test to be
    informative. Low positive reporter counts are not, by themselves, a
    permutation-test assumption violation; they are a precision and power
    limitation.
    """

    info("Building 2022 positive-reporter sample-size diagnostics.")
    year_frame = frame.loc[frame["tax_year"].astype(str).eq("2022")].copy()
    benchmark_regions = [region for region in REGION_ORDER if region != "Black Hills"]
    rows: list[dict[str, object]] = []

    for variable in CLIENT_PRESENTATION_VARIABLES:
        if variable not in year_frame.columns:
            continue
        label = CLIENT_RAW_DISPLAY_LABELS.get(variable, variable)
        for benchmark_region in benchmark_regions:
            bh_values = (
                year_frame.loc[year_frame["region_label"].eq("Black Hills"), variable]
                .dropna()
                .clip(lower=0)
            )
            benchmark_values = (
                year_frame.loc[year_frame["region_label"].eq(benchmark_region), variable]
                .dropna()
                .clip(lower=0)
            )
            bh_eligible_n = int(len(bh_values))
            benchmark_eligible_n = int(len(benchmark_values))
            bh_positive = bh_values.loc[bh_values.gt(0)].to_numpy(dtype=float)
            benchmark_positive = benchmark_values.loc[benchmark_values.gt(0)].to_numpy(dtype=float)
            bh_positive_n = int(len(bh_positive))
            benchmark_positive_n = int(len(benchmark_positive))
            total_positive_n = bh_positive_n + benchmark_positive_n
            min_positive_n = min(bh_positive_n, benchmark_positive_n)
            harmonic_positive_n = (
                2.0 / ((1.0 / bh_positive_n) + (1.0 / benchmark_positive_n))
                if bh_positive_n > 0 and benchmark_positive_n > 0
                else 0.0
            )
            combined_positive = np.concatenate([bh_positive, benchmark_positive])
            if total_positive_n:
                value_counts = pd.Series(combined_positive).value_counts()
                tied_observations = int(value_counts.loc[value_counts.gt(1)].sum())
                tie_fraction = float(tied_observations / total_positive_n)
                unique_positive_values = int(value_counts.size)
                pooled_positive_median = float(np.median(combined_positive))
                pooled_positive_iqr = float(
                    np.percentile(combined_positive, 75) - np.percentile(combined_positive, 25)
                )
            else:
                tie_fraction = np.nan
                unique_positive_values = 0
                pooled_positive_median = np.nan
                pooled_positive_iqr = np.nan

            bh_nonzero_rate = float(bh_positive_n / bh_eligible_n) if bh_eligible_n else np.nan
            benchmark_nonzero_rate = (
                float(benchmark_positive_n / benchmark_eligible_n) if benchmark_eligible_n else np.nan
            )
            log10_labelings = _log10_combinations(total_positive_n, bh_positive_n)
            monte_carlo_se_at_alpha = math.sqrt(0.05 * 0.95 / PERMUTATION_ITERATIONS_2022)
            concern_level = _sample_size_concern_level(
                min_positive_n=min_positive_n,
                harmonic_positive_n=harmonic_positive_n,
                tie_fraction=0.0 if pd.isna(tie_fraction) else tie_fraction,
            )

            rows.append(
                {
                    "tax_year": "2022",
                    "variable": variable,
                    "variable_label": label,
                    "comparison": f"Black Hills vs {benchmark_region}",
                    "benchmark_region": benchmark_region,
                    "black_hills_eligible_n": bh_eligible_n,
                    "benchmark_eligible_n": benchmark_eligible_n,
                    "black_hills_positive_n": bh_positive_n,
                    "benchmark_positive_n": benchmark_positive_n,
                    "min_positive_n": min_positive_n,
                    "total_positive_n": total_positive_n,
                    "harmonic_positive_n": harmonic_positive_n,
                    "black_hills_nonzero_rate": bh_nonzero_rate,
                    "benchmark_nonzero_rate": benchmark_nonzero_rate,
                    "unique_positive_values": unique_positive_values,
                    "tie_fraction_positive_values": tie_fraction,
                    "pooled_positive_median": pooled_positive_median,
                    "pooled_positive_iqr": pooled_positive_iqr,
                    "log10_possible_label_permutations": log10_labelings,
                    "monte_carlo_se_near_p_0_05": monte_carlo_se_at_alpha,
                    "sample_size_concern": concern_level,
                }
            )

    diagnostics = pd.DataFrame(rows)
    if pairwise_summary is not None and not pairwise_summary.empty and not diagnostics.empty:
        diagnostics = diagnostics.merge(
            pairwise_summary,
            on=["variable", "benchmark_region"],
            how="left",
        )
        if {"median_difference_ci_low", "median_difference_ci_high"}.issubset(diagnostics.columns):
            diagnostics["median_difference_ci_width"] = (
                diagnostics["median_difference_ci_high"] - diagnostics["median_difference_ci_low"]
            )
            diagnostics["median_difference_ci_includes_zero"] = (
                diagnostics["median_difference_ci_low"].le(0)
                & diagnostics["median_difference_ci_high"].ge(0)
            )
            diagnostics["ci_width_to_pooled_iqr"] = diagnostics["median_difference_ci_width"] / diagnostics[
                "pooled_positive_iqr"
            ].replace(0, np.nan)
        if "median_difference" in diagnostics.columns:
            diagnostics["absolute_median_difference_to_iqr"] = diagnostics["median_difference"].abs() / diagnostics[
                "pooled_positive_iqr"
            ].replace(0, np.nan)

    return diagnostics


def check_2022_presentation_sample_sizes(sample_df: pd.DataFrame) -> list[dict[str, object]]:
    """Convert 2022 sample-size diagnostics into assumption-status rows."""

    rows: list[dict[str, object]] = []
    if sample_df.empty:
        return rows

    info("Checking 2022 presentation sample-size assumptions.")
    for row in sample_df.to_dict("records"):
        variable = str(row["variable"])
        comparison = str(row["comparison"])
        min_positive_n = int(row["min_positive_n"])
        concern = str(row["sample_size_concern"])
        if concern == "invalid":
            status = "VIOLATED"
        elif concern in {"high", "moderate"}:
            status = "CAVEAT"
        else:
            status = "OK"

        rows.append(
            make_row(
                test="2022_positive_median_permutation",
                assumption="minimum_positive_reporters_per_group",
                variable=variable,
                group=comparison,
                statistic=min_positive_n,
                status=status,
                notes=(
                    f"Positive reporter counts: Black Hills={int(row['black_hills_positive_n'])}, "
                    f"{row['benchmark_region']}={int(row['benchmark_positive_n'])}. "
                    f"Concern level={concern}. Permutation tests remain valid with small samples "
                    "when observations are exchangeable, but low positive n limits power and makes "
                    "the median estimate less precise. Fewer than 2 positive reporters in either "
                    "group would make the positive-only median test unusable."
                ),
            )
        )

        asymptotic_status = "OK"
        if min_positive_n < 2:
            asymptotic_status = "VIOLATED"
        elif min_positive_n < 20 or float(row["tie_fraction_positive_values"]) >= 0.50:
            asymptotic_status = "CAVEAT"
        rows.append(
            make_row(
                test="mann_whitney_positive_only_crosscheck",
                assumption="asymptotic_rank_approximation",
                variable=variable,
                group=comparison,
                statistic=float(row["tie_fraction_positive_values"]),
                status=asymptotic_status,
                notes=(
                    f"Tied positive observations={float(row['tie_fraction_positive_values']):.1%}; "
                    f"harmonic positive n={float(row['harmonic_positive_n']):.1f}. "
                    "Mann-Whitney U is reported as a cross-check. The slide headline uses a "
                    "permutation test on the median difference, so this asymptotic-rank caveat "
                    "does not invalidate the presentation p-values."
                ),
            )
        )

    return rows


def _format_percent(value: float | None) -> str:
    if value is None or pd.isna(value):
        return "NA"
    return f"{float(value) * 100:.1f}%"


def write_sample_size_report(report_md: Path, sample_df: pd.DataFrame) -> None:
    """Write a concise report focused on the 2022 sample-size question."""

    lines: list[str] = []
    lines.append("# Q9 2022 Sample-Size Diagnostics")
    lines.append("")
    lines.append(
        "This report checks whether the 2022 client-presentation tests have a sample-size problem. "
        "The source slides use positive-only medians and a two-sided permutation test on the median "
        "difference, so the key sample size is the number of organizations in each region that report "
        "a positive dollar amount for that source."
    )
    lines.append("")

    if sample_df.empty:
        lines.append("_No diagnostics produced._")
        report_md.parent.mkdir(parents=True, exist_ok=True)
        report_md.write_text("\n".join(lines) + "\n", encoding="utf-8")
        return

    concern_counts = sample_df["sample_size_concern"].value_counts().to_dict()
    lines.append("## Bottom Line")
    lines.append("")
    invalid = int(concern_counts.get("invalid", 0))
    high = int(concern_counts.get("high", 0))
    moderate = int(concern_counts.get("moderate", 0))
    low = int(concern_counts.get("low", 0))
    if invalid:
        lines.append(
            f"- {invalid} comparisons have fewer than 2 positive reporters in at least one group; "
            "those positive-only median tests are not usable."
        )
    else:
        lines.append(
            "- No positive-only median comparison is invalidated by sample size: every comparison "
            "has at least 2 positive reporters in both Black Hills and the benchmark region."
        )
    lines.append(f"- Sample-size concern counts: low={low}, moderate={moderate}, high={high}, invalid={invalid}.")
    lines.append(
        "- Small positive-reporter counts are not a formal permutation-test assumption violation. "
        "They mainly reduce power and widen uncertainty, especially for rare contribution channels."
    )
    lines.append(
        f"- With {PERMUTATION_ITERATIONS_2022:,} random reshuffles, the Monte Carlo standard error near "
        "p=0.05 is about "
        f"{math.sqrt(0.05 * 0.95 / PERMUTATION_ITERATIONS_2022):.3f}; borderline p-values can move slightly "
        "with a different random seed, but the main issue for rare sources is the low number of positive reporters."
    )
    lines.append("")

    limited = sample_df.loc[sample_df["sample_size_concern"].isin(["high", "moderate", "invalid"])].copy()
    if not limited.empty:
        concern_order = {"invalid": 0, "high": 1, "moderate": 2, "low": 3}
        limited["_concern_order"] = limited["sample_size_concern"].map(concern_order).fillna(9)
        limited = limited.sort_values(["_concern_order", "variable", "benchmark_region"])
        lines.append("## Comparisons Most Limited By Sample Size")
        lines.append("")
        lines.append(
            "| Source | Comparison | BH positive n | Benchmark positive n | BH reporting rate | Benchmark reporting rate | Concern |"
        )
        lines.append("| --- | --- | ---: | ---: | ---: | ---: | --- |")
        for row in limited.to_dict("records"):
            lines.append(
                "| "
                f"{row['variable_label']} | {row['comparison']} | "
                f"{int(row['black_hills_positive_n'])} | {int(row['benchmark_positive_n'])} | "
                f"{_format_percent(row['black_hills_nonzero_rate'])} | "
                f"{_format_percent(row['benchmark_nonzero_rate'])} | "
                f"{row['sample_size_concern']} |"
            )
        lines.append("")

    source_summary = (
        sample_df.groupby(["variable", "variable_label"], dropna=False)
        .agg(
            min_positive_n=("min_positive_n", "min"),
            median_min_positive_n=("min_positive_n", "median"),
            max_tie_fraction=("tie_fraction_positive_values", "max"),
            worst_concern=(
                "sample_size_concern",
                lambda values: "invalid"
                if (values == "invalid").any()
                else "high"
                if (values == "high").any()
                else "moderate"
                if (values == "moderate").any()
                else "low",
            ),
        )
        .reset_index()
    )
    lines.append("## Source-Level Summary")
    lines.append("")
    lines.append("| Source | Smallest positive n in any pair | Median of pairwise minimum n | Max tie rate | Worst concern |")
    lines.append("| --- | ---: | ---: | ---: | --- |")
    concern_order = {"invalid": 0, "high": 1, "moderate": 2, "low": 3}
    source_summary["_concern_order"] = source_summary["worst_concern"].map(concern_order).fillna(9)
    for row in source_summary.sort_values(["_concern_order", "min_positive_n"]).to_dict("records"):
        lines.append(
            "| "
            f"{row['variable_label']} | {int(row['min_positive_n'])} | "
            f"{float(row['median_min_positive_n']):.1f} | "
            f"{_format_percent(row['max_tie_fraction'])} | {row['worst_concern']} |"
        )
    lines.append("")

    if "permutation_p_value" in sample_df.columns:
        borderline = sample_df.loc[
            sample_df["permutation_p_value"].between(0.04, 0.06, inclusive="both")
        ].copy()
        lines.append("## Borderline P-Values")
        lines.append("")
        if borderline.empty:
            lines.append("_No 2022 positive-only median p-values are between 0.04 and 0.06._")
        else:
            lines.append("| Source | Comparison | p-value | Concern |")
            lines.append("| --- | --- | ---: | --- |")
            for row in borderline.sort_values("permutation_p_value").to_dict("records"):
                lines.append(
                    f"| {row['variable_label']} | {row['comparison']} | "
                    f"{float(row['permutation_p_value']):.4f} | {row['sample_size_concern']} |"
                )
        lines.append("")

    lines.append("## Interpretation")
    lines.append("")
    lines.append(
        "The rare contribution subcategories are the places where sample size most affects interpretation. "
        "Federated campaign contributions and related organization contributions have the smallest positive "
        "reporter counts; membership dues and fundraising event contributions have more information, but some "
        "benchmark pairs are still modest. For total revenue, total contributions, program service revenue, "
        "mixed / unclassified contributions, and other revenue, non-significant results are less likely to be "
        "explained mainly by sample size because positive reporter counts are much larger."
    )

    report_md.parent.mkdir(parents=True, exist_ok=True)
    report_md.write_text("\n".join(lines) + "\n", encoding="utf-8")
    info(f"Saved sample-size diagnostics report: {report_md}")


# ---------------------------------------------------------------------------
# Normality within groups
# ---------------------------------------------------------------------------


def check_normality(frame: pd.DataFrame) -> list[dict[str, object]]:
    """Describe within-group distribution shape of the share variables.

    Normality is no longer an assumption of the headline share tests because
    those tests are Kruskal-Wallis, Mann-Whitney U, and permutation checks.
    We still report D'Agostino-Pearson, skew, and excess kurtosis because
    extreme shape or zero-heavy distributions affect how rank-test results
    should be interpreted.
    """

    info("Describing distribution shape within comparison groups.")
    rows: list[dict[str, object]] = []
    for variable in SHARE_COMPONENTS:
        if variable not in frame.columns:
            continue
        for group, group_frame in frame.groupby("comparison_group"):
            values = group_frame[variable].dropna().to_numpy(dtype=float)
            if len(values) < 20:
                continue
            try:
                stat, p_value = stats.normaltest(values)
            except Exception:
                continue
            skew = float(stats.skew(values))
            kurt = float(stats.kurtosis(values))
            extreme_shape = (abs(skew) > 1.0) or (abs(kurt) > 3.0)
            if p_value < 0.001 and extreme_shape:
                status = "CAVEAT"
            elif p_value < 0.05:
                status = "CAVEAT"
            else:
                status = "OK"
            rows.append(
                make_row(
                    test="rank_share_tests",
                    assumption="distribution_shape_diagnostic",
                    variable=variable,
                    group=str(group),
                    statistic=stat,
                    p_value=p_value,
                    status=status,
                    notes=(
                        f"D'Agostino-Pearson omnibus test; skew={skew:.2f}, excess_kurtosis={kurt:.2f}. "
                        "Normality is not required for Kruskal-Wallis, Mann-Whitney U, or permutation "
                        "tests. CAVEAT means the share distribution is strongly skewed or heavy-tailed, "
                        "so interpret rank-test findings as distributional differences rather than "
                        "simple mean-only differences."
                    ),
                )
            )
    return rows


def check_raw_dollar_normality(frame: pd.DataFrame) -> list[dict[str, object]]:
    """Check raw and log1p dollar normality for the primary Q9 variables."""

    info("Checking raw-dollar and log1p-dollar normality by region.")
    rows: list[dict[str, object]] = []
    for variable in CLIENT_RAW_LEVEL_VARIABLES:
        if variable not in frame.columns:
            continue
        for region, group_frame in frame.groupby("region_label"):
            values = group_frame[variable].dropna().clip(lower=0).to_numpy(dtype=float)
            if len(values) < 20:
                continue
            log_values = np.log1p(values)
            try:
                raw_stat, raw_p = stats.normaltest(values)
                log_stat, log_p = stats.normaltest(log_values)
            except Exception:
                continue
            mean = float(np.mean(values))
            median = float(np.median(values))
            mean_median_ratio = mean / median if median > 0 else (np.inf if mean > 0 else 1.0)
            skew = float(stats.skew(values, bias=False))
            notes = (
                f"Raw dollars: mean=${mean:,.0f}, median=${median:,.0f}, "
                f"mean/median={'inf' if np.isinf(mean_median_ratio) else f'{mean_median_ratio:.2f}'}, "
                f"zero_rate={np.mean(values == 0):.1%}, skew={skew:.2f}; "
                f"log1p normality p={log_p:.4g}. Normality is rejected for raw dollars "
                "and usually remains rejected after log1p, supporting non-parametric rank tests."
            )
            status = "CAVEAT" if raw_p < 0.05 or log_p < 0.05 else "OK"
            rows.append(
                make_row(
                    test="kruskal_wallis_raw_dollars",
                    assumption="raw_and_log1p_normality_diagnostic",
                    variable=variable,
                    group=str(region),
                    statistic=raw_stat,
                    p_value=raw_p,
                    status=status,
                    notes=notes,
                )
            )
    return rows


# ---------------------------------------------------------------------------
# Homogeneity of variance
# ---------------------------------------------------------------------------


def check_variance_homogeneity(frame: pd.DataFrame) -> list[dict[str, object]]:
    """Check whether rank-test group distributions have similar spreads.

    Kruskal-Wallis does not require equal variances in the same way classical
    ANOVA does. However, if regions have very different spreads or shapes, a
    significant Kruskal-Wallis result should be interpreted as a difference in
    distributions, not strictly as a difference in central tendency.
    """

    info("Checking distribution spread similarity across regions.")
    rows: list[dict[str, object]] = []
    for variable in SHARE_COMPONENTS:
        if variable not in frame.columns:
            continue
        groups = [
            group_frame[variable].dropna().to_numpy(dtype=float)
            for _, group_frame in frame.groupby("region_label")
        ]
        groups = [arr for arr in groups if len(arr) >= 5]
        if len(groups) < 2:
            continue
        try:
            stat, p_value = stats.levene(*groups, center="median")
        except Exception:
            continue
        if p_value < 0.05:
            status = "CAVEAT"
            note = (
                "Levene/Brown-Forsythe suggests unequal spreads across regions. Kruskal-Wallis "
                "remains a valid distributional rank test, but a significant result may reflect "
                "spread/shape differences as well as central tendency differences."
            )
        else:
            status = "OK"
            note = "Group spreads are comparable enough for a central-tendency interpretation of the rank test."
        rows.append(
            make_row(
                test="kruskal_wallis_share",
                assumption="similar_distribution_spread",
                variable=variable,
                group="region_label",
                statistic=stat,
                p_value=p_value,
                status=status,
                notes=note,
            )
        )
    return rows


# ---------------------------------------------------------------------------
# Bounded outcome and zero inflation diagnostics
# ---------------------------------------------------------------------------


def check_zero_inflation(frame: pd.DataFrame) -> list[dict[str, object]]:
    """Quantify zero inflation in share variables, by group.

    Many orgs report zero on a particular Line 1 sub-channel. This is part of
    why the analysis layers logistic presence models and permutation checks on
    top of the share rank tests.
    """

    info("Quantifying zero inflation in share variables.")
    rows: list[dict[str, object]] = []
    for variable in SHARE_COMPONENTS:
        if variable not in frame.columns:
            continue
        for group, group_frame in frame.groupby("comparison_group"):
            values = group_frame[variable].dropna()
            if values.empty:
                continue
            zero_rate = float((values <= 0).mean())
            status = "CAVEAT" if zero_rate >= 0.50 else "OK"
            rows.append(
                make_row(
                    test="rank_share_tests",
                    assumption="continuous_outcome_minimal_zero_inflation",
                    variable=variable,
                    group=str(group),
                    statistic=zero_rate,
                    status=status,
                    notes=(
                        f"{zero_rate:.1%} of {len(values):,} rows have a zero share. "
                        "The analysis pairs each share rank test with a logistic presence model "
                        "(does the org report any of this source) so size and presence effects can "
                        "be separated."
                    ),
                )
            )
    return rows


# ---------------------------------------------------------------------------
# OLS residual diagnostics
# ---------------------------------------------------------------------------


def check_ols_residuals(frame: pd.DataFrame) -> list[dict[str, object]]:
    """Run residual diagnostics for the EIN-clustered OLS share regressions.

    Cluster-robust SEs already protect inference against heteroscedasticity
    and within-cluster correlation, but we still report Breusch-Pagan and a
    Durbin-Watson statistic so the user knows which OLS assumptions are
    violated.
    """

    info("Running OLS residual diagnostics for share regressions.")
    rows: list[dict[str, object]] = []
    try:
        import statsmodels.api as sm
        from statsmodels.stats.diagnostic import het_breuschpagan
        from statsmodels.stats.stattools import durbin_watson
    except Exception:  # pragma: no cover - statsmodels is a hard dependency of the main analysis
        return rows

    for variable in SHARE_COMPONENTS:
        if variable not in frame.columns:
            continue
        sub = frame[[variable, "is_black_hills", "tax_year", "form_type"]].dropna().copy()
        if sub.empty or sub["is_black_hills"].nunique() < 2:
            continue
        sub["tax_year"] = sub["tax_year"].astype(str)
        sub["form_type"] = sub["form_type"].astype(str)
        design = pd.get_dummies(sub[["tax_year", "form_type"]], drop_first=True)
        x = pd.concat(
            [pd.Series(1.0, index=sub.index, name="const"), sub["is_black_hills"].astype(float), design.astype(float)],
            axis=1,
        )
        try:
            model = sm.OLS(sub[variable].astype(float), x).fit()
        except Exception:
            continue
        residuals = np.asarray(model.resid, dtype=float)
        try:
            bp_stat, bp_p, _, _ = het_breuschpagan(residuals, np.asarray(x, dtype=float))
        except Exception:
            bp_stat, bp_p = np.nan, np.nan
        dw = float(durbin_watson(residuals))
        bp_status = "CAVEAT" if (not np.isnan(bp_p) and bp_p < 0.05) else "OK"
        rows.append(
            make_row(
                test="ols_clustered_by_ein",
                assumption="homoscedasticity_of_residuals",
                variable=variable,
                statistic=bp_stat,
                p_value=bp_p,
                status=bp_status,
                notes=(
                    "Breusch-Pagan test for heteroscedasticity. Cluster-robust SEs in the main "
                    "analysis already adjust for both heteroscedasticity and within-EIN correlation, "
                    "so a CAVEAT here does not invalidate inference."
                ),
            )
        )
        dw_status = "CAVEAT" if dw < 1.5 or dw > 2.5 else "OK"
        rows.append(
            make_row(
                test="ols_clustered_by_ein",
                assumption="independence_of_residuals_durbin_watson",
                variable=variable,
                statistic=dw,
                status=dw_status,
                notes=(
                    "Durbin-Watson statistic. Values far from 2 indicate residual autocorrelation "
                    "(typically within EIN across years). Cluster-robust SEs handle this; reported "
                    "for transparency."
                ),
            )
        )
    return rows


# ---------------------------------------------------------------------------
# Logistic regression separation/convergence
# ---------------------------------------------------------------------------


def check_logistic_separation(frame: pd.DataFrame) -> list[dict[str, object]]:
    """Sanity-check the logistic presence model for quasi-separation.

    Quasi-separation can produce huge coefficients with huge SEs that look
    significant but are unstable. We flag share variables where one group has
    near-zero or near-one presence rates (where separation is most likely).
    """

    info("Checking for quasi-separation in logistic presence models.")
    rows: list[dict[str, object]] = []
    for variable in SHARE_COMPONENTS:
        if variable not in frame.columns:
            continue
        sub = frame[["is_black_hills", variable]].dropna()
        if sub.empty:
            continue
        sub = sub.assign(present=(sub[variable] > 0).astype(int))
        rates = sub.groupby("is_black_hills")["present"].mean()
        if len(rates) < 2:
            continue
        rate_by_group = {int(group): float(rate) for group, rate in rates.items()}
        black_hills_rate = rate_by_group.get(1, np.nan)
        benchmark_rate = rate_by_group.get(0, np.nan)
        any_extreme = bool((rates < 0.02).any() or (rates > 0.98).any())
        status = "CAVEAT" if any_extreme else "OK"
        rows.append(
            make_row(
                test="logistic_presence_clustered_by_ein",
                assumption="absence_of_quasi_separation",
                variable=variable,
                statistic=float(min(black_hills_rate, benchmark_rate)),
                status=status,
                notes=(
                    f"Presence rates by group: BH={black_hills_rate:.3f}, "
                    f"Benchmark={benchmark_rate:.3f}. Rates near 0 or 1 indicate "
                    "quasi-separation risk; large coefficients with large SEs in the regression "
                    "table for this variable should be interpreted with caution."
                ),
            )
        )
    return rows


# ---------------------------------------------------------------------------
# PERMANOVA beta dispersion
# ---------------------------------------------------------------------------


def check_permanova_dispersion(frame: pd.DataFrame) -> list[dict[str, object]]:
    """PERMDISP-style check that group dispersions in CLR space are similar.

    PERMANOVA is sensitive to differences in within-group dispersion: a
    significant PERMANOVA can sometimes reflect dispersion differences rather
    than location differences. We report Levene on each CLR axis as a
    coarse PERMDISP-style diagnostic.
    """

    info("Checking PERMANOVA-CLR dispersion homogeneity.")
    rows: list[dict[str, object]] = []
    components = composition_matrix(frame)
    if components.empty:
        return rows
    nonzero = components.replace(0, np.nan)
    with np.errstate(divide="ignore", invalid="ignore"):
        geometric_mean = np.exp(np.log(nonzero).mean(axis=1))
        clr = np.log(nonzero.div(geometric_mean, axis=0))
    clr = clr.dropna(how="any")
    if clr.empty:
        return rows
    aligned_groups = frame.loc[clr.index, "comparison_group"]
    for column in clr.columns:
        groups = [
            clr.loc[aligned_groups.eq(group_label), column].dropna().to_numpy(dtype=float)
            for group_label in aligned_groups.dropna().unique()
        ]
        groups = [arr for arr in groups if len(arr) >= 5]
        if len(groups) < 2:
            continue
        try:
            stat, p_value = stats.levene(*groups, center="median")
        except Exception:
            continue
        status = "CAVEAT" if p_value < 0.05 else "OK"
        rows.append(
            make_row(
                test="permanova_clr",
                assumption="similar_within_group_dispersion",
                variable=f"clr_{column}",
                statistic=stat,
                p_value=p_value,
                status=status,
                notes=(
                    "Levene-on-CLR-axis as a coarse PERMDISP-style check. CAVEAT means part of "
                    "the PERMANOVA signal could reflect spread differences rather than location "
                    "differences; the MANOVA-Pillai test triangulates the conclusion."
                ),
            )
        )
    return rows


# ---------------------------------------------------------------------------
# MANOVA: multivariate normality
# ---------------------------------------------------------------------------


def check_manova_normality(frame: pd.DataFrame) -> list[dict[str, object]]:
    """Approximate multivariate normality check on ALR-transformed shares.

    Pillai's trace is used in the analysis specifically because it is the
    most robust of the four classical MANOVA statistics to multivariate
    non-normality. We still report whether the marginal ALR axes look
    skewed/heavy-tailed, so the user can judge how much robustness is needed.
    """

    info("Checking MANOVA-ALR multivariate normality (per-axis diagnostics).")
    rows: list[dict[str, object]] = []
    components = composition_matrix(frame)
    if components.empty or components.shape[1] < 2:
        return rows
    base = components.iloc[:, -1].replace(0, np.nan)
    alr_columns = []
    for column in components.columns[:-1]:
        with np.errstate(divide="ignore", invalid="ignore"):
            alr_columns.append(np.log(components[column].replace(0, np.nan) / base))
    alr = pd.concat(alr_columns, axis=1, keys=[f"alr_{name}" for name in components.columns[:-1]])
    alr = alr.dropna(how="any")
    if alr.empty:
        return rows
    for column in alr.columns:
        values = alr[column].to_numpy(dtype=float)
        if len(values) < 20:
            continue
        skew = float(stats.skew(values))
        kurt = float(stats.kurtosis(values))
        extreme = abs(skew) > 1.0 or abs(kurt) > 3.0
        status = "CAVEAT" if extreme else "OK"
        rows.append(
            make_row(
                test="manova_alr_pillai",
                assumption="approximate_multivariate_normality",
                variable=column,
                statistic=skew,
                status=status,
                notes=(
                    f"Per-axis skew={skew:.2f}, excess_kurtosis={kurt:.2f}. Pillai's trace is the "
                    "most robust MANOVA statistic to multivariate non-normality, which is why the "
                    "main analysis uses it; treat CAVEAT as informational."
                ),
            )
        )
    return rows


# ---------------------------------------------------------------------------
# Permutation/PERMANOVA exchangeability under H0
# ---------------------------------------------------------------------------


def check_exchangeability(frame: pd.DataFrame) -> list[dict[str, object]]:
    """Document the exchangeability assumption for permutation-style tests.

    Strict exchangeability under H0 requires permuting at the cluster (EIN)
    level when the same EIN files multiple times. The main analysis permutes
    at the row level. We log this as a CAVEAT and point to the cluster
    bootstrap and the one-row-per-EIN sensitivity as triangulating evidence.
    """

    info("Documenting permutation exchangeability assumption.")
    rows: list[dict[str, object]] = []
    rows.append(
        make_row(
            test="permutation_mean_diff",
            assumption="exchangeability_under_H0",
            status="CAVEAT",
            notes=(
                "Row-level permutation. Strict exchangeability with repeated EINs would require "
                "permuting at the EIN level. The cluster bootstrap CIs and the one-row-per-EIN "
                "sensitivity provide cluster-aware triangulation for the row-level permutation "
                "results."
            ),
        )
    )
    rows.append(
        make_row(
            test="permanova_clr",
            assumption="exchangeability_under_H0",
            status="CAVEAT",
            notes=(
                "Same row-level permutation caveat as above. PERMDISP-style dispersion check "
                "is reported per CLR axis; the MANOVA-Pillai test provides parametric "
                "triangulation."
            ),
        )
    )
    return rows


# ---------------------------------------------------------------------------
# Cluster-bootstrap effective sample size sanity
# ---------------------------------------------------------------------------


def check_cluster_bootstrap(frame: pd.DataFrame) -> list[dict[str, object]]:
    """Sanity-check the cluster-bootstrap setup.

    The cluster bootstrap requires enough EINs per group for the resampling
    to be informative. With fewer than ~30 clusters per group, cluster-robust
    inference can be unreliable.
    """

    info("Checking cluster-bootstrap sample sizes.")
    rows: list[dict[str, object]] = []
    if "comparison_group" not in frame.columns or "ein" not in frame.columns:
        return rows
    cluster_counts = frame.groupby("comparison_group")["ein"].nunique()
    for group_label, count in cluster_counts.items():
        if count >= 100:
            status = "OK"
        elif count >= 30:
            status = "CAVEAT"
        else:
            status = "VIOLATED"
        rows.append(
            make_row(
                test="cluster_bootstrap_ci",
                assumption="sufficient_cluster_count_per_group",
                group=str(group_label),
                statistic=int(count),
                status=status,
                notes=(
                    f"{int(count)} unique EINs in group {group_label!r}. Cluster bootstrap CIs "
                    "are reliable from roughly 30+ clusters per group; below that, treat the "
                    "interval width as a lower bound on uncertainty."
                ),
            )
        )
    return rows


# ---------------------------------------------------------------------------
# Composition reconciliation (already produced by the main script, surfaced here)
# ---------------------------------------------------------------------------


def check_composition_closure(frame: pd.DataFrame) -> list[dict[str, object]]:
    """Confirm that reported segment shares partition revenue (~100%).

    Mutually exclusive segments are an analytic *requirement* of the Q9 plan
    and a precondition for stacked-bar interpretation. The main script
    reports an overlap diagnostic; we surface a single summary row here so
    the assumptions report is self-contained.
    """

    info("Confirming composition closure of the detailed revenue segments.")
    rows: list[dict[str, object]] = []
    component_columns = [column for column in SOURCE_COMPONENTS if column in frame.columns]
    if not component_columns or "total_revenue" not in frame.columns:
        return rows
    sub = frame[component_columns + ["total_revenue", "comparison_group"]].dropna(subset=["total_revenue"])
    if sub.empty:
        return rows
    component_sum = sub[component_columns].clip(lower=0).sum(axis=1)
    share_sum = component_sum / sub["total_revenue"].replace(0, np.nan)
    over_100 = float((share_sum > 1.05).mean())
    under_95 = float((share_sum < 0.95).mean())
    status = "CAVEAT" if (over_100 + under_95) > 0.10 else "OK"
    rows.append(
        make_row(
            test="all_share_tests",
            assumption="mutually_exclusive_segments_sum_to_total_revenue",
            statistic=float(share_sum.mean()),
            status=status,
            notes=(
                f"Mean component-share sum = {share_sum.mean():.3f}. "
                f"{over_100:.1%} of rows are above 105% of total revenue; "
                f"{under_95:.1%} of rows are below 95%. Detail rows are listed in "
                "tables/negative_residual_diagnostics.csv. Because the share variables are "
                "parts of a whole, separate share rank tests are univariate follow-ups; the "
                "PERMANOVA-style CLR test is the overall revenue-mix test."
            ),
        )
    )
    return rows


# ---------------------------------------------------------------------------
# Report writer
# ---------------------------------------------------------------------------


def _summarize_status(rows: Iterable[dict[str, object]]) -> tuple[int, int, int]:
    """Count OK / CAVEAT / VIOLATED rows for the markdown summary."""

    ok = caveat = violated = 0
    for row in rows:
        status = str(row.get("status", ""))
        if status == "OK":
            ok += 1
        elif status == "CAVEAT":
            caveat += 1
        elif status == "VIOLATED":
            violated += 1
    return ok, caveat, violated


def _markdown_table(rows: list[dict[str, object]]) -> str:
    """Render the assumptions rows as a compact Markdown table."""

    if not rows:
        return "_No diagnostics produced._"
    headers = ["test", "assumption", "variable", "group", "statistic", "p_value", "status", "notes"]
    lines = ["| " + " | ".join(headers) + " |", "| " + " | ".join(["---"] * len(headers)) + " |"]
    for row in rows:
        cells = []
        for header in headers:
            value = row.get(header, "")
            if value is None:
                cells.append("")
            else:
                cells.append(str(value).replace("|", "\\|"))
        lines.append("| " + " | ".join(cells) + " |")
    return "\n".join(lines)


def write_report(report_md: Path, rows: list[dict[str, object]], frame: pd.DataFrame) -> None:
    """Write the human-readable assumptions report alongside the analysis."""

    ok, caveat, violated = _summarize_status(rows)
    grouped: dict[str, list[dict[str, object]]] = {}
    for row in rows:
        grouped.setdefault(str(row["test"]), []).append(row)

    lines: list[str] = []
    lines.append("# Revenue-Sources Assumptions Check")
    lines.append("")
    lines.append(
        "This report empirically checks the assumptions behind every test reported in "
        "`revenue_sources_black_hills_results.md`. Each row in the table below is one assumption, "
        "evaluated for one variable (and where relevant, one group). The status column flags whether "
        "the assumption holds (`OK`), is formally violated but mitigated by the analysis design "
        "(`CAVEAT`), or is violated without any mitigation in place (`VIOLATED`)."
    )
    lines.append("")
    lines.append(f"- Org-years analyzed: {len(frame):,}")
    lines.append(f"- Unique EINs: {frame['ein'].nunique():,}")
    lines.append(f"- Tax years: {sorted(frame['tax_year'].unique().tolist())}")
    lines.append(f"- Regions: {sorted(frame['region_label'].unique().tolist())}")
    lines.append("")
    lines.append("## Summary")
    lines.append("")
    lines.append(f"- OK rows: {ok}")
    lines.append(f"- CAVEAT rows: {caveat}")
    lines.append(f"- VIOLATED rows: {violated}")
    lines.append("")
    if violated == 0:
        lines.append(
            "**Verdict:** every assumption that is formally violated by the data is mitigated by "
            "the analysis design (rank tests, distribution-free permutation, compositional "
            "PERMANOVA-style testing, cluster-robust SEs, MANOVA-Pillai triangulation, "
            "one-row-per-EIN sensitivity, or cluster bootstrap). Use the caveat rows below to "
            "separate distribution-shape, compositional, clustering, and sample-size limits from "
            "the substantive Q9 result."
        )
    else:
        lines.append(
            "**Verdict:** at least one assumption is violated without mitigation. See the "
            "`VIOLATED` rows below before relying on the affected test."
        )
    lines.append("")
    for test_name, test_rows in grouped.items():
        lines.append(f"## {test_name}")
        lines.append("")
        lines.append(_markdown_table(test_rows))
        lines.append("")
    report_md.parent.mkdir(parents=True, exist_ok=True)
    report_md.write_text("\n".join(lines) + "\n", encoding="utf-8")
    info(f"Saved assumptions report: {report_md}")


# ---------------------------------------------------------------------------
# Top-level orchestration
# ---------------------------------------------------------------------------


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    """Parse CLI options for the assumptions checker."""

    parser = argparse.ArgumentParser(
        description="Empirically check the statistical assumptions behind the Black Hills revenue-source analysis.",
    )
    parser.add_argument("--data-root", type=Path, default=DEFAULT_DATA_ROOT, help="Project 01_data root.")
    parser.add_argument("--output-dir", type=Path, default=None, help="Analysis output directory (must contain the cleaned parquet).")
    parser.add_argument("--results-dir", type=Path, default=None, help="User-facing results directory; defaults to revenue_sources_black_hills/results.")
    return parser.parse_args(argv)


def run(
    *,
    data_root: Path,
    output_dir: Path | None,
    results_dir: Path | None,
) -> None:
    """Run all assumption checks and write the report."""

    paths = resolve_paths(data_root, output_dir, results_dir)
    if not paths.cleaned_parquet.exists():
        raise FileNotFoundError(
            "Could not find the cleaned analysis parquet at "
            f"{paths.cleaned_parquet}. Run revenue_sources_black_hills.py first."
        )
    info(f"Loading cleaned analysis frame: {paths.cleaned_parquet}")
    frame = pd.read_parquet(paths.cleaned_parquet)
    if "tax_year" in frame.columns:
        frame["tax_year"] = frame["tax_year"].astype(str)
    pairwise_2022_summary = _load_pairwise_2022_summary(paths.pairwise_2022_summary_csv)
    sample_size_diagnostics = build_2022_sample_size_diagnostics(frame, pairwise_2022_summary)

    rows: list[dict[str, object]] = []
    rows.extend(check_independence(frame))
    rows.extend(check_rank_test_sample_sizes(frame))
    rows.extend(check_2022_presentation_sample_sizes(sample_size_diagnostics))
    rows.extend(check_raw_dollar_normality(frame))
    rows.extend(check_normality(frame))
    rows.extend(check_variance_homogeneity(frame))
    rows.extend(check_zero_inflation(frame))
    rows.extend(check_ols_residuals(frame))
    rows.extend(check_logistic_separation(frame))
    rows.extend(check_permanova_dispersion(frame))
    rows.extend(check_manova_normality(frame))
    rows.extend(check_exchangeability(frame))
    rows.extend(check_cluster_bootstrap(frame))
    rows.extend(check_composition_closure(frame))

    details = pd.DataFrame(rows)
    paths.details_csv.parent.mkdir(parents=True, exist_ok=True)
    details.to_csv(paths.details_csv, index=False)
    info(f"Saved assumptions detail table: {paths.details_csv} ({len(details):,} rows)")

    paths.sample_size_csv.parent.mkdir(parents=True, exist_ok=True)
    sample_size_diagnostics.to_csv(paths.sample_size_csv, index=False)
    info(
        f"Saved 2022 sample-size diagnostics table: "
        f"{paths.sample_size_csv} ({len(sample_size_diagnostics):,} rows)"
    )
    write_sample_size_report(paths.sample_size_report_md, sample_size_diagnostics)

    write_report(paths.report_md, rows, frame)
    ok, caveat, violated = _summarize_status(rows)
    info(f"Assumption status summary: OK={ok}, CAVEAT={caveat}, VIOLATED={violated}")


def main(argv: list[str] | None = None) -> None:
    """CLI entry point."""

    args = parse_args(argv)
    run(
        data_root=args.data_root.resolve(),
        output_dir=args.output_dir.resolve() if args.output_dir else None,
        results_dir=args.results_dir.resolve() if args.results_dir else None,
    )


if __name__ == "__main__":
    main()
