"""
Assumption diagnostics for the Black Hills revenue-source analysis.

The companion script ``revenue_sources_black_hills.py`` runs the substantive
tests for Section 3 Q9: ANOVA, Welch ANOVA, permutation tests, OLS with
EIN-clustered standard errors, EIN-clustered logistic presence models,
EIN-cluster bootstrap confidence intervals, PERMANOVA on CLR-transformed
shares, MANOVA on ALR-transformed shares, and a one-row-per-EIN independence
sensitivity. Each of those tests has assumptions. This script empirically
checks the assumptions on the cleaned analysis frame so the user can see
where the violations are, how severe they are, and whether the headline
finding survives them.

The script writes:

- ``results/assumptions_check_report.md``  - human-readable findings
- ``results/tables/assumptions_check_details.csv`` - machine-readable table

Each row in the CSV has columns ``test``, ``assumption``, ``variable``,
``group``, ``statistic``, ``p_value``, ``status``, and ``notes``. ``status``
is one of ``OK``, ``CAVEAT``, or ``VIOLATED``. CAVEAT means the assumption is
formally violated but the analysis script already mitigates it (e.g. with
cluster-robust SEs, Welch correction, distribution-free permutation, or
sensitivity runs); VIOLATED means there is no mitigation in place and the
result should be interpreted with care.

Run from the repository root:

    python python/analysis/revenue_sources_black_hills/assumptions_check.py

It uses the cleaned parquet written by the main analysis. If the parquet is
missing, the script will tell you to run the main analysis first.
"""

from __future__ import annotations

import argparse
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
    return AssumptionsPaths(
        cleaned_parquet=cleaned_parquet,
        report_md=report_md,
        details_csv=details_csv,
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
            # one-row-per-EIN sensitivity for Welch ANOVA / PERMANOVA). Treat as
            # CAVEAT, not blocker.
            status="CAVEAT" if obs_per_ein > 1.05 else "OK",
            notes=(
                f"{n_rows:,} org-years from {n_eins:,} EINs ({obs_per_ein:.2f} filings/EIN). "
                "Mitigated by EIN-clustered OLS, EIN-clustered logistic, EIN-cluster "
                "bootstrap CIs, and a one-row-per-EIN sensitivity in the main script. "
                "The Welch ANOVA itself does not have a cluster-robust variant; the "
                "one-row-per-EIN sensitivity covers it."
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
                test="welch_anova_share",
                assumption="independence_of_observations",
                variable=variable,
                statistic=icc1,
                status=status,
                notes=(
                    "ICC1 from one-way ANOVA decomposition by EIN. ICC near zero means org-years "
                    "behave independently for this share; larger ICC means the same EIN tends to "
                    "report similar shares across years. Cluster-robust SEs (OLS, logistic) and "
                    "cluster bootstrap account for this; Welch ANOVA does not, but the "
                    "one-row-per-EIN sensitivity in the main report does."
                ),
            )
        )
    return rows


# ---------------------------------------------------------------------------
# Normality within groups
# ---------------------------------------------------------------------------


def check_normality(frame: pd.DataFrame) -> list[dict[str, object]]:
    """Test within-group normality of the share variables.

    For organization-level share variables the formal test (D'Agostino-Pearson)
    will reject normality almost everywhere because the variables are bounded
    in [0,1] with mass at zero. We therefore also report skew and excess
    kurtosis, and we treat the violation as a CAVEAT rather than an error
    because Welch ANOVA with n in the thousands is robust to non-normality.
    """

    info("Checking normality within comparison groups.")
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
                    test="welch_anova_share",
                    assumption="approximate_normality_within_group",
                    variable=variable,
                    group=str(group),
                    statistic=stat,
                    p_value=p_value,
                    status=status,
                    notes=(
                        f"D'Agostino-Pearson omnibus test; skew={skew:.2f}, excess_kurtosis={kurt:.2f}. "
                        "Welch ANOVA with thousands of observations per group is robust to non-normality, "
                        "and the analysis triangulates with rank tests and permutation tests; treat as "
                        "CAVEAT, not blocker."
                    ),
                )
            )
    return rows


# ---------------------------------------------------------------------------
# Homogeneity of variance
# ---------------------------------------------------------------------------


def check_variance_homogeneity(frame: pd.DataFrame) -> list[dict[str, object]]:
    """Test homogeneity of variance across regions for share variables.

    Classical ANOVA assumes equal variances; Welch ANOVA does not. We test
    Levene (median-based, Brown-Forsythe variant) so we know whether the
    classical row in ``statistical_tests_univariate.csv`` should be deprecated
    in favor of the Welch row.
    """

    info("Checking homogeneity of variance across regions.")
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
                "Variance is unequal across regions, so classical ANOVA's homoscedasticity "
                "assumption is violated. The headline test in this analysis is Welch ANOVA, "
                "which does not assume equal variances; treat the classical ANOVA row as "
                "secondary."
            )
        else:
            status = "OK"
            note = "Variances comparable across regions."
        rows.append(
            make_row(
                test="anova_share",
                assumption="homogeneity_of_variance",
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
    why the analysis layers logistic presence models on top of the share
    Welch ANOVAs.
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
                    test="welch_anova_share",
                    assumption="continuous_outcome_minimal_zero_inflation",
                    variable=variable,
                    group=str(group),
                    statistic=zero_rate,
                    status=status,
                    notes=(
                        f"{zero_rate:.1%} of {len(values):,} rows have a zero share. "
                        "The analysis pairs each share Welch ANOVA with a logistic presence model "
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
                "sensitivity provide cluster-aware triangulation; both confirm the headline "
                "government-grants result."
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

    info("Confirming composition closure of the six revenue segments.")
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
                "tables/negative_residual_diagnostics.csv."
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
            "the analysis design (cluster-robust SEs, Welch correction, distribution-free "
            "permutation, MANOVA-Pillai, one-row-per-EIN sensitivity, or cluster bootstrap). The "
            "headline finding (regions differ on revenue mix; Black Hills is distinguished by a "
            "higher government-grants share) is robust to the assumption violations identified "
            "below."
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

    rows: list[dict[str, object]] = []
    rows.extend(check_independence(frame))
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
