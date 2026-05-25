"""Validate 2022 pairwise direction/significance vs positive medians."""

from __future__ import annotations

import math
import sys
from pathlib import Path

import numpy as np
import pandas as pd
from scipy import stats

REPO = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO / "python"))

import analysis.revenue_sources_black_hills.revenue_sources_black_hills as ra  # noqa: E402


def expected_direction(bh: float, bm: float) -> str:
    if pd.isna(bh) or pd.isna(bm):
        return ""
    if math.isclose(float(bh), float(bm), rel_tol=0, abs_tol=0.001):
        return "No median difference among reporters"
    if bh > bm:
        return "Black Hills higher among reporters"
    return "Black Hills lower among reporters"


def recompute_mann_whitney(frame: pd.DataFrame, variable: str, benchmark: str) -> tuple[float, float]:
    year = frame.loc[frame["tax_year"].astype(str).eq("2022")]
    bh = year.loc[year["region_label"].eq("Black Hills"), variable].dropna().clip(lower=0)
    bm = year.loc[year["region_label"].eq(benchmark), variable].dropna().clip(lower=0)
    bh_pos = bh.loc[bh.gt(0)]
    bm_pos = bm.loc[bm.gt(0)]
    if len(bh_pos) < 2 or len(bm_pos) < 2:
        return np.nan, np.nan
    u, p = stats.mannwhitneyu(
        bh_pos.to_numpy(dtype=float),
        bm_pos.to_numpy(dtype=float),
        alternative="two-sided",
    )
    return float(u), float(p)


def main() -> int:
    csv_paths = list(REPO.rglob("client_2022_pairwise_presentation_summary.csv"))
    if not csv_paths:
        print("ERROR: client_2022_pairwise_presentation_summary.csv not found")
        return 1

    summary = pd.read_csv(csv_paths[0])
    print(f"Loaded {len(summary)} rows from {csv_paths[0].relative_to(REPO)}")

    direction_issues: list[str] = []
    sig_issues: list[str] = []
    mann_issues: list[str] = []
    ci_issues: list[str] = []

    for _, row in summary.iterrows():
        bh = row["black_hills_positive_median"]
        bm = row["benchmark_positive_median"]
        exp_dir = expected_direction(bh, bm)
        if row["direction"] != exp_dir:
            direction_issues.append(
                f"{row['variable_label']} vs {row['benchmark_region']}: "
                f"stored={row['direction']!r} expected={exp_dir!r} "
                f"(BH={bh}, BM={bm})"
            )

        p = row["p_value"]
        exp_sig = "Yes" if pd.notna(p) and float(p) < 0.05 else "No"
        if row["significant_before_fdr"] != exp_sig:
            sig_issues.append(
                f"{row['variable_label']} vs {row['benchmark_region']}: "
                f"stored={row['significant_before_fdr']} expected={exp_sig} (p={p})"
            )

        # Headline p_value should be the permutation p when present.
        perm_p = row.get("permutation_p_value")
        if pd.notna(perm_p) and pd.notna(p) and not math.isclose(float(perm_p), float(p), rel_tol=0, abs_tol=1e-12):
            sig_issues.append(
                f"{row['variable_label']} vs {row['benchmark_region']}: "
                f"p_value column ({p}) is not the permutation p ({perm_p})"
            )

        # CI must contain the point estimate when both are present.
        gap = row.get("median_difference")
        low = row.get("median_difference_ci_low")
        high = row.get("median_difference_ci_high")
        if pd.notna(gap) and pd.notna(low) and pd.notna(high):
            if not (float(low) - 1e-6 <= float(gap) <= float(high) + 1e-6):
                ci_issues.append(
                    f"{row['variable_label']} vs {row['benchmark_region']}: "
                    f"median_difference {gap} outside 95% CI [{low}, {high}]"
                )

    parquet_paths = list(REPO.rglob("cleaned_revenue_sources_analysis.parquet"))
    if parquet_paths:
        frame = pd.read_parquet(parquet_paths[0])
        if "tax_year" not in frame.columns and "year" in frame.columns:
            frame["tax_year"] = frame["year"]
        for _, row in summary.iterrows():
            u_exp, p_exp = recompute_mann_whitney(frame, row["variable"], row["benchmark_region"])
            u = row["mann_whitney_u"]
            mw_p = row.get("mann_whitney_p_value", row.get("p_value"))
            if pd.notna(u_exp) and pd.notna(u):
                if not math.isclose(float(u), float(u_exp), rel_tol=1e-6, abs_tol=0.5):
                    mann_issues.append(
                        f"{row['variable_label']} vs {row['benchmark_region']}: "
                        f"U stored={u} recomputed={u_exp}"
                    )
            if pd.notna(p_exp) and pd.notna(mw_p):
                if not math.isclose(float(mw_p), float(p_exp), rel_tol=0, abs_tol=1e-9):
                    mann_issues.append(
                        f"{row['variable_label']} vs {row['benchmark_region']}: "
                        f"mann_whitney_p stored={mw_p} recomputed={p_exp}"
                    )

    print(f"Direction mismatches: {len(direction_issues)}")
    print(f"Significance mismatches: {len(sig_issues)}")
    print(f"Mann-Whitney recomputation mismatches: {len(mann_issues)}")
    print(f"CI containment mismatches: {len(ci_issues)}")

    sig = summary[summary["significant_before_fdr"] == "Yes"]
    print(f"\nSignificant comparisons ({len(sig)}):")
    for _, row in sig.iterrows():
        print(
            f"  {row['variable_label']} vs {row['benchmark_region']}: "
            f"{row['direction']}; BH median=${row['black_hills_positive_median']:,.0f}; "
            f"benchmark=${row['benchmark_positive_median']:,.0f}; p={row['p_value']:.4f}"
        )

    # Cases where significant but medians are very close
    print("\nSignificant but median gap < $1,000:")
    for _, row in sig.iterrows():
        gap = abs(float(row["black_hills_positive_median"]) - float(row["benchmark_positive_median"]))
        if gap < 1000:
            print(f"  {row['variable_label']} vs {row['benchmark_region']}: gap=${gap:,.0f}")

    all_issues = direction_issues + sig_issues + mann_issues + ci_issues
    if all_issues:
        print("\nFirst issues:")
        for msg in all_issues[:15]:
            print(" ", msg)
        return 1

    print("\nOK: direction always matches positive medians; significance matches p < 0.05.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
