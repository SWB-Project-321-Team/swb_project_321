"""Compare 2022 pairwise presentation results with vs without special orgs."""

from __future__ import annotations

import sys
from pathlib import Path

import pandas as pd

REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT / "python" / "analysis" / "revenue_sources_black_hills"))
sys.path.insert(0, str(REPO_ROOT / "python"))
import revenue_sources_black_hills as ra  # noqa: E402
from utils.paths import DATA  # noqa: E402


def sig_label(p: float) -> str:
    if pd.isna(p):
        return "na"
    return "sig" if float(p) < 0.05 else "ns"


def summarize_special_orgs(year: pd.DataFrame) -> None:
    print("=== 2022 row counts by region (full universe) ===")
    for region in ra.REGION_ORDER:
        region_rows = year.loc[year["region_label"].eq(region)]
        special = region_rows.loc[region_rows["special_org"]]
        print(
            f"{region}: all={len(region_rows):,}  special={len(special):,}  "
            f"(hosp={int(region_rows['is_hospital'].fillna(False).sum())}, "
            f"uni={int(region_rows['is_university'].fillna(False).sum())}, "
            f"pol={int(region_rows['is_political_org'].fillna(False).sum())})"
        )
    print()
    print(
        f"Total 2022 valid universe: {len(year):,}; special org rows: {int(year['special_org'].sum()):,}; "
        f"EINs flagged: {year.loc[year['special_org'], 'ein'].nunique():,}"
    )


def compare_pairwise(excluded: pd.DataFrame, included: pd.DataFrame) -> None:
    tbl_excl = ra.build_2022_pairwise_presentation_table(excluded, ra.CLIENT_PRESENTATION_VARIABLES)
    tbl_incl = ra.build_2022_pairwise_presentation_table(included, ra.CLIENT_PRESENTATION_VARIABLES)

    key_cols = [
        "variable",
        "benchmark_region",
        "direction",
        "p_value",
        "black_hills_positive_median",
        "benchmark_positive_median",
    ]
    merged = tbl_excl[key_cols].merge(
        tbl_incl[key_cols],
        on=["variable", "benchmark_region"],
        suffixes=("_excl", "_incl"),
        how="outer",
    )
    merged["sig_excl"] = merged["p_value_excl"].map(sig_label)
    merged["sig_incl"] = merged["p_value_incl"].map(sig_label)
    merged["dir_changed"] = merged["direction_excl"] != merged["direction_incl"]
    merged["sig_changed"] = merged["sig_excl"] != merged["sig_incl"]

    changed = merged.loc[merged["dir_changed"] | merged["sig_changed"]].copy()
    print()
    print("=== 2022 pairwise: EXCLUDING vs INCLUDING hospitals/universities/political ===")
    print(f"Significant pairs (p<0.05): excluding={int((tbl_excl['p_value'] < 0.05).sum())}, including={int((tbl_incl['p_value'] < 0.05).sum())}")
    print(f"Rows with direction or significance change: {len(changed)} of {len(merged)}")
    for _, row in changed.iterrows():
        print()
        print(f"{row['variable']} vs {row['benchmark_region']}:")
        print(
            f"  EXCL: {row['sig_excl']} p={row['p_value_excl']:.4g} {row['direction_excl']} "
            f"BH={row['black_hills_positive_median_excl']:,.0f} bench={row['benchmark_positive_median_excl']:,.0f}"
        )
        print(
            f"  INCL: {row['sig_incl']} p={row['p_value_incl']:.4g} {row['direction_incl']} "
            f"BH={row['black_hills_positive_median_incl']:,.0f} bench={row['benchmark_positive_median_incl']:,.0f}"
        )

    only_excl = merged.loc[(merged["sig_excl"] == "sig") & (merged["sig_incl"] != "sig")]
    only_incl = merged.loc[(merged["sig_incl"] == "sig") & (merged["sig_excl"] != "sig")]
    print()
    print(f"Only sig when EXCLUDING special orgs ({len(only_excl)}):")
    for _, row in only_excl.iterrows():
        print(f"  {row['variable']} vs {row['benchmark_region']}: p_excl={row['p_value_excl']:.4g} p_incl={row['p_value_incl']:.4g}")
    print(f"Only sig when INCLUDING special orgs ({len(only_incl)}):")
    for _, row in only_incl.iterrows():
        print(f"  {row['variable']} vs {row['benchmark_region']}: p_excl={row['p_value_excl']:.4g} p_incl={row['p_value_incl']:.4g}")

    if changed.empty:
        print()
        print("No pairwise comparison changed significance or direction.")

    merged["p_delta"] = merged["p_value_incl"] - merged["p_value_excl"]
    near = merged.loc[merged["p_delta"].abs().gt(0.02)].sort_values("p_delta", key=abs, ascending=False)
    if not near.empty:
        print()
        print("Largest p-value shifts (|delta| > 0.02, same significance status):")
        for _, row in near.iterrows():
            if row["sig_changed"]:
                continue
            print(
                f"  {row['variable']} vs {row['benchmark_region']}: "
                f"p {row['p_value_excl']:.4g} -> {row['p_value_incl']:.4g}"
            )


def main() -> None:
    gt_raw = ra.load_givingtuesday_analysis(DATA, [2022])
    full = ra.prepare_givingtuesday_analysis(gt_raw, exclude_outliers=False)
    excluded = ra.prepare_givingtuesday_analysis(gt_raw, exclude_outliers=True)

    year_full = full.loc[full["tax_year"].astype(str).eq("2022")].copy()
    year_full["special_org"] = (
        year_full["is_hospital"].fillna(False)
        | year_full["is_university"].fillna(False)
        | year_full["is_political_org"].fillna(False)
    )
    summarize_special_orgs(year_full)
    compare_pairwise(excluded, full)


if __name__ == "__main__":
    main()
