"""Quick diagnostic: net margin ratio and months of reserves on the GT all-forms file."""

from __future__ import annotations

from pathlib import Path

import numpy as np
import pandas as pd


PARQUET_PATH = Path(
    "data/321_Black_Hills_Area_Community_Foundation_2025_08/01_data/staging/filing/"
    "givingtuesday_990_basic_allforms_analysis_variables.parquet"
)


def main() -> None:
    pd.set_option("display.width", 240)
    pd.set_option("display.max_columns", 30)

    df = pd.read_parquet(PARQUET_PATH)

    # Flagstaff focus
    fl = df.loc[df["region"].eq("Flagstaff")].copy()
    print("Flagstaff rows:", len(fl))
    print(fl["form_type"].value_counts())
    print()

    cols_show = [
        "ein",
        "tax_year",
        "form_type",
        "analysis_total_revenue_amount",
        "analysis_total_expense_amount",
        "analysis_calculated_surplus_amount",
        "analysis_calculated_net_asset_amount",
        "analysis_calculated_months_of_reserves",
        "analysis_calculated_net_margin_ratio",
    ]

    print("=== Flagstaff: 5 lowest net_margin rows ===")
    print(fl.nsmallest(5, "analysis_calculated_net_margin_ratio")[cols_show].to_string(index=False))
    print()
    print("=== Flagstaff: 5 highest net_margin rows ===")
    print(fl.nlargest(5, "analysis_calculated_net_margin_ratio")[cols_show].to_string(index=False))
    print()
    print("=== Flagstaff: 5 lowest months_of_reserves rows ===")
    print(fl.nsmallest(5, "analysis_calculated_months_of_reserves")[cols_show].to_string(index=False))
    print()
    print("=== Flagstaff: 5 highest months_of_reserves rows ===")
    print(fl.nlargest(5, "analysis_calculated_months_of_reserves")[cols_show].to_string(index=False))
    print()

    print("=== Net margin: low-denominator (|total revenue|) by region ===")
    for reg, g in df.groupby("region"):
        valid = g["analysis_calculated_net_margin_ratio"].notna()
        rev_abs = g.loc[valid, "analysis_total_revenue_amount"].abs()
        rev = g.loc[valid, "analysis_total_revenue_amount"]
        n_low_1k = int((rev_abs < 1_000).sum())
        n_low_10k = int((rev_abs < 10_000).sum())
        n_neg_rev = int((rev < 0).sum())
        print(
            f"  {reg:11s} n_valid={int(valid.sum()):4d}  "
            f"|rev|<1k: {n_low_1k:3d}   |rev|<10k: {n_low_10k:3d}   negative rev: {n_neg_rev:3d}"
        )
    print()

    print("=== Months of reserves: low-denominator (expense) by region ===")
    for reg, g in df.groupby("region"):
        valid = g["analysis_calculated_months_of_reserves"].notna()
        exp = g.loc[valid, "analysis_total_expense_amount"]
        n_low_1k = int((exp.abs() < 1_000).sum())
        n_low_10k = int((exp.abs() < 10_000).sum())
        n_neg_assets = int(
            (g.loc[valid, "analysis_calculated_net_asset_amount"] < 0).sum()
        )
        print(
            f"  {reg:11s} n_valid={int(valid.sum()):4d}  "
            f"expense<1k: {n_low_1k:3d}   expense<10k: {n_low_10k:3d}   "
            f"negative net assets: {n_neg_assets:3d}"
        )
    print()

    print("=== Winsorized 1%/99% summary by region ===")
    for col in [
        "analysis_calculated_net_margin_ratio",
        "analysis_calculated_months_of_reserves",
    ]:
        print(f"-- {col} --")
        rows = []
        for reg, g in df.groupby("region"):
            s = g[col].dropna()
            if len(s) == 0:
                continue
            lo, hi = s.quantile(0.01), s.quantile(0.99)
            w = s.clip(lo, hi)
            rows.append(
                (
                    reg,
                    int(len(s)),
                    float(w.mean()),
                    float(w.median()),
                    float(w.std()),
                    float(w.quantile(0.25)),
                    float(w.quantile(0.75)),
                )
            )
        out = pd.DataFrame(
            rows,
            columns=["region", "n", "mean_w", "median_w", "std_w", "p25_w", "p75_w"],
        ).set_index("region")
        print(out.round(3))
        print()

    # Leave-one-out: how do Flagstaff summary stats change when each of the top-5
    # extreme rows is removed?
    print("=== Flagstaff leave-one-out impact (net_margin) ===")
    fl_nm = fl.dropna(subset=["analysis_calculated_net_margin_ratio"]).copy()
    base = fl_nm["analysis_calculated_net_margin_ratio"]
    print(
        f"  baseline  n={len(base):4d}  mean={base.mean():.3f}  median={base.median():.3f}  "
        f"std={base.std():.3f}  min={base.min():.3f}  max={base.max():.3f}"
    )
    top_idx = base.abs().nlargest(5).index
    for idx in top_idx:
        without = fl_nm.drop(index=idx)["analysis_calculated_net_margin_ratio"]
        ein = fl_nm.loc[idx, "ein"]
        yr = fl_nm.loc[idx, "tax_year"]
        v = base.loc[idx]
        print(
            f"  drop ein={ein} ({yr}) value={v:.3f}   "
            f"-> n={len(without):4d}  mean={without.mean():.3f}  median={without.median():.3f}  "
            f"std={without.std():.3f}  min={without.min():.3f}  max={without.max():.3f}"
        )
    print()

    print("=== Flagstaff leave-one-out impact (months_of_reserves) ===")
    fl_mr = fl.dropna(subset=["analysis_calculated_months_of_reserves"]).copy()
    base = fl_mr["analysis_calculated_months_of_reserves"]
    print(
        f"  baseline  n={len(base):4d}  mean={base.mean():.2f}  median={base.median():.2f}  "
        f"std={base.std():.2f}  min={base.min():.2f}  max={base.max():.2f}"
    )
    top_idx = base.abs().nlargest(5).index
    for idx in top_idx:
        without = fl_mr.drop(index=idx)["analysis_calculated_months_of_reserves"]
        ein = fl_mr.loc[idx, "ein"]
        yr = fl_mr.loc[idx, "tax_year"]
        v = base.loc[idx]
        print(
            f"  drop ein={ein} ({yr}) value={v:.2f}   "
            f"-> n={len(without):4d}  mean={without.mean():.2f}  median={without.median():.2f}  "
            f"std={without.std():.2f}  min={without.min():.2f}  max={without.max():.2f}"
        )


if __name__ == "__main__":
    main()
