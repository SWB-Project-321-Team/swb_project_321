"""Missingness breakdown for net margin and months of reserves by form type."""

from __future__ import annotations

from pathlib import Path

import pandas as pd


PARQUET_PATH = Path(
    "data/321_Black_Hills_Area_Community_Foundation_2025_08/01_data/staging/filing/"
    "givingtuesday_990_basic_allforms_analysis_variables.parquet"
)


def main() -> None:
    pd.set_option("display.width", 220)
    pd.set_option("display.max_columns", 30)

    df = pd.read_parquet(PARQUET_PATH)
    print(f"Total rows: {len(df):,}")
    print()

    cols = {
        "analysis_total_revenue_amount": "total revenue",
        "analysis_total_expense_amount": "total expense",
        "analysis_calculated_net_asset_amount": "calculated net assets",
        "analysis_calculated_net_margin_ratio": "net margin ratio",
        "analysis_calculated_months_of_reserves": "months of reserves",
    }

    rows = []
    for col, label in cols.items():
        for form, sub in df.groupby("form_type"):
            n_total = len(sub)
            n_present = int(sub[col].notna().sum())
            n_missing = n_total - n_present
            pct = (n_missing / n_total) * 100 if n_total else 0.0
            rows.append(
                {
                    "metric": label,
                    "form_type": form,
                    "n_total": n_total,
                    "n_missing": n_missing,
                    "pct_missing": round(pct, 1),
                }
            )

    out = pd.DataFrame(rows)
    print("Missingness by form type:")
    pivot = out.pivot(index="metric", columns="form_type", values=["n_missing", "pct_missing"])
    print(pivot)
    print()

    # Decompose months-of-reserves missingness: where exactly does it drop?
    print("Why is months_of_reserves missing? (mutually exclusive reasons)")
    mr_missing = df["analysis_calculated_months_of_reserves"].isna()
    na_missing = df["analysis_calculated_net_asset_amount"].isna()
    exp_missing = df["analysis_total_expense_amount"].isna()
    exp_zero = df["analysis_total_expense_amount"].eq(0)

    for form, sub in df.groupby("form_type"):
        m_sub = mr_missing & (df["form_type"] == form)
        only_na = (na_missing & ~exp_missing & ~exp_zero) & (df["form_type"] == form)
        only_exp = (~na_missing & (exp_missing | exp_zero)) & (df["form_type"] == form)
        both = (na_missing & (exp_missing | exp_zero)) & (df["form_type"] == form)
        print(
            f"  {form}: missing={int(m_sub.sum()):4d}  "
            f"due_to_missing_net_assets_only={int(only_na.sum()):4d}  "
            f"due_to_missing/zero_expense_only={int(only_exp.sum()):4d}  "
            f"both={int(both.sum()):4d}"
        )

    print()
    print("Net assets missingness drilldown:")
    for form, sub in df.groupby("form_type"):
        n = len(sub)
        miss = int(sub["analysis_calculated_net_asset_amount"].isna().sum())
        print(f"  {form}: net_assets missing = {miss:4d} / {n:4d} ({miss/n*100:.1f}%)")


if __name__ == "__main__":
    main()
