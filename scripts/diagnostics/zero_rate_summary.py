"""Summarize zero rates for revenue-source variables."""

from __future__ import annotations

from pathlib import Path

import pandas as pd

REPO = Path(__file__).resolve().parents[2]
PARQUET = (
    REPO
    / "data"
    / "321_Black_Hills_Area_Community_Foundation_2025_08"
    / "01_data"
    / "analysis"
    / "revenue_sources_black_hills"
    / "cleaned_revenue_sources_analysis.parquet"
)

AMOUNT_VARS = [
    "total_revenue",
    "program_service_revenue",
    "total_contributions",
    "government_grants_received",
    "federated_campaigns",
    "related_org_contributions",
    "membership_dues",
    "fundraising_events_contributions",
    "mixed_unclassified_contributions",
    "residual_other_revenue",
]

LINE1_990 = [
    "government_grants_received",
    "federated_campaigns",
    "related_org_contributions",
    "membership_dues",
    "fundraising_events_contributions",
]


def zero_rate(series: pd.Series) -> tuple[float, int, int]:
    values = series.dropna()
    n = len(values)
    nonzero = int(values.gt(0).sum())
    zero = n - nonzero
    return (zero / n if n else float("nan"), nonzero, n)


def main() -> None:
    df = pd.read_parquet(PARQUET)
    y2022 = df.loc[df["tax_year"].astype(str).eq("2022")].copy()
    f990 = y2022.loc[y2022["form_type"].eq("990")]

    print("2022 cleaned frame (all forms, n=%d org-years)" % len(y2022))
    print("-" * 72)
    rows = []
    for var in AMOUNT_VARS:
        if var not in y2022.columns:
            continue
        zr, nz, n = zero_rate(y2022[var])
        rows.append((var, zr, nz, n, "all_forms"))
    rows.sort(key=lambda r: -r[1])
    for var, zr, nz, n, _ in rows:
        label = ""
        if zr >= 0.9:
            label = " [>=90% zeros]"
        elif zr >= 0.8:
            label = " [>=80% zeros]"
        elif zr >= 0.5:
            label = " [>=50% zeros]"
        print(f"{var:42s} {zr:6.1%} zero  ({nz:4d}/{n:4d} nonzero){label}")

    print()
    print("2022 Form 990 only (n=%d) — Line 1 subcomponents" % len(f990))
    print("-" * 72)
    for var in LINE1_990:
        zr, nz, n = zero_rate(f990[var])
        label = ""
        if zr >= 0.9:
            label = " [>=90% zeros]"
        elif zr >= 0.8:
            label = " [>=80% zeros]"
        print(f"{var:42s} {zr:6.1%} zero  ({nz:4d}/{n:4d} nonzero){label}")


if __name__ == "__main__":
    main()
