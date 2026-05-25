"""Reconciliation check on the raw GivingTuesday API CSV.

This is a tighter version of `form_990_line1_blanks_audit.py` that ignores all
local staging derivatives and audits the literal CSV emitted by the
GivingTuesday API endpoint
`https://990-infrastructure.gtdata.org/irs-data/990basic120fields`. The
combined CSV preserves every API field name (FEDERACAMPAI, MEMBERDUESUE, ...)
and the verbatim null/empty-vs-numeric pattern that GivingTuesday serves.
"""

from __future__ import annotations

import sys
from pathlib import Path

import numpy as np
import pandas as pd

_THIS_FILE = Path(__file__).resolve()
_REPO_ROOT = _THIS_FILE.parents[2]
_PYTHON_DIR = _REPO_ROOT / "python"
if str(_PYTHON_DIR) not in sys.path:
    sys.path.insert(0, str(_PYTHON_DIR))

from utils.paths import DATA as DEFAULT_DATA_ROOT  # noqa: E402

RAW_CSV_PATH = (
    DEFAULT_DATA_ROOT
    / "raw"
    / "givingtuesday_990"
    / "990_basic120_combined.csv"
)

SUB_COLS = [
    "FEDERACAMPAI",
    "MEMBERDUESUE",
    "FUNDRAEVENTS",
    "RELATEORGANI",
    "GOVERNGRANTS",
    "ALLOOTHECONT",
]
TOTAL_COL = "TOTACASHCONT"


def main() -> None:
    if not RAW_CSV_PATH.exists():
        raise SystemExit(f"Missing raw GT CSV at {RAW_CSV_PATH}")

    df = pd.read_csv(RAW_CSV_PATH, dtype={col: "object" for col in SUB_COLS + [TOTAL_COL]})
    total_rows = len(df)
    print(f"Raw GivingTuesday API CSV rows: {total_rows:,}")
    print(f"Tax years: {sorted(df['TAXYEAR'].dropna().unique().tolist())}")
    print(f"Return types: {sorted(df['RETURNTYPE'].dropna().unique().tolist())}")

    numeric = df[SUB_COLS + [TOTAL_COL]].apply(pd.to_numeric, errors="coerce")
    blank_mask = df[SUB_COLS].isna()
    blank_count = blank_mask.sum(axis=1)
    sub_sum = numeric[SUB_COLS].fillna(0).sum(axis=1)
    line_1h = numeric[TOTAL_COL]

    print("\n--- Blank vs explicit-zero vs positive on raw GT API ---")
    for column in SUB_COLS + [TOTAL_COL]:
        column_numeric = numeric[column]
        blank = df[column].isna().sum()
        explicit_zero = (df[column].notna() & column_numeric.eq(0)).sum()
        positive = column_numeric.gt(0).sum()
        negative = column_numeric.lt(0).sum()
        print(
            f"{column:15s}  blank={blank:5d} ({blank / total_rows:.1%})  "
            f"explicit_zero={explicit_zero:5d} ({explicit_zero / total_rows:.1%})  "
            f"positive={positive:5d} ({positive / total_rows:.1%})  "
            f"negative={negative:5d}"
        )

    print("\n--- Reconciliation of populated 1a-1f to Line 1h ---")
    classes = {
        "All 1a-1f BLANK (treated as zero)": blank_count.eq(len(SUB_COLS)),
        "Some blank, some populated": blank_count.between(1, len(SUB_COLS) - 1),
        "All 1a-1f populated (no blanks)": blank_count.eq(0),
    }
    for label, mask in classes.items():
        n = int(mask.sum())
        sub_subset = sub_sum.loc[mask]
        line_subset = line_1h.loc[mask]
        diff = (line_subset.fillna(0) - sub_subset.fillna(0)).abs()
        within_one_dollar = (diff <= 1.0).mean() if n else float("nan")
        relative = (diff / line_subset.where(line_subset.abs() > 0)).abs()
        within_one_percent = relative.dropna()
        within_one_percent_share = within_one_percent.le(0.01).mean() if len(within_one_percent) else float("nan")
        line_pos_share = line_subset.gt(0).mean() if n else float("nan")
        line_blank_share = line_subset.isna().mean() if n else float("nan")
        gap_dollars = f"${diff.median():,.2f}" if n else "n/a"
        within_dollar_str = f"{within_one_dollar:.1%}" if n else "n/a"
        within_percent_str = f"{within_one_percent_share:.1%}" if len(within_one_percent) else "n/a"
        print(f"{label}")
        print(f"  rows={n}  share_of_csv={n / total_rows:.1%}")
        print(f"  Line 1h > 0: {line_pos_share:.1%}    Line 1h blank: {line_blank_share:.1%}")
        print(f"  median |sum_sub - Line_1h|: {gap_dollars}    within $1: {within_dollar_str}    within 1%: {within_percent_str}")

    print("\n--- Spotlight: every 1a-1f blank yet Line 1h > 0 ---")
    spotlight = blank_count.eq(len(SUB_COLS)) & line_1h.gt(0)
    print(f"Rows: {int(spotlight.sum())} of {total_rows:,}")
    if spotlight.sum():
        print(f"  Median Line 1h on those rows: ${line_1h.loc[spotlight].median():,.0f}")
        print(f"  Max Line 1h on those rows:    ${line_1h.loc[spotlight].max():,.0f}")


if __name__ == "__main__":
    main()
