"""Verify Table 5-13 aggregate totals against client_raw_level_region_summary.csv."""

from __future__ import annotations

import sys
from pathlib import Path

import pandas as pd
from docx import Document

REPO = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO / "scripts"))
from sync_final_report_black_sections import (  # noqa: E402
    OVERVIEW_TABLE_HEADERS,
    RAW_LEVEL_CSV,
    REPORT_DISPLAY_LABELS,
    REPORT_REGION_ORDER,
    REPORT_VARIABLES,
    load_aggregate_rows,
)

DOCX = REPO / "docs/final_report/Philanthropic characteristics of the Black Hills Area and co color-coded.docx"
MD = REPO / "docs/final_report/Philanthropic characteristics of the Black Hills Area and co (1).md"


def main() -> None:
    frame = pd.read_csv(RAW_LEVEL_CSV)
    computed = load_aggregate_rows()
    issues: list[str] = []

    for variable in REPORT_VARIABLES:
        subset = frame.loc[frame["variable"].eq(variable)].set_index("region_label").reindex(
            REPORT_REGION_ORDER
        )
        for region in REPORT_REGION_ORDER:
            row = subset.loc[region]
            exact = float(row["mean"]) * int(row["n"])
            rounded = round(exact)
            shown = next(r for r in computed if r[0] == REPORT_DISPLAY_LABELS[variable])[
                REPORT_REGION_ORDER.index(region) + 1
            ]
            shown_val = int(shown.replace("$", "").replace(",", ""))
            if shown_val != rounded:
                issues.append(
                    f"Rounding mismatch {variable}/{region}: shown={shown_val} computed={rounded}"
                )

    overview_tables = [
        table
        for table in Document(DOCX).tables
        if [cell.text.strip() for cell in table.rows[0].cells] == OVERVIEW_TABLE_HEADERS
    ]
    if len(overview_tables) != 2:
        issues.append(f"Expected 2 overview-format tables in docx, found {len(overview_tables)}")
    else:
        docx_rows = [[cell.text.strip() for cell in row.cells] for row in overview_tables[1].rows]
        expected = [OVERVIEW_TABLE_HEADERS, *computed]
        if docx_rows != expected:
            issues.append("Docx aggregate table does not match load_aggregate_rows()")

    md_text = MD.read_text(encoding="utf-8")
    for row in computed:
        if row[0] not in md_text or row[1] not in md_text:
            issues.append(f"Markdown missing values for row: {row[0]}")

    print("Table 5-13 verification")
    print(f"Source: {RAW_LEVEL_CSV.name}")
    print(f"Formula: round(mean * n) by variable and region")
    print()
    print(f"{'Revenue source':<40} " + " ".join(f"{r:>14}" for r in REPORT_REGION_ORDER))
    for row in computed:
        print(f"{row[0]:<40} " + " ".join(f"{row[i+1]:>14}" for i in range(5)))

    print()
    print("Universe counts (n) by variable — Black Hills:")
    bh = frame.loc[frame["region_label"].eq("Black Hills")].set_index("variable")
    for variable in REPORT_VARIABLES:
        print(f"  {REPORT_DISPLAY_LABELS[variable]:<38} n={int(bh.loc[variable, 'n']):>4}  reporters={int(bh.loc[variable, 'nonzero_count']):>4}")

    print()
    print("Total revenue cross-check (program + contributions + other vs total):")
    for region in REPORT_REGION_ORDER:
        sub = frame.loc[frame["region_label"].eq(region)].set_index("variable")

        def total(var: str) -> float:
            row = sub.loc[var]
            return float(row["mean"]) * int(row["n"])

        tr = total("total_revenue")
        parts = total("program_service_revenue") + total("total_contributions") + total("residual_other_revenue")
        print(f"  {region:<12} total={tr:>15,.0f}  parts={parts:>15,.0f}  ratio={parts/tr:.4f}")

    print()
    if issues:
        print(f"ISSUES: {len(issues)}")
        for item in issues:
            print(f"  - {item}")
    else:
        print("RESULT: Table 5-13 matches client_raw_level_region_summary.csv and the synced docx/markdown.")


if __name__ == "__main__":
    main()
