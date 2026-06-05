"""
Audit whether Form 990 Part VIII Line 1 sub-component blanks behave like
reported zeros or like missing data.

We answer this empirically by looking at the raw GivingTuesday all-forms
benchmark parquet (which carries the raw IRS source columns) joined with the
harmonized analysis-variable parquet. For every Form 990 row we check, for
each Line 1 sub-component (1a-1f and 1e separately), whether the upstream
field is NULL or a numeric value, and we reconcile the sub-component sum to
Line 1h (reported total contributions). The reconciliation check is the key
signal: if blanks really mean zero, then the populated 1a-1f values must add
up to Line 1h on most rows. If blanks really mean missing, then rows with at
least one blank sub-line should systematically under-add to Line 1h.

The script writes a small Markdown report to stdout and also drops a CSV
breakdown per sub-component into the same folder as this file so it can be
attached to the analysis audit trail.
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

STAGING_DIR = DEFAULT_DATA_ROOT / "staging" / "filing"
HARMONIZED_PATH = STAGING_DIR / "givingtuesday_990_basic_allforms_analysis_variables.parquet"
RAW_BENCHMARK_PATH = STAGING_DIR / "givingtuesday_990_basic_allforms_benchmark.parquet"

LINE1_RAW_TO_LABEL = {
    "FEDERACAMPAI": "1a federated_campaigns",
    "MEMBERDUESUE": "1b membership_dues",
    "FUNDRAEVENTS": "1c fundraising_events_contributions",
    "RELATEORGANI": "1d related_org_contributions",
    "GOVERNGRANTS": "1e government_grants_received",
    "ALLOOTHECONT": "1f all_other_contributions",
}
LINE1_RAW_COLUMNS = list(LINE1_RAW_TO_LABEL.keys())
TOTAL_CASH_CONT_COLUMN = "TOTACASHCONT"


def coerce_numeric(series: pd.Series) -> pd.Series:
    """Coerce a possibly-string Series to numeric, preserving NaN for blanks."""
    return pd.to_numeric(series, errors="coerce")


def column_blank_breakdown(form_990: pd.DataFrame, raw_column: str) -> dict[str, object]:
    """Compute blank vs zero vs positive vs negative counts for one raw column."""
    values = form_990[raw_column]
    numeric = coerce_numeric(values)
    is_blank = values.isna()
    is_explicit_zero = (~is_blank) & numeric.eq(0)
    is_positive = numeric.gt(0)
    is_negative = numeric.lt(0)
    total = len(form_990)
    return {
        "raw_column": raw_column,
        "sub_line_label": LINE1_RAW_TO_LABEL[raw_column],
        "form_990_rows": int(total),
        "blank_rows": int(is_blank.sum()),
        "blank_share": float(is_blank.mean()) if total else float("nan"),
        "explicit_zero_rows": int(is_explicit_zero.sum()),
        "explicit_zero_share": float(is_explicit_zero.mean()) if total else float("nan"),
        "positive_rows": int(is_positive.sum()),
        "positive_share": float(is_positive.mean()) if total else float("nan"),
        "negative_rows": int(is_negative.sum()),
    }


def reconciliation_breakdown(form_990: pd.DataFrame) -> pd.DataFrame:
    """Reconcile populated 1a-1f sum against Line 1h on each Form 990 row.

    Three classes of rows are reported:

    - ``all_subs_blank``: every 1a-1f raw value is NULL. Treating these blanks
      as zero forces the sub-component sum to zero. The Line 1h reported total
      tells us whether that is plausible (Line 1h ~ 0 = consistent with real
      zeros; Line 1h > 0 = inconsistent, suggesting missing rather than zero).
    - ``some_subs_blank``: at least one but not all 1a-1f values are blank.
    - ``all_subs_populated``: every 1a-1f value is non-blank (explicit zero or
      positive). This is the clean baseline.

    Within each class, we report how often the populated 1a-1f sum reconciles
    to Line 1h within a $1 tolerance and within a 1% tolerance.
    """
    numeric_subs = form_990[LINE1_RAW_COLUMNS].apply(coerce_numeric)
    blank_mask = form_990[LINE1_RAW_COLUMNS].isna()
    blank_count_per_row = blank_mask.sum(axis=1)
    populated_sum_fillna0 = numeric_subs.fillna(0).sum(axis=1)
    total_line_1h = coerce_numeric(form_990[TOTAL_CASH_CONT_COLUMN])

    rows: list[dict[str, object]] = []
    classes = {
        "all_subs_blank (every 1a-1f is NULL)": blank_count_per_row.eq(len(LINE1_RAW_COLUMNS)),
        "some_subs_blank (1-5 blanks among 1a-1f)": blank_count_per_row.between(1, len(LINE1_RAW_COLUMNS) - 1),
        "all_subs_populated (every 1a-1f is non-blank)": blank_count_per_row.eq(0),
    }
    for label, mask in classes.items():
        subset = mask
        n = int(subset.sum())
        line_1h = total_line_1h.loc[subset]
        sub_sum = populated_sum_fillna0.loc[subset]
        diff = line_1h.fillna(0) - sub_sum.fillna(0)
        abs_diff = diff.abs()
        line_1h_nonzero = line_1h.gt(0)
        line_1h_zero_or_null = ~line_1h_nonzero
        within_1_dollar = abs_diff.le(1.0)
        denom = line_1h.where(line_1h.abs().gt(0), other=np.nan)
        relative_diff = (diff.abs() / denom).where(denom.notna())
        within_one_percent = relative_diff.le(0.01)
        rows.append(
            {
                "class": label,
                "rows": n,
                "share_of_form_990": float(n / len(form_990)) if len(form_990) else float("nan"),
                "share_with_line_1h_gt_0": float(line_1h_nonzero.mean()) if n else float("nan"),
                "share_with_line_1h_zero_or_null": float(line_1h_zero_or_null.mean()) if n else float("nan"),
                "median_line_1h_dollars": float(line_1h.median()) if n else float("nan"),
                "median_abs_reconciliation_gap": float(abs_diff.median()) if n else float("nan"),
                "share_reconciled_within_$1": float(within_1_dollar.mean()) if n else float("nan"),
                "share_reconciled_within_1_percent": float(within_one_percent.dropna().mean()) if within_one_percent.notna().any() else float("nan"),
            }
        )
    return pd.DataFrame(rows)


def all_blanks_with_positive_total(form_990: pd.DataFrame) -> dict[str, object]:
    """Spotlight the most worrying class: all sub-lines blank yet Line 1h > 0.

    If "blank means zero" is correct, this class should be very rare (since a
    nonzero Line 1h has to come from somewhere on 1a-1f). A meaningful share of
    rows here is evidence that some filers report only Line 1h and leave the
    sub-components blank, in which case the fillna(0) interpretation would
    pull every sub-component median artificially toward zero.
    """
    numeric_subs = form_990[LINE1_RAW_COLUMNS].apply(coerce_numeric)
    blank_mask = form_990[LINE1_RAW_COLUMNS].isna()
    all_blank = blank_mask.all(axis=1)
    total_line_1h = coerce_numeric(form_990[TOTAL_CASH_CONT_COLUMN])
    bad = all_blank & total_line_1h.gt(0)
    return {
        "all_subs_blank_rows": int(all_blank.sum()),
        "all_subs_blank_with_line_1h_gt_0": int(bad.sum()),
        "all_subs_blank_with_line_1h_gt_0_share_of_form_990": (
            float(bad.sum() / len(form_990)) if len(form_990) else float("nan")
        ),
        "median_line_1h_when_all_blank_and_positive_total": (
            float(total_line_1h.loc[bad].median()) if bad.any() else float("nan")
        ),
        "p75_line_1h_when_all_blank_and_positive_total": (
            float(total_line_1h.loc[bad].quantile(0.75)) if bad.any() else float("nan")
        ),
        "p95_line_1h_when_all_blank_and_positive_total": (
            float(total_line_1h.loc[bad].quantile(0.95)) if bad.any() else float("nan")
        ),
    }


def render_markdown(
    breakdown: pd.DataFrame,
    reconciliation: pd.DataFrame,
    spotlight: dict[str, object],
    form_990_total_rows: int,
    tax_years: list[str],
    regions: list[str],
) -> str:
    """Render the audit as a human-readable Markdown report."""
    lines: list[str] = []
    lines.append("# Form 990 Part VIII Line 1 blanks audit")
    lines.append("")
    lines.append(
        "This audit empirically checks whether the analysis's `fillna(0.0)` for "
        "Form 990 Part VIII Line 1 sub-components on Form 990 rows is consistent "
        "with how filers actually use the form. We compare populated 1a-1f values "
        "against the reported Line 1h total contributions and look for rows where "
        "Line 1h is positive but every sub-line is blank."
    )
    lines.append("")
    lines.append(f"- Form 990 rows audited: {form_990_total_rows:,}")
    lines.append(f"- Tax years: {sorted(set(tax_years))}")
    lines.append(f"- Regions: {sorted(set(regions))}")
    lines.append("")

    lines.append("## Blank, explicit-zero, and positive rates by sub-line")
    lines.append("")
    lines.append("| Sub-line | Raw column | Blank | Explicit zero | Positive | Negative |")
    lines.append("| --- | --- | --- | --- | --- | --- |")
    for _, row in breakdown.iterrows():
        lines.append(
            "| "
            f"{row['sub_line_label']} | {row['raw_column']} | "
            f"{row['blank_share']:.1%} ({row['blank_rows']:,}) | "
            f"{row['explicit_zero_share']:.1%} ({row['explicit_zero_rows']:,}) | "
            f"{row['positive_share']:.1%} ({row['positive_rows']:,}) | "
            f"{row['negative_rows']:,} |"
        )
    lines.append("")

    lines.append("## Reconciliation of populated 1a-1f against Line 1h")
    lines.append("")
    lines.append("Each row class below is computed on the same Form 990 universe. The reconciliation columns treat blanks as zero (matching the current analysis) and compare the sum of 1a through 1f to the reported Line 1h total contributions on the same return.")
    lines.append("")
    lines.append("| Row class | Rows | Share of Form 990 | Line 1h > 0 | Line 1h = 0 or blank | Median Line 1h | Median abs gap (sum 1a-1f vs Line 1h) | Reconciled within $1 | Reconciled within 1% |")
    lines.append("| --- | --- | --- | --- | --- | --- | --- | --- | --- |")
    for _, row in reconciliation.iterrows():
        lines.append(
            "| "
            f"{row['class']} | {row['rows']:,} | {row['share_of_form_990']:.1%} | "
            f"{row['share_with_line_1h_gt_0']:.1%} | {row['share_with_line_1h_zero_or_null']:.1%} | "
            f"${row['median_line_1h_dollars']:,.0f} | "
            f"${row['median_abs_reconciliation_gap']:,.0f} | "
            f"{row['share_reconciled_within_$1']:.1%} | "
            f"{row['share_reconciled_within_1_percent']:.1%} |"
        )
    lines.append("")

    lines.append("## Spotlight: every sub-line blank but Line 1h is positive")
    lines.append("")
    lines.append(
        "If blanks really meant zero, then a return with every sub-line blank "
        "should also report a $0 Line 1h. Any rows here are evidence that the "
        "`fillna(0.0)` choice converts real missing data into spurious zeros "
        "for the affected sub-components."
    )
    lines.append("")
    lines.append(
        "- Rows with every 1a-1f blank: "
        f"{spotlight['all_subs_blank_rows']:,}"
    )
    lines.append(
        "- Of those, rows where Line 1h > 0: "
        f"{spotlight['all_subs_blank_with_line_1h_gt_0']:,} "
        f"({spotlight['all_subs_blank_with_line_1h_gt_0_share_of_form_990']:.2%} of Form 990 rows)"
    )
    if spotlight["all_subs_blank_with_line_1h_gt_0"]:
        lines.append(
            "- Median Line 1h on those rows: "
            f"${spotlight['median_line_1h_when_all_blank_and_positive_total']:,.0f}"
        )
        lines.append(
            "- 75th-percentile Line 1h: "
            f"${spotlight['p75_line_1h_when_all_blank_and_positive_total']:,.0f}"
        )
        lines.append(
            "- 95th-percentile Line 1h: "
            f"${spotlight['p95_line_1h_when_all_blank_and_positive_total']:,.0f}"
        )
    lines.append("")
    return "\n".join(lines)


def main() -> None:
    if not HARMONIZED_PATH.exists():
        raise SystemExit(f"Missing harmonized parquet: {HARMONIZED_PATH}")
    if not RAW_BENCHMARK_PATH.exists():
        raise SystemExit(f"Missing raw benchmark parquet: {RAW_BENCHMARK_PATH}")

    harmonized = pd.read_parquet(HARMONIZED_PATH)
    raw = pd.read_parquet(RAW_BENCHMARK_PATH)
    if len(harmonized) != len(raw):
        raise SystemExit(
            f"Harmonized ({len(harmonized):,}) and raw benchmark ({len(raw):,}) "
            "row counts differ; expected same order."
        )

    form_type = harmonized["form_type"].astype(str).str.upper().str.strip()
    form_990_mask = form_type.eq("990")
    raw_990 = raw.loc[form_990_mask.values].reset_index(drop=True)
    harmonized_990 = harmonized.loc[form_990_mask.values].reset_index(drop=True)

    missing_raw_cols = [
        column for column in LINE1_RAW_COLUMNS + [TOTAL_CASH_CONT_COLUMN]
        if column not in raw_990.columns
    ]
    if missing_raw_cols:
        raise SystemExit(
            f"Raw benchmark parquet is missing expected source columns: {missing_raw_cols}"
        )

    breakdown = pd.DataFrame([column_blank_breakdown(raw_990, column) for column in LINE1_RAW_COLUMNS])
    reconciliation = reconciliation_breakdown(raw_990)
    spotlight = all_blanks_with_positive_total(raw_990)

    out_dir = _THIS_FILE.parent
    out_dir.mkdir(parents=True, exist_ok=True)
    breakdown.to_csv(out_dir / "form_990_line1_blanks_breakdown.csv", index=False)
    reconciliation.to_csv(out_dir / "form_990_line1_reconciliation.csv", index=False)

    tax_years = harmonized_990["tax_year"].astype(str).unique().tolist() if "tax_year" in harmonized_990.columns else []
    regions = harmonized_990["region"].astype(str).unique().tolist() if "region" in harmonized_990.columns else []
    report = render_markdown(
        breakdown=breakdown,
        reconciliation=reconciliation,
        spotlight=spotlight,
        form_990_total_rows=len(raw_990),
        tax_years=tax_years,
        regions=regions,
    )
    report_path = out_dir / "form_990_line1_blanks_audit.md"
    report_path.write_text(report, encoding="utf-8")
    print(report)
    print(f"\nReport written to: {report_path}")


if __name__ == "__main__":
    main()
