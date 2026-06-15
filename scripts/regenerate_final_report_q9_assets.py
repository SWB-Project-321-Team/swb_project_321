"""Regenerate the eleven added Q9 report charts from the presentation summary."""

from __future__ import annotations

import shutil
import textwrap
from pathlib import Path

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd


REPO_ROOT = Path(__file__).resolve().parents[1]
SUMMARY_CSV = (
    REPO_ROOT
    / "python"
    / "analysis"
    / "revenue_sources_black_hills"
    / "results"
    / "tables"
    / "client_2022_pairwise_presentation_summary.csv"
)
CANONICAL_DIR = REPO_ROOT / "docs" / "assets" / "section3_q9_2022"
REPORT_ASSET_DIR = REPO_ROOT / "docs" / "final_report" / "updated_report_assets"

REGION_ORDER = ["Black Hills", "Billings", "Flagstaff", "Missoula", "Sioux Falls"]
REGION_COLORS = {
    "Black Hills": "#66C2A5",
    "Billings": "#FC8D62",
    "Flagstaff": "#8DA0CB",
    "Missoula": "#E78AC3",
    "Sioux Falls": "#A6D854",
}
VARIABLES = [
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
DISPLAY_LABELS = {
    "total_revenue": "Total revenue",
    "program_service_revenue": "Program service revenue",
    "total_contributions": "Total contributions",
    "government_grants_received": "Government grants received",
    "federated_campaigns": "Federated campaign contributions",
    "related_org_contributions": "Related organization contributions",
    "membership_dues": "Membership dues",
    "fundraising_events_contributions": "Fundraising event contributions",
    "mixed_unclassified_contributions": "Mixed or unclassified contributions",
    "residual_other_revenue": "Other revenue",
}
REPORT_IMAGE_NAMES = {
    "overview": "updated-report-image-21.png",
    **{
        variable: f"updated-report-image-{index:02d}.png"
        for index, variable in enumerate(VARIABLES, start=22)
    },
}
CHART_STYLE = {
    "font.family": "Arial",
    "font.size": 8,
    "axes.titlesize": 8,
    "axes.labelsize": 8,
    "xtick.labelsize": 8,
    "ytick.labelsize": 8,
    "legend.fontsize": 8,
}


def compact_dollars(value: float) -> str:
    absolute = abs(float(value))
    if absolute >= 1_000_000:
        return f"${value / 1_000_000:.1f}M" if absolute < 10_000_000 else f"${value / 1_000_000:.0f}M"
    if absolute >= 1_000:
        return f"${value / 1_000:.0f}K"
    return f"${value:.0f}"


def load_values() -> tuple[pd.DataFrame, pd.DataFrame]:
    summary = pd.read_csv(SUMMARY_CSV)
    expected_pairs = {(variable, region) for variable in VARIABLES for region in REGION_ORDER[1:]}
    actual_pairs = set(zip(summary["variable"], summary["benchmark_region"]))
    missing = expected_pairs - actual_pairs
    if missing:
        raise RuntimeError(f"Presentation summary is missing rows: {sorted(missing)}")

    rows: list[dict[str, object]] = []
    for variable in VARIABLES:
        variable_rows = summary.loc[summary["variable"].eq(variable)]
        first = variable_rows.iloc[0]
        rows.append(
            {
                "variable": variable,
                "region": "Black Hills",
                "median": float(first["black_hills_positive_median"]),
                "positive_n": int(first["black_hills_positive_n"]),
            }
        )
        for region in REGION_ORDER[1:]:
            row = variable_rows.loc[variable_rows["benchmark_region"].eq(region)].iloc[0]
            rows.append(
                {
                    "variable": variable,
                    "region": region,
                    "median": float(row["benchmark_positive_median"]),
                    "positive_n": int(row["benchmark_positive_n"]),
                }
            )
    values = pd.DataFrame(rows)
    return summary, values


def save_individual_charts(values: pd.DataFrame) -> dict[str, Path]:
    outputs: dict[str, Path] = {}
    with plt.rc_context(CHART_STYLE):
        for variable in VARIABLES:
            subset = values.loc[values["variable"].eq(variable)].set_index("region").reindex(REGION_ORDER)
            medians = subset["median"].to_numpy(dtype=float)
            upper = max(1.0, float(np.nanmax(medians)) * 1.12)
            fig, ax = plt.subplots(figsize=(7.2, 4.0))
            x = np.arange(len(REGION_ORDER))
            ax.bar(
                x,
                medians,
                color=[REGION_COLORS[region] for region in REGION_ORDER],
                edgecolor="none",
                linewidth=0,
                width=0.66,
            )
            ax.set_xticks(x, REGION_ORDER)
            ax.set_xlabel("")
            ax.set_ylabel("Median dollars")
            ax.set_ylim(0, upper)
            ax.yaxis.set_major_formatter(lambda value, _: compact_dollars(value))
            ax.grid(axis="y", alpha=0.18, linewidth=0.6)
            ax.set_axisbelow(True)
            ax.spines["top"].set_visible(False)
            ax.spines["right"].set_visible(False)
            fig.tight_layout()
            path = CANONICAL_DIR / f"client_2022_pairwise_positive_median_{variable}.png"
            fig.savefig(path, dpi=300, bbox_inches="tight")
            plt.close(fig)
            outputs[variable] = path
    return outputs


def save_overview(values: pd.DataFrame) -> Path:
    tick_labels = {
        "Black Hills": "Black\nHills",
        "Billings": "Billings",
        "Flagstaff": "Flagstaff",
        "Missoula": "Missoula",
        "Sioux Falls": "Sioux\nFalls",
    }
    with plt.rc_context(CHART_STYLE):
        fig, axes = plt.subplots(5, 2, figsize=(8.0, 10.0), sharey=False)
        axes = np.asarray(axes).ravel()
        x = np.arange(len(REGION_ORDER))
        for ax, variable in zip(axes, VARIABLES):
            subset = values.loc[values["variable"].eq(variable)].set_index("region").reindex(REGION_ORDER)
            medians = subset["median"].to_numpy(dtype=float)
            upper = max(1.0, float(np.nanmax(medians)) * 1.12)
            ax.bar(
                x,
                medians,
                color=[REGION_COLORS[region] for region in REGION_ORDER],
                edgecolor="none",
                linewidth=0,
                width=0.68,
            )
            ax.set_title(textwrap.fill(DISPLAY_LABELS[variable], width=30), weight="bold", pad=4)
            ax.set_xticks(x, [tick_labels[region] for region in REGION_ORDER])
            ax.set_xlabel("")
            ax.set_ylabel("Median dollars")
            ax.set_ylim(0, upper)
            ax.yaxis.set_major_formatter(lambda value, _: compact_dollars(value))
            ax.grid(axis="y", alpha=0.18, linewidth=0.6)
            ax.set_axisbelow(True)
            ax.spines["top"].set_visible(False)
            ax.spines["right"].set_visible(False)
        fig.tight_layout(h_pad=1.25, w_pad=1.0)
        path = CANONICAL_DIR / "client_2022_positive_median_overview_by_region.png"
        fig.savefig(path, dpi=300, bbox_inches="tight")
        plt.close(fig)
    return path


def main() -> None:
    CANONICAL_DIR.mkdir(parents=True, exist_ok=True)
    REPORT_ASSET_DIR.mkdir(parents=True, exist_ok=True)
    _, values = load_values()
    overview = save_overview(values)
    individual = save_individual_charts(values)

    shutil.copy2(overview, REPORT_ASSET_DIR / REPORT_IMAGE_NAMES["overview"])
    for variable, source_path in individual.items():
        shutil.copy2(source_path, REPORT_ASSET_DIR / REPORT_IMAGE_NAMES[variable])

    print(f"Regenerated overview and {len(individual)} source charts.")
    for region in REGION_ORDER:
        print(f"{region}: {REGION_COLORS[region]}")


if __name__ == "__main__":
    main()
