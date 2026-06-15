"""Rebuild the black Q9 results and appendix Markdown from the presentation CSV."""

from __future__ import annotations

from pathlib import Path

import pandas as pd


REPO_ROOT = Path(__file__).resolve().parents[1]
MD_PATH = REPO_ROOT / "docs" / "final_report" / "Philanthropic characteristics of the Black Hills Area and co (1).md"
SUMMARY_CSV = (
    REPO_ROOT
    / "python"
    / "analysis"
    / "revenue_sources_black_hills"
    / "results"
    / "tables"
    / "client_2022_pairwise_presentation_summary.csv"
)
RAW_LEVEL_CSV = (
    REPO_ROOT
    / "python"
    / "analysis"
    / "revenue_sources_black_hills"
    / "results"
    / "tables"
    / "client_raw_level_region_summary.csv"
)
AGGREGATE_TABLE_NOTE = (
    "Table 5-13 sums reported dollars for each revenue source and region over that source's eligible "
    "reporting universe in the 2022 analysis file. Totals for total revenue, total contributions, mixed or "
    "unclassified contributions, and other revenue include all organizations in the file. Government grants, "
    "federated campaigns, related-organization contributions, membership dues, and fundraising event "
    "contributions include Form 990 filers only; 990-EZ and 990-PF filers do not report those Part VIII "
    "sub-lines and are excluded from those rows, not counted as zero. Within Form 990, a blank sub-line is "
    "treated as zero. Program service revenue includes organizations for which that line is reported on the "
    "filed form. Regional totals reflect both universe size and report size; a few very large filers can "
    "dominate the sum. Table 5-1 reports medians among positive reporters only and is the basis for the "
    "pairwise comparisons in Tables 5-2 through 5-11."
)
MEDIAN_JUSTIFICATION = (
    "This report presents two complementary views of the same filing data. Regional totals (Appendix Table 5-13) sum "
    "reported dollars across all organizations in the analysis file for each source and region. That is the direct "
    "read on how much money flowed through each channel in the overall landscape, but a few very large filers can "
    "dominate those sums, so totals can differ sharply from what most organizations experience.\n\n"
    "The headline comparisons in this section use the median reported dollar amount among organizations that reported "
    "a positive amount for that source. That answers a different question: when a local organization does use the "
    "channel, how large is the amount it reports? The median is less pulled up or down by a handful of extreme "
    "filers than a regional total or average would be, and it compares only organizations that actually reported the "
    "source rather than treating every non-reporter as zero. That separation matters because reporting rates differ "
    "by source and region, and on IRS forms a missing line does not always mean the organization earned nothing from "
    "that source. Reporting rates and reporter counts appear in Appendix Tables 5-2 through 5-11; regional totals "
    "appear in Table 5-13."
)
SAMPLE_SIZE_BLOCK = [
    "### How many organizations could we compare?",
    "",
    "Only organizations that reported income from a given revenue source were included in that source's comparison. The number of organizations varies widely by source.",
    "",
    "The 422 Black Hills records above are all Form 990-family filers with positive total revenue in 2022. Comparisons for Form 990 Part VIII contribution lines (government grants, federated campaigns, related-organization contributions, membership dues, and fundraising events) use a smaller eligible pool: 256 Black Hills organization-years where those lines exist (primarily full Form 990). Reporting rates for those rare sources are shares of that pool, not of all 422 records. Program service revenue uses a slightly different eligible count because 990-EZ filers can report it; Tables 5-2 through 5-11 provide reporter counts for each region.",
    "",
    "For total revenue, total contributions, program service revenue, mixed or unclassified contributions, and other revenue, most pairwise comparisons involved dozens to hundreds of organizations with positive amounts on both sides. Those comparisons are more stable.",
    "",
    "Some Part VIII lines are reported by far fewer organizations. In 2022, about 7% of eligible Black Hills filings (the Form 990 subset) reported federated campaign contributions, with 18 organizations showing a positive amount. Some benchmark comparisons for that source involved as few as three to five organizations with positive amounts. Related-organization contributions were reported by about 4% of eligible Black Hills filings, with 10 organizations showing a positive amount. Fundraising event contributions and membership dues fell in between, with moderate sample sizes in some benchmark pairs.",
    "",
    "Patterns for sources with very small reporter groups should be read with extra caution because a few organizations can have a large effect on the median. The reporter counts in Tables 5-2 through 5-11 provide context for every regional comparison.",
]
RESULTS_START = "We compared whether non-profit revenue sources differ"
APPENDIX_START = "# Appendix: Non-profit revenue source comparison details"
REGION_ORDER = ["Black Hills", "Billings", "Flagstaff", "Missoula", "Sioux Falls"]
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
TITLE_LABELS = {
    "total_revenue": "Total Revenue",
    "program_service_revenue": "Program Service Revenue",
    "total_contributions": "Total Contributions",
    "government_grants_received": "Government Grants Received",
    "federated_campaigns": "Federated Campaign Contributions",
    "related_org_contributions": "Related Organization Contributions",
    "membership_dues": "Membership Dues",
    "fundraising_events_contributions": "Fundraising Event Contributions",
    "mixed_unclassified_contributions": "Mixed or Unclassified Contributions",
    "residual_other_revenue": "Other Revenue",
}
FIGURE_TITLES = {
    **{
        variable: f"Figure 5-{index}: {TITLE_LABELS[variable]} by Region"
        for index, variable in enumerate(VARIABLES, start=2)
    },
}
TABLE_TITLES = {
    "overview": "Table 5-1: Median Reported Dollars by Revenue Source and Region",
    "aggregate": "Table 5-13: Total Reported Dollars by Revenue Source and Region (Aggregate)",
    "definitions": "Table 5-12: Revenue Source Definitions",
    **{
        variable: f"Table 5-{index}: {TITLE_LABELS[variable]} by Region"
        for index, variable in enumerate(VARIABLES, start=2)
    },
}
IMAGE_NAMES = {
    **{
        variable: f"updated-report-image-{index:02d}.png"
        for index, variable in enumerate(VARIABLES, start=22)
    },
}
DEFINITION_ROWS = [
    ["Total revenue", "All reported revenue for the organization in tax year 2022.", "Form 990, Form 990-EZ, and Form 990-PF totals."],
    ["Total contributions", "Total reported contributions, gifts, grants, and similar amounts.", "Form 990, Form 990-EZ, and Form 990-PF totals."],
    ["Program service revenue", "Money earned from services, programs, fees, or sales related to the organization's mission.", "Form 990 and Form 990-EZ; not comparable on Form 990-PF."],
    ["Government grants received", "Funds reported from government sources.", "Form 990 Part VIII Line 1e; comparisons use Form 990 filers only."],
    ["Federated campaign contributions", "Support reported through federated campaigns (for example, United Way-style allocations).", "Form 990 Part VIII Line 1a; comparisons use Form 990 filers only."],
    ["Related organization contributions", "Contributions from related organizations or affiliates.", "Form 990 Part VIII Line 1d; comparisons use Form 990 filers only."],
    ["Membership dues", "Membership dues reported on the return.", "Form 990 Part VIII Line 1b; comparisons use Form 990 filers only."],
    ["Fundraising event contributions", "Contributions tied to fundraising events.", "Form 990 Part VIII Line 1c; comparisons use Form 990 filers only."],
    ["Mixed / unclassified contributions", "Contribution dollars the forms do not break down cleanly by donor type.", "Form 990 Line 1f plus 990-EZ/PF contribution totals that cannot be split into Part VIII lines."],
    ["Other revenue", "Remaining revenue after program service revenue and the contribution components above.", "Calculated as total revenue minus those components."],
]


def money(value: float, *, signed: bool = False) -> str:
    prefix = ""
    if signed:
        prefix = "+" if float(value) > 0 else ""
    return f"{prefix}${float(value):,.0f}".replace("$-", "-$")


def p_value(value: float) -> str:
    return "&lt; 0.001" if float(value) < 0.001 else f"{float(value):.3f}"


def ci_text(row: pd.Series) -> str:
    return (
        f"{money(row['median_difference'], signed=True)} "
        f"(95% CI {money(row['median_difference_ci_low'], signed=True)} "
        f"to {money(row['median_difference_ci_high'], signed=True)})"
    )


def table_line(values: list[str]) -> str:
    return "| " + " | ".join(values) + " |"


def load_data() -> tuple[pd.DataFrame, pd.DataFrame]:
    summary = pd.read_csv(SUMMARY_CSV)
    rows: list[dict[str, object]] = []
    for variable in VARIABLES:
        variable_rows = summary.loc[summary["variable"].eq(variable)]
        first = variable_rows.iloc[0]
        rows.append(
            {
                "variable": variable,
                "region": "Black Hills",
                "positive_n": int(first["black_hills_positive_n"]),
                "median": float(first["black_hills_positive_median"]),
                "reporting_rate": float(first["black_hills_nonzero_percent"]),
            }
        )
        for region in REGION_ORDER[1:]:
            row = variable_rows.loc[variable_rows["benchmark_region"].eq(region)].iloc[0]
            rows.append(
                {
                    "variable": variable,
                    "region": region,
                    "positive_n": int(row["benchmark_positive_n"]),
                    "median": float(row["benchmark_positive_median"]),
                    "reporting_rate": float(row["benchmark_nonzero_percent"]),
                }
            )
    return summary, pd.DataFrame(rows)


def significant_table(summary: pd.DataFrame) -> list[str]:
    significant = summary.loc[summary["p_value"].lt(0.05)].copy()
    significant["variable_order"] = significant["variable"].map({v: i for i, v in enumerate(VARIABLES)})
    significant["region_order"] = significant["benchmark_region"].map(
        {region: i for i, region in enumerate(REGION_ORDER)}
    )
    significant = significant.sort_values(["variable_order", "region_order"])

    lines = [
        table_line(
            [
                "Revenue source",
                "Benchmark region",
                "Direction",
                "Black Hills median",
                "Benchmark median",
                "Median gap (95% CI)",
                "p-value",
            ]
        ),
        table_line(["---"] * 7),
    ]
    for _, row in significant.iterrows():
        lines.append(
            table_line(
                [
                    DISPLAY_LABELS[row["variable"]],
                    row["benchmark_region"],
                    f"{row['direction']}; significant",
                    money(row["black_hills_positive_median"]),
                    money(row["benchmark_positive_median"]),
                    ci_text(row),
                    p_value(row["p_value"]),
                ]
            )
        )
    return lines


def overview_table(values: pd.DataFrame) -> list[str]:
    lines = [
        table_line(["Revenue source", *REGION_ORDER]),
        table_line(["---"] * 6),
    ]
    for variable in VARIABLES:
        subset = values.loc[values["variable"].eq(variable)].set_index("region").reindex(REGION_ORDER)
        lines.append(
            table_line(
                [
                    DISPLAY_LABELS[variable],
                    *[money(subset.loc[region, "median"]) for region in REGION_ORDER],
                ]
            )
        )
    return lines


def aggregate_table() -> list[str]:
    frame = pd.read_csv(RAW_LEVEL_CSV)
    lines = [
        table_line(["Revenue source", *REGION_ORDER]),
        table_line(["---"] * 6),
    ]
    for variable in VARIABLES:
        subset = frame.loc[frame["variable"].eq(variable)].set_index("region_label").reindex(REGION_ORDER)
        lines.append(
            table_line(
                [
                    DISPLAY_LABELS[variable],
                    *[
                        money(round(float(subset.loc[region, "mean"]) * float(subset.loc[region, "n"])))
                        for region in REGION_ORDER
                    ],
                ]
            )
        )
    return lines


def source_table(summary: pd.DataFrame, values: pd.DataFrame, variable: str) -> list[str]:
    variable_summary = summary.loc[summary["variable"].eq(variable)].set_index("benchmark_region")
    variable_values = values.loc[values["variable"].eq(variable)].set_index("region")
    lines = [
        table_line(
            [
                "Region",
                "Reporting rate",
                "Positive reporters (n)",
                "Median reported dollars",
                "Black Hills minus benchmark region (95% CI)",
                "Pairwise p-value vs Black Hills",
            ]
        ),
        table_line(["---"] * 6),
        table_line(
            [
                "Black Hills",
                f"{100 * float(variable_values.loc['Black Hills', 'reporting_rate']):.1f}%",
                f"{int(variable_values.loc['Black Hills', 'positive_n']):,}",
                money(variable_values.loc["Black Hills", "median"]),
                "NA",
                "NA",
            ]
        ),
    ]
    for region in REGION_ORDER[1:]:
        row = variable_summary.loc[region]
        lines.append(
            table_line(
                [
                    region,
                    f"{100 * float(variable_values.loc[region, 'reporting_rate']):.1f}%",
                    f"{int(variable_values.loc[region, 'positive_n']):,}",
                    money(variable_values.loc[region, "median"]),
                    ci_text(row),
                    p_value(row["p_value"]),
                ]
            )
        )
    return lines


def build_block(summary: pd.DataFrame, values: pd.DataFrame) -> str:
    lines = [
        "### Non-profit revenue source comparison (2022)",
        "",
        "We compared whether non-profit revenue sources differ between the Black Hills and the benchmark regions using the approach described above (2022 filings; medians among positive reporters for the headline comparisons; reporting rates and regional totals in the appendix). The pattern varies by revenue source and benchmark region.",
        "",
        "Appendix Table 5-1 provides the exact median reported dollars among positive reporters for all ten revenue sources and regions. Appendix Table 5-13 provides the corresponding regional totals summed across all organizations in the analysis file.",
        "",
        "### What the 2022 revenue-source comparison shows",
        "",
        "Across the ten revenue sources, the Black Hills medians were often near the lower end of the regional range, but the pattern was not uniform; the size and direction of each gap depended on the source and the benchmark region.",
        "",
        "For the two broadest measures, the regions were broadly similar. The Black Hills median total revenue was $176,760, somewhat below the benchmark medians of about $208,000 to $222,000, though that gap is small next to the wide spread in organization size within every region. The Black Hills median total contributions was $110,308, lower than Billings, Flagstaff, and Missoula but higher than Sioux Falls ($100,000).",
        "",
        "Several sources showed wider gaps, with the Black Hills median well below the benchmark: program service revenue versus Flagstaff ($106,346 versus $188,968); government grants versus Billings ($163,367 versus $274,793) and Flagstaff ($163,367 versus $468,152); fundraising event contributions versus Billings, Missoula, and Sioux Falls ($12,427 versus $38,516, $41,756, and $70,761); other revenue versus Billings and Sioux Falls ($14,805 versus $27,630 and $32,467); mixed or unclassified contributions versus Flagstaff ($65,792 versus $92,278); and federated campaign contributions versus Sioux Falls ($30,096 versus $150,655). Across these sources the gap ran the same way, with the Black Hills amount the lower of the two.",
        "",
        "Black Hills was not lower everywhere. Its median was the higher one in at least one comparison for total contributions, membership dues, federated campaign contributions, and related-organization contributions. For membership dues the regional medians were similar in size, and related-organization contributions rest on very few reporters (noted above), so that ordering is unstable. Complete regional values and reporter counts for every comparison appear in Appendix Tables 5-1 through 5-11.",
        "",
        "Participation and dollar amounts can tell different stories. Government grants are a useful example: about 44% of eligible Black Hills organizations reported this source compared with about 34% in Billings, while the median reported amount was $163,367 in Black Hills and $274,793 in Billings. A region can have more organizations using a revenue source while still showing a lower typical dollar amount among the organizations that use it.",
        "",
        "The connection to the Black Hills Area Community Foundation is most direct where participation and dollar amounts point in different directions. For example, a relatively large share of eligible Black Hills organizations reported government grants, but the typical grant amount among those organizations was lower than in every benchmark region. Black Hills organizations also reported lower typical amounts for program service revenue and several other sources. Together, these patterns suggest that local organizations may be accessing some funding channels but receiving smaller amounts through them. If this interpretation matches local experience, the Foundation could use it to inform conversations about revenue diversification, grant-seeking capacity, and financial planning. These data do not show why the patterns occur or describe the circumstances of any individual organization.",
        "",
        "These findings describe observed 2022 revenue patterns, not causal explanations or proof that the regions differ in every comparable organization. Differences in organization type, local service needs, government funding, tourism-related activity, or the presence of large institutions could contribute but would require context beyond these filings. The results also do not separate individual giving from institutional giving. Appendix Tables 5-1 through 5-11 provide the complete numerical and technical comparison details.",
        "",
        "# <span style=\"color: #666666;\">6. Conclusions</span>",
        "",
        "<span style=\"color: #666666;\">Write when finished</span>",
        "",
        APPENDIX_START,
        "",
        "**The appendix provides the complete 2022 revenue-source comparison details. Table 5-1 lists the median reported dollars among organizations with a positive amount for each source. Table 5-13 lists the corresponding regional totals summed across all organizations in the analysis file. Figures 5-2 through 5-11 show each source separately, and their paired tables provide reporting rates, reporter counts, medians, median gaps, confidence intervals, and p-values. The comparison columns apply only to each benchmark region versus Black Hills; no Black Hills-only p-value or median-gap confidence interval exists, so those Black Hills cells are marked NA. Table 5-12 defines the revenue sources. Rare revenue sources have fewer reporting organizations; see the sample-size discussion in the non-profit revenue analysis section.**",
        "",
        f"**{TABLE_TITLES['overview']}**",
        "",
        *overview_table(values),
        "",
        AGGREGATE_TABLE_NOTE,
        "",
        f"**{TABLE_TITLES['aggregate']}**",
        "",
        *aggregate_table(),
        "",
    ]
    for variable in VARIABLES:
        lines.extend(
            [
                f"**{FIGURE_TITLES[variable]}**",
                "",
                f"![{DISPLAY_LABELS[variable]} by region](updated_report_assets/{IMAGE_NAMES[variable]})",
                "",
                f"**{TABLE_TITLES[variable]}**",
                "",
                *source_table(summary, values, variable),
                "",
            ]
        )
    lines.extend(
        [
            f"**{TABLE_TITLES['definitions']}**",
            "",
            table_line(["Revenue source", "What it measures", "How it is reported on IRS filings"]),
            table_line(["---"] * 3),
            *[table_line(row) for row in DEFINITION_ROWS],
            "",
            "**The full client presentation and supporting analysis files include reporting rates and additional source-by-source interpretation.**",
            "",
        ]
    )
    return "\n".join(lines)


def update_relevant_details(text: str) -> str:
    section_start = text.index("### Non-profit revenue analysis")
    results_start = text.index("## <span style=\"color: #666666;\">Results of non-profit organization analysis</span>", section_start)
    section = text[section_start:results_start]

    paragraph_start = section.index("For tax year 2022,")
    paragraph_end = section.index("\n\n", paragraph_start)
    replacement = (
        "For tax year 2022, after requiring positive total revenue, the primary analysis file includes "
        "1,799 filing records for 1,797 organizations: 422 in the Black Hills and 1,377 in benchmark counties. "
        "The filing mix is 1,085 Form 990 records, 563 Form 990-EZ records, and 151 Form 990-PF records. "
        "We ran the analysis both with and without 25 records for hospitals, universities, and political "
        "organizations. Including those records did not reverse the direction of any of the 40 Black Hills-to-"
        "benchmark comparisons and did not change the overall interpretation. The results presented here exclude "
        "those organizations so the regions better represent comparable non-profit peers."
    )
    section = section[:paragraph_start] + replacement + section[paragraph_end:]

    definitions_start = section.find("The revenue sources analyzed in this section are defined below.")
    caveat_start = section.index("The data cannot cleanly separate individual giving")
    if definitions_start >= 0:
        section = (
            section[:definitions_start]
            + "Definitions for the revenue sources analyzed in this section are provided in Appendix Table 5-12.\n\n"
            + section[caveat_start:]
        )
    elif "Appendix Table 5-12" not in section:
        section = (
            section[:caveat_start]
            + "Definitions for the revenue sources analyzed in this section are provided in Appendix Table 5-12.\n\n"
            + section[caveat_start:]
        )

    old_tail = (
        "A small number of very large organizations can affect how a region looks in the data. "
        "The findings below describe typical dollars among organizations that report a source, "
        "not a complete picture of every non-profit in the region."
    )
    if old_tail in section:
        section = section.replace(
            old_tail,
            MEDIAN_JUSTIFICATION + "\n\n" + "\n".join(SAMPLE_SIZE_BLOCK),
        )
    elif MEDIAN_JUSTIFICATION not in section:
        results_marker = "## <span style=\"color: #666666;\">Results of non-profit organization analysis</span>"
        insert_at = section.index(results_marker)
        block = MEDIAN_JUSTIFICATION + "\n\n" + "\n".join(SAMPLE_SIZE_BLOCK) + "\n\n"
        section = section[:insert_at] + block + section[insert_at:]

    return text[:section_start] + section + text[results_start:]


def main() -> None:
    summary, values = load_data()
    text = update_relevant_details(MD_PATH.read_text(encoding="utf-8"))
    start = text.index(RESULTS_START)
    appendix = text.index(APPENDIX_START, start)
    conclusions = text.rfind('# <span style="color: #666666;">6. Conclusions</span>', start, appendix)
    if conclusions < 0:
        raise RuntimeError("Could not locate the gray Conclusions block.")
    replacement = build_block(summary, values)
    MD_PATH.write_text(text[:start] + replacement, encoding="utf-8")
    print(f"Updated {MD_PATH}")


if __name__ == "__main__":
    main()
