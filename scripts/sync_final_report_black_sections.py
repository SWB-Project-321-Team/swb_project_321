"""Sync black (non-gray) paragraphs from final report markdown into color-coded docx files."""

from __future__ import annotations

from copy import deepcopy
from pathlib import Path

import pandas as pd
from docx import Document
from docx.oxml import OxmlElement
from docx.oxml.ns import qn
from docx.table import Table
from docx.text.paragraph import Paragraph

REVENUE_DEFINITION_COLUMN_FRACTIONS = (0.17, 0.33, 0.50)

REPO_ROOT = Path(__file__).resolve().parents[1]
MD_PATH = REPO_ROOT / "docs/final_report/Philanthropic characteristics of the Black Hills Area and co (1).md"
DOCX_PATH = REPO_ROOT / "docs/final_report/Philanthropic characteristics of the Black Hills Area and co color-coded.docx"
PAIRWISE_CSV = (
    REPO_ROOT
    / "python/analysis/revenue_sources_black_hills/results/tables/client_2022_pairwise_presentation_summary.csv"
)
SIGNIFICANT_TABLE_HEADERS = [
    "Revenue source",
    "Benchmark region",
    "Direction",
    "Black Hills median",
    "Benchmark median",
    "Median gap (95% CI)",
    "p-value",
]
APPENDIX_TABLE_HEADERS = SIGNIFICANT_TABLE_HEADERS[1:]
FINDINGS_INTRO_PREFIX = (
    "Among organizations that reported a positive amount for the source, Black Hills showed"
)
FINDINGS_AFTER_PREFIX = "For total revenue and total contributions"
STALE_PRE_PREFIXES = (
    "The GivingTuesday 990 DataMart is based on public IRS Form 990-family filings",
    "The current project file covers tax year 2022 for the Black Hills and benchmark-region counties.",
    "The GivingTuesday data contain financial detail that is not available",
    "Hospitals, universities, and political organizations are flagged in the project data",
    "Because these are public filing records, a small number of very large organizations can affect regional totals",
    "The revenue categories mean the following",
    "Program service revenue is money an organization earns",
)
STALE_RESULTS_PREFIXES = (
    "These data do not show why the revenue mix differs",
    "These findings describe reported revenue patterns by region in 2022. They do not establish why",
)

GRAY = "666666"
RESULTS_START = "We compared whether non-profit revenue sources differ"
APPENDIX_START = "Appendix: Non-profit revenue source comparison details"
CAPTION_MARKER = "Figure 4."
DATA_SOURCES_HEADING = "GivingTuesday 990 DataMart (data sources)"
RELEVANT_DETAILS_HEADING = "Non-profit revenue analysis (relevant details)"
HEADING_4_TITLES = {
    "How many organizations could we compare?",
    "Findings where Black Hills differed from a benchmark region (2022)",
    DATA_SOURCES_HEADING,
    RELEVANT_DETAILS_HEADING,
}
REVENUE_DEFINITIONS_INTRO_PREFIX = "The revenue sources analyzed in this section are defined below"
REVENUE_DEFINITION_TABLE_HEADERS = [
    "Revenue source",
    "What it measures",
    "How it is reported on IRS filings",
]
REVENUE_DEFINITION_TABLE_ROWS = [
    [
        "Total revenue",
        "All reported revenue for the organization in tax year 2022.",
        "Form 990, Form 990-EZ, and Form 990-PF totals.",
    ],
    [
        "Total contributions",
        "Total reported contributions, gifts, grants, and similar amounts.",
        "Form 990, Form 990-EZ, and Form 990-PF totals.",
    ],
    [
        "Program service revenue",
        "Money earned from services, programs, fees, or sales related to the organization's mission.",
        "Form 990 and Form 990-EZ; not comparable on Form 990-PF.",
    ],
    [
        "Government grants received",
        "Funds reported from government sources.",
        "Form 990 Part VIII Line 1e; pairwise tests use Form 990 filers only.",
    ],
    [
        "Federated campaign contributions",
        "Support reported through federated campaigns (for example, United Way-style allocations).",
        "Form 990 Part VIII Line 1a; pairwise tests use Form 990 filers only.",
    ],
    [
        "Related organization contributions",
        "Contributions from related organizations or affiliates.",
        "Form 990 Part VIII Line 1d; pairwise tests use Form 990 filers only.",
    ],
    [
        "Membership dues",
        "Membership dues reported on the return.",
        "Form 990 Part VIII Line 1b; pairwise tests use Form 990 filers only.",
    ],
    [
        "Fundraising event contributions",
        "Contributions tied to fundraising events.",
        "Form 990 Part VIII Line 1c; pairwise tests use Form 990 filers only.",
    ],
    [
        "Mixed / unclassified contributions",
        "Contribution dollars the forms do not break down cleanly by donor type.",
        "Form 990 Line 1f plus 990-EZ/PF contribution totals that cannot be split into Part VIII lines.",
    ],
    [
        "Other revenue",
        "Remaining revenue after program service revenue and the contribution components above.",
        "Calculated as total revenue minus those components.",
    ],
]
REVENUE_DEFINITIONS_CAVEAT_PREFIX = (
    "The data cannot cleanly separate individual giving from foundation grants"
)
APPENDIX_H3_TITLES = {
    "Total revenue",
    "Program service revenue",
    "Total contributions",
    "Government grants received",
    "Federated campaign contributions",
    "Related organization contributions",
    "Membership dues",
    "Fundraising event contributions",
    "Mixed / unclassified contributions",
    "Other revenue",
}


def is_gray_md_line(line: str) -> bool:
    stripped = line.strip()
    return bool(stripped) and "#666666" in stripped


def extract_black_paragraphs_from_md(md_text: str) -> list[str]:
    paragraphs: list[str] = []
    in_table = False
    for raw_line in md_text.splitlines():
        stripped = raw_line.strip()
        if not stripped:
            in_table = False
            continue
        if stripped.startswith("|"):
            in_table = True
            continue
        if in_table or stripped.startswith("!["):
            continue
        if is_gray_md_line(stripped):
            continue
        if stripped.startswith("*") and stripped.endswith("*") and stripped.count("*") >= 2:
            paragraphs.append(stripped.strip("*").strip())
            continue
        if stripped.startswith("#### "):
            paragraphs.append(stripped.removeprefix("#### ").strip())
            continue
        if stripped.startswith("### "):
            paragraphs.append(stripped.removeprefix("### ").strip())
            continue
        if stripped.startswith("## ") and not is_gray_md_line(stripped):
            paragraphs.append(stripped.removeprefix("## ").strip())
            continue
        if stripped.startswith("**Figure "):
            paragraphs.append(stripped.replace("**", "").strip())
            continue
        if stripped.startswith("*Figure "):
            paragraphs.append(stripped.strip("*").strip())
            continue
        if stripped.startswith("- "):
            paragraphs.append(stripped)
            continue
        paragraphs.append(stripped)
    return paragraphs


def paragraph_is_gray(paragraph: Paragraph) -> bool:
    for run in paragraph.runs:
        if run.font.color.rgb is not None and str(run.font.color.rgb).lower().replace("#", "") == GRAY:
            return True
        r_pr = run._element.find(qn("w:rPr"))
        if r_pr is not None:
            color_el = r_pr.find(qn("w:color"))
            if color_el is not None:
                if (color_el.get(qn("w:val")) or "").lower() == GRAY:
                    return True
    return False


def paragraph_has_image(paragraph: Paragraph) -> bool:
    return bool(paragraph._element.findall(".//" + qn("w:drawing")))


def set_paragraph_style(paragraph: Paragraph, style_name: str) -> None:
    try:
        paragraph.style = style_name
    except KeyError:
        paragraph.style = "Normal"


def style_for_black_text(text: str, *, in_appendix: bool = False) -> str:
    stripped = text.strip()
    if stripped.startswith(APPENDIX_START):
        return "Heading 2"
    if stripped in HEADING_4_TITLES:
        return "Heading 4"
    if in_appendix and stripped in APPENDIX_H3_TITLES:
        return "Heading 3"
    return "Normal"


def set_paragraph_text_black(
    paragraph: Paragraph,
    text: str,
    *,
    style: str | None = None,
) -> None:
    for child in list(paragraph._element):
        if child.tag.endswith("}r"):
            paragraph._element.remove(child)
    if style is not None:
        set_paragraph_style(paragraph, style)
    run = paragraph.add_run(text)
    if text.strip().startswith("Figure "):
        run.italic = True


def insert_paragraph_after(
    paragraph: Paragraph,
    text: str,
    *,
    style: str | None = None,
) -> Paragraph:
    resolved_style = style if style is not None else style_for_black_text(text)
    new_p = deepcopy(paragraph._element)
    for child in list(new_p):
        if child.tag.endswith("}r"):
            new_p.remove(child)
    p_pr = new_p.find(qn("w:pPr"))
    if p_pr is not None:
        p_style = p_pr.find(qn("w:pStyle"))
        if p_style is not None:
            p_pr.remove(p_style)
    paragraph._element.addnext(new_p)
    new_para = Paragraph(new_p, paragraph._parent)
    set_paragraph_text_black(new_para, text, style=resolved_style)
    return new_para


def remove_paragraph(paragraph: Paragraph) -> None:
    paragraph._element.getparent().remove(paragraph._element)


def remove_aggregate_share_table(doc: Document) -> None:
    for table in list(doc.tables):
        if len(table.rows) < 2:
            continue
        header = " ".join(cell.text for cell in table.rows[0].cells).lower()
        if "black hills share of aggregate" in header:
            table._element.getparent().remove(table._element)


def content_width_emu(doc: Document) -> int:
    section = doc.sections[0]
    return int(section.page_width - section.left_margin - section.right_margin)


def emu_to_twips(emu: int) -> int:
    return int(round(emu * 1440 / 914400))


def set_cell_width_twips(cell, width_twips: int) -> None:
    tc = cell._tc
    tc_pr = tc.find(qn("w:tcPr"))
    if tc_pr is None:
        tc_pr = OxmlElement("w:tcPr")
        tc.insert(0, tc_pr)
    tc_w = tc_pr.find(qn("w:tcW"))
    if tc_w is None:
        tc_w = OxmlElement("w:tcW")
        tc_pr.append(tc_w)
    tc_w.set(qn("w:type"), "dxa")
    tc_w.set(qn("w:w"), str(width_twips))


def set_table_full_page_width(
    table: Table,
    doc: Document,
    column_fractions: tuple[float, ...],
) -> None:
    """Span the printable area and use fixed column widths (template cells are often narrow)."""
    total_emu = content_width_emu(doc)
    total_twips = emu_to_twips(total_emu)

    tbl = table._tbl
    tbl_pr = tbl.tblPr
    if tbl_pr is None:
        tbl_pr = OxmlElement("w:tblPr")
        tbl.insert(0, tbl_pr)

    for tag in ("w:tblW", "w:tblLayout", "w:tblInd"):
        existing = tbl_pr.find(qn(tag))
        if existing is not None:
            tbl_pr.remove(existing)

    tbl_w = OxmlElement("w:tblW")
    tbl_w.set(qn("w:type"), "dxa")
    tbl_w.set(qn("w:w"), str(total_twips))
    tbl_pr.append(tbl_w)

    layout = OxmlElement("w:tblLayout")
    layout.set(qn("w:type"), "fixed")
    tbl_pr.append(layout)

    if len(column_fractions) != len(table.columns):
        raise ValueError("column_fractions must match table column count")

    assigned_twips = 0
    col_twips: list[int] = []
    for idx, fraction in enumerate(column_fractions):
        if idx == len(column_fractions) - 1:
            col_twips.append(total_twips - assigned_twips)
        else:
            width = int(round(total_twips * fraction))
            col_twips.append(width)
            assigned_twips += width

    for col_idx, width_twips in enumerate(col_twips):
        width_emu = int(round(width_twips * 914400 / 1440))
        table.columns[col_idx].width = width_emu
        for row in table.rows:
            set_cell_width_twips(row.cells[col_idx], width_twips)


def remove_revenue_definition_tables(doc: Document) -> None:
    for table in list(doc.tables):
        if len(table.rows) < 2:
            continue
        header = [cell.text.strip() for cell in table.rows[0].cells]
        if header == REVENUE_DEFINITION_TABLE_HEADERS:
            table._element.getparent().remove(table._element)


def insert_definition_table_after(
    doc: Document,
    paragraph: Paragraph,
    headers: list[str],
    rows: list[list[str]],
) -> Table:
    template = find_appendix_template_table(doc)
    template_header = template.rows[0].cells
    template_data = template.rows[1].cells
    style_map = [0, 1, 1]

    table = doc.add_table(rows=1 + len(rows), cols=len(headers))
    table.style = template.style

    all_rows = [headers, *rows]
    for row_idx, row_values in enumerate(all_rows):
        template_row = template_header if row_idx == 0 else template_data
        for col_idx, value in enumerate(row_values):
            set_cell_text_like_template(
                table.rows[row_idx].cells[col_idx],
                value,
                template_row[style_map[col_idx]],
            )

    tbl_element = table._tbl
    doc.element.body.remove(tbl_element)
    paragraph._element.addnext(tbl_element)
    set_table_full_page_width(table, doc, REVENUE_DEFINITION_COLUMN_FRACTIONS)
    return table


def upsert_revenue_definitions_table(doc: Document) -> None:
    intro_para: Paragraph | None = None
    for paragraph in doc.paragraphs:
        if paragraph.text.strip().startswith(REVENUE_DEFINITIONS_INTRO_PREFIX):
            intro_para = paragraph
            break
    if intro_para is None:
        return

    remove_following_revenue_definition_table(intro_para)

    to_remove: list[Paragraph] = []
    found_intro = False
    for paragraph in doc.paragraphs:
        text = paragraph.text.strip()
        if text.startswith(REVENUE_DEFINITIONS_INTRO_PREFIX):
            found_intro = True
            continue
        if not found_intro:
            continue
        if text.startswith(REVENUE_DEFINITIONS_CAVEAT_PREFIX):
            break
        if next_element_is_table(paragraph):
            break
        if text.startswith("A small number of very large organizations"):
            break

    for paragraph in reversed(to_remove):
        remove_paragraph(paragraph)

    insert_definition_table_after(
        doc,
        intro_para,
        REVENUE_DEFINITION_TABLE_HEADERS,
        REVENUE_DEFINITION_TABLE_ROWS,
    )


def remove_following_revenue_definition_table(paragraph: Paragraph) -> None:
    next_element = paragraph._element.getnext()
    if next_element is not None and next_element.tag.endswith("tbl"):
        paragraph._element.getparent().remove(next_element)


def remove_significant_results_tables(doc: Document) -> None:
    for table in list(doc.tables):
        if len(table.rows) != 11:
            continue
        header = [cell.text.strip() for cell in table.rows[0].cells]
        if header and header[0] == "Revenue source" and (
            "Black Hills median" in header or "Typical amount" in " ".join(header)
        ):
            table._element.getparent().remove(table._element)


def paragraph_following_element(paragraph: Paragraph) -> Paragraph | None:
    next_element = paragraph._element.getnext()
    while next_element is not None:
        if next_element.tag.endswith("p"):
            return Paragraph(next_element, paragraph._parent)
        if next_element.tag.endswith("tbl"):
            next_element = next_element.getnext()
            continue
        next_element = next_element.getnext()
    return None


def split_pre_section(pre: list[str]) -> tuple[list[str], list[str]]:
    for idx, text in enumerate(pre):
        if text == RELEVANT_DETAILS_HEADING:
            return pre[:idx], pre[idx:]
    legacy_split = 7
    if len(pre) > legacy_split and pre[legacy_split].startswith("This section uses"):
        return pre[:legacy_split], [RELEVANT_DETAILS_HEADING, *pre[legacy_split:]]
    return pre[:legacy_split], pre[legacy_split:]


def sync_data_sources_block(doc: Document, data_pre: list[str]) -> None:
    if not data_pre:
        return

    heading = data_pre[0] if data_pre[0] == DATA_SOURCES_HEADING else None
    body = data_pre[1:] if heading else data_pre

    anchor: Paragraph | None = None
    in_data_sources = False
    for paragraph in doc.paragraphs:
        text = paragraph.text.strip()
        if paragraph_is_gray(paragraph) and text == "Data sources":
            in_data_sources = True
            continue
        if not in_data_sources:
            continue
        if paragraph_is_gray(paragraph) and text.startswith("Selection and demographic"):
            break
        if paragraph_is_gray(paragraph):
            anchor = paragraph

    if anchor is None:
        return

    to_remove: list[Paragraph] = []
    removing = False
    for paragraph in doc.paragraphs:
        if paragraph._element is anchor._element:
            removing = True
            continue
        if not removing:
            continue
        if paragraph_is_gray(paragraph) and paragraph.text.strip().startswith(
            "Selection and demographic"
        ):
            break
        if not paragraph_is_gray(paragraph) and paragraph.text.strip():
            to_remove.append(paragraph)

    for paragraph in reversed(to_remove):
        remove_paragraph(paragraph)

    current = anchor
    if heading:
        current = insert_paragraph_after(current, heading, style="Heading 4")
    for text in body:
        current = insert_paragraph_after(current, text, style="Normal")


def split_md_sections(black_paragraphs: list[str]) -> tuple[list[str], list[str], list[str]]:
    pre: list[str] = []
    results: list[str] = []
    appendix: list[str] = []
    section = "pre"
    for text in black_paragraphs:
        if text.startswith(RESULTS_START):
            section = "results"
        elif text.startswith(APPENDIX_START):
            section = "appendix"
        if section == "pre":
            pre.append(text)
        elif section == "results":
            results.append(text)
        else:
            appendix.append(text)
    return pre, results, appendix


def split_results_around_caption(results: list[str]) -> tuple[list[str], str | None, list[str]]:
    before: list[str] = []
    after: list[str] = []
    caption: str | None = None
    for text in results:
        if caption is None and (CAPTION_MARKER in text or text.startswith("Median reported")):
            caption = text
            continue
        if caption is None:
            before.append(text)
        else:
            after.append(text)
    return before, caption, after


def remove_stale_pre_paragraphs(doc: Document) -> None:
    to_remove: list[Paragraph] = []
    for paragraph in doc.paragraphs:
        text = paragraph.text.strip()
        if text.startswith(RESULTS_START) or text.startswith(APPENDIX_START):
            break
        if paragraph_is_gray(paragraph) or paragraph_has_image(paragraph) or not text:
            continue
        if any(text.startswith(prefix) for prefix in STALE_PRE_PREFIXES):
            to_remove.append(paragraph)
    for paragraph in reversed(to_remove):
        remove_paragraph(paragraph)


def find_appendix_paragraph_index(doc: Document) -> int:
    for idx, paragraph in enumerate(doc.paragraphs):
        text = paragraph.text.strip()
        if text.startswith(APPENDIX_START):
            return idx
        if text.startswith("The tables below list all p-values"):
            return idx
    raise RuntimeError("Could not find appendix section in document.")


def ensure_appendix_heading(doc: Document) -> None:
    if any(p.text.strip().startswith(APPENDIX_START) for p in doc.paragraphs):
        return
    for paragraph in doc.paragraphs:
        if paragraph.text.strip().startswith("The tables below list all p-values"):
            new_p = deepcopy(paragraph._element)
            for child in list(new_p):
                if child.tag.endswith("}r"):
                    new_p.remove(child)
            paragraph._element.addprevious(new_p)
            heading = Paragraph(new_p, paragraph._parent)
            set_paragraph_text_black(heading, APPENDIX_START, style="Heading 2")
            return


def find_nonprofit_results_anchor(doc: Document) -> Paragraph:
    paragraphs = doc.paragraphs
    for idx, paragraph in enumerate(paragraphs):
        if not paragraph_is_gray(paragraph):
            continue
        if "Results of non-profit organization analysis" not in paragraph.text:
            continue
        for j in range(idx + 1, min(idx + 6, len(paragraphs))):
            candidate = paragraphs[j]
            if paragraph_is_gray(candidate) and "Main analysis content goes here" in candidate.text:
                return candidate
    raise RuntimeError("Could not find non-profit results anchor paragraph.")


def paragraph_index(doc: Document, paragraph: Paragraph) -> int:
    for idx, candidate in enumerate(doc.paragraphs):
        if candidate._element is paragraph._element:
            return idx
    raise RuntimeError("Paragraph not found in document.")


def sync_nonprofit_relevant_details(doc: Document, relevant_pre: list[str]) -> None:
    if not relevant_pre:
        return

    heading = relevant_pre[0] if relevant_pre[0] == RELEVANT_DETAILS_HEADING else None
    details = relevant_pre[1:] if heading else relevant_pre

    target_reader: Paragraph | None = None
    target_results: Paragraph | None = None
    in_nonprofit_section = False
    for paragraph in doc.paragraphs:
        text = paragraph.text.strip()
        if paragraph_is_gray(paragraph) and "Characterization of non-profit organizations" in text:
            if "..." not in text:
                in_nonprofit_section = True
            continue
        if not in_nonprofit_section:
            continue
        if paragraph_is_gray(paragraph) and text.startswith("Anything the reader needs to know"):
            target_reader = paragraph
        if paragraph_is_gray(paragraph) and "Results of non-profit organization analysis" in text:
            target_results = paragraph
            break

    if target_reader is None or target_results is None:
        return

    to_remove: list[Paragraph] = []
    removing = False
    for paragraph in doc.paragraphs:
        if paragraph._element is target_reader._element:
            removing = True
            continue
        if paragraph._element is target_results._element:
            break
        if removing and not paragraph_is_gray(paragraph):
            to_remove.append(paragraph)

    for paragraph in reversed(to_remove):
        remove_paragraph(paragraph)

    current = target_reader
    if heading:
        current = insert_paragraph_after(current, heading, style="Heading 4")
    for text in details:
        current = insert_paragraph_after(current, text, style="Normal")

    upsert_revenue_definitions_table(doc)


def replace_results_block(doc: Document, results: list[str]) -> None:
    ensure_appendix_heading(doc)
    appendix_idx = find_appendix_paragraph_index(doc)
    anchor = find_nonprofit_results_anchor(doc)
    anchor_idx = paragraph_index(doc, anchor)
    image_para: Paragraph | None = None
    to_remove: list[Paragraph] = []

    for i, paragraph in enumerate(doc.paragraphs):
        if i >= appendix_idx:
            break
        text = paragraph.text.strip()
        if paragraph_is_gray(paragraph):
            continue
        if i < anchor_idx and (
            text.startswith(RESULTS_START)
            or text.startswith("Each revenue source was analyzed")
            or text.startswith("Figure 4.")
            or text.startswith("How many organizations could we compare")
            or text.startswith("Findings where Black Hills differed")
        ):
            to_remove.append(paragraph)
            continue
        if i <= anchor_idx:
            continue
        if paragraph_has_image(paragraph):
            if image_para is None:
                image_para = paragraph
            else:
                to_remove.append(paragraph)
            continue
        if text:
            to_remove.append(paragraph)

    for paragraph in reversed(to_remove):
        remove_paragraph(paragraph)

    before, caption, after = split_results_around_caption(results)
    anchor = find_nonprofit_results_anchor(doc)
    current = anchor
    for text in before:
        current = insert_paragraph_after(current, text, style="Normal")

    if image_para is None:
        for paragraph in doc.paragraphs:
            if paragraph_has_image(paragraph):
                image_para = paragraph
                break

    if caption and image_para is not None:
        caption_para = paragraph_following_element(image_para)
        if caption_para is not None and not paragraph_has_image(caption_para):
            set_paragraph_text_black(caption_para, caption, style="Normal")
            current = caption_para
        else:
            current = insert_paragraph_after(image_para, caption, style="Normal")
    elif image_para is not None:
        current = image_para
    else:
        current = anchor

    for text in after:
        current = insert_paragraph_after(
            current,
            text,
            style=style_for_black_text(text),
        )

    upsert_significant_results_table(doc)


def format_p_value(p_value: float) -> str:
    if pd.isna(p_value):
        return ""
    if float(p_value) < 0.001:
        return "< 0.001"
    return f"{float(p_value):.3f}"


def format_dollar(value: float) -> str:
    if float(value) < 0:
        return f"-${abs(float(value)):,.0f}"
    return f"${float(value):,.0f}"


def format_gap_ci(row: pd.Series) -> str:
    gap = row["median_difference"]
    low = row["median_difference_ci_low"]
    high = row["median_difference_ci_high"]
    return (
        f"{format_dollar(gap)} (95% CI {format_dollar(low)} to {format_dollar(high)})"
    )


def format_direction(row: pd.Series) -> str:
    direction = str(row["direction"]).strip()
    suffix = "significant" if float(row["p_value"]) < 0.05 else "not significant"
    if direction.endswith("; significant") or direction.endswith("; not significant"):
        return direction
    return f"{direction}; {suffix}"


def load_significant_results_table_rows() -> list[list[str]]:
    """Significant 2022 pairwise rows in presentation table order."""

    presentation_order = [
        ("federated_campaigns", "Sioux Falls"),
        ("fundraising_events_contributions", "Billings"),
        ("fundraising_events_contributions", "Missoula"),
        ("fundraising_events_contributions", "Sioux Falls"),
        ("government_grants_received", "Billings"),
        ("government_grants_received", "Flagstaff"),
        ("mixed_unclassified_contributions", "Flagstaff"),
        ("residual_other_revenue", "Billings"),
        ("residual_other_revenue", "Sioux Falls"),
        ("program_service_revenue", "Flagstaff"),
    ]
    df = pd.read_csv(PAIRWISE_CSV)
    rows: list[list[str]] = []
    for variable, benchmark in presentation_order:
        match = df.loc[
            df["variable"].eq(variable) & df["benchmark_region"].eq(benchmark)
        ].iloc[0]
        rows.append(
            [
                str(match["variable_label"]),
                str(match["benchmark_region"]),
                format_direction(match),
                f"${float(match['black_hills_positive_median']):,.0f}",
                f"${float(match['benchmark_positive_median']):,.0f}",
                format_gap_ci(match),
                format_p_value(float(match["p_value"])),
            ]
        )
    return rows


def find_appendix_template_table(doc: Document) -> Table:
    for table in doc.tables:
        header = [cell.text.strip() for cell in table.rows[0].cells]
        if header == APPENDIX_TABLE_HEADERS:
            return table
    raise RuntimeError("Could not find appendix table template in document.")


def copy_cell_format(source_cell, target_cell) -> None:
    src_tc = source_cell._tc
    tgt_tc = target_cell._tc
    src_tcPr = src_tc.find(qn("w:tcPr"))
    if src_tcPr is None:
        return
    existing = tgt_tc.find(qn("w:tcPr"))
    if existing is not None:
        tgt_tc.remove(existing)
    tgt_tc.insert(0, deepcopy(src_tcPr))


def set_cell_text_like_template(target_cell, text: str, template_cell) -> None:
    copy_cell_format(template_cell, target_cell)
    target_cell.text = ""
    paragraph = target_cell.paragraphs[0]
    template_paragraph = template_cell.paragraphs[0]
    paragraph.alignment = template_paragraph.alignment
    run = paragraph.add_run(text)
    if template_paragraph.runs:
        template_run = template_paragraph.runs[0]
        template_r_pr = template_run._element.find(qn("w:rPr"))
        if template_r_pr is not None:
            existing = run._element.find(qn("w:rPr"))
            if existing is not None:
                run._element.remove(existing)
            run._element.insert(0, deepcopy(template_r_pr))


def insert_significant_results_table_after(
    doc: Document,
    paragraph: Paragraph,
    headers: list[str],
    rows: list[list[str]],
) -> Table:
    template = find_appendix_template_table(doc)
    template_header = template.rows[0].cells
    template_data = template.rows[1].cells
    style_map = [1, 0, 1, 2, 3, 4, 5]

    table = doc.add_table(rows=1 + len(rows), cols=len(headers))
    table.style = template.style

    all_rows = [headers, *rows]
    for row_idx, row_values in enumerate(all_rows):
        template_row = template_header if row_idx == 0 else template_data
        for col_idx, value in enumerate(row_values):
            set_cell_text_like_template(
                table.rows[row_idx].cells[col_idx],
                value,
                template_row[style_map[col_idx]],
            )

    tbl_element = table._tbl
    doc.element.body.remove(tbl_element)
    paragraph._element.addnext(tbl_element)
    return table


def remove_following_significant_table(paragraph: Paragraph) -> None:
    next_element = paragraph._element.getnext()
    while next_element is not None and next_element.tag.endswith("tbl"):
        parent = paragraph._element.getparent()
        parent.remove(next_element)
        next_element = paragraph._element.getnext()


def upsert_significant_results_table(doc: Document) -> None:
    intro_para: Paragraph | None = None
    for paragraph in doc.paragraphs:
        if paragraph.text.strip().startswith(FINDINGS_INTRO_PREFIX):
            intro_para = paragraph
            break
    if intro_para is None:
        return

    remove_following_significant_table(intro_para)

    to_remove: list[Paragraph] = []
    found_intro = False
    for paragraph in doc.paragraphs:
        text = paragraph.text.strip()
        if text.startswith(FINDINGS_INTRO_PREFIX):
            found_intro = True
            continue
        if not found_intro:
            continue
        if text.startswith(FINDINGS_AFTER_PREFIX):
            break
        if text.startswith("- "):
            to_remove.append(paragraph)
            continue
        if next_element_is_table(paragraph):
            break

    for paragraph in reversed(to_remove):
        remove_paragraph(paragraph)

    insert_significant_results_table_after(
        doc,
        intro_para,
        SIGNIFICANT_TABLE_HEADERS,
        load_significant_results_table_rows(),
    )


def next_element_is_table(paragraph: Paragraph) -> bool:
    next_element = paragraph._element.getnext()
    return next_element is not None and next_element.tag.endswith("tbl")


def extract_figure_captions_from_md(md_text: str) -> list[str]:
    captions: list[str] = []
    for line in md_text.splitlines():
        stripped = line.strip()
        if stripped.startswith("**Figure "):
            captions.append(stripped.replace("**", "").strip())
    return captions


def apply_figure_captions(doc: Document, captions: list[str]) -> None:
    """Set the paragraph immediately after each embedded image to the figure caption."""

    caption_idx = 0
    paragraphs = doc.paragraphs
    for i, paragraph in enumerate(paragraphs):
        if not paragraph_has_image(paragraph):
            continue
        if caption_idx >= len(captions):
            break
        next_idx = i + 1
        while next_idx < len(paragraphs) and paragraph_has_image(paragraphs[next_idx]):
            next_idx += 1
        if next_idx >= len(paragraphs):
            break
        caption_para = paragraphs[next_idx]
        if caption_para.text.strip().startswith(SIGNIFICANT_TABLE_HEADERS[0]):
            caption_para = insert_paragraph_after(paragraph, captions[caption_idx])
        elif paragraph_is_gray(caption_para):
            caption_para = insert_paragraph_after(paragraph, captions[caption_idx])
        else:
            set_paragraph_text_black(
                caption_para,
                captions[caption_idx],
                style="Normal",
            )
        caption_idx += 1


def apply_black_paragraph_formatting(doc: Document) -> None:
    """Ensure synced black text uses Normal/Heading styles, not inherited Heading 2."""

    in_appendix = False
    for paragraph in doc.paragraphs:
        text = paragraph.text.strip()
        if text.startswith(APPENDIX_START):
            in_appendix = True
        if paragraph_is_gray(paragraph) or paragraph_has_image(paragraph):
            continue
        if not text:
            continue
        set_paragraph_style(paragraph, style_for_black_text(text, in_appendix=in_appendix))
        if text.startswith("Figure "):
            for run in paragraph.runs:
                run.italic = True


def remove_empty_black_paragraphs(doc: Document) -> None:
    for paragraph in list(doc.paragraphs):
        if paragraph_is_gray(paragraph) or paragraph_has_image(paragraph):
            continue
        if not paragraph.text.strip():
            remove_paragraph(paragraph)


def sync_docx(docx_path: Path, black_paragraphs: list[str]) -> Path:
    doc = Document(docx_path)
    remove_aggregate_share_table(doc)
    remove_revenue_definition_tables(doc)
    remove_significant_results_tables(doc)
    pre, results, appendix = split_md_sections(black_paragraphs)

    remove_stale_pre_paragraphs(doc)
    data_pre, relevant_pre = split_pre_section(pre)
    sync_data_sources_block(doc, data_pre)
    sync_nonprofit_relevant_details(doc, relevant_pre)

    replace_results_block(doc, results)

    ensure_appendix_heading(doc)
    appendix_idx = 0
    in_appendix = False
    for paragraph in doc.paragraphs:
        text = paragraph.text.strip()
        if text.startswith(APPENDIX_START):
            in_appendix = True
        elif not in_appendix and text.startswith("The tables below list all p-values"):
            in_appendix = True
        if not in_appendix or paragraph_is_gray(paragraph) or paragraph_has_image(paragraph):
            continue
        text = paragraph.text.strip()
        if not text:
            continue
        if appendix_idx < len(appendix):
            set_paragraph_text_black(
                paragraph,
                appendix[appendix_idx],
                style=style_for_black_text(appendix[appendix_idx], in_appendix=True),
            )
            appendix_idx += 1

    apply_figure_captions(doc, extract_figure_captions_from_md(MD_PATH.read_text(encoding="utf-8")))
    apply_black_paragraph_formatting(doc)
    remove_empty_black_paragraphs(doc)

    try:
        doc.save(docx_path)
        return docx_path
    except PermissionError:
        fallback = docx_path.with_name(docx_path.stem + "_synced.docx")
        doc.save(fallback)
        print(f"  (file locked; saved to {fallback.name})")
        return fallback


def main() -> None:
    md_text = MD_PATH.read_text(encoding="utf-8")
    black_paragraphs = extract_black_paragraphs_from_md(md_text)
    pre, results, appendix = split_md_sections(black_paragraphs)
    print(f"Markdown black paragraphs: {len(black_paragraphs)} (pre={len(pre)}, results={len(results)}, appendix={len(appendix)})")
    gray_check = "Need to write it at the end based on conclusions"
    if not DOCX_PATH.exists():
        print(f"Missing: {DOCX_PATH}")
        return
    saved = sync_docx(DOCX_PATH, black_paragraphs)
    doc = Document(saved)
    full = "\n".join(p.text for p in doc.paragraphs)
    sig_table_ok = any(
        len(t.rows) == 11
        and [c.text.strip() for c in t.rows[0].cells] == SIGNIFICANT_TABLE_HEADERS
        and "Government grants received" in t.rows[6].cells[0].text
        for t in doc.tables
    )
    print(
        f"{saved.name}: gray intact={gray_check in full}, "
        f"sample-size heading={'How many organizations could we compare?' in full}, "
        f"eligible pool 256={'256 Black Hills organization-years' in full}, "
        f"nonprofit data ref={'described under Data sources' in full}, "
        f"ci note={'Significance is based on the two-sided permutation test' in full}, "
        f"sig results table={sig_table_ok}, "
        f"aggregate gone={'Black Hills share of aggregate' not in full}, "
        f"figure labels={'Figure 14.' in full}, "
        f"results body Normal={all(doc.paragraphs[i].style.name == 'Normal' for i in range(len(doc.paragraphs)) if doc.paragraphs[i].text.strip().startswith('Only organizations that reported'))}, "
        f"h4 sample size={any(p.text.strip()=='How many organizations could we compare?' and p.style.name=='Heading 4' for p in doc.paragraphs)}, "
        f"revenue defs table={any([c.text.strip() for c in t.rows[0].cells]==REVENUE_DEFINITION_TABLE_HEADERS for t in doc.tables)}, "
        f"h4 data sources={any(p.text.strip()==DATA_SOURCES_HEADING and p.style.name=='Heading 4' for p in doc.paragraphs)}, "
        f"h4 relevant details={any(p.text.strip()==RELEVANT_DETAILS_HEADING and p.style.name=='Heading 4' for p in doc.paragraphs)}"
    )


if __name__ == "__main__":
    main()
