"""Quick audit of the color-coded final report docx."""

from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from docx import Document
from sync_final_report_black_sections import (
    APPENDIX_START,
    paragraph_has_image,
    paragraph_is_gray,
)

DOCX = Path(__file__).resolve().parents[1] / (
    "docs/final_report/Philanthropic characteristics of the Black Hills Area and co color-coded.docx"
)


def main() -> None:
    doc = Document(DOCX)
    full = "\n".join(p.text for p in doc.paragraphs)
    issues: list[str] = []

    if full.count("Main analysis content goes here") != 2:
        issues.append(
            f"Gray stubs: expected 2 donation/nonprofit stubs, got {full.count('Main analysis content goes here')}"
        )
    if APPENDIX_START not in full:
        issues.append("Missing appendix H2 heading")
    if full.count("Participation and dollar amounts") > 1:
        issues.append("Duplicate participation paragraph")

    for stale in (
        "Black Hills share of aggregate",
        "Typical amount, Black Hills",
        "These data do not show why",
        "in plain terms",
    ):
        if stale in full:
            issues.append(f"Stale text: {stale!r}")

    sig = [t for t in doc.tables if t.rows[0].cells[0].text.strip() == "Revenue source"]
    app = [t for t in doc.tables if t.rows[0].cells[0].text.strip() == "Benchmark region"]
    if len(sig) != 1:
        issues.append(f"Significant-results tables: {len(sig)} (expect 1)")
    if len(app) != 10:
        issues.append(f"Appendix tables: {len(app)} (expect 10)")
    if sig and sig[0].rows[0].cells[2].text.strip() != "Direction":
        issues.append("Results table missing Direction column")

    empty = sum(
        1
        for p in doc.paragraphs
        if not p.text.strip() and not paragraph_is_gray(p) and not paragraph_has_image(p)
    )
    if empty:
        issues.append(f"Empty black paragraphs: {empty}")

    for n in range(4, 15):
        if f"Figure {n}." not in full:
            issues.append(f"Missing Figure {n} caption")

    app_i = next(
        (i for i, p in enumerate(doc.paragraphs) if p.text.strip().startswith(APPENDIX_START)),
        None,
    )
    imgs_results = (
        sum(1 for i, p in enumerate(doc.paragraphs) if i < app_i and paragraph_has_image(p))
        if app_i
        else 0
    )
    imgs_total = sum(1 for p in doc.paragraphs if paragraph_has_image(p))
    if imgs_results != 1:
        issues.append(f"Overview chart count before appendix: {imgs_results} (expect 1)")
    if imgs_total != 11:
        issues.append(f"Total embedded charts: {imgs_total} (expect 11)")

    if "Figure 1a" in full and "Figure 1a." not in full:
        issues.append("Demographics cites Figure 1a but chart not embedded")
    if "Figure 3" in full and "Figure 3." not in full:
        issues.append("Demographics cites Figure 3 but chart not embedded")

    print(f"Auditing: {DOCX.name}")
    print(f"Issues: {len(issues)}")
    for item in issues:
        print(f"  - {item}")
    if not issues:
        print("  - No automated issues found")


if __name__ == "__main__":
    main()
