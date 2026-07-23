# Final report artifacts

**Repository status:** the final OneDrive delivery was imported and structurally verified on 2026-07-23. The two DOCX files under `published/` are the current client deliverables in this repository. The protected `source/` and generated `working/` files remain the editable/color-coded workflow and were not overwritten. Superseded PDF exports are omitted from the active tree and remain available through Git history.

The final report is separated by artifact role:

| Directory | Purpose |
| --- | --- |
| [`source/`](source/) | Protected teammate-provided source DOCX |
| [`working/`](working/) | Generated color-coded DOCX, Markdown, and extracted image assets |
| [`published/`](published/) | Current client-ready final report and appendices DOCX files |
| [`supporting/`](supporting/) | Supporting tables or report components |
| [`notes/`](notes/) | Review requests and change notes |

The active source is `source/black_hills_philanthropy_source.docx`. Generate the working copies from the repository root with:

```powershell
python scripts/reporting/update_final_report_from_source_docx.py
```

Validate the generated Word report without changing it with:

```powershell
python scripts/reporting/audit_final_report_docx.py
```

Do not manually treat the generated Markdown as the formatting source of truth. In the working DOCX, teammate/source content is gray (`#666666`), the project’s added GivingTuesday/Q9 content is black (`#000000`), and added Q9 table headers use light blue (`#DEEAF0`). After generation, run the structural audit above and confirm the source and working copies preserve paragraph/table text and embedded media.

Current source-DOCX image extracts live directly under `working/assets/`.

## Current Published Delivery

- [`published/black_hills_philanthropy_report.docx`](published/black_hills_philanthropy_report.docx) — final report received in the 2026-07-23 OneDrive ZIP.
- [`published/appendices.docx`](published/appendices.docx) — separate final appendices received in the same delivery.
- [`notes/final_delivery_2026-07-23.md`](notes/final_delivery_2026-07-23.md) — import provenance, hashes, and validation record.
