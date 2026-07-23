# Final report delivery import — 2026-07-23

## Delivery source

- ZIP filename: `OneDrive_1_7-23-2026.zip`
- ZIP SHA-256: `D278A0933E6B215EB9C4BF65DFAEBE12D74E7B6BD5FFFE292E7EE19501F2961A`
- Two identical ZIP copies were found: one in the repository root and one in Downloads.

## Published files

| Archive entry | Repository destination | Bytes | SHA-256 |
| --- | --- | ---: | --- |
| `Philanthropic characteristics of the Black Hills Area and co.docx` | `published/black_hills_philanthropy_report.docx` | 3,417,279 | `14748C695E9A9B5F40AA42BAA8DA4480EEE0BB6B266D4AD41CBC64574C2F0DC6` |
| `Appendices.docx` | `published/appendices.docx` | 752,516 | `419DFBC50D26418D8B2ACD99BCBAF7C914325057C5534EFA9E4A59E952EB289F` |

## Placement decisions

- The delivered DOCX files are client-ready artifacts, so they were placed under `published/`.
- The protected teammate source and generated color-coded working report were left unchanged.
- The older generic PDFs were not retained in the active tree because the final DOCX delivery supersedes them; they remain recoverable from Git history.

## Validation

- Both DOCX files open through `python-docx`.
- All internal Office-package relationship targets resolve.
- Main report structure: 322 paragraphs, 24 tables, 38 inline shapes.
- Appendices structure: 288 paragraphs, 20 tables, 13 inline shapes.
