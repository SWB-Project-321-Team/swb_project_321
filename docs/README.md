# Documentation

Documentation is grouped by purpose so planning material, technical references, analysis outputs, and client deliverables do not get mixed together.

| Directory | Contents |
| --- | --- |
| [`planning/`](planning/) | Analysis plans and the project scope/variable checklist |
| [`analysis/`](analysis/) | Source assessments, schema questions, and analysis results |
| [`analysis/revenue_sources/`](analysis/revenue_sources/) | Section 3/Q9 analysis, presentation, and chart assets |
| [`data_sources/`](data_sources/) | IRS 990, IRS SOI, and USAspending source notes |
| [`infrastructure/`](infrastructure/) | S3 snapshots, OneDrive setup, and infrastructure audits |
| [`deliverables/preprocessing/`](deliverables/preprocessing/) | Client guides and technical preprocessing references |
| [`deliverables/final_report/`](deliverables/final_report/) | Source, working, published, review-note, and supporting report artifacts |

## Key files

- [Project scope and variables](planning/project_scope_and_variables.md)
- [Analysis plan draft](planning/analysis_plan_draft.md)
- [Final report workflow](deliverables/final_report/README.md)
- [S3 decommissioning status and historical snapshots](infrastructure/s3/README.md)
- [Last confirmed S3 bucket snapshot (2026-05-02)](infrastructure/s3/bucket_tree_2026-05-02.md)
- [OneDrive structure](infrastructure/onedrive/structure.md)

Raw and curated data remain outside the repository. Documentation metadata and small fixtures are the only data-like artifacts that should be committed here.
