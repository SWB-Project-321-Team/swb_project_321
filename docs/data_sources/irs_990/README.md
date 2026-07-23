# IRS Form 990 source notes

**Documentation reviewed:** 2026-07-23. The former project S3 bucket was intentionally deleted; S3-oriented pipeline and diagnostic material is retained for historical lineage only.

This directory holds IRS Form 990 source documentation that belongs beside the broader data-source index.

- [`data_sources_and_available_variables.csv`](data_sources_and_available_variables.csv) inventories available sources and variables. Documentation CSVs are intentionally commit-visible under the repository’s scoped data policy.
- The implemented IRS TEOS ingestion workflow is documented in [`../../../python/ingest/irs_990/README.md`](../../../python/ingest/irs_990/README.md).
- IRS parser and historical S3 inspection utilities live in [`../../../scripts/diagnostics/irs_990/`](../../../scripts/diagnostics/irs_990/).
- Project scope and requested variables are maintained in [`../../planning/project_scope_and_variables.md`](../../planning/project_scope_and_variables.md).

Return to the [data-source index](../README.md).
