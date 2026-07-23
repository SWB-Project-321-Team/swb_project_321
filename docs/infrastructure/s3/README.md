# S3 infrastructure status

**Status as of 2026-07-23:** the project bucket `swb-321-irs990-teos` was intentionally deleted. It is not an active project storage location and must not be recreated from the historical commands, pipeline documentation, inventories, or manifests in this repository.

The files in this directory are retained only for data lineage and reproducibility:

- [`bucket_tree_2026-05-02.md`](bucket_tree_2026-05-02.md) is the last confirmed bucket snapshot: 489 objects and 35,711,714,691 bytes.
- [`bucket_tree_2026-04-11.md`](bucket_tree_2026-04-11.md) is an earlier prefix-tree snapshot.
- [`bucket_inventory_2026-03-22.md`](bucket_inventory_2026-03-22.md) is the earliest retained row-level inventory.
- [`../audits/s3_pipeline_documentation_audit_2026-05-01.md`](../audits/s3_pipeline_documentation_audit_2026-05-01.md) records a historical audit performed while the bucket still existed.

S3 keys embedded in source-code documentation and retained metadata describe past publication targets. Active local work should use the configured local/OneDrive data root. Any future cloud-storage design requires a separately approved target; do not reuse the deleted bucket name.
