# IRS EO BMF Pipeline

> **Infrastructure status (2026-07-23):** source discovery and local processing remain documented, but the former project S3 bucket was intentionally deleted. Upload and S3 verification steps are historical and must not recreate `swb-321-irs990-teos`.

This pipeline downloads, uploads, verifies, filters, and analyzes the IRS EO BMF
state CSV extracts. It follows the repo's filter-first combine rule from
`secrets/coding_rules/CODING_RULES.md`: each raw state file is filtered to
benchmark geography before any cross-state combine occurs.

Run from repo root:

```powershell
python python/run_irs_bmf.py
```

Steps:

| Step | Script | Output |
|------|--------|--------|
| 01 | `01_fetch_bmf_release.py` | raw IRS EO BMF state CSVs + raw manifest |
| 02 | `02_upload_bmf_release_to_s3.py` | Bronze raw + manifest upload |
| 03 | `03_verify_bmf_source_local_s3.py` | raw source/local/S3 verification report |
| 04 | `04_filter_bmf_to_benchmark_local.py` | combined and yearly benchmark-filtered IRS EO BMF Parquets + filter manifest |
| 05 | `05_upload_filtered_bmf_to_s3.py` | Silver filtered benchmark upload |
| 06 | `06_verify_filtered_bmf_local_s3.py` | filtered local/S3 verification report |
| 07 | `07_extract_analysis_variables_local.py` | IRS EO BMF analysis rowset + geography metrics + field metrics + coverage/mapping docs |
| 08 | `08_upload_analysis_outputs.py` | Silver analysis output upload |

Additional analysis outputs:
- `01_data/staging/irs_bmf/irs_bmf_analysis_variables.parquet`
- `01_data/staging/irs_bmf/irs_bmf_analysis_geography_metrics.parquet`
- `01_data/staging/irs_bmf/irs_bmf_analysis_field_metrics.parquet`
- `01_data/raw/irs_bmf/metadata/irs_bmf_analysis_variable_coverage.csv`
- `docs/deliverables/preprocessing/technical_reference/variable_mappings/irs_bmf_analysis_variable_mapping.md`
- `docs/deliverables/preprocessing/technical_reference/pipelines/irs_bmf_pipeline.md`

Official analysis S3 layout:
- `silver/irs990/bmf/analysis/irs_bmf_analysis_variables.parquet`
- `silver/irs990/bmf/analysis/irs_bmf_analysis_geography_metrics.parquet`
- `silver/irs990/bmf/analysis/irs_bmf_analysis_field_metrics.parquet`
- `silver/irs990/bmf/analysis/documentation/irs_bmf_pipeline.md`
- `silver/irs990/bmf/analysis/variable_mappings/irs_bmf_analysis_variable_mapping.md`
- `silver/irs990/bmf/analysis/quality/coverage/irs_bmf_analysis_variable_coverage.csv`

Compatibility notes:
- `python/ingest/irs_990/00_fetch_bmf.py` remains available as a thin wrapper.
- `python/ingest/irs_990/00b_filter_bmf_to_benchmark_upload_silver.py` remains available as a thin wrapper.
- The legacy compatibility filtered parquet is still refreshed at `01_data/staging/org/irs_bmf_benchmark_counties.parquet`.
