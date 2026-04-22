# NCCS BMF Pipeline

This pipeline discovers, downloads, uploads, verifies, filters, and uploads yearly representative NCCS BMF assets.
It also now builds and uploads official `2022-2024` BMF analysis outputs.

Selection rules:
- the standard run remains `2022-present`
- explicit `2021` support is implemented, but only when the upstream NCCS source exposes a usable `2021` legacy BMF file
- `2021-2022` use the latest available legacy BMF file from the matching year
- `2023+` uses one representative raw monthly snapshot per year
- completed years prefer December
- the current year uses the latest available month

Run from repo root:

```powershell
python python/run_nccs_bmf.py
```

Steps:

| Step | Script | Output |
|------|--------|--------|
| 01 | `01_discover_bmf_release.py` | `latest_release.json` + source page snapshots |
| 02 | `02_download_bmf_release.py` | yearly raw BMF CSVs under `01_data/raw/nccs_bmf/raw/` |
| 03 | `03_upload_bmf_release_to_s3.py` | Bronze raw + metadata upload |
| 04 | `04_verify_bmf_source_local_s3.py` | raw source/local/S3 verification report |
| 05 | `05_filter_bmf_to_benchmark_local.py` | yearly benchmark-filtered Parquets under `01_data/staging/nccs_bmf/` |
| 06 | `06_upload_filtered_bmf_to_s3.py` | Silver filtered yearly Parquet upload |
| 07 | `07_extract_analysis_variables_local.py` | BMF analysis rowset + geography metrics + NTEE field metrics + coverage/mapping docs |
| 08 | `08_upload_analysis_outputs.py` | Silver analysis output upload |

The filtered yearly outputs are the BMF inputs for the `combined_990` source-union pipeline.
That contract is intentionally filter-first: each yearly raw BMF file is reduced to benchmark
geography in step 05, and downstream combine stages read only those filtered yearly outputs.

Additional analysis outputs:
- `01_data/staging/nccs_bmf/nccs_bmf_analysis_variables.parquet`
- `01_data/staging/nccs_bmf/nccs_bmf_analysis_geography_metrics.parquet`
- `01_data/staging/nccs_bmf/nccs_bmf_analysis_field_metrics.parquet`
- `01_data/raw/nccs_bmf/metadata/nccs_bmf_analysis_variable_coverage.csv`
- `docs/analysis/nccs_bmf_analysis_variable_mapping.md`
- `docs/data_processing/nccs_bmf_pipeline.md`

Official analysis S3 layout:
- `silver/nccs_bmf/analysis/nccs_bmf_analysis_variables.parquet`
- `silver/nccs_bmf/analysis/nccs_bmf_analysis_geography_metrics.parquet`
- `silver/nccs_bmf/analysis/nccs_bmf_analysis_field_metrics.parquet`
- `silver/nccs_bmf/analysis/metadata/<coverage, mapping, pipeline doc>`
