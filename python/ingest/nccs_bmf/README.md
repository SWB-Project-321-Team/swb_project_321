# NCCS BMF 2022-Present Pipeline

This pipeline discovers, downloads, uploads, verifies, filters, and uploads yearly representative NCCS BMF assets for `2022-present`.

Selection rules:
- `2022` uses the latest available legacy 2022 BMF file
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

The filtered yearly outputs are the BMF inputs for the `combined_990` source-union pipeline.
