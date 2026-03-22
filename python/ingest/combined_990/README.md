# Combined Filtered 990 Source Union Pipeline

This pipeline builds one source-preserving union table from already-filtered inputs:
- GivingTuesday benchmark filings
- NCCS postcard benchmark output
- NCCS Core charities + nonprofit benchmark outputs
- NCCS BMF yearly benchmark outputs for `2022-present`

Design rules:
- one output table only
- one row still corresponds to one original filtered source row
- source-native columns are always prefixed by source family
- harmonized fields always carry per-field provenance columns
- BMF overlay is limited to approved identity/classification fields and only on exact `EIN + tax_year`

Run from repo root:

```powershell
python python/run_combined_990.py
```

Steps:

| Step | Script | Output |
|------|--------|--------|
| 01 | `01_build_combined_filtered_local.py` | combined parquet + metadata + diagnostics under `01_data/staging/combined_990/` |
| 02 | `02_upload_combined_filtered_to_s3.py` | Silver parquet + metadata upload |
| 03 | `03_verify_combined_filtered_local_s3.py` | local/S3 size verification report |
