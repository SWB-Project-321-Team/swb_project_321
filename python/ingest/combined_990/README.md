# Combined Filtered 990 Union + Master Pipeline

This pipeline builds two outputs from already-filtered inputs:
- GivingTuesday benchmark filings
- NCCS postcard benchmark output restricted to `tax_year >= 2022`
- NCCS efile annual benchmark outputs for every published year from `2022` onward
- NCCS BMF yearly benchmark outputs for `2022-present`

Design rules:
- one source-preserving union table where one row still corresponds to one original filtered source row
- one organization-year master table where one row corresponds to one `EIN + tax_year`
- source-native columns are always prefixed by source family
- harmonized fields always carry per-field provenance columns
- postcard remains a separate sibling source and continues to provide `harm_gross_receipts_under_25000_flag`
- BMF overlay is limited to approved identity/classification fields and only on exact `EIN + tax_year`
- numeric helper columns treat literal `NA` amount sentinels as missing while keeping the source-faithful harmonized string values unchanged
- `harm_is_hospital` and `harm_is_university` are normalized to lowercase `true` / `false` when a source provides recognized boolean-ish tokens
- duplicate source rows are intentionally preserved when the underlying filtered source contains duplicate `EIN + tax_year` records
- `combined_990` must continue consuming only the finalized filtered GT Silver artifact `givingtuesday_990_filings_benchmark.parquet`; it does not read the GT step-07 pre-Silver mixed build
- this pipeline already complies with `CODING_RULES.md` because its union step reads only finalized filtered source artifacts
- the master table is built from the finalized union frame using field-specific precedence and keeps:
  - value-origin provenance for each selected field
  - selected-union-row provenance for audit/debug clarity
  - explicit filing-vs-reference semantics for financial fields
  - a `nonfinancial_only` record type when no selected financial values exist
  - limited cross-year identity backfill for unresolved `org_name/state/zip5`
  - exact-year raw NCCS BMF identity rescue for any rows still unresolved after the cross-year pass
  - conflict flags plus `master_conflict_detail.csv` and `master_conflict_detail.parquet` artifacts for field-level disagreement review

Source precedence in the default master build now treats efile as the filing-source replacement for Core:
- identity/geography: `givingtuesday_datamart > nccs_postcard > nccs_efile > nccs_bmf`
- financials: `givingtuesday_datamart > nccs_efile > nccs_bmf`
- `harm_filing_form`: `givingtuesday_datamart > nccs_postcard > nccs_efile`
- `harm_gross_receipts_under_25000_flag`: `nccs_postcard` only

Run from repo root:

```powershell
python python/run_combined_990.py
```

Steps:

| Step | Script | Output |
|------|--------|--------|
| 01 | `01_build_combined_filtered_local.py` | union parquet + master parquet + metadata + diagnostics under `01_data/staging/combined_990/` |
| 02 | `02_upload_combined_filtered_to_s3.py` | Silver union/master parquet + metadata upload |
| 03 | `03_verify_combined_filtered_local_s3.py` | local/S3 size verification report for both parquets + metadata |

Master-specific metadata files:
- `master_column_dictionary.csv`
- `master_field_selection_summary.csv`
- `master_conflict_summary.csv`
- `master_conflict_detail.csv`
- `master_conflict_detail.parquet`
