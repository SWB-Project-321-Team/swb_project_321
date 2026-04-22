# 990_givingtuesday/datamart

DataMart-first pipeline for SWB 321:
- uses **Combined Forms Datamart** + **Basic Fields** (990, 990EZ, 990PF)
- stores full raw files locally
- materializes one cached normalized parquet mirror of the raw Combined CSV for faster rebuilds in step 07
- uploads full raw + dictionary to Bronze before filtering
- writes two benchmark-filtered files to Silver
- can extract and upload one GT basic-only analysis-variable layer for the current analysis draft
- enforces strict source/local/S3 byte-size parity for required raw files
- uses **Polars lazy** processing in steps 06/07/08 to reduce memory pressure and improve throughput
- caches steps 03-08 by default when inputs and outputs are unchanged
- writes filtered schema snapshots, a full GT artifact manifest, and orchestrator step-timing summaries during curated verification/orchestration
- follows `secrets/coding_rules/CODING_RULES.md`: combine stages operate only on filtered/admitted inputs

## Independence Contract

- This pipeline is self-contained in `python/ingest/990_givingtuesday/datamart/`.
- It does **not** read code outputs from `990_givingtuesday/api` or `990_irs` pipelines.
- The only external pipeline inputs are location-reference CSVs:
  - `01_data/reference/GEOID_reference.csv`
  - `01_data/reference/zip_to_county_fips.csv`
- No `GEOID_reference.xlsx` fallback is used in this pipeline.

## Data Sources

- DataMarts page: <https://990data.givingtuesday.org/datamarts/>
- Public catalog API: `https://grantstory-api.gtdata.org/v2/public/678d25291eceb5d121099200/items?limit=200`

## Script Order

Run from repo root:

```powershell
python python/ingest/990_givingtuesday/datamart/01_fetch_datamart_catalog.py
python python/ingest/990_givingtuesday/datamart/02_export_data_dictionary.py
python python/ingest/990_givingtuesday/datamart/03_download_raw_datamarts.py
python python/ingest/990_givingtuesday/datamart/04_upload_bronze_raw_and_dictionary.py
python python/ingest/990_givingtuesday/datamart/05_verify_source_local_s3_sizes.py
python python/ingest/990_givingtuesday/datamart/06_build_basic_allforms_presilver.py
python python/ingest/990_givingtuesday/datamart/07_build_basic_plus_combined_presilver.py
python python/ingest/990_givingtuesday/datamart/08_filter_benchmark_outputs_local.py
python python/ingest/990_givingtuesday/datamart/09_upload_bronze_presilver_outputs.py
python python/ingest/990_givingtuesday/datamart/10_upload_silver_filtered_outputs.py
python python/ingest/990_givingtuesday/datamart/11_verify_curated_outputs_local_s3.py
python python/ingest/990_givingtuesday/datamart/13_extract_basic_analysis_variables_local.py
python python/ingest/990_givingtuesday/datamart/14_upload_analysis_outputs.py
```

Or run orchestrator:

```powershell
python python/ingest/990_givingtuesday/datamart/run_990_datamart_pipeline.py
```

Useful orchestrator options:

```powershell
python python/ingest/990_givingtuesday/datamart/run_990_datamart_pipeline.py --start-step 1 --end-step 5 --overwrite-download --refresh-catalog
python python/ingest/990_givingtuesday/datamart/run_990_datamart_pipeline.py --bucket swb-321-irs990-teos --region us-east-2 --tax-year-min 2022 --tax-year-max 2024 --emit both
```

## Local Outputs

- Raw files: `01_data/raw/givingtuesday_990/datamarts/raw/`
- Dictionary metadata: `01_data/raw/givingtuesday_990/datamarts/metadata/`
- Cached normalized Combined parquet mirror:
  - `01_data/raw/givingtuesday_990/datamarts/cache/givingtuesday_990_combined_forms_normalized.parquet`
  - `01_data/raw/givingtuesday_990/datamarts/cache/givingtuesday_990_combined_forms_normalized_manifest.json`
- Pre-Silver parquet:
  - `01_data/staging/filing/givingtuesday_990_basic_allforms_presilver.parquet`
  - `01_data/staging/filing/givingtuesday_990_basic_plus_combined_presilver.parquet`
    - both artifacts are already filtered/admitted before the heavy combine stages
- Filtered parquet:
  - `01_data/staging/filing/givingtuesday_990_basic_allforms_benchmark.parquet`
  - `01_data/staging/filing/givingtuesday_990_filings_benchmark.parquet`
  - `01_data/staging/filing/manifest_basic_allforms_filtered.json`
  - `01_data/staging/filing/manifest_filtered.json`
- Metadata:
  - `01_data/raw/givingtuesday_990/datamarts/metadata/schema_snapshot_basic_allforms_filtered.json`
  - `01_data/raw/givingtuesday_990/datamarts/metadata/schema_snapshot_filings_filtered.json`
  - `01_data/raw/givingtuesday_990/datamarts/metadata/curated_output_size_verification_report.csv`
  - `01_data/raw/givingtuesday_990/datamarts/metadata/gt_pipeline_artifact_manifest.json`
  - `01_data/raw/givingtuesday_990/datamarts/metadata/gt_pipeline_artifact_manifest.csv`
  - `01_data/raw/givingtuesday_990/datamarts/metadata/run_990_datamart_pipeline_step_timings.csv`
- GT analysis extraction:
  - `01_data/staging/filing/givingtuesday_990_basic_allforms_analysis_variables.parquet`
  - `01_data/staging/filing/givingtuesday_990_basic_allforms_analysis_region_metrics.parquet`
  - `01_data/raw/givingtuesday_990/datamarts/metadata/givingtuesday_990_basic_allforms_analysis_variable_coverage.csv`
  - `docs/analysis/givingtuesday_basic_analysis_variable_mapping.md`

## S3 Layout

- Bronze raw: `bronze/givingtuesday_990/datamarts/raw/`
- Bronze metadata: `bronze/givingtuesday_990/datamarts/metadata/`
- Bronze pre-Silver: `bronze/givingtuesday_990/datamarts/presilver/`
- Silver filtered: `silver/givingtuesday_990/filing/`
- Silver filtered metadata: `silver/givingtuesday_990/filing/metadata/`
- Silver analysis outputs: `silver/givingtuesday_990/analysis/`
- Silver analysis metadata: `silver/givingtuesday_990/analysis/metadata/`

## Notes

- `givingtuesday_990_filings_benchmark.parquet` remains the canonical downstream GivingTuesday input consumed by `combined_990`.
- `givingtuesday_990_basic_allforms_benchmark.parquet` is the benchmark-filtered basic-only analyst artifact.
- Step `06` now enforces the filtered-only combine rule upstream by keeping only tax-year-in-scope rows admitted by benchmark geography or by a Combined ROI key.
- Steps `03` through `08` support cached rebuild/verify skipping by default and can be forced with `--force-rebuild`.
- Step `03` now refreshes the cached normalized Combined parquet mirror so step `07` does not have to reparse the raw Combined CSV on every rebuild.
- Step `07` now combines only admitted Basic and Combined rows, so the expensive mixed build is restricted to the benchmark geography as early as safely possible.
- Step `07` supports configurable DuckDB tuning through CLI flags such as `--duckdb-threads`, `--duckdb-memory-limit`, `--duckdb-temp-dir`, and `--duckdb-max-temp-directory-size`.
- Steps `09` and `10` now use bounded parallel uploads driven by the artifact registry.
- The orchestrator can run steps `09` and `10` in parallel with `--parallel-late-stage` enabled, then records per-step elapsed seconds in `run_990_datamart_pipeline_step_timings.csv`.
- Step `11` supports `--verify-only-changed` by default, so unchanged curated artifacts reuse the last successful S3-size result instead of re-heading every object.
- Steps `07` and `08` now keep the GT benchmark outputs within the explicit `2022-2024` analysis window by default.
- Step `13` extracts GT analysis variables from the filtered basic-only artifact, including the approved NCCS BMF NTEE enrichment needed for classification analyses.
- Step `14` uploads the step-13 analysis parquet, region metrics parquet, coverage CSV, and mapping Markdown to the dedicated GT analysis S3 prefix.
- Step `08` keeps the canonical Silver mixed output stable by validating the ROI-scoped mixed stage and dropping the internal ROI-admission metadata columns before the final Silver write.

Default bucket: `swb-321-irs990-teos`
