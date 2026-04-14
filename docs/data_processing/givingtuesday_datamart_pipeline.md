# GivingTuesday Datamart Pipeline

## Status

This document is the authoritative technical reference for the current GivingTuesday datamart pipeline implemented in this repository.

It reflects the code as it exists now under `python/ingest/990_givingtuesday/datamart/`, including the current benchmark tax-year window of `2022-2024`, the current ROI-first mixed-file build in step 07, the current GT final analysis extract in step 13, and the current GT analysis-upload step in step 14.

This document is intentionally detailed. It is meant to let another engineer or analyst understand:

- where the GT data comes from
- how it is stored locally and in S3
- the exact script order and responsibilities
- every major transformation from raw CSV data to the final GT analysis parquet
- how the pipeline is tested and verified
- what is direct-source, calculated, enriched, or imputed in the final analysis-ready dataset

## Pipeline At A Glance

The GivingTuesday datamart pipeline is a DataMart-first ingestion and curation pipeline. It does not build GT outputs from IRS XML filings or GT API-formatted record-by-record pulls. Instead, it starts from the published GT DataMart catalog, downloads the required GT raw datamart files in full, and then materializes a local curated workflow with staged Bronze, Silver, and analysis outputs.

At a high level, the pipeline has these phases:

1. Retrieve the GT datamart catalog and flatten it into human-usable metadata.
2. Export the field dictionary by reading remote CSV headers.
3. Download the required raw GT datamart CSVs.
4. Upload the raw GT files and core metadata to Bronze S3.
5. Strictly verify source/local/S3 byte parity for the required GT raw files.
6. Build an admitted Basic pre-Silver parquet from the three GT Basic raw files.
7. Build an ROI-scoped mixed GT pre-Silver parquet by combining only admitted GT Combined and GT Basic data.
8. Filter GT benchmark outputs to the benchmark regions of interest and attach benchmark geography.
9. Upload the GT pre-Silver parquet outputs to Bronze S3.
10. Upload the GT filtered benchmark outputs and related metadata to Silver S3.
11. Verify local/S3 parity for all curated GT datamart outputs and write pipeline-wide manifests.
12. Optionally run the local audit utility for raw-vs-built Basic comparisons.
13. Extract the final GT analysis-variable parquet from the filtered GT basic-only benchmark file.
14. Upload the final GT analysis outputs and metadata to the dedicated GT analysis S3 prefix.

The main artifact classes are:

- Raw data: downloaded GT CSV files exactly as distributed by the GT datamart.
- Metadata/manifests: catalog exports, field dictionary exports, required-dataset manifest, cached state manifests, build manifests, schema snapshots, and verification reports.
- Bronze pre-Silver artifacts: GT parquet artifacts produced before the final Silver benchmark publication step.
- Silver filtered artifacts: GT benchmark-filtered outputs that are intended as canonical curated GT filing outputs.
- Final analysis artifact: the GT analysis-variable parquet and region-metrics parquet used by downstream analysis work.

This pipeline now follows the `CODING_RULES.md` filtered-only combine contract:

- combine stages consume only filtered or explicitly admitted inputs
- upstream admission/filtering happens as close to the raw source as safely possible
- downstream rescue remains fallback-only and auditable

## Script Order And Responsibilities

The current canonical GT pipeline lives in:

- `python/ingest/990_givingtuesday/datamart/`

The canonical orchestrator is:

- `python/ingest/990_givingtuesday/datamart/run_990_datamart_pipeline.py`

The canonical step order is:


| Step | Script                                         | Primary role                                                                             | Main inputs                                                                           | Main outputs                                                                                               | External systems |
| ---- | ---------------------------------------------- | ---------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------- | ---------------------------------------------------------------------------------------------------------- | ---------------- |
| 01   | `01_fetch_datamart_catalog.py`                 | Fetch GT datamart catalog JSON                                                           | GT public catalog API                                                                 | `datamart_catalog_raw.json`                                                                                | HTTP             |
| 02   | `02_export_data_dictionary.py`                 | Flatten catalog, probe URLs, build field dictionary                                      | step-01 raw catalog JSON or refreshed API data                                        | `datamart_catalog.csv`, `datamart_catalog.md`, `datamart_fields.csv`, `datamart_fields.md`                 | HTTP             |
| 03   | `03_download_raw_datamarts.py`                 | Download required GT raw datamart CSVs and build Combined cache parquet                  | catalog CSV, GT raw URLs                                                              | raw GT CSV files, `required_datasets_manifest.csv`, download-state JSON, normalized Combined parquet cache | HTTP             |
| 04   | `04_upload_bronze_raw_and_dictionary.py`       | Upload GT raw files and raw-phase metadata to Bronze                                     | local raw GT CSVs and selected metadata                                               | Bronze raw and Bronze metadata objects in S3                                                               | S3               |
| 05   | `05_verify_source_local_s3_sizes.py`           | Strict raw byte-parity verification                                                      | catalog CSV, local raw GT files, Bronze S3 raw objects                                | `size_verification_report.csv`                                                                             | HTTP, S3         |
| 06   | `06_build_basic_allforms_presilver.py`         | Build one admitted GT Basic pre-Silver parquet                                           | GT Basic raw CSVs for 990, 990EZ, 990PF, GT Combined normalized parquet cache, ROI reference files | `givingtuesday_990_basic_allforms_presilver.parquet`, build manifest                         | local files      |
| 07   | `07_build_basic_plus_combined_presilver.py`    | Build ROI-scoped mixed GT pre-Silver parquet from admitted inputs                        | GT Basic pre-Silver parquet, GT Combined normalized parquet cache, ROI reference files | `givingtuesday_990_basic_plus_combined_presilver.parquet`, build manifest                    | local files      |
| 08   | `08_filter_benchmark_outputs_local.py`         | Write benchmark-filtered GT outputs                                                      | step-06 and step-07 parquet files, ROI reference files                                | GT benchmark-filtered basic and mixed parquets, manifests, schema snapshots                                | local files      |
| 09   | `09_upload_bronze_presilver_outputs.py`        | Upload GT pre-Silver parquet outputs to Bronze                                           | step-06 and step-07 parquet outputs                                                   | Bronze pre-Silver S3 objects                                                                               | S3               |
| 10   | `10_upload_silver_filtered_outputs.py`         | Upload GT benchmark-filtered outputs and metadata to Silver                              | step-08 outputs, manifests, schema snapshots                                          | Silver GT parquet and metadata objects in S3                                                               | S3               |
| 11   | `11_verify_curated_outputs_local_s3.py`        | Verify local/S3 parity for curated GT output artifacts and write pipeline-wide manifests | Bronze/Silver GT curated outputs and S3 objects                                       | curated verification report, GT pipeline artifact manifests, orchestrator summary references               | S3               |
| 13   | `13_extract_basic_analysis_variables_local.py` | Build final GT analysis-variable parquet and GT region-metrics parquet                   | filtered GT basic-only benchmark parquet, NCCS BMF lookup files, IRS EO BMF raw files | GT analysis parquet, GT region metrics parquet, coverage CSV, analysis mapping Markdown                    | local files      |
| 14   | `14_upload_analysis_outputs.py`                | Upload GT analysis outputs and metadata to the dedicated analysis Silver prefix          | step-13 analysis parquet, step-13 region metrics parquet, coverage CSV, mapping Markdown | Silver GT analysis parquet and metadata objects in S3                                                    | S3               |


### Important orchestration notes

- `run_990_datamart_pipeline.py` is the canonical orchestrator for steps `01-11`, `13`, and `14`.
- The orchestrator currently supports `--parallel-late-stage`, which allows step 09 and step 10 to run in parallel after step 08 has completed.
- Step `12` (`12_audit_basic_raw_vs_basic_allforms_local.py`) is a local audit utility, not part of the normal orchestrated production path.
- The canonical filter/upload scripts are:
  - `08_filter_benchmark_outputs_local.py`
  - `09_upload_bronze_presilver_outputs.py`
  - `10_upload_silver_filtered_outputs.py`
  - `14_upload_analysis_outputs.py`

## How Data Is Retrieved

### Step 01: GT catalog retrieval

The GT raw data discovery process starts with the GT public catalog API:

- `https://grantstory-api.gtdata.org/v2/public/678d25291eceb5d121099200/items?limit=200`

The script:

- `python/ingest/990_givingtuesday/datamart/01_fetch_datamart_catalog.py`

fetches the GT catalog documents and writes the full raw payload to:

- `01_data/raw/givingtuesday_990/datamarts/metadata/datamart_catalog_raw.json`

This step is the source of truth for GT catalog discovery. It does not yet decide which datasets are required for the pipeline.

### Step 02: catalog flattening and field dictionary export

The script:

- `python/ingest/990_givingtuesday/datamart/02_export_data_dictionary.py`

takes the raw GT catalog documents and performs several metadata transformations:

1. It loads the raw catalog JSON or refreshes the catalog directly from the GT API.
2. It flattens the nested document structure into row-based catalog records.
3. It probes each GT dataset URL with HEAD-style metadata logic to capture:
  - URL status code
  - source content length in bytes
  - URL status text
4. It writes the flattened catalog to:
  - `datamart_catalog.csv`
  - `datamart_catalog.md`
5. It builds a field dictionary by reading remote header rows only from each GT dataset and writes:
  - `datamart_fields.csv`
  - `datamart_fields.md`

This is an important design choice: the field dictionary is built without downloading every GT dataset body. It only inspects header rows remotely.

### Required dataset selection

The GT pipeline does not download every GT datamart file. It selects a small required set from the catalog using rules in `common.py`.

The required GT datasets are:

- `Combined Forms Datamart`
- `Basic Fields` with `form_type = 990`
- `Basic Fields` with `form_type = 990EZ`
- `Basic Fields` with `form_type = 990PF`

This means the GT pipeline is intentionally built on one Combined datamart file plus three form-specific Basic datamart files.

### Step 03: raw file download and Combined cache materialization

The script:

- `python/ingest/990_givingtuesday/datamart/03_download_raw_datamarts.py`

does the following:

1. Loads the flattened catalog CSV from step 02.
2. Resolves the four required GT datasets.
3. Downloads the raw GT CSV files into the GT raw directory.
4. Verifies local file sizes against the GT source byte counts captured in step 02.
5. Writes a required-dataset manifest describing what was selected and downloaded.
6. Builds a cached normalized parquet mirror of the large Combined raw CSV.

The Combined cache is a critical optimization. The pipeline does not want to reparse the large raw Combined CSV on every later rebuild. Instead, step 03 calls `ensure_combined_normalized_cache(...)`, which materializes:

- `01_data/raw/givingtuesday_990/datamarts/cache/givingtuesday_990_combined_forms_normalized.parquet`
- `01_data/raw/givingtuesday_990/datamarts/cache/givingtuesday_990_combined_forms_normalized_manifest.json`

Step 03 also writes:

- `01_data/raw/givingtuesday_990/datamarts/metadata/required_datasets_manifest.csv`
- `01_data/raw/givingtuesday_990/datamarts/metadata/download_state_manifest.json`

### Retrieval metadata captured

By the end of step 03, the GT retrieval layer has captured:

- raw GT catalog JSON
- flattened catalog CSV and Markdown
- field dictionary CSV and Markdown
- GT URL health metadata
- GT source byte counts
- required-dataset manifest
- download-state manifest
- downloaded raw GT CSV files
- normalized Combined parquet cache and its manifest

## Where Data Is Stored

### Local directory layout

The GT pipeline uses these main local locations.

#### Raw GT root

- `01_data/raw/givingtuesday_990/datamarts/`

#### Raw GT CSV files

- `01_data/raw/givingtuesday_990/datamarts/raw/`

This directory contains the downloaded GT raw CSV files, including the Combined datamart and the three Basic form files.

#### GT metadata

- `01_data/raw/givingtuesday_990/datamarts/metadata/`

This directory contains:

- `datamart_catalog_raw.json`
- `datamart_catalog.csv`
- `datamart_catalog.md`
- `datamart_fields.csv`
- `datamart_fields.md`
- `required_datasets_manifest.csv`
- `download_state_manifest.json`
- `raw_bronze_upload_state.json`
- `raw_size_verification_state.json`
- `size_verification_report.csv`
- `curated_output_size_verification_report.csv`
- `gt_pipeline_artifact_manifest.json`
- `gt_pipeline_artifact_manifest.csv`
- `run_990_datamart_pipeline_summary.json`
- `run_990_datamart_pipeline_step_timings.csv`
- `schema_snapshot_basic_allforms_filtered.json`
- `schema_snapshot_filings_filtered.json`
- `givingtuesday_990_basic_allforms_analysis_variable_coverage.csv`
- audit CSVs such as the raw-vs-built Basic comparison outputs

#### GT raw cache

- `01_data/raw/givingtuesday_990/datamarts/cache/`

This contains the normalized Combined parquet cache and cache manifest:

- `givingtuesday_990_combined_forms_normalized.parquet`
- `givingtuesday_990_combined_forms_normalized_manifest.json`

#### GT staging outputs

- `01_data/staging/filing/`

This contains the main local GT parquet outputs:

- `givingtuesday_990_basic_allforms_presilver.parquet`
- `givingtuesday_990_basic_plus_combined_presilver.parquet`
- `givingtuesday_990_basic_allforms_benchmark.parquet`
- `givingtuesday_990_filings_benchmark.parquet`
- `givingtuesday_990_basic_allforms_analysis_variables.parquet`
- `givingtuesday_990_basic_allforms_analysis_region_metrics.parquet`
- step-specific manifest JSON files

#### GT analysis documentation

- `docs/analysis/`

This contains:

- `givingtuesday_basic_analysis_variable_mapping.md`

### Important local file semantics

The staged GT combine inputs are now named honestly and follow the filtered-only combine rule:

- `givingtuesday_990_basic_allforms_presilver.parquet` is already admitted in step 06
- `givingtuesday_990_basic_plus_combined_presilver.parquet` is already ROI-scoped before final Silver publication

### Reference inputs outside the GT folder

The GT benchmark geography filter depends on these reference files:

- `01_data/reference/GEOID_reference.csv`
- `01_data/reference/zip_to_county_fips.csv`

The GT step-13 NTEE enrichment depends on:

- staged NCCS BMF exact-year lookup outputs under `01_data/staging/nccs_bmf/`
- raw IRS EO BMF files under `01_data/raw/irs_bmf/`

### S3 layout

The default bucket is:

- `swb-321-irs990-teos`

The current GT prefixes are:

- Bronze raw data:
  - `bronze/givingtuesday_990/datamarts/raw/`
- Bronze raw metadata:
  - `bronze/givingtuesday_990/datamarts/metadata/`
- Bronze pre-Silver parquet outputs:
  - `bronze/givingtuesday_990/datamarts/presilver/`
- Silver filtered outputs:
  - `silver/givingtuesday_990/filing/`
- Silver filtered metadata:
  - `silver/givingtuesday_990/filing/metadata/`
- Silver analysis outputs:
  - `silver/givingtuesday_990/analysis/`
- Silver analysis metadata:
  - `silver/givingtuesday_990/analysis/metadata/`

### Artifact groups by pipeline phase

The GT code explicitly organizes artifacts into groups:

- Bronze raw metadata allowlist
  - raw catalog JSON
  - catalog CSV
  - catalog Markdown
  - field dictionary CSV
  - field dictionary Markdown
  - required dataset manifest
- Bronze pre-Silver artifacts
  - `givingtuesday_990_basic_allforms_presilver.parquet`
  - `givingtuesday_990_basic_plus_combined_presilver.parquet`
- Silver filtered artifacts
  - benchmark-filtered basic parquet
  - benchmark-filtered mixed parquet
  - one manifest per filtered artifact
  - one schema snapshot per filtered artifact
- Pipeline-wide metadata artifacts
  - curated size verification report
  - GT pipeline artifact manifest JSON
  - GT pipeline artifact manifest CSV

## End-To-End Transformation Narrative

This section explains the full GT data lineage from raw GT source discovery to final GT analysis variables.

### 1. Catalog fetch and flatten

#### Raw API retrieval

Step 01 fetches the GT catalog documents from the GT public API and writes them verbatim as raw metadata.

No row filtering or dataset selection happens here.

#### Catalog flattening

Step 02 transforms the nested GT catalog documents into a flattened row model suitable for scripting, review, and downstream selection logic.

This step enriches each row with:

- GT dataset title
- GT form type
- download URL
- dataset documentation link
- part/category metadata
- URL status probe results
- source content length in bytes

#### Remote field dictionary extraction

Step 02 then reads header rows from the GT dataset URLs and builds a field dictionary. This dictionary is a metadata view of the GT datamart schemas, not a local materialized union of the full data bodies.

### 2. Raw download and strict size validation

#### Required dataset resolution

Step 03 reads the flattened catalog and selects only:

- one Combined datamart file
- three Basic datamart files

#### Raw file download

Step 03 downloads the GT raw files in parallel and stores them under:

- `01_data/raw/givingtuesday_990/datamarts/raw/`

The local file names come from the GT download URLs.

#### Strict size matching

Step 03 validates that each downloaded local file matches the GT source byte count from the catalog metadata. If the file size does not match, step 03 raises an error.

#### Combined parquet cache

The GT Combined raw CSV is large enough that reparsing it on every later rebuild is too expensive. Step 03 therefore builds a cached normalized parquet mirror.

This is an upstream performance optimization. It does not replace the raw GT CSV as the source of truth. The raw CSV remains the canonical downloaded source file.

### 3. Bronze raw upload and raw verification

#### Step 04 upload behavior

Step 04 uploads:

- the GT raw CSV files to Bronze raw S3
- a constrained allowlist of raw-phase metadata files to Bronze metadata S3

This allowlist exists to avoid sweeping unrelated metadata into the Bronze raw metadata prefix.

#### Step 05 strict raw parity verification

Step 05 verifies, for each required GT dataset:

- source bytes from GT
- local bytes after download
- Bronze S3 bytes after upload

All three values must match. If they do not, step 05 fails.

This step writes:

- `size_verification_report.csv`

and uploads that report to Bronze metadata as part of the verification flow.

### 4. Step 06 Basic allforms build

The script:

- `python/ingest/990_givingtuesday/datamart/06_build_basic_allforms_presilver.py`

builds:

- `givingtuesday_990_basic_allforms_presilver.parquet`

from the three GT Basic raw files:

- Basic Fields 990
- Basic Fields 990EZ
- Basic Fields 990PF

#### What step 06 does

Step 06:

1. identifies the three GT Basic raw source files
2. builds ROI-admitted Combined keys first from the normalized Combined cache
3. reads the Basic headers and aligns the datasets to a shared column universe
4. casts columns to stable text types
5. creates normalized key fields
6. applies low-risk normalization to overlapping GT text fields
7. keeps only tax-year-in-scope rows admitted by Basic geography or by a Combined ROI key
8. writes explicit admission metadata and then dedupes to one best filing per key

#### Key normalization in step 06

Step 06 normalizes:

- `ein`
  - derived from `FILEREIN`
  - non-digits removed
  - left-padded to 9 digits when needed
- `tax_year`
  - normalized to integer-like string form
- `returntype_norm`
  - uppercase
  - spaces removed
  - hyphens removed
- `form_type`
  - inferred from normalized return type

#### Low-risk normalization in step 06

Step 06 applies low-risk canonicalization to overlapping GT fields:

- name-like fields
  - trimmed
  - internal whitespace collapsed
  - uppercased
- state fields
  - uppercased
- ZIP codes
  - normalized to ZIP5
- phone numbers
  - digits-only
- date fields
  - normalized to `YYYY-MM-DD` when parseable

These are deliberately low-risk normalization changes. They are meant to remove formatting noise, not overwrite substantive GT values.

### 5. Step 07 mixed ROI-scoped build

The script:

- `python/ingest/990_givingtuesday/datamart/07_build_basic_plus_combined_presilver.py`

builds:

- `givingtuesday_990_basic_plus_combined_presilver.parquet`

This output is an ROI-scoped mixed pre-Silver artifact built only from admitted inputs.

#### Why step 07 exists

The GT Combined datamart and the GT Basic datamarts each have strengths:

- Combined provides the mixed GT datamart view.
- Basic provides values that can fill blanks in Combined and also provides rows that may not appear in Combined.

Step 07 creates the mixed GT pre-Silver dataset by combining the two, but only for rows that are already admitted and relevant to the benchmark ROI.

#### ROI map construction

Step 07 reads:

- `01_data/reference/GEOID_reference.csv`
- `01_data/reference/zip_to_county_fips.csv`

and builds a benchmark ROI ZIP map by:

1. normalizing the benchmark county reference
2. normalizing the ZIP-to-county crosswalk
3. restricting the ZIP crosswalk to benchmark counties only
4. rejecting ambiguous ROI ZIP assignments

This ROI map is the basis for early geographic restriction.

#### Early ROI restriction before the heavy merge

This is one of the most important current pipeline behaviors.

Older logic merged the full GT data first and filtered later. The current logic does the opposite:

1. step 06 admits Basic rows upstream
2. step 07 builds ROI-admitted key candidates from the admitted Basic stage and the Combined geography stage
3. the heavy merge runs only on those ROI-eligible keys

This dramatically reduces the size of the expensive mixed merge.

#### Combined as the base

Step 07 uses the GT Combined file as the base row set for the mixed artifact.

It reads from the normalized Combined parquet cache built in step 03, not directly from the raw Combined CSV every time.

#### Tax-year restriction in step 07

Step 07 currently applies:

- `--tax-year-min 2022`
- `--tax-year-max 2024`

by default.

This means the mixed pre-Silver build itself is now scoped to GT benchmark years `2022-2024`.

#### ROI key union

Step 07 builds the set of candidate mixed merge keys from the union of:

- keys already admitted in the Basic pre-Silver stage
- keys admitted by Combined geography

This is important because it allows Basic to rescue ROI-eligible rows that the Combined geography alone might miss.

#### Deterministic filing dedupe

When GT Basic has duplicate rows for the same:

- `ein`
- `tax_year`
- `returntype_norm`

the pipeline now resolves them upstream and deterministically rather than carrying all return versions forward.

The ranking rule prefers, in order:

- amended returns (`AMENDERETURN`)
- final returns (`FINALRRETURN`)
- initial returns (`INITIARETURN`)
- then higher completeness and selected priority fields

Step 07 keeps the same filing-preference ranking as a safety net before merge/fill, and now applies it to the ROI-scoped Combined base rows as well. The intended primary dedupe point is still the upstream Basic all-forms build, but the mixed build also keeps one best filing row per `ein + tax_year + returntype_norm`.

#### Basic-to-Combined fill behavior

For overlapping fields, step 07 fills Combined from Basic in a controlled way. This is intended to preserve the Combined row base while using Basic as a source of additional nonblank values.

The merge also retains:

- unmatched Basic rows for ROI-admitted keys
- Basic-only columns needed in the mixed build

#### ROI admission metadata

Step 07 writes explicit metadata columns that explain why a mixed row is in the ROI-scoped pre-Silver file:

- `roi_admitted_by_basic`
- `roi_admitted_by_combined`
- `roi_admission_basis`
- `roi_geography_override_from_basic`
- `roi_geography_override_field_count_from_basic`

These columns make the ROI admission basis auditable before step 08 removes them from the final mixed Silver parquet.

#### DuckDB role in step 07

Step 07 is the one GT step that uses DuckDB for the large mixed merge.

DuckDB is used because this stage needs:

- large local-file scanning
- SQL-style joins
- deterministic dedupe logic
- spill-to-disk support for large intermediate work
- stable parquet writing for the mixed artifact

This stage also exposes DuckDB tuning parameters such as:

- thread count
- memory limit
- temp directory
- max temp directory size

The current default is conservative and reliability-oriented.

### 6. Step 08 benchmark filtering

The script:

- `python/ingest/990_givingtuesday/datamart/08_filter_benchmark_outputs_local.py`

writes the canonical benchmark-filtered GT outputs:

- `givingtuesday_990_basic_allforms_benchmark.parquet`
- `givingtuesday_990_filings_benchmark.parquet`

#### Step 08 inputs

Step 08 reads:

- the step-06 Basic allforms parquet
- the step-07 ROI-scoped mixed pre-Silver parquet
- the benchmark GEOID reference
- the ZIP-to-county crosswalk

#### Current filtering rules

Step 08 applies:

- tax-year filtering to `2022-2024`
- ZIP normalization to ZIP5
- ROI ZIP join against the benchmark ROI ZIP map
- county and region attachment from the benchmark geography map

#### Geography behavior

Both filtered outputs end up with benchmark geography fields such as:

- `zip5`
- `county_fips`
- `region`

#### Derived income calculation

Step 08 derives GT income as:

- revenue minus expenses

using form-aware revenue and expense logic:

- non-`990EZ` rows use:
  - `TOTREVCURYEA`
  - `TOTEXPCURYEA`
- `990EZ` rows use:
  - `TOTALRREVENU`
  - `TOTALEEXPENS`

Step 08 writes:

- `derived_income_amount`
- `derived_income_derivation`

#### Mixed Silver cleanup

The mixed input to step 08 already contains internal ROI metadata from step 07. Step 08 explicitly drops those internal ROI columns from the canonical mixed Silver output so the public mixed Silver artifact keeps a stable downstream schema.

### 7. Steps 09-11 publish and verify curated outputs

#### Step 09: Bronze pre-Silver upload

Step 09 uploads the GT pre-Silver parquet artifacts to Bronze:

- `givingtuesday_990_basic_allforms_presilver.parquet`
- `givingtuesday_990_basic_plus_combined_presilver.parquet`

Both files are already filtered/admitted before Bronze publication.

#### Step 10: Silver filtered upload

Step 10 uploads:

- filtered GT parquet outputs
- filtered manifest JSON files
- filtered schema snapshot JSON files

to the Silver prefixes.

It supports:

- `--emit basic`
- `--emit mixed`
- `--emit both`

#### Step 11: curated verification

Step 11 verifies local/S3 size parity for all curated GT artifacts:

- Bronze pre-Silver parquet outputs
- Silver filtered parquet outputs
- Silver manifests
- Silver schema snapshots

It writes:

- `curated_output_size_verification_report.csv`
- `gt_pipeline_artifact_manifest.json`
- `gt_pipeline_artifact_manifest.csv`

It also records build manifest metadata such as:

- `rows_output`
- `columns_output`
- `input_signature`

#### Orchestrator summary and timing outputs

The orchestrator writes:

- `run_990_datamart_pipeline_summary.json`
- `run_990_datamart_pipeline_step_timings.csv`

These summarize:

- requested step range
- run options
- per-step elapsed time
- manifest summaries for the main build/filter stages

### 8. Step 13 final GT analysis extraction

The script:

- `python/ingest/990_givingtuesday/datamart/13_extract_basic_analysis_variables_local.py`

creates the final GT analysis dataset:

- `givingtuesday_990_basic_allforms_analysis_variables.parquet`
- `givingtuesday_990_basic_allforms_analysis_region_metrics.parquet`

and companion documentation:

- `givingtuesday_990_basic_allforms_analysis_variable_coverage.csv`
- `docs/analysis/givingtuesday_basic_analysis_variable_mapping.md`

These files are written locally by step 13 and then uploaded by step 14 to:

- `silver/givingtuesday_990/analysis/`
- `silver/givingtuesday_990/analysis/metadata/`

#### Step 13 source scope

Step 13 intentionally reads the GT basic-only filtered benchmark parquet:

- `givingtuesday_990_basic_allforms_benchmark.parquet`

It does not use the GT mixed filtered Silver file as the base analysis dataset.

#### Form-aware revenue and expense mapping

Step 13 extracts:

- `analysis_total_revenue_amount`
- `analysis_total_expense_amount`

using form-aware rules:

- `990`
  - revenue from `TOTREVCURYEA`
  - expense from `TOTEXPCURYEA`
- `990PF`
  - revenue from `ANREEXTOREEX`
  - expense from `ARETEREXPNSS`
- `990EZ`
  - revenue from `TOTALRREVENU`
  - expense from `TOTALEEXPENS`

#### Program service revenue

Step 13 extracts:

- `analysis_program_service_revenue_amount`

using:

- `TOTPROSERREV` for `990`
- `PROGSERVREVE` for `990EZ`
- null for `990PF`

#### Direct-source contribution and grant components

Step 13 carries GT source component fields such as:

- `analysis_cash_contributions_amount`
- `analysis_noncash_contributions_amount`
- `analysis_other_contributions_amount`
- `analysis_foundation_grants_amount`
- `analysis_government_grants_amount`
- `analysis_other_grant_component_amount`

#### Calculated totals

Step 13 calculates:

- `analysis_calculated_total_contributions_amount`
  - `TOTACASHCONT + NONCASCONTRI + ALLOOTHECONT`
- `analysis_calculated_grants_total_amount`
  - `FOREGRANTOTA + GOVERNGRANTS + GRANTOORORGA`
- `analysis_calculated_grants_share_of_revenue_ratio`
  - `analysis_calculated_grants_total_amount / analysis_total_revenue_amount`
  - null when total revenue is missing, zero, or negative
- `analysis_calculated_surplus_amount`
  - calculated in step 13 as `analysis_total_revenue_amount - analysis_total_expense_amount`
  - this keeps the surplus calculation aligned with the PF-specific revenue and expense mapping
- `analysis_calculated_net_margin_ratio`
  - `surplus / total_revenue`
- `analysis_calculated_net_asset_amount`
  - GT proxy using assets minus liabilities where support exists
- `analysis_calculated_months_of_reserves`
  - `net_asset / total_expense * 12`

Step 13 also writes the region-level analysis parquet:

- `givingtuesday_990_basic_allforms_analysis_region_metrics.parquet`

This region metrics parquet includes both:

- `analysis_calculated_region_grants_share_of_revenue_ratio`
  - raw region-year aggregate grants divided by raw region-year aggregate revenue
- `analysis_calculated_cleaned_region_grants_share_of_revenue_ratio`
  - cleaned region-year aggregate grants divided by cleaned region-year aggregate revenue using only rows with positive revenue and nonmissing grants totals

It also includes both normalization denominator choices:

- filing-count normalization
- unique-EIN normalization

The cleaned region metric is intentionally stricter than the raw aggregate metric and can differ from it or remain null.

#### Null and blank behavior for calculated totals

For the component-sum metrics:

- blank components are treated as missing values, not as parse failures
- the derived total stays null only when all source components are blank

#### NCCS BMF NTEE enrichment

GT does not carry the needed NTEE classification directly in the GT raw source files used by this pipeline. Step 13 therefore performs an explicit, documented enrichment exception using NCCS BMF and IRS EO BMF support files.

The NTEE fill order is:

1. NCCS BMF exact-year lookup by `ein + tax_year`
2. NCCS BMF nearest-year fallback by `ein`
3. IRS EO BMF fallback by `ein`

Step 13 writes:

- `analysis_ntee_code`
- `analysis_ntee_code_source_family`
- `analysis_ntee_code_source_variant`
- `analysis_ntee_code_source_column`
- `analysis_calculated_ntee_broad_code`
- `analysis_calculated_ntee_broad_code_source_column`

The broad NTEE category is derived as the first letter of the exact NTEE code when present.

#### Source-backed boolean proxy columns

Step 13 currently derives these source-backed classification columns from NTEE:

- `analysis_is_hospital`
- `analysis_is_university`
- `analysis_is_political_org`

These are NTEE-based proxy booleans, not GT raw boolean fields.

Their behavior is:

- blank NTEE stays missing
- recognized positive NTEE prefixes become `True`
- other nonblank NTEE codes become `False`

#### Fully populated imputed boolean columns

Step 13 also writes complete imputed versions:

- `analysis_imputed_is_hospital`
- `analysis_imputed_is_university`
- `analysis_imputed_is_political_org`

These use a staged fallback pattern:

- use the canonical source-backed `analysis_is_*` column when available
- use allowed higher-confidence fallback logic where configured
- otherwise default to `False`

Current imputation logic includes:

- hospital
  - primarily NTEE-based
  - remaining missing values default to `False`
- university
  - primarily NTEE-based
  - some higher-confidence organization-name rules are allowed
  - remaining missing values default to `False`
- political organization
  - primarily NTEE-based
  - subsection-based fallback is also used when NTEE is missing
  - limited higher-confidence organization-name rules are allowed
  - remaining missing values default to `False`

#### Direct-source vs calculated vs enriched vs imputed

The final GT analysis parquet intentionally mixes several kinds of variables. These must not be conflated.

- Direct-source
  - directly mapped from GT source columns
- Calculated
  - computed from GT source columns or upstream GT-derived fields
- Enriched
  - attached from non-GT support files such as NCCS BMF
- Imputed
  - filled to completeness using documented fallback/default logic

### 9. Final GT analysis outputs

The final GT analysis layer currently consists of:

- one representative row per GT basic-only filtered benchmark filing key (`ein`, `tax_year`, `form_type`) after the upstream amended/final filing ranking rule is applied
- core GT revenue/expense/contribution/grant variables
- calculated GT financial proxy variables
- NTEE classification enrichment
- source-backed proxy booleans
- fully populated imputed boolean columns
- provenance fields describing source family, source variant, and source columns

Step 13 also writes a second GT analysis-ready table for region-level normalization work:

- `givingtuesday_990_basic_allforms_analysis_region_metrics.parquet`

This region metrics parquet is derived from the final row-level GT analysis parquet and currently includes region/tax-year aggregates for:

- filing counts
- summed total revenue
- summed calculated grants total
- summed calculated net-asset proxy
- normalized total revenue per nonprofit filing
- normalized net-asset proxy per nonprofit filing
- region-level grants-share-of-revenue ratio
- both `all_rows` and `exclude_imputed_hospital_university_political_org` scopes

## Testing And Verification

### Main GT test target

The main GT test target is:

```bash
python -m pytest tests/990_givingtuesday/test_datamart_pipeline.py -q
```

### What the GT tests cover

The current GT test module covers the GT artifact contract and core GT pipeline behavior, including:

- GT artifact registry expectations
- emit-mode handling
- stale-output warnings
- benchmark filtering behavior
- ROI ZIP ambiguity rejection
- step-06 low-risk normalization behavior
- step-07 ROI-scoped mixed build behavior
- step-08 benchmark filtering and derived income behavior
- step-13 analysis extraction behavior
- NTEE enrichment behavior
- boolean proxy and imputation behavior
- orchestrator command construction and late-stage parallel behavior

### Specific test themes

#### Artifact contract tests

These verify that the GT artifact groups and file contracts are consistent with the current code.

#### ROI filter tests

These verify:

- only benchmark geography rows survive filtering
- county and region are attached correctly
- ambiguous benchmark ZIP assignments are rejected

#### Step-06 normalization tests

These verify the low-risk normalization rules such as:

- normalized names
- ZIP normalization
- phone normalization
- date normalization
- stable key generation

#### Step-13 extraction tests

These verify:

- form-aware revenue and expense mapping
- calculated contributions and grants totals
- NTEE enrichment columns
- broad NTEE derivation
- boolean proxy columns
- imputed boolean column completeness and source behavior

### Operational verification artifacts

The GT pipeline also produces non-pytest verification artifacts.

#### Raw verification

- `size_verification_report.csv`

This validates byte parity across:

- GT source URL content length
- local downloaded raw file
- Bronze raw S3 object

#### Curated verification

- `curated_output_size_verification_report.csv`

This validates local/S3 parity for the curated GT Bronze/Silver outputs.

#### Build manifests

Build manifests record:

- input signature
- build options
- output path
- row counts
- column counts

#### Pipeline artifact manifest

The GT pipeline artifact manifest records the curated artifact inventory and verification state across Bronze/Silver outputs.

#### Orchestrator summary

The orchestrator summary JSON and step timings CSV provide run-level observability for:

- step ordering
- options used
- total elapsed time
- per-step elapsed time
- summary references to the main build/filter manifests

## Current Rules, Constraints, And Caveats

### Current rules

- GT benchmark scope is currently `2022-2024`.
- Benchmark ROI is derived from benchmark counties using the ZIP crosswalk.
- The mixed GT pre-Silver build is ROI-scoped before the heavy merge.
- Step 13 is the final GT analysis extract step.
- Step 14 uploads the official GT analysis outputs and metadata to the dedicated GT analysis S3 prefix.
- Direct-source, calculated, enriched, and imputed fields must be treated as different categories.

### Important caveats

- The step script filenames still retain their historical numbering/order names, but the staged artifact names now reflect current pre-Silver semantics.
- NTEE is not coming from GT raw files in this pipeline.
  - It is enriched in step 13 from NCCS BMF with fallback support.
- Source-backed boolean proxy columns can remain missing when NTEE is missing.
- Imputed boolean columns are complete because the final fallback is default `False`.
- GT-calculated net asset and months-of-reserves fields are proxies, not authoritative GT raw financial statements.
- Step 12 is a useful audit script but not part of the standard orchestrated `01-11` flow.

## Appendices

### Appendix A: Artifact Inventory By Phase

#### Retrieval phase

- `datamart_catalog_raw.json`
- `datamart_catalog.csv`
- `datamart_catalog.md`
- `datamart_fields.csv`
- `datamart_fields.md`
- `required_datasets_manifest.csv`
- `download_state_manifest.json`

#### Raw cache phase

- `givingtuesday_990_combined_forms_normalized.parquet`
- `givingtuesday_990_combined_forms_normalized_manifest.json`

#### Raw verification phase

- `size_verification_report.csv`
- `raw_bronze_upload_state.json`
- `raw_size_verification_state.json`

#### Build phase

- `givingtuesday_990_basic_allforms_presilver.parquet`
- `manifest_basic_allforms_presilver.json`
- `givingtuesday_990_basic_plus_combined_presilver.parquet`
- `manifest_basic_plus_combined_presilver.json`

#### Filter phase

- `givingtuesday_990_basic_allforms_benchmark.parquet`
- `manifest_basic_allforms_filtered.json`
- `schema_snapshot_basic_allforms_filtered.json`
- `givingtuesday_990_filings_benchmark.parquet`
- `manifest_filtered.json`
- `schema_snapshot_filings_filtered.json`

#### Curated verification phase

- `curated_output_size_verification_report.csv`
- `gt_pipeline_artifact_manifest.json`
- `gt_pipeline_artifact_manifest.csv`
- `run_990_datamart_pipeline_summary.json`
- `run_990_datamart_pipeline_step_timings.csv`

#### Analysis phase

- `givingtuesday_990_basic_allforms_analysis_variables.parquet`
- `givingtuesday_990_basic_allforms_analysis_region_metrics.parquet`
- `givingtuesday_990_basic_allforms_analysis_variable_coverage.csv`
- `docs/analysis/givingtuesday_basic_analysis_variable_mapping.md`

#### Analysis publish phase

- step-14 upload to `silver/givingtuesday_990/analysis/`
- step-14 upload to `silver/givingtuesday_990/analysis/metadata/`

### Appendix B: Step-by-Step Input/Output Matrix


| Step | Inputs                                                                                  | Outputs                                                                             |
| ---- | --------------------------------------------------------------------------------------- | ----------------------------------------------------------------------------------- |
| 01   | GT catalog API                                                                          | raw catalog JSON                                                                    |
| 02   | raw catalog JSON or refreshed catalog API                                               | flattened catalog CSV/MD, field dictionary CSV/MD                                   |
| 03   | catalog CSV, GT download URLs                                                           | raw GT CSVs, required dataset manifest, download-state JSON, Combined cache parquet |
| 04   | raw GT CSVs, raw-phase metadata allowlist                                               | Bronze raw S3 objects, Bronze metadata S3 objects                                   |
| 05   | catalog CSV, local raw GT files, Bronze raw S3 objects                                  | raw size verification report                                                        |
| 06   | GT Basic raw CSVs, Combined cache parquet, benchmark geography reference files          | admitted Basic pre-Silver parquet, build manifest                                   |
| 07   | Basic pre-Silver parquet, Combined cache parquet, benchmark geography reference files   | ROI-scoped mixed pre-Silver parquet, build manifest                                 |
| 08   | step-06 and step-07 outputs, benchmark geography reference files                        | filtered basic parquet, filtered mixed parquet, manifests, schema snapshots         |
| 09   | step-06 and step-07 parquet outputs                                                     | Bronze pre-Silver S3 objects                                                        |
| 10   | step-08 outputs, manifests, schema snapshots                                            | Silver GT parquet and metadata S3 objects                                           |
| 11   | curated GT local artifacts and S3 objects                                               | curated verification report, pipeline artifact manifests                            |
| 13   | filtered GT basic benchmark parquet, staged NCCS BMF lookup files, raw IRS EO BMF files | GT analysis parquet, GT region metrics parquet, coverage CSV, mapping Markdown      |
| 14   | step-13 local analysis outputs                                                          | GT analysis S3 objects under analysis and analysis/metadata prefixes                |


### Appendix C: Final Analysis-Variable Lineage Summary

The authoritative step-13 variable-lineage specification is:

- `docs/analysis/givingtuesday_basic_analysis_variable_mapping.md`

That document should be read as the field-level companion to this pipeline document. In summary:

- GT revenue and expense fields are form-aware direct-source mappings.
- GT contribution and grant totals are calculated from GT component fields.
- GT surplus, net margin, net asset proxy, and months-of-reserves proxy are calculated fields.
- GT NTEE variables are enriched from NCCS BMF and fallback support files.
- GT hospital, university, and political-organization source-backed proxy columns are NTEE-derived.
- GT imputed boolean columns are complete analysis convenience fields with documented fallback/default logic.

### Appendix D: Final GT Analysis Dataset Variable Dictionary

The final GT analysis parquet currently contains the following columns.

#### D1. Filing identity and benchmark geography columns


| Column         | Meaning                                                            | Source or calculation                                                                                                                   |
| -------------- | ------------------------------------------------------------------ | --------------------------------------------------------------------------------------------------------------------------------------- |
| `ein`          | Normalized employer identification number for the filing entity.   | Carried from the filtered GT basic benchmark source; originally normalized from GT EIN fields earlier in the pipeline.                  |
| `tax_year`     | Filing tax year as a normalized year value.                        | Carried from the filtered GT basic benchmark source; originally normalized earlier in the pipeline.                                     |
| `form_type`    | Canonical GT form type for the filing (`990`, `990EZ`, `990PF`).   | Carried from the filtered GT basic benchmark source.                                                                                    |
| `FILERNAME1`   | Primary GT organization name text.                                 | Direct GT source field from the filtered GT basic benchmark parquet.                                                                    |
| `FILERUSSTATE` | Filing organization state code.                                    | Direct GT source field from the filtered GT basic benchmark parquet after earlier low-risk state normalization.                         |
| `zip5`         | Benchmark filtering ZIP code in canonical ZIP5 form.               | Derived in step 08 from the best GT ZIP field using ZIP normalization.                                                                  |
| `county_fips`  | Benchmark county FIPS code used for region-of-interest assignment. | Attached in step 08 by joining the filing ZIP to the benchmark ROI ZIP map built from the ZIP crosswalk and benchmark county reference. |
| `region`       | Benchmark region/cluster name for the filing.                      | Attached in step 08 from the benchmark GEOID reference after the ROI ZIP join.                                                          |


#### D2. NTEE and subsection enrichment columns


| Column                                              | Meaning                                                                                | Source or calculation                                                                                                                               |
| --------------------------------------------------- | -------------------------------------------------------------------------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------- |
| `analysis_ntee_code`                                | Final resolved NTEE classification code for the GT filing.                             | Enriched in step 13 using this fallback order: NCCS BMF exact-year `ein + tax_year`, then NCCS BMF nearest-year by `ein`, then IRS EO BMF by `ein`. |
| `analysis_ntee_code_source_family`                  | Source family that provided `analysis_ntee_code`.                                      | Written in step 13; indicates whether the winning NTEE value came from NCCS BMF or IRS EO BMF.                                                      |
| `analysis_ntee_code_source_variant`                 | More specific source variant/year context for `analysis_ntee_code`.                    | Written in step 13; identifies the exact staged lookup variant that supplied the final NTEE value.                                                  |
| `analysis_ntee_code_source_column`                  | Exact upstream source column that supplied `analysis_ntee_code`.                       | Written in step 13; for example the resolved BMF NTEE source column such as `NTEE_CD` or legacy `NTEEFINAL`.                                        |
| `analysis_subsection_code`                          | Final resolved subsection classification used to support political-organization logic. | Enriched in step 13 using the same exact-year, nearest-year, and IRS fallback pattern used for NTEE.                                                |
| `analysis_subsection_code_source_family`            | Source family that provided `analysis_subsection_code`.                                | Written in step 13; indicates whether the winning subsection value came from NCCS BMF or IRS EO BMF.                                                |
| `analysis_subsection_code_source_variant`           | More specific source variant/year context for `analysis_subsection_code`.              | Written in step 13; identifies which lookup variant supplied the final subsection code.                                                             |
| `analysis_subsection_code_source_column`            | Exact upstream source column that supplied `analysis_subsection_code`.                 | Written in step 13; for example `SUBSECTION` or `SUBSECCD`.                                                                                         |
| `analysis_calculated_ntee_broad_code`               | Broad NTEE field grouping represented by the first letter of the exact NTEE code.      | Calculated in step 13 as the first letter of `analysis_ntee_code` when `analysis_ntee_code` is nonblank.                                            |
| `analysis_calculated_ntee_broad_code_source_column` | Provenance column for `analysis_calculated_ntee_broad_code`.                           | Written in step 13; records that the broad code was derived from `analysis_ntee_code`.                                                              |


#### D3. Source-backed and imputed classification flags


| Column                                            | Meaning                                                         | Source or calculation                                                                                                                                                                                                                                             |
| ------------------------------------------------- | --------------------------------------------------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `analysis_is_hospital`                            | Source-backed hospital proxy flag for the filing.               | Calculated in step 13 from `analysis_ntee_code`; `True` when the resolved NTEE code begins with `E20`, `E21`, `E22`, or `E24`, `False` for other populated NTEE values, and missing when NTEE is missing.                                                         |
| `analysis_is_hospital_source_column`              | Provenance column for `analysis_is_hospital`.                   | Written in step 13; records that the source-backed flag came from `analysis_ntee_code`.                                                                                                                                                                           |
| `analysis_is_university`                          | Source-backed university proxy flag for the filing.             | Calculated in step 13 from `analysis_ntee_code`; `True` when the resolved NTEE code begins with `B40`, `B41`, `B42`, `B43`, or `B50`, `False` for other populated NTEE values, and missing when NTEE is missing.                                                  |
| `analysis_is_university_source_column`            | Provenance column for `analysis_is_university`.                 | Written in step 13; records that the source-backed flag came from `analysis_ntee_code`.                                                                                                                                                                           |
| `analysis_is_political_org`                       | Source-backed political-organization proxy flag for the filing. | Calculated in step 13 from `analysis_ntee_code`; `True` when the resolved NTEE code begins with `R40` or `W24`, `False` for other populated NTEE values, and missing when NTEE is missing.                                                                        |
| `analysis_is_political_org_source_column`         | Provenance column for `analysis_is_political_org`.              | Written in step 13; records that the source-backed flag came from `analysis_ntee_code`.                                                                                                                                                                           |
| `analysis_imputed_is_hospital`                    | Fully populated hospital analysis flag.                         | Calculated in step 13 as `coalesce(analysis_is_hospital, high-confidence organization-name rule, False)`. Current high-confidence hospital name rules are conservative; remaining unresolved rows default to `False`.                                             |
| `analysis_imputed_is_hospital_source_column`      | Provenance column for `analysis_imputed_is_hospital`.           | Written in step 13; records whether the filled value came from `analysis_ntee_code`, a high-confidence `FILERNAME1` rule, or `imputed_default_false`.                                                                                                             |
| `analysis_imputed_is_university`                  | Fully populated university analysis flag.                       | Calculated in step 13 as `coalesce(analysis_is_university, high-confidence organization-name rule, False)`. High-confidence university name rules include strong terms such as `university`, `college`, `collegiate`, and selected curated abbreviation patterns. |
| `analysis_imputed_is_university_source_column`    | Provenance column for `analysis_imputed_is_university`.         | Written in step 13; records whether the filled value came from `analysis_ntee_code`, a high-confidence `FILERNAME1` rule, or `imputed_default_false`.                                                                                                             |
| `analysis_imputed_is_political_org`               | Fully populated political-organization analysis flag.           | Calculated in step 13 as `coalesce(analysis_is_political_org, subsection-based political-org rule, high-confidence organization-name rule, False)`. The subsection fallback is attempted before any name-based fallback.                                          |
| `analysis_imputed_is_political_org_source_column` | Provenance column for `analysis_imputed_is_political_org`.      | Written in step 13; records whether the filled value came from `analysis_ntee_code`, subsection fallback, `FILERNAME1`, or `imputed_default_false`.                                                                                                               |


#### D4. Revenue, expense, contribution, and grant amount columns


| Column                                                            | Meaning                                                                    | Source or calculation                                                                                                                                                                                                          |
| ----------------------------------------------------------------- | -------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| `analysis_total_revenue_amount`                                   | Filing total revenue amount used for GT analysis.                          | Direct-source, form-aware mapping in step 13: `TOTREVCURYEA` for `990`, `TOTALRREVENU` for `990EZ`, and PF-specific `ANREEXTOREEX` for `990PF`.                                                                                 |
| `analysis_total_revenue_amount_source_column`                     | Provenance column for `analysis_total_revenue_amount`.                     | Written in step 13; records the exact GT source field used for that row: `TOTREVCURYEA`, `TOTALRREVENU`, or `ANREEXTOREEX`.                                                                                                    |
| `analysis_total_expense_amount`                                   | Filing total expense amount used for GT analysis.                          | Direct-source, form-aware mapping in step 13: `TOTEXPCURYEA` for `990`, `TOTALEEXPENS` for `990EZ`, and PF-specific `ARETEREXPNSS` for `990PF`.                                                                                 |
| `analysis_total_expense_amount_source_column`                     | Provenance column for `analysis_total_expense_amount`.                     | Written in step 13; records the exact GT source field used for that row: `TOTEXPCURYEA`, `TOTALEEXPENS`, or `ARETEREXPNSS`.                                                                                                    |
| `analysis_program_service_revenue_amount`                         | Filing program service revenue amount.                                     | Direct-source, form-aware mapping in step 13: `TOTPROSERREV` for `990`, `PROGSERVREVE` for `990EZ`, and null for `990PF`.                                                                                                      |
| `analysis_program_service_revenue_amount_source_column`           | Provenance column for `analysis_program_service_revenue_amount`.           | Written in step 13; records whether the row used `TOTPROSERREV`, `PROGSERVREVE`, or remained blank.                                                                                                                            |
| `analysis_calculated_surplus_amount`                              | Filing surplus amount used in the net-margin calculation.                  | Calculated in step 13 as `analysis_total_revenue_amount - analysis_total_expense_amount`, using the final form-aware revenue and expense mappings.                                                                              |
| `analysis_calculated_surplus_amount_source_column`                | Provenance column for `analysis_calculated_surplus_amount`.                | Written in step 13; records `derived:analysis_total_revenue_amount-minus-analysis_total_expense_amount` when the calculation is populated.                                                                                       |
| `analysis_cash_contributions_amount`                              | GT cash contributions component.                                           | Direct GT source field `TOTACASHCONT`.                                                                                                                                                                                         |
| `analysis_cash_contributions_amount_source_column`                | Provenance column for `analysis_cash_contributions_amount`.                | Written in step 13; records `TOTACASHCONT` when populated.                                                                                                                                                                     |
| `analysis_noncash_contributions_amount`                           | GT noncash contributions component.                                        | Direct GT source field `NONCASCONTRI`.                                                                                                                                                                                         |
| `analysis_noncash_contributions_amount_source_column`             | Provenance column for `analysis_noncash_contributions_amount`.             | Written in step 13; records `NONCASCONTRI` when populated.                                                                                                                                                                     |
| `analysis_other_contributions_amount`                             | GT other contributions component.                                          | Direct GT source field `ALLOOTHECONT`.                                                                                                                                                                                         |
| `analysis_other_contributions_amount_source_column`               | Provenance column for `analysis_other_contributions_amount`.               | Written in step 13; records `ALLOOTHECONT` when populated.                                                                                                                                                                     |
| `analysis_foundation_grants_amount`                               | GT foundation-grants component.                                            | Direct GT source field `FOREGRANTOTA`.                                                                                                                                                                                         |
| `analysis_foundation_grants_amount_source_column`                 | Provenance column for `analysis_foundation_grants_amount`.                 | Written in step 13; records `FOREGRANTOTA` when populated.                                                                                                                                                                     |
| `analysis_government_grants_amount`                               | GT government-grants component.                                            | Direct GT source field `GOVERNGRANTS`.                                                                                                                                                                                         |
| `analysis_government_grants_amount_source_column`                 | Provenance column for `analysis_government_grants_amount`.                 | Written in step 13; records `GOVERNGRANTS` when populated.                                                                                                                                                                     |
| `analysis_other_grant_component_amount`                           | GT other-grant component.                                                  | Direct GT source field `GRANTOORORGA`.                                                                                                                                                                                         |
| `analysis_other_grant_component_amount_source_column`             | Provenance column for `analysis_other_grant_component_amount`.             | Written in step 13; records `GRANTOORORGA` when populated.                                                                                                                                                                     |
| `analysis_calculated_total_contributions_amount`                  | Calculated total contributions amount for the filing.                      | Calculated in step 13 as the row-wise sum of `TOTACASHCONT`, `NONCASCONTRI`, and `ALLOOTHECONT`. The result stays null only when all three components are blank.                                                               |
| `analysis_calculated_total_contributions_amount_source_column`    | Provenance column for `analysis_calculated_total_contributions_amount`.    | Written in step 13; records the derivation label `derived:TOTACASHCONT + NONCASCONTRI + ALLOOTHECONT`.                                                                                                                         |
| `analysis_calculated_grants_total_amount`                         | Calculated total grants amount for the filing.                             | Calculated in step 13 as the row-wise sum of `FOREGRANTOTA`, `GOVERNGRANTS`, and `GRANTOORORGA`. The result stays null only when all three components are blank.                                                               |
| `analysis_calculated_grants_total_amount_source_column`           | Provenance column for `analysis_calculated_grants_total_amount`.           | Written in step 13; records the derivation label `derived:FOREGRANTOTA + GOVERNGRANTS + GRANTOORORGA`.                                                                                                                         |
| `analysis_calculated_grants_share_of_revenue_ratio`               | Filing-level grants dependency ratio.                                      | Calculated in step 13 as `analysis_calculated_grants_total_amount / analysis_total_revenue_amount`; null when total revenue is missing, zero, or negative.                                                                     |
| `analysis_calculated_grants_share_of_revenue_ratio_source_column` | Provenance column for `analysis_calculated_grants_share_of_revenue_ratio`. | Written in step 13; records that the ratio was derived from GT grants total and GT total revenue.                                                                                                                              |
| `analysis_grants_share_of_revenue_quality_flag`                   | Row-level usability flag for the filing-level grants-share ratio.          | Calculated in step 13 to classify each row as `valid_0_to_1`, `gt_1_source_anomaly`, `nonpositive_revenue`, or `missing_component` based on the availability of grants total, total revenue, and the resulting ratio behavior. |


#### D5. Financial-ratio and reserve proxy columns


| Column                                                 | Meaning                                                         | Source or calculation                                                                                                                                                                                                                                                                       |
| ------------------------------------------------------ | --------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `analysis_calculated_net_asset_amount`                 | GT-derived net-asset proxy used for analysis.                   | Calculated in step 13 as `TOASEOOYY - TOLIEOOYY` for `990` and the subset of `990EZ` rows where both assets and liabilities are present. `990PF` stays null because this GT source layer does not provide a matching liability field needed for a source-faithful GT net-asset calculation. |
| `analysis_calculated_net_asset_amount_source_column`   | Provenance column for `analysis_calculated_net_asset_amount`.   | Written in step 13; records the derivation label for the GT assets-minus-liabilities proxy.                                                                                                                                                                                                 |
| `analysis_calculated_months_of_reserves`               | GT-derived months-of-reserves proxy.                            | Calculated in step 13 as `(analysis_calculated_net_asset_amount / analysis_total_expense_amount) * 12` when net asset is available and total expense is nonzero.                                                                                                                            |
| `analysis_calculated_months_of_reserves_source_column` | Provenance column for `analysis_calculated_months_of_reserves`. | Written in step 13; records that the value was derived from the GT net-asset proxy and GT total expense.                                                                                                                                                                                    |
| `analysis_calculated_net_margin_ratio`                 | GT-derived net-margin ratio.                                    | Calculated in step 13 as `analysis_calculated_surplus_amount / analysis_total_revenue_amount`; null when total revenue is missing or zero.                                                                                                                                                  |
| `analysis_calculated_net_margin_ratio_source_column`   | Provenance column for `analysis_calculated_net_margin_ratio`.   | Written in step 13; records that the value was derived from GT surplus and GT total revenue.                                                                                                                                                                                                |


### Appendix E: Final GT Region Metrics Dataset Variable Dictionary

The region-level GT analysis parquet at `givingtuesday_990_basic_allforms_analysis_region_metrics.parquet` currently contains the following columns.


| Column                                                             | Meaning                                                                                            | Source or calculation                                                                                                                                                                                                          |
| ------------------------------------------------------------------ | -------------------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| `region`                                                           | Benchmark region/cluster for the aggregate row.                                                    | Derived from the final row-level GT analysis parquet by grouping on `region`.                                                                                                                                                  |
| `tax_year`                                                         | Tax year for the aggregate row.                                                                    | Derived from the final row-level GT analysis parquet by grouping on `tax_year`.                                                                                                                                                |
| `analysis_exclusion_variant`                                       | Scope variant used for the aggregate row.                                                          | Calculated in step 13 as either `all_rows` or `exclude_imputed_hospital_university_political_org`.                                                                                                                             |
| `nonprofit_filing_count`                                           | Number of GT filing rows included in the aggregate scope.                                          | Calculated in step 13 as the count of row-level GT analysis records in the region/year/scope group.                                                                                                                            |
| `unique_nonprofit_ein_count`                                       | Number of unique EINs included in the aggregate scope.                                             | Calculated in step 13 as the distinct count of nonblank `ein` values in the region/year/scope group.                                                                                                                          |
| `analysis_total_revenue_amount_sum`                                | Sum of row-level GT total revenue within the region/year/scope group.                              | Calculated in step 13 as the grouped sum of `analysis_total_revenue_amount` using pandas `sum(min_count=1)`.                                                                                                                   |
| `analysis_calculated_grants_total_amount_sum`                      | Sum of row-level GT grants totals within the region/year/scope group.                              | Calculated in step 13 as the grouped sum of `analysis_calculated_grants_total_amount` using pandas `sum(min_count=1)`.                                                                                                         |
| `analysis_calculated_net_asset_amount_sum`                         | Sum of row-level GT net-asset proxy amounts within the region/year/scope group.                    | Calculated in step 13 as the grouped sum of `analysis_calculated_net_asset_amount` using pandas `sum(min_count=1)`.                                                                                                            |
| `analysis_calculated_normalized_total_revenue_per_nonprofit`       | Region-year revenue normalized by filing count.                                                    | Calculated in step 13 as `analysis_total_revenue_amount_sum / nonprofit_filing_count` when the filing count is positive and grouped revenue is available.                                                                      |
| `analysis_calculated_normalized_net_asset_per_nonprofit`           | Region-year net-asset proxy normalized by filing count.                                            | Calculated in step 13 as `analysis_calculated_net_asset_amount_sum / nonprofit_filing_count` when the filing count is positive and grouped net-asset proxy is available.                                                       |
| `analysis_calculated_normalized_total_revenue_per_unique_nonprofit` | Region-year revenue normalized by unique EIN count.                                                | Calculated in step 13 as `analysis_total_revenue_amount_sum / unique_nonprofit_ein_count` when the unique EIN count is positive and grouped revenue is available.                                                               |
| `analysis_calculated_normalized_net_asset_per_unique_nonprofit`     | Region-year net-asset proxy normalized by unique EIN count.                                        | Calculated in step 13 as `analysis_calculated_net_asset_amount_sum / unique_nonprofit_ein_count` when the unique EIN count is positive and grouped net-asset proxy is available.                                                |
| `analysis_calculated_region_grants_share_of_revenue_ratio`         | Raw region-year grants share of revenue.                                                           | Calculated in step 13 as `analysis_calculated_grants_total_amount_sum / analysis_total_revenue_amount_sum` when both grouped sums are available and grouped revenue is nonzero.                                                |
| `analysis_calculated_cleaned_region_grants_share_of_revenue_ratio` | Cleaned region-year grants share of revenue.                                                       | Calculated in step 13 using only rows with positive `analysis_total_revenue_amount` and nonmissing `analysis_calculated_grants_total_amount`, then dividing the cleaned grouped grants sum by the cleaned grouped revenue sum. |
| `analysis_grants_share_valid_row_count`                            | Count of row-level filings in the group with a grants-share quality flag of `valid_0_to_1`.        | Calculated in step 13 by counting grouped rows where `analysis_grants_share_of_revenue_quality_flag == 'valid_0_to_1'`.                                                                                                        |
| `analysis_grants_share_gt_1_row_count`                             | Count of row-level filings in the group with a grants-share quality flag of `gt_1_source_anomaly`. | Calculated in step 13 by counting grouped rows where `analysis_grants_share_of_revenue_quality_flag == 'gt_1_source_anomaly'`.                                                                                                 |
| `analysis_imputed_is_hospital_true_count`                          | Count of grouped filings flagged `True` on the complete imputed hospital flag.                     | Calculated in step 13 by summing `analysis_imputed_is_hospital` within the region/year/scope group.                                                                                                                            |
| `analysis_imputed_is_university_true_count`                        | Count of grouped filings flagged `True` on the complete imputed university flag.                   | Calculated in step 13 by summing `analysis_imputed_is_university` within the region/year/scope group.                                                                                                                          |
| `analysis_imputed_is_political_org_true_count`                     | Count of grouped filings flagged `True` on the complete imputed political-organization flag.       | Calculated in step 13 by summing `analysis_imputed_is_political_org` within the region/year/scope group.                                                                                                                       |


## Canonical Commands

### Run the full GT orchestrated pipeline

```bash
python python/ingest/990_givingtuesday/datamart/run_990_datamart_pipeline.py
```

### Run the final GT analysis extract

```bash
python python/ingest/990_givingtuesday/datamart/13_extract_basic_analysis_variables_local.py
```

### Run the GT tests

```bash
python -m pytest tests/990_givingtuesday/test_datamart_pipeline.py -q
```

## Final Notes

This document is meant to describe the GT pipeline as currently implemented, not as historically implemented and not as a generic nonprofit pipeline description.

When this pipeline changes, this document should be updated alongside the code in the same way that:

- output contracts
- build manifests
- schema snapshots
- analysis mapping rules

are kept current with the actual implemented behavior.
