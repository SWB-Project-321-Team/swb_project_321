# S3 Pipeline Documentation Audit

Audit date: 2026-05-01

Bucket audited: `s3://swb-321-irs990-teos/`

Supporting inventory: `docs/audit/s3_object_inventory_classification_2026-05-01.csv`

## Executive Summary

The S3-published Markdown documentation for the six requested families is extensive and largely consistent with the repo code and S3 artifact layout. I did not find a pipeline defect that makes the produced target datasets questionable based on this documentation/code/S3 review.

The main documentation corrections are:

1. The root S3 inventory docs are stale: current live inventory is 378 objects and 35,690,814,575 bytes, while `s3_bucket_tree.md` says 373 objects and `docs/s3_bucket_inventory.md` remains a 298-object historical snapshot.
2. GivingTuesday documents its exact public API URL. The IRS/NCCS pipeline Markdown docs describe release pages, catalogs, probing, and filenames, but they do not include the exact literal source URLs used by code. Those URLs are present as code constants and should be added to the published docs.
3. Some published docs contain machine-local absolute Windows paths from this workstation. They are accurate for the run, but should be paired with repo-relative or `SWB_321_DATA_ROOT`-based paths for reproducibility.

All 12 target Markdown docs published to S3 are byte-identical to their local tracked Markdown copies under `docs/data_processing/` and `docs/analysis/`. The six coverage CSVs are generated S3 metadata artifacts and do not have tracked local counterparts in `docs/`.

## Scope

Audited families:

| Family | Interpreted S3/code family |
| --- | --- |
| IRS EO BMF | `irs_eo_bmf`, `bronze/irs990/bmf`, `silver/irs990/bmf` |
| NCCS BMF | `nccs_bmf`, `bronze/nccs_bmf`, `silver/nccs_bmf` |
| NCCS 990 file | `nccs_efile`, `bronze/nccs_efile`, `silver/nccs_efile` |
| NCCS 990 Core | `nccs_990_core`, `bronze/nccs_990/core`, `silver/nccs_990/core` |
| NCCS 990 Postcard | `nccs_990_postcard`, `bronze/nccs_990/postcard`, `silver/nccs_990/postcard` |
| GivingTuesday 990 | `givingtuesday_990`, `bronze/givingtuesday_990/datamarts`, `silver/givingtuesday_990` |

Code reviewed included the target `python/ingest/*` families, shared helpers under `python/ingest/_shared`, path configuration in `python/utils/paths.py`, target tests under `tests/`, and repo references under `scripts/`, `sql/`, and notebooks.

A whole-repo code-reference sweep across `python/`, `scripts/`, `sql/`, and notebooks found the expected downstream `combined_990` consumers of the filtered target outputs, plus older GivingTuesday API scripts. Those downstream/legacy files do not define the audited S3 DataMart, NCCS, or IRS source retrieval contracts; no additional documentation finding was opened from them.

Tests were reviewed for audit relevance but not executed because this audit did not change pipeline code or dataset-building behavior.

## S3 Inventory Reconciliation

Fresh inventory command:

```powershell
aws s3 ls s3://swb-321-irs990-teos/ --recursive --summarize
```

Current result:

| Metric | Value |
| --- | ---: |
| Total objects | 378 |
| Total bytes | 35,690,814,575 |
| Target-family objects | 157 |
| Target-family bytes | 14,213,002,142 |
| Other-family objects | 221 |
| Other-family bytes | 21,477,812,433 |

Target-family classification:

| Family | Bronze objects | Bronze bytes | Silver objects | Silver bytes | Total objects |
| --- | ---: | ---: | ---: | ---: | ---: |
| IRS EO BMF | 5 | 15,480,018 | 12 | 889,833 | 17 |
| NCCS BMF | 10 | 1,927,797,133 | 17 | 179,181,067 | 27 |
| NCCS efile | 14 | 1,775,136,549 | 15 | 1,291,098 | 29 |
| NCCS 990 Core | 17 | 195,481,068 | 12 | 1,063,610 | 29 |
| NCCS 990 Postcard | 7 | 1,135,071,946 | 10 | 2,789,417 | 17 |
| GivingTuesday 990 | 24 | 8,974,763,150 | 14 | 4,057,253 | 38 |

Inventory drift:

| Repo document | Published claim | Current audit result | Assessment |
| --- | --- | --- | --- |
| `s3_bucket_tree.md` | Live verification on 2026-04-11: 373 objects | 2026-05-01 live listing: 378 objects | Stale by 5 objects |
| `docs/s3_bucket_inventory.md` | Historical snapshot on 2026-03-22: 298 objects | 2026-05-01 live listing: 378 objects | Correctly labeled historical, but no longer usable for current counts |

The classified object-level inventory is stored in `docs/audit/s3_object_inventory_classification_2026-05-01.csv`.

## Published Documentation Parity

S3 Markdown docs checked:

| Family | S3 Markdown docs | Local parity |
| --- | --- | --- |
| IRS EO BMF | `silver/irs990/bmf/analysis/metadata/irs_bmf_pipeline.md`; `silver/irs990/bmf/analysis/metadata/irs_bmf_analysis_variable_mapping.md` | Byte-identical to local tracked docs |
| NCCS BMF | `silver/nccs_bmf/analysis/metadata/nccs_bmf_pipeline.md`; `silver/nccs_bmf/analysis/metadata/nccs_bmf_analysis_variable_mapping.md` | Byte-identical to local tracked docs |
| NCCS efile | `silver/nccs_efile/analysis/metadata/nccs_efile_pipeline.md`; `silver/nccs_efile/analysis/metadata/nccs_efile_analysis_variable_mapping.md` | Byte-identical to local tracked docs |
| NCCS 990 Core | `silver/nccs_990/core/analysis/metadata/nccs_990_core_pipeline.md`; `silver/nccs_990/core/analysis/metadata/nccs_990_core_analysis_variable_mapping.md` | Byte-identical to local tracked docs |
| NCCS 990 Postcard | `silver/nccs_990/postcard/analysis/metadata/nccs_990_postcard_pipeline.md`; `silver/nccs_990/postcard/analysis/metadata/nccs_990_postcard_analysis_variable_mapping.md` | Byte-identical to local tracked docs |
| GivingTuesday 990 | `silver/givingtuesday_990/analysis/metadata/givingtuesday_datamart_pipeline.md`; `silver/givingtuesday_990/analysis/metadata/givingtuesday_basic_analysis_variable_mapping.md` | Byte-identical to local tracked docs |

S3 coverage artifacts checked:

| Family | Coverage artifact | Rows | Columns |
| --- | --- | ---: | --- |
| IRS EO BMF | `irs_bmf_analysis_variable_coverage.csv` | 72 | canonical variable, status, role, draft variable, tax year, row/populated counts, fill rate, notes |
| NCCS BMF | `nccs_bmf_analysis_variable_coverage.csv` | 57 | canonical variable, status, role, draft variable, tax year, row/populated counts, fill rate, notes |
| NCCS efile | `nccs_efile_analysis_variable_coverage.csv` | 95 | canonical variable, status, role, draft variable, tax year, form type, row/populated counts, fill rate, notes |
| NCCS 990 Core | `nccs_990_core_analysis_variable_coverage.csv` | 81 | tax year, core scope, canonical variable, role, row/populated counts, fill rate, notes |
| NCCS 990 Postcard | `nccs_990_postcard_analysis_variable_coverage.csv` | 57 | canonical variable, status, role, draft variable, tax year, row/populated counts, fill rate, notes |
| GivingTuesday 990 | `givingtuesday_990_basic_allforms_analysis_variable_coverage.csv` | 234 | variable name, tax year, form type, row/populated counts, fill rate, status, notes |

## Source To Code To S3 To Documentation Trace

| Family | Source authority in code | Processing code reviewed | S3 documentation and outputs reviewed | Audit result |
| --- | --- | --- | --- | --- |
| IRS EO BMF | `IRS_BMF_BASE_URL = "https://www.irs.gov/pub/irs-soi"`; state files `eo_sd.csv`, `eo_mn.csv`, `eo_mt.csv`, `eo_az.csv`; tax years 2022-2024 derived from `TAX_PERIOD` | `python/ingest/irs_bmf/common.py`; scripts `01` through `08`; shared geography helpers | Bronze state CSVs and raw manifest under `bronze/irs990/bmf`; silver filtered yearly parquet, combined parquet, analysis parquet/metrics/docs under `silver/irs990/bmf` | Accurate processing, S3 layout, region filter, variable mapping, and final outputs. Add literal source base URL to doc. |
| NCCS BMF | `BMF_DATASET_URL`, `BMF_CATALOG_URL`, `RAW_MONTHLY_BASE_URL`, `LEGACY_BASE_URL`; start year 2022; analysis window 2022-2024 | `python/ingest/nccs_bmf/common.py`; scripts `01` through `08` | Bronze dataset/catalog HTML, latest release, release manifest, size report; silver yearly benchmark/exact-year lookups, analysis parquet/metrics/docs | Accurate release discovery, filter-before-analysis, exact-year lookup, fallbacks, and outputs. Add exact source URLs to doc. |
| NCCS efile | `EFILE_DATASET_URL`, `EFILE_CATALOG_URL`, `EFILE_PUBLIC_BASE_URL`; required tables `HEADER`, `SUMMARY`, Schedule A public charity status; tax years 2022-2024 | `python/ingest/nccs_efile/common.py`; scripts `01` through `09` | Bronze dataset/catalog HTML, latest release, release manifest, size report; silver yearly benchmarks, efile-vs-core comparison, analysis parquet/metrics/docs | Accurate HEADER-first filtering, auxiliary joins, dedupe, variable mapping, and outputs. Add exact source URLs to doc. |
| NCCS 990 Core | `CORE_CATALOG_URL`, `BMF_CATALOG_URL`; 2022 Core release groups; bridge BMF states from benchmark states | `python/ingest/nccs_990_core/common.py`; scripts `01` through `08` | Bronze Core raw files, BMF bridge, data dictionaries, catalogs, manifests; silver filtered Core CSV/parquet, analysis parquet/metrics/docs | Accurate 2022 scope, bridge-BMF region filtering, overlap caveat, variable mapping, and outputs. Add exact source URLs to doc. |
| NCCS 990 Postcard | `POSTCARD_DATASET_URL`, `RAW_BASE_URL`; snapshot year 2026 in S3; analysis tax year clamp 2022-2024 | `python/ingest/nccs_990_postcard/common.py`; scripts `01` through `08` | Bronze postcard page HTML, latest release, release/size manifests, raw monthly files; silver snapshot benchmark CSV/parquet, analysis parquet/metrics/docs | Accurate snapshot discovery, ZIP/state filtering, retained latest-snapshot semantics, variable mapping, and outputs. Add exact source URLs to doc. |
| GivingTuesday 990 | `CATALOG_API_URL = "https://grantstory-api.gtdata.org/v2/public/678d25291eceb5d121099200/items?limit=200"`; required Combined and Basic 990/990EZ/990PF datasets | `python/ingest/990_givingtuesday/datamart/common.py`; datamart scripts and tests | Bronze raw/cache/metadata/presilver artifacts; silver filing outputs, manifests, schema snapshots; analysis parquet/metrics/docs | Strongest documentation coverage. Exact API, required datasets, retrieval metadata, variable dictionaries, region filter, and outputs are documented. No documentation issue found. |

## Region Filtering Verification

Region filtering claims match the reviewed code for the target families.

Shared reference inputs:

| Reference file | Role |
| --- | --- |
| `data/321_Black_Hills_Area_Community_Foundation_2025_08/01_data/reference/GEOID_reference.csv` | Benchmark counties, states, and region names |
| `data/321_Black_Hills_Area_Community_Foundation_2025_08/01_data/reference/zip_to_county_fips.csv` | ZIP5 to county FIPS bridge |
| `data/321_Black_Hills_Area_Community_Foundation_2025_08/01_data/reference/eins_in_benchmark_regions.csv` | EIN reference used by other repo workflows and relevant to benchmark scope |
| `data/321_Black_Hills_Area_Community_Foundation_2025_08/01_data/reference/zip_codes_in_benchmark_regions.csv` | Benchmark ZIP reference |

Verified behavior by family:

| Family | Region logic verified |
| --- | --- |
| IRS EO BMF | Loads benchmark GEOIDs and ZIP-to-county pairs through shared Core helpers; rejects ambiguous ZIP assignments; normalizes ZIP5; validates state match; filters before combining; resolves cross-state duplicate EIN-years. |
| NCCS BMF | Filters each selected yearly/snapshot file by benchmark ZIP and state validation; writes yearly benchmark outputs; builds unfiltered exact-year lookup overlays for enrichment/fallback. |
| NCCS efile | Filters HEADER records to benchmark geography and supported forms before joining SUMMARY and Schedule A tables; dedupes by EIN/tax year with URL/build/amend ranking. |
| NCCS 990 Core | Uses benchmark counties and BMF bridge source data to map Core organizations to region; preserves PC/PZ overlap caveat; filters 2022 Core assets into benchmark outputs. |
| NCCS 990 Postcard | Uses organization ZIP and officer ZIP to map county; validates state match; retains latest snapshot/tax-year/end-date one-row-per-EIN derivative; clamps analysis to tax years 2022-2024. |
| GivingTuesday 990 | Builds ROI ZIP map from `GEOID_reference.csv` and `zip_to_county_fips.csv`; filters Basic all-forms benchmark parquet before analysis outputs. |

Target tests contain relevant coverage for ambiguous ZIP rejection, state mismatch drops, filter-before-join behavior, dedupe/ranking, coverage file generation, and analysis metrics.

## Variables And Coverage Verification

The variable mapping docs and coverage CSVs are present for every family. They describe source variables, normalized variables, analysis variables, role/status, unavailable variables, fallback/enrichment fields, and generated metrics. Coverage artifacts include row counts, populated counts, fill rates, and notes. This is sufficient for the requested audit standard.

No mismatch was found between the reviewed extraction code and the published variable mapping docs for target families. The only recommendation is to state more explicitly that S3 coverage CSVs are generated run artifacts and are the authoritative coverage evidence, because they are not tracked under `docs/`.

## Completeness Scorecard

| Family | Exact source location | Retrieval method | Region filtering | Variables kept/filtered | Processing steps | Final datasets | Validation/caveats |
| --- | --- | --- | --- | --- | --- | --- | --- |
| IRS EO BMF | Partial | Pass | Pass | Pass | Pass | Pass | Pass |
| NCCS BMF | Partial | Pass | Pass | Pass | Pass | Pass | Pass |
| NCCS efile | Partial | Pass | Pass | Pass | Pass | Pass | Pass |
| NCCS 990 Core | Partial | Pass | Pass | Pass | Pass | Pass | Pass |
| NCCS 990 Postcard | Partial | Pass | Pass | Pass | Pass | Pass | Pass |
| GivingTuesday 990 | Pass | Pass | Pass | Pass | Pass | Pass | Pass |

Reason for `Partial` in exact source location: the docs describe the source family, release pages, catalogs, file names, discovery behavior, and source metadata capture, but lack the literal URLs that the code uses. This is a documentation completeness gap, not a code defect.

## Findings

| ID | Severity | Finding | Evidence | Affected docs/S3 keys | Recommended correction |
| --- | --- | --- | --- | --- | --- |
| F-001 | P2 | Current bucket inventory docs are stale. | Current S3 listing has 378 objects and 35,690,814,575 bytes. `s3_bucket_tree.md` says live verification on 2026-04-11 had 373 objects. `docs/s3_bucket_inventory.md` is a historical 2026-03-22 snapshot with 298 objects. | `s3_bucket_tree.md`; `docs/s3_bucket_inventory.md` | Update `s3_bucket_tree.md` with the 2026-05-01 count or add a prominent "last verified" warning. Keep `docs/s3_bucket_inventory.md` explicitly historical or regenerate it from the current classified inventory. |
| F-002 | P2 | Most target pipeline docs omit exact literal public source URLs. | Markdown URL search found a literal `https://` URL only in the GivingTuesday pipeline doc. Code constants define the exact IRS/NCCS source URLs. | IRS BMF, NCCS BMF, NCCS efile, NCCS Core, and NCCS Postcard pipeline docs in S3/local docs | Add source URL subsections listing exact code constants: IRS `https://www.irs.gov/pub/irs-soi`; NCCS BMF dataset/catalog/raw/legacy URLs; NCCS efile dataset/catalog/public base URLs; NCCS Core catalog/BMF catalog URLs; NCCS Postcard dataset/raw URLs. |
| F-003 | P3 | Some published docs include workstation-specific absolute Windows paths. | IRS BMF, NCCS BMF, and NCCS Postcard docs/mapping files include `C:\Users\eilke\Desktop\Github Repo\swb_project_321\...` paths. `python/utils/paths.py` already supports repo-root defaults and `SWB_321_DATA_ROOT`. | Affected local and S3-published docs for IRS BMF, NCCS BMF, NCCS Postcard | Keep absolute paths only as run provenance if desired, but add repo-relative and `SWB_321_DATA_ROOT`-based path forms so the docs are reproducible on another machine. |
| F-004 | Info | Coverage CSVs are S3-generated artifacts, not tracked docs. | Six S3 coverage CSVs were present and inspected; no tracked copies under `docs/` were found. | `silver/*/analysis/metadata/*_analysis_variable_coverage.csv` | No pipeline fix required. Add a sentence in the docs that coverage CSVs are generated with each analysis build and that the S3 object is the audited/published coverage evidence. |

## Pipeline Concerns

No pipeline correctness defect was found during this audit that would invalidate the target documentation or the final target datasets.

The only pipeline-adjacent concern is documentation portability: several docs contain absolute local paths even though `python/utils/paths.py` supports `SWB_321_DATA_ROOT`. This can make the docs look machine-bound, but it does not change produced data.

## Acceptance Criteria Status

| Criterion | Status |
| --- | --- |
| Fresh S3 inventory reconciles target raw, silver, analysis, metadata, and documentation objects | Met; 378 objects inventoried and classified in the supporting CSV |
| Target documentation checked against code paths, constants, source URLs, S3 keys, manifests, and outputs | Met |
| Region filtering claims verified against code and reference files | Met |
| Variable mapping claims checked against extraction code and coverage artifacts | Met |
| Existing target-family tests reviewed for audit relevance | Met |
| Final report separates documentation gaps from pipeline defects | Met |

## No-Issue Statements By Family

GivingTuesday 990: no issue found in the audited documentation relative to the code and S3 artifacts.

IRS EO BMF: no processing or S3-output issue found. Documentation should add the exact IRS base/source URL and improve path portability.

NCCS BMF: no processing or S3-output issue found. Documentation should add exact NCCS dataset/catalog/raw/legacy URLs and improve path portability.

NCCS efile: no processing or S3-output issue found. Documentation should add exact NCCS dataset/catalog/public base URLs.

NCCS 990 Core: no processing or S3-output issue found. Documentation should add exact Core and BMF catalog URLs.

NCCS 990 Postcard: no processing or S3-output issue found. Documentation should add exact NCCS dataset/raw URLs and improve path portability.
