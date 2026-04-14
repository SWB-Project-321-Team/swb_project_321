# S3 Bucket Tree

- Bucket: `swb-321-irs990-teos` (default; override with env `IRS_990_S3_BUCKET`)
- Region: `us-east-2` (default; override with `AWS_DEFAULT_REGION`)
- **Live verification:** `2026-04-11` — full bucket listing via AWS CLI / boto3 (`list_objects_v2`): **373 objects** (`bronze/` 266, `silver/` 107).
- **Code vs bucket:** Rows below combine ingest defaults with keys **actually present** in S3. Some families (ACS, USAspending, Silver BLS derivatives) may be produced outside `python/ingest/`; search the repo for those path strings.
- Older object-level table: `docs/s3_bucket_inventory.md` (snapshot `2026-03-22`, 298 objects; superseded for counts).

## Default prefix map (from code)


| Dataset                 | Bronze                                                        | Silver (curated / filtered)                               | Analysis                                                      |
| ----------------------- | ------------------------------------------------------------- | --------------------------------------------------------- | ------------------------------------------------------------- |
| BLS QCEW (nonprofit)    | `bronze/bls/` (raw XLSX)                                      | `silver/bls/` (CSV + QA metadata)                         | —                                                             |
| Giving Tuesday DataMart | `bronze/givingtuesday_990/datamarts/{raw,metadata,presilver}` | `silver/givingtuesday_990/filing/` (+ `filing/metadata/`) | `silver/givingtuesday_990/analysis/` (+ `analysis/metadata/`) |
| IRS TEOS 990 XML        | `bronze/irs990/teos_xml/`                                     | —                                                         | —                                                             |
| IRS EO BMF (state CSVs) | `bronze/irs990/bmf/` (+ `bmf/metadata/`)                      | `silver/irs990/bmf/` (+ `metadata/`, `year=YYYY/`)        | `silver/irs990/bmf/analysis/` (+ `analysis/metadata/`)        |
| IRS SOI county          | `bronze/irs_soi/county/`                                      | `silver/irs_soi/county/`                                  | —                                                             |
| NCCS 990 Core           | `bronze/nccs_990/core/`                                       | `silver/nccs_990/core/`                                   | `silver/nccs_990/core/analysis/`                              |
| NCCS e-Postcard         | `bronze/nccs_990/postcard/`                                   | `silver/nccs_990/postcard/`                               | `silver/nccs_990/postcard/analysis/`                          |
| NCCS BMF                | `bronze/nccs_bmf/`                                            | `silver/nccs_bmf/`                                        | `silver/nccs_bmf/analysis/`                                   |
| NCCS efile              | `bronze/nccs_efile/`                                          | `silver/nccs_efile/` (+ `comparison/`)                    | `silver/nccs_efile/analysis/`                                 |
| Combined 990            | —                                                             | `silver/combined_990/`                                    | —                                                             |
| ACS (Census API pulls)  | `bronze/acs/`                                                 | `silver/acs/` (+ `metadata/`)                               | —                                                             |
| USAspending (benchmark) | —                                                             | `silver/usa_spending/`                                      | —                                                             |


## Representative tree (illustrative keys)

Filenames and years vary with pipeline runs. Analysis subtrees follow each family’s upload scripts (steps 08/09/14 as applicable).

```text
swb-321-irs990-teos/
|-- bronze/
|   |-- acs/
|   |   |-- acs5_full_2024.csv
|   |   `-- acs5_subject_2024.csv
|   |-- bls/
|   |   `-- qcew-nonprofits-2022.xlsx
|   |-- givingtuesday_990/
|   |   `-- datamarts/
|   |       |-- metadata/
|   |       |   |-- datamart_catalog.csv
|   |       |   |-- datamart_catalog.md
|   |       |   |-- datamart_catalog_raw.json
|   |       |   |-- datamart_fields.csv
|   |       |   |-- datamart_fields.md
|   |       |   |-- required_datasets_manifest.csv
|   |       |   `-- size_verification_report.csv
|   |       |-- raw/
|   |       |   |-- 2025_07_10_All_Years_990_990ez_990pf_990n_Combined_DataMart.csv
|   |       |   |-- 2025_08_29_All_Years_990PFStandardFields.csv
|   |       |   |-- 2025_10_18_All_Years_990StandardFields.csv
|   |       |   `-- 2025_10_28_All_Years_990EZStandardFields.csv
|   |       |-- presilver/
|   |       |   |-- givingtuesday_990_basic_allforms_presilver.parquet
|   |       |   `-- givingtuesday_990_basic_plus_combined_presilver.parquet
|   |       `-- unfiltered/   # legacy intermediate; current pipeline uses presilver/
|   |           |-- givingtuesday_990_basic_allforms_unfiltered.parquet
|   |           `-- givingtuesday_990_basic_plus_combined_unfiltered.parquet
|   |-- irs990/
|   |   |-- bmf/
|   |   |   |-- metadata/
|   |   |   |   |-- irs_bmf_raw_manifest.csv
|   |   |   |   `-- irs_bmf_raw_size_verification.csv
|   |   |   |-- eo_az.csv
|   |   |   |-- eo_mn.csv
|   |   |   |-- eo_mt.csv
|   |   |   `-- eo_sd.csv
|   |   `-- teos_xml/
|   |       |-- code/
|   |       |   `-- python.zip
|   |       |-- index/
|   |       |   |-- year=2021/
|   |       |   |   |-- .keep
|   |       |   |   `-- index_2021.csv
|   |       |   |-- year=2022/
|   |       |   |   |-- .keep
|   |       |   |   `-- index_2022.csv
|   |       |   |-- year=2023/
|   |       |   |   |-- .keep
|   |       |   |   `-- index_2023.csv
|   |       |   |-- year=2024/
|   |       |   |   |-- .keep
|   |       |   |   `-- index_2024.csv
|   |       |   |-- year=2025/
|   |       |   |   |-- .keep
|   |       |   |   `-- index_2025.csv
|   |       |   `-- year=2026/
|   |       |       `-- index_2026.csv
|   |       `-- zips/
|   |           |-- year=2021/
|   |           |   `-- 121 objects total: .keep, 2021_TEOS_XML_01A.zip, and 119 additional ZIP objects
|   |           |-- year=2022/
|   |           |   `-- 3 objects total: .keep, 2022_TEOS_XML_01A.zip, 2022_TEOS_XML_02A.zip
|   |           |-- year=2023/
|   |           |   `-- 13 objects total: .keep, 2023_TEOS_XML_01A.zip, and 11 additional ZIP objects
|   |           |-- year=2024/
|   |           |   `-- 13 objects total: .keep, 2024_TEOS_XML_01A.zip, and 11 additional ZIP objects
|   |           |-- year=2025/
|   |           |   `-- 17 objects total: .keep, 2025_TEOS_XML_01A.zip, and 15 additional ZIP objects
|   |           `-- year=2026/
|   |               `-- 1 objects total: 2026_TEOS_XML_01A.zip
|   |-- irs_soi/
|   |   `-- county/
|   |       |-- metadata/
|   |       |   |-- latest_release.json
|   |       |   |-- release_manifest_tax_year=2022.csv
|   |       |   `-- size_verification_tax_year=2022.csv
|   |       `-- raw/
|   |           `-- tax_year=2022/
|   |               |-- 22incyallagi.csv
|   |               |-- 22incyallnoagi.csv
|   |               `-- 22incydocguide.docx
|   |-- nccs_990/
|   |   |-- core/
|   |   |   |-- bridge_bmf/
|   |   |   |   |-- state=AZ/
|   |   |   |   |   `-- AZ_BMF_V1.1.csv
|   |   |   |   |-- state=MN/
|   |   |   |   |   `-- MN_BMF_V1.1.csv
|   |   |   |   |-- state=MT/
|   |   |   |   |   `-- MT_BMF_V1.1.csv
|   |   |   |   `-- state=SD/
|   |   |   |       `-- SD_BMF_V1.1.csv
|   |   |   |-- metadata/
|   |   |   |   |-- CORE-HRMN_dd.csv
|   |   |   |   |-- DD-PF-HRMN-V0.csv
|   |   |   |   |-- catalog_bmf.html
|   |   |   |   |-- catalog_core.html
|   |   |   |   |-- harmonized_data_dictionary.xlsx
|   |   |   |   |-- latest_release.json
|   |   |   |   |-- release_manifest_year=2022.csv
|   |   |   |   `-- size_verification_year=2022.csv
|   |   |   `-- raw/
|   |   |       `-- year=2022/
|   |   |           |-- CORE-2022-501C3-CHARITIES-PC-HRMN.csv
|   |   |           |-- CORE-2022-501C3-CHARITIES-PZ-HRMN.csv
|   |   |           |-- CORE-2022-501C3-PRIVFOUND-PF-HRMN-V0.csv
|   |   |           |-- CORE-2022-501CE-NONPROFIT-PC-HRMN.csv
|   |   |           `-- CORE-2022-501CE-NONPROFIT-PZ-HRMN.csv
|   |   `-- postcard/
|   |       |-- metadata/
|   |       |   |-- latest_release.json
|   |       |   |-- postcard_page.html
|   |       |   |-- release_manifest_snapshot_year=2026.csv
|   |       |   `-- size_verification_snapshot_year=2026.csv
|   |       `-- raw/
|   |           `-- snapshot_year=2026/
|   |               |-- snapshot_month=2026-01/
|   |               |   `-- 2026-01-E-POSTCARD.csv
|   |               |-- snapshot_month=2026-02/
|   |               |   `-- 2026-02-E-POSTCARD.csv
|   |               `-- snapshot_month=2026-03/
|   |                   `-- 2026-03-E-POSTCARD.csv
|   |-- nccs_bmf/
|   |   |-- metadata/
|   |   |   |-- catalog_bmf.html
|   |   |   |-- dataset_bmf.html
|   |   |   |-- latest_release.json
|   |   |   |-- release_manifest_start_year=2022.csv
|   |   |   `-- size_verification_start_year=2022.csv
|   |   `-- raw/
|   |       |-- year=2022/
|   |       |   `-- BMF-2022-08-501CX-NONPROFIT-PX.csv
|   |       |-- year=2023/
|   |       |   `-- 2023-12-BMF.csv
|   |       |-- year=2024/
|   |       |   `-- 2024-12-BMF.csv
|   |       |-- year=2025/
|   |       |   `-- 2025-12-BMF.csv
|   |       `-- year=2026/
|   |           `-- 2026-03-BMF.csv
|   `-- nccs_efile/
|       |-- metadata/
|       |   |-- catalog_efile.html
|       |   |-- dataset_efile.html
|       |   |-- latest_release.json
|       |   |-- release_manifest_start_year=2022.csv
|       |   `-- size_verification_start_year=2022.csv
|       `-- raw/
|           |-- tax_year=2022/
|           |   |-- table=F9-P00-T00-HEADER/
|           |   |   `-- F9-P00-T00-HEADER-2022.CSV
|           |   |-- table=F9-P01-T00-SUMMARY/
|           |   |   `-- F9-P01-T00-SUMMARY-2022.CSV
|           |   `-- table=SA-P01-T00-PUBLIC-CHARITY-STATUS/
|           |       `-- SA-P01-T00-PUBLIC-CHARITY-STATUS-2022.CSV
|           |-- tax_year=2023/
|           |   |-- table=F9-P00-T00-HEADER/
|           |   |   `-- F9-P00-T00-HEADER-2023.CSV
|           |   |-- table=F9-P01-T00-SUMMARY/
|           |   |   `-- F9-P01-T00-SUMMARY-2023.CSV
|           |   `-- table=SA-P01-T00-PUBLIC-CHARITY-STATUS/
|           |       `-- SA-P01-T00-PUBLIC-CHARITY-STATUS-2023.CSV
|           `-- tax_year=2024/
|               |-- table=F9-P00-T00-HEADER/
|               |   `-- F9-P00-T00-HEADER-2024.CSV
|               |-- table=F9-P01-T00-SUMMARY/
|               |   `-- F9-P01-T00-SUMMARY-2024.CSV
|               `-- table=SA-P01-T00-PUBLIC-CHARITY-STATUS/
|                   `-- SA-P01-T00-PUBLIC-CHARITY-STATUS-2024.CSV
`-- silver/
    |-- acs/
    |   |-- acs_merged_2024.csv
    |   `-- metadata/
    |       `-- acs_variable_dictionary.csv
    |-- bls/
    |   |-- qcew_nonprofits_2022.csv
    |   `-- metadata/
    |       |-- bls_data_dictionary.xlsx
    |       `-- missing_regions_2022.csv
    |-- combined_990/
    |   |-- metadata/
    |   |   |-- build_summary.json
    |   |   |-- column_dictionary.csv
    |   |   |-- diag_overlap_by_ein.csv
    |   |   |-- diag_overlap_by_ein_tax_year.csv
    |   |   |-- diag_overlap_summary.csv
    |   |   |-- field_availability_matrix.csv
    |   |   |-- master_column_dictionary.csv
    |   |   |-- master_conflict_detail.csv
    |   |   |-- master_conflict_detail.parquet
    |   |   |-- master_conflict_summary.csv
    |   |   |-- master_field_selection_summary.csv
    |   |   |-- size_verification.csv
    |   |   `-- source_input_manifest.csv
    |   |-- combined_990_filtered_source_union.parquet
    |   `-- combined_990_master_ein_tax_year.parquet
    |-- givingtuesday_990/
    |   |-- filing/
    |   |   |-- metadata/
    |   |   |-- givingtuesday_990_filings_benchmark.parquet
    |   |   `-- manifest_filtered.json
    |   `-- analysis/
    |       |-- metadata/
    |       |   |-- givingtuesday_990_basic_allforms_analysis_variable_coverage.csv
    |       |   |-- givingtuesday_basic_analysis_variable_mapping.md
    |       |   `-- givingtuesday_datamart_pipeline.md
    |       |-- givingtuesday_990_basic_allforms_analysis_variables.parquet
    |       `-- givingtuesday_990_basic_allforms_analysis_region_metrics.parquet
    |-- irs990/
    |   `-- bmf/
    |       |-- irs_bmf_combined_filtered.parquet
    |       |-- bmf_benchmark_counties.parquet
    |       |-- year=2022/
    |       |   `-- irs_bmf_benchmark_year=2022.parquet
    |       |-- year=2023/
    |       |   `-- irs_bmf_benchmark_year=2023.parquet
    |       |-- year=2024/
    |       |   `-- irs_bmf_benchmark_year=2024.parquet
    |       |-- metadata/
    |       |   |-- irs_bmf_filter_manifest.csv
    |       |   `-- irs_bmf_filtered_size_verification.csv
    |       `-- analysis/
    |           |-- metadata/
    |           |   |-- irs_bmf_analysis_variable_coverage.csv
    |           |   |-- irs_bmf_analysis_variable_mapping.md
    |           |   `-- irs_bmf_pipeline.md
    |           |-- irs_bmf_analysis_variables.parquet
    |           |-- irs_bmf_analysis_geography_metrics.parquet
    |           `-- irs_bmf_analysis_field_metrics.parquet
    |-- irs_soi/
    |   `-- county/
    |       `-- tax_year=2022/
    |           |-- filter_manifest_2022.csv
    |           |-- irs_soi_county_benchmark_agi_2022.csv
    |           `-- irs_soi_county_benchmark_noagi_2022.csv
    |-- nccs_990/
    |   |-- core/
    |   |   |-- year=2022/
    |   |   |   |-- CORE-2022-501C3-CHARITIES-PC-HRMN__benchmark.csv
    |   |   |   |-- CORE-2022-501C3-CHARITIES-PZ-HRMN__benchmark.csv
    |   |   |   |-- CORE-2022-501C3-PRIVFOUND-PF-HRMN-V0__benchmark.csv
    |   |   |   |-- CORE-2022-501CE-NONPROFIT-PC-HRMN__benchmark.csv
    |   |   |   |-- CORE-2022-501CE-NONPROFIT-PZ-HRMN__benchmark.csv
    |   |   |   |-- nccs_990_core_combined_filtered_year=2022.parquet
    |   |   |   `-- filter_manifest_year=2022.csv
    |   |   `-- analysis/
    |   |       |-- metadata/
    |   |       |   |-- nccs_990_core_analysis_variable_coverage.csv
    |   |       |   |-- nccs_990_core_analysis_variable_mapping.md
    |   |       |   `-- nccs_990_core_pipeline.md
    |   |       |-- nccs_990_core_analysis_variables.parquet
    |   |       `-- nccs_990_core_analysis_geography_metrics.parquet
    |   `-- postcard/
    |       |-- snapshot_year=2026/
    |       |   |-- filter_manifest_snapshot_year=2026.csv
    |       |   |-- filter_manifest_snapshot_year=2026_tax_year_start=2022.csv
    |       |   |-- nccs_990_postcard_benchmark_snapshot_year=2026.csv
    |       |   `-- nccs_990_postcard_benchmark_tax_year_start=2022_snapshot_year=2026.csv
    |       `-- analysis/
    |           |-- metadata/
    |           |   |-- nccs_990_postcard_analysis_variable_coverage.csv
    |           |   |-- nccs_990_postcard_analysis_variable_mapping.md
    |           |   `-- nccs_990_postcard_pipeline.md
    |           |-- nccs_990_postcard_analysis_variables.parquet
    |           `-- nccs_990_postcard_analysis_geography_metrics.parquet
    |-- nccs_bmf/
    |   |-- metadata/
    |   |   `-- filter_manifest_start_year=2022.csv
    |   |-- year=2022/
    |   |   |-- nccs_bmf_benchmark_year=2022.parquet
    |   |   `-- nccs_bmf_exact_year_lookup_year=2022.parquet
    |   |-- year=2023/
    |   |   |-- nccs_bmf_benchmark_year=2023.parquet
    |   |   `-- nccs_bmf_exact_year_lookup_year=2023.parquet
    |   |-- year=2024/
    |   |   |-- nccs_bmf_benchmark_year=2024.parquet
    |   |   `-- nccs_bmf_exact_year_lookup_year=2024.parquet
    |   |-- year=2025/
    |   |   |-- nccs_bmf_benchmark_year=2025.parquet
    |   |   `-- nccs_bmf_exact_year_lookup_year=2025.parquet
    |   |-- year=2026/
    |   |   |-- nccs_bmf_benchmark_year=2026.parquet
    |   |   `-- nccs_bmf_exact_year_lookup_year=2026.parquet
    |   `-- analysis/
    |       |-- metadata/
    |       |   |-- nccs_bmf_analysis_variable_coverage.csv
    |       |   |-- nccs_bmf_analysis_variable_mapping.md
    |       |   `-- nccs_bmf_pipeline.md
    |       |-- nccs_bmf_analysis_variables.parquet
    |       |-- nccs_bmf_analysis_geography_metrics.parquet
    |       `-- nccs_bmf_analysis_field_metrics.parquet
    |-- nccs_efile/
        |-- comparison/
        |   `-- tax_year=2022/
        |       |-- efile_vs_core_conflicts_tax_year=2022.csv
        |       |-- efile_vs_core_fill_rates_tax_year=2022.csv
        |       |-- efile_vs_core_row_counts_tax_year=2022.csv
        |       `-- efile_vs_core_summary_tax_year=2022.json
        |-- tax_year=2022/
        |   |-- filter_manifest_tax_year=2022.csv
        |   `-- nccs_efile_benchmark_tax_year=2022.parquet
        |-- tax_year=2023/
        |   |-- filter_manifest_tax_year=2023.csv
        |   `-- nccs_efile_benchmark_tax_year=2023.parquet
        |-- tax_year=2024/
        |   |-- filter_manifest_tax_year=2024.csv
        |   `-- nccs_efile_benchmark_tax_year=2024.parquet
        `-- analysis/
            |-- metadata/
            |   |-- nccs_efile_analysis_variable_coverage.csv
            |   |-- nccs_efile_analysis_variable_mapping.md
            |   `-- nccs_efile_pipeline.md
            |-- nccs_efile_analysis_variables.parquet
            `-- nccs_efile_analysis_geography_metrics.parquet
    `-- usa_spending/
        |-- usaspending_benchmark_awards_consolidated.csv
        `-- usaspending_benchmark_awards_consolidated.parquet
```

## Code references

- Bucket / region defaults: `python/ingest/nccs_990_core/common.py` (`DEFAULT_S3_BUCKET`, `DEFAULT_S3_REGION`)
- BLS raw upload: `python/ingest/BLS/fetchBLS.py` (`bronze/bls/…`); Silver BLS paths are not defined in that script (search repo for `silver/bls`).
- IRS BMF keys: `python/ingest/irs_bmf/common.py`
- Giving Tuesday: `python/ingest/990_givingtuesday/datamart/common.py`
- Combined: `python/ingest/combined_990/common.py` (`SILVER_PREFIX`, `METADATA_S3_PREFIX`)
- ACS bronze uploads: `python/ingest/ACS/acs_api.py`. Silver ACS / Silver BLS / USAspending S3 keys are not centralized in `common.py`; see `docs/data_processing/usa_spending.md` and repo search for `usaspending`.

