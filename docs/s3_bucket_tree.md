# S3 Bucket Tree

- Bucket: `swb-321-irs990-teos`
- Region: `us-east-2`
- Snapshot date: `2026-03-20`
- Source: live recursive S3 listing

```text
swb-321-irs990-teos/
|-- bronze/
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
|   |       `-- unfiltered/
|   |           |-- givingtuesday_990_basic_allforms_unfiltered.parquet
|   |           `-- givingtuesday_990_basic_plus_combined_unfiltered.parquet
|   |-- irs990/
|   |   |-- bmf/
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
|   |               `-- 1 object total: 2026_TEOS_XML_01A.zip
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
|   `-- nccs_990/
|       `-- core/
|           |-- bridge_bmf/
|           |   |-- state=AZ/
|           |   |   `-- AZ_BMF_V1.1.csv
|           |   |-- state=MN/
|           |   |   `-- MN_BMF_V1.1.csv
|           |   |-- state=MT/
|           |   |   `-- MT_BMF_V1.1.csv
|           |   `-- state=SD/
|           |       `-- SD_BMF_V1.1.csv
|           |-- metadata/
|           |   |-- CORE-HRMN_dd.csv
|           |   |-- DD-PF-HRMN-V0.csv
|           |   |-- catalog_bmf.html
|           |   |-- catalog_core.html
|           |   |-- harmonized_data_dictionary.xlsx
|           |   |-- latest_release.json
|           |   |-- release_manifest_year=2022.csv
|           |   `-- size_verification_year=2022.csv
|           `-- raw/
|               `-- year=2022/
|                   |-- CORE-2022-501C3-CHARITIES-PC-HRMN.csv
|                   |-- CORE-2022-501C3-CHARITIES-PZ-HRMN.csv
|                   |-- CORE-2022-501C3-PRIVFOUND-PF-HRMN-V0.csv
|                   |-- CORE-2022-501CE-NONPROFIT-PC-HRMN.csv
|                   `-- CORE-2022-501CE-NONPROFIT-PZ-HRMN.csv
`-- silver/
    |-- givingtuesday_990/
    |   `-- filing/
    |       |-- givingtuesday_990_filings_benchmark.parquet
    |       `-- manifest_filtered.json
    |-- irs990/
    |   `-- bmf/
    |       `-- bmf_benchmark_counties.parquet
    |-- irs_soi/
    |   `-- county/
    |       `-- tax_year=2022/
    |           |-- filter_manifest_2022.csv
    |           |-- irs_soi_county_benchmark_agi_2022.csv
    |           `-- irs_soi_county_benchmark_noagi_2022.csv
    `-- nccs_990/
        `-- core/
            `-- year=2022/
                |-- CORE-2022-501C3-CHARITIES-PC-HRMN__benchmark.csv
                |-- CORE-2022-501C3-CHARITIES-PZ-HRMN__benchmark.csv
                |-- CORE-2022-501C3-PRIVFOUND-PF-HRMN-V0__benchmark.csv
                |-- CORE-2022-501CE-NONPROFIT-PC-HRMN__benchmark.csv
                |-- CORE-2022-501CE-NONPROFIT-PZ-HRMN__benchmark.csv
                `-- filter_manifest_year=2022.csv
```
