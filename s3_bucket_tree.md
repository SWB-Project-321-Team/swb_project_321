# S3 Bucket Tree

- Bucket: `swb-321-irs990-teos`
- Region: `us-east-2`
- Live verification: `2026-05-02` via `aws s3 ls s3://swb-321-irs990-teos --recursive --summarize --region us-east-2`
- Object count: `489`
- Total bytes: `35,711,714,691`
- Current classified inventory: `docs/audit/s3_object_inventory_classification_2026-05-02.csv`
- Final preprocessing documentation package: `documentation/final_preprocessing_docs/` (111 objects)

## Current Analysis Documentation Layout

The six target analysis families use separate S3 prefixes for narrative documentation, variable mappings, and coverage evidence.

| Family | Pipeline doc | Variable mapping | Coverage evidence |
| --- | --- | --- | --- |
| IRS EO BMF | `silver/irs990/bmf/analysis/documentation/irs_bmf_pipeline.md` | `silver/irs990/bmf/analysis/variable_mappings/irs_bmf_analysis_variable_mapping.md` | `silver/irs990/bmf/analysis/quality/coverage/irs_bmf_analysis_variable_coverage.csv` |
| NCCS BMF | `silver/nccs_bmf/analysis/documentation/nccs_bmf_pipeline.md` | `silver/nccs_bmf/analysis/variable_mappings/nccs_bmf_analysis_variable_mapping.md` | `silver/nccs_bmf/analysis/quality/coverage/nccs_bmf_analysis_variable_coverage.csv` |
| NCCS efile | `silver/nccs_efile/analysis/documentation/nccs_efile_pipeline.md` | `silver/nccs_efile/analysis/variable_mappings/nccs_efile_analysis_variable_mapping.md` | `silver/nccs_efile/analysis/quality/coverage/nccs_efile_analysis_variable_coverage.csv` |
| NCCS 990 Core | `silver/nccs_990/core/analysis/documentation/nccs_990_core_pipeline.md` | `silver/nccs_990/core/analysis/variable_mappings/nccs_990_core_analysis_variable_mapping.md` | `silver/nccs_990/core/analysis/quality/coverage/nccs_990_core_analysis_variable_coverage.csv` |
| NCCS 990 Postcard | `silver/nccs_990/postcard/analysis/documentation/nccs_990_postcard_pipeline.md` | `silver/nccs_990/postcard/analysis/variable_mappings/nccs_990_postcard_analysis_variable_mapping.md` | `silver/nccs_990/postcard/analysis/quality/coverage/nccs_990_postcard_analysis_variable_coverage.csv` |
| GivingTuesday 990 | `silver/givingtuesday_990/analysis/documentation/givingtuesday_datamart_pipeline.md` | `silver/givingtuesday_990/analysis/variable_mappings/givingtuesday_basic_analysis_variable_mapping.md` | `silver/givingtuesday_990/analysis/quality/coverage/givingtuesday_990_basic_allforms_analysis_variable_coverage.csv` |

Old target analysis metadata object count after migration: `0`.

## Default Prefix Map

| Dataset | Bronze | Silver / curated | Analysis / documentation |
| --- | --- | --- | --- |
| BLS QCEW nonprofit | `bronze/bls/` | `silver/bls/` | No target analysis layout |
| GivingTuesday DataMart | `bronze/givingtuesday_990/datamarts/{raw,metadata,presilver}` | `silver/givingtuesday_990/filing/` | `silver/givingtuesday_990/analysis/{documentation,variable_mappings,quality/coverage}` |
| IRS TEOS 990 XML | `bronze/irs990/teos_xml/` | Not applicable | Not applicable |
| IRS EO BMF | `bronze/irs990/bmf/` | `silver/irs990/bmf/` | `silver/irs990/bmf/analysis/{documentation,variable_mappings,quality/coverage}` |
| IRS SOI county | `bronze/irs_soi/county/` | `silver/irs_soi/county/` | Not applicable |
| NCCS 990 Core | `bronze/nccs_990/core/` | `silver/nccs_990/core/` | `silver/nccs_990/core/analysis/{documentation,variable_mappings,quality/coverage}` |
| NCCS e-Postcard | `bronze/nccs_990/postcard/` | `silver/nccs_990/postcard/` | `silver/nccs_990/postcard/analysis/{documentation,variable_mappings,quality/coverage}` |
| NCCS BMF | `bronze/nccs_bmf/` | `silver/nccs_bmf/` | `silver/nccs_bmf/analysis/{documentation,variable_mappings,quality/coverage}` |
| NCCS efile | `bronze/nccs_efile/` | `silver/nccs_efile/` | `silver/nccs_efile/analysis/{documentation,variable_mappings,quality/coverage}` |
| Combined 990 | Not applicable | `silver/combined_990/` | Not applicable |
| ACS | `bronze/acs/` | `silver/acs/` | Not applicable |
| USAspending benchmark | Not applicable | `silver/usa_spending/` | Not applicable |
| Final preprocessing documentation package | Not applicable | `documentation/final_preprocessing_docs/` | Documentation package mirror |

## Object Counts By Top-Level Prefix

| Top-level prefix | Objects | Bytes |
| --- | --- | --- |
| `bronze/` | 269 | 35,426,450,220 |
| `documentation/` | 111 | 20,880,595 |
| `silver/` | 109 | 264,383,876 |

## Object Counts By Family

| Family | Objects |
| --- | --- |
| acs | 6 |
| bls | 4 |
| combined_990 | 15 |
| final_preprocessing_docs | 111 |
| givingtuesday_990 | 38 |
| irs_bmf | 17 |
| irs_soi_county | 11 |
| irs_teos_xml | 180 |
| nccs_990_core | 29 |
| nccs_990_postcard | 17 |
| nccs_bmf | 27 |
| nccs_efile | 29 |
| other | 3 |
| usa_spending | 2 |

## Object Counts By Role

| Role | Objects |
| --- | --- |
| client_wrapper | 12 |
| coverage | 12 |
| data | 274 |
| documentation | 13 |
| documentation_package | 2 |
| metadata | 133 |
| other | 11 |
| raw_dictionary | 20 |
| variable_mapping | 12 |

## Live Prefix Counts

These prefix counts are generated from the current object inventory and show the first two path components.

| Prefix | Objects |
| --- | --- |
| `bronze/acs/` | 3 |
| `bronze/bls/` | 1 |
| `bronze/givingtuesday_990/` | 24 |
| `bronze/irs990/` | 185 |
| `bronze/irs_soi/` | 6 |
| `bronze/land_area/` | 1 |
| `bronze/nccs_990/` | 24 |
| `bronze/nccs_bmf/` | 10 |
| `bronze/nccs_efile/` | 14 |
| `bronze/rural/` | 1 |
| `documentation/final_preprocessing_docs/` | 111 |
| `silver/acs/` | 3 |
| `silver/bls/` | 3 |
| `silver/combined_990/` | 15 |
| `silver/givingtuesday_990/` | 14 |
| `silver/irs990/` | 12 |
| `silver/irs_soi/` | 5 |
| `silver/land_area/` | 1 |
| `silver/nccs_990/` | 22 |
| `silver/nccs_bmf/` | 17 |
| `silver/nccs_efile/` | 15 |
| `silver/usa_spending/` | 2 |

## Notes

- S3 does not have real folders; folder names here are inferred from object key prefixes.
- The target analysis Markdown and coverage files are documentation/metadata artifacts only. Raw source data, Silver parquet outputs, and final datasets were not moved during the documentation restructure.
- Historical row-level inventory in `docs/s3_bucket_inventory.md` remains a 2026-03-22 snapshot; use the classified CSV above for the current object list.
