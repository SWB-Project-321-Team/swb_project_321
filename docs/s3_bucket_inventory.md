# S3 Bucket Inventory

- Bucket: `swb-321-irs990-teos`
- Region: `us-east-2`
- Snapshot date: `2026-03-22`
- Total object count: `298`
- Total discovered folder-prefix count: `97`

This file is a historical inventory snapshot generated from a recursive S3 listing on `2026-03-22`. S3 does not have real folders, so the "folders" below are key prefixes inferred from object paths.

For the **canonical prefix layout** (including Bronze `bronze/bls/`, IRS BMF metadata, and Silver `analysis/` trees added after this snapshot), see `docs/s3_bucket_tree.md` (repo root: `s3_bucket_tree.md`).

Important GT note:

- This snapshot was taken on `2026-03-22`, before the GT pre-Silver rename from `unfiltered/` to `presilver/`.
- The current GT pipeline now targets `bronze/givingtuesday_990/datamarts/presilver/` instead.

Prefixes present in code but **not** represented in the folder list below (often because no upload had run yet as of `2026-03-22`) include, for example: `bronze/bls/`, `bronze/irs990/bmf/metadata/`, `silver/givingtuesday_990/analysis/`, `silver/irs990/bmf/` (full filtered + analysis layout), `silver/nccs_990/core/analysis/`, `silver/nccs_990/postcard/analysis/`, `silver/nccs_bmf/analysis/`, `silver/nccs_efile/analysis/`.

## Folder Prefixes

- `bronze/`
- `bronze/givingtuesday_990/`
- `bronze/givingtuesday_990/datamarts/`
- `bronze/givingtuesday_990/datamarts/metadata/`
- `bronze/givingtuesday_990/datamarts/raw/`
- `bronze/givingtuesday_990/datamarts/presilver/`
- `bronze/irs990/`
- `bronze/irs990/bmf/`
- `bronze/irs990/teos_xml/`
- `bronze/irs990/teos_xml/code/`
- `bronze/irs990/teos_xml/index/`
- `bronze/irs990/teos_xml/index/year=2021/`
- `bronze/irs990/teos_xml/index/year=2022/`
- `bronze/irs990/teos_xml/index/year=2023/`
- `bronze/irs990/teos_xml/index/year=2024/`
- `bronze/irs990/teos_xml/index/year=2025/`
- `bronze/irs990/teos_xml/index/year=2026/`
- `bronze/irs990/teos_xml/zips/`
- `bronze/irs990/teos_xml/zips/year=2021/`
- `bronze/irs990/teos_xml/zips/year=2022/`
- `bronze/irs990/teos_xml/zips/year=2023/`
- `bronze/irs990/teos_xml/zips/year=2024/`
- `bronze/irs990/teos_xml/zips/year=2025/`
- `bronze/irs990/teos_xml/zips/year=2026/`
- `bronze/irs_soi/`
- `bronze/irs_soi/county/`
- `bronze/irs_soi/county/metadata/`
- `bronze/irs_soi/county/raw/`
- `bronze/irs_soi/county/raw/tax_year=2022/`
- `bronze/nccs_990/`
- `bronze/nccs_990/core/`
- `bronze/nccs_990/core/bridge_bmf/`
- `bronze/nccs_990/core/bridge_bmf/state=AZ/`
- `bronze/nccs_990/core/bridge_bmf/state=MN/`
- `bronze/nccs_990/core/bridge_bmf/state=MT/`
- `bronze/nccs_990/core/bridge_bmf/state=SD/`
- `bronze/nccs_990/core/metadata/`
- `bronze/nccs_990/core/raw/`
- `bronze/nccs_990/core/raw/year=2022/`
- `bronze/nccs_990/postcard/`
- `bronze/nccs_990/postcard/metadata/`
- `bronze/nccs_990/postcard/raw/`
- `bronze/nccs_990/postcard/raw/snapshot_year=2026/`
- `bronze/nccs_990/postcard/raw/snapshot_year=2026/snapshot_month=2026-01/`
- `bronze/nccs_990/postcard/raw/snapshot_year=2026/snapshot_month=2026-02/`
- `bronze/nccs_990/postcard/raw/snapshot_year=2026/snapshot_month=2026-03/`
- `bronze/nccs_bmf/`
- `bronze/nccs_bmf/metadata/`
- `bronze/nccs_bmf/raw/`
- `bronze/nccs_bmf/raw/year=2022/`
- `bronze/nccs_bmf/raw/year=2023/`
- `bronze/nccs_bmf/raw/year=2024/`
- `bronze/nccs_bmf/raw/year=2025/`
- `bronze/nccs_bmf/raw/year=2026/`
- `bronze/nccs_efile/`
- `bronze/nccs_efile/metadata/`
- `bronze/nccs_efile/raw/`
- `bronze/nccs_efile/raw/tax_year=2022/`
- `bronze/nccs_efile/raw/tax_year=2022/table=F9-P00-T00-HEADER/`
- `bronze/nccs_efile/raw/tax_year=2022/table=F9-P01-T00-SUMMARY/`
- `bronze/nccs_efile/raw/tax_year=2022/table=SA-P01-T00-PUBLIC-CHARITY-STATUS/`
- `bronze/nccs_efile/raw/tax_year=2023/`
- `bronze/nccs_efile/raw/tax_year=2023/table=F9-P00-T00-HEADER/`
- `bronze/nccs_efile/raw/tax_year=2023/table=F9-P01-T00-SUMMARY/`
- `bronze/nccs_efile/raw/tax_year=2023/table=SA-P01-T00-PUBLIC-CHARITY-STATUS/`
- `bronze/nccs_efile/raw/tax_year=2024/`
- `bronze/nccs_efile/raw/tax_year=2024/table=F9-P00-T00-HEADER/`
- `bronze/nccs_efile/raw/tax_year=2024/table=F9-P01-T00-SUMMARY/`
- `bronze/nccs_efile/raw/tax_year=2024/table=SA-P01-T00-PUBLIC-CHARITY-STATUS/`
- `silver/`
- `silver/combined_990/`
- `silver/combined_990/metadata/`
- `silver/givingtuesday_990/`
- `silver/givingtuesday_990/filing/`
- `silver/irs990/`
- `silver/irs990/bmf/`
- `silver/irs_soi/`
- `silver/irs_soi/county/`
- `silver/irs_soi/county/tax_year=2022/`
- `silver/nccs_990/`
- `silver/nccs_990/core/`
- `silver/nccs_990/core/year=2022/`
- `silver/nccs_990/postcard/`
- `silver/nccs_990/postcard/snapshot_year=2026/`
- `silver/nccs_bmf/`
- `silver/nccs_bmf/metadata/`
- `silver/nccs_bmf/year=2022/`
- `silver/nccs_bmf/year=2023/`
- `silver/nccs_bmf/year=2024/`
- `silver/nccs_bmf/year=2025/`
- `silver/nccs_bmf/year=2026/`
- `silver/nccs_efile/`
- `silver/nccs_efile/comparison/`
- `silver/nccs_efile/comparison/tax_year=2022/`
- `silver/nccs_efile/tax_year=2022/`
- `silver/nccs_efile/tax_year=2023/`
- `silver/nccs_efile/tax_year=2024/`

## Object Inventory


| Key                                                                                                                    | Size (bytes)                                                | Last modified (UTC)       |
| ---------------------------------------------------------------------------------------------------------------------- | ----------------------------------------------------------- | ------------------------- |
| `bronze/givingtuesday_990/datamarts/metadata/datamart_catalog.csv`                                                     | 14798                                                       | 2026-03-18T19:11:07+00:00 |
| `bronze/givingtuesday_990/datamarts/metadata/datamart_catalog.md`                                                      | 14196                                                       | 2026-03-18T19:11:08+00:00 |
| `bronze/givingtuesday_990/datamarts/metadata/datamart_catalog_raw.json`                                                | 42672                                                       | 2026-03-18T19:11:09+00:00 |
| `bronze/givingtuesday_990/datamarts/metadata/datamart_fields.csv`                                                      | 238445                                                      | 2026-03-18T19:11:10+00:00 |
| `bronze/givingtuesday_990/datamarts/metadata/datamart_fields.md`                                                       | 157412                                                      | 2026-03-18T19:11:11+00:00 |
| `bronze/givingtuesday_990/datamarts/metadata/required_datasets_manifest.csv`                                           | 1917                                                        | 2026-03-18T19:11:12+00:00 |
| `bronze/givingtuesday_990/datamarts/metadata/size_verification_report.csv`                                             | 2461                                                        | 2026-03-18T19:41:29+00:00 |
| `bronze/givingtuesday_990/datamarts/raw/2025_07_10_All_Years_990_990ez_990pf_990n_Combined_DataMart.csv`               | 2140103678                                                  | 2026-03-18T19:11:13+00:00 |
| `bronze/givingtuesday_990/datamarts/raw/2025_08_29_All_Years_990PFStandardFields.csv`                                  | 677317763                                                   | 2026-03-18T19:20:03+00:00 |
| `bronze/givingtuesday_990/datamarts/raw/2025_10_18_All_Years_990StandardFields.csv`                                    | 2302094678                                                  | 2026-03-18T19:22:48+00:00 |
| `bronze/givingtuesday_990/datamarts/raw/2025_10_28_All_Years_990EZStandardFields.csv`                                  | 1432510202                                                  | 2026-03-18T19:31:59+00:00 |
| `bronze/givingtuesday_990/datamarts/presilver/givingtuesday_990_basic_allforms_presilver.parquet`                      | historical-pre-rename object not re-listed in this snapshot | renamed after 2026-03-22  |
| `bronze/givingtuesday_990/datamarts/presilver/givingtuesday_990_basic_plus_combined_presilver.parquet`                 | historical-pre-rename object not re-listed in this snapshot | renamed after 2026-03-22  |
| `bronze/irs990/bmf/eo_az.csv`                                                                                          | 5259463                                                     | 2026-03-03T04:00:14+00:00 |
| `bronze/irs990/bmf/eo_mn.csv`                                                                                          | 6863374                                                     | 2026-03-03T03:59:31+00:00 |
| `bronze/irs990/bmf/eo_mt.csv`                                                                                          | 2082762                                                     | 2026-03-03T04:00:00+00:00 |
| `bronze/irs990/bmf/eo_sd.csv`                                                                                          | 1252794                                                     | 2026-03-03T03:59:18+00:00 |
| `bronze/irs990/teos_xml/code/python.zip`                                                                               | 105604                                                      | 2026-02-27T22:14:53+00:00 |
| `bronze/irs990/teos_xml/index/year=2021/.keep`                                                                         | 0                                                           | 2026-02-22T22:44:36+00:00 |
| `bronze/irs990/teos_xml/index/year=2021/index_2021.csv`                                                                | 74764447                                                    | 2026-03-03T04:13:36+00:00 |
| `bronze/irs990/teos_xml/index/year=2022/.keep`                                                                         | 0                                                           | 2026-02-22T22:44:40+00:00 |
| `bronze/irs990/teos_xml/index/year=2022/index_2022.csv`                                                                | 72228246                                                    | 2026-03-03T04:13:23+00:00 |
| `bronze/irs990/teos_xml/index/year=2023/.keep`                                                                         | 0                                                           | 2026-02-22T22:44:43+00:00 |
| `bronze/irs990/teos_xml/index/year=2023/index_2023.csv`                                                                | 77519435                                                    | 2026-03-03T04:13:31+00:00 |
| `bronze/irs990/teos_xml/index/year=2024/.keep`                                                                         | 0                                                           | 2026-02-22T22:44:46+00:00 |
| `bronze/irs990/teos_xml/index/year=2024/index_2024.csv`                                                                | 91056866                                                    | 2026-03-03T04:13:39+00:00 |
| `bronze/irs990/teos_xml/index/year=2025/.keep`                                                                         | 0                                                           | 2026-02-22T22:44:50+00:00 |
| `bronze/irs990/teos_xml/index/year=2025/index_2025.csv`                                                                | 93053080                                                    | 2026-03-03T04:29:39+00:00 |
| `bronze/irs990/teos_xml/index/year=2026/index_2026.csv`                                                                | 1474371                                                     | 2026-03-03T04:31:46+00:00 |
| `bronze/irs990/teos_xml/zips/year=2021/.keep`                                                                          | 0                                                           | 2026-02-22T22:44:38+00:00 |
| `bronze/irs990/teos_xml/zips/year=2021/2021_TEOS_XML_01A.zip`                                                          | 3715545729                                                  | 2026-03-04T03:28:19+00:00 |
| `bronze/irs990/teos_xml/zips/year=2021/2021_TEOS_XML_17903370.zip`                                                     | 1917                                                        | 2026-02-23T04:50:36+00:00 |
| `bronze/irs990/teos_xml/zips/year=2021/2021_TEOS_XML_17903377.zip`                                                     | 1917                                                        | 2026-02-23T04:50:49+00:00 |
| `bronze/irs990/teos_xml/zips/year=2021/2021_TEOS_XML_17903389.zip`                                                     | 1917                                                        | 2026-02-23T04:51:10+00:00 |
| `bronze/irs990/teos_xml/zips/year=2021/2021_TEOS_XML_17903396.zip`                                                     | 1917                                                        | 2026-02-23T04:51:23+00:00 |
| `bronze/irs990/teos_xml/zips/year=2021/2021_TEOS_XML_17903401.zip`                                                     | 1917                                                        | 2026-02-23T04:51:33+00:00 |
| `bronze/irs990/teos_xml/zips/year=2021/2021_TEOS_XML_17903405.zip`                                                     | 1917                                                        | 2026-02-23T04:51:41+00:00 |
| `bronze/irs990/teos_xml/zips/year=2021/2021_TEOS_XML_17903435.zip`                                                     | 1917                                                        | 2026-02-23T04:52:33+00:00 |
| `bronze/irs990/teos_xml/zips/year=2021/2021_TEOS_XML_17903453.zip`                                                     | 1917                                                        | 2026-02-23T04:52:58+00:00 |
| `bronze/irs990/teos_xml/zips/year=2021/2021_TEOS_XML_17903479.zip`                                                     | 1917                                                        | 2026-02-23T04:53:07+00:00 |
| `bronze/irs990/teos_xml/zips/year=2021/2021_TEOS_XML_17903511.zip`                                                     | 1917                                                        | 2026-02-23T04:54:03+00:00 |
| `bronze/irs990/teos_xml/zips/year=2021/2021_TEOS_XML_17903533.zip`                                                     | 1917                                                        | 2026-02-23T04:54:43+00:00 |
| `bronze/irs990/teos_xml/zips/year=2021/2021_TEOS_XML_17903543.zip`                                                     | 1917                                                        | 2026-02-23T04:55:01+00:00 |
| `bronze/irs990/teos_xml/zips/year=2021/2021_TEOS_XML_17903556.zip`                                                     | 1917                                                        | 2026-02-23T04:55:23+00:00 |
| `bronze/irs990/teos_xml/zips/year=2021/2021_TEOS_XML_17903566.zip`                                                     | 1917                                                        | 2026-02-23T04:55:41+00:00 |
| `bronze/irs990/teos_xml/zips/year=2021/2021_TEOS_XML_17903572.zip`                                                     | 1917                                                        | 2026-02-23T04:55:52+00:00 |
| `bronze/irs990/teos_xml/zips/year=2021/2021_TEOS_XML_17903578.zip`                                                     | 1917                                                        | 2026-02-23T04:56:04+00:00 |
| `bronze/irs990/teos_xml/zips/year=2021/2021_TEOS_XML_17903583.zip`                                                     | 1917                                                        | 2026-02-23T04:56:14+00:00 |
| `bronze/irs990/teos_xml/zips/year=2021/2021_TEOS_XML_17903590.zip`                                                     | 1917                                                        | 2026-02-23T04:56:26+00:00 |
| `bronze/irs990/teos_xml/zips/year=2021/2021_TEOS_XML_17903593.zip`                                                     | 1917                                                        | 2026-02-23T04:56:33+00:00 |
| `bronze/irs990/teos_xml/zips/year=2021/2021_TEOS_XML_17903597.zip`                                                     | 1917                                                        | 2026-02-23T04:56:41+00:00 |
| `bronze/irs990/teos_xml/zips/year=2021/2021_TEOS_XML_17903610.zip`                                                     | 1917                                                        | 2026-02-23T04:57:05+00:00 |
| `bronze/irs990/teos_xml/zips/year=2021/2021_TEOS_XML_17903612.zip`                                                     | 1917                                                        | 2026-02-23T04:57:09+00:00 |
| `bronze/irs990/teos_xml/zips/year=2021/2021_TEOS_XML_17903622.zip`                                                     | 1917                                                        | 2026-02-23T04:57:27+00:00 |
| `bronze/irs990/teos_xml/zips/year=2021/2021_TEOS_XML_17903624.zip`                                                     | 1917                                                        | 2026-02-23T04:57:32+00:00 |
| `bronze/irs990/teos_xml/zips/year=2021/2021_TEOS_XML_17903631.zip`                                                     | 1917                                                        | 2026-02-23T04:57:46+00:00 |
| `bronze/irs990/teos_xml/zips/year=2021/2021_TEOS_XML_17903645.zip`                                                     | 1917                                                        | 2026-02-23T04:58:11+00:00 |
| `bronze/irs990/teos_xml/zips/year=2021/2021_TEOS_XML_17903646.zip`                                                     | 1917                                                        | 2026-02-23T04:58:14+00:00 |
| `bronze/irs990/teos_xml/zips/year=2021/2021_TEOS_XML_17903654.zip`                                                     | 1917                                                        | 2026-02-23T04:58:28+00:00 |
| `bronze/irs990/teos_xml/zips/year=2021/2021_TEOS_XML_17906754.zip`                                                     | 1917                                                        | 2026-02-23T06:24:00+00:00 |
| `bronze/irs990/teos_xml/zips/year=2021/2021_TEOS_XML_17906755.zip`                                                     | 1917                                                        | 2026-02-23T06:24:02+00:00 |
| `bronze/irs990/teos_xml/zips/year=2021/2021_TEOS_XML_17906760.zip`                                                     | 1917                                                        | 2026-02-23T06:24:11+00:00 |
| `bronze/irs990/teos_xml/zips/year=2021/2021_TEOS_XML_17906763.zip`                                                     | 1917                                                        | 2026-02-23T06:24:18+00:00 |
| `bronze/irs990/teos_xml/zips/year=2021/2021_TEOS_XML_17906769.zip`                                                     | 1917                                                        | 2026-02-23T06:24:29+00:00 |
| `bronze/irs990/teos_xml/zips/year=2021/2021_TEOS_XML_17906783.zip`                                                     | 1917                                                        | 2026-02-23T06:24:53+00:00 |
| `bronze/irs990/teos_xml/zips/year=2021/2021_TEOS_XML_17906784.zip`                                                     | 1917                                                        | 2026-02-23T06:24:55+00:00 |
| `bronze/irs990/teos_xml/zips/year=2021/2021_TEOS_XML_17906795.zip`                                                     | 1917                                                        | 2026-02-23T06:25:16+00:00 |
| `bronze/irs990/teos_xml/zips/year=2021/2021_TEOS_XML_17906803.zip`                                                     | 1917                                                        | 2026-02-23T06:25:30+00:00 |
| `bronze/irs990/teos_xml/zips/year=2021/2021_TEOS_XML_17906804.zip`                                                     | 1917                                                        | 2026-02-23T06:25:33+00:00 |
| `bronze/irs990/teos_xml/zips/year=2021/2021_TEOS_XML_17906805.zip`                                                     | 1917                                                        | 2026-02-23T06:25:35+00:00 |
| `bronze/irs990/teos_xml/zips/year=2021/2021_TEOS_XML_17906809.zip`                                                     | 1917                                                        | 2026-02-23T06:25:43+00:00 |
| `bronze/irs990/teos_xml/zips/year=2021/2021_TEOS_XML_17906813.zip`                                                     | 1917                                                        | 2026-02-23T06:25:50+00:00 |
| `bronze/irs990/teos_xml/zips/year=2021/2021_TEOS_XML_17906815.zip`                                                     | 1917                                                        | 2026-02-23T06:25:54+00:00 |
| `bronze/irs990/teos_xml/zips/year=2021/2021_TEOS_XML_17906822.zip`                                                     | 1917                                                        | 2026-02-23T06:26:06+00:00 |
| `bronze/irs990/teos_xml/zips/year=2021/2021_TEOS_XML_17906832.zip`                                                     | 1917                                                        | 2026-02-23T06:26:23+00:00 |
| `bronze/irs990/teos_xml/zips/year=2021/2021_TEOS_XML_17906833.zip`                                                     | 1917                                                        | 2026-02-23T06:26:26+00:00 |
| `bronze/irs990/teos_xml/zips/year=2021/2021_TEOS_XML_17906839.zip`                                                     | 1917                                                        | 2026-02-23T06:26:36+00:00 |
| `bronze/irs990/teos_xml/zips/year=2021/2021_TEOS_XML_17906841.zip`                                                     | 1917                                                        | 2026-02-23T06:26:41+00:00 |
| `bronze/irs990/teos_xml/zips/year=2021/2021_TEOS_XML_17906842.zip`                                                     | 1917                                                        | 2026-02-23T06:26:44+00:00 |
| `bronze/irs990/teos_xml/zips/year=2021/2021_TEOS_XML_17906845.zip`                                                     | 1917                                                        | 2026-02-23T06:26:50+00:00 |
| `bronze/irs990/teos_xml/zips/year=2021/2021_TEOS_XML_17906865.zip`                                                     | 1917                                                        | 2026-02-23T06:27:24+00:00 |
| `bronze/irs990/teos_xml/zips/year=2021/2021_TEOS_XML_17906871.zip`                                                     | 1917                                                        | 2026-02-23T06:27:36+00:00 |
| `bronze/irs990/teos_xml/zips/year=2021/2021_TEOS_XML_17921176.zip`                                                     | 1917                                                        | 2026-02-23T07:07:41+00:00 |
| `bronze/irs990/teos_xml/zips/year=2021/2021_TEOS_XML_17921180.zip`                                                     | 1917                                                        | 2026-02-23T07:07:48+00:00 |
| `bronze/irs990/teos_xml/zips/year=2021/2021_TEOS_XML_17921185.zip`                                                     | 1917                                                        | 2026-02-23T07:07:58+00:00 |
| `bronze/irs990/teos_xml/zips/year=2021/2021_TEOS_XML_17921188.zip`                                                     | 1917                                                        | 2026-02-23T07:08:04+00:00 |
| `bronze/irs990/teos_xml/zips/year=2021/2021_TEOS_XML_17921190.zip`                                                     | 1917                                                        | 2026-02-23T07:08:08+00:00 |
| `bronze/irs990/teos_xml/zips/year=2021/2021_TEOS_XML_17921191.zip`                                                     | 1917                                                        | 2026-02-23T07:08:10+00:00 |
| `bronze/irs990/teos_xml/zips/year=2021/2021_TEOS_XML_17921195.zip`                                                     | 1917                                                        | 2026-02-23T07:08:18+00:00 |
| `bronze/irs990/teos_xml/zips/year=2021/2021_TEOS_XML_17921196.zip`                                                     | 1917                                                        | 2026-02-23T07:08:21+00:00 |
| `bronze/irs990/teos_xml/zips/year=2021/2021_TEOS_XML_17921197.zip`                                                     | 1917                                                        | 2026-02-23T07:08:23+00:00 |
| `bronze/irs990/teos_xml/zips/year=2021/2021_TEOS_XML_17921200.zip`                                                     | 1917                                                        | 2026-02-23T07:08:30+00:00 |
| `bronze/irs990/teos_xml/zips/year=2021/2021_TEOS_XML_17921203.zip`                                                     | 1917                                                        | 2026-02-23T07:08:35+00:00 |
| `bronze/irs990/teos_xml/zips/year=2021/2021_TEOS_XML_17921223.zip`                                                     | 1917                                                        | 2026-02-23T07:09:09+00:00 |
| `bronze/irs990/teos_xml/zips/year=2021/2021_TEOS_XML_17921224.zip`                                                     | 1917                                                        | 2026-02-23T07:09:11+00:00 |
| `bronze/irs990/teos_xml/zips/year=2021/2021_TEOS_XML_17921230.zip`                                                     | 1917                                                        | 2026-02-23T07:09:22+00:00 |
| `bronze/irs990/teos_xml/zips/year=2021/2021_TEOS_XML_17921232.zip`                                                     | 1917                                                        | 2026-02-23T07:09:26+00:00 |
| `bronze/irs990/teos_xml/zips/year=2021/2021_TEOS_XML_17921235.zip`                                                     | 1917                                                        | 2026-02-23T07:09:31+00:00 |
| `bronze/irs990/teos_xml/zips/year=2021/2021_TEOS_XML_17921236.zip`                                                     | 1917                                                        | 2026-02-23T07:09:33+00:00 |
| `bronze/irs990/teos_xml/zips/year=2021/2021_TEOS_XML_17921238.zip`                                                     | 1917                                                        | 2026-02-23T07:09:37+00:00 |
| `bronze/irs990/teos_xml/zips/year=2021/2021_TEOS_XML_17921246.zip`                                                     | 1917                                                        | 2026-02-23T07:09:50+00:00 |
| `bronze/irs990/teos_xml/zips/year=2021/2021_TEOS_XML_17921268.zip`                                                     | 1917                                                        | 2026-02-23T07:10:26+00:00 |
| `bronze/irs990/teos_xml/zips/year=2021/2021_TEOS_XML_17921297.zip`                                                     | 1917                                                        | 2026-02-23T07:11:11+00:00 |
| `bronze/irs990/teos_xml/zips/year=2021/2021_TEOS_XML_17924491.zip`                                                     | 77485                                                       | 2026-02-23T08:31:15+00:00 |
| `bronze/irs990/teos_xml/zips/year=2021/2021_TEOS_XML_18177320.zip`                                                     | 1917                                                        | 2026-02-23T10:29:05+00:00 |
| `bronze/irs990/teos_xml/zips/year=2021/2021_TEOS_XML_18177366.zip`                                                     | 1917                                                        | 2026-02-23T10:30:20+00:00 |
| `bronze/irs990/teos_xml/zips/year=2021/2021_TEOS_XML_18177368.zip`                                                     | 1917                                                        | 2026-02-23T10:30:23+00:00 |
| `bronze/irs990/teos_xml/zips/year=2021/2021_TEOS_XML_18199786.zip`                                                     | 1917                                                        | 2026-02-23T16:20:35+00:00 |
| `bronze/irs990/teos_xml/zips/year=2021/2021_TEOS_XML_18199792.zip`                                                     | 1917                                                        | 2026-02-23T16:20:44+00:00 |
| `bronze/irs990/teos_xml/zips/year=2021/2021_TEOS_XML_18199796.zip`                                                     | 1917                                                        | 2026-02-23T16:20:51+00:00 |
| `bronze/irs990/teos_xml/zips/year=2021/2021_TEOS_XML_18199806.zip`                                                     | 1917                                                        | 2026-02-23T16:21:08+00:00 |
| `bronze/irs990/teos_xml/zips/year=2021/2021_TEOS_XML_18199983.zip`                                                     | 1917                                                        | 2026-02-23T16:25:32+00:00 |
| `bronze/irs990/teos_xml/zips/year=2021/2021_TEOS_XML_18199986.zip`                                                     | 1917                                                        | 2026-02-23T16:25:38+00:00 |
| `bronze/irs990/teos_xml/zips/year=2021/2021_TEOS_XML_18200019.zip`                                                     | 1917                                                        | 2026-02-23T16:26:32+00:00 |
| `bronze/irs990/teos_xml/zips/year=2021/2021_TEOS_XML_18200057.zip`                                                     | 1917                                                        | 2026-02-23T16:27:29+00:00 |
| `bronze/irs990/teos_xml/zips/year=2021/2021_TEOS_XML_18200060.zip`                                                     | 1917                                                        | 2026-02-23T16:27:35+00:00 |
| `bronze/irs990/teos_xml/zips/year=2021/2021_TEOS_XML_18200064.zip`                                                     | 1917                                                        | 2026-02-23T16:27:42+00:00 |
| `bronze/irs990/teos_xml/zips/year=2021/2021_TEOS_XML_18200067.zip`                                                     | 1917                                                        | 2026-02-23T16:27:48+00:00 |
| `bronze/irs990/teos_xml/zips/year=2021/2021_TEOS_XML_18200075.zip`                                                     | 1917                                                        | 2026-02-23T16:28:02+00:00 |
| `bronze/irs990/teos_xml/zips/year=2021/2021_TEOS_XML_18200078.zip`                                                     | 1917                                                        | 2026-02-23T16:28:08+00:00 |
| `bronze/irs990/teos_xml/zips/year=2021/2021_TEOS_XML_18200087.zip`                                                     | 1917                                                        | 2026-02-23T16:28:24+00:00 |
| `bronze/irs990/teos_xml/zips/year=2021/2021_TEOS_XML_18200092.zip`                                                     | 1917                                                        | 2026-02-23T16:28:34+00:00 |
| `bronze/irs990/teos_xml/zips/year=2021/2021_TEOS_XML_18200093.zip`                                                     | 1917                                                        | 2026-02-23T16:28:37+00:00 |
| `bronze/irs990/teos_xml/zips/year=2021/2021_TEOS_XML_18200096.zip`                                                     | 1917                                                        | 2026-02-23T16:28:42+00:00 |
| `bronze/irs990/teos_xml/zips/year=2021/2021_TEOS_XML_18200099.zip`                                                     | 1917                                                        | 2026-02-23T16:28:48+00:00 |
| `bronze/irs990/teos_xml/zips/year=2021/2021_TEOS_XML_18200104.zip`                                                     | 1917                                                        | 2026-02-23T16:28:57+00:00 |
| `bronze/irs990/teos_xml/zips/year=2021/2021_TEOS_XML_18200108.zip`                                                     | 1917                                                        | 2026-02-23T16:29:05+00:00 |
| `bronze/irs990/teos_xml/zips/year=2021/2021_TEOS_XML_18200110.zip`                                                     | 1917                                                        | 2026-02-23T16:29:10+00:00 |
| `bronze/irs990/teos_xml/zips/year=2021/2021_TEOS_XML_18200112.zip`                                                     | 1917                                                        | 2026-02-23T16:29:14+00:00 |
| `bronze/irs990/teos_xml/zips/year=2021/2021_TEOS_XML_18200114.zip`                                                     | 1917                                                        | 2026-02-23T16:29:18+00:00 |
| `bronze/irs990/teos_xml/zips/year=2021/2021_TEOS_XML_18200117.zip`                                                     | 1917                                                        | 2026-02-23T16:29:24+00:00 |
| `bronze/irs990/teos_xml/zips/year=2021/2021_TEOS_XML_18200120.zip`                                                     | 1917                                                        | 2026-02-23T16:29:29+00:00 |
| `bronze/irs990/teos_xml/zips/year=2021/2021_TEOS_XML_18200122.zip`                                                     | 1917                                                        | 2026-02-23T16:29:34+00:00 |
| `bronze/irs990/teos_xml/zips/year=2021/2021_TEOS_XML_18200126.zip`                                                     | 1917                                                        | 2026-02-23T16:29:41+00:00 |
| `bronze/irs990/teos_xml/zips/year=2021/2021_TEOS_XML_18200141.zip`                                                     | 1917                                                        | 2026-02-23T16:30:06+00:00 |
| `bronze/irs990/teos_xml/zips/year=2021/2021_TEOS_XML_18200149.zip`                                                     | 1917                                                        | 2026-02-23T16:30:20+00:00 |
| `bronze/irs990/teos_xml/zips/year=2021/2021_TEOS_XML_18200154.zip`                                                     | 1917                                                        | 2026-02-23T16:30:28+00:00 |
| `bronze/irs990/teos_xml/zips/year=2021/2021_TEOS_XML_18200156.zip`                                                     | 1917                                                        | 2026-02-23T16:30:32+00:00 |
| `bronze/irs990/teos_xml/zips/year=2021/2021_TEOS_XML_18200163.zip`                                                     | 1917                                                        | 2026-02-23T16:30:45+00:00 |
| `bronze/irs990/teos_xml/zips/year=2021/2021_TEOS_XML_18200405.zip`                                                     | 1917                                                        | 2026-02-23T16:35:41+00:00 |
| `bronze/irs990/teos_xml/zips/year=2021/2021_TEOS_XML_18200408.zip`                                                     | 1917                                                        | 2026-02-23T16:35:48+00:00 |
| `bronze/irs990/teos_xml/zips/year=2021/2021_TEOS_XML_18200442.zip`                                                     | 1917                                                        | 2026-02-23T16:36:40+00:00 |
| `bronze/irs990/teos_xml/zips/year=2021/2021_TEOS_XML_18200447.zip`                                                     | 1917                                                        | 2026-02-23T16:36:50+00:00 |
| `bronze/irs990/teos_xml/zips/year=2021/2021_TEOS_XML_18200560.zip`                                                     | 1917                                                        | 2026-02-23T16:39:47+00:00 |
| `bronze/irs990/teos_xml/zips/year=2021/2021_TEOS_XML_18200561.zip`                                                     | 1917                                                        | 2026-02-23T16:39:50+00:00 |
| `bronze/irs990/teos_xml/zips/year=2021/2021_TEOS_XML_18200562.zip`                                                     | 1917                                                        | 2026-02-23T16:39:52+00:00 |
| `bronze/irs990/teos_xml/zips/year=2021/2021_TEOS_XML_18200566.zip`                                                     | 1917                                                        | 2026-02-23T16:39:59+00:00 |
| `bronze/irs990/teos_xml/zips/year=2021/2021_TEOS_XML_18200576.zip`                                                     | 1917                                                        | 2026-02-23T16:40:16+00:00 |
| `bronze/irs990/teos_xml/zips/year=2021/2021_TEOS_XML_18200577.zip`                                                     | 1917                                                        | 2026-02-23T16:40:18+00:00 |
| `bronze/irs990/teos_xml/zips/year=2021/2021_TEOS_XML_18200588.zip`                                                     | 1917                                                        | 2026-02-23T16:40:38+00:00 |
| `bronze/irs990/teos_xml/zips/year=2022/.keep`                                                                          | 0                                                           | 2026-02-22T22:44:41+00:00 |
| `bronze/irs990/teos_xml/zips/year=2022/2022_TEOS_XML_01A.zip`                                                          | 2603666925                                                  | 2026-03-04T03:11:30+00:00 |
| `bronze/irs990/teos_xml/zips/year=2022/2022_TEOS_XML_02A.zip`                                                          | 1408196885                                                  | 2026-03-04T01:08:09+00:00 |
| `bronze/irs990/teos_xml/zips/year=2023/.keep`                                                                          | 0                                                           | 2026-02-22T22:44:45+00:00 |
| `bronze/irs990/teos_xml/zips/year=2023/2023_TEOS_XML_01A.zip`                                                          | 123052879                                                   | 2026-03-03T21:39:36+00:00 |
| `bronze/irs990/teos_xml/zips/year=2023/2023_TEOS_XML_02A.zip`                                                          | 234617123                                                   | 2026-03-03T21:59:25+00:00 |
| `bronze/irs990/teos_xml/zips/year=2023/2023_TEOS_XML_03A.zip`                                                          | 197165153                                                   | 2026-03-03T21:46:01+00:00 |
| `bronze/irs990/teos_xml/zips/year=2023/2023_TEOS_XML_04A.zip`                                                          | 244491049                                                   | 2026-03-03T22:25:51+00:00 |
| `bronze/irs990/teos_xml/zips/year=2023/2023_TEOS_XML_05A.zip`                                                          | 990540072                                                   | 2026-03-04T00:55:57+00:00 |
| `bronze/irs990/teos_xml/zips/year=2023/2023_TEOS_XML_06A.zip`                                                          | 163716234                                                   | 2026-03-03T22:44:09+00:00 |
| `bronze/irs990/teos_xml/zips/year=2023/2023_TEOS_XML_07A.zip`                                                          | 219928555                                                   | 2026-03-03T23:26:50+00:00 |
| `bronze/irs990/teos_xml/zips/year=2023/2023_TEOS_XML_08A.zip`                                                          | 266247548                                                   | 2026-03-03T23:47:21+00:00 |
| `bronze/irs990/teos_xml/zips/year=2023/2023_TEOS_XML_09A.zip`                                                          | 200422523                                                   | 2026-03-04T00:23:50+00:00 |
| `bronze/irs990/teos_xml/zips/year=2023/2023_TEOS_XML_10A.zip`                                                          | 332505047                                                   | 2026-03-04T01:09:33+00:00 |
| `bronze/irs990/teos_xml/zips/year=2023/2023_TEOS_XML_11A.zip`                                                          | 1200256933                                                  | 2026-03-04T02:40:33+00:00 |
| `bronze/irs990/teos_xml/zips/year=2023/2023_TEOS_XML_12A.zip`                                                          | 120729513                                                   | 2026-03-04T02:27:27+00:00 |
| `bronze/irs990/teos_xml/zips/year=2024/.keep`                                                                          | 0                                                           | 2026-02-22T22:44:48+00:00 |
| `bronze/irs990/teos_xml/zips/year=2024/2024_TEOS_XML_01A.zip`                                                          | 104816571                                                   | 2026-03-04T03:05:06+00:00 |
| `bronze/irs990/teos_xml/zips/year=2024/2024_TEOS_XML_02A.zip`                                                          | 243689721                                                   | 2026-03-04T03:46:19+00:00 |
| `bronze/irs990/teos_xml/zips/year=2024/2024_TEOS_XML_03A.zip`                                                          | 209335079                                                   | 2026-03-04T04:15:05+00:00 |
| `bronze/irs990/teos_xml/zips/year=2024/2024_TEOS_XML_04A.zip`                                                          | 350268985                                                   | 2026-03-04T05:20:34+00:00 |
| `bronze/irs990/teos_xml/zips/year=2024/2024_TEOS_XML_05A.zip`                                                          | 977460330                                                   | 2026-03-04T06:02:55+00:00 |
| `bronze/irs990/teos_xml/zips/year=2024/2024_TEOS_XML_06A.zip`                                                          | 163872638                                                   | 2026-03-04T06:47:48+00:00 |
| `bronze/irs990/teos_xml/zips/year=2024/2024_TEOS_XML_07A.zip`                                                          | 288252859                                                   | 2026-03-04T07:06:58+00:00 |
| `bronze/irs990/teos_xml/zips/year=2024/2024_TEOS_XML_08A.zip`                                                          | 260603085                                                   | 2026-03-04T07:38:04+00:00 |
| `bronze/irs990/teos_xml/zips/year=2024/2024_TEOS_XML_09A.zip`                                                          | 247036759                                                   | 2026-03-04T08:16:53+00:00 |
| `bronze/irs990/teos_xml/zips/year=2024/2024_TEOS_XML_10A.zip`                                                          | 251614735                                                   | 2026-03-04T08:37:38+00:00 |
| `bronze/irs990/teos_xml/zips/year=2024/2024_TEOS_XML_11A.zip`                                                          | 1165636963                                                  | 2026-03-04T08:55:43+00:00 |
| `bronze/irs990/teos_xml/zips/year=2024/2024_TEOS_XML_12A.zip`                                                          | 128322545                                                   | 2026-03-04T09:14:06+00:00 |
| `bronze/irs990/teos_xml/zips/year=2025/.keep`                                                                          | 0                                                           | 2026-02-22T22:44:51+00:00 |
| `bronze/irs990/teos_xml/zips/year=2025/2025_TEOS_XML_01A.zip`                                                          | 103669610                                                   | 2026-03-04T09:40:50+00:00 |
| `bronze/irs990/teos_xml/zips/year=2025/2025_TEOS_XML_02A.zip`                                                          | 239929743                                                   | 2026-03-04T09:48:38+00:00 |
| `bronze/irs990/teos_xml/zips/year=2025/2025_TEOS_XML_03A.zip`                                                          | 222596630                                                   | 2026-03-04T10:05:48+00:00 |
| `bronze/irs990/teos_xml/zips/year=2025/2025_TEOS_XML_04A.zip`                                                          | 338249494                                                   | 2026-03-04T10:35:35+00:00 |
| `bronze/irs990/teos_xml/zips/year=2025/2025_TEOS_XML_05A.zip`                                                          | 495924982                                                   | 2026-03-04T11:07:50+00:00 |
| `bronze/irs990/teos_xml/zips/year=2025/2025_TEOS_XML_05B.zip`                                                          | 501021178                                                   | 2026-03-04T11:09:12+00:00 |
| `bronze/irs990/teos_xml/zips/year=2025/2025_TEOS_XML_06A.zip`                                                          | 246788129                                                   | 2026-03-04T12:02:02+00:00 |
| `bronze/irs990/teos_xml/zips/year=2025/2025_TEOS_XML_07A.zip`                                                          | 143450588                                                   | 2026-03-04T12:56:52+00:00 |
| `bronze/irs990/teos_xml/zips/year=2025/2025_TEOS_XML_08A.zip`                                                          | 262744207                                                   | 2026-03-04T13:02:26+00:00 |
| `bronze/irs990/teos_xml/zips/year=2025/2025_TEOS_XML_09A.zip`                                                          | 256276809                                                   | 2026-03-04T13:11:53+00:00 |
| `bronze/irs990/teos_xml/zips/year=2025/2025_TEOS_XML_10A.zip`                                                          | 56507372                                                    | 2026-03-04T13:33:23+00:00 |
| `bronze/irs990/teos_xml/zips/year=2025/2025_TEOS_XML_11A.zip`                                                          | 340137018                                                   | 2026-03-04T13:51:14+00:00 |
| `bronze/irs990/teos_xml/zips/year=2025/2025_TEOS_XML_11B.zip`                                                          | 494528498                                                   | 2026-03-04T14:09:16+00:00 |
| `bronze/irs990/teos_xml/zips/year=2025/2025_TEOS_XML_11C.zip`                                                          | 277886481                                                   | 2026-03-04T14:16:38+00:00 |
| `bronze/irs990/teos_xml/zips/year=2025/2025_TEOS_XML_11D.zip`                                                          | 332775121                                                   | 2026-03-04T14:20:17+00:00 |
| `bronze/irs990/teos_xml/zips/year=2025/2025_TEOS_XML_12A.zip`                                                          | 151835020                                                   | 2026-03-04T14:36:29+00:00 |
| `bronze/irs990/teos_xml/zips/year=2026/2026_TEOS_XML_01A.zip`                                                          | 71497607                                                    | 2026-03-04T15:08:37+00:00 |
| `bronze/irs_soi/county/metadata/latest_release.json`                                                                   | 1721                                                        | 2026-03-20T18:06:14+00:00 |
| `bronze/irs_soi/county/metadata/release_manifest_tax_year=2022.csv`                                                    | 1520                                                        | 2026-03-20T18:06:15+00:00 |
| `bronze/irs_soi/county/metadata/size_verification_tax_year=2022.csv`                                                   | 1552                                                        | 2026-03-20T18:07:25+00:00 |
| `bronze/irs_soi/county/raw/tax_year=2022/22incyallagi.csv`                                                             | 35567992                                                    | 2026-03-20T18:01:24+00:00 |
| `bronze/irs_soi/county/raw/tax_year=2022/22incyallnoagi.csv`                                                           | 5075247                                                     | 2026-03-20T18:05:34+00:00 |
| `bronze/irs_soi/county/raw/tax_year=2022/22incydocguide.docx`                                                          | 47920                                                       | 2026-03-20T18:06:13+00:00 |
| `bronze/nccs_990/core/bridge_bmf/state=AZ/AZ_BMF_V1.1.csv`                                                             | 21127297                                                    | 2026-03-20T22:43:06+00:00 |
| `bronze/nccs_990/core/bridge_bmf/state=MN/MN_BMF_V1.1.csv`                                                             | 28598894                                                    | 2026-03-20T22:43:19+00:00 |
| `bronze/nccs_990/core/bridge_bmf/state=MT/MT_BMF_V1.1.csv`                                                             | 7297506                                                     | 2026-03-20T22:43:32+00:00 |
| `bronze/nccs_990/core/bridge_bmf/state=SD/SD_BMF_V1.1.csv`                                                             | 5225014                                                     | 2026-03-20T22:43:44+00:00 |
| `bronze/nccs_990/core/metadata/CORE-HRMN_dd.csv`                                                                       | 149914                                                      | 2026-03-20T22:43:00+00:00 |
| `bronze/nccs_990/core/metadata/DD-PF-HRMN-V0.csv`                                                                      | 60919                                                       | 2026-03-20T22:43:02+00:00 |
| `bronze/nccs_990/core/metadata/catalog_bmf.html`                                                                       | 94214                                                       | 2026-03-20T22:43:59+00:00 |
| `bronze/nccs_990/core/metadata/catalog_core.html`                                                                      | 72527                                                       | 2026-03-20T22:43:57+00:00 |
| `bronze/nccs_990/core/metadata/harmonized_data_dictionary.xlsx`                                                        | 68223                                                       | 2026-03-20T22:43:04+00:00 |
| `bronze/nccs_990/core/metadata/latest_release.json`                                                                    | 11904                                                       | 2026-03-20T22:43:55+00:00 |
| `bronze/nccs_990/core/metadata/release_manifest_year=2022.csv`                                                         | 6014                                                        | 2026-03-20T22:44:01+00:00 |
| `bronze/nccs_990/core/metadata/size_verification_year=2022.csv`                                                        | 6147                                                        | 2026-03-20T22:44:29+00:00 |
| `bronze/nccs_990/core/raw/year=2022/CORE-2022-501C3-CHARITIES-PC-HRMN.csv`                                             | 19238005                                                    | 2026-03-20T22:42:13+00:00 |
| `bronze/nccs_990/core/raw/year=2022/CORE-2022-501C3-CHARITIES-PZ-HRMN.csv`                                             | 33236336                                                    | 2026-03-20T22:41:57+00:00 |
| `bronze/nccs_990/core/raw/year=2022/CORE-2022-501C3-PRIVFOUND-PF-HRMN-V0.csv`                                          | 60221148                                                    | 2026-03-20T22:42:41+00:00 |
| `bronze/nccs_990/core/raw/year=2022/CORE-2022-501CE-NONPROFIT-PC-HRMN.csv`                                             | 7597326                                                     | 2026-03-20T22:42:35+00:00 |
| `bronze/nccs_990/core/raw/year=2022/CORE-2022-501CE-NONPROFIT-PZ-HRMN.csv`                                             | 12469680                                                    | 2026-03-20T22:42:26+00:00 |
| `bronze/nccs_990/postcard/metadata/latest_release.json`                                                                | 3234                                                        | 2026-03-21T03:07:18+00:00 |
| `bronze/nccs_990/postcard/metadata/postcard_page.html`                                                                 | 59797                                                       | 2026-03-21T03:07:19+00:00 |
| `bronze/nccs_990/postcard/metadata/release_manifest_snapshot_year=2026.csv`                                            | 1763                                                        | 2026-03-21T03:07:20+00:00 |
| `bronze/nccs_990/postcard/metadata/size_verification_snapshot_year=2026.csv`                                           | 1802                                                        | 2026-03-22T22:48:25+00:00 |
| `bronze/nccs_990/postcard/raw/snapshot_year=2026/snapshot_month=2026-01/2026-01-E-POSTCARD.csv`                        | 377496069                                                   | 2026-03-21T03:05:12+00:00 |
| `bronze/nccs_990/postcard/raw/snapshot_year=2026/snapshot_month=2026-02/2026-02-E-POSTCARD.csv`                        | 377607602                                                   | 2026-03-21T03:05:12+00:00 |
| `bronze/nccs_990/postcard/raw/snapshot_year=2026/snapshot_month=2026-03/2026-03-E-POSTCARD.csv`                        | 379901679                                                   | 2026-03-21T03:07:16+00:00 |
| `bronze/nccs_bmf/metadata/catalog_bmf.html`                                                                            | 94214                                                       | 2026-03-22T03:50:39+00:00 |
| `bronze/nccs_bmf/metadata/dataset_bmf.html`                                                                            | 47744                                                       | 2026-03-22T03:50:37+00:00 |
| `bronze/nccs_bmf/metadata/latest_release.json`                                                                         | 5117                                                        | 2026-03-22T03:50:35+00:00 |
| `bronze/nccs_bmf/metadata/release_manifest_start_year=2022.csv`                                                        | 2422                                                        | 2026-03-22T03:50:41+00:00 |
| `bronze/nccs_bmf/metadata/size_verification_start_year=2022.csv`                                                       | 2487                                                        | 2026-03-22T03:50:54+00:00 |
| `bronze/nccs_bmf/raw/year=2022/BMF-2022-08-501CX-NONPROFIT-PX.csv`                                                     | 345836470                                                   | 2026-03-22T03:45:03+00:00 |
| `bronze/nccs_bmf/raw/year=2023/2023-12-BMF.csv`                                                                        | 386754648                                                   | 2026-03-22T03:46:09+00:00 |
| `bronze/nccs_bmf/raw/year=2024/2024-12-BMF.csv`                                                                        | 391222247                                                   | 2026-03-22T03:47:18+00:00 |
| `bronze/nccs_bmf/raw/year=2025/2025-12-BMF.csv`                                                                        | 400623508                                                   | 2026-03-22T03:48:21+00:00 |
| `bronze/nccs_bmf/raw/year=2026/2026-03-BMF.csv`                                                                        | 403208276                                                   | 2026-03-22T03:49:26+00:00 |
| `bronze/nccs_efile/metadata/catalog_efile.html`                                                                        | 809444                                                      | 2026-03-22T22:41:10+00:00 |
| `bronze/nccs_efile/metadata/dataset_efile.html`                                                                        | 45571                                                       | 2026-03-22T22:41:09+00:00 |
| `bronze/nccs_efile/metadata/latest_release.json`                                                                       | 9063                                                        | 2026-03-22T22:41:08+00:00 |
| `bronze/nccs_efile/metadata/release_manifest_start_year=2022.csv`                                                      | 5313                                                        | 2026-03-22T22:41:12+00:00 |
| `bronze/nccs_efile/metadata/size_verification_start_year=2022.csv`                                                     | 5427                                                        | 2026-03-22T22:45:15+00:00 |
| `bronze/nccs_efile/raw/tax_year=2022/table=F9-P00-T00-HEADER/F9-P00-T00-HEADER-2022.CSV`                               | 353293716                                                   | 2026-03-22T22:34:52+00:00 |
| `bronze/nccs_efile/raw/tax_year=2022/table=F9-P01-T00-SUMMARY/F9-P01-T00-SUMMARY-2022.CSV`                             | 300076556                                                   | 2026-03-22T22:36:21+00:00 |
| `bronze/nccs_efile/raw/tax_year=2022/table=SA-P01-T00-PUBLIC-CHARITY-STATUS/SA-P01-T00-PUBLIC-CHARITY-STATUS-2022.CSV` | 144514771                                                   | 2026-03-22T22:37:30+00:00 |
| `bronze/nccs_efile/raw/tax_year=2023/table=F9-P00-T00-HEADER/F9-P00-T00-HEADER-2023.CSV`                               | 335234643                                                   | 2026-03-22T22:37:52+00:00 |
| `bronze/nccs_efile/raw/tax_year=2023/table=F9-P01-T00-SUMMARY/F9-P01-T00-SUMMARY-2023.CSV`                             | 285762444                                                   | 2026-03-22T22:38:45+00:00 |
| `bronze/nccs_efile/raw/tax_year=2023/table=SA-P01-T00-PUBLIC-CHARITY-STATUS/SA-P01-T00-PUBLIC-CHARITY-STATUS-2023.CSV` | 137688638                                                   | 2026-03-22T22:39:43+00:00 |
| `bronze/nccs_efile/raw/tax_year=2024/table=F9-P00-T00-HEADER/F9-P00-T00-HEADER-2024.CSV`                               | 95861181                                                    | 2026-03-22T22:40:12+00:00 |
| `bronze/nccs_efile/raw/tax_year=2024/table=F9-P01-T00-SUMMARY/F9-P01-T00-SUMMARY-2024.CSV`                             | 80900384                                                    | 2026-03-22T22:40:35+00:00 |
| `bronze/nccs_efile/raw/tax_year=2024/table=SA-P01-T00-PUBLIC-CHARITY-STATUS/SA-P01-T00-PUBLIC-CHARITY-STATUS-2024.CSV` | 40929398                                                    | 2026-03-22T22:40:56+00:00 |
| `silver/combined_990/combined_990_filtered_source_union.parquet`                                                       | 4716125                                                     | 2026-03-22T22:52:31+00:00 |
| `silver/combined_990/combined_990_master_ein_tax_year.parquet`                                                         | 1915660                                                     | 2026-03-22T22:52:38+00:00 |
| `silver/combined_990/metadata/build_summary.json`                                                                      | 8948                                                        | 2026-03-22T22:52:49+00:00 |
| `silver/combined_990/metadata/column_dictionary.csv`                                                                   | 72114                                                       | 2026-03-22T22:52:42+00:00 |
| `silver/combined_990/metadata/diag_overlap_by_ein.csv`                                                                 | 418494                                                      | 2026-03-22T22:52:44+00:00 |
| `silver/combined_990/metadata/diag_overlap_by_ein_tax_year.csv`                                                        | 1596439                                                     | 2026-03-22T22:52:46+00:00 |
| `silver/combined_990/metadata/diag_overlap_summary.csv`                                                                | 955                                                         | 2026-03-22T22:52:48+00:00 |
| `silver/combined_990/metadata/field_availability_matrix.csv`                                                           | 165155                                                      | 2026-03-22T22:52:43+00:00 |
| `silver/combined_990/metadata/master_column_dictionary.csv`                                                            | 47257                                                       | 2026-03-22T22:52:51+00:00 |
| `silver/combined_990/metadata/master_conflict_detail.csv`                                                              | 10763664                                                    | 2026-03-22T22:52:53+00:00 |
| `silver/combined_990/metadata/master_conflict_detail.parquet`                                                          | 466080                                                      | 2026-03-22T22:53:02+00:00 |
| `silver/combined_990/metadata/master_conflict_summary.csv`                                                             | 708                                                         | 2026-03-22T22:52:52+00:00 |
| `silver/combined_990/metadata/master_field_selection_summary.csv`                                                      | 2663                                                        | 2026-03-22T22:52:51+00:00 |
| `silver/combined_990/metadata/size_verification.csv`                                                                   | 3858                                                        | 2026-03-22T22:53:15+00:00 |
| `silver/combined_990/metadata/source_input_manifest.csv`                                                               | 2673                                                        | 2026-03-22T22:52:41+00:00 |
| `silver/givingtuesday_990/filing/givingtuesday_990_filings_benchmark.parquet`                                          | 2310896                                                     | 2026-03-18T20:18:38+00:00 |
| `silver/givingtuesday_990/filing/manifest_filtered.json`                                                               | 687                                                         | 2026-03-18T20:18:42+00:00 |
| `silver/irs990/bmf/bmf_benchmark_counties.parquet`                                                                     | 510542                                                      | 2026-03-03T19:09:18+00:00 |
| `silver/irs_soi/county/tax_year=2022/filter_manifest_2022.csv`                                                         | 1455                                                        | 2026-03-20T18:07:46+00:00 |
| `silver/irs_soi/county/tax_year=2022/irs_soi_county_benchmark_agi_2022.csv`                                            | 204147                                                      | 2026-03-20T18:07:41+00:00 |
| `silver/irs_soi/county/tax_year=2022/irs_soi_county_benchmark_noagi_2022.csv`                                          | 30136                                                       | 2026-03-20T18:07:37+00:00 |
| `silver/nccs_990/core/year=2022/CORE-2022-501C3-CHARITIES-PC-HRMN__benchmark.csv`                                      | 61552                                                       | 2026-03-20T22:44:29+00:00 |
| `silver/nccs_990/core/year=2022/CORE-2022-501C3-CHARITIES-PZ-HRMN__benchmark.csv`                                      | 86369                                                       | 2026-03-20T22:44:26+00:00 |
| `silver/nccs_990/core/year=2022/CORE-2022-501C3-PRIVFOUND-PF-HRMN-V0__benchmark.csv`                                   | 179793                                                      | 2026-03-20T22:44:37+00:00 |
| `silver/nccs_990/core/year=2022/CORE-2022-501CE-NONPROFIT-PC-HRMN__benchmark.csv`                                      | 29233                                                       | 2026-03-20T22:44:34+00:00 |
| `silver/nccs_990/core/year=2022/CORE-2022-501CE-NONPROFIT-PZ-HRMN__benchmark.csv`                                      | 44517                                                       | 2026-03-20T22:44:32+00:00 |
| `silver/nccs_990/core/year=2022/filter_manifest_year=2022.csv`                                                         | 4414                                                        | 2026-03-20T22:44:40+00:00 |
| `silver/nccs_990/postcard/snapshot_year=2026/filter_manifest_snapshot_year=2026.csv`                                   | 1655                                                        | 2026-03-21T03:10:31+00:00 |
| `silver/nccs_990/postcard/snapshot_year=2026/filter_manifest_snapshot_year=2026_tax_year_start=2022.csv`               | 1241                                                        | 2026-03-22T22:49:24+00:00 |
| `silver/nccs_990/postcard/snapshot_year=2026/nccs_990_postcard_benchmark_snapshot_year=2026.csv`                       | 1411937                                                     | 2026-03-21T03:10:23+00:00 |
| `silver/nccs_990/postcard/snapshot_year=2026/nccs_990_postcard_benchmark_tax_year_start=2022_snapshot_year=2026.csv`   | 863185                                                      | 2026-03-22T22:49:21+00:00 |
| `silver/nccs_bmf/metadata/filter_manifest_start_year=2022.csv`                                                         | 4149                                                        | 2026-03-22T03:51:32+00:00 |
| `silver/nccs_bmf/year=2022/nccs_bmf_benchmark_year=2022.parquet`                                                       | 313799                                                      | 2026-03-22T03:51:21+00:00 |
| `silver/nccs_bmf/year=2023/nccs_bmf_benchmark_year=2023.parquet`                                                       | 342299                                                      | 2026-03-22T03:51:23+00:00 |
| `silver/nccs_bmf/year=2024/nccs_bmf_benchmark_year=2024.parquet`                                                       | 352242                                                      | 2026-03-22T03:51:25+00:00 |
| `silver/nccs_bmf/year=2025/nccs_bmf_benchmark_year=2025.parquet`                                                       | 363112                                                      | 2026-03-22T03:51:27+00:00 |
| `silver/nccs_bmf/year=2026/nccs_bmf_benchmark_year=2026.parquet`                                                       | 367947                                                      | 2026-03-22T03:51:29+00:00 |
| `silver/nccs_efile/comparison/tax_year=2022/efile_vs_core_conflicts_tax_year=2022.csv`                                 | 472                                                         | 2026-03-22T22:44:16+00:00 |
| `silver/nccs_efile/comparison/tax_year=2022/efile_vs_core_fill_rates_tax_year=2022.csv`                                | 1136                                                        | 2026-03-22T22:44:15+00:00 |
| `silver/nccs_efile/comparison/tax_year=2022/efile_vs_core_row_counts_tax_year=2022.csv`                                | 731                                                         | 2026-03-22T22:44:14+00:00 |
| `silver/nccs_efile/comparison/tax_year=2022/efile_vs_core_summary_tax_year=2022.json`                                  | 1550                                                        | 2026-03-22T22:44:16+00:00 |
| `silver/nccs_efile/tax_year=2022/filter_manifest_tax_year=2022.csv`                                                    | 1514                                                        | 2026-03-22T22:43:21+00:00 |
| `silver/nccs_efile/tax_year=2022/nccs_efile_benchmark_tax_year=2022.parquet`                                           | 282654                                                      | 2026-03-22T22:43:20+00:00 |
| `silver/nccs_efile/tax_year=2023/filter_manifest_tax_year=2023.csv`                                                    | 1514                                                        | 2026-03-22T22:43:23+00:00 |
| `silver/nccs_efile/tax_year=2023/nccs_efile_benchmark_tax_year=2023.parquet`                                           | 274085                                                      | 2026-03-22T22:43:22+00:00 |
| `silver/nccs_efile/tax_year=2024/filter_manifest_tax_year=2024.csv`                                                    | 1511                                                        | 2026-03-22T22:43:25+00:00 |
| `silver/nccs_efile/tax_year=2024/nccs_efile_benchmark_tax_year=2024.parquet`                                           | 103581                                                      | 2026-03-22T22:43:24+00:00 |


