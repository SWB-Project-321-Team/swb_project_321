# S3 Bucket Inventory

- Bucket: `swb-321-irs990-teos`
- Region: `us-east-2`
- Snapshot date: `2026-03-20`
- Total object count: `209`
- Total discovered folder-prefix count: `37`

This file is a live inventory generated from the current recursive S3 listing. S3 does not have real folders, so the "folders" below are key prefixes inferred from object paths.

## Folder Prefixes

- `bronze/`
- `bronze/givingtuesday_990/`
- `bronze/givingtuesday_990/datamarts/`
- `bronze/givingtuesday_990/datamarts/metadata/`
- `bronze/givingtuesday_990/datamarts/raw/`
- `bronze/givingtuesday_990/datamarts/unfiltered/`
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
- `silver/`
- `silver/givingtuesday_990/`
- `silver/givingtuesday_990/filing/`
- `silver/irs990/`
- `silver/irs990/bmf/`
- `silver/irs_soi/`
- `silver/irs_soi/county/`
- `silver/irs_soi/county/tax_year=2022/`

## Object Inventory

### `bronze/`


| Key                                                                                                      | Size (bytes) | Last modified (UTC)       |
| -------------------------------------------------------------------------------------------------------- | ------------ | ------------------------- |
| `bronze/givingtuesday_990/datamarts/metadata/datamart_catalog.csv`                                       | 14798        | 2026-03-18T19:11:07+00:00 |
| `bronze/givingtuesday_990/datamarts/metadata/datamart_catalog.md`                                        | 14196        | 2026-03-18T19:11:08+00:00 |
| `bronze/givingtuesday_990/datamarts/metadata/datamart_catalog_raw.json`                                  | 42672        | 2026-03-18T19:11:09+00:00 |
| `bronze/givingtuesday_990/datamarts/metadata/datamart_fields.csv`                                        | 238445       | 2026-03-18T19:11:10+00:00 |
| `bronze/givingtuesday_990/datamarts/metadata/datamart_fields.md`                                         | 157412       | 2026-03-18T19:11:11+00:00 |
| `bronze/givingtuesday_990/datamarts/metadata/required_datasets_manifest.csv`                             | 1917         | 2026-03-18T19:11:12+00:00 |
| `bronze/givingtuesday_990/datamarts/metadata/size_verification_report.csv`                               | 2461         | 2026-03-18T19:41:29+00:00 |
| `bronze/givingtuesday_990/datamarts/raw/2025_07_10_All_Years_990_990ez_990pf_990n_Combined_DataMart.csv` | 2140103678   | 2026-03-18T19:11:13+00:00 |
| `bronze/givingtuesday_990/datamarts/raw/2025_08_29_All_Years_990PFStandardFields.csv`                    | 677317763    | 2026-03-18T19:20:03+00:00 |
| `bronze/givingtuesday_990/datamarts/raw/2025_10_18_All_Years_990StandardFields.csv`                      | 2302094678   | 2026-03-18T19:22:48+00:00 |
| `bronze/givingtuesday_990/datamarts/raw/2025_10_28_All_Years_990EZStandardFields.csv`                    | 1432510202   | 2026-03-18T19:31:59+00:00 |
| `bronze/givingtuesday_990/datamarts/unfiltered/givingtuesday_990_basic_allforms_unfiltered.parquet`      | 2395061328   | 2026-03-18T19:57:48+00:00 |
| `bronze/givingtuesday_990/datamarts/unfiltered/givingtuesday_990_basic_plus_combined_unfiltered.parquet` | 2622793846   | 2026-03-18T20:07:00+00:00 |
| `bronze/irs990/bmf/eo_az.csv`                                                                            | 5259463      | 2026-03-03T04:00:14+00:00 |
| `bronze/irs990/bmf/eo_mn.csv`                                                                            | 6863374      | 2026-03-03T03:59:31+00:00 |
| `bronze/irs990/bmf/eo_mt.csv`                                                                            | 2082762      | 2026-03-03T04:00:00+00:00 |
| `bronze/irs990/bmf/eo_sd.csv`                                                                            | 1252794      | 2026-03-03T03:59:18+00:00 |
| `bronze/irs990/teos_xml/code/python.zip`                                                                 | 105604       | 2026-02-27T22:14:53+00:00 |
| `bronze/irs990/teos_xml/index/year=2021/.keep`                                                           | 0            | 2026-02-22T22:44:36+00:00 |
| `bronze/irs990/teos_xml/index/year=2021/index_2021.csv`                                                  | 74764447     | 2026-03-03T04:13:36+00:00 |
| `bronze/irs990/teos_xml/index/year=2022/.keep`                                                           | 0            | 2026-02-22T22:44:40+00:00 |
| `bronze/irs990/teos_xml/index/year=2022/index_2022.csv`                                                  | 72228246     | 2026-03-03T04:13:23+00:00 |
| `bronze/irs990/teos_xml/index/year=2023/.keep`                                                           | 0            | 2026-02-22T22:44:43+00:00 |
| `bronze/irs990/teos_xml/index/year=2023/index_2023.csv`                                                  | 77519435     | 2026-03-03T04:13:31+00:00 |
| `bronze/irs990/teos_xml/index/year=2024/.keep`                                                           | 0            | 2026-02-22T22:44:46+00:00 |
| `bronze/irs990/teos_xml/index/year=2024/index_2024.csv`                                                  | 91056866     | 2026-03-03T04:13:39+00:00 |
| `bronze/irs990/teos_xml/index/year=2025/.keep`                                                           | 0            | 2026-02-22T22:44:50+00:00 |
| `bronze/irs990/teos_xml/index/year=2025/index_2025.csv`                                                  | 93053080     | 2026-03-03T04:29:39+00:00 |
| `bronze/irs990/teos_xml/index/year=2026/index_2026.csv`                                                  | 1474371      | 2026-03-03T04:31:46+00:00 |
| `bronze/irs990/teos_xml/zips/year=2021/.keep`                                                            | 0            | 2026-02-22T22:44:38+00:00 |
| `bronze/irs990/teos_xml/zips/year=2021/2021_TEOS_XML_01A.zip`                                            | 3715545729   | 2026-03-04T03:28:19+00:00 |
| `bronze/irs990/teos_xml/zips/year=2021/2021_TEOS_XML_17903370.zip`                                       | 1917         | 2026-02-23T04:50:36+00:00 |
| `bronze/irs990/teos_xml/zips/year=2021/2021_TEOS_XML_17903377.zip`                                       | 1917         | 2026-02-23T04:50:49+00:00 |
| `bronze/irs990/teos_xml/zips/year=2021/2021_TEOS_XML_17903389.zip`                                       | 1917         | 2026-02-23T04:51:10+00:00 |
| `bronze/irs990/teos_xml/zips/year=2021/2021_TEOS_XML_17903396.zip`                                       | 1917         | 2026-02-23T04:51:23+00:00 |
| `bronze/irs990/teos_xml/zips/year=2021/2021_TEOS_XML_17903401.zip`                                       | 1917         | 2026-02-23T04:51:33+00:00 |
| `bronze/irs990/teos_xml/zips/year=2021/2021_TEOS_XML_17903405.zip`                                       | 1917         | 2026-02-23T04:51:41+00:00 |
| `bronze/irs990/teos_xml/zips/year=2021/2021_TEOS_XML_17903435.zip`                                       | 1917         | 2026-02-23T04:52:33+00:00 |
| `bronze/irs990/teos_xml/zips/year=2021/2021_TEOS_XML_17903453.zip`                                       | 1917         | 2026-02-23T04:52:58+00:00 |
| `bronze/irs990/teos_xml/zips/year=2021/2021_TEOS_XML_17903479.zip`                                       | 1917         | 2026-02-23T04:53:07+00:00 |
| `bronze/irs990/teos_xml/zips/year=2021/2021_TEOS_XML_17903511.zip`                                       | 1917         | 2026-02-23T04:54:03+00:00 |
| `bronze/irs990/teos_xml/zips/year=2021/2021_TEOS_XML_17903533.zip`                                       | 1917         | 2026-02-23T04:54:43+00:00 |
| `bronze/irs990/teos_xml/zips/year=2021/2021_TEOS_XML_17903543.zip`                                       | 1917         | 2026-02-23T04:55:01+00:00 |
| `bronze/irs990/teos_xml/zips/year=2021/2021_TEOS_XML_17903556.zip`                                       | 1917         | 2026-02-23T04:55:23+00:00 |
| `bronze/irs990/teos_xml/zips/year=2021/2021_TEOS_XML_17903566.zip`                                       | 1917         | 2026-02-23T04:55:41+00:00 |
| `bronze/irs990/teos_xml/zips/year=2021/2021_TEOS_XML_17903572.zip`                                       | 1917         | 2026-02-23T04:55:52+00:00 |
| `bronze/irs990/teos_xml/zips/year=2021/2021_TEOS_XML_17903578.zip`                                       | 1917         | 2026-02-23T04:56:04+00:00 |
| `bronze/irs990/teos_xml/zips/year=2021/2021_TEOS_XML_17903583.zip`                                       | 1917         | 2026-02-23T04:56:14+00:00 |
| `bronze/irs990/teos_xml/zips/year=2021/2021_TEOS_XML_17903590.zip`                                       | 1917         | 2026-02-23T04:56:26+00:00 |
| `bronze/irs990/teos_xml/zips/year=2021/2021_TEOS_XML_17903593.zip`                                       | 1917         | 2026-02-23T04:56:33+00:00 |
| `bronze/irs990/teos_xml/zips/year=2021/2021_TEOS_XML_17903597.zip`                                       | 1917         | 2026-02-23T04:56:41+00:00 |
| `bronze/irs990/teos_xml/zips/year=2021/2021_TEOS_XML_17903610.zip`                                       | 1917         | 2026-02-23T04:57:05+00:00 |
| `bronze/irs990/teos_xml/zips/year=2021/2021_TEOS_XML_17903612.zip`                                       | 1917         | 2026-02-23T04:57:09+00:00 |
| `bronze/irs990/teos_xml/zips/year=2021/2021_TEOS_XML_17903622.zip`                                       | 1917         | 2026-02-23T04:57:27+00:00 |
| `bronze/irs990/teos_xml/zips/year=2021/2021_TEOS_XML_17903624.zip`                                       | 1917         | 2026-02-23T04:57:32+00:00 |
| `bronze/irs990/teos_xml/zips/year=2021/2021_TEOS_XML_17903631.zip`                                       | 1917         | 2026-02-23T04:57:46+00:00 |
| `bronze/irs990/teos_xml/zips/year=2021/2021_TEOS_XML_17903645.zip`                                       | 1917         | 2026-02-23T04:58:11+00:00 |
| `bronze/irs990/teos_xml/zips/year=2021/2021_TEOS_XML_17903646.zip`                                       | 1917         | 2026-02-23T04:58:14+00:00 |
| `bronze/irs990/teos_xml/zips/year=2021/2021_TEOS_XML_17903654.zip`                                       | 1917         | 2026-02-23T04:58:28+00:00 |
| `bronze/irs990/teos_xml/zips/year=2021/2021_TEOS_XML_17906754.zip`                                       | 1917         | 2026-02-23T06:24:00+00:00 |
| `bronze/irs990/teos_xml/zips/year=2021/2021_TEOS_XML_17906755.zip`                                       | 1917         | 2026-02-23T06:24:02+00:00 |
| `bronze/irs990/teos_xml/zips/year=2021/2021_TEOS_XML_17906760.zip`                                       | 1917         | 2026-02-23T06:24:11+00:00 |
| `bronze/irs990/teos_xml/zips/year=2021/2021_TEOS_XML_17906763.zip`                                       | 1917         | 2026-02-23T06:24:18+00:00 |
| `bronze/irs990/teos_xml/zips/year=2021/2021_TEOS_XML_17906769.zip`                                       | 1917         | 2026-02-23T06:24:29+00:00 |
| `bronze/irs990/teos_xml/zips/year=2021/2021_TEOS_XML_17906783.zip`                                       | 1917         | 2026-02-23T06:24:53+00:00 |
| `bronze/irs990/teos_xml/zips/year=2021/2021_TEOS_XML_17906784.zip`                                       | 1917         | 2026-02-23T06:24:55+00:00 |
| `bronze/irs990/teos_xml/zips/year=2021/2021_TEOS_XML_17906795.zip`                                       | 1917         | 2026-02-23T06:25:16+00:00 |
| `bronze/irs990/teos_xml/zips/year=2021/2021_TEOS_XML_17906803.zip`                                       | 1917         | 2026-02-23T06:25:30+00:00 |
| `bronze/irs990/teos_xml/zips/year=2021/2021_TEOS_XML_17906804.zip`                                       | 1917         | 2026-02-23T06:25:33+00:00 |
| `bronze/irs990/teos_xml/zips/year=2021/2021_TEOS_XML_17906805.zip`                                       | 1917         | 2026-02-23T06:25:35+00:00 |
| `bronze/irs990/teos_xml/zips/year=2021/2021_TEOS_XML_17906809.zip`                                       | 1917         | 2026-02-23T06:25:43+00:00 |
| `bronze/irs990/teos_xml/zips/year=2021/2021_TEOS_XML_17906813.zip`                                       | 1917         | 2026-02-23T06:25:50+00:00 |
| `bronze/irs990/teos_xml/zips/year=2021/2021_TEOS_XML_17906815.zip`                                       | 1917         | 2026-02-23T06:25:54+00:00 |
| `bronze/irs990/teos_xml/zips/year=2021/2021_TEOS_XML_17906822.zip`                                       | 1917         | 2026-02-23T06:26:06+00:00 |
| `bronze/irs990/teos_xml/zips/year=2021/2021_TEOS_XML_17906832.zip`                                       | 1917         | 2026-02-23T06:26:23+00:00 |
| `bronze/irs990/teos_xml/zips/year=2021/2021_TEOS_XML_17906833.zip`                                       | 1917         | 2026-02-23T06:26:26+00:00 |
| `bronze/irs990/teos_xml/zips/year=2021/2021_TEOS_XML_17906839.zip`                                       | 1917         | 2026-02-23T06:26:36+00:00 |
| `bronze/irs990/teos_xml/zips/year=2021/2021_TEOS_XML_17906841.zip`                                       | 1917         | 2026-02-23T06:26:41+00:00 |
| `bronze/irs990/teos_xml/zips/year=2021/2021_TEOS_XML_17906842.zip`                                       | 1917         | 2026-02-23T06:26:44+00:00 |
| `bronze/irs990/teos_xml/zips/year=2021/2021_TEOS_XML_17906845.zip`                                       | 1917         | 2026-02-23T06:26:50+00:00 |
| `bronze/irs990/teos_xml/zips/year=2021/2021_TEOS_XML_17906865.zip`                                       | 1917         | 2026-02-23T06:27:24+00:00 |
| `bronze/irs990/teos_xml/zips/year=2021/2021_TEOS_XML_17906871.zip`                                       | 1917         | 2026-02-23T06:27:36+00:00 |
| `bronze/irs990/teos_xml/zips/year=2021/2021_TEOS_XML_17921176.zip`                                       | 1917         | 2026-02-23T07:07:41+00:00 |
| `bronze/irs990/teos_xml/zips/year=2021/2021_TEOS_XML_17921180.zip`                                       | 1917         | 2026-02-23T07:07:48+00:00 |
| `bronze/irs990/teos_xml/zips/year=2021/2021_TEOS_XML_17921185.zip`                                       | 1917         | 2026-02-23T07:07:58+00:00 |
| `bronze/irs990/teos_xml/zips/year=2021/2021_TEOS_XML_17921188.zip`                                       | 1917         | 2026-02-23T07:08:04+00:00 |
| `bronze/irs990/teos_xml/zips/year=2021/2021_TEOS_XML_17921190.zip`                                       | 1917         | 2026-02-23T07:08:08+00:00 |
| `bronze/irs990/teos_xml/zips/year=2021/2021_TEOS_XML_17921191.zip`                                       | 1917         | 2026-02-23T07:08:10+00:00 |
| `bronze/irs990/teos_xml/zips/year=2021/2021_TEOS_XML_17921195.zip`                                       | 1917         | 2026-02-23T07:08:18+00:00 |
| `bronze/irs990/teos_xml/zips/year=2021/2021_TEOS_XML_17921196.zip`                                       | 1917         | 2026-02-23T07:08:21+00:00 |
| `bronze/irs990/teos_xml/zips/year=2021/2021_TEOS_XML_17921197.zip`                                       | 1917         | 2026-02-23T07:08:23+00:00 |
| `bronze/irs990/teos_xml/zips/year=2021/2021_TEOS_XML_17921200.zip`                                       | 1917         | 2026-02-23T07:08:30+00:00 |
| `bronze/irs990/teos_xml/zips/year=2021/2021_TEOS_XML_17921203.zip`                                       | 1917         | 2026-02-23T07:08:35+00:00 |
| `bronze/irs990/teos_xml/zips/year=2021/2021_TEOS_XML_17921223.zip`                                       | 1917         | 2026-02-23T07:09:09+00:00 |
| `bronze/irs990/teos_xml/zips/year=2021/2021_TEOS_XML_17921224.zip`                                       | 1917         | 2026-02-23T07:09:11+00:00 |
| `bronze/irs990/teos_xml/zips/year=2021/2021_TEOS_XML_17921230.zip`                                       | 1917         | 2026-02-23T07:09:22+00:00 |
| `bronze/irs990/teos_xml/zips/year=2021/2021_TEOS_XML_17921232.zip`                                       | 1917         | 2026-02-23T07:09:26+00:00 |
| `bronze/irs990/teos_xml/zips/year=2021/2021_TEOS_XML_17921235.zip`                                       | 1917         | 2026-02-23T07:09:31+00:00 |
| `bronze/irs990/teos_xml/zips/year=2021/2021_TEOS_XML_17921236.zip`                                       | 1917         | 2026-02-23T07:09:33+00:00 |
| `bronze/irs990/teos_xml/zips/year=2021/2021_TEOS_XML_17921238.zip`                                       | 1917         | 2026-02-23T07:09:37+00:00 |
| `bronze/irs990/teos_xml/zips/year=2021/2021_TEOS_XML_17921246.zip`                                       | 1917         | 2026-02-23T07:09:50+00:00 |
| `bronze/irs990/teos_xml/zips/year=2021/2021_TEOS_XML_17921268.zip`                                       | 1917         | 2026-02-23T07:10:26+00:00 |
| `bronze/irs990/teos_xml/zips/year=2021/2021_TEOS_XML_17921297.zip`                                       | 1917         | 2026-02-23T07:11:11+00:00 |
| `bronze/irs990/teos_xml/zips/year=2021/2021_TEOS_XML_17924491.zip`                                       | 77485        | 2026-02-23T08:31:15+00:00 |
| `bronze/irs990/teos_xml/zips/year=2021/2021_TEOS_XML_18177320.zip`                                       | 1917         | 2026-02-23T10:29:05+00:00 |
| `bronze/irs990/teos_xml/zips/year=2021/2021_TEOS_XML_18177366.zip`                                       | 1917         | 2026-02-23T10:30:20+00:00 |
| `bronze/irs990/teos_xml/zips/year=2021/2021_TEOS_XML_18177368.zip`                                       | 1917         | 2026-02-23T10:30:23+00:00 |
| `bronze/irs990/teos_xml/zips/year=2021/2021_TEOS_XML_18199786.zip`                                       | 1917         | 2026-02-23T16:20:35+00:00 |
| `bronze/irs990/teos_xml/zips/year=2021/2021_TEOS_XML_18199792.zip`                                       | 1917         | 2026-02-23T16:20:44+00:00 |
| `bronze/irs990/teos_xml/zips/year=2021/2021_TEOS_XML_18199796.zip`                                       | 1917         | 2026-02-23T16:20:51+00:00 |
| `bronze/irs990/teos_xml/zips/year=2021/2021_TEOS_XML_18199806.zip`                                       | 1917         | 2026-02-23T16:21:08+00:00 |
| `bronze/irs990/teos_xml/zips/year=2021/2021_TEOS_XML_18199983.zip`                                       | 1917         | 2026-02-23T16:25:32+00:00 |
| `bronze/irs990/teos_xml/zips/year=2021/2021_TEOS_XML_18199986.zip`                                       | 1917         | 2026-02-23T16:25:38+00:00 |
| `bronze/irs990/teos_xml/zips/year=2021/2021_TEOS_XML_18200019.zip`                                       | 1917         | 2026-02-23T16:26:32+00:00 |
| `bronze/irs990/teos_xml/zips/year=2021/2021_TEOS_XML_18200057.zip`                                       | 1917         | 2026-02-23T16:27:29+00:00 |
| `bronze/irs990/teos_xml/zips/year=2021/2021_TEOS_XML_18200060.zip`                                       | 1917         | 2026-02-23T16:27:35+00:00 |
| `bronze/irs990/teos_xml/zips/year=2021/2021_TEOS_XML_18200064.zip`                                       | 1917         | 2026-02-23T16:27:42+00:00 |
| `bronze/irs990/teos_xml/zips/year=2021/2021_TEOS_XML_18200067.zip`                                       | 1917         | 2026-02-23T16:27:48+00:00 |
| `bronze/irs990/teos_xml/zips/year=2021/2021_TEOS_XML_18200075.zip`                                       | 1917         | 2026-02-23T16:28:02+00:00 |
| `bronze/irs990/teos_xml/zips/year=2021/2021_TEOS_XML_18200078.zip`                                       | 1917         | 2026-02-23T16:28:08+00:00 |
| `bronze/irs990/teos_xml/zips/year=2021/2021_TEOS_XML_18200087.zip`                                       | 1917         | 2026-02-23T16:28:24+00:00 |
| `bronze/irs990/teos_xml/zips/year=2021/2021_TEOS_XML_18200092.zip`                                       | 1917         | 2026-02-23T16:28:34+00:00 |
| `bronze/irs990/teos_xml/zips/year=2021/2021_TEOS_XML_18200093.zip`                                       | 1917         | 2026-02-23T16:28:37+00:00 |
| `bronze/irs990/teos_xml/zips/year=2021/2021_TEOS_XML_18200096.zip`                                       | 1917         | 2026-02-23T16:28:42+00:00 |
| `bronze/irs990/teos_xml/zips/year=2021/2021_TEOS_XML_18200099.zip`                                       | 1917         | 2026-02-23T16:28:48+00:00 |
| `bronze/irs990/teos_xml/zips/year=2021/2021_TEOS_XML_18200104.zip`                                       | 1917         | 2026-02-23T16:28:57+00:00 |
| `bronze/irs990/teos_xml/zips/year=2021/2021_TEOS_XML_18200108.zip`                                       | 1917         | 2026-02-23T16:29:05+00:00 |
| `bronze/irs990/teos_xml/zips/year=2021/2021_TEOS_XML_18200110.zip`                                       | 1917         | 2026-02-23T16:29:10+00:00 |
| `bronze/irs990/teos_xml/zips/year=2021/2021_TEOS_XML_18200112.zip`                                       | 1917         | 2026-02-23T16:29:14+00:00 |
| `bronze/irs990/teos_xml/zips/year=2021/2021_TEOS_XML_18200114.zip`                                       | 1917         | 2026-02-23T16:29:18+00:00 |
| `bronze/irs990/teos_xml/zips/year=2021/2021_TEOS_XML_18200117.zip`                                       | 1917         | 2026-02-23T16:29:24+00:00 |
| `bronze/irs990/teos_xml/zips/year=2021/2021_TEOS_XML_18200120.zip`                                       | 1917         | 2026-02-23T16:29:29+00:00 |
| `bronze/irs990/teos_xml/zips/year=2021/2021_TEOS_XML_18200122.zip`                                       | 1917         | 2026-02-23T16:29:34+00:00 |
| `bronze/irs990/teos_xml/zips/year=2021/2021_TEOS_XML_18200126.zip`                                       | 1917         | 2026-02-23T16:29:41+00:00 |
| `bronze/irs990/teos_xml/zips/year=2021/2021_TEOS_XML_18200141.zip`                                       | 1917         | 2026-02-23T16:30:06+00:00 |
| `bronze/irs990/teos_xml/zips/year=2021/2021_TEOS_XML_18200149.zip`                                       | 1917         | 2026-02-23T16:30:20+00:00 |
| `bronze/irs990/teos_xml/zips/year=2021/2021_TEOS_XML_18200154.zip`                                       | 1917         | 2026-02-23T16:30:28+00:00 |
| `bronze/irs990/teos_xml/zips/year=2021/2021_TEOS_XML_18200156.zip`                                       | 1917         | 2026-02-23T16:30:32+00:00 |
| `bronze/irs990/teos_xml/zips/year=2021/2021_TEOS_XML_18200163.zip`                                       | 1917         | 2026-02-23T16:30:45+00:00 |
| `bronze/irs990/teos_xml/zips/year=2021/2021_TEOS_XML_18200405.zip`                                       | 1917         | 2026-02-23T16:35:41+00:00 |
| `bronze/irs990/teos_xml/zips/year=2021/2021_TEOS_XML_18200408.zip`                                       | 1917         | 2026-02-23T16:35:48+00:00 |
| `bronze/irs990/teos_xml/zips/year=2021/2021_TEOS_XML_18200442.zip`                                       | 1917         | 2026-02-23T16:36:40+00:00 |
| `bronze/irs990/teos_xml/zips/year=2021/2021_TEOS_XML_18200447.zip`                                       | 1917         | 2026-02-23T16:36:50+00:00 |
| `bronze/irs990/teos_xml/zips/year=2021/2021_TEOS_XML_18200560.zip`                                       | 1917         | 2026-02-23T16:39:47+00:00 |
| `bronze/irs990/teos_xml/zips/year=2021/2021_TEOS_XML_18200561.zip`                                       | 1917         | 2026-02-23T16:39:50+00:00 |
| `bronze/irs990/teos_xml/zips/year=2021/2021_TEOS_XML_18200562.zip`                                       | 1917         | 2026-02-23T16:39:52+00:00 |
| `bronze/irs990/teos_xml/zips/year=2021/2021_TEOS_XML_18200566.zip`                                       | 1917         | 2026-02-23T16:39:59+00:00 |
| `bronze/irs990/teos_xml/zips/year=2021/2021_TEOS_XML_18200576.zip`                                       | 1917         | 2026-02-23T16:40:16+00:00 |
| `bronze/irs990/teos_xml/zips/year=2021/2021_TEOS_XML_18200577.zip`                                       | 1917         | 2026-02-23T16:40:18+00:00 |
| `bronze/irs990/teos_xml/zips/year=2021/2021_TEOS_XML_18200588.zip`                                       | 1917         | 2026-02-23T16:40:38+00:00 |
| `bronze/irs990/teos_xml/zips/year=2022/.keep`                                                            | 0            | 2026-02-22T22:44:41+00:00 |
| `bronze/irs990/teos_xml/zips/year=2022/2022_TEOS_XML_01A.zip`                                            | 2603666925   | 2026-03-04T03:11:30+00:00 |
| `bronze/irs990/teos_xml/zips/year=2022/2022_TEOS_XML_02A.zip`                                            | 1408196885   | 2026-03-04T01:08:09+00:00 |
| `bronze/irs990/teos_xml/zips/year=2023/.keep`                                                            | 0            | 2026-02-22T22:44:45+00:00 |
| `bronze/irs990/teos_xml/zips/year=2023/2023_TEOS_XML_01A.zip`                                            | 123052879    | 2026-03-03T21:39:36+00:00 |
| `bronze/irs990/teos_xml/zips/year=2023/2023_TEOS_XML_02A.zip`                                            | 234617123    | 2026-03-03T21:59:25+00:00 |
| `bronze/irs990/teos_xml/zips/year=2023/2023_TEOS_XML_03A.zip`                                            | 197165153    | 2026-03-03T21:46:01+00:00 |
| `bronze/irs990/teos_xml/zips/year=2023/2023_TEOS_XML_04A.zip`                                            | 244491049    | 2026-03-03T22:25:51+00:00 |
| `bronze/irs990/teos_xml/zips/year=2023/2023_TEOS_XML_05A.zip`                                            | 990540072    | 2026-03-04T00:55:57+00:00 |
| `bronze/irs990/teos_xml/zips/year=2023/2023_TEOS_XML_06A.zip`                                            | 163716234    | 2026-03-03T22:44:09+00:00 |
| `bronze/irs990/teos_xml/zips/year=2023/2023_TEOS_XML_07A.zip`                                            | 219928555    | 2026-03-03T23:26:50+00:00 |
| `bronze/irs990/teos_xml/zips/year=2023/2023_TEOS_XML_08A.zip`                                            | 266247548    | 2026-03-03T23:47:21+00:00 |
| `bronze/irs990/teos_xml/zips/year=2023/2023_TEOS_XML_09A.zip`                                            | 200422523    | 2026-03-04T00:23:50+00:00 |
| `bronze/irs990/teos_xml/zips/year=2023/2023_TEOS_XML_10A.zip`                                            | 332505047    | 2026-03-04T01:09:33+00:00 |
| `bronze/irs990/teos_xml/zips/year=2023/2023_TEOS_XML_11A.zip`                                            | 1200256933   | 2026-03-04T02:40:33+00:00 |
| `bronze/irs990/teos_xml/zips/year=2023/2023_TEOS_XML_12A.zip`                                            | 120729513    | 2026-03-04T02:27:27+00:00 |
| `bronze/irs990/teos_xml/zips/year=2024/.keep`                                                            | 0            | 2026-02-22T22:44:48+00:00 |
| `bronze/irs990/teos_xml/zips/year=2024/2024_TEOS_XML_01A.zip`                                            | 104816571    | 2026-03-04T03:05:06+00:00 |
| `bronze/irs990/teos_xml/zips/year=2024/2024_TEOS_XML_02A.zip`                                            | 243689721    | 2026-03-04T03:46:19+00:00 |
| `bronze/irs990/teos_xml/zips/year=2024/2024_TEOS_XML_03A.zip`                                            | 209335079    | 2026-03-04T04:15:05+00:00 |
| `bronze/irs990/teos_xml/zips/year=2024/2024_TEOS_XML_04A.zip`                                            | 350268985    | 2026-03-04T05:20:34+00:00 |
| `bronze/irs990/teos_xml/zips/year=2024/2024_TEOS_XML_05A.zip`                                            | 977460330    | 2026-03-04T06:02:55+00:00 |
| `bronze/irs990/teos_xml/zips/year=2024/2024_TEOS_XML_06A.zip`                                            | 163872638    | 2026-03-04T06:47:48+00:00 |
| `bronze/irs990/teos_xml/zips/year=2024/2024_TEOS_XML_07A.zip`                                            | 288252859    | 2026-03-04T07:06:58+00:00 |
| `bronze/irs990/teos_xml/zips/year=2024/2024_TEOS_XML_08A.zip`                                            | 260603085    | 2026-03-04T07:38:04+00:00 |
| `bronze/irs990/teos_xml/zips/year=2024/2024_TEOS_XML_09A.zip`                                            | 247036759    | 2026-03-04T08:16:53+00:00 |
| `bronze/irs990/teos_xml/zips/year=2024/2024_TEOS_XML_10A.zip`                                            | 251614735    | 2026-03-04T08:37:38+00:00 |
| `bronze/irs990/teos_xml/zips/year=2024/2024_TEOS_XML_11A.zip`                                            | 1165636963   | 2026-03-04T08:55:43+00:00 |
| `bronze/irs990/teos_xml/zips/year=2024/2024_TEOS_XML_12A.zip`                                            | 128322545    | 2026-03-04T09:14:06+00:00 |
| `bronze/irs990/teos_xml/zips/year=2025/.keep`                                                            | 0            | 2026-02-22T22:44:51+00:00 |
| `bronze/irs990/teos_xml/zips/year=2025/2025_TEOS_XML_01A.zip`                                            | 103669610    | 2026-03-04T09:40:50+00:00 |
| `bronze/irs990/teos_xml/zips/year=2025/2025_TEOS_XML_02A.zip`                                            | 239929743    | 2026-03-04T09:48:38+00:00 |
| `bronze/irs990/teos_xml/zips/year=2025/2025_TEOS_XML_03A.zip`                                            | 222596630    | 2026-03-04T10:05:48+00:00 |
| `bronze/irs990/teos_xml/zips/year=2025/2025_TEOS_XML_04A.zip`                                            | 338249494    | 2026-03-04T10:35:35+00:00 |
| `bronze/irs990/teos_xml/zips/year=2025/2025_TEOS_XML_05A.zip`                                            | 495924982    | 2026-03-04T11:07:50+00:00 |
| `bronze/irs990/teos_xml/zips/year=2025/2025_TEOS_XML_05B.zip`                                            | 501021178    | 2026-03-04T11:09:12+00:00 |
| `bronze/irs990/teos_xml/zips/year=2025/2025_TEOS_XML_06A.zip`                                            | 246788129    | 2026-03-04T12:02:02+00:00 |
| `bronze/irs990/teos_xml/zips/year=2025/2025_TEOS_XML_07A.zip`                                            | 143450588    | 2026-03-04T12:56:52+00:00 |
| `bronze/irs990/teos_xml/zips/year=2025/2025_TEOS_XML_08A.zip`                                            | 262744207    | 2026-03-04T13:02:26+00:00 |
| `bronze/irs990/teos_xml/zips/year=2025/2025_TEOS_XML_09A.zip`                                            | 256276809    | 2026-03-04T13:11:53+00:00 |
| `bronze/irs990/teos_xml/zips/year=2025/2025_TEOS_XML_10A.zip`                                            | 56507372     | 2026-03-04T13:33:23+00:00 |
| `bronze/irs990/teos_xml/zips/year=2025/2025_TEOS_XML_11A.zip`                                            | 340137018    | 2026-03-04T13:51:14+00:00 |
| `bronze/irs990/teos_xml/zips/year=2025/2025_TEOS_XML_11B.zip`                                            | 494528498    | 2026-03-04T14:09:16+00:00 |
| `bronze/irs990/teos_xml/zips/year=2025/2025_TEOS_XML_11C.zip`                                            | 277886481    | 2026-03-04T14:16:38+00:00 |
| `bronze/irs990/teos_xml/zips/year=2025/2025_TEOS_XML_11D.zip`                                            | 332775121    | 2026-03-04T14:20:17+00:00 |
| `bronze/irs990/teos_xml/zips/year=2025/2025_TEOS_XML_12A.zip`                                            | 151835020    | 2026-03-04T14:36:29+00:00 |
| `bronze/irs990/teos_xml/zips/year=2026/2026_TEOS_XML_01A.zip`                                            | 71497607     | 2026-03-04T15:08:37+00:00 |
| `bronze/irs_soi/county/metadata/latest_release.json`                                                     | 1721         | 2026-03-20T18:06:14+00:00 |
| `bronze/irs_soi/county/metadata/release_manifest_tax_year=2022.csv`                                      | 1520         | 2026-03-20T18:06:15+00:00 |
| `bronze/irs_soi/county/metadata/size_verification_tax_year=2022.csv`                                     | 1552         | 2026-03-20T18:07:25+00:00 |
| `bronze/irs_soi/county/raw/tax_year=2022/22incyallagi.csv`                                               | 35567992     | 2026-03-20T18:01:24+00:00 |
| `bronze/irs_soi/county/raw/tax_year=2022/22incyallnoagi.csv`                                             | 5075247      | 2026-03-20T18:05:34+00:00 |
| `bronze/irs_soi/county/raw/tax_year=2022/22incydocguide.docx`                                            | 47920        | 2026-03-20T18:06:13+00:00 |


### `silver/`


| Key                                                                           | Size (bytes) | Last modified (UTC)       |
| ----------------------------------------------------------------------------- | ------------ | ------------------------- |
| `silver/givingtuesday_990/filing/givingtuesday_990_filings_benchmark.parquet` | 2310896      | 2026-03-18T20:18:38+00:00 |
| `silver/givingtuesday_990/filing/manifest_filtered.json`                      | 687          | 2026-03-18T20:18:42+00:00 |
| `silver/irs990/bmf/bmf_benchmark_counties.parquet`                            | 510542       | 2026-03-03T19:09:18+00:00 |
| `silver/irs_soi/county/tax_year=2022/filter_manifest_2022.csv`                | 1455         | 2026-03-20T18:07:46+00:00 |
| `silver/irs_soi/county/tax_year=2022/irs_soi_county_benchmark_agi_2022.csv`   | 204147       | 2026-03-20T18:07:41+00:00 |
| `silver/irs_soi/county/tax_year=2022/irs_soi_county_benchmark_noagi_2022.csv` | 30136        | 2026-03-20T18:07:37+00:00 |


