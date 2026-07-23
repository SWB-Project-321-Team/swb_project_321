# BLS nonprofit-table ingest (historical S3 target)

> **Infrastructure status (2026-07-23):** the former project S3 bucket was intentionally deleted and must not be recreated. Retain the BLS public-source download logic, but treat the former upload target as historical.

Scripts pull **BLS** nonprofit tables from **https://www.bls.gov/bdm/nonprofits/tables**. The retained upload behavior originally published them to the project AWS bucket.

**Former bucket:** `swb-321-irs990-teos`
**Prefix:** `bronze/bls`
**Years:**  2022 (configurable).
