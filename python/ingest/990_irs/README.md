# 990_irs — IRS Form 990 TEOS XML → S3

Scripts that pull **Form 990 series (e-file) XML** from the IRS TEOS downloads and upload them **directly to the project AWS bucket**. No large local files: index and ZIPs are streamed (IRS → S3).

**Bucket:** `swb-321-irs990-teos`  
**Prefix:** `bronze/irs990/teos_xml`  
**Years:** 2021 through current year (configurable).

See **docs/990_irs_teos_s3_plan.md** for the full plan and S3 layout.

## Prerequisites

- **AWS CLI** configured (or env: `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, `AWS_DEFAULT_REGION`).
- **Python:** `requests`, `pandas`, `boto3` (e.g. `pip install requests pandas boto3`).
- Optional: run **01** before **02** so the index is in S3; 02 can also fetch the index from the IRS if missing in S3.

## Run order

1. **01_upload_irs_990_index_to_s3.py** — Upload index CSV for each year (2021–present) to S3.
2. **02_upload_irs_990_zips_to_s3.py** — For each year, read the index (from S3 or IRS), determine which ZIP parts to upload, then stream each ZIP from IRS to S3.

## Commands (from repo root)

**Upload indexes (all years 2021–current):**
```bash
python python/ingest/990_irs/01_upload_irs_990_index_to_s3.py
```

**Upload only ZIP parts that contain EINs from the project EIN list** (benchmark regions):
```bash
python python/ingest/990_irs/02_upload_irs_990_zips_to_s3.py
```
(Uses `01_data/reference/eins_in_benchmark_regions.csv` if present.)

**Upload all ZIP parts** (full TEOS set per year; larger):
```bash
python python/ingest/990_irs/02_upload_irs_990_zips_to_s3.py --all-parts
```

**Custom EIN list or year range:**
```bash
python python/ingest/990_irs/02_upload_irs_990_zips_to_s3.py --ein-list path/to/eins.csv --start-year 2022 --end-year 2025
```

**Override bucket/prefix/region:**
```bash
python python/ingest/990_irs/01_upload_irs_990_index_to_s3.py --bucket my-bucket --prefix bronze/irs990 --region us-east-2
```
Or set env: `IRS_990_S3_BUCKET`, `IRS_990_S3_PREFIX`, `AWS_DEFAULT_REGION`.

## Output in S3

- **Index:** `s3://{bucket}/{prefix}/index/year={YEAR}/index_{YEAR}.csv`
- **ZIPs:** `s3://{bucket}/{prefix}/zips/year={YEAR}/{YEAR}_TEOS_XML_{PART}.zip`

Example: `s3://swb-321-irs990-teos/bronze/irs990/teos_xml/zips/year=2024/2024_TEOS_XML_01A.zip`
