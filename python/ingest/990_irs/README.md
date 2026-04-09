# 990_irs — IRS Form 990 TEOS XML → S3

Scripts that pull **Form 990 series (e-file) XML** from the IRS TEOS downloads and upload them **directly to the project AWS bucket**. No large local files: index and ZIPs are streamed (IRS → S3).

**Bucket:** `swb-321-irs990-teos`  
**Prefix:** `bronze/irs990/teos_xml`  
**Years:** 2021 through current year (configurable).

See **docs/990/990_irs_teos_s3_plan.md** for the full plan and S3 layout.

## Prerequisites

- **AWS CLI** configured (or env: `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, `AWS_DEFAULT_REGION`).
- **Python:** `requests`, `pandas`, `boto3` (e.g. `pip install requests pandas boto3`).
- Optional: run **01** (index) before **02** (ZIPs) so the index is in S3; 02 can also fetch the index from the IRS if missing in S3.

## Run order

0. **00_fetch_bmf.py** — (Optional compatibility wrapper.) Delegates to the canonical `python/ingest/irs_bmf/01_fetch_bmf_release.py` pipeline step and can also trigger the raw upload step. Use this if older commands still point at `990_irs`.
0b. **00b_filter_bmf_to_benchmark_upload_silver.py** — (Optional compatibility wrapper.) Delegates to the canonical `python/ingest/irs_bmf/04_filter_bmf_to_benchmark_local.py` and `05_upload_filtered_bmf_to_s3.py` steps while still refreshing `01_data/staging/org/irs_bmf_benchmark_counties.parquet`.
1. **01_upload_irs_990_index_to_s3.py** — Upload index CSV for each year (2021–present) to S3.
2. **02_upload_irs_990_zips_to_s3.py** — Upload all ZIP parts. Filter by geography happens in 03/04.
3. **03_parse_irs_990_zips_to_staging.py** — Read ZIPs from S3, parse 990 XML, **filter by geography** (org address ZIP in benchmark counties via GEOID reference + zip_to_county), join region, write staging.
4. **04_merge_990_parts_to_staging.py** — Merge silver part Parquets into one staging table; **geography filter** (GEOID/county or legacy benchmark ZIPs).

## Filter by GEOID/county (default)

Staging is filtered by **GEOID/county**: keep only filings whose org address ZIP maps to a county in **`01_data/reference/GEOID_reference.csv`** via **`01_data/reference/zip_to_county_fips.csv`**. Region is set from the GEOID reference (e.g. Cluster_name).

1. **location_processing**: Run **01_fetch_geoid_reference.py** → `GEOID_reference.csv`. Run **02_fetch_zip_to_county.py** → `zip_to_county_fips.csv`. See location_processing/README.md. (Optional: **03_build_zip_list_for_geoids.py** produces `zip_codes_in_benchmark_regions.csv` for legacy ZIP-list filtering.)

2. **990_irs**: Run **01** (index), **02** (uploads all ZIP parts), **03** (parses and keeps only rows whose org ZIP → county is in GEOID reference; sets `region` from the reference). **04** (merge) uses the same GEOID reference + zip_to_county when merging from silver parts.

No EIN list or BMF is required. Override with `--geoid-reference` and `--zip-to-county` if files are elsewhere.

## Commands (from repo root)

**Fetch BMF by state (optional; for BMF in raw/silver or reference EIN list):**
```bash
python python/ingest/990_irs/00_fetch_bmf.py
python python/ingest/990_irs/00_fetch_bmf.py --states sd mn mt az
```
Writes to `01_data/raw/irs_bmf/eo_{state}.csv`. Default states: sd, mn, mt, az.

**Fetch BMF and upload to S3 (same bucket as TEOS):**
```bash
python python/ingest/990_irs/00_fetch_bmf.py --upload
```
Uploads to `s3://{bucket}/bronze/irs990/bmf/eo_{state}.csv`. Override with `--bucket`, `--prefix`, `--region`; or env `IRS_990_S3_BUCKET`, `IRS_990_S3_BMF_PREFIX`, `AWS_DEFAULT_REGION`.

**Filter BMF to benchmark counties and upload to silver:**
```bash
python python/ingest/990_irs/00b_filter_bmf_to_benchmark_upload_silver.py
python python/ingest/990_irs/00b_filter_bmf_to_benchmark_upload_silver.py --no-upload
```
Reads raw BMF from `01_data/raw/irs_bmf/`, GEOID_reference.csv, and zip_to_county_fips.csv. Writes `01_data/staging/org/irs_bmf_benchmark_counties.parquet` and uploads to `s3://{bucket}/silver/irs990/bmf/bmf_benchmark_counties.parquet`. Use `--no-upload` to skip S3.

**Build EIN list** (optional reference; after 00_fetch_bmf and location_processing 01 + 02):
```bash
python python/ingest/990_irs/build_ein_list.py
```
Writes `01_data/reference/eins_in_benchmark_regions.csv` (EIN and optionally Region). Not used for filtering; pipeline filters by GEOID/county in 03/04.

**Upload indexes (all years 2021–current):**
```bash
python python/ingest/990_irs/01_upload_irs_990_index_to_s3.py
```
**Faster:** use **--workers N** to upload multiple years in parallel (e.g. `--workers 4`). Index files are streamed to S3 when the IRS sends Content-Length (no full buffer). Re-runs skip years already in S3 unless **--no-skip-existing**. Optional **--chunk-size MB** (default 8) for download chunks.

**Upload all ZIP parts** (filter by geography in 03/04):
```bash
python python/ingest/990_irs/02_upload_irs_990_zips_to_s3.py
```

**Faster uploads (script 02):** use **--workers N** to upload multiple ZIPs in parallel (e.g. `--workers 4`). The script uses one S3 list per year to skip existing ZIPs (instead of one head per ZIP), streams IRS→S3 when Content-Length is present, and uses 8 MB download chunks (override with **--chunk-size MB**).

**Custom year range:**
```bash
python python/ingest/990_irs/02_upload_irs_990_zips_to_s3.py --start-year 2022 --end-year 2025
```

**Verify S3 ZIPs match IRS source (size, or size + MD5):**
```bash
python tests/990_irs/verify_irs_vs_s3.py --year 2021
python tests/990_irs/verify_irs_vs_s3.py --year 2021 --zip 2021_TEOS_XML_01A.zip --hash
```
Uses HEAD for size-only; use `--hash` to download and compare MD5 (confirms byte-for-byte match). Only bulk-part ZIPs are checked.

**Override bucket/prefix/region:**
```bash
python python/ingest/990_irs/01_upload_irs_990_index_to_s3.py --bucket my-bucket --prefix bronze/irs990 --region us-east-2
```
Or set env: `IRS_990_S3_BUCKET`, `IRS_990_S3_PREFIX`, `AWS_DEFAULT_REGION`.

**Parse S3 ZIPs and write staging** (after 01 and 02). By default filters by **GEOID/county** (org ZIP → county in `GEOID_reference.csv` via `zip_to_county_fips.csv`) and sets region:
```bash
python python/ingest/990_irs/03_parse_irs_990_zips_to_staging.py
```
Ensure `01_data/reference/GEOID_reference.csv` and `01_data/reference/zip_to_county_fips.csv` exist (from location_processing 01 and 02). Override with `--geoid-reference` and `--zip-to-county`.

**Parse with custom year range or output path:**
```bash
python python/ingest/990_irs/03_parse_irs_990_zips_to_staging.py --start-year 2022 --end-year 2024 --output path/to/filings.parquet
```

**Faster runs: use lxml and parallel workers**
- Install **lxml** (`pip install lxml`) so the script uses it for XML parsing (typically 2–5× faster than stdlib). The script falls back to stdlib if lxml is not installed.
- Use **--workers N** to process N ZIPs in parallel (e.g. `--workers 4`). Helps when many ZIPs per year; use 2–4 to balance speed and memory.
- **--fast-parse**: single-pass regex extraction (no DOM). Fastest per-XML; use when schema is stable.
- **--stream-parse**: use lxml iterparse when not using --fast-parse. Lower memory and often faster for large XMLs.
- **--zip-workers N**: within each ZIP, parse N XMLs in parallel (default 1). Try 4–8 for large bulk-part ZIPs.
- **--zip-parse-batch N**: within each ZIP, read and parse at most N XMLs at a time (default: all). Lowers memory for huge ZIPs.
- **Faster ZIP I/O:**  
  - **--in-memory-mb MB**: keep ZIPs smaller than this in RAM (default 2000 = 2 GB). Lower if you need to reduce memory use.  
  - **--download-workers N**: for ZIPs &gt;20 MB, use N parallel S3 Range requests (default 4). Speeds up large single-ZIP download. Use `--download-workers 0` to disable.  
  - **--prefetch** (default on): when processing one ZIP at a time, download the next ZIP in the background while parsing the current one. Use `--no-prefetch` to disable.
```bash
python python/ingest/990_irs/03_parse_irs_990_zips_to_staging.py --workers 4
python python/ingest/990_irs/03_parse_irs_990_zips_to_staging.py --prefetch --download-workers 4 --in-memory-mb 200
```
- Run from a machine in the **same AWS region** as the bucket (e.g. us-east-2) for faster S3 downloads.

**Skip already processed ZIPs (resume / incremental):**  
If the output file exists and contains a `source_zip` column, only S3 keys not yet in that file are downloaded and parsed; new rows are merged with existing and the file is overwritten.
```bash
python python/ingest/990_irs/03_parse_irs_990_zips_to_staging.py --skip-processed
```

**Write both filtered and full (unfiltered) Parquet in one run:**  
Use `--output-full PATH` to write the full unfiltered table to `PATH` before applying the geography filter to the main output. When `--output-full` is set, the script writes that to `--output-full` and applies the GEOID/county filter for the main `--output`. One run yields both files.
```bash
python python/ingest/990_irs/03_parse_irs_990_zips_to_staging.py --output-full 01_data/staging/filing/irs_990_filings_full.parquet
```

**Silver layer (per-ZIP Parquet), then merge:**  
Use `--output-parts-dir DIR` so 03 writes one Parquet per ZIP under `DIR/year=YYYY/` (e.g. `2021_TEOS_XML_01A.parquet`). Parse is unfiltered so each part is complete. Then run 04 to merge and apply the geography filter without re-parsing XML. This also keeps 04 from holding all rows in memory (rows are written per ZIP).

**Memory-limited runs:** Use **--output-parts-dir** and then 04 to merge, so 03 never accumulates every row in memory. For very large bulk ZIPs, add **--zip-parse-batch 2000** (or similar) so each ZIP only holds that many XML bytes in memory at once.
```bash
# One-time: parse and write silver parts + main staging
python python/ingest/990_irs/03_parse_irs_990_zips_to_staging.py --output-parts-dir 01_data/staging/filing/parts --output 01_data/staging/filing/irs_990_filings.parquet

# Later: merge from silver only, no re-parse
python python/ingest/990_irs/04_merge_990_parts_to_staging.py --parts-dir 01_data/staging/filing/parts --output 01_data/staging/filing/irs_990_filings.parquet
```

## Output in S3

- **Index:** `s3://{bucket}/{prefix}/index/year={YEAR}/index_{YEAR}.csv`
- **ZIPs:** `s3://{bucket}/{prefix}/zips/year={YEAR}/{YEAR}_TEOS_XML_{PART}.zip`

Example: `s3://swb-321-irs990-teos/bronze/irs990/teos_xml/zips/year=2024/2024_TEOS_XML_01A.zip`

## Staging output (script 03)

- **Path:** `01_data/staging/filing/irs_990_filings.parquet` (or `.csv` if pyarrow is not installed). With `--output-full PATH`, a second file at `PATH` contains all parsed rows (unfiltered).
- **Columns:** `ein`, `tax_year`, `form_type`, `filing_ts`, `tax_period_begin_dt`, `tax_period_end_dt`, `org_name`, `address_line1`, `city`, `state`, `zip`, `exempt_purpose_txt`, `subsection_code`, plus financials (`revenue`, `expenses`, `assets`, `net_assets`, `total_liabilities`, `program_service_expenses`, `management_general_expenses`, `fundraising_expenses`, `contributions_grants`, `program_service_revenue`, `grants_paid`), `source_file`, `source_zip`, and optionally `region` when using the GEOID reference (region from benchmark counties). With **--fast-parse**, `org_name` and address fields are not extracted (remain None). See `docs/990/990_irs_parser_extraction_gap.md` for full mapping.

## Performance and optimization

Current bottlenecks in step 04 (parse):

1. **Full parse of every XML** — The script parses every filing in each ZIP (e.g. ~590k in the 2021 bulk ZIP) and applies the geography filter (GEOID/county) afterward.
2. **Processing non-ZIP S3 objects** — If the bucket has many small objects that are HTML error pages (e.g. `2021_TEOS_XML_17903370.zip`), the script lists and downloads each, then skips. **Optimization:** Restrict to bulk-part keys only (e.g. `{YEAR}_TEOS_XML_01A.zip`, `01B.zip`, …) so only real ZIPs are fetched.
3. **Single-threaded** — One ZIP at a time, one XML at a time. **Optimization:** Process multiple ZIPs in parallel (overlap download with parse), or parse XMLs within a ZIP in parallel (e.g. `concurrent.futures.ProcessPoolExecutor`). Requires care with memory.
4. **Faster XML parser** — `xml.etree.ElementTree` is pure Python. **Optimization:** Use `lxml` (C extension) for full parsing if available; same API, much faster for large batches.
5. **Memory** — All parsed rows are held in memory, then written once. **Optimization:** Write Parquet in chunks (e.g. every 50k rows) to reduce peak memory and allow incremental progress.

Script 04 implements (1) bulk-part key filtering, (2) **lxml** for XML parsing when installed (fallback to stdlib), (3) **--workers N** for parallel ZIP processing, (4) **stream-to-temp** for large S3 objects (ZIP body streamed to a temp file when size > --in-memory-mb, default 2 GB), and (5) **single-pass DOM** extraction (one tree walk per XML). Use **--fast-parse** for regex-only extraction (no DOM, faster; slightly more fragile to schema changes). See **docs/990/990_irs_efficiency_overhaul.md** for more options (streaming IRS→S3 in 02, silver layer, run in AWS).

## SQL database vs Python filter

**Current approach:** Parse in Python, filter by geography (GEOID/county) in Python, write staging Parquet. Downstream, **sql/staging** and **sql/curated** run over that Parquet (e.g. DuckDB reads Parquet directly; no separate “load into DB” step). Curated and analysis then use the filtered staging table.

**Alternative: load all parsed data into a SQL database, then filter with SQL**

- **How it would work:** Parse every filing; write to a table in DuckDB, PostgreSQL, or similar. Then run geography-based filters in SQL (e.g. join to zip→county and GEOID reference) to get the benchmark-region subset. Optional: build indexes on `ein`, `tax_year`, `zip` for fast filtered queries.
- **Pros:** One full parse and load; filter and aggregations can change in SQL without re-parsing; ad-hoc queries (different regions, year ranges, form types) without re-running the parser; fits a “raw in DB, refine in SQL” pattern.
- **Cons:** Must parse and store all filings (e.g. ~590k+ per year for 2021), so parse time and storage are higher than the current geography-filtered staging table; requires a database (file-based DuckDB is enough and matches the project’s `01_data/database/` pattern).

**When SQL filtering is a better fit**

- Multiple different region definitions or filters without re-parsing.
- Heavy ad-hoc exploration (many one-off SQL queries).
- Downstream pipeline is already SQL-heavy and expects to read from a single DB.

**When the current approach (filter in Python, staging Parquet) is a better fit**

- One primary geography (benchmark counties) and stable use case: filter once during parse, keep staging small, then run SQL on staging/curated as today.
- Minimal storage and no DB server: staging stays as Parquet in OneDrive; DuckDB can query Parquet directly (see **sql/README.md** and **docs/onedrive/OneDrive_Structure.md**).
- Parsing is the bottleneck; storing and indexing everything in a DB does not reduce parse cost.

**Recommendation for this project:** Keep the current design: filter by geography during parse; write staging Parquet; run **sql/staging** and **sql/curated** (e.g. DuckDB over Parquet) for further refinement. If the need arises for multiple region sets or heavy ad-hoc SQL, add an option to write an unfiltered staging table and load it into `01_data/database/swb_321.duckdb` (or similar), then apply geography and other filters in SQL when building curated or running analysis.

## Format and conversion

**Source (IRS):** Data is only available as **XML inside ZIPs**. There is no official Parquet/CSV from the IRS for the TEOS 990 bulk files, so the pipeline must download ZIPs and parse XML. Converting the *source* to another format is not an option; the only lever is what we do *after* parsing.

**Staging output:** The script already writes **Parquet** (with CSV fallback). Parquet is an efficient format: columnar, compressed, and supports predicate pushdown (e.g. DuckDB can read only needed columns or row groups). Converting the staging output to a “better” format (e.g. different compression or layout) usually gives only small gains; Parquet is already a good choice.

**More efficient pipeline: “convert once” to Parquet by source**

A more efficient *pipeline* is to separate “parse raw once” from “filter and merge”:

1. **One-time (or periodic) conversion:** Run a job that reads each S3 ZIP, parses XML, and writes **one Parquet file per ZIP** (e.g. `staging/filing/year=2021/2021_TEOS_XML_01A.parquet`) to S3 or OneDrive. No geography filter at this stage; just XML → Parquet.
2. **Downstream step:** Read those Parquet files (e.g. `read_parquet('staging/filing/**/*.parquet')`), apply geography filter and region join, optionally aggregate into a single table. This step is fast (no XML parsing, no ZIP download) and can be run often with different region definitions.

**Benefits:** Parsing and ZIP download happen once per ZIP. Reruns and ad-hoc filters only read Parquet. Same total parse work up front, but later runs are much faster. Aligns with a “bronze (ZIP/XML) → silver (Parquet by source) → gold (filtered/curated)” layout.

**Implementation options:** (a) Extend script 03 to optionally write **per-ZIP Parquet** under a directory (e.g. `staging/filing/parts/year=2021/`) and add a small merge step that reads those and applies the geography filter (script 04 does this); or (b) run 03 once, write one large staging Parquet, then use DuckDB/SQL to filter and export the benchmark subset. Option (a) avoids re-downloading and re-parsing when new years are added (only new ZIPs get new Parquet files).
