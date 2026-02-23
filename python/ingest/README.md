# python/ingest/

Scripts that pull **raw** data from sources and write to OneDrive `01_data/raw/`.

- IRS 990 extracts, Giving Tuesday data, Census/ACS, or other APIs/files.
- Output goes to the appropriate subfolder under `01_data/raw/` (e.g. `irs_990/`, `census_acs/`).
- No data or credentials in this repo—paths and secrets from environment or config.

## location_processing/ — Census location and GEOID scripts

- **fetch_location_data.py** — Fetches US county GEOIDs (5-digit FIPS) from the Census 2020 Decennial API. Writes a CSV with GEOID, County, State, State_FIPS, County_FIPS, NAME_full, City, ZIPs. City is the county seat (from an external list); ZIPs are semicolon-separated ZCTAs. Default output: `data/locations.csv`. Use `--states 04 27 30 46` to limit to specific states; omit for all US. Depends: `requests`, `pandas`.
- **fetch_geoid_reference.py** — Writes `GEOID_reference.csv` with the same 18 rows and columns as the project’s GEOID_reference.xlsx (County, State, GEOID, Cluster_ID, Cluster_name) plus a ZIPs column from the Census ZCTA–county file. Does not read the xlsx; the 18 benchmark counties are defined in the script. Also writes `geoid_zip_codes.csv` (GEOID, ZIP). Depends: `requests`, `pandas`.
- **build_zip_list_for_geoids.py** — Builds `01_data/reference/zip_codes_in_benchmark_regions.csv` (ZIP, GEOID, Region) for every ZIP that falls in the 18 benchmark counties. Uses the same `GEOID_reference.xlsx` and `zip_to_county_fips.csv` as the 990 pipeline (run `990_givingtuesday/01_fetch_zip_to_county.py` first). Depends: `pandas`, `openpyxl`.

Run from repo root, e.g. `python python/ingest/location_processing/fetch_location_data.py`.

## 990_givingtuesday/ — Form 990 pipeline (run in order)

Scripts to fetch Form 990 data for the benchmark regions. Run **01 → 02 → 03 → 04** from repo root. See `990_givingtuesday/README.md` and docs/990_data_fetch_plan.md.

| Step | Script | Output |
|------|--------|--------|
| 01 | `990_givingtuesday/01_fetch_zip_to_county.py` | `01_data/reference/zip_to_county_fips.csv` |
| 02 | `990_givingtuesday/02_fetch_bmf.py` | `01_data/raw/irs_bmf/eo_*.csv` |
| 03 | `990_givingtuesday/03_build_ein_list.py` | `01_data/reference/eins_in_benchmark_regions.csv` |
| 04 | `990_givingtuesday/04_fetch_990_givingtuesday.py` | `01_data/raw/givingtuesday_990/` (JSON + combined CSV) |

## 990_irs/ — IRS TEOS 990 XML → S3 (2021–present)

Scripts that stream **IRS Form 990 series (e-file) XML** from the IRS TEOS downloads **directly to the project AWS bucket** (no large local files). Run **01** then **02**. See `990_irs/README.md` and docs/990_irs_teos_s3_plan.md.

| Step | Script | Output |
|------|--------|--------|
| 01 | `990_irs/01_upload_irs_990_index_to_s3.py` | S3: `{prefix}/index/year={YEAR}/index_{YEAR}.csv` |
| 02 | `990_irs/02_upload_irs_990_zips_to_s3.py` | S3: `{prefix}/zips/year={YEAR}/{YEAR}_TEOS_XML_*.zip` (optionally filtered by EIN list) |

Default bucket: `swb-321-irs990-teos`, prefix: `bronze/irs990/teos_xml`. Depends: `requests`, `pandas`, `boto3`.

---

## Output file checklist (01_data paths)

| Script | Output | Columns / contents | Sanity check |
|--------|--------|--------------------|--------------|
| 990_givingtuesday/01_fetch_zip_to_county | reference/zip_to_county_fips.csv | ZIP (5-digit), FIPS (5-digit county); one row per ZIP (county = max land overlap) | ~33.8k rows; SD 57xxx→46xxx, IN 46xxx→18xxx |
| 990_givingtuesday/02_fetch_bmf | raw/irs_bmf/eo_&lt;state&gt;.csv | EIN, NAME, STREET, CITY, STATE, ZIP, … (IRS BMF columns) | One CSV per state (default: sd, mn, mt, az) |
| 990_givingtuesday/03_build_ein_list | reference/eins_in_benchmark_regions.csv | EIN (9-digit, leading zeros) | One EIN per row; count ≈ orgs in 18 benchmark counties from BMF |
| 990_givingtuesday/04_fetch_990_givingtuesday | raw/givingtuesday_990/api_responses/ein_*.json | statusCode, body.query, body.no_results, body.results | One JSON per EIN; many have results: [] |
| 990_givingtuesday/04_fetch_990_givingtuesday | raw/givingtuesday_990/990_basic120_combined.csv | Basic 120 fields, one row per EIN×TAXYEAR (2021+) | Written only after full run completes |
| location_processing/fetch_location_data | data/locations.csv (default) | GEOID, County, State, State_name, State_FIPS, County_FIPS, NAME_full, City, ZIPs | State_name and ZIPs populated; City = county seat |
| location_processing/fetch_geoid_reference | reference/GEOID_reference.csv, geoid_zip_codes.csv | GEOID, Cluster_ID, Cluster_name, ZIPs; GEOID, ZIP | 18 benchmark rows; ZIPs from ZCTA–county |
| location_processing/build_zip_list_for_geoids | reference/zip_codes_in_benchmark_regions.csv | ZIP, GEOID, Region | One row per ZIP in 18 counties |
