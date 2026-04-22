# location_processing — Census and GEOID scripts

Scripts for Census/ACS location data and GEOID reference files used by the benchmark regions. They write to project data paths under `01_data/` (or a configurable root).

## Flow at a glance

```
[No project file inputs]
         │
    fetch_location_data.py   ──►  locations.csv  (all US counties: GEOID, County, State, City, ZIPs, …)
         │
[No project file inputs; 18 GEOIDs hardcoded in script]
         │
    01_fetch_geoid_reference.py  ──►  GEOID_reference.csv   (18 rows: County, State, GEOID, Cluster_ID, Cluster_name, ZIPs)
                                  ──►  geoid_zip_codes.csv  (GEOID, ZIP)
         │
[No project file inputs; downloads Census ZCTA–county or --local CSV]
         │
    02_fetch_zip_to_county.py  ──►  zip_to_county_fips.csv  (ZIP, FIPS)
         │
         │   Reads: GEOID_reference.csv (from 01) or GEOID_reference.xlsx
         │          zip_to_county_fips.csv (from 02_fetch_zip_to_county or you provide)
         ▼
    03_build_zip_list_for_geoids.py  ──►  zip_codes_in_benchmark_regions.csv  (ZIP, GEOID, Region)
```

- **01_fetch_geoid_reference** does not read any project files; it only calls external APIs or uses hardcoded GEOIDs.
- **02_fetch_zip_to_county** produces `zip_to_county_fips.csv` (Census ZCTA–county or `--local` CSV). **03_build_zip_list_for_geoids** reads project files: the GEOID reference (xlsx or 01’s CSV) and that file, and writes the benchmark ZIP list used by 990_irs.

## Purpose

- Fetch US county GEOIDs (5-digit FIPS) and related reference data from the Census API.
- Build the 18 benchmark counties GEOID reference and ZIP-to-region lists for the 990 and other pipelines.

## Scripts (run order)

| Order | Script | Description |
|-------|--------|-------------|
| — | **fetch_location_data.py** | Fetches US county GEOIDs from Census 2020 Decennial API. Writes GEOID, County, State, State_FIPS, County_FIPS, NAME_full, City, ZIPs. City = county seat; ZIPs = semicolon-separated ZCTAs. |
| 1 | **01_fetch_geoid_reference.py** | Builds the 18 benchmark counties reference: `GEOID_reference.csv` (same shape as project GEOID_reference.xlsx) and `geoid_zip_codes.csv` (GEOID, ZIP). Does not read the xlsx; counties are defined in script. |
| 2 | **02_fetch_zip_to_county.py** | Downloads Census ZCTA–county (or normalizes `--local` CSV) and writes `zip_to_county_fips.csv` (ZIP, FIPS). No project file inputs. |
| 3 | **03_build_zip_list_for_geoids.py** | Builds `01_data/reference/zip_codes_in_benchmark_regions.csv` (ZIP, GEOID, Region) for every ZIP in the 18 benchmark counties. Reads GEOID reference (01's CSV or xlsx) and `zip_to_county_fips.csv` (from 02_fetch_zip_to_county or you provide). |

Run **03** after GEOID reference and `zip_to_county_fips.csv` exist (run **02_fetch_zip_to_county.py** to produce the latter).

## Flow: what each script reads and writes

All paths under `01_data/reference/` (or your `DATA/reference`). **Inputs** = files/APIs the script reads. **Outputs** = files the script creates.

| Script | Inputs (read only) | Outputs (created/overwritten) |
|--------|--------------------|--------------------------------|
| **fetch_location_data.py** | No project files. Fetches from **Census APIs** (state list, decennial county, ZCTA–county). | **`locations.csv`** — all US counties (or `--states` subset): GEOID, County, State, State_FIPS, County_FIPS, NAME_full, City, ZIPs. |
| **01_fetch_geoid_reference.py** | No project files. Uses **18 GEOIDs + Cluster_name** hardcoded in script; fetches county names/state from Census API or Healy FIPS CSV; fetches ZCTA–county from Census URL. | **`GEOID_reference.csv`** — 18 rows: County, State, GEOID, Cluster_ID, Cluster_name, ZIPs. **`geoid_zip_codes.csv`** — GEOID, ZIP (all ZIPs in those counties). |
| **02_fetch_zip_to_county.py** | No project files. **Census URL** (ZCTA–county) or **`--local`** CSV (e.g. HUD). | **`zip_to_county_fips.csv`** — ZIP, FIPS (one row per ZIP). |
| **03_build_zip_list_for_geoids.py** | **`GEOID_reference.csv`** (from 01; fallback: xlsx); **`zip_to_county_fips.csv`** (from 02_fetch_zip_to_county or you provide). | **`zip_codes_in_benchmark_regions.csv`** — ZIP, GEOID, Region (only ZIPs in the 18 counties; Region from ref). |

So: **01** doesn’t read any project files (only APIs/hardcoded data). **02** fetches the ZIP–county crosswalk. **03** reads the GEOID reference (01’s CSV, or xlsx if CSV missing) plus zip_to_county_fips, and writes the benchmark ZIP list used by 990_irs.

## Main inputs / outputs (summary)

| Script | Main inputs | Main outputs |
|--------|-------------|--------------|
| fetch_location_data.py | Census API (no key) | `data/reference/locations.csv` (default) or `--out` path |
| 01_fetch_geoid_reference.py | Census / FIPS URLs; 18 counties in script | `GEOID_reference.csv`, `geoid_zip_codes.csv` |
| 02_fetch_zip_to_county.py | Census ZCTA–county URL or `--local` CSV | `zip_to_county_fips.csv` |
| 03_build_zip_list_for_geoids.py | `GEOID_reference.csv` (from 01; fallback: xlsx), `zip_to_county_fips.csv` | `zip_codes_in_benchmark_regions.csv` |

## Commands (from repo root)

```bash
# All US counties (default output: data/locations.csv)
python python/ingest/location_processing/fetch_location_data.py

# Limit to specific state FIPS (e.g. 04, 27, 30, 46)
python python/ingest/location_processing/fetch_location_data.py --states 04 27 30 46

# GEOID reference and geoid–ZIP list
python python/ingest/location_processing/01_fetch_geoid_reference.py

# ZIP→county FIPS crosswalk (Census default; or --local path/to/file.csv)
python python/ingest/location_processing/02_fetch_zip_to_county.py

# ZIP list for benchmark regions (run after GEOID reference and zip_to_county_fips.csv)
python python/ingest/location_processing/03_build_zip_list_for_geoids.py
```

## Integration with 990_irs pipeline (no 990_givingtuesday)

You can run **benchmark-region** 990 staging using **only** location_processing and 990_irs:

1. **location_processing**: Run **01_fetch_geoid_reference.py** → `GEOID_reference.csv`. Run **02_fetch_zip_to_county.py** → `zip_to_county_fips.csv`. Then run **03_build_zip_list_for_geoids.py** (reads both). That produces **`01_data/reference/zip_codes_in_benchmark_regions.csv`** (ZIP, GEOID, Region).

2. **990_irs**: Run **01** (index), **02** with **--all-parts** (upload all ZIPs; no EIN list), then **03** with **--benchmark-zips** (default path is that same CSV). Script 03 keeps only rows whose **org address ZIP** is in the benchmark list and sets **region** from the CSV. Script **04** (merge from silver parts) also accepts **--benchmark-zips** with the same behavior.

So **990_irs** reads **only** the benchmark ZIP list from location_processing. No EIN list and no 990_givingtuesday pipeline are required.

**Run order (location_processing + 990_irs only):**

```bash
# 1. Geography (fetch ZIP→county, then GEOID reference, then benchmark ZIP list)
python python/ingest/location_processing/02_fetch_zip_to_county.py
python python/ingest/location_processing/01_fetch_geoid_reference.py
python python/ingest/location_processing/03_build_zip_list_for_geoids.py

# 2. 990_irs: upload all parts, then parse and filter by benchmark ZIPs
python python/ingest/990_irs/01_upload_irs_990_index_to_s3.py
python python/ingest/990_irs/02_upload_irs_990_zips_to_s3.py
python python/ingest/990_irs/03_parse_irs_990_zips_to_staging.py --benchmark-zips 01_data/reference/zip_codes_in_benchmark_regions.csv
```

If `zip_codes_in_benchmark_regions.csv` is already at the default path, you can omit the path: `--benchmark-zips` alone uses `01_data/reference/zip_codes_in_benchmark_regions.csv`.

## Dependencies

- **fetch_location_data.py**: `requests`, `pandas`, `tqdm`
- **01_fetch_geoid_reference.py**: `requests`, `pandas`
- **02_fetch_zip_to_county.py**: `requests`, `pandas`
- **03_build_zip_list_for_geoids.py**: `pandas`, `openpyxl`

See also **python/ingest/README.md** (location_processing section and output checklist).
