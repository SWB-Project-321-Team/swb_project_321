# IRS SOI County Data -> Local Raw -> S3 -> Benchmark Filter

Scripts that fetch the latest published **IRS Statistics of Income county release**, preserve the
official raw assets locally, upload them to the project S3 bucket, and build separate
benchmark-county filtered CSVs.

## What this pipeline does

For the resolved tax year, the pipeline preserves exactly three official IRS assets:

- county all-states CSV including AGI
- county all-states CSV excluding AGI
- County Income Data Users Guide DOCX

The raw files remain immutable. A later step builds benchmark-filtered county CSVs from the two
raw county CSVs using the same benchmark county scope and region labels as the `990_irs` pipeline.

## Run order

Run from repo root:

```bash
python python/ingest/irs_soi/01_discover_county_release.py
python python/ingest/irs_soi/02_download_county_release.py
python python/ingest/irs_soi/03_upload_county_release_to_s3.py
python python/ingest/irs_soi/04_verify_county_release_source_local_s3.py
python python/ingest/irs_soi/05_filter_county_release_to_benchmark_local.py
python python/ingest/irs_soi/06_upload_filtered_county_release_to_s3.py
```

Or use the runner:

```bash
python python/run_irs_soi_county.py
```

## Defaults

- Bucket: `swb-321-irs990-teos`
- Region: `us-east-2`
- Raw prefix: `bronze/irs_soi/county/raw`
- Metadata prefix: `bronze/irs_soi/county/metadata`
- Filtered prefix: `silver/irs_soi/county`

## Local outputs

Raw:

- `01_data/raw/irs_soi/county/raw/tax_year=YYYY/<official filename>`
- `01_data/raw/irs_soi/county/metadata/latest_release.json`
- `01_data/raw/irs_soi/county/metadata/release_manifest_tax_year=YYYY.csv`
- `01_data/raw/irs_soi/county/metadata/size_verification_tax_year=YYYY.csv`

Filtered:

- `01_data/staging/irs_soi/tax_year=YYYY/irs_soi_county_benchmark_agi_YYYY.csv`
- `01_data/staging/irs_soi/tax_year=YYYY/irs_soi_county_benchmark_noagi_YYYY.csv`
- `01_data/staging/irs_soi/tax_year=YYYY/filter_manifest_YYYY.csv`

## Filtering logic

The county CSV is already county-level. The filter therefore does **not** use ZIP-to-county
crosswalks at runtime.

It matches the current `990_irs` benchmark geography semantics by:

1. normalizing `STATEFIPS` to 2 digits
2. normalizing `COUNTYFIPS` to 3 digits
3. building `county_fips = STATEFIPS + COUNTYFIPS`
4. excluding state totals where `COUNTYFIPS == 000`
5. keeping only counties present in `GEOID_reference.csv`
6. populating `region` from the same region/cluster column detection used by `990_irs`

## Notes

- `--year latest` is the default for every step.
- `05_filter_county_release_to_benchmark_local.py` supports `--source-type agi|noagi|both`.
- The scripts print per-step status, progress bars, and elapsed times so long runs are visible.
