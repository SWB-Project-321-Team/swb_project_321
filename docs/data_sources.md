# Data Sources

This project uses a mix of nonprofit-filing sources, benchmark geography references, and public
context datasets. Local file paths are environment-specific through `SWB_321_DATA_ROOT`.

## IRS Form 990 sources

- **IRS TEOS XML bulk files**
  - Purpose: filing-level nonprofit returns for the `990_irs` pipeline
  - Storage: S3 bronze under `bronze/irs990/teos_xml/`, staging output under `01_data/staging/filing/`
- **IRS EO BMF**
  - Purpose: exempt organization reference extracts by state
  - Storage: `01_data/raw/irs_bmf/`

## IRS SOI county data

- **IRS Statistics of Income county data**
  - Source landing page: `https://www.irs.gov/statistics/soi-tax-stats-county-data`
  - Purpose: county-level tax-return aggregates for benchmark-region context and giving/income proxies
  - Raw local storage: `01_data/raw/irs_soi/county/raw/tax_year=YYYY/`
  - Raw S3 storage: `bronze/irs_soi/county/raw/tax_year=YYYY/`
  - Metadata local storage: `01_data/raw/irs_soi/county/metadata/`
  - Metadata S3 storage: `bronze/irs_soi/county/metadata/`
  - Filtered local storage: `01_data/staging/irs_soi/tax_year=YYYY/`
  - Filtered S3 storage: `silver/irs_soi/county/tax_year=YYYY/`
  - Files preserved in v1:
    - all-states county CSV including AGI
    - all-states county CSV excluding AGI
    - County Income Data Users Guide DOCX

## NCCS Core 990 data

- **NCCS Core Series**
  - Source catalog: `https://nccs.urban.org/nccs/catalogs/catalog-core.html`
  - Purpose: official harmonized 990-family extracts used as a benchmark-filtered alternative to IRS TEOS XML
  - Coverage rule in this pipeline: latest common complete year across required PZ/PC/PF Core families
  - Raw local storage: `01_data/raw/nccs_990/core/raw/year=YYYY/`
  - Raw S3 storage: `bronze/nccs_990/core/raw/year=YYYY/`
  - Metadata local storage: `01_data/raw/nccs_990/core/metadata/`
  - Metadata S3 storage: `bronze/nccs_990/core/metadata/`
- **NCCS Unified BMF**
  - Source catalog: `https://nccs.urban.org/nccs/catalogs/catalog-bmf.html`
  - Purpose: geography bridge for mapping Core EINs to benchmark counties
  - Local bridge storage: `01_data/raw/nccs_990/core/bridge_bmf/state=XX/`
  - S3 bridge storage: `bronze/nccs_990/core/bridge_bmf/state=XX/`
- **NCCS Core benchmark-filtered outputs**
  - Purpose: county-benchmark subset of each selected Core raw file
  - Filtered local storage: `01_data/staging/nccs_990/core/year=YYYY/`
  - Filtered S3 storage: `silver/nccs_990/core/year=YYYY/`

## NCCS e-Postcard 990-N data

- **NCCS 990-N e-Postcard monthly snapshots**
  - Source page: `https://nccs.urban.org/nccs/datasets/postcard/`
  - Raw source pattern: `https://nccsdata.s3.us-east-1.amazonaws.com/raw/e-postcard/YYYY-MM-E-POSTCARD.csv`
  - Purpose: official monthly snapshot of small nonprofits filing Form 990-N
  - Coverage rule in this pipeline: latest snapshot year, with all available months in that year preserved raw
  - Raw local storage: `01_data/raw/nccs_990/postcard/raw/snapshot_year=YYYY/snapshot_month=YYYY-MM/`
  - Raw S3 storage: `bronze/nccs_990/postcard/raw/snapshot_year=YYYY/snapshot_month=YYYY-MM/`
  - Metadata local storage: `01_data/raw/nccs_990/postcard/metadata/`
  - Metadata S3 storage: `bronze/nccs_990/postcard/metadata/`
- **NCCS e-Postcard benchmark-filtered output**
  - Purpose: combined annual benchmark-county derivative built from the raw monthly postcard snapshots
  - Filtered local storage: `01_data/staging/nccs_990/postcard/snapshot_year=YYYY/`
  - Filtered S3 storage: `silver/nccs_990/postcard/snapshot_year=YYYY/`

## NCCS BMF 2022-present yearly snapshots

- **NCCS Legacy BMF + raw monthly BMF archives**
  - Source overview: `https://nccs.urban.org/nccs/datasets/bmf/`
  - Source catalog: `https://nccs.urban.org/nccs/catalogs/catalog-bmf.html`
  - Purpose: yearly reference-snapshot BMF coverage for `2022-present`, replacing the old current-only IRS BMF input in the combined source-union pipeline
  - Coverage rule in this pipeline:
    - `2022`: latest available legacy 2022 BMF file
    - `2023+`: one representative raw monthly BMF snapshot per year
    - completed years prefer December; current year uses the latest available month
  - Raw local storage: `01_data/raw/nccs_bmf/raw/year=YYYY/`
  - Raw S3 storage: `bronze/nccs_bmf/raw/year=YYYY/`
  - Metadata local storage: `01_data/raw/nccs_bmf/metadata/`
  - Metadata S3 storage: `bronze/nccs_bmf/metadata/`
- **NCCS BMF benchmark-filtered yearly outputs**
  - Purpose: benchmark-county subset of the selected yearly BMF snapshots
  - Filtered local storage: `01_data/staging/nccs_bmf/year=YYYY/`
  - Filtered S3 storage: `silver/nccs_bmf/year=YYYY/`

## Combined filtered source union

- **Combined filtered 990 source-union table**
  - Purpose: one source-preserving union of the already-filtered GivingTuesday, NCCS postcard, NCCS Core charities/nonprofits, and NCCS BMF yearly outputs
  - Local storage: `01_data/staging/combined_990/`
  - S3 storage: `silver/combined_990/`
  - Key outputs:
    - `combined_990_filtered_source_union.parquet`
    - `source_input_manifest.csv`
    - `column_dictionary.csv`
    - `field_availability_matrix.csv`
    - overlap diagnostics CSVs
    - `build_summary.json`

## Benchmark geography references

- **GEOID_reference.csv**
  - Purpose: benchmark county inclusion and region labels
  - Storage: `01_data/reference/GEOID_reference.csv`
- **zip_to_county_fips.csv**
  - Purpose: ZIP-to-county mapping used by filing-level pipelines such as `990_irs`
  - Storage: `01_data/reference/zip_to_county_fips.csv`

## Other public sources

- **GivingTuesday 990 Data Commons**
  - Purpose: alternate 990 pipeline and datamart-based benchmark outputs
- **Census / ACS**
  - Purpose: population and community context variables
- **BLS / CPI / other public statistics**
  - Purpose: inflation adjustment and context metrics as needed
