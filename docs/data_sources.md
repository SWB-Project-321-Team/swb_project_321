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
