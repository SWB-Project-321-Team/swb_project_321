# Raw Data Dictionaries

This folder contains public raw-source dictionaries fetched from source websites, plus small generated companion CSVs that make the fetched documentation easier to audit.

## Fetched Source Documentation

- `irs_eo_bmf/eo-info.pdf`
  - Source: `https://www.irs.gov/pub/irs-soi/eo-info.pdf`
  - Applies to: IRS EO BMF state files and NCCS raw monthly BMF snapshots, because NCCS documents raw BMF as unmodified IRS EO BMF files.
- `irs_990n_postcard/990n-data-dictionary.pdf`
  - Source: `https://www.irs.gov/pub/irs-tege/990n-data-dictionary.pdf`
  - Applies to: Form 990-N / e-Postcard raw fields.
- `nccs_efile/data-dictionary.html`
  - Source: `https://nonprofit-open-data-collective.github.io/irs990efile/data-dictionary/data-dictionary.html`
  - Applies to: NCCS efile raw public tables.
- `nccs_bmf_legacy_2022/dd_unavailable.html`
  - Source: `https://urbaninstitute.github.io/nccs/catalogs/dd_unavailable.html`
  - Applies to: NCCS legacy BMF 2022 profile links. This documents that the official per-file profile dictionary is unavailable.
- `givingtuesday_datamarts/datamart_catalog.csv` and `givingtuesday_datamarts/datamart_fields.csv`
  - Source: GivingTuesday public catalog API captured by the pipeline.
  - Applies to: GivingTuesday raw datamart discovery and field metadata.
- `nccs_990_core/CORE-HRMN_dd.csv`, `nccs_990_core/DD-PF-HRMN-V0.csv`, and `nccs_990_core/harmonized_data_dictionary.xlsx`
  - Source: NCCS Core/BMF catalog metadata mirrored from S3.
  - Applies to: NCCS 990 Core raw public charity, private foundation, and BMF bridge fields.

## Generated Companions

- `nccs_efile/data-dictionary_variables.csv`
  - Parsed from the public NCCS efile HTML dictionary.
- `nccs_bmf_legacy_2022/BMF-2022-08-501CX-NONPROFIT-PX_header_dictionary.csv`
  - Inferred from the actual raw CSV header because NCCS does not publish a 2022 legacy profile dictionary.
- `nccs_bmf_raw_monthly/2026-03-BMF_header_dictionary.csv`
  - Inferred from the latest local raw monthly BMF CSV header. Definitions come from the IRS EO BMF PDF.
- `irs_990n_postcard/2026-03-E-POSTCARD_header_dictionary.csv`
  - Inferred from the latest local raw e-Postcard CSV header. Definitions come from the IRS 990-N PDF.

## Manifests

- `_source_download_manifest.csv` records fetched source URLs, local paths, sizes, hashes, and applicability notes.
- `_generated_companion_manifest.csv` records generated companion CSV paths, sizes, hashes, and notes.

These files are included in the client-ready package. In this repository they live under `docs/final_preprocessing_docs/technical_docs/source_dictionaries/`; published S3 keys use `documentation/final_preprocessing_docs/technical_docs/source_dictionaries/`.
