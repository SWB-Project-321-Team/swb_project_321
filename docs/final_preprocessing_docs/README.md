# Final Preprocessing Documentation Package

This folder is the local final preprocessing documentation package for `s3://swb-321-irs990-teos/`.

Paths below use `**docs/final_preprocessing_docs/**` because this directory lives under the repository `docs/` tree. The same files are published on S3 under `**documentation/final_preprocessing_docs/**` (swap `docs` → `documentation` for bucket keys).

## Read First

- Start with `general_docs/markdown/` for short dataset-by-dataset client summaries.
- Use `general_docs/word/` for Word delivery copies of the same client-facing summaries.
- Use `technical_docs/pipeline_docs/` for detailed source locations, retrieval logic, region filtering, processing steps, final outputs, and validation.
- Use `technical_docs/analysis_variable_mappings/` for the exact analysis variables, provenance columns, unavailable-variable explanations, and coverage evidence.
- Use `technical_docs/quality/coverage_evidence/` for the published analysis variable coverage CSVs.
- Use `technical_docs/source_dictionaries/` for source dictionaries and generated raw-header companions.
- Use `technical_docs/s3_metadata/` for supporting S3 manifests, raw catalog snapshots, size checks, and schema evidence. Source dictionary CSVs are kept under `technical_docs/source_dictionaries/` instead of duplicated here.

## Published S3 Layout

The six target families publish analysis documentation under a docs/coverage split:

- Pipeline docs: `silver/<family>/analysis/documentation/`
- Variable mappings: `silver/<family>/analysis/variable_mappings/`
- Coverage evidence: `silver/<family>/analysis/quality/coverage/`

The former flat analysis metadata target objects were removed after the new keys were uploaded and verified.

## Included Artifacts

This package includes:

- Standardized client-facing wrapper documents in Markdown and Word format for IRS EO BMF, NCCS BMF, NCCS efile, NCCS 990 Core, NCCS 990 Postcard, and GivingTuesday 990 DataMart.
- Pipeline Markdown for IRS EO BMF, NCCS BMF, NCCS efile, NCCS 990 Core, NCCS 990 Postcard, and GivingTuesday 990.
- Analysis variable mapping Markdown for the same six families.
- GivingTuesday datamart catalog and field Markdown.
- Raw source dictionaries and generated companion CSVs.
- Analysis variable coverage CSVs.
- Supporting S3 metadata snapshots and manifests under `technical_docs/s3_metadata/bronze/` and `technical_docs/s3_metadata/silver/`; duplicate source dictionary CSVs are intentionally centralized under `technical_docs/source_dictionaries/`.

The `_manifest.csv` file records each package file, source S3 key when applicable, size, timestamp, local path, and SHA-256 hash.

## Not Included

This folder intentionally does not mirror raw source data, parquet datasets, staged/final analysis datasets, or other non-metadata data objects from S3.