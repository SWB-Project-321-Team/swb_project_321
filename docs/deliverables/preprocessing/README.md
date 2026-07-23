# Final Preprocessing Documentation Package

This folder is the retained local final preprocessing documentation package that was formerly published to `s3://swb-321-irs990-teos/`.

**Infrastructure status as of 2026-07-23:** the former project bucket was intentionally deleted and must not be recreated. S3 keys and metadata below are historical publication and run-lineage records; the local documentation package remains the accessible source of truth.

Local paths below use `docs/deliverables/preprocessing/`. The same files retain their historical published S3 prefix under `documentation/final_preprocessing_docs/`.

## Read First

- **Cross-pipeline naming (contributions):** GivingTuesday Step 13 publishes **`analysis_total_contributions_amount`** (form-aware total contributions). NCCS 990 Core promotes **`analysis_calculated_total_contributions_amount`** from Core candidates. Other families often leave contribution-related harmonized columns **unavailable** and document the GT column names in their mapping tables.
- **Individual versus institutional contributions (Section 3 Q9 caveat):** Form 990 Part VIII Line 1 sub-channels can identify *unambiguously institutional* contributions (Line 1a federated campaigns, Line 1d related-organization contributions, Line 1e government grants) and *predominantly individual* contributions (Line 1b membership dues, Line 1c fundraising-event contributions), but Line 1f (`ALLOOTHECONT`, "all other contributions") mixes individual gifts with private foundation grants, donor-advised fund distributions, corporate gifts, and bequests in a single line that the IRS does not break out by donor type. Form 990-EZ and Form 990-PF expose only a single contributions total. Step 13 publishes the Line 1 sub-components as separate analysis columns plus an institutional-channel aggregate `analysis_calculated_grants_total_amount` = `FEDERACAMPAI` + `RELATEORGANI` + `GOVERNGRANTS`; a cleaner individual-versus-institutional split would require Form 990 Schedule B, which is not currently in this package. See the GT client documentation and `docs/analysis/990_open_questions.md` (OQ-008) for the authoritative version of this caveat.
- Start with `client_guides/markdown/` for short dataset-by-dataset client summaries.
- Use `client_guides/docx/` for Word delivery copies of the same client-facing summaries.
- Use `technical_reference/pipelines/` for detailed source locations, retrieval logic, region filtering, processing steps, final outputs, and validation.
- Use `technical_reference/variable_mappings/` for the exact analysis variables, provenance columns, unavailable-variable explanations, and coverage evidence.
- Use `technical_reference/quality/coverage_evidence/` for the published analysis variable coverage CSVs.
- Use `technical_reference/source_dictionaries/` for source dictionaries and generated raw-header companions.
- Use `technical_reference/s3_metadata/` for supporting S3 manifests, raw catalog snapshots, size checks, and schema evidence.

## Historical Published S3 Layout

While the former bucket was active, the six target families published analysis documentation under a docs/coverage split:

- Pipeline docs: `silver/<family>/analysis/documentation/`
- Variable mappings: `silver/<family>/analysis/variable_mappings/`
- Coverage evidence: `silver/<family>/analysis/quality/coverage/`

The former flat analysis metadata target objects were removed after the replacement keys were uploaded and verified. All keys in this section are retained as historical lineage.

## Included Artifacts

This package includes:

- Standardized client-facing wrapper documents in Markdown and Word format for IRS EO BMF, NCCS BMF, NCCS efile, NCCS 990 Core, NCCS 990 Postcard, and GivingTuesday 990 DataMart.
- Pipeline Markdown for IRS EO BMF, NCCS BMF, NCCS efile, NCCS 990 Core, NCCS 990 Postcard, and GivingTuesday 990.
- Analysis variable mapping Markdown for the same six families.
- GivingTuesday datamart catalog and field Markdown.
- Raw source dictionaries and generated companion CSVs.
- Analysis variable coverage CSVs.
- Supporting S3 metadata snapshots and manifests under `technical_reference/s3_metadata/bronze/` and `technical_reference/s3_metadata/silver/`; duplicate source dictionary CSVs are intentionally centralized under `technical_reference/source_dictionaries/`.

## Not Included

This folder intentionally does not mirror raw source data, parquet datasets, staged/final analysis datasets, or other non-metadata data objects from S3.
