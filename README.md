# SWB Project 321

This repository contains the code, tests, notebooks, SQL, and documentation for Statistics Without Borders Project 321. The project profiles the nonprofit landscape in the Black Hills region and compares it with selected peer regions using IRS Form 990 and complementary public data.

## Repository map

| Location | Purpose |
| --- | --- |
| [`python/`](python/) | Reproducible ingestion, transformation, analysis, and export code |
| [`sql/`](sql/) | Staging, curated, and metadata SQL |
| [`notebooks/`](notebooks/) | Numbered exploratory and communication notebooks |
| [`tests/`](tests/) | Automated tests, organized to mirror the source datasets |
| [`scripts/reporting/`](scripts/reporting/) | Report and presentation generation utilities |
| [`scripts/diagnostics/`](scripts/diagnostics/) | Manual audits, benchmarks, and data-quality checks |
| [`docs/planning/`](docs/planning/) | Project scope and analysis plans |
| [`docs/analysis/`](docs/analysis/) | Analysis notes, results, and presentation sources |
| [`docs/data_sources/`](docs/data_sources/) | Source-specific documentation |
| [`docs/infrastructure/`](docs/infrastructure/) | S3, OneDrive, and infrastructure audit documentation |
| [`docs/deliverables/`](docs/deliverables/) | Client-facing preprocessing and final-report artifacts |

See [`docs/README.md`](docs/README.md) for the full documentation index and [`scripts/README.md`](scripts/README.md) for script entry points.

## Data policy

Raw and curated datasets live outside Git in the configured local/OneDrive data root. The former project bucket `swb-321-irs990-teos` was intentionally deleted by 2026-07-23 and must not be recreated; retained S3 paths and inventories are historical lineage only. This repository intentionally excludes row-level data, credentials, local environment files, and generated caches. Only small fixtures and documentation metadata belong in Git.

## Getting started

From the repository root:

```powershell
python -m pip install -r python/requirements.txt
pytest tests -q
```

The following retained pipeline entry points describe the original end-to-end workflows. Their S3 upload and verification stages are inactive because the former bucket was deleted; do not run those stages without a separately approved replacement design.

```powershell
python python/run_irs_990.py
python python/run_irs_soi.py
python python/run_nccs_990_core.py
python python/run_nccs_990_postcard.py
```

Dataset-specific options and run order are documented in [`python/README.md`](python/README.md) and [`python/ingest/README.md`](python/ingest/README.md).

## Collaboration

- Keep `main` stable and review-ready.
- Use feature branches for material code, schema, or methodology changes.
- Do not commit data extracts, credentials, private keys, or local environment files.
- Preserve source documents in `docs/deliverables/final_report/source/`; generated working copies belong in the adjacent `working/` directory.

## License

Released under the [MIT License](LICENSE).
