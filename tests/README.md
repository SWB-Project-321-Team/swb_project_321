# Tests

Automated tests mirror the source dataset layout. In particular, `givingtuesday_990/` covers `python/ingest/givingtuesday_990/`, and `irs_990/` covers `python/ingest/irs_990/`.

Run the full suite from the repository root:

```powershell
pytest tests -q
```

Run a focused suite with, for example:

```powershell
pytest tests/givingtuesday_990 -q
pytest tests/irs_990 -q
pytest tests/analysis -q
```

Historical S3 inspections, parser benchmarks, and sample generators live in [`../scripts/diagnostics/`](../scripts/diagnostics/) rather than this directory. The former project bucket was intentionally deleted; keep tests deterministic and use small fixtures or mocks instead of project data.
