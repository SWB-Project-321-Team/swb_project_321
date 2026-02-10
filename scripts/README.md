# scripts/

One-off or utility scripts that don’t belong in the main pipeline (e.g. `python/`).

- Format conversions (e.g. Markdown → Word), repo maintenance, or local automation.
- Ad-hoc data checks or one-time migrations.
- Anything that supports the project but isn’t part of the core ingest → staging → curated → export flow.

No data or credentials in repo. Prefer `python/` for anything that becomes part of the repeatable pipeline.
