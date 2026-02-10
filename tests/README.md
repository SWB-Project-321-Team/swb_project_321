# tests/

Tests for code in this repository (Python, SQL, or pipeline steps).

- Unit tests for `python/utils`, ingest/transform/export helpers.
- SQL or data tests (e.g. run staging/curated and assert row counts or constraints).
- No data files or credentials; use fixtures, mocks, or small embedded samples only.

Run tests via your preferred runner (pytest, etc.). Keep tests fast and deterministic.
