# tests/

Tests for code in this repository (Python, SQL, or pipeline steps). Layout mirrors source:

- **location_processing/** — GEOID reference (CSV vs xlsx) and zip_to_county vs GEOID_reference consistency.
- **990_givingtuesday/** — GivingTuesday API fetch (connectivity and response shape).
- **990_irs/** — Parser test, S3 verification, and debug scripts: `test_parser_30_per_year.py`, `fetch_sample_xml_and_test_parser.py`, `verify_irs_vs_s3.py`, `count_xmls_in_s3_zips.py`, `benchmark_parser_one_xml.py`, `inspect_s3_zips.py`, `list_s3_bucket.py`.

- Unit tests for `python/utils`, ingest/transform/export helpers.
- SQL or data tests (e.g. run staging/curated and assert row counts or constraints).
- No data files or credentials; use fixtures, mocks, or small embedded samples only.

Run all tests from repo root: `pytest tests/ -v`. Run a subset: `pytest tests/location_processing/ -v`, `pytest tests/990_givingtuesday/ -v`, or `pytest tests/990_irs/ -v`. Keep tests fast and deterministic.
