# 990/

Docs for **Form 990** and the **IRS pipeline**:

- **Sample parsed data:** [sample_parsed_irs_990_10.csv](sample_parsed_irs_990_10.csv) — 10-row sample of parser staging output (varied form types and years) for exploration. Regenerate with `python scripts/sample_parsed_990_to_csv.py` (use `AWS_PROFILE=neilken-admin` or appropriate profile with S3 access). See [sample_parsed_irs_990_10_validation.md](sample_parsed_irs_990_10_validation.md) for a column-by-column check and when missing values are expected.
- [990_irs_teos_s3_plan.md](990_irs_teos_s3_plan.md) — IRS TEOS 990 XML → S3 streaming; bucket layout; scripts 01–03.
- [990_irs_run_parser_on_ec2.md](990_irs_run_parser_on_ec2.md) — Run script 03 on EC2 (same region as S3) for fast parsing in the cloud.
- [990_irs_coverage_checklist.md](990_irs_coverage_checklist.md) — Does the IRS data we have contain all we need? (TEOS vs plan; BMF; variables.)
- [990_irs_parser_extraction_gap.md](990_irs_parser_extraction_gap.md) — What the parser extracts vs. project variable checklist; remaining gaps (org name, address, etc.).
- [990_irs_form_types_parsing.md](990_irs_form_types_parsing.md) — How the parser handles 990, 990-EZ, 990-PF, 990-T; form-type coverage and known gaps.
- [990_irs_parser_form_type_check.md](990_irs_parser_form_type_check.md) — Per-form check using sample XMLs in `data/sample_990_xml_by_form/`; what we extract and known limitations.
- [990_irs_efficiency_overhaul.md](990_irs_efficiency_overhaul.md) — Efficiency and overhaul options (streaming, memory, silver layer, run in AWS).
- [990_data_fetch_plan.md](990_data_fetch_plan.md) — Plan to fetch 990 data (GivingTuesday, BMF, EIN list, ZIP–county).
- [990_api_403_investigation.md](990_api_403_investigation.md) — GivingTuesday API 403 investigation and workarounds.

See also [../README.md](../README.md) for the full docs index.
