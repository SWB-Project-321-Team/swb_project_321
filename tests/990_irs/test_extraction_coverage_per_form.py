"""
Test that the 990 parser extracts all intended staging columns for each form type.

Runs the parser on a few filings per form type (990, 990EZ, 990PF, 990T), asserts
every row has the full set of expected columns and required fields (ein, form_type),
and prints a coverage report: per form type, how many sampled rows had each column
non-null.

Run from repo root:
  python tests/990_irs/test_extraction_coverage_per_form.py
  python tests/990_irs/test_extraction_coverage_per_form.py --s3 --per-type 3

Without --s3: uses only the 4 sample XMLs in data/sample_990_xml_by_form/ (1 per type).
With --s3: downloads the smallest bulk ZIP and adds up to --per-type filings per type.
"""
import os
import re
import sys
import zipfile
from io import BytesIO
from pathlib import Path

_FILE_DIR = Path(__file__).resolve().parent
_REPO_ROOT = _FILE_DIR.parent.parent
_PYTHON = _REPO_ROOT / "python"
if str(_PYTHON) not in sys.path:
    sys.path.insert(0, str(_PYTHON))

# Must match the keys returned by _row_from_found in 03_parse_irs_990_zips_to_staging.py
EXPECTED_STAGING_COLUMNS = frozenset({
    "ein", "tax_year", "form_type", "filing_ts", "tax_period_begin_dt", "tax_period_end_dt",
    "org_name", "address_line1", "city", "state", "zip",
    "exempt_purpose_txt", "subsection_code",
    "revenue", "expenses", "assets", "net_assets", "total_liabilities",
    "program_service_expenses", "management_general_expenses", "fundraising_expenses",
    "contributions_grants", "program_service_revenue", "grants_paid",
    "investment_income", "other_revenue", "government_grants", "ntee_code",
    "employee_cnt", "formation_yr", "excess_or_deficit",
    "source_file",
})
REQUIRED_COLUMNS = {"ein", "form_type"}
TARGET_FORM_TYPES = ("990", "990EZ", "990PF", "990T")
SAMPLE_XML_DIR = _REPO_ROOT / "data" / "sample_990_xml_by_form"
PARSER_SCRIPT = _REPO_ROOT / "python" / "ingest" / "990_irs" / "03_parse_irs_990_zips_to_staging.py"
BUCKET = os.environ.get("IRS_990_S3_BUCKET", "swb-321-irs990-teos")
PREFIX = os.environ.get("IRS_990_S3_PREFIX", "bronze/irs990/teos_xml")
REGION = os.environ.get("AWS_DEFAULT_REGION", "us-east-2")
_BULK_PART_PATTERN = re.compile(r"^\d{4}_TEOS_XML_\d{2}[A-Z]?\.zip$", re.IGNORECASE)


def _load_parser():
    import importlib.util
    spec = importlib.util.spec_from_file_location("parse_990_module", PARSER_SCRIPT)
    parse_module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(parse_module)
    return parse_module.parse_990_xml


def _load_env() -> None:
    env_file = _REPO_ROOT / "secrets" / ".env"
    if not env_file.exists():
        return
    with open(env_file, encoding="utf-8-sig") as f:
        for line in f:
            line = line.strip()
            if line and not line.startswith("#") and "=" in line:
                key, _, value = line.partition("=")
                key, value = key.strip(), value.strip()
                if value and value[0] in ("'", '"') and value[-1] == value[0]:
                    value = value[1:-1]
                if key:
                    os.environ.setdefault(key, value)


def _gather_local_samples():
    """Return dict form_type -> list of (xml_bytes, source_name)."""
    out = {ft: [] for ft in TARGET_FORM_TYPES}
    if not SAMPLE_XML_DIR.exists():
        return out
    # 990.xml, 990EZ.xml, 990PF.xml, 990T.xml
    for path in SAMPLE_XML_DIR.iterdir():
        if path.suffix.lower() != ".xml":
            continue
        name = path.stem.upper().replace("-", "")
        if name in TARGET_FORM_TYPES:
            out[name].append((path.read_bytes(), path.name))
    return out


def _gather_s3_samples(parse_990_xml, per_type: int):
    """Download smallest bulk ZIP, return dict form_type -> list of (xml_bytes, source_name)."""
    import boto3
    _load_env()
    session = boto3.Session(region_name=REGION)
    s3 = session.client("s3")
    start_year = 2021
    end_year = int(__import__("datetime").date.today().year)
    smallest_key = None
    smallest_size = None
    for year in range(start_year, end_year + 1):
        prefix_key = f"{PREFIX}/zips/year={year}/"
        paginator = s3.get_paginator("list_objects_v2")
        for page in paginator.paginate(Bucket=BUCKET, Prefix=prefix_key):
            for obj in page.get("Contents", []):
                k = obj.get("Key", "")
                name = k.split("/")[-1]
                if k.endswith(".zip") and _BULK_PART_PATTERN.match(name):
                    size = obj.get("Size", 0)
                    if smallest_size is None or size < smallest_size:
                        smallest_size = size
                        smallest_key = k
    if not smallest_key:
        return {}
    obj = s3.get_object(Bucket=BUCKET, Key=smallest_key)
    zip_bytes = obj["Body"].read()
    buf = BytesIO(zip_bytes)
    # form_type -> list of (xml_bytes, source_name), max per_type per form
    collected = {ft: [] for ft in TARGET_FORM_TYPES}
    with zipfile.ZipFile(buf, "r") as zf:
        for name in zf.namelist():
            if not name.lower().endswith(".xml"):
                continue
            try:
                xml_bytes = zf.read(name)
            except Exception:
                continue
            row = parse_990_xml(xml_bytes, source_name=Path(name).name, use_regex_only=False)
            if not row:
                continue
            ft = str(row.get("form_type") or "").strip().upper().replace("-", "")
            if ft in TARGET_FORM_TYPES and len(collected[ft]) < per_type:
                collected[ft].append((xml_bytes, Path(name).name))
    return collected


def _merge_samples(local: dict, s3: dict):
    """Merge s3 into local; for each form type, local first then s3, no duplicates by source_name."""
    merged = {}
    for ft in TARGET_FORM_TYPES:
        seen = set()
        out = []
        for xml_bytes, name in local.get(ft, []) + s3.get(ft, []):
            if name in seen:
                continue
            seen.add(name)
            out.append((xml_bytes, name))
        merged[ft] = out
    return merged


def _is_filled(val) -> bool:
    if val is None:
        return False
    if isinstance(val, (int, float)):
        return True  # 0 is filled
    return len(str(val).strip()) > 0


def run_tests(parse_990_xml, samples: dict) -> tuple[bool, dict]:
    """
    Run parser on all samples. Return (all_passed, coverage).
    coverage[form_type][column] = count of rows (for that type) with non-null value.
    """
    all_passed = True
    coverage = {ft: {col: 0 for col in EXPECTED_STAGING_COLUMNS} for ft in TARGET_FORM_TYPES}
    for form_type, file_list in samples.items():
        for xml_bytes, source_name in file_list:
            row = parse_990_xml(xml_bytes, source_name=source_name, use_regex_only=False)
            if row is None:
                print(f"  FAIL [{form_type}] {source_name}: parser returned None", file=sys.stderr)
                all_passed = False
                continue
            # All expected columns present
            row_keys = set(row.keys())
            missing = EXPECTED_STAGING_COLUMNS - row_keys
            extra = row_keys - EXPECTED_STAGING_COLUMNS
            if missing:
                print(f"  FAIL [{form_type}] {source_name}: missing columns: {sorted(missing)}", file=sys.stderr)
                all_passed = False
            if extra:
                print(f"  WARN [{form_type}] {source_name}: extra columns: {sorted(extra)}", file=sys.stderr)
            # Required columns non-null
            for col in REQUIRED_COLUMNS:
                if not _is_filled(row.get(col)):
                    print(f"  FAIL [{form_type}] {source_name}: required column '{col}' empty", file=sys.stderr)
                    all_passed = False
            # Coverage
            for col in EXPECTED_STAGING_COLUMNS:
                if _is_filled(row.get(col)):
                    coverage[form_type][col] += 1
    return all_passed, coverage


def print_coverage_report(samples: dict, coverage: dict) -> None:
    print("\n--- Extraction coverage (non-null count per form type) ---")
    print("Columns we intend to extract (parser output); N = number of sampled filings with that column non-null.\n")
    # Header
    cols = sorted(EXPECTED_STAGING_COLUMNS)
    form_types = [ft for ft in TARGET_FORM_TYPES if samples.get(ft)]
    widths = [max(len(c), 4) for c in cols]
    header = "column".ljust(max(len("column"), 22)) + "  " + "  ".join(ft.rjust(4) for ft in form_types)
    print(header)
    print("-" * len(header))
    for col in cols:
        counts = [str(coverage[ft][col]) for ft in form_types]
        n_per_type = [str(len(samples.get(ft, []))) for ft in form_types]
        max_n = max(len(n) for n in n_per_type)
        row = col.ljust(22) + "  " + "  ".join(c.rjust(max(4, max_n)) for c in counts)
        print(row)
    print("\n(Number under each form type = count of sampled filings with that column non-null; max = samples per type.)")
    for ft in form_types:
        n = len(samples.get(ft, []))
        print(f"  {ft}: {n} filing(s) tested.")


def main() -> int:
    import argparse
    ap = argparse.ArgumentParser(description="Test parser extraction coverage per form type")
    ap.add_argument("--s3", action="store_true", help="Also fetch extra filings from S3 (requires AWS credentials)")
    ap.add_argument("--per-type", type=int, default=3, help="Max filings per form type from S3 (default 3)")
    args = ap.parse_args()

    if not PARSER_SCRIPT.exists():
        print(f"Parser not found: {PARSER_SCRIPT}", file=sys.stderr)
        return 1

    parse_990_xml = _load_parser()
    local = _gather_local_samples()
    s3_samples = {}
    if args.s3:
        print("Fetching extra filings from S3 (smallest bulk ZIP)...")
        try:
            s3_samples = _gather_s3_samples(parse_990_xml, args.per_type)
        except Exception as e:
            print(f"S3 fetch failed: {e}", file=sys.stderr)
    samples = _merge_samples(local, s3_samples)

    total = sum(len(v) for v in samples.values())
    if total == 0:
        print("No sample XMLs found. Put 990.xml, 990EZ.xml, 990PF.xml, 990T.xml in data/sample_990_xml_by_form/ or use --s3.", file=sys.stderr)
        return 1

    print(f"Running parser on {total} filing(s) across form types...")
    for ft in TARGET_FORM_TYPES:
        n = len(samples.get(ft, []))
        if n:
            print(f"  {ft}: {n} file(s)")

    all_passed, coverage = run_tests(parse_990_xml, samples)
    print_coverage_report(samples, coverage)

    if not all_passed:
        print("\nOne or more checks failed.", file=sys.stderr)
        return 1
    print("\nAll checks passed: every row has expected columns and required fields (ein, form_type).")
    return 0


if __name__ == "__main__":
    sys.exit(main())
