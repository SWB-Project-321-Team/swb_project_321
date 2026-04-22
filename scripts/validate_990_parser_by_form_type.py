"""
Read 20 XML files per 990 form type (990, 990EZ, 990PF, 990T) from the smallest
bulk ZIP, run the 03 parser on each, and validate using the same tree walk as
the parser (no regex reference).

Validation:
- Expected = parser(xml); actual = parser(xml) again. Compares for determinism.
- Sanity checks: ein present and 9 digits, form_type in allowed set, tax_year valid.

Output: per-form-type summary (passed/failed) and any error details.

Requires: AWS credentials (secrets/.env or env) and S3 access to TEOS ZIPs.
Run from repo root: python scripts/validate_990_parser_by_form_type.py
"""
from __future__ import annotations

import importlib.util
import os
import re
import sys
import zipfile
from io import BytesIO
from pathlib import Path

import boto3

REPO_ROOT = Path(__file__).resolve().parent.parent
PARSER_PATH = REPO_ROOT / "python" / "ingest" / "990_irs" / "03_parse_irs_990_zips_to_staging.py"
if str(REPO_ROOT / "python") not in sys.path:
    sys.path.insert(0, str(REPO_ROOT / "python"))


def _load_parser():
    spec = importlib.util.spec_from_file_location("parse_990_module", PARSER_PATH)
    mod = importlib.util.module_from_spec(spec)
    sys.modules["parse_990_module"] = mod
    spec.loader.exec_module(mod)
    return mod.parse_990_xml


BUCKET = os.environ.get("IRS_990_S3_BUCKET", "swb-321-irs990-teos")
PREFIX = os.environ.get("IRS_990_S3_PREFIX", "bronze/irs990/teos_xml")
REGION = os.environ.get("AWS_DEFAULT_REGION", "us-east-2")
PER_FORM_TYPE = 20
FORM_TYPES = ("990", "990EZ", "990PF", "990T")


def _load_env() -> None:
    env_file = REPO_ROOT / "secrets" / ".env"
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


def _normalize_form_type(raw: str | None) -> str:
    if not raw or not str(raw).strip():
        return "990"
    s = str(raw).strip().upper().replace("-", "")
    if not s.startswith("990"):
        return "990"
    if s == "990":
        return "990"
    if s in ("990EZ", "990PF", "990T", "990N"):
        return s
    if len(s) <= 8 and s[3:].isalpha():
        return s
    return "990"


def get_form_type_from_xml(xml_bytes: bytes) -> str:
    """Quick form type from XML for bucketing."""
    _RE_RETURN_TYPE = re.compile(rb"<[^>]*(?:ReturnTypeCd|FormTypeCd)[^>]*>\s*([^<]+)", re.IGNORECASE)
    m = _RE_RETURN_TYPE.search(xml_bytes)
    raw = m.group(1).decode("utf-8", errors="replace").strip() if m else None
    return _normalize_form_type(raw or "990")


def find_smallest_zip(session: boto3.Session) -> tuple[int, str]:
    s3 = session.client("s3")
    start_year = 2021
    end_year = int(__import__("datetime").date.today().year)
    bulk_pattern = re.compile(r"^\d{4}_TEOS_XML_\d{2}[A-Z]?\.zip$", re.IGNORECASE)
    smallest_key = None
    smallest_size = None
    for year in range(start_year, end_year + 1):
        prefix_key = f"{PREFIX}/zips/year={year}/"
        paginator = s3.get_paginator("list_objects_v2")
        for page in paginator.paginate(Bucket=BUCKET, Prefix=prefix_key):
            for obj in page.get("Contents", []):
                k = obj.get("Key", "")
                name = k.split("/")[-1]
                if k.endswith(".zip") and bulk_pattern.match(name):
                    size = obj.get("Size", 0)
                    if smallest_size is None or size < smallest_size:
                        smallest_size = size
                        smallest_key = k
    if smallest_key is None:
        raise SystemExit("No bulk-part ZIPs found in S3.")
    parts = smallest_key.split("/")
    year_part = next((p for p in parts if p.startswith("year=")), "")
    year = int(year_part.replace("year=", "")) if year_part else start_year
    return year, smallest_key


def collect_xmls_by_form_type(session: boto3.Session, zip_key: str) -> dict[str, list[tuple[str, bytes]]]:
    """Download ZIP, collect up to PER_FORM_TYPE XMLs per form type."""
    s3 = session.client("s3")
    buf = BytesIO()
    s3.download_fileobj(BUCKET, zip_key, buf)
    buf.seek(0)
    z = zipfile.ZipFile(buf, "r")
    by_type: dict[str, list[tuple[str, bytes]]] = {ft: [] for ft in FORM_TYPES}
    for name in z.namelist():
        if all(len(by_type[ft]) >= PER_FORM_TYPE for ft in FORM_TYPES):
            break
        if not name.lower().endswith(".xml"):
            continue
        try:
            xml_bytes = z.read(name)
        except Exception:
            continue
        ft = get_form_type_from_xml(xml_bytes)
        if ft not in by_type or len(by_type[ft]) >= PER_FORM_TYPE:
            continue
        by_type[ft].append((name, xml_bytes))
    z.close()
    return by_type


def _sanity_check(row: dict) -> list[str]:
    """Validate parser output structure. Return list of error messages."""
    errs = []
    ein = row.get("ein")
    if not ein or not str(ein).strip():
        errs.append("  ein: missing")
    else:
        s = str(ein).strip().replace("-", "").replace(" ", "")
        if not s.isdigit() or len(s) != 9:
            errs.append(f"  ein: must be 9 digits, got {ein!r}")
    form_type = row.get("form_type")
    if form_type not in FORM_TYPES:
        errs.append(f"  form_type: must be one of {FORM_TYPES}, got {form_type!r}")
    tax_yr = row.get("tax_year")
    if tax_yr is not None:
        try:
            y = int(tax_yr)
            if y < 1990 or y > 2030:
                errs.append(f"  tax_year: out of range, got {tax_yr!r}")
        except (TypeError, ValueError):
            errs.append(f"  tax_year: invalid, got {tax_yr!r}")
    return errs


def _row_diff(expected: dict, actual: dict) -> list[str]:
    """Compare two parser output dicts (same keys). Return list of differences."""
    errs = []
    keys = sorted(set(expected) | set(actual))
    for k in keys:
        exp = expected.get(k)
        act = actual.get(k)
        if exp == act:
            continue
        # Normalize for display (e.g. float 91766.0 vs str "91766")
        if exp is not None and act is not None:
            if isinstance(exp, float) and isinstance(act, (int, float)):
                if abs(exp - float(act)) < 0.01:
                    continue
            if isinstance(exp, (int, float)) and isinstance(act, (int, float)):
                if abs(float(exp) - float(act)) < 0.01:
                    continue
        errs.append(f"  {k}: first run {exp!r} != second run {act!r}")
    return errs


def validate_one(name: str, xml_bytes: bytes, parse_990_xml) -> tuple[dict | None, list[str]]:
    """
    Use parser as source of truth (same tree walk).
    Run parser twice and compare for determinism; run sanity checks on output.
    Return (parsed_row, list of error messages).
    """
    row1 = parse_990_xml(xml_bytes, source_name=name)
    if row1 is None:
        return None, [f"  Parser returned None for {name}"]
    errors = _sanity_check(row1)
    row2 = parse_990_xml(xml_bytes, source_name=name)
    if row2 is None:
        errors.append("  Parser returned None on second run (non-deterministic)")
    else:
        errors.extend(_row_diff(row1, row2))
    return row1, errors


def main() -> None:
    _load_env()
    parse_990_xml = _load_parser()
    session = boto3.Session(region_name=REGION)
    print("Finding smallest bulk-part ZIP in S3...")
    year, zip_key = find_smallest_zip(session)
    print(f"Using {zip_key}")
    print("Downloading and collecting 20 XMLs per form type...")
    by_type = collect_xmls_by_form_type(session, zip_key)
    for ft in FORM_TYPES:
        n = len(by_type[ft])
        print(f"  {ft}: {n} XMLs")
    print("Validating (parser = source of truth: determinism + sanity checks)...")
    results = {}
    for form_type in FORM_TYPES:
        results[form_type] = {"passed": 0, "failed": 0, "errors": []}
        for name, xml_bytes in by_type[form_type]:
            row, errors = validate_one(name, xml_bytes, parse_990_xml)
            if not errors:
                results[form_type]["passed"] += 1
            else:
                results[form_type]["failed"] += 1
                results[form_type]["errors"].append((name, errors))
    print()
    print("Summary by form type")
    print("-" * 50)
    for ft in FORM_TYPES:
        r = results[ft]
        total = r["passed"] + r["failed"]
        print(f"  {ft}: {r['passed']}/{total} passed, {r['failed']} failed")
        for name, errs in r["errors"][:5]:
            print(f"    {name}:")
            for e in errs[:10]:
                print(e)
            if len(errs) > 10:
                print(f"    ... and {len(errs) - 10} more")
        if len(r["errors"]) > 5:
            print(f"    ... and {len(r['errors']) - 5} more files with errors")
    print("-" * 50)


if __name__ == "__main__":
    main()
