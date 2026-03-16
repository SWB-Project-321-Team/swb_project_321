"""
Fetch 10 sample 990 XML files from an S3 ZIP (or a local ZIP), save them, examine structure,
and verify our parser (parse_990_xml) can parse them.

Run from repo root:
  python tests/990_irs/fetch_sample_xml_and_test_parser.py
  python tests/990_irs/fetch_sample_xml_and_test_parser.py --local-zip path/to/file.zip
  python tests/990_irs/fetch_sample_xml_and_test_parser.py --year 2024 --count 10
"""
import os
import sys
import zipfile
from io import BytesIO
from pathlib import Path

# Repo root and python on path (this file lives in tests/990_irs/)
_FILE_DIR = Path(__file__).resolve().parent
_REPO_ROOT = _FILE_DIR.parent.parent
_PYTHON = _REPO_ROOT / "python"
_990_IRS_SRC = _REPO_ROOT / "python" / "ingest" / "990_irs"
if str(_PYTHON) not in sys.path:
    sys.path.insert(0, str(_PYTHON))
from utils.paths import DATA

# Import parser and S3 listing from 03 (source lives in python/ingest/990_irs/)
import importlib.util
_spec = importlib.util.spec_from_file_location(
    "parse_990_module",
    _990_IRS_SRC / "03_parse_irs_990_zips_to_staging.py",
)
_parse_module = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(_parse_module)

parse_990_xml = _parse_module.parse_990_xml
list_zip_keys_for_year = _parse_module.list_zip_keys_for_year
_filter_bulk_part_keys = _parse_module._filter_bulk_part_keys

BUCKET = os.environ.get("IRS_990_S3_BUCKET", "swb-321-irs990-teos")
PREFIX = os.environ.get("IRS_990_S3_PREFIX", "bronze/irs990/teos_xml")
REGION = os.environ.get("AWS_DEFAULT_REGION", "us-east-2")
SAMPLE_DIR = DATA / "sample_990_xml"
SAMPLE_DIR.mkdir(parents=True, exist_ok=True)


def get_zip_stream_from_s3(year: int, session) -> tuple[BytesIO | None, str]:
    """Download first bulk-part ZIP for year from S3. Returns (BytesIO of zip bytes, key) or (None, '')."""
    import boto3
    keys = list_zip_keys_for_year(BUCKET, PREFIX, year, session, bulk_part_only=True)
    if not keys:
        return None, ""
    key = keys[0]
    s3 = session.client("s3")
    obj = s3.get_object(Bucket=BUCKET, Key=key)
    body = obj["Body"].read()
    return BytesIO(body), key


def get_xml_names_from_zip(zf) -> list[str]:
    """Return first N .xml member names in ZIP."""
    return [n for n in zf.namelist() if n.lower().endswith(".xml")]


def examine_xml(xml_bytes: bytes, name: str, preview_chars: int = 1600) -> None:
    """Print a short structural preview of the XML (declaration, root tag, first elements)."""
    text = xml_bytes.decode("utf-8", errors="replace")
    if len(text) > preview_chars:
        text = text[:preview_chars] + "\n... [truncated]"
    print(f"\n--- Structure preview: {name} ({len(xml_bytes):,} bytes) ---")
    print(text)
    print("--- end preview ---\n")


def main() -> None:
    import argparse
    import boto3

    parser = argparse.ArgumentParser(description="Fetch sample 990 XMLs and test parser")
    parser.add_argument("--year", type=int, default=2024, help="Year for S3 ZIP (default 2024)")
    parser.add_argument("--count", type=int, default=10, help="Number of XML files to fetch (default 10)")
    parser.add_argument("--local-zip", type=Path, default=None, help="Use local ZIP instead of S3")
    parser.add_argument("--sample-dir", type=Path, default=None, help="Only parse XMLs in this dir (no fetch); use after a previous run saved files")
    parser.add_argument("--no-examine", action="store_true", help="Skip printing XML structure preview")
    args = parser.parse_args()

    # If --sample-dir: just parse existing files and exit.
    if args.sample_dir is not None:
        sample_dir = args.sample_dir.resolve()
        if not sample_dir.is_dir():
            print(f"Error: not a directory: {sample_dir}", file=sys.stderr)
            sys.exit(1)
        xml_files = sorted(sample_dir.glob("*.xml"))[:100]
        if not xml_files:
            print(f"No .xml files in {sample_dir}", file=sys.stderr)
            sys.exit(1)
        print(f"Parsing {len(xml_files)} XMLs in {sample_dir}")
        all_ok = True
        for i, p in enumerate(xml_files):
            xml_bytes = p.read_bytes()
            name = p.name
            row_regex = parse_990_xml(xml_bytes, source_name=name, use_regex_only=True)
            row_dom = parse_990_xml(xml_bytes, source_name=name, use_regex_only=False)
            ok = row_regex is not None and row_dom is not None
            if not ok:
                all_ok = False
            if row_regex and row_dom and row_regex.get("ein") != row_dom.get("ein"):
                all_ok = False
            status = "OK" if ok and (row_regex or row_dom) else "FAIL"
            ein_r = (row_regex or {}).get("ein")
            ein_d = (row_dom or {}).get("ein")
            print(f"  [{status}] {p.name}  regex_ein={ein_r}  dom_ein={ein_d}")
        print("\nAll parsed successfully (regex and DOM agree)." if all_ok else "\nSome failed or mismatch.", file=sys.stderr if not all_ok else sys.stdout)
        sys.exit(0 if all_ok else 1)

    count = max(1, min(args.count, 100))
    zip_buf = None
    zip_name = "local"

    if args.local_zip is not None:
        zpath = args.local_zip.resolve()
        if not zpath.is_file():
            print(f"Error: not a file: {zpath}", file=sys.stderr)
            sys.exit(1)
        zip_buf = BytesIO(zpath.read_bytes())
        zip_name = zpath.name
        print(f"Using local ZIP: {zpath} ({zpath.stat().st_size:,} bytes)")
    else:
        print(f"Listing S3 ZIPs for year={args.year}...")
        session = boto3.Session(region_name=REGION)
        zip_buf, s3_key = get_zip_stream_from_s3(args.year, session)
        if zip_buf is None:
            print("No bulk-part ZIP found in S3 for that year. Use --local-zip path/to/file.zip", file=sys.stderr)
            sys.exit(1)
        zip_name = s3_key.split("/")[-1]
        print(f"Downloaded {zip_name} ({len(zip_buf.getvalue()):,} bytes)")

    with zipfile.ZipFile(zip_buf, "r") as zf:
        xml_names = get_xml_names_from_zip(zf)
        if not xml_names:
            print("No .xml members in ZIP.", file=sys.stderr)
            sys.exit(1)
        to_fetch = xml_names[:count]
        print(f"Found {len(xml_names):,} .xml members; fetching first {len(to_fetch)}.")

    # Extract and save
    with zipfile.ZipFile(zip_buf, "r") as zf:
        saved = []
        for i, name in enumerate(to_fetch):
            try:
                xml_bytes = zf.read(name)
            except Exception as e:
                print(f"  Skip {name}: {e}", file=sys.stderr)
                continue
            # Safe filename: use last part of path and index
            base = Path(name).name or f"file_{i}"
            if not base.lower().endswith(".xml"):
                base += ".xml"
            out_path = SAMPLE_DIR / base
            if out_path.exists():
                out_path = SAMPLE_DIR / f"{i:02d}_{base}"
            out_path.write_bytes(xml_bytes)
            saved.append((name, out_path, xml_bytes))
            print(f"  Saved: {out_path.name}")

    print(f"\nSaved {len(saved)} files under {SAMPLE_DIR}\n")

    # Examine first file
    if saved and not args.no_examine:
        examine_xml(saved[0][2], saved[0][0])

    # Run parser on each (regex and DOM)
    print("Parser results (regex and DOM):")
    all_ok = True
    for name, out_path, xml_bytes in saved:
        row_regex = parse_990_xml(xml_bytes, source_name=name, use_regex_only=True)
        row_dom = parse_990_xml(xml_bytes, source_name=name, use_regex_only=False)
        ok = row_regex is not None and row_dom is not None
        if not ok:
            all_ok = False
        status = "OK" if ok else "FAIL"
        print(f"  [{status}] {out_path.name}")
        if row_regex:
            print(f"      regex -> ein={row_regex.get('ein')} tax_year={row_regex.get('tax_year')} form_type={row_regex.get('form_type')} revenue={row_regex.get('revenue')}")
        else:
            print("      regex -> None")
        if row_dom:
            print(f"      DOM   -> ein={row_dom.get('ein')} tax_year={row_dom.get('tax_year')} form_type={row_dom.get('form_type')} revenue={row_dom.get('revenue')}")
        else:
            print("      DOM   -> None")
        if row_regex and row_dom:
            # Sanity: same EIN
            if row_regex.get("ein") != row_dom.get("ein"):
                print("      WARN: EIN mismatch between regex and DOM")
                all_ok = False

    if all_ok:
        print("\nAll sample files parsed successfully (regex and DOM).")
    else:
        print("\nSome files failed to parse or had mismatches.", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
