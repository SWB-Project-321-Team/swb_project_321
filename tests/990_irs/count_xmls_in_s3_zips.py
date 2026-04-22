"""
List S3 bulk-part ZIPs per year and report how many .xml files are in each.
Requires downloading each ZIP to open it (no XML count in S3 metadata).

Run from repo root:
  python tests/990_irs/count_xmls_in_s3_zips.py
  python tests/990_irs/count_xmls_in_s3_zips.py --year 2024
  python tests/990_irs/count_xmls_in_s3_zips.py --year 2024 --max-zips 2
"""
import os
import sys
import zipfile
from io import BytesIO
from pathlib import Path

_FILE_DIR = Path(__file__).resolve().parent
_REPO_ROOT = _FILE_DIR.parent.parent
_PYTHON = _REPO_ROOT / "python"
_990_IRS_SRC = _REPO_ROOT / "python" / "ingest" / "990_irs"
if str(_PYTHON) not in sys.path:
    sys.path.insert(0, str(_PYTHON))

import boto3

# Reuse 03's list and filter (source in python/ingest/990_irs/)
import importlib.util
_spec = importlib.util.spec_from_file_location(
    "parse_990_module",
    _990_IRS_SRC / "03_parse_irs_990_zips_to_staging.py",
)
_mod = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(_mod)

list_zip_keys_for_year = _mod.list_zip_keys_for_year
BUCKET = os.environ.get("IRS_990_S3_BUCKET", "swb-321-irs990-teos")
PREFIX = os.environ.get("IRS_990_S3_PREFIX", "bronze/irs990/teos_xml")
REGION = os.environ.get("AWS_DEFAULT_REGION", "us-east-2")


def count_xmls_in_zip_bytes(zip_bytes: bytes) -> int:
    with zipfile.ZipFile(BytesIO(zip_bytes), "r") as zf:
        return sum(1 for n in zf.namelist() if n.lower().endswith(".xml"))


def main() -> None:
    import argparse
    parser = argparse.ArgumentParser(description="Count .xml members in S3 bulk-part ZIPs")
    parser.add_argument("--year", type=int, default=None, help="Single year (default: all 2021–current)")
    parser.add_argument("--max-zips", type=int, default=None, help="Max ZIPs to open per year (default: all)")
    args = parser.parse_args()

    from datetime import date
    end_year = date.today().year
    years = [args.year] if args.year else list(range(2021, end_year + 1))
    session = boto3.Session(region_name=REGION)
    s3 = session.client("s3")

    total_zips = 0
    total_xmls = 0
    for year in years:
        keys = list_zip_keys_for_year(BUCKET, PREFIX, year, session, bulk_part_only=True)
        if not keys:
            print(f"{year}: no bulk-part ZIPs")
            continue
        if args.max_zips:
            keys = keys[: args.max_zips]
        print(f"{year}: {len(keys)} bulk-part ZIP(s)")
        year_xmls = 0
        for key in keys:
            name = key.split("/")[-1]
            try:
                obj = s3.get_object(Bucket=BUCKET, Key=key)
                body = obj["Body"].read()
                n = count_xmls_in_zip_bytes(body)
                year_xmls += n
                total_xmls += n
                total_zips += 1
                print(f"  {name}: {n:,} .xml files ({len(body):,} bytes)")
            except Exception as e:
                print(f"  {name}: ERROR - {e}")
        print(f"  Year {year} subtotal: {year_xmls:,} XMLs in {len(keys)} ZIP(s)\n")
    print(f"Total: {total_xmls:,} XMLs in {total_zips} ZIP(s)")


if __name__ == "__main__":
    main()
