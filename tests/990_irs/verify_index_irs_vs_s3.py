"""
Verify that index CSVs in S3 (uploaded by 01_upload_irs_990_index_to_s3.py) match the IRS source.

For each year, fetches index_{year}.csv from IRS and from S3 and compares byte-for-byte.
Run from repo root. Loads secrets/.env for AWS.

  python tests/990_irs/verify_index_irs_vs_s3.py
  python tests/990_irs/verify_index_irs_vs_s3.py --start-year 2022 --end-year 2024
"""

import os
import sys
from pathlib import Path

import boto3
import requests

_FILE_DIR = Path(__file__).resolve().parent
_REPO_ROOT = _FILE_DIR.parent.parent
_PYTHON = _REPO_ROOT / "python"
if str(_PYTHON) not in sys.path:
    sys.path.insert(0, str(_PYTHON))

# Load secrets/.env for AWS
_ENV_FILE = _REPO_ROOT / "secrets" / ".env"
if _ENV_FILE.exists():
    with open(_ENV_FILE, encoding="utf-8-sig") as f:
        for line in f:
            line = line.strip()
            if line and not line.startswith("#") and "=" in line:
                key, _, value = line.partition("=")
                key, value = key.strip(), value.strip()
                if value and value[0] in ("'", '"') and value[-1] == value[0]:
                    value = value[1:-1]
                if key:
                    os.environ.setdefault(key, value)

BUCKET = os.environ.get("IRS_990_S3_BUCKET", "swb-321-irs990-teos")
PREFIX = os.environ.get("IRS_990_S3_PREFIX", "bronze/irs990/teos_xml")
IRS_INDEX_URL = "https://apps.irs.gov/pub/epostcard/990/xml/{year}/index_{year}.csv"


def _session():
    region = os.environ.get("AWS_DEFAULT_REGION", "us-east-2")
    return boto3.Session(region_name=region)


def fetch_irs_index(year: int, session: requests.Session) -> bytes | None:
    url = IRS_INDEX_URL.format(year=year)
    try:
        r = session.get(url, timeout=120, allow_redirects=True)
        r.raise_for_status()
        return r.content
    except requests.RequestException as e:
        print(f"  [{year}] IRS fetch failed: {e}", file=sys.stderr)
        return None


def fetch_s3_index(year: int, bucket: str, prefix: str, s3_client) -> bytes | None:
    s3_key = f"{prefix}/index/year={year}/index_{year}.csv"
    try:
        obj = s3_client.get_object(Bucket=bucket, Key=s3_key)
        return obj["Body"].read()
    except Exception as e:
        print(f"  [{year}] S3 fetch failed: {e}", file=sys.stderr)
        return None


def verify_year(year: int, bucket: str, prefix: str, http_sess: requests.Session, s3_client) -> tuple[bool, str]:
    irs_body = fetch_irs_index(year, http_sess)
    if irs_body is None:
        return (False, "IRS fetch failed")
    s3_body = fetch_s3_index(year, bucket, prefix, s3_client)
    if s3_body is None:
        return (False, "S3 fetch failed")
    if len(irs_body) != len(s3_body):
        return (False, f"size mismatch: IRS {len(irs_body):,} vs S3 {len(s3_body):,}")
    if irs_body != s3_body:
        return (False, "content mismatch (bytes differ)")
    return (True, f"match ({len(irs_body):,} bytes)")


def main():
    from datetime import date
    import argparse
    parser = argparse.ArgumentParser(description="Verify S3 index CSVs match IRS source")
    parser.add_argument("--start-year", type=int, default=2021)
    parser.add_argument("--end-year", type=int, default=date.today().year)
    parser.add_argument("--bucket", default=BUCKET)
    parser.add_argument("--prefix", default=PREFIX)
    args = parser.parse_args()

    start, end = args.start_year, args.end_year
    if start > end:
        print("start-year must be <= end-year", file=sys.stderr)
        sys.exit(1)

    print(f"[verify_index_irs_vs_s3] Comparing index files: IRS vs s3://{args.bucket}/{args.prefix}/index/year=*/")
    print(f"[verify_index_irs_vs_s3] Years: {start} through {end}")
    http_sess = requests.Session()
    s3 = _session().client("s3")
    all_ok = True
    for year in range(start, end + 1):
        ok, msg = verify_year(year, args.bucket, args.prefix, http_sess, s3)
        print(f"  [{year}] {msg}")
        if not ok:
            all_ok = False
    if all_ok:
        print("[verify_index_irs_vs_s3] All index files match IRS source.")
    else:
        print("[verify_index_irs_vs_s3] One or more index files did not match.", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
