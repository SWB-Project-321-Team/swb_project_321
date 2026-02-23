"""
Upload IRS Form 990 TEOS index CSV files (2021 through current year) from IRS to S3.

Streams each index_{YEAR}.csv from the IRS URL directly to the project's S3 bucket.
No large local files. Uses requests (stream=True) and boto3.

IRS index URL: https://apps.irs.gov/pub/epostcard/990/xml/{YEAR}/index_{YEAR}.csv
S3 key: s3://{bucket}/{prefix}/index/year={YEAR}/index_{YEAR}.csv

Environment (or .env): AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, AWS_DEFAULT_REGION.
Override via: --bucket, --prefix, --region. Defaults: bucket/prefix from docs/990_irs_teos_s3_plan.md.

Run from repo root:
  python python/ingest/990_irs/01_upload_irs_990_index_to_s3.py
  python python/ingest/990_irs/01_upload_irs_990_index_to_s3.py --start-year 2023 --end-year 2025
"""

import os
import sys
from io import BytesIO
import boto3
import requests

DEST_BUCKET = os.environ.get("IRS_990_S3_BUCKET", "swb-321-irs990-teos")
PREFIX = os.environ.get("IRS_990_S3_PREFIX", "bronze/irs990/teos_xml")
IRS_INDEX_URL = "https://apps.irs.gov/pub/epostcard/990/xml/{year}/index_{year}.csv"


def _session():
    region = os.environ.get("AWS_DEFAULT_REGION", "us-east-2")
    return boto3.Session(region_name=region)


def upload_index_for_year(year: int, bucket: str, prefix: str, session: boto3.Session) -> bool:
    """Stream index_{year}.csv from IRS to S3. Returns True on success, False on 404/error."""
    url = IRS_INDEX_URL.format(year=year)
    s3_key = f"{prefix}/index/year={year}/index_{year}.csv"
    print(f"  [{year}] Downloading index from IRS...", flush=True)
    try:
        r = requests.get(url, stream=True, timeout=60, allow_redirects=True)
        r.raise_for_status()
    except requests.RequestException as e:
        print(f"  Skip {year}: {e}", file=sys.stderr)
        return False

    body = r.iter_content(chunk_size=1024 * 1024)
    s3 = session.client("s3")
    try:
        buf = BytesIO()
        for chunk in body:
            buf.write(chunk)
        size_mb = len(buf.getvalue()) / (1024 * 1024)
        buf.seek(0)
        print(f"  [{year}] Uploading to S3 ({size_mb:.2f} MB)...", flush=True)
        s3.upload_fileobj(buf, bucket, s3_key)
        print(f"  [{year}] Uploaded index_{year}.csv -> s3://{bucket}/{s3_key}")
        return True
    except Exception as e:
        print(f"  Upload failed {year}: {e}", file=sys.stderr)
        return False


def main() -> None:
    import argparse
    parser = argparse.ArgumentParser(description="Stream IRS 990 TEOS index CSVs to S3")
    parser.add_argument("--bucket", default=DEST_BUCKET, help="S3 bucket name")
    parser.add_argument("--prefix", default=PREFIX, help="S3 key prefix")
    parser.add_argument("--region", default=os.environ.get("AWS_DEFAULT_REGION", "us-east-2"), help="AWS region")
    parser.add_argument("--start-year", type=int, default=2021, help="First tax year (default 2021)")
    parser.add_argument("--end-year", type=int, default=None, help="Last tax year (default: current year)")
    args = parser.parse_args()

    end_year = args.end_year
    if end_year is None:
        from datetime import date
        end_year = date.today().year

    if args.start_year > end_year:
        print("start-year must be <= end-year", file=sys.stderr)
        sys.exit(1)

    print(f"Uploading IRS 990 TEOS index files to s3://{args.bucket}/{args.prefix}/")
    print(f"Years: {args.start_year} through {end_year} ({end_year - args.start_year + 1} files)")
    print()

    os.environ["AWS_DEFAULT_REGION"] = args.region
    session = _session()
    ok = 0
    for year in range(args.start_year, end_year + 1):
        if upload_index_for_year(year, args.bucket, args.prefix, session):
            ok += 1
    print(f"Done: {ok}/{end_year - args.start_year + 1} index files uploaded to s3://{args.bucket}/{args.prefix}/")


if __name__ == "__main__":
    main()
