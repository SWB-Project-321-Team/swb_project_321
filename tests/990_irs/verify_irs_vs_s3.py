"""
Verify that ZIPs in S3 (uploaded by 02_upload_irs_990_zips_to_s3.py) match the IRS source.

Lists bulk-part ZIPs on the IRS (by probing HEAD requests), then confirms we have each one
in S3 and that sizes match. Optionally compares MD5 hash for byte identity.
Run from repo root. Uses same bucket/prefix/IRS base URL as script 02.

  python tests/990_irs/verify_irs_vs_s3.py --year 2021
  python tests/990_irs/verify_irs_vs_s3.py --year 2021 --zip 2021_TEOS_XML_01A.zip --hash
"""

import hashlib
import os
import re
import sys
from pathlib import Path

import boto3
import requests

_FILE_DIR = Path(__file__).resolve().parent
_REPO_ROOT = _FILE_DIR.parent.parent
_PYTHON = _REPO_ROOT / "python"
if str(_PYTHON) not in sys.path:
    sys.path.insert(0, str(_PYTHON))

BUCKET = os.environ.get("IRS_990_S3_BUCKET", "swb-321-irs990-teos")
PREFIX = os.environ.get("IRS_990_S3_PREFIX", "bronze/irs990/teos_xml")
IRS_BASE = "https://apps.irs.gov/pub/epostcard/990/xml"

# Only verify bulk-part ZIPs (e.g. 01A), not per-filing or error objects
BULK_PART_PATTERN = re.compile(r"^\d{4}_TEOS_XML_\d{2}[A-Z]?\.zip$", re.IGNORECASE)
CHUNK = 4 * 1024 * 1024  # 4 MB


def _session():
    region = os.environ.get("AWS_DEFAULT_REGION", "us-east-2")
    return boto3.Session(region_name=region)


def _probe_one_zip(year: int, part: str, session: requests.Session) -> str | None:
    """HEAD one ZIP URL; return zip_name if 200, else None."""
    zip_name = f"{year}_TEOS_XML_{part}.zip"
    url = f"{IRS_BASE}/{year}/{zip_name}"
    try:
        r = session.head(url, timeout=30, allow_redirects=True)
        return zip_name if r.status_code == 200 else None
    except Exception:
        return None


def list_irs_zips_for_year(year: int, http_session: requests.Session) -> list[str]:
    """Probe IRS for existing bulk-part ZIPs (01A, 01B, ... 12D); return sorted list of filenames."""
    parts_to_try = [f"{m:02d}{s}" for m in range(1, 13) for s in "ABCD"]
    existing = []
    for part in parts_to_try:
        name = _probe_one_zip(year, part, http_session)
        if name:
            existing.append(name)
    return sorted(existing)


def list_s3_zips_for_year(bucket: str, prefix: str, year: int, s3_client) -> list[str]:
    prefix_with_year = f"{prefix}/zips/year={year}/"
    names = []
    paginator = s3_client.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix_with_year):
        for obj in page.get("Contents") or []:
            key = obj.get("Key") or ""
            if key.endswith(".zip"):
                name = key.split("/")[-1]
                if BULK_PART_PATTERN.match(name):
                    names.append(name)
    return sorted(names)


def get_irs_size(year: int, zip_name: str, session: requests.Session) -> int | None:
    """HEAD request; return Content-Length or None."""
    url = f"{IRS_BASE}/{year}/{zip_name}"
    try:
        r = session.head(url, timeout=60, allow_redirects=True)
        r.raise_for_status()
        cl = r.headers.get("Content-Length")
        return int(cl) if cl is not None else None
    except (requests.RequestException, ValueError):
        return None


def get_irs_size_and_md5(year: int, zip_name: str, session: requests.Session, do_hash: bool) -> tuple[int | None, str | None]:
    """If do_hash: GET and compute size + MD5. Else: HEAD and return (Content-Length, None)."""
    url = f"{IRS_BASE}/{year}/{zip_name}"
    if not do_hash:
        size = get_irs_size(year, zip_name, session)
        return (size, None)
    try:
        r = session.get(url, stream=True, timeout=300, allow_redirects=True)
        r.raise_for_status()
    except requests.RequestException:
        return (None, None)
    size = 0
    h = hashlib.md5()
    for chunk in r.iter_content(chunk_size=CHUNK):
        size += len(chunk)
        h.update(chunk)
    return (size, h.hexdigest())


def get_s3_size_and_etag(bucket: str, key: str, s3_client, do_hash: bool) -> tuple[int | None, str | None]:
    """Return (ContentLength, ETag or MD5). For single-part uploads ETag is MD5 hex (with quotes)."""
    try:
        if do_hash:
            obj = s3_client.get_object(Bucket=bucket, Key=key)
            size = 0
            h = hashlib.md5()
            for chunk in obj["Body"].iter_chunks(chunk_size=CHUNK):
                size += len(chunk)
                h.update(chunk)
            return (size, h.hexdigest())
        resp = s3_client.head_object(Bucket=bucket, Key=key)
        size = resp.get("ContentLength")
        etag = resp.get("ETag", "").strip('"')
        return (size, etag)
    except Exception:
        return (None, None)


def main() -> None:
    import argparse
    parser = argparse.ArgumentParser(description="Verify S3 ZIPs match IRS source (size and optional hash)")
    parser.add_argument("--bucket", default=BUCKET, help="S3 bucket")
    parser.add_argument("--prefix", default=PREFIX, help="S3 prefix")
    parser.add_argument("--year", type=int, required=True, help="Tax year (e.g. 2021)")
    parser.add_argument("--zip", type=str, default=None, help="Single ZIP filename (e.g. 2021_TEOS_XML_01A.zip); default: all bulk-part ZIPs for year")
    parser.add_argument("--hash", action="store_true", help="Compare MD5 (download both; slower but confirms byte identity)")
    args = parser.parse_args()

    os.environ["AWS_DEFAULT_REGION"] = os.environ.get("AWS_DEFAULT_REGION", "us-east-2")
    session = _session()
    s3 = session.client("s3")
    http_session = requests.Session()

    if args.zip:
        zips = [args.zip]
        if not BULK_PART_PATTERN.match(args.zip):
            print(f"Warning: {args.zip} does not match bulk-part pattern; verifying anyway.", file=sys.stderr)
        irs_zips = zips
        s3_zips = list_s3_zips_for_year(args.bucket, args.prefix, args.year, s3)
    else:
        print(f"Listing bulk-part ZIPs on IRS for {args.year}...", flush=True)
        irs_zips = list_irs_zips_for_year(args.year, http_session)
        print(f"  IRS has {len(irs_zips)} bulk-part ZIP(s): {', '.join(irs_zips)}", flush=True)
        s3_zips = list_s3_zips_for_year(args.bucket, args.prefix, args.year, s3)
        print(f"  S3 has {len(s3_zips)} bulk-part ZIP(s): {', '.join(s3_zips)}", flush=True)

        missing_in_s3 = sorted(set(irs_zips) - set(s3_zips))
        extra_in_s3 = sorted(set(s3_zips) - set(irs_zips))
        if missing_in_s3:
            print(f"  MISSING IN S3 (on IRS but not in our bucket): {missing_in_s3}", file=sys.stderr)
        if extra_in_s3:
            print(f"  EXTRA IN S3 (in bucket but not on IRS): {extra_in_s3}", flush=True)

        if not irs_zips:
            print(f"No bulk-part ZIPs found on IRS for year={args.year}.", file=sys.stderr)
            sys.exit(1)
        if missing_in_s3:
            print(f"FAIL: We are missing {len(missing_in_s3)} ZIP(s) that exist on the IRS.", file=sys.stderr)
            sys.exit(1)

        zips = irs_zips  # verify each IRS zip (size/hash)
        print()
        print(f"Verifying each of {len(zips)} IRS ZIP(s) exists in S3 with matching size...", flush=True)

    print(f"  IRS base: {IRS_BASE}")
    print(f"  S3: s3://{args.bucket}/{args.prefix}/zips/year={args.year}/")
    print(f"  Hash comparison: {args.hash}")
    print()

    ok = 0
    size_mismatch = 0
    hash_mismatch = 0
    irs_error = 0
    s3_error = 0

    for zip_name in zips:
        s3_key = f"{args.prefix}/zips/year={args.year}/{zip_name}"
        irs_size, irs_md5 = get_irs_size_and_md5(args.year, zip_name, http_session, do_hash=args.hash)
        s3_size, s3_etag = get_s3_size_and_etag(args.bucket, s3_key, s3, do_hash=args.hash)

        if irs_size is None:
            print(f"  {zip_name}: IRS error (missing or request failed)")
            irs_error += 1
            continue
        if s3_size is None:
            print(f"  {zip_name}: S3 error (missing or request failed)")
            s3_error += 1
            continue
        if irs_size != s3_size:
            print(f"  {zip_name}: SIZE MISMATCH  IRS={irs_size:,}  S3={s3_size:,}")
            size_mismatch += 1
            continue
        if args.hash and irs_md5 and s3_etag and irs_md5 != s3_etag:
            print(f"  {zip_name}: HASH MISMATCH  IRS MD5={irs_md5}  S3={s3_etag}")
            hash_mismatch += 1
            continue
        ok += 1
        if args.hash:
            print(f"  {zip_name}: OK  size={s3_size:,}  MD5={irs_md5}")
        else:
            print(f"  {zip_name}: OK  size={s3_size:,}")

    print()
    print(f"Summary: {ok} match, {size_mismatch} size mismatch, {hash_mismatch} hash mismatch, {irs_error} IRS errors, {s3_error} S3 errors")
    if size_mismatch or hash_mismatch or s3_error:
        sys.exit(1)
    if irs_error and ok == 0:
        sys.exit(1)


if __name__ == "__main__":
    main()
