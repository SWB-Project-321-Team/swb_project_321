"""
Upload IRS Form 990 TEOS index CSV files (2021 through current year) from IRS to S3.

Streams each index_{YEAR}.csv from the IRS URL directly to the project's S3 bucket.
No large local files. Uses requests (stream=True) and boto3.

IRS index URL: https://apps.irs.gov/pub/epostcard/990/xml/{YEAR}/index_{YEAR}.csv
S3 key: s3://{bucket}/{prefix}/index/year={YEAR}/index_{YEAR}.csv

Environment (or .env): AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, AWS_DEFAULT_REGION.
Override via: --bucket, --prefix, --region. Defaults: bucket/prefix from docs/990/990_irs_teos_s3_plan.md.

Run from repo root:
  python python/ingest/990_irs/01_upload_irs_990_index_to_s3.py
  python python/ingest/990_irs/01_upload_irs_990_index_to_s3.py --start-year 2021 --end-year 2026
  python python/ingest/990_irs/01_upload_irs_990_index_to_s3.py --workers 4
"""

import os
import sys
from io import BytesIO
from pathlib import Path

import boto3
import requests
from tqdm import tqdm

# Load secrets/.env so AWS credentials (and region) are set like 00_fetch_bmf.py S3 uploads.
_REPO_ROOT = Path(__file__).resolve().parents[3]
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
    print("[01_upload_irs_990_index_to_s3] Loaded credentials from secrets/.env", flush=True)
else:
    print("[01_upload_irs_990_index_to_s3] No secrets/.env found; using existing env for AWS", flush=True)

# Chunk size for streaming download (larger = fewer syscalls; 16 MB default)
DOWNLOAD_CHUNK_MB = 16
DOWNLOAD_CHUNK = 1024 * 1024 * DOWNLOAD_CHUNK_MB


class _StreamingBody:
    """File-like wrapper for HTTP response; stream to S3 without buffering full body."""

    def __init__(self, response, chunk_size: int = DOWNLOAD_CHUNK):
        self._iter = response.iter_content(chunk_size=chunk_size)
        self._buf = b""
        self._exhausted = False

    def read(self, size: int = -1) -> bytes:
        if size == -1:
            chunks = [self._buf]
            self._buf = b""
            if not self._exhausted:
                try:
                    for c in self._iter:
                        chunks.append(c)
                except StopIteration:
                    pass
                self._exhausted = True
            return b"".join(chunks)
        while len(self._buf) < size and not self._exhausted:
            try:
                c = next(self._iter)
                self._buf += c
            except StopIteration:
                self._exhausted = True
                break
        out = self._buf[:size]
        self._buf = self._buf[size:]
        return out

DEST_BUCKET = os.environ.get("IRS_990_S3_BUCKET", "swb-321-irs990-teos")
PREFIX = os.environ.get("IRS_990_S3_PREFIX", "bronze/irs990/teos_xml")
IRS_INDEX_URL = "https://apps.irs.gov/pub/epostcard/990/xml/{year}/index_{year}.csv"


def _session(region: str | None = None):
    r = region or os.environ.get("AWS_DEFAULT_REGION", "us-east-2")
    return boto3.Session(region_name=r)


def upload_index_for_year(
    year: int,
    bucket: str,
    prefix: str,
    region: str,
    http_session: requests.Session | None = None,
    boto_session: boto3.Session | None = None,
    skip_existing: bool = True,
    chunk_bytes: int = DOWNLOAD_CHUNK,
) -> bool:
    """Stream index_{year}.csv from IRS to S3. Returns True on success or skip, False on error."""
    url = IRS_INDEX_URL.format(year=year)
    s3_key = f"{prefix}/index/year={year}/index_{year}.csv"
    s3 = (boto_session or _session(region)).client("s3")
    if skip_existing:
        try:
            s3.head_object(Bucket=bucket, Key=s3_key)
            print(f"  [{year}] Skip (already in S3)", flush=True)
            return True
        except Exception:
            pass
    print(f"  [{year}] Downloading index from IRS...", flush=True)
    sess = http_session or requests.Session()
    try:
        r = sess.get(url, stream=True, timeout=60, allow_redirects=True)
        r.raise_for_status()
    except requests.RequestException as e:
        print(f"  Skip {year}: {e}", file=sys.stderr)
        return False

    content_length = r.headers.get("Content-Length")
    try:
        # Buffer full body then upload (same pattern as 00_fetch_bmf) so boto3 signing matches and works with same credentials.
        buf = BytesIO()
        for chunk in r.iter_content(chunk_size=chunk_bytes):
            buf.write(chunk)
        body = buf.getvalue()
        size_mb = len(body) / (1024 * 1024)
        print(f"  [{year}] Uploading to S3 ({size_mb:.2f} MB)...", flush=True)
        s3.put_object(Bucket=bucket, Key=s3_key, Body=body, ContentType="text/csv")
        print(f"  [{year}] Uploaded index_{year}.csv -> s3://{bucket}/{s3_key}")
        return True
    except Exception as e:
        print(f"  Upload failed {year}: {e}", file=sys.stderr)
        return False


def _upload_one_year(
    year: int, bucket: str, prefix: str, region: str, skip_existing: bool, chunk_bytes: int
) -> tuple[int, bool]:
    """Worker-friendly: no shared session. Returns (year, success)."""
    boto_sess = _session(region)
    http_sess = requests.Session()
    ok = upload_index_for_year(
        year, bucket, prefix, region,
        http_session=http_sess, boto_session=boto_sess,
        skip_existing=skip_existing, chunk_bytes=chunk_bytes,
    )
    return (year, ok)


def main() -> None:
    import argparse
    import time
    from concurrent.futures import ThreadPoolExecutor, as_completed

    parser = argparse.ArgumentParser(description="Stream IRS 990 TEOS index CSVs to S3")
    parser.add_argument("--bucket", default=DEST_BUCKET, help="S3 bucket name")
    parser.add_argument("--prefix", default=PREFIX, help="S3 key prefix")
    parser.add_argument("--region", default=os.environ.get("AWS_DEFAULT_REGION", "us-east-2"), help="AWS region")
    parser.add_argument("--start-year", type=int, default=2021, help="First tax year (default 2021)")
    parser.add_argument("--end-year", type=int, default=None, help="Last tax year (default: current year)")
    parser.add_argument("--workers", type=int, default=1, help="Parallel year uploads (default 1)")
    parser.add_argument("--no-skip-existing", action="store_true", help="Re-upload even if index already in S3 (default: skip existing)")
    parser.add_argument("--chunk-size", type=int, default=DOWNLOAD_CHUNK_MB, metavar="MB", help=f"Download chunk size in MB (default {DOWNLOAD_CHUNK_MB})")
    args = parser.parse_args()

    end_year = args.end_year
    if end_year is None:
        from datetime import date
        end_year = date.today().year

    if args.start_year > end_year:
        print("start-year must be <= end-year", file=sys.stderr)
        sys.exit(1)

    n_years = end_year - args.start_year + 1
    workers = max(1, min(args.workers, n_years))
    skip_existing = not args.no_skip_existing
    chunk_bytes = max(1, args.chunk_size) * 1024 * 1024
    print(f"Uploading IRS 990 TEOS index files to s3://{args.bucket}/{args.prefix}/")
    print(f"Years: {args.start_year} through {end_year} ({n_years} files), workers: {workers}, skip existing: {skip_existing}")
    print()

    os.environ["AWS_DEFAULT_REGION"] = args.region
    start = time.perf_counter()
    ok = 0
    if workers <= 1:
        boto_sess = _session(args.region)
        http_sess = requests.Session()
        for year in tqdm(range(args.start_year, end_year + 1), desc="Index uploads", unit="year"):
            if upload_index_for_year(
                year, args.bucket, args.prefix, args.region,
                http_session=http_sess, boto_session=boto_sess,
                skip_existing=skip_existing, chunk_bytes=chunk_bytes,
            ):
                ok += 1
    else:
        with ThreadPoolExecutor(max_workers=workers) as executor:
            years = list(range(args.start_year, end_year + 1))
            futures = {
                executor.submit(_upload_one_year, y, args.bucket, args.prefix, args.region, skip_existing, chunk_bytes): y
                for y in years
            }
            for fut in tqdm(as_completed(futures), total=len(futures), desc="Index uploads", unit="year"):
                y, success = fut.result()
                if success:
                    ok += 1
                tqdm.write(f"  [{y}] Completed.")
    elapsed = time.perf_counter() - start
    print(f"Done: {ok}/{n_years} index files uploaded to s3://{args.bucket}/{args.prefix}/")
    print(f"Elapsed: {elapsed:.1f} s ({elapsed / 60:.1f} min)")


if __name__ == "__main__":
    main()
