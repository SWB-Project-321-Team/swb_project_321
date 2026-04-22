"""
Upload IRS Form 990 TEOS XML ZIP parts (2021 through current year) from IRS to S3.

Uploads all ZIP parts per year. Filter by geography (GEOID/county) is applied in 03_parse
and 04_merge using GEOID_reference.csv and zip_to_county_fips.csv.

Reads the index for each year (from S3 or IRS) to get ZIP part filenames, then streams each
ZIP from IRS to S3.

IRS ZIP URL: https://apps.irs.gov/pub/epostcard/990/xml/{YEAR}/{YEAR}_TEOS_XML_{PART}.zip
S3 key: s3://{bucket}/{prefix}/zips/year={YEAR}/{ZIP_FILENAME}

Environment: AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, AWS_DEFAULT_REGION.
Optional: IRS_990_S3_BUCKET, IRS_990_S3_PREFIX.

Run from repo root:
  python python/ingest/990_irs/02_upload_irs_990_zips_to_s3.py
  python python/ingest/990_irs/02_upload_irs_990_zips_to_s3.py --workers 4
"""

import os
import re
import sys
from io import BytesIO
from pathlib import Path

import boto3
import pandas as pd
import requests
from tqdm import tqdm

# Load secrets/.env so AWS credentials and region are set (same as 00_fetch_bmf and 01_upload_irs_990_index_to_s3).
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

# Chunk size for streaming download (larger = fewer syscalls; 16 MB default)
DOWNLOAD_CHUNK_MB = 16
DOWNLOAD_CHUNK = 16 * 1024 * 1024  # 16 MB


class _StreamingBody:
    """File-like wrapper for HTTP response body; allows streaming to S3 without buffering entire body in memory."""

    def __init__(self, response: requests.Response, chunk_size: int = DOWNLOAD_CHUNK):
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

# Ensure python/ is on path so we can import utils
_SCRIPT_DIR = Path(__file__).resolve()
_PYTHON_DIR = _SCRIPT_DIR.parents[2]
if str(_PYTHON_DIR) not in sys.path:
    sys.path.insert(0, str(_PYTHON_DIR))
from utils.paths import DATA

DEST_BUCKET = os.environ.get("IRS_990_S3_BUCKET", "swb-321-irs990-teos")
PREFIX = os.environ.get("IRS_990_S3_PREFIX", "bronze/irs990/teos_xml")
IRS_BASE = "https://apps.irs.gov/pub/epostcard/990/xml"

# Column name candidates for index CSV (EIN/TIN and ZIP part filename)
# Do NOT use OBJECT_ID - IRS index uses it for per-filing numeric IDs (17897417), not bulk ZIP names (01A).
PART_COL_CANDIDATES = ("Filename", "ZipFile", "Part", "File", "Package", "ZipPart")
# Index values that are NOT bulk ZIP part codes (e.g. filing type "EFILE") - exclude from part list
NOT_PART_VALUES = frozenset({"EFILE", "PDF", "XML", "990", "990EZ", "990PF", "990T", "FULL", "PARTIAL"})


def _session(region: str | None = None):
    if region is None:
        region = os.environ.get("AWS_DEFAULT_REGION", "us-east-2")
    return boto3.Session(region_name=region)


def _find_part_column(df: pd.DataFrame, year: int) -> str | None:
    # Find column whose values look like "2024_TEOS_XML_01A" or "01A" - NOT per-filing numeric IDs (OBJECT_ID).
    pattern = re.compile(rf"^{year}_TEOS_XML_[A-Z0-9]+(\.zip)?$", re.I)
    pattern_any_year = re.compile(r"^\d{4}_TEOS_XML_[A-Z0-9]+(\.zip)?$", re.I)
    part_code_pattern = re.compile(r"^[A-Z0-9]{2,6}$", re.I)
    # IRS index OBJECT_ID is per-filing numeric (17897417) - never use it as bulk ZIP part
    numeric_id_pattern = re.compile(r"^\d{5,}$")

    def sample_matches(s: pd.Series, col_name: str) -> bool:
        s = s.astype(str).str.strip()
        # Never use OBJECT_ID / ObjectID when values are numeric IDs (per-filing, not bulk ZIP names)
        if col_name.upper().replace("_", "") == "OBJECTID":
            if s.str.match(numeric_id_pattern, na=False).any() or s.str.match(r"^\d{5,}$", na=False).sum() >= len(s) * 0.5:
                return False
        # Reject any column that is mostly long numeric IDs
        if s.str.match(r"^\d{6,}$", na=False).sum() >= len(s) * 0.5:
            return False
        if s.str.match(pattern).any():
            return True
        if s.str.match(pattern_any_year).any():
            return True
        if s.str.contains("TEOS_XML", case=False, na=False).any():
            return True
        if s.str.match(part_code_pattern, na=False).any():
            return True
        return False

    for col in PART_COL_CANDIDATES:
        if col in df.columns:
            sample = df[col].dropna().head(200)
            if sample_matches(sample, col):
                return col
    for col in df.columns:
        if col.upper().replace("_", "") == "OBJECTID":
            continue
        sample = df[col].dropna().head(100)
        if sample_matches(sample, col):
            return col
    return None


def _is_likely_bulk_part_code(val: str) -> bool:
    """True if value looks like a bulk ZIP part code (01A, 11D), not a per-filing ID or filing type."""
    s = str(val).strip().upper()
    if not s:
        return False
    if s in NOT_PART_VALUES:
        return False
    # Reject pure numeric IDs (5+ digits) - per-filing OBJECT_ID
    if re.match(r"^\d{5,}$", s):
        return False
    # Full filename like 2021_TEOS_XML_01A.zip
    if re.match(r"^\d{4}_TEOS_XML_[A-Z0-9]+(\.ZIP)?$", s):
        return True
    # Bulk part codes are 2 digits + 1 letter (01A, 11D), not "EFILE" or "PDF"
    if re.match(r"^\d{2}[A-Z]$", s):
        return True
    return False


def _probe_one_zip(year: int, part: str, session: requests.Session | None = None) -> str | None:
    """HEAD one ZIP URL; return zip_name if 200, else None."""
    zip_name = f"{year}_TEOS_XML_{part}.zip"
    url = f"{IRS_BASE}/{year}/{zip_name}"
    sess = session or requests.Session()
    try:
        r = sess.head(url, timeout=30, allow_redirects=True)
        return zip_name if r.status_code == 200 else None
    except Exception:
        return None


def get_teos_zip_parts_by_probing(
    year: int, stop_after_404s: int = 5, session: requests.Session | None = None, max_workers: int = 12
) -> list[str]:
    """Probe IRS for existing TEOS bulk ZIPs; return list of filenames that exist.
    Part codes tried: 01A, 01B, ... 12D. Uses parallel HEAD requests when max_workers > 1.
    """
    from concurrent.futures import ThreadPoolExecutor, as_completed

    parts_to_try = [f"{m:02d}{s}" for m in range(1, 13) for s in "ABCD"]
    if max_workers <= 1:
        sess = session or requests.Session()
        existing = []
        consecutive_404 = 0
        for part in parts_to_try:
            zip_name = _probe_one_zip(year, part, sess)
            if zip_name:
                existing.append(zip_name)
                consecutive_404 = 0
            else:
                consecutive_404 += 1
            if consecutive_404 >= stop_after_404s:
                break
        return existing
    sess = session or requests.Session()
    existing = []
    with ThreadPoolExecutor(max_workers=min(max_workers, len(parts_to_try))) as executor:
        futures = {executor.submit(_probe_one_zip, year, p, sess): p for p in parts_to_try}
        for fut in as_completed(futures):
            name = fut.result()
            if name:
                existing.append(name)
    return sorted(existing)


def _normalize_zip_filename(val: str, year: int) -> str:
    """Return ZIP filename like 2024_TEOS_XML_01A.zip."""
    s = str(val).strip()
    # Already a full filename (with or without .zip)? Same year or any year.
    if re.match(rf"^{year}_TEOS_XML_[A-Z0-9]+(\.zip)?$", s, re.I):
        return s if s.lower().endswith(".zip") else f"{s}.zip"
    if re.match(r"^\d{4}_TEOS_XML_[A-Z0-9]+(\.zip)?$", s, re.I):
        return s if s.lower().endswith(".zip") else f"{s}.zip"
    if s.lower().endswith(".zip") and len(s) > 4:
        return s
    # Assume part id like 01A
    return f"{year}_TEOS_XML_{s}.zip"


def get_index_df_from_s3(year: int, bucket: str, prefix: str, session: boto3.Session) -> pd.DataFrame | None:
    key = f"{prefix}/index/year={year}/index_{year}.csv"
    try:
        s3 = session.client("s3")
        obj = s3.get_object(Bucket=bucket, Key=key)
        return pd.read_csv(obj["Body"], low_memory=False)
    except Exception:
        return None


def get_index_df_from_irs(year: int, session: requests.Session | None = None) -> pd.DataFrame | None:
    url = f"{IRS_BASE}/{year}/index_{year}.csv"
    sess = session or requests.Session()
    try:
        r = sess.get(url, timeout=120, allow_redirects=True)
        r.raise_for_status()
        return pd.read_csv(BytesIO(r.content), low_memory=False)
    except Exception:
        return None


def get_index_for_year(
    year: int,
    bucket: str,
    prefix: str,
    session: boto3.Session,
    http_session: requests.Session | None = None,
) -> pd.DataFrame | None:
    df = get_index_df_from_s3(year, bucket, prefix, session)
    if df is not None:
        print(f"  [{year}] Read index from S3 ({len(df):,} rows)", flush=True)
        return df
    print(f"  [{year}] Index not in S3, fetching from IRS...", flush=True)
    df = get_index_df_from_irs(year, session=http_session)
    if df is not None:
        print(f"  [{year}] Read index from IRS ({len(df):,} rows)", flush=True)
    return df


def list_existing_zip_keys_for_year(
    bucket: str, prefix: str, year: int, s3_client
) -> set[str]:
    """List S3 keys under prefix/zips/year={year}/ and return set of ZIP filenames (e.g. 2021_TEOS_XML_01A.zip)."""
    prefix_with_year = f"{prefix}/zips/year={year}/"
    existing = set()
    paginator = s3_client.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix_with_year):
        for obj in page.get("Contents") or []:
            key = obj.get("Key") or ""
            if key.endswith(".zip"):
                existing.add(key.split("/")[-1])
    return existing


def unique_zip_parts_for_year(
    year: int,
    bucket: str,
    prefix: str,
    session: boto3.Session,
    http_session: requests.Session | None = None,
) -> list[str]:
    """Return list of ZIP filenames (e.g. 2024_TEOS_XML_01A.zip) to upload for this year. Uploads all parts; filter by GEOID/county in 04/05."""
    df = get_index_for_year(year, bucket, prefix, session, http_session=http_session)
    if df is None or df.empty:
        return []

    part_col = _find_part_column(df, year)
    if part_col is None:
        # Try any column that looks like a filename
        for col in df.columns:
            sample = df[col].dropna().astype(str)
            if sample.str.contains("TEOS_XML", case=False, na=False).any():
                part_col = col
                break
    if part_col is None:
        print(f"  [{year}] No ZIP part column in index; probing IRS for bulk ZIPs...", flush=True)
        part_list = get_teos_zip_parts_by_probing(year)
        print(f"  [{year}] Found {len(part_list)} bulk ZIP(s) to upload", flush=True)
        return part_list

    parts = df[part_col].dropna().astype(str).str.strip().unique()
    # Keep only values that look like bulk part codes (01A, 2021_TEOS_XML_01A), not per-filing numeric IDs
    parts = [p for p in parts if _is_likely_bulk_part_code(p)]
    if not parts:
        print(f"  [{year}] Index part column has no bulk part codes; probing IRS for bulk ZIPs...", flush=True)
        part_list = get_teos_zip_parts_by_probing(year)
        print(f"  [{year}] Found {len(part_list)} bulk ZIP(s) to upload", flush=True)
        return part_list
    part_list = sorted({_normalize_zip_filename(p, year) for p in parts})
    print(f"  [{year}] Found {len(part_list)} bulk ZIP(s) to upload", flush=True)
    return part_list


def upload_zip_to_s3(
    year: int,
    zip_name: str,
    bucket: str,
    prefix: str,
    session: boto3.Session,
    skip_existing: bool = True,
    existing_zip_names: set[str] | None = None,
    http_session: requests.Session | None = None,
    s3_client=None,
    chunk_size: int = DOWNLOAD_CHUNK,
) -> bool:
    s3_key = f"{prefix}/zips/year={year}/{zip_name}"
    if existing_zip_names is not None and zip_name in existing_zip_names:
        print(f"    Skip {zip_name} (already in S3)", flush=True)
        return True
    if skip_existing and existing_zip_names is None:
        s3 = s3_client or session.client("s3")
        try:
            s3.head_object(Bucket=bucket, Key=s3_key)
            print(f"    Skip {zip_name} (already in S3)", flush=True)
            return True
        except Exception:
            pass
    url = f"{IRS_BASE}/{year}/{zip_name}"
    print(f"    Downloading {zip_name} from IRS...", flush=True)
    sess = http_session or requests.Session()
    try:
        r = sess.get(url, stream=True, timeout=300, allow_redirects=True)
        r.raise_for_status()
    except requests.RequestException as e:
        print(f"    Skip {zip_name}: {e}", file=sys.stderr)
        return False

    s3 = s3_client or session.client("s3")
    try:
        # Buffer full body then upload (same pattern as 00_fetch_bmf and 02) so boto3 signing works with same credentials.
        buf = BytesIO()
        for chunk in r.iter_content(chunk_size=chunk_size):
            buf.write(chunk)
        body = buf.getvalue()
        size_mb = len(body) / (1024 * 1024)
        print(f"    Uploading {zip_name} to S3 ({size_mb:.1f} MB)...", flush=True)
        s3.put_object(Bucket=bucket, Key=s3_key, Body=body)
        print(f"    Uploaded {zip_name} -> s3://{bucket}/{s3_key}")
        return True
    except Exception as e:
        print(f"    Upload failed {zip_name}: {e}", file=sys.stderr)
        return False


def main() -> None:
    import argparse
    import time
    from datetime import date

    parser = argparse.ArgumentParser(description="Stream IRS 990 TEOS ZIP parts to S3")
    parser.add_argument("--bucket", default=DEST_BUCKET, help="S3 bucket")
    parser.add_argument("--prefix", default=PREFIX, help="S3 prefix")
    parser.add_argument("--region", default=os.environ.get("AWS_DEFAULT_REGION", "us-east-2"))
    parser.add_argument("--start-year", type=int, default=2021)
    parser.add_argument("--end-year", type=int, default=None)
    parser.add_argument("--no-skip-existing", action="store_true", help="Upload even if object already exists in S3 (default: skip existing)")
    parser.add_argument("--workers", type=int, default=1, help="Parallel ZIP uploads (default 1)")
    parser.add_argument("--chunk-size", type=int, default=DOWNLOAD_CHUNK_MB, metavar="MB", help=f"Download stream chunk size in MB (default {DOWNLOAD_CHUNK_MB})")
    args = parser.parse_args()

    end_year = args.end_year or date.today().year
    if args.start_year > end_year:
        print("start-year must be <= end-year", file=sys.stderr)
        sys.exit(1)

    print("Uploading all ZIP parts (filter by GEOID/county in 04/05).")

    skip_existing = not args.no_skip_existing
    workers = max(1, args.workers)
    chunk_size = max(1, args.chunk_size) * 1024 * 1024
    print(f"Target: s3://{args.bucket}/{args.prefix}/")
    print(f"Years: {args.start_year} through {end_year}, workers: {workers}")
    print()

    os.environ["AWS_DEFAULT_REGION"] = args.region
    session = _session()
    s3_client = session.client("s3")
    http_session = requests.Session() if workers <= 1 else None
    start = time.perf_counter()
    total_ok = 0
    total_skip = 0

    if workers <= 1:
        for year in tqdm(range(args.start_year, end_year + 1), desc="Years", unit="year"):
            tqdm.write(f"--- Year {year} ---")
            parts = unique_zip_parts_for_year(
                year, args.bucket, args.prefix, session, http_session=http_session
            )
            if not parts:
                tqdm.write(f"  [{year}] No parts to upload, skipping.")
                continue
            existing = set()
            if skip_existing:
                existing = list_existing_zip_keys_for_year(args.bucket, args.prefix, year, s3_client)
                if existing:
                    tqdm.write(f"  [{year}] Found {len(existing)} existing ZIP(s) in S3; skipping those.")
            for zip_name in tqdm(parts, desc=f"ZIPs {year}", unit="zip", leave=False):
                if upload_zip_to_s3(
                    year, zip_name, args.bucket, args.prefix, session,
                    skip_existing=False, existing_zip_names=existing or None,
                    http_session=http_session, s3_client=s3_client,
                    chunk_size=chunk_size,
                ):
                    total_ok += 1
                else:
                    total_skip += 1
    else:
        from concurrent.futures import ThreadPoolExecutor, as_completed

        tasks = []
        for year in range(args.start_year, end_year + 1):
            tqdm.write(f"--- Year {year} ---")
            parts = unique_zip_parts_for_year(year, args.bucket, args.prefix, session)
            if not parts:
                tqdm.write(f"  [{year}] No parts to upload, skipping.")
                continue
            existing = list_existing_zip_keys_for_year(args.bucket, args.prefix, year, s3_client) if skip_existing else set()
            if existing:
                tqdm.write(f"  [{year}] Found {len(existing)} existing ZIP(s) in S3; skipping those.")
            for zip_name in parts:
                tasks.append((year, zip_name, existing if skip_existing else None))

        def _upload_one_zip(item):
            year, zip_name, existing_for_year = item
            boto_sess = _session(args.region)
            http_sess = requests.Session()
            ok = upload_zip_to_s3(
                year, zip_name, args.bucket, args.prefix, boto_sess,
                skip_existing=False, existing_zip_names=existing_for_year,
                http_session=http_sess,
                chunk_size=chunk_size,
            )
            return (year, zip_name, ok)

        with ThreadPoolExecutor(max_workers=workers) as executor:
            futures = {executor.submit(_upload_one_zip, t): t for t in tasks}
            for fut in tqdm(as_completed(futures), total=len(futures), desc="ZIP uploads", unit="zip"):
                year, zip_name, ok = fut.result()
                if ok:
                    total_ok += 1
                else:
                    total_skip += 1
                tqdm.write(f"  [{year}] {zip_name} completed.")

    elapsed = time.perf_counter() - start
    print(f"Done: {total_ok} uploaded, {total_skip} skipped.")
    print(f"Elapsed: {elapsed:.1f} s ({elapsed / 60:.1f} min)")


if __name__ == "__main__":
    main()
