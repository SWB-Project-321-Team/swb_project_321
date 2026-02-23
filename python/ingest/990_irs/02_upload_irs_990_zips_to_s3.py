"""
Upload IRS Form 990 TEOS XML ZIP parts (2021 through current year) from IRS to S3.

Reads the index for each year (from S3 or from IRS) to get the list of ZIP part filenames,
optionally filters to EINs in the project EIN list, then streams each required ZIP from IRS to S3.

IRS ZIP URL: https://apps.irs.gov/pub/epostcard/990/xml/{YEAR}/{YEAR}_TEOS_XML_{PART}.zip
S3 key: s3://{bucket}/{prefix}/zips/year={YEAR}/{ZIP_FILENAME}

Options:
  --all-parts     Upload every ZIP part that appears in the index (no EIN filter).
  --ein-list PATH Use project EIN list and upload only ZIP parts that contain at least one listed EIN.
  If neither: defaults to --ein-list from 01_data/reference/eins_in_benchmark_regions.csv if present,
              else --all-parts.

Environment: AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, AWS_DEFAULT_REGION.
Optional: IRS_990_S3_BUCKET, IRS_990_S3_PREFIX.

Run from repo root:
  python python/ingest/990_irs/02_upload_irs_990_zips_to_s3.py --all-parts
  python python/ingest/990_irs/02_upload_irs_990_zips_to_s3.py --ein-list path/to/eins.csv
"""

import os
import re
import sys
from io import BytesIO
from pathlib import Path

import boto3
import pandas as pd
import requests

# Repo root = 4 levels up
BASE = Path(__file__).resolve().parent.parent.parent.parent
DATA = BASE / "data" / "321_Black_Hills_Area_Community_Foundation_2025_08" / "01_data"
EIN_LIST_CSV = DATA / "reference" / "eins_in_benchmark_regions.csv"

DEST_BUCKET = os.environ.get("IRS_990_S3_BUCKET", "swb-321-irs990-teos")
PREFIX = os.environ.get("IRS_990_S3_PREFIX", "bronze/irs990/teos_xml")
IRS_BASE = "https://apps.irs.gov/pub/epostcard/990/xml"

# Column name candidates for index CSV (EIN/TIN and ZIP part filename)
# Do NOT use OBJECT_ID - IRS index uses it for per-filing numeric IDs (17897417), not bulk ZIP names (01A).
EIN_COL_CANDIDATES = ("EIN", "TIN", "EmployerIdentificationNumber", "ein", "tin")
PART_COL_CANDIDATES = ("Filename", "ZipFile", "Part", "File", "Package", "ZipPart")
# Index values that are NOT bulk ZIP part codes (e.g. filing type "EFILE") - exclude from part list
NOT_PART_VALUES = frozenset({"EFILE", "PDF", "XML", "990", "990EZ", "990PF", "990T", "FULL", "PARTIAL"})


def _session():
    region = os.environ.get("AWS_DEFAULT_REGION", "us-east-2")
    return boto3.Session(region_name=region)


def _normalize_ein(ein: str) -> str:
    s = str(ein).strip().replace("-", "").replace(" ", "")
    return s.zfill(9) if len(s) <= 9 else s[:9]


def _find_ein_column(df: pd.DataFrame) -> str | None:
    for c in EIN_COL_CANDIDATES:
        if c in df.columns:
            return c
    for c in df.columns:
        if "ein" in c.lower() or "tin" in c.lower():
            return c
    return None


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


def get_teos_zip_parts_by_probing(year: int, stop_after_404s: int = 5) -> list[str]:
    """Probe IRS for existing TEOS bulk ZIPs; return list of filenames that exist.
    Part codes tried: 01A, 01B, 02A, 02B, ... 12A, 12B; stop after stop_after_404s consecutive 404s.
    """
    # Common TEOS part codes (month-like 01-12, suffix A/B or A-D)
    parts_to_try = []
    for m in range(1, 13):
        for suffix in "ABCD":
            parts_to_try.append(f"{m:02d}{suffix}")
    existing = []
    consecutive_404 = 0
    for part in parts_to_try:
        zip_name = f"{year}_TEOS_XML_{part}.zip"
        url = f"{IRS_BASE}/{year}/{zip_name}"
        try:
            r = requests.head(url, timeout=30, allow_redirects=True)
            if r.status_code == 200:
                existing.append(zip_name)
                consecutive_404 = 0
            else:
                consecutive_404 += 1
        except Exception:
            consecutive_404 += 1
        if consecutive_404 >= stop_after_404s:
            break
    return existing


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


def get_index_df_from_irs(year: int) -> pd.DataFrame | None:
    url = f"{IRS_BASE}/{year}/index_{year}.csv"
    try:
        r = requests.get(url, timeout=120, allow_redirects=True)
        r.raise_for_status()
        return pd.read_csv(BytesIO(r.content), low_memory=False)
    except Exception:
        return None


def get_index_for_year(year: int, bucket: str, prefix: str, session: boto3.Session) -> pd.DataFrame | None:
    df = get_index_df_from_s3(year, bucket, prefix, session)
    if df is not None:
        print(f"  [{year}] Read index from S3 ({len(df):,} rows)", flush=True)
        return df
    print(f"  [{year}] Index not in S3, fetching from IRS...", flush=True)
    df = get_index_df_from_irs(year)
    if df is not None:
        print(f"  [{year}] Read index from IRS ({len(df):,} rows)", flush=True)
    return df


def unique_zip_parts_for_year(
    year: int,
    bucket: str,
    prefix: str,
    session: boto3.Session,
    ein_set: set[str] | None,
) -> list[str]:
    """Return list of ZIP filenames (e.g. 2024_TEOS_XML_01A.zip) to upload for this year."""
    df = get_index_for_year(year, bucket, prefix, session)
    if df is None or df.empty:
        return []

    ein_col = _find_ein_column(df)
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

    if ein_set is not None and ein_col is None:
        print(f"  [{year}] No EIN column in index; uploading all parts for this year.", flush=True)
    if ein_set is not None and ein_col is not None:
        df["_ein_norm"] = df[ein_col].astype(str).apply(_normalize_ein)
        df = df[df["_ein_norm"].isin(ein_set)]

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
    year: int, zip_name: str, bucket: str, prefix: str, session: boto3.Session, skip_existing: bool = True
) -> bool:
    s3_key = f"{prefix}/zips/year={year}/{zip_name}"
    if skip_existing:
        try:
            s3 = session.client("s3")
            s3.head_object(Bucket=bucket, Key=s3_key)
            print(f"    Skip {zip_name} (already in S3)", flush=True)
            return True
        except Exception:
            pass
    url = f"{IRS_BASE}/{year}/{zip_name}"
    print(f"    Downloading {zip_name} from IRS...", flush=True)
    try:
        r = requests.get(url, stream=True, timeout=300, allow_redirects=True)
        r.raise_for_status()
    except requests.RequestException as e:
        print(f"    Skip {zip_name}: {e}", file=sys.stderr)
        return False

    buf = BytesIO()
    for chunk in r.iter_content(chunk_size=2 * 1024 * 1024):
        buf.write(chunk)
    size_mb = len(buf.getvalue()) / (1024 * 1024)
    buf.seek(0)
    print(f"    Uploading {zip_name} to S3 ({size_mb:.1f} MB)...", flush=True)
    try:
        s3 = session.client("s3")
        s3.upload_fileobj(buf, bucket, s3_key)
        print(f"    Uploaded {zip_name} -> s3://{bucket}/{s3_key}")
        return True
    except Exception as e:
        print(f"    Upload failed {zip_name}: {e}", file=sys.stderr)
        return False


def main() -> None:
    import argparse
    from datetime import date

    parser = argparse.ArgumentParser(description="Stream IRS 990 TEOS ZIP parts to S3")
    parser.add_argument("--bucket", default=DEST_BUCKET, help="S3 bucket")
    parser.add_argument("--prefix", default=PREFIX, help="S3 prefix")
    parser.add_argument("--region", default=os.environ.get("AWS_DEFAULT_REGION", "us-east-2"))
    parser.add_argument("--start-year", type=int, default=2021)
    parser.add_argument("--end-year", type=int, default=None)
    parser.add_argument("--all-parts", action="store_true", help="Upload all ZIP parts (no EIN filter)")
    parser.add_argument("--ein-list", type=Path, default=None, help="CSV with EIN column; only upload parts containing these EINs")
    parser.add_argument("--no-skip-existing", action="store_true", help="Upload even if object already exists in S3 (default: skip existing)")
    args = parser.parse_args()

    end_year = args.end_year or date.today().year
    if args.start_year > end_year:
        print("start-year must be <= end-year", file=sys.stderr)
        sys.exit(1)

    ein_set = None
    if not args.all_parts:
        ein_path = args.ein_list or (EIN_LIST_CSV if EIN_LIST_CSV.exists() else None)
        if ein_path is not None:
            ein_df = pd.read_csv(ein_path)
            ein_col = next((c for c in ein_df.columns if "ein" in c.lower()), ein_df.columns[0])
            ein_set = {_normalize_ein(x) for x in ein_df[ein_col].dropna()}
            print(f"Filtering to {len(ein_set)} EINs from {ein_path}")
        else:
            print("No --ein-list and no default EIN list found; uploading all parts for each year.")
    else:
        print("Uploading all ZIP parts (--all-parts).")

    print(f"Target: s3://{args.bucket}/{args.prefix}/")
    print(f"Years: {args.start_year} through {end_year}")
    print()

    os.environ["AWS_DEFAULT_REGION"] = args.region
    session = _session()
    total_ok = 0
    total_skip = 0
    for year in range(args.start_year, end_year + 1):
        print(f"--- Year {year} ---", flush=True)
        parts = unique_zip_parts_for_year(year, args.bucket, args.prefix, session, ein_set)
        if not parts:
            print(f"  [{year}] No parts to upload, skipping.")
            continue
        skip_existing = not args.no_skip_existing
        for i, zip_name in enumerate(parts, 1):
            print(f"  [{year}] ZIP {i}/{len(parts)}: {zip_name}", flush=True)
            if upload_zip_to_s3(year, zip_name, args.bucket, args.prefix, session, skip_existing=skip_existing):
                total_ok += 1
            else:
                total_skip += 1
    print(f"Done: {total_ok} uploaded, {total_skip} skipped.")


if __name__ == "__main__":
    main()
