"""
Download IRS Exempt Organizations Business Master File (EO BMF) by state
and save CSVs under 01_data/raw/irs_bmf/. Optionally upload to S3 (--upload).

Reads: none (downloads from IRS).
Writes: 01_data/raw/irs_bmf/eo_{state}.csv (e.g. eo_sd.csv, eo_mn.csv).
With --upload: also uploads to s3://{bucket}/{prefix}/eo_{state}.csv (same bucket as TEOS).

Source: https://www.irs.gov/charities-non-profits/exempt-organizations-business-master-file-extract-eo-bmf

Run from repo root:
  python python/ingest/990_irs/00_fetch_bmf.py
  python python/ingest/990_irs/00_fetch_bmf.py --states sd mn mt az
  python python/ingest/990_irs/00_fetch_bmf.py --upload
"""

import os
import sys
from pathlib import Path

import requests

# Path setup: ensure python/ is on path for utils.paths
_SCRIPT_DIR = Path(__file__).resolve()
_PYTHON_DIR = _SCRIPT_DIR.parents[2]
_REPO_ROOT = _SCRIPT_DIR.parents[3]
if str(_PYTHON_DIR) not in sys.path:
    sys.path.insert(0, str(_PYTHON_DIR))
from utils.paths import DATA

# Load secrets/.env so AWS credentials and region are set for S3 uploads (same as 01_upload_irs_990_index_to_s3).
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

# Output directory and IRS EO BMF source (one CSV per state, e.g. eo_sd.csv)
BMF_DIR = DATA / "raw" / "irs_bmf"
IRS_BMF_BASE = "https://www.irs.gov/pub/irs-soi"
DEFAULT_STATES = ["sd", "mn", "mt", "az"]

# S3: same bucket as TEOS scripts; BMF under its own prefix
S3_BUCKET = os.environ.get("IRS_990_S3_BUCKET", "swb-321-irs990-teos")
S3_BMF_PREFIX = os.environ.get("IRS_990_S3_BMF_PREFIX", "bronze/irs990/bmf")


def _download_state(state: str) -> bytes:
    """Download one state BMF CSV from IRS. State = lowercase 2-letter (e.g. sd)."""
    url = f"{IRS_BMF_BASE}/eo_{state}.csv"
    r = requests.get(url, timeout=120)
    r.raise_for_status()
    return r.content


def _upload_to_s3(content: bytes, s3_key: str, bucket: str, region: str) -> None:
    """Upload bytes to S3. Key is full key path (e.g. bronze/irs990/bmf/eo_sd.csv)."""
    import boto3
    s3 = boto3.Session(region_name=region).client("s3")
    s3.put_object(Bucket=bucket, Key=s3_key, Body=content, ContentType="text/csv")


def main() -> None:
    import argparse

    # Parse CLI: which states to download; whether to upload to S3
    parser = argparse.ArgumentParser(
        description="Download IRS EO BMF CSVs by state to 01_data/raw/irs_bmf/; optionally upload to S3"
    )
    parser.add_argument(
        "--states",
        nargs="+",
        default=DEFAULT_STATES,
        metavar="ST",
        help=f"State codes (lowercase, e.g. sd mn mt az). Default: {' '.join(DEFAULT_STATES)}",
    )
    parser.add_argument(
        "--upload",
        action="store_true",
        help="Upload each BMF CSV to S3 (same bucket as TEOS; prefix bronze/irs990/bmf)",
    )
    parser.add_argument(
        "--bucket",
        default=S3_BUCKET,
        help=f"S3 bucket (default: {S3_BUCKET}; or env IRS_990_S3_BUCKET)",
    )
    parser.add_argument(
        "--prefix",
        default=S3_BMF_PREFIX,
        help=f"S3 key prefix for BMF (default: {S3_BMF_PREFIX}; or env IRS_990_S3_BMF_PREFIX)",
    )
    parser.add_argument(
        "--region",
        default=os.environ.get("AWS_DEFAULT_REGION", "us-east-2"),
        help="AWS region for S3",
    )
    args = parser.parse_args()

    states = [s.lower().strip()[:2] for s in args.states]
    print(f"[00_fetch_bmf] Starting: download IRS EO BMF for {len(states)} state(s): {', '.join(states)}.")
    print(f"[00_fetch_bmf] Output directory: {BMF_DIR}")
    if args.upload:
        print(f"[00_fetch_bmf] S3 upload enabled: s3://{args.bucket}/{args.prefix}/")

    # Ensure output directory exists
    BMF_DIR.mkdir(parents=True, exist_ok=True)
    print("[00_fetch_bmf] Output directory ready.")

    # Download each state CSV from IRS, write to disk, optionally upload to S3
    ok = 0
    upload_ok = 0
    for state in states:
        path = BMF_DIR / f"eo_{state}.csv"
        try:
            content = _download_state(state)
            path.write_bytes(content)
            print(f"[00_fetch_bmf] Wrote {path.name}: {len(content):,} bytes")
            ok += 1

            if args.upload:
                s3_key = f"{args.prefix.rstrip('/')}/eo_{state}.csv"
                try:
                    _upload_to_s3(content, s3_key, args.bucket, args.region)
                    print(f"[00_fetch_bmf] Uploaded to s3://{args.bucket}/{s3_key}")
                    upload_ok += 1
                except Exception as e:
                    print(f"[00_fetch_bmf] WARNING: S3 upload failed for {path.name}: {e}", file=sys.stderr)

        except requests.RequestException as e:
            print(f"[00_fetch_bmf] WARNING: {path.name} failed: {e}", file=sys.stderr)

    print(f"[00_fetch_bmf] Done. {ok}/{len(states)} state file(s) written to {BMF_DIR}")
    if args.upload:
        print(f"[00_fetch_bmf] S3: {upload_ok}/{len(states)} file(s) uploaded to s3://{args.bucket}/{args.prefix}")


if __name__ == "__main__":
    main()
