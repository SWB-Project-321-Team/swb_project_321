"""
Inspect S3 ZIP objects for year=2021: list keys and check first bytes.
Valid ZIPs start with PK\x03\x04 (local file header). HTML/error pages start with <! or <?.
Run from repo root: python tests/990_irs/inspect_s3_zips.py
"""
import os
import sys
from pathlib import Path

import boto3

_FILE_DIR = Path(__file__).resolve().parent
_REPO_ROOT = _FILE_DIR.parent.parent
_PYTHON = _REPO_ROOT / "python"
if str(_PYTHON) not in sys.path:
    sys.path.insert(0, str(_PYTHON))
from utils.paths import DATA

BUCKET = os.environ.get("IRS_990_S3_BUCKET", "swb-321-irs990-teos")
PREFIX = os.environ.get("IRS_990_S3_PREFIX", "bronze/irs990/teos_xml")
REGION = os.environ.get("AWS_DEFAULT_REGION", "us-east-2")


def main():
    session = boto3.Session(region_name=REGION)
    s3 = session.client("s3")
    prefix_key = f"{PREFIX}/zips/year=2021/"
    print(f"Bucket: {BUCKET}")
    print(f"Prefix: {prefix_key}")
    print()

    paginator = s3.get_paginator("list_objects_v2")
    keys = []
    for page in paginator.paginate(Bucket=BUCKET, Prefix=prefix_key):
        for obj in page.get("Contents", []):
            k = obj.get("Key", "")
            if k.endswith(".zip"):
                keys.append(k)
    keys = sorted(keys)
    print(f"Found {len(keys)} .zip objects for 2021.\n")

    for i, key in enumerate(keys):
        name = key.split("/")[-1]
        try:
            obj = s3.get_object(Bucket=BUCKET, Key=key)
            body = obj["Body"].read(256)
            size = obj.get("ContentLength") or len(body)
        except Exception as e:
            print(f"  [{i+1}] {name}: ERROR reading - {e}")
            continue

        header = body[:4] if len(body) >= 4 else body
        if header[:2] == b"PK":
            sig = "ZIP (PK)"
        elif body.startswith(b"<!") or body.startswith(b"<?xml"):
            sig = "HTML/XML (likely error page)"
        elif body.strip() == b"":
            sig = "empty"
        else:
            sig = f"other (hex: {header.hex()})"

        content_type = obj.get("ContentType", "")
        print(f"  [{i+1}] {name}")
        print(f"       Size: {size:,} bytes  ContentType: {content_type}")
        print(f"       Header: {sig}")
        if header[:2] != b"PK":
            print(f"       First 80 chars: {body[:80]!r}")
        print()


if __name__ == "__main__":
    main()
