"""
List all files and folders in the IRS 990 S3 bucket (under the configured prefix).

Shows folder tree with object counts and sizes. Run from repo root:
  python tests/990_irs/list_s3_bucket.py
  python tests/990_irs/list_s3_bucket.py --recursive   # flat list of every key
"""
import os
import sys
from pathlib import Path
from collections import defaultdict

import boto3

_FILE_DIR = Path(__file__).resolve().parent
_REPO_ROOT = _FILE_DIR.parent.parent
_PYTHON = _REPO_ROOT / "python"
if str(_PYTHON) not in sys.path:
    sys.path.insert(0, str(_PYTHON))

BUCKET = os.environ.get("IRS_990_S3_BUCKET", "swb-321-irs990-teos")
PREFIX = os.environ.get("IRS_990_S3_PREFIX", "bronze/irs990/teos_xml").rstrip("/")
REGION = os.environ.get("AWS_DEFAULT_REGION", "us-east-2")


def _size_fmt(n: int) -> str:
    if n is None or n < 0:
        return "?"
    if n < 1024:
        return f"{n} B"
    if n < 1024 * 1024:
        return f"{n / 1024:.1f} KB"
    if n < 1024 * 1024 * 1024:
        return f"{n / (1024 * 1024):.1f} MB"
    return f"{n / (1024 * 1024 * 1024):.1f} GB"


def main():
    import argparse
    parser = argparse.ArgumentParser(description="List S3 bucket structure (files and folders)")
    parser.add_argument("--bucket", default=BUCKET, help="S3 bucket")
    parser.add_argument("--prefix", default=PREFIX, help="Key prefix")
    parser.add_argument("--recursive", action="store_true", help="Print every object key (flat)")
    args = parser.parse_args()

    session = boto3.Session(region_name=REGION)
    s3 = session.client("s3")
    prefix = args.prefix.strip("/")
    if prefix:
        prefix += "/"

    print(f"Bucket: s3://{args.bucket}/")
    print(f"Prefix: {prefix or '(root)'}\n")

    paginator = s3.get_paginator("list_objects_v2")
    all_keys = []
    for page in paginator.paginate(Bucket=args.bucket, Prefix=prefix):
        for obj in page.get("Contents") or []:
            key = obj.get("Key", "")
            size = obj.get("Size") or obj.get("ContentLength") or 0
            all_keys.append((key, size))

    if not all_keys:
        print("No objects found.")
        return

    def rel(key):
        return key[len(prefix):].lstrip("/") if prefix else key.lstrip("/")

    if args.recursive:
        for k, sz in sorted(all_keys):
            print(rel(k) or k, _size_fmt(sz))
        print(f"\nTotal: {len(all_keys)} objects, {_size_fmt(sum(s for _, s in all_keys))}")
        return

    # Build tree: for each path segment level, sum count and size under that path
    # path_str -> (count, size) for all keys under that path (path is "segment1/segment2/")
    by_path = defaultdict(lambda: [0, 0])  # path -> [count, size]
    for k, sz in all_keys:
        r = rel(k)
        parts = r.split("/") if r else []
        for i in range(1, len(parts) + 1):
            p = "/".join(parts[:i])
            by_path[p][0] += 1 if i == len(parts) else 0
            by_path[p][1] += sz

    # Top-level segments
    top_segments = sorted(set(r.split("/")[0] for r in (rel(k) for k, _ in all_keys) if r))
    for seg in top_segments:
        sub = [(k, s) for k, s in all_keys if rel(k) == seg or rel(k).startswith(seg + "/")]
        cnt, total = len(sub), sum(s for _, s in sub)
        print(f"{seg}/  ({cnt} objects, {_size_fmt(total)})")
        # Next level under seg
        next_level = set()
        for k, _ in all_keys:
            r = rel(k)
            if r.startswith(seg + "/"):
                rest = r[len(seg) + 1:]
                if rest:
                    next_level.add(rest.split("/")[0])
        for n in sorted(next_level):
            sub2 = [(k, s) for k, s in all_keys if rel(k) == seg + "/" + n or rel(k).startswith(seg + "/" + n + "/")]
            cnt2, total2 = len(sub2), sum(s for _, s in sub2)
            print(f"  {n}/  ({cnt2} objects, {_size_fmt(total2)})")
    print(f"\nTotal: {len(all_keys)} objects, {_size_fmt(sum(s for _, s in all_keys))}")


if __name__ == "__main__":
    main()
