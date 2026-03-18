"""
Step 04: Upload raw DataMart files and dictionary metadata to Bronze.

Uploads:
- metadata/* -> bronze/givingtuesday_990/datamarts/metadata/
- raw/*      -> bronze/givingtuesday_990/datamarts/raw/
"""

from __future__ import annotations

import argparse
import mimetypes
import time
from pathlib import Path

from tqdm import tqdm

from common import (
    BRONZE_META_PREFIX,
    BRONZE_RAW_PREFIX,
    DEFAULT_S3_BUCKET,
    DEFAULT_S3_REGION,
    META_DIR,
    RAW_DIR,
    banner,
    ensure_dirs,
    load_env_from_secrets,
    print_elapsed,
    upload_file_with_progress,
)


def _content_type_for(path: Path) -> str:
    """Infer content type for S3 metadata."""
    ct, _ = mimetypes.guess_type(str(path))
    return ct or "application/octet-stream"


def _upload_folder(folder: Path, bucket: str, prefix: str, region: str) -> int:
    """Upload all files in a folder (non-recursive) to a prefix."""
    files = sorted([p for p in folder.glob("*") if p.is_file()])
    print(f"[upload] Folder {folder} -> s3://{bucket}/{prefix}/ ({len(files)} file(s))", flush=True)
    for path in tqdm(files, desc=f"Upload {folder.name}", unit="file"):
        key = f"{prefix.rstrip('/')}/{path.name}"
        ctype = _content_type_for(path)
        print(f"\n[upload] {path} -> s3://{bucket}/{key} ({ctype})", flush=True)
        upload_file_with_progress(path, bucket, key, region, extra_args={"ContentType": ctype})
    return len(files)


def main() -> None:
    parser = argparse.ArgumentParser(description="Upload local raw + metadata files to Bronze S3 prefixes.")
    parser.add_argument("--bucket", default=DEFAULT_S3_BUCKET, help="Target S3 bucket")
    parser.add_argument("--region", default=DEFAULT_S3_REGION, help="Target S3 region")
    parser.add_argument("--meta-dir", default=str(META_DIR), help="Local metadata directory")
    parser.add_argument("--raw-dir", default=str(RAW_DIR), help="Local raw files directory")
    parser.add_argument("--meta-prefix", default=BRONZE_META_PREFIX, help="Bronze metadata prefix")
    parser.add_argument("--raw-prefix", default=BRONZE_RAW_PREFIX, help="Bronze raw prefix")
    args = parser.parse_args()

    start = time.perf_counter()
    banner("STEP 04 - UPLOAD BRONZE RAW + DICTIONARY")
    load_env_from_secrets()
    ensure_dirs()

    meta_dir = Path(args.meta_dir)
    raw_dir = Path(args.raw_dir)
    if not meta_dir.exists():
        raise FileNotFoundError(f"Metadata directory not found: {meta_dir}")
    if not raw_dir.exists():
        raise FileNotFoundError(f"Raw directory not found: {raw_dir}")

    n_meta = _upload_folder(meta_dir, args.bucket, args.meta_prefix, args.region)
    n_raw = _upload_folder(raw_dir, args.bucket, args.raw_prefix, args.region)
    print(f"[upload] Completed Bronze upload: metadata={n_meta}, raw={n_raw}", flush=True)
    print_elapsed(start, "Step 04")


if __name__ == "__main__":
    main()

