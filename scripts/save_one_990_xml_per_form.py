"""
Save one XML file of each 990 form type (990, 990EZ, 990PF, 990T) into the data folder.

Downloads the smallest bulk-part ZIP from S3, parses XMLs to detect form_type,
keeps the first of each type, and writes them to data/sample_990_xml_by_form/
as 990.xml, 990EZ.xml, 990PF.xml, 990T.xml.

Requires: AWS credentials with S3 access (e.g. secrets/.env or AWS_PROFILE).

Run from repo root:
  python scripts/save_one_990_xml_per_form.py
"""
import os
import re
import sys
import zipfile
from io import BytesIO
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
# One XML per form type saved under repo data folder (create if missing)
OUT_DIR = REPO_ROOT / "data" / "sample_990_xml_by_form"
PARSER_SCRIPT = REPO_ROOT / "python" / "ingest" / "990_irs" / "03_parse_irs_990_zips_to_staging.py"
BUCKET = os.environ.get("IRS_990_S3_BUCKET", "swb-321-irs990-teos")
PREFIX = os.environ.get("IRS_990_S3_PREFIX", "bronze/irs990/teos_xml")
REGION = os.environ.get("AWS_DEFAULT_REGION", "us-east-2")
_BULK_PART_PATTERN = re.compile(r"^\d{4}_TEOS_XML_\d{2}[A-Z]?\.zip$", re.IGNORECASE)
TARGET_FORM_TYPES = ("990", "990EZ", "990PF", "990T")


def _load_env() -> None:
    """Load secrets/.env for AWS credentials."""
    env_file = REPO_ROOT / "secrets" / ".env"
    if not env_file.exists():
        return
    with open(env_file, encoding="utf-8-sig") as f:
        for line in f:
            line = line.strip()
            if line and not line.startswith("#") and "=" in line:
                key, _, value = line.partition("=")
                key, value = key.strip(), value.strip()
                if value and value[0] in ("'", '"') and value[-1] == value[0]:
                    value = value[1:-1]
                if key:
                    os.environ.setdefault(key, value)


def _find_smallest_zip(s3) -> tuple[int, str, str]:
    """Return (year, zip_filename, full_s3_key) for smallest bulk-part ZIP."""
    start_year = 2021
    end_year = int(__import__("datetime").date.today().year)
    smallest_key = None
    smallest_size = None
    for year in range(start_year, end_year + 1):
        prefix_key = f"{PREFIX}/zips/year={year}/"
        paginator = s3.get_paginator("list_objects_v2")
        for page in paginator.paginate(Bucket=BUCKET, Prefix=prefix_key):
            for obj in page.get("Contents", []):
                k = obj.get("Key", "")
                name = k.split("/")[-1]
                if k.endswith(".zip") and _BULK_PART_PATTERN.match(name):
                    size = obj.get("Size", 0)
                    if smallest_size is None or size < smallest_size:
                        smallest_size = size
                        smallest_key = k
    if not smallest_key:
        raise SystemExit("No bulk-part ZIPs found in S3.")
    parts = smallest_key.split("/")
    filename = parts[-1]
    year_part = next((p for p in parts if p.startswith("year=")), "")
    year = int(year_part.replace("year=", "")) if year_part else start_year
    return year, filename, smallest_key


def main() -> None:
    _load_env()
    if str(REPO_ROOT / "python") not in sys.path:
        sys.path.insert(0, str(REPO_ROOT / "python"))
    import boto3

    # Import parser
    import importlib.util
    spec = importlib.util.spec_from_file_location(
        "parse_990_module", PARSER_SCRIPT
    )
    parse_module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(parse_module)
    parse_990_xml = parse_module.parse_990_xml

    session = boto3.Session(region_name=REGION)
    s3 = session.client("s3")
    print("Finding smallest bulk-part ZIP in S3...")
    year, zip_name, s3_key = _find_smallest_zip(s3)
    print(f"Downloading {zip_name} (year={year})...")
    obj = s3.get_object(Bucket=BUCKET, Key=s3_key)
    zip_bytes = obj["Body"].read()
    buf = BytesIO(zip_bytes)

    needed = set(TARGET_FORM_TYPES)
    saved = {}  # form_type -> (xml_bytes, source_name)

    with zipfile.ZipFile(buf, "r") as zf:
        names = [n for n in zf.namelist() if n.lower().endswith(".xml")]
        for name in names:
            if not needed:
                break
            try:
                xml_bytes = zf.read(name)
            except Exception as e:
                print(f"  Skip {name}: {e}", file=sys.stderr)
                continue
            row = parse_990_xml(
                xml_bytes,
                source_name=Path(name).name,
                use_regex_only=False,
            )
            if not row:
                continue
            ft = str(row.get("form_type") or "").strip().upper().replace("-", "")
            if ft in needed:
                saved[ft] = (xml_bytes, Path(name).name)
                needed.discard(ft)
                print(f"  Found {ft}: {Path(name).name}")

    OUT_DIR.mkdir(parents=True, exist_ok=True)
    for ft in TARGET_FORM_TYPES:
        if ft not in saved:
            print(f"  Warning: no {ft} found in this ZIP; skipping.", file=sys.stderr)
            continue
        xml_bytes, _ = saved[ft]
        out_path = OUT_DIR / f"{ft}.xml"
        out_path.write_bytes(xml_bytes)
        print(f"Wrote {out_path}")

    if len(saved) < len(TARGET_FORM_TYPES):
        sys.exit(1)
    print(f"Done. One XML per form type in {OUT_DIR}")


if __name__ == "__main__":
    main()
