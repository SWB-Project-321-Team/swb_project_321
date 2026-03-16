"""
Produce a 10-row sample CSV of parsed IRS 990 staging data for teammates.

Uses only GEOID-filtered rows (org address ZIP in the 18-county reference).
Finds the smallest bulk-part ZIP in S3, runs the 03 parser, loads the
filtered (main) output, samples 10 rows with diverse form_type (and year),
and writes docs/990/sample_parsed_irs_990_10.csv.

Requires: AWS credentials with access to the project S3 bucket (TEOS index and
ZIPs). Use AWS_PROFILE=neilken-admin (or set before running) to use
neilken-admin credentials.

Run from repo root:
  python scripts/sample_parsed_990_to_csv.py
  AWS_PROFILE=neilken-admin python scripts/sample_parsed_990_to_csv.py
"""
import argparse
import os
import re
import subprocess
import sys
import tempfile
from pathlib import Path

import boto3
import pandas as pd

REPO_ROOT = Path(__file__).resolve().parent.parent
PARSER_SCRIPT = REPO_ROOT / "python" / "ingest" / "990_irs" / "03_parse_irs_990_zips_to_staging.py"
DEFAULT_SAMPLE_SIZE = 10
PREFERRED_FORM_TYPES = ("990", "990EZ", "990PF", "990T")
PREFERRED_YEARS = (2021, 2022, 2023, 2024)
BUCKET = os.environ.get("IRS_990_S3_BUCKET", "swb-321-irs990-teos")
PREFIX = os.environ.get("IRS_990_S3_PREFIX", "bronze/irs990/teos_xml")
REGION = os.environ.get("AWS_DEFAULT_REGION", "us-east-2")
# Bulk-part ZIP filename pattern (e.g. 2025_TEOS_XML_01A.zip)
_BULK_PART_PATTERN = re.compile(r"^\d{4}_TEOS_XML_\d{2}[A-Z]?\.zip$", re.IGNORECASE)


def _load_env() -> None:
    """Load secrets/.env so AWS credentials are set (same as 03 parser)."""
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


def _find_smallest_zip(session: boto3.Session) -> tuple[int, str]:
    """List bulk-part ZIPs under prefix/zips/year=YYYY/, return (year, filename) for smallest by size."""
    s3 = session.client("s3")
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
    if smallest_key is None:
        raise SystemExit("No bulk-part ZIPs found in S3.")
    parts = smallest_key.split("/")
    filename = parts[-1]
    year_part = next((p for p in parts if p.startswith("year=")), "")
    year = int(year_part.replace("year=", "")) if year_part else start_year
    return year, filename


def _run_parser(temp_dir: Path, year: int, only_zip_key: str) -> tuple[Path, Path | None]:
    """Run 03 parser on the single ZIP. Returns (main/filtered path, full path or None)."""
    full_out = temp_dir / "irs_990_full.parquet"
    main_out = temp_dir / "irs_990_main.parquet"
    env = os.environ.copy()
    cmd = [
        sys.executable,
        str(PARSER_SCRIPT),
        "--start-year", str(year),
        "--end-year", str(year),
        "--only-zip-key", only_zip_key,
        "--output-full", str(full_out),
        "--output", str(main_out),
    ]
    result = subprocess.run(cmd, cwd=str(REPO_ROOT), env=env)
    if result.returncode != 0:
        sys.exit(result.returncode)
    main_path = main_out if main_out.exists() else main_out.with_suffix(".csv")
    if not main_path.exists():
        print("Parser did not produce GEOID-filtered output file.", file=sys.stderr)
        sys.exit(1)
    full_path = None
    for p in (full_out, full_out.with_suffix(".csv")):
        if p.exists():
            full_path = p
            break
    return main_path, full_path


def _load_parsed(path: Path) -> pd.DataFrame:
    """Load Parquet or CSV from path."""
    if path.suffix.lower() == ".parquet":
        return pd.read_parquet(path)
    return pd.read_csv(path)


def _sample_diverse(df: pd.DataFrame, n: int) -> pd.DataFrame:
    """Sample n rows with at least one per form type (990, 990EZ, 990PF, 990T), then diverse years."""
    if len(df) == 0:
        return df
    df = df.copy()
    form_col = "form_type" if "form_type" in df.columns else None
    year_col = "tax_year" if "tax_year" in df.columns else None
    if form_col is not None:
        df["form_type"] = df["form_type"].astype(str).str.strip().str.upper()
    sampled_indices = []
    if form_col:
        # Phase 1: at least one row per form type (when that type exists in the data)
        for ft in PREFERRED_FORM_TYPES:
            mask = df[form_col] == ft
            idx = df.index[mask]
            if len(idx) > 0:
                sampled_indices.append(idx[0])
        sampled_indices = list(dict.fromkeys(sampled_indices))  # preserve order, no duplicates
    if form_col and year_col and len(sampled_indices) < n:
        # Phase 2: add rows for (form_type, tax_year) diversity
        for ft in PREFERRED_FORM_TYPES:
            for yr in PREFERRED_YEARS:
                if len(sampled_indices) >= n:
                    break
                mask = (df[form_col] == ft) & (df[year_col] == yr)
                idx = df.index[mask]
                for i in idx:
                    if i not in sampled_indices:
                        sampled_indices.append(i)
                        break
                if len(sampled_indices) >= n:
                    break
            if len(sampled_indices) >= n:
                break
        if len(sampled_indices) < n:
            for (ft, yr), grp in df.groupby([form_col, year_col]):
                if len(sampled_indices) >= n:
                    break
                i = grp.index[0]
                if i not in sampled_indices:
                    sampled_indices.append(i)
    # Phase 3: fill to n with any remaining rows
    if len(sampled_indices) < n:
        remaining = df.index.difference(sampled_indices)
        take = min(n - len(sampled_indices), len(remaining))
        if take > 0:
            sampled_indices.extend(df.loc[remaining].head(take).index.tolist())
    if not sampled_indices:
        return df.head(n).reset_index(drop=True)
    return df.loc[sampled_indices[:n]].reset_index(drop=True)


def main() -> None:
    parser = argparse.ArgumentParser(description="Sample parsed IRS 990 (GEOID-filtered) to CSV.")
    parser.add_argument("-n", "--num", type=int, default=None, metavar="N", help=f"Number of rows to sample (default: {DEFAULT_SAMPLE_SIZE}; use with --all for no limit)")
    parser.add_argument("--all", action="store_true", help="No limit: write all GEOID-filtered rows (overrides -n).")
    args = parser.parse_args()
    use_all = args.all
    if use_all:
        n = None  # no cap; use full filtered set
        sample_csv = REPO_ROOT / "docs" / "990" / "sample_parsed_irs_990_all.csv"
    else:
        n = max(1, args.num if args.num is not None else DEFAULT_SAMPLE_SIZE)
        sample_csv = REPO_ROOT / "docs" / "990" / f"sample_parsed_irs_990_{n}.csv"

    _load_env()
    if not PARSER_SCRIPT.exists():
        print(f"Parser not found: {PARSER_SCRIPT}", file=sys.stderr)
        sys.exit(1)
    session = boto3.Session(region_name=REGION)
    print("Finding smallest bulk-part ZIP in S3...")
    year, only_zip_key = _find_smallest_zip(session)
    print(f"Using {only_zip_key} (year={year})")
    with tempfile.TemporaryDirectory(prefix="sample_990_") as tmp:
        temp_dir = Path(tmp)
        print("Running 03 parser on that ZIP...")
        main_path, full_path = _run_parser(temp_dir, year, only_zip_key)
        print(f"Loaded GEOID-filtered output from {main_path.name}")
        df_main = _load_parsed(main_path)
        df_main["in_geoid_region"] = True
        if use_all:
            n_use = len(df_main)
            print(f"Writing all {n_use} GEOID-filtered rows (--all).")
            sample = df_main.copy()
        elif len(df_main) < n:
            n_use = len(df_main)
            print(f"Only {len(df_main)} rows in filtered set; requested {n}. Using all {n_use}.", file=sys.stderr)
            sample = _sample_diverse(df_main, n_use)
        else:
            n_use = n
            sample = _sample_diverse(df_main, n_use)
        # Add 990-T / 990PF from full output if missing (form-type diversity; ntee_code when present)
        if full_path is not None:
            df_full = _load_parsed(full_path)
            df_full["in_geoid_region"] = False
            if "region" not in df_full.columns:
                df_full["region"] = ""
            in_sample = set(sample["form_type"].astype(str).str.strip().str.upper()) if "form_type" in sample.columns else set()
            for ft in ("990T", "990PF"):
                if ft not in in_sample:
                    subset = df_full.loc[df_full["form_type"].astype(str).str.strip().str.upper() == ft]
                    if len(subset) > 0:
                        extra = subset.head(1)
                        # Align columns with sample (full may have different order or missing region)
                        extra = extra.reindex(columns=sample.columns, copy=False)
                        sample = pd.concat([sample, extra], ignore_index=True)
                        print(f"Added 1 {ft} row from full set (not in GEOID region) for form-type diversity.", file=sys.stderr)
        sample_csv.parent.mkdir(parents=True, exist_ok=True)
        sample.to_csv(sample_csv, index=False)
        print(f"Wrote {len(sample)} rows to {sample_csv}")


if __name__ == "__main__":
    main()
