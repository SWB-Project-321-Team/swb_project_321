"""
Filter BMF data to benchmark counties (GEOID reference) and write to silver layer.

Reads raw BMF CSVs from 01_data/raw/irs_bmf/ (from 00_fetch_bmf.py), GEOID_reference.csv,
and zip_to_county_fips.csv. Keeps only rows whose org ZIP maps to a county in the GEOID
reference. Writes one combined file to local staging and uploads to S3 silver.

Local output: 01_data/staging/org/irs_bmf_benchmark_counties.parquet (or .csv)
S3 silver:    s3://{bucket}/silver/irs990/bmf/bmf_benchmark_counties.parquet

Requires: 00_fetch_bmf.py (raw BMF), location_processing 01 + 02 (GEOID reference, zip_to_county).
Uses secrets/.env for AWS when uploading.

Run from repo root:
  python python/ingest/990_irs/00b_filter_bmf_to_benchmark_upload_silver.py
  python python/ingest/990_irs/00b_filter_bmf_to_benchmark_upload_silver.py --no-upload
  python python/ingest/990_irs/00b_filter_bmf_to_benchmark_upload_silver.py --geoid-reference path/to/GEOID_reference.csv
"""

import os
import sys
from pathlib import Path

import pandas as pd

# -----------------------------------------------------------------------------
# Path setup: ensure python/ is on path so utils.paths.DATA is available
# -----------------------------------------------------------------------------
_SCRIPT_DIR = Path(__file__).resolve()
_PYTHON_DIR = _SCRIPT_DIR.parents[2]
_REPO_ROOT = _SCRIPT_DIR.parents[3]
if str(_PYTHON_DIR) not in sys.path:
    sys.path.insert(0, str(_PYTHON_DIR))
from utils.paths import DATA

# -----------------------------------------------------------------------------
# Load secrets/.env so AWS credentials (and region) are set for S3 upload
# -----------------------------------------------------------------------------
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

# -----------------------------------------------------------------------------
# Default paths: raw BMF (input), GEOID reference + zip_to_county (filter), staging output, S3 silver
# -----------------------------------------------------------------------------
BMF_RAW_DIR = DATA / "raw" / "irs_bmf"
GEOID_REFERENCE_CSV = DATA / "reference" / "GEOID_reference.csv"
GEOID_REFERENCE_XLSX = DATA / "reference" / "GEOID_reference.xlsx"
ZIP_TO_COUNTY_CSV = DATA / "reference" / "zip_to_county_fips.csv"
STAGING_ORG_DIR = DATA / "staging" / "org"
DEFAULT_OUTPUT_PARQUET = STAGING_ORG_DIR / "irs_bmf_benchmark_counties.parquet"
S3_BUCKET = os.environ.get("IRS_990_S3_BUCKET", "swb-321-irs990-teos")
S3_SILVER_PREFIX = "silver/irs990/bmf"
S3_SILVER_KEY = f"{S3_SILVER_PREFIX}/bmf_benchmark_counties.parquet"


def _normalize_zip(zip_val: str) -> str:
    """Return 5-digit ZIP string; empty or invalid -> empty string. Used to match BMF ZIPs to the crosswalk."""
    if zip_val is None or (isinstance(zip_val, float) and pd.isna(zip_val)):
        return ""
    s = str(zip_val).strip().replace(" ", "").replace("-", "")
    digits = "".join(c for c in s if c.isdigit())
    return digits[:5] if len(digits) >= 5 else ""


def _find_region_column(ref: pd.DataFrame) -> str | None:
    """Return the column name in GEOID reference that holds region/cluster (e.g. Cluster_name). Used to attach Region to BMF rows."""
    for c in ref.columns:
        if c and str(c).lower() in ("region", "cluster_name", "cluster", "benchmark_region", "area"):
            return c
    if "Region" in ref.columns:
        return "Region"
    return None


def load_geoid_reference(csv_path: Path) -> pd.DataFrame:
    """
    Load the GEOID reference (18 benchmark counties). Prefer CSV from location_processing 01;
    fall back to XLSX if CSV is missing. Normalizes GEOID to 5-digit zero-padded string.
    """
    if csv_path.exists():
        ref = pd.read_csv(csv_path)
        print(f"[00b_filter_bmf]     Source: {csv_path.name} (CSV)")
    elif GEOID_REFERENCE_XLSX.exists():
        ref = pd.read_excel(GEOID_REFERENCE_XLSX)
        print(f"[00b_filter_bmf]     Source: {GEOID_REFERENCE_XLSX.name} (XLSX fallback)")
    else:
        raise FileNotFoundError(
            f"GEOID reference not found at {csv_path} or {GEOID_REFERENCE_XLSX}. "
            "Run location_processing/01_fetch_geoid_reference.py first."
        )
    ref["GEOID"] = ref["GEOID"].astype(str).str.strip().str.zfill(5)
    return ref


def load_zip_to_geoid(csv_path: Path) -> dict[str, str]:
    """
    Load the ZIP-to-county crosswalk (from location_processing 02). Returns a dict mapping
    normalized 5-digit ZIP -> 5-digit county FIPS (GEOID). One ZIP may map to one primary county.
    """
    if not csv_path.exists():
        raise FileNotFoundError(
            f"ZIP-county file not found: {csv_path}. "
            "Run location_processing/02_fetch_zip_to_county.py first."
        )
    df = pd.read_csv(csv_path)
    zip_col = next((c for c in df.columns if "zip" in c.lower()), df.columns[0])
    fips_candidates = [
        c for c in df.columns
        if "fips" in c.lower() or ("county" in c.lower() and "name" not in c.lower())
    ]
    fips_col = fips_candidates[0] if fips_candidates else df.columns[1]
    df["_zip"] = df[zip_col].astype(str).str.replace(r"\D", "", regex=True).str[:5]
    df["_geoid"] = df[fips_col].astype(str).str.strip().str.zfill(5)
    df = df[["_zip", "_geoid"]].drop_duplicates(subset=["_zip"], keep="first").dropna(subset=["_geoid"])
    valid = (df["_zip"].str.len() == 5) & (df["_geoid"].str.len() == 5)
    return dict(zip(df.loc[valid, "_zip"], df.loc[valid, "_geoid"]))


def load_raw_bmf(bmf_dir: Path) -> pd.DataFrame:
    """
    Load all eo_*.csv files from the raw BMF directory (output of 00_fetch_bmf.py).
    Normalizes the ZIP column name to 'ZIP' and concatenates into one DataFrame.
    """
    if not bmf_dir.is_dir():
        raise FileNotFoundError(f"BMF directory not found: {bmf_dir}. Run 00_fetch_bmf.py first.")
    files = sorted(bmf_dir.glob("eo_*.csv"))
    if not files:
        raise FileNotFoundError(f"No eo_*.csv files in {bmf_dir}. Run 00_fetch_bmf.py first.")
    frames = []
    for path in files:
        df = pd.read_csv(path, dtype=str, low_memory=False)
        # Ensure we have a ZIP column (BMF may use different header names)
        zip_col = next((c for c in df.columns if c and "zip" in str(c).lower()), None)
        if zip_col is None:
            zip_col = df.columns[0]
        if zip_col != "ZIP":
            df = df.rename(columns={zip_col: "ZIP"})
        frames.append(df)
        print(f"[00b_filter_bmf]     Loaded {path.name}: {len(df):,} rows.")
    return pd.concat(frames, ignore_index=True)


def filter_bmf_by_geoid(
    bmf: pd.DataFrame,
    geoid_set: set[str],
    zip_to_geoid: dict[str, str],
    geoid_to_region: dict[str, str] | None,
) -> pd.DataFrame:
    """
    Keep only BMF rows whose org address ZIP maps to a county (GEOID) in geoid_set.
    Uses zip_to_geoid to map each row's ZIP to county; drops rows that don't map or map outside benchmark.
    Optionally adds a Region column from the GEOID reference (e.g. SiouxFalls, BlackHills).
    """
    zip_col = "ZIP" if "ZIP" in bmf.columns else bmf.columns[0]
    bmf = bmf.copy()
    bmf["_zip_norm"] = bmf[zip_col].astype(str).apply(_normalize_zip)
    bmf["_geoid"] = bmf["_zip_norm"].map(zip_to_geoid)
    out = bmf[bmf["_geoid"].isin(geoid_set)].copy()
    if geoid_to_region:
        out["Region"] = out["_geoid"].map(geoid_to_region)
    out = out.drop(columns=["_zip_norm", "_geoid"])
    return out


def upload_to_s3(local_path: Path, bucket: str, key: str, region: str) -> None:
    """Upload the local file (Parquet or CSV) to S3 at the given bucket and key."""
    import boto3
    s3 = boto3.Session(region_name=region).client("s3")
    body = local_path.read_bytes()
    s3.put_object(Bucket=bucket, Key=key, Body=body, ContentType="application/octet-stream")


def main() -> None:
    import argparse

    parser = argparse.ArgumentParser(
        description="Filter BMF to benchmark counties (GEOID reference) and write to silver (local + S3)"
    )
    parser.add_argument(
        "--bmf-dir",
        type=Path,
        default=BMF_RAW_DIR,
        help=f"Raw BMF directory (default: {BMF_RAW_DIR})",
    )
    parser.add_argument(
        "--geoid-reference",
        type=Path,
        default=GEOID_REFERENCE_CSV,
        help=f"GEOID reference CSV (default: {GEOID_REFERENCE_CSV})",
    )
    parser.add_argument(
        "--zip-to-county",
        type=Path,
        default=ZIP_TO_COUNTY_CSV,
        help=f"ZIP to county FIPS CSV (default: {ZIP_TO_COUNTY_CSV})",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=DEFAULT_OUTPUT_PARQUET,
        help=f"Local output path (default: {DEFAULT_OUTPUT_PARQUET})",
    )
    parser.add_argument(
        "--no-upload",
        action="store_true",
        help="Skip S3 upload; only write local staging file",
    )
    parser.add_argument(
        "--bucket",
        default=S3_BUCKET,
        help=f"S3 bucket (default: {S3_BUCKET})",
    )
    parser.add_argument(
        "--prefix",
        default=S3_SILVER_PREFIX,
        help=f"S3 key prefix for silver BMF (default: {S3_SILVER_PREFIX})",
    )
    parser.add_argument(
        "--region",
        default=os.environ.get("AWS_DEFAULT_REGION", "us-east-2"),
        help="AWS region for S3",
    )
    args = parser.parse_args()

    print("[00b_filter_bmf] Starting: filter BMF to benchmark counties (GEOID reference) and write to silver.")
    print(f"[00b_filter_bmf] Input BMF dir: {args.bmf_dir}")
    print(f"[00b_filter_bmf] GEOID reference: {args.geoid_reference}")
    print(f"[00b_filter_bmf] ZIP-to-county crosswalk: {args.zip_to_county}")
    print(f"[00b_filter_bmf] Local output: {args.output}")
    if not args.no_upload:
        print(f"[00b_filter_bmf] S3 destination: s3://{args.bucket}/{args.prefix}/")
    print()

    # Step 1: Load GEOID reference (list of benchmark county GEOIDs we want to keep)
    print("[00b_filter_bmf] Step 1: Loading GEOID reference (benchmark counties)...")
    ref = load_geoid_reference(args.geoid_reference)
    geoid_set = set(ref["GEOID"].astype(str))
    print(f"[00b_filter_bmf]   Loaded {len(geoid_set)} benchmark GEOIDs (counties) from {args.geoid_reference.name}.")

    # Step 2: Load ZIP → county (GEOID) crosswalk so we can map each BMF org's ZIP to a county
    print("[00b_filter_bmf] Step 2: Loading ZIP-to-county crosswalk...")
    zip_to_geoid = load_zip_to_geoid(args.zip_to_county)
    print(f"[00b_filter_bmf]   Crosswalk has {len(zip_to_geoid):,} ZIPs mapped to county FIPS (GEOID).")

    # Step 3: Build GEOID → Region mapping from reference (for optional Region column in output)
    print("[00b_filter_bmf] Step 3: Checking for Region/cluster column in GEOID reference...")
    region_col = _find_region_column(ref)
    geoid_to_region = None
    if region_col:
        geoid_to_region = dict(zip(ref["GEOID"].astype(str), ref[region_col].astype(str).str.strip()))
        print(f"[00b_filter_bmf]   Found column '{region_col}'; will add Region to output.")
    else:
        print("[00b_filter_bmf]   No region/cluster column found; output will not include Region.")
    print()

    # Step 4: Load raw BMF (all eo_*.csv files from raw directory)
    print("[00b_filter_bmf] Step 4: Loading raw BMF from disk...")
    bmf = load_raw_bmf(args.bmf_dir)
    n_raw = len(bmf)
    n_files = len(list(args.bmf_dir.glob("eo_*.csv")))
    print(f"[00b_filter_bmf]   Read {n_files} file(s); combined {n_raw:,} rows.")

    # Step 5: Filter to rows whose org ZIP maps to a benchmark county (GEOID in reference)
    print("[00b_filter_bmf] Step 5: Filtering BMF to rows in benchmark counties (ZIP → GEOID in reference)...")
    filtered = filter_bmf_by_geoid(bmf, geoid_set, zip_to_geoid, geoid_to_region)
    n_filtered = len(filtered)
    print(f"[00b_filter_bmf]   Before filter: {n_raw:,} rows. After filter: {n_filtered:,} rows.")
    if n_raw > 0:
        pct = 100.0 * n_filtered / n_raw
        print(f"[00b_filter_bmf]   Retained {pct:.1f}% of rows.")

    # Step 6: Write filtered BMF to local staging (silver layer on disk)
    print("[00b_filter_bmf] Step 6: Writing filtered BMF to local staging...")
    args.output.parent.mkdir(parents=True, exist_ok=True)
    try:
        filtered.to_parquet(args.output, index=False)
        print(f"[00b_filter_bmf]   Wrote Parquet: {args.output} ({n_filtered:,} rows).")
    except Exception:
        out_csv = args.output.with_suffix(".csv")
        filtered.to_csv(out_csv, index=False)
        args.output = out_csv
        print(f"[00b_filter_bmf]   Parquet unavailable; wrote CSV: {args.output} ({n_filtered:,} rows).")
    print()

    # Step 7: Upload to S3 silver layer (unless --no-upload)
    if not args.no_upload:
        print("[00b_filter_bmf] Step 7: Uploading to S3 silver layer...")
        s3_key = f"{args.prefix.rstrip('/')}/bmf_benchmark_counties.parquet"
        if args.output.suffix.lower() == ".csv":
            s3_key = s3_key.replace(".parquet", ".csv")
        try:
            upload_to_s3(args.output, args.bucket, s3_key, args.region)
            print(f"[00b_filter_bmf]   Uploaded to s3://{args.bucket}/{s3_key}")
        except Exception as e:
            print(f"[00b_filter_bmf] WARNING: S3 upload failed: {e}", file=sys.stderr)
    else:
        print("[00b_filter_bmf] Step 7: S3 upload skipped (--no-upload).")

    print()
    print("[00b_filter_bmf] Done.")


if __name__ == "__main__":
    main()
