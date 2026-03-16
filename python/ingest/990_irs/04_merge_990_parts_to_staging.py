"""
Merge silver-layer Parquet parts (per-ZIP) into a single staging table.

Filters by GEOID/county only: keeps only rows whose org ZIP maps to a county in
GEOID_reference.csv (via zip_to_county_fips.csv). Legacy: --benchmark-zips uses a
precomputed ZIP list when GEOID reference or zip_to_county are missing.

Reads all Parquet files under --parts-dir (e.g. year=2021/2021_TEOS_XML_01A.parquet),
applies GEOID/county or benchmark-ZIP filter, adds region column, writes one staging Parquet.

Use after 03_parse_irs_990_zips_to_staging.py --output-parts-dir DIR to build the
single staging table from silver without re-parsing XML.

Run from repo root:
  python python/ingest/990_irs/04_merge_990_parts_to_staging.py --parts-dir 01_data/staging/filing/parts
"""

import sys
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

import pandas as pd
from tqdm import tqdm

_SCRIPT_DIR = Path(__file__).resolve()
_PYTHON_DIR = _SCRIPT_DIR.parents[2]
if str(_PYTHON_DIR) not in sys.path:
    sys.path.insert(0, str(_PYTHON_DIR))
from utils.paths import DATA

DEFAULT_PARTS_DIR = DATA / "staging" / "filing" / "parts"
DEFAULT_OUTPUT = DATA / "staging" / "filing" / "irs_990_filings.parquet"
GEOID_REFERENCE_CSV = DATA / "reference" / "GEOID_reference.csv"
ZIP_TO_COUNTY_CSV = DATA / "reference" / "zip_to_county_fips.csv"
BENCHMARK_ZIPS_CSV = DATA / "reference" / "zip_codes_in_benchmark_regions.csv"


def _normalize_zip(zip_val: str | None) -> str:
    """Return 5-digit ZIP string; empty or invalid -> empty string."""
    if zip_val is None or pd.isna(zip_val):
        return ""
    s = str(zip_val).strip().replace(" ", "").replace("-", "")
    digits = "".join(c for c in s if c.isdigit())
    if len(digits) < 5:
        return ""
    return digits[:5]


def load_benchmark_zip_set(csv_path: Path) -> tuple[set[str], dict[str, str] | None]:
    """Load benchmark ZIP list (from location_processing/03). Returns (zip set, zip->region or None)."""
    if not csv_path.exists():
        return (set(), None)
    df = pd.read_csv(csv_path)
    zip_col = next((c for c in df.columns if "zip" in c.lower()), None)
    if not zip_col:
        return (set(), None)
    zip_norm = df[zip_col].astype(str).apply(_normalize_zip)
    valid = zip_norm.str.len() == 5
    zip_set = set(zip_norm[valid].unique())
    region_col = next(
        (c for c in df.columns if str(c).lower() in ("region", "cluster")), None
    )
    zip_to_region = None
    if region_col:
        zip_to_region = dict(
            zip(zip_norm[valid], df.loc[valid, region_col].astype(str).str.strip())
        )
    return (zip_set, zip_to_region)


def _find_region_column_geoid(ref: pd.DataFrame) -> str | None:
    """Return column name for region/cluster in GEOID reference, or None."""
    for c in ref.columns:
        if c and str(c).lower() in ("region", "cluster_name", "cluster", "benchmark_region", "area"):
            return c
    if "Region" in ref.columns:
        return "Region"
    return None


def load_geoid_reference_set(csv_path: Path) -> tuple[set[str], dict[str, str] | None]:
    """Load GEOID reference. Returns (set of GEOIDs, geoid->region dict or None)."""
    if not csv_path.exists():
        return (set(), None)
    df = pd.read_csv(csv_path)
    geoid_col = next((c for c in df.columns if "geoid" in c.lower()), None)
    if not geoid_col:
        return (set(), None)
    geoid_norm = df[geoid_col].astype(str).str.strip().str.zfill(5)
    valid = geoid_norm.str.len() == 5
    geoid_set = set(geoid_norm[valid].unique())
    region_col = _find_region_column_geoid(df)
    geoid_to_region = None
    if region_col:
        geoid_to_region = dict(
            zip(geoid_norm[valid], df.loc[valid, region_col].astype(str).str.strip())
        )
    return (geoid_set, geoid_to_region)


def load_zip_to_geoid(csv_path: Path) -> dict[str, str]:
    """Load zip_to_county_fips; return dict mapping normalized 5-digit ZIP to 5-digit GEOID."""
    if not csv_path.exists():
        return {}
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


def main() -> None:
    import argparse

    parser = argparse.ArgumentParser(
        description="Merge silver (per-ZIP) Parquet parts into one staging table"
    )
    parser.add_argument(
        "--parts-dir",
        type=Path,
        default=DEFAULT_PARTS_DIR,
        help="Directory containing year=YYYY/*.parquet (default: DATA/staging/filing/parts)",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=DEFAULT_OUTPUT,
        help="Output Parquet path (default: DATA/staging/filing/irs_990_filings.parquet)",
    )
    parser.add_argument(
        "--geoid-reference",
        type=Path,
        default=GEOID_REFERENCE_CSV,
        help="GEOID reference CSV (benchmark counties). With --zip-to-county, keep rows whose org ZIP maps to a county in this list. Default: DATA/reference/GEOID_reference.csv",
    )
    parser.add_argument(
        "--zip-to-county",
        type=Path,
        default=ZIP_TO_COUNTY_CSV,
        help="ZIP to county FIPS crosswalk. With --geoid-reference, map org ZIP to GEOID and filter. Default: DATA/reference/zip_to_county_fips.csv",
    )
    parser.add_argument(
        "--benchmark-zips",
        type=Path,
        default=BENCHMARK_ZIPS_CSV,
        help="Legacy: CSV with ZIP (and optional Region); keep only rows whose org ZIP is in this list. Ignored if --geoid-reference and --zip-to-county exist.",
    )
    parser.add_argument(
        "--workers",
        type=int,
        default=4,
        help="Parallel workers for reading part files (default: 4)",
    )
    args = parser.parse_args()

    parts_dir = args.parts_dir
    if not parts_dir.is_dir():
        print(f"Parts dir not found: {parts_dir}", file=sys.stderr)
        sys.exit(1)

    # Collect Parquet (and CSV fallback) under parts_dir
    files = sorted(parts_dir.glob("**/*.parquet")) or sorted(parts_dir.glob("**/*.csv"))
    if not files:
        print(f"No .parquet or .csv files under {parts_dir}", file=sys.stderr)
        sys.exit(1)

    def _read_one(path: Path) -> pd.DataFrame | None:
        try:
            if path.suffix.lower() == ".parquet":
                return pd.read_parquet(path)
            return pd.read_csv(path)
        except Exception as e:
            print(f"  Skip {path.name}: {e}", file=sys.stderr)
            return None

    workers = max(1, getattr(args, "workers", 4))
    print(f"Reading {len(files)} part file(s) from {parts_dir} (workers={workers})...", flush=True)
    dfs = []
    with ThreadPoolExecutor(max_workers=workers) as ex:
        future_to_idx = {ex.submit(_read_one, f): i for i, f in enumerate(files)}
        results = [None] * len(files)
        for future in tqdm(as_completed(future_to_idx), total=len(files), desc="Reading parts", unit="file"):
            idx = future_to_idx[future]
            results[idx] = future.result()
    dfs = [r for r in results if r is not None]
    if not dfs:
        print("No parts could be read.", file=sys.stderr)
        sys.exit(1)

    df = pd.concat(dfs, ignore_index=True, copy=False)
    print(f"Combined {len(df):,} rows.", flush=True)

    geoid_reference_set, geoid_to_region = load_geoid_reference_set(args.geoid_reference)
    zip_to_geoid = load_zip_to_geoid(args.zip_to_county)
    benchmark_zip_set, benchmark_zip_to_region = load_benchmark_zip_set(args.benchmark_zips)

    if geoid_reference_set and zip_to_geoid and "zip" in df.columns:
        zip_norm = df["zip"].astype(str).apply(_normalize_zip)
        df_geoid = zip_norm.map(zip_to_geoid)
        before = len(df)
        df = df[df_geoid.isin(geoid_reference_set)].copy()
        df_geoid = df_geoid.loc[df.index]
        print(f"After GEOID/county filter: {len(df):,} rows (from {before:,}).", flush=True)
        if geoid_to_region:
            df["region"] = df_geoid.map(geoid_to_region)
            print("Region column populated from GEOID reference.", flush=True)
        else:
            df["region"] = None
    elif benchmark_zip_set and "zip" in df.columns:
        zip_norm = df["zip"].astype(str).apply(_normalize_zip)
        before = len(df)
        df = df[zip_norm.isin(benchmark_zip_set)].copy()
        zip_norm = zip_norm.loc[df.index]
        print(f"After benchmark-ZIP filter: {len(df):,} rows (from {before:,}).", flush=True)
        if benchmark_zip_to_region:
            df["region"] = zip_norm.map(benchmark_zip_to_region)
            print("Region column populated from benchmark ZIP list.", flush=True)
        else:
            df["region"] = None
    else:
        if "region" not in df.columns:
            df["region"] = None

    args.output.parent.mkdir(parents=True, exist_ok=True)
    try:
        df.to_parquet(args.output, index=False)
        print(f"Wrote {len(df):,} rows to {args.output}", flush=True)
    except Exception:
        out_csv = args.output.with_suffix(".csv")
        df.to_csv(out_csv, index=False)
        print(f"Parquet unavailable; wrote {len(df):,} rows to {out_csv}", flush=True)


if __name__ == "__main__":
    main()
