"""
Step 09: Filter unified unfiltered dataset to benchmark regions and write local Silver parquet.

Filter logic:
- tax_year >= configured minimum
- FILERUSZIP (normalized ZIP5) maps to county FIPS in GEOID reference list
- Region label attached from GEOID reference

External pipeline inputs (only):
- 01_data/reference/GEOID_reference.csv (from location pipeline)
- 01_data/reference/zip_to_county_fips.csv (from location pipeline)
"""

from __future__ import annotations

import argparse
import json
import time
from pathlib import Path

import polars as pl
from tqdm import tqdm

from common import (
    BASIC_PLUS_COMBINED_PARQUET,
    FILTERED_MANIFEST_JSON,
    FILTERED_SILVER_PARQUET,
    GEOID_REFERENCE_CSV,
    ZIP_TO_COUNTY_CSV,
    banner,
    print_elapsed,
)


def _blank(expr: pl.Expr) -> pl.Expr:
    """True where value is blank-like (None/whitespace)."""
    return expr.is_null() | expr.cast(pl.Utf8, strict=False).str.strip_chars().eq("")


def _normalize_zip5_expr(expr: pl.Expr) -> pl.Expr:
    """Normalize ZIP to 5-digit code where possible, else blank."""
    digits = expr.cast(pl.Utf8, strict=False).fill_null("").str.replace_all(r"[^0-9]", "")
    return pl.when(digits.str.len_chars() >= 5).then(digits.str.slice(0, 5)).otherwise(pl.lit(""))


def _normalize_fips5_expr(expr: pl.Expr) -> pl.Expr:
    """Normalize FIPS/GEOID to 5-digit county code."""
    digits = expr.cast(pl.Utf8, strict=False).fill_null("").str.replace_all(r"[^0-9]", "")
    return (
        pl.when(digits.str.len_chars() == 0)
        .then(pl.lit(""))
        .when(digits.str.len_chars() < 5)
        .then(digits.str.zfill(5))
        .otherwise(digits.str.slice(0, 5))
    )


def _normalize_tax_year_expr(expr: pl.Expr) -> pl.Expr:
    """Normalize tax year to integer-like string when parseable."""
    raw = expr.cast(pl.Utf8, strict=False).fill_null("").str.strip_chars()
    as_float = raw.cast(pl.Float64, strict=False)
    return (
        pl.when(raw.eq(""))
        .then(pl.lit(""))
        .when(as_float.is_not_null())
        .then(as_float.cast(pl.Int64, strict=False).cast(pl.Utf8, strict=False))
        .otherwise(raw)
    )


def _load_geoid_reference(path_csv: Path) -> pl.DataFrame:
    """Load and normalize benchmark county FIPS with region labels."""
    if not path_csv.exists():
        raise FileNotFoundError(
            f"GEOID reference CSV not found: {path_csv}. "
            "Run python/ingest/location_processing/01_fetch_geoid_reference.py first."
        )
    ref = pl.read_csv(str(path_csv), infer_schema_length=0)
    geoid_col = next((c for c in ref.columns if "geoid" in c.lower()), None)
    if geoid_col is None:
        raise RuntimeError("GEOID reference missing GEOID column.")
    region_col = next((c for c in ref.columns if c.lower() in ("cluster_name", "region", "cluster")), None)
    if region_col is None:
        raise RuntimeError("GEOID reference missing region/cluster column.")
    out = (
        ref.select(
            [
                _normalize_fips5_expr(pl.col(geoid_col)).alias("county_fips"),
                pl.col(region_col).cast(pl.Utf8, strict=False).fill_null("").alias("region"),
            ]
        )
        .filter((pl.col("county_fips").str.len_chars() == 5) & ~_blank(pl.col("region")))
        .unique(subset=["county_fips"], keep="first", maintain_order=True)
    )
    return out


def _load_zip_to_fips(path: Path) -> pl.DataFrame:
    """Load ZIP->county FIPS map from CSV."""
    if not path.exists():
        raise FileNotFoundError(f"ZIP-to-county file not found: {path}")
    z = pl.read_csv(str(path), infer_schema_length=0)
    zip_col = next((c for c in z.columns if "zip" in c.lower()), z.columns[0])
    fips_col = next((c for c in z.columns if "fips" in c.lower()), z.columns[1] if len(z.columns) > 1 else None)
    if fips_col is None:
        raise RuntimeError("ZIP-to-county CSV missing FIPS column.")
    out = (
        z.select(
            [
                _normalize_zip5_expr(pl.col(zip_col)).alias("zip5"),
                _normalize_fips5_expr(pl.col(fips_col)).alias("county_fips"),
            ]
        )
        .filter((pl.col("zip5").str.len_chars() == 5) & (pl.col("county_fips").str.len_chars() == 5))
        .unique(subset=["zip5"], keep="first", maintain_order=True)
    )
    return out


def _pick_zip_column(columns: list[str]) -> str:
    """Pick best ZIP column candidate from source schema."""
    candidates = ["FILERUSZIP", "zip", "ZIP", "FODTKEAZIPCO", "ODTKEAZIPCOD"]
    for c in candidates:
        if c in columns:
            return c
    raise RuntimeError(f"No ZIP column found. Checked: {candidates}")


def _align_columns_as_string(lf: pl.LazyFrame, ordered_cols: list[str]) -> pl.LazyFrame:
    """Align to target column order and cast all to Utf8 for stable output."""
    names = set(lf.collect_schema().names())
    exprs: list[pl.Expr] = []
    for c in ordered_cols:
        if c in names:
            exprs.append(pl.col(c).cast(pl.Utf8, strict=False).alias(c))
        else:
            exprs.append(pl.lit(None, dtype=pl.Utf8).alias(c))
    return lf.select(exprs)


def main() -> None:
    parser = argparse.ArgumentParser(description="Filter unified unfiltered parquet to benchmark-region Silver output.")
    parser.add_argument("--input", default=str(BASIC_PLUS_COMBINED_PARQUET), help="Input unfiltered parquet")
    parser.add_argument("--output", default=str(FILTERED_SILVER_PARQUET), help="Output filtered parquet")
    parser.add_argument("--manifest-json", default=str(FILTERED_MANIFEST_JSON), help="Output filter manifest JSON")
    parser.add_argument("--zip-to-county", default=str(ZIP_TO_COUNTY_CSV), help="ZIP->county FIPS CSV")
    parser.add_argument("--geoid-csv", default=str(GEOID_REFERENCE_CSV), help="GEOID reference CSV")
    parser.add_argument("--tax-year-min", type=int, default=2021, help="Minimum tax year to keep")
    parser.add_argument(
        "--batch-size",
        type=int,
        default=200_000,
        help="Legacy compatibility argument (not used in Polars lazy mode)",
    )
    args = parser.parse_args()

    start = time.perf_counter()
    banner("STEP 09 - FILTER TO BENCHMARK AND WRITE SILVER LOCAL")

    in_path = Path(args.input)
    out_path = Path(args.output)
    manifest_path = Path(args.manifest_json)
    if not in_path.exists():
        raise FileNotFoundError(f"Input parquet not found: {in_path}")
    out_path.parent.mkdir(parents=True, exist_ok=True)

    print(f"[filter] GEOID reference CSV: {args.geoid_csv}", flush=True)
    print(f"[filter] ZIP->county CSV:      {args.zip_to_county}", flush=True)
    print(f"[filter] batch-size argument is ignored in Polars mode: {args.batch_size}", flush=True)

    with tqdm(total=8, desc="Filter stages", unit="stage") as stage:
        geoid_df = _load_geoid_reference(Path(args.geoid_csv))
        stage.update(1)
        zip_df = _load_zip_to_fips(Path(args.zip_to_county))
        stage.update(1)

        print(f"[filter] Target benchmark counties: {geoid_df.height}", flush=True)
        print(f"[filter] ZIP->FIPS map size: {zip_df.height:,}", flush=True)

        input_lf = pl.scan_parquet(str(in_path))
        input_cols = input_lf.collect_schema().names()
        stage.update(1)

        zip_col_used = _pick_zip_column(input_cols)
        tax_col = "tax_year" if "tax_year" in input_cols else ("TAXYEAR" if "TAXYEAR" in input_cols else "")
        print(f"[filter] Using ZIP column: {zip_col_used}", flush=True)
        if tax_col:
            print(f"[filter] Using tax-year column: {tax_col}", flush=True)
        else:
            print("[filter] No tax-year column found; tax-year filter will produce zero rows.", flush=True)

        rows_input = input_lf.select(pl.len().alias("n")).collect().item(0, 0)
        stage.update(1)
        print(f"[filter] rows input: {int(rows_input):,}", flush=True)

        if tax_col:
            tax_num_expr = _normalize_tax_year_expr(pl.col(tax_col)).cast(pl.Int64, strict=False).alias("__tax_num")
        else:
            tax_num_expr = pl.lit(None, dtype=pl.Int64).alias("__tax_num")

        tax_filtered_lf = input_lf.with_columns([tax_num_expr]).filter(pl.col("__tax_num") >= args.tax_year_min).drop("__tax_num")
        rows_after_tax = tax_filtered_lf.select(pl.len().alias("n")).collect().item(0, 0)
        stage.update(1)
        print(f"[filter] rows after tax filter: {int(rows_after_tax):,}", flush=True)

        geo_joined_lf = (
            tax_filtered_lf.with_columns([_normalize_zip5_expr(pl.col(zip_col_used)).alias("zip5")])
            .join(zip_df.lazy(), on="zip5", how="left")
            .join(geoid_df.lazy(), on="county_fips", how="left")
        )
        geo_filtered_lf = geo_joined_lf.filter(pl.col("county_fips").is_not_null() & pl.col("region").is_not_null())
        print("[filter] Geo filter built; writing output in streaming mode...", flush=True)
        stage.update(1)

        output_cols = geo_filtered_lf.collect_schema().names()
        final_lf = _align_columns_as_string(geo_filtered_lf, output_cols)
        print(f"[filter] Output columns: {len(output_cols):,}", flush=True)
        print(f"[filter] Writing filtered parquet: {out_path}", flush=True)
        try:
            final_lf.sink_parquet(str(out_path), compression="snappy")
        except Exception as e:
            # Compatibility fallback for environments where sink_parquet is unavailable.
            print(
                f"[filter] sink_parquet unavailable ({type(e).__name__}), falling back to collect().write_parquet()",
                flush=True,
            )
            final_lf.collect().write_parquet(str(out_path), compression="snappy")
        stage.update(1)

        rows_output = (
            pl.scan_parquet(str(out_path))
            .select(pl.len().alias("n"))
            .collect()
            .item(0, 0)
        )
        rows_after_geo = rows_output
        print(f"[filter] rows after geo filter: {int(rows_after_geo):,}", flush=True)
        stage.update(1)

    manifest = {
        "created_at_utc": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "input_path": str(in_path),
        "output_path": str(out_path),
        "tax_year_min": args.tax_year_min,
        "zip_column_used": zip_col_used or "",
        "rows_input": int(rows_input),
        "rows_after_tax_filter": int(rows_after_tax),
        "rows_after_geo_filter": int(rows_after_geo),
        "rows_output": int(rows_output),
        "target_county_fips_count": int(geoid_df.height),
    }
    manifest_path.write_text(json.dumps(manifest, indent=2), encoding="utf-8")

    print(f"[filter] Wrote filtered parquet: {out_path}", flush=True)
    print(f"[filter] Wrote manifest JSON:   {manifest_path}", flush=True)
    print(f"[filter] rows input:            {int(rows_input):,}", flush=True)
    print(f"[filter] rows after tax filter: {int(rows_after_tax):,}", flush=True)
    print(f"[filter] rows after geo filter: {int(rows_after_geo):,}", flush=True)
    print(f"[filter] rows output:           {int(rows_output):,}", flush=True)
    print_elapsed(start, "Step 09")


if __name__ == "__main__":
    main()
