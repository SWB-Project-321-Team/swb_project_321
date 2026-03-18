"""
Step 06: Build one unfiltered file with Basic forms combined.

Input files:
- Basic Fields 990
- Basic Fields 990EZ
- Basic Fields 990PF

Output:
- givingtuesday_990_basic_allforms_unfiltered.parquet
"""

from __future__ import annotations

import argparse
import csv
import time
from pathlib import Path

import polars as pl
from tqdm import tqdm

from common import (
    BASIC_ALLFORMS_PARQUET,
    CATALOG_CSV,
    RAW_DIR,
    banner,
    ensure_dirs,
    load_catalog_rows,
    load_env_from_secrets,
    print_elapsed,
    select_required_datasets,
    url_basename,
)


def _required_basic_rows(catalog_rows: list[dict[str, str]]) -> list[dict[str, str]]:
    """Subset required rows to the three Basic datasets."""
    required = select_required_datasets(catalog_rows)
    return [r for r in required if (r.get("title", "") == "Basic Fields")]


def _read_csv_header(path: Path) -> list[str]:
    """Read the first CSV header row from a local file."""
    with open(path, newline="", encoding="utf-8-sig") as f:
        reader = csv.reader(f)
        return next(reader, [])


def _blank(expr: pl.Expr) -> pl.Expr:
    """True where value is blank-like (None/whitespace)."""
    return expr.is_null() | expr.cast(pl.Utf8, strict=False).str.strip_chars().eq("")


def _normalize_ein_expr(expr: pl.Expr) -> pl.Expr:
    """Normalize EIN into 9-digit string; blank when missing."""
    digits = expr.cast(pl.Utf8, strict=False).fill_null("").str.replace_all(r"[^0-9]", "")
    return (
        pl.when(digits.str.len_chars() == 0)
        .then(pl.lit(""))
        .when(digits.str.len_chars() < 9)
        .then(digits.str.zfill(9))
        .otherwise(digits.str.slice(0, 9))
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


def _normalize_returntype_expr(expr: pl.Expr) -> pl.Expr:
    """Normalize return type to uppercase with no spaces or hyphens."""
    return (
        expr.cast(pl.Utf8, strict=False)
        .fill_null("")
        .str.strip_chars()
        .str.to_uppercase()
        .str.replace_all("-", "")
        .str.replace_all(" ", "")
    )


def _infer_form_type_expr(returntype_expr: pl.Expr) -> pl.Expr:
    """Infer canonical form type from normalized return type."""
    rt = returntype_expr
    return (
        pl.when(rt.str.starts_with("990EZ"))
        .then(pl.lit("990EZ"))
        .when(rt.str.starts_with("990PF"))
        .then(pl.lit("990PF"))
        .when(rt.str.starts_with("990N"))
        .then(pl.lit("990N"))
        .when(rt.str.starts_with("990"))
        .then(pl.lit("990"))
        .otherwise(pl.when(_blank(rt)).then(pl.lit("UNKNOWN")).otherwise(rt))
    )


def _align_columns_as_string(lf: pl.LazyFrame, ordered_cols: list[str]) -> pl.LazyFrame:
    """Align to target column order and cast all columns to Utf8."""
    names = set(lf.collect_schema().names())
    exprs: list[pl.Expr] = []
    for c in ordered_cols:
        if c in names:
            exprs.append(pl.col(c).cast(pl.Utf8, strict=False).alias(c))
        else:
            exprs.append(pl.lit(None, dtype=pl.Utf8).alias(c))
    return lf.select(exprs)


def _source_expr(schema_cols: set[str], col_name: str) -> pl.Expr:
    """
    Return source column expression when present; otherwise empty-string literal.
    This matches previous pandas behavior when expected columns are missing.
    """
    if col_name in schema_cols:
        return pl.col(col_name)
    return pl.lit("")


def main() -> None:
    parser = argparse.ArgumentParser(description="Build unfiltered Basic-only all-forms parquet.")
    parser.add_argument("--catalog-csv", default=str(CATALOG_CSV), help="Catalog CSV path")
    parser.add_argument("--raw-dir", default=str(RAW_DIR), help="Raw files directory")
    parser.add_argument("--output", default=str(BASIC_ALLFORMS_PARQUET), help="Output parquet path")
    parser.add_argument(
        "--chunksize",
        type=int,
        default=200_000,
        help="Legacy compatibility argument (not used in Polars lazy mode)",
    )
    args = parser.parse_args()

    start = time.perf_counter()
    banner("STEP 06 - BUILD BASIC ALL-FORMS UNFILTERED FILE")
    load_env_from_secrets()
    ensure_dirs()

    raw_dir = Path(args.raw_dir)
    out_path = Path(args.output)
    out_path.parent.mkdir(parents=True, exist_ok=True)

    catalog_rows = load_catalog_rows(Path(args.catalog_csv))
    basic_rows = _required_basic_rows(catalog_rows)
    if len(basic_rows) != 3:
        raise RuntimeError(f"Expected 3 required Basic rows, found {len(basic_rows)}")
    print(f"[basic] Required Basic datasets: {len(basic_rows)}", flush=True)
    print(f"[basic] chunksize argument is ignored in Polars mode: {args.chunksize}", flush=True)

    # Determine union columns from headers for stable output schema.
    union_cols: list[str] = []
    with tqdm(total=len(basic_rows), desc="Inspect headers", unit="dataset") as pbar:
        for row in basic_rows:
            local_file = raw_dir / url_basename(row.get("download_url", ""))
            if not local_file.exists():
                raise FileNotFoundError(f"Missing basic raw file: {local_file}")
            header_cols = _read_csv_header(local_file)
            print(
                f"[basic] Header scan {row.get('title', '')} [{row.get('form_type', '')}] "
                f"=> {local_file.name} ({len(header_cols)} columns)",
                flush=True,
            )
            for c in header_cols:
                if c not in union_cols:
                    union_cols.append(c)
            pbar.update(1)

    extra_cols = [
        "ein",
        "tax_year",
        "returntype_norm",
        "form_type",
        "source_dataset_id",
        "source_dataset_title",
        "source_filename",
        "from_basic",
        "from_combined",
    ]
    output_cols = union_cols + [c for c in extra_cols if c not in union_cols]
    print(f"[basic] Union schema columns: {len(output_cols)}", flush=True)

    dataset_lfs: list[pl.LazyFrame] = []
    with tqdm(total=len(basic_rows), desc="Build lazy plans", unit="dataset") as pbar:
        for row in basic_rows:
            title = row.get("title", "")
            form = row.get("form_type", "")
            file_path = raw_dir / url_basename(row.get("download_url", ""))
            print(f"\n[basic] Processing {title} [{form}] from {file_path}", flush=True)
            scan_lf = pl.scan_csv(str(file_path), infer_schema_length=0)
            scan_cols = set(scan_lf.collect_schema().names())
            print(f"[basic] Source columns discovered: {len(scan_cols)}", flush=True)

            returntype_norm = _normalize_returntype_expr(_source_expr(scan_cols, "RETURNTYPE"))
            dataset_lf = scan_lf.with_columns(
                [
                    _normalize_ein_expr(_source_expr(scan_cols, "FILEREIN")).alias("ein"),
                    _normalize_tax_year_expr(_source_expr(scan_cols, "TAXYEAR")).alias("tax_year"),
                    returntype_norm.alias("returntype_norm"),
                    _infer_form_type_expr(returntype_norm).alias("form_type"),
                    pl.lit(row.get("dataset_id", "")).alias("source_dataset_id"),
                    pl.lit(title).alias("source_dataset_title"),
                    pl.lit(file_path.name).alias("source_filename"),
                    pl.lit("1").alias("from_basic"),
                    pl.lit("0").alias("from_combined"),
                ]
            )
            dataset_lfs.append(_align_columns_as_string(dataset_lf, output_cols))
            pbar.update(1)

    print("[basic] Concatenating Basic forms and writing parquet...", flush=True)
    final_lf = pl.concat(dataset_lfs, how="vertical_relaxed")
    try:
        final_lf.sink_parquet(str(out_path), compression="snappy")
    except Exception as e:
        # Compatibility fallback for older Polars builds.
        print(f"[basic] sink_parquet unavailable ({type(e).__name__}), falling back to collect().write_parquet()", flush=True)
        final_lf.collect().write_parquet(str(out_path), compression="snappy")

    total_rows = (
        pl.scan_parquet(str(out_path))
        .select(pl.len().alias("rows"))
        .collect()
        .item(0, 0)
    )

    print(f"[basic] Output written: {out_path}", flush=True)
    print(f"[basic] Total datasets processed: {len(basic_rows):,}", flush=True)
    print(f"[basic] Total rows output:        {int(total_rows):,}", flush=True)
    print(f"[basic] Total columns output:     {len(output_cols):,}", flush=True)
    print_elapsed(start, "Step 06")


if __name__ == "__main__":
    main()
