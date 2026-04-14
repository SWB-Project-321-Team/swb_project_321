"""
Step 06: Build the admitted Basic pre-Silver parquet.

Input files:
- Basic Fields 990
- Basic Fields 990EZ
- Basic Fields 990PF
- Combined Forms Datamart cache parquet
- ROI geography reference files

Output:
- givingtuesday_990_basic_allforms_presilver.parquet
"""

from __future__ import annotations

import argparse
import csv
import time
from pathlib import Path

import duckdb
import polars as pl
import pyarrow as pa
import pyarrow.parquet as pq
from tqdm import tqdm

from common import (
    BASIC_ALLFORMS_PARQUET,
    BASIC_ALLFORMS_BUILD_MANIFEST_JSON,
    BASIC_DEDUP_PRIORITY_COLUMNS,
    DATE_COLUMNS,
    NAME_LIKE_COLUMNS,
    PHONE_NUMBER_COLUMNS,
    STATE_CODE_COLUMNS,
    CATALOG_CSV,
    COMBINED_NORMALIZED_CACHE_MANIFEST_JSON,
    COMBINED_NORMALIZED_CACHE_PARQUET,
    GEOID_REFERENCE_CSV,
    GT_BASIC_PRESILVER_METADATA_COLUMNS,
    RAW_DIR,
    ZIP_CODE_COLUMNS,
    ZIP_TO_COUNTY_CSV,
    banner,
    build_gt_roi_zip_map,
    build_input_signature,
    ensure_combined_normalized_cache,
    ensure_dirs,
    gt_normalize_tax_year_expr,
    gt_normalize_zip5_expr,
    load_gt_geoid_reference,
    load_gt_zip_to_fips,
    load_catalog_rows,
    load_env_from_secrets,
    manifest_matches_expectation,
    print_elapsed,
    read_json,
    select_required_datasets,
    url_basename,
    write_json,
)

KEY_COLS = ["ein", "tax_year", "returntype_norm"]


def _required_basic_rows(catalog_rows: list[dict[str, str]]) -> list[dict[str, str]]:
    """Subset required rows to the three Basic datasets."""
    required = select_required_datasets(catalog_rows)
    return [r for r in required if (r.get("title", "") == "Basic Fields")]


def _required_combined_row(catalog_rows: list[dict[str, str]]) -> dict[str, str]:
    """Return the required Combined catalog row used to derive ROI-admitted keys."""
    required = select_required_datasets(catalog_rows)
    combined_row = next((r for r in required if (r.get("title", "") == "Combined Forms Datamart")), None)
    if combined_row is None:
        raise RuntimeError("Required dataset not found: Combined Forms Datamart")
    return combined_row


def _read_csv_header(path: Path) -> list[str]:
    """Read the first CSV header row from a local file."""
    with open(path, newline="", encoding="utf-8-sig") as f:
        reader = csv.reader(f)
        return next(reader, [])


def _blank(expr: pl.Expr) -> pl.Expr:
    """True where value is blank-like (None/whitespace)."""
    return expr.is_null() | expr.cast(pl.Utf8, strict=False).str.strip_chars().eq("")


def _flag_true(expr: pl.Expr) -> pl.Expr:
    """True where a GT boolean-ish filing flag is set."""
    normalized = expr.cast(pl.Utf8, strict=False).fill_null("").str.strip_chars().str.to_uppercase()
    return normalized.is_in(["1", "Y", "YES", "TRUE", "T", "X"])


def _qid(name: str) -> str:
    """Safely quote one DuckDB identifier."""
    return '"' + name.replace('"', '""') + '"'


def _qstr(text: str) -> str:
    """Safely quote one DuckDB string literal."""
    return "'" + text.replace("'", "''") + "'"


def _blank_sql(alias: str, col: str) -> str:
    """DuckDB SQL expression for blank-like values."""
    qcol = f"{alias}.{_qid(col)}"
    return f"({qcol} IS NULL OR trim({qcol}) = '')"


def _trueish_sql(alias: str, col: str) -> str:
    """DuckDB SQL expression for GT filing flags that use truthy markers like X/Y/1."""
    qcol = f"{alias}.{_qid(col)}"
    normalized = f"upper(trim(coalesce({qcol}, '')))"
    return f"{normalized} IN ('1', 'Y', 'YES', 'TRUE', 'T', 'X')"


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


def _normalized_text_expr(expr: pl.Expr) -> pl.Expr:
    """Trim and collapse internal whitespace for one text-like value."""
    return (
        expr.cast(pl.Utf8, strict=False)
        .fill_null("")
        .str.replace_all(r"\s+", " ")
        .str.strip_chars()
    )


def _normalize_name_like_expr(expr: pl.Expr) -> pl.Expr:
    """Canonicalize names/titles to trimmed uppercase text."""
    text = _normalized_text_expr(expr)
    return pl.when(text.eq("")).then(pl.lit("")).otherwise(text.str.to_uppercase())


def _normalize_state_expr(expr: pl.Expr) -> pl.Expr:
    """Canonicalize state abbreviations."""
    text = _normalized_text_expr(expr).str.to_uppercase()
    return pl.when(text.eq("")).then(pl.lit("")).otherwise(text)


def _normalize_zip_expr(expr: pl.Expr) -> pl.Expr:
    """Canonicalize US ZIP codes to the first five digits when available."""
    digits = expr.cast(pl.Utf8, strict=False).fill_null("").str.replace_all(r"[^0-9]", "")
    return (
        pl.when(digits.str.len_chars() < 5)
        .then(pl.lit(""))
        .otherwise(digits.str.slice(0, 5))
    )


def _normalize_phone_expr(expr: pl.Expr) -> pl.Expr:
    """Canonicalize phone numbers to digits only."""
    digits = expr.cast(pl.Utf8, strict=False).fill_null("").str.replace_all(r"[^0-9]", "")
    return pl.when(digits.eq("")).then(pl.lit("")).otherwise(digits)


def _normalize_date_expr(expr: pl.Expr) -> pl.Expr:
    """Canonicalize date-like values to YYYY-MM-DD when parseable."""
    text = _normalized_text_expr(expr)
    parsed_iso = text.str.strptime(pl.Date, format="%Y-%m-%d", strict=False).dt.strftime("%Y-%m-%d")
    parsed_iso_slash = text.str.strptime(pl.Date, format="%Y/%m/%d", strict=False).dt.strftime("%Y-%m-%d")
    parsed_us_slash = text.str.strptime(pl.Date, format="%m/%d/%Y", strict=False).dt.strftime("%Y-%m-%d")
    parsed_us_dash = text.str.strptime(pl.Date, format="%m-%d-%Y", strict=False).dt.strftime("%Y-%m-%d")
    return (
        pl.when(text.eq(""))
        .then(pl.lit(""))
        .otherwise(pl.coalesce([parsed_iso, parsed_iso_slash, parsed_us_slash, parsed_us_dash, text]))
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


def _low_risk_normalization_exprs(schema_cols: set[str]) -> list[pl.Expr]:
    """Return in-place cleanup expressions for low-risk overlapping GT fields."""
    exprs: list[pl.Expr] = []
    for col_name in NAME_LIKE_COLUMNS:
        if col_name in schema_cols:
            exprs.append(_normalize_name_like_expr(pl.col(col_name)).alias(col_name))
    for col_name in STATE_CODE_COLUMNS:
        if col_name in schema_cols:
            exprs.append(_normalize_state_expr(pl.col(col_name)).alias(col_name))
    for col_name in ZIP_CODE_COLUMNS:
        if col_name in schema_cols:
            exprs.append(_normalize_zip_expr(pl.col(col_name)).alias(col_name))
    for col_name in PHONE_NUMBER_COLUMNS:
        if col_name in schema_cols:
            exprs.append(_normalize_phone_expr(pl.col(col_name)).alias(col_name))
    for col_name in DATE_COLUMNS:
        if col_name in schema_cols:
            exprs.append(_normalize_date_expr(pl.col(col_name)).alias(col_name))
    return exprs


def _present_key_exprs() -> list[pl.Expr]:
    """Return normalized key-present checks for the GT filing key fields."""
    return [
        ~_blank(pl.col("ein")),
        ~_blank(pl.col("tax_year")),
        ~_blank(pl.col("returntype_norm")),
    ]


def main() -> None:
    parser = argparse.ArgumentParser(description="Build the admitted GT Basic pre-Silver parquet.")
    parser.add_argument("--catalog-csv", default=str(CATALOG_CSV), help="Catalog CSV path")
    parser.add_argument("--raw-dir", default=str(RAW_DIR), help="Raw files directory")
    parser.add_argument("--zip-to-county", default=str(ZIP_TO_COUNTY_CSV), help="ZIP->county FIPS CSV")
    parser.add_argument("--geoid-csv", default=str(GEOID_REFERENCE_CSV), help="Benchmark GEOID reference CSV")
    parser.add_argument("--tax-year-min", type=int, default=2022, help="Minimum tax year to retain in the admitted Basic pre-Silver build")
    parser.add_argument("--tax-year-max", type=int, default=2024, help="Maximum tax year to retain in the admitted Basic pre-Silver build")
    parser.add_argument(
        "--combined-cache-parquet",
        default=str(COMBINED_NORMALIZED_CACHE_PARQUET),
        help="Cached normalized parquet mirror for the raw Combined CSV",
    )
    parser.add_argument(
        "--combined-cache-manifest-json",
        default=str(COMBINED_NORMALIZED_CACHE_MANIFEST_JSON),
        help="Manifest JSON for the cached normalized Combined parquet",
    )
    parser.add_argument("--output", default=str(BASIC_ALLFORMS_PARQUET), help="Output admitted Basic pre-Silver parquet path")
    parser.add_argument("--build-manifest-json", default=str(BASIC_ALLFORMS_BUILD_MANIFEST_JSON), help="Local build manifest JSON path")
    parser.add_argument(
        "--chunksize",
        type=int,
        default=200_000,
        help="Legacy compatibility argument (not used in Polars lazy mode)",
    )
    parser.add_argument(
        "--skip-if-unchanged",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="Skip rebuild when the cached input signature and outputs still match",
    )
    parser.add_argument("--force-rebuild", action="store_true", help="Rebuild even when the cached input signature matches")
    args = parser.parse_args()

    start = time.perf_counter()
    banner("STEP 06 - BUILD BASIC ALL-FORMS PRE-SILVER FILE")
    load_env_from_secrets()
    ensure_dirs()

    raw_dir = Path(args.raw_dir)
    out_path = Path(args.output)
    manifest_path = Path(args.build_manifest_json)
    geoid_csv = Path(args.geoid_csv)
    zip_to_county_csv = Path(args.zip_to_county)
    combined_cache_path = Path(args.combined_cache_parquet)
    combined_cache_manifest_path = Path(args.combined_cache_manifest_json)
    out_path.parent.mkdir(parents=True, exist_ok=True)

    catalog_rows = load_catalog_rows(Path(args.catalog_csv))
    basic_rows = _required_basic_rows(catalog_rows)
    combined_row = _required_combined_row(catalog_rows)
    if len(basic_rows) != 3:
        raise RuntimeError(f"Expected 3 required Basic rows, found {len(basic_rows)}")
    combined_path = raw_dir / url_basename(combined_row.get("download_url", ""))
    if not combined_path.exists():
        raise FileNotFoundError(f"Missing Combined raw file: {combined_path}")

    print(f"[basic] Required Basic datasets: {len(basic_rows)}", flush=True)
    print(f"[basic] Combined raw source:      {combined_path}", flush=True)
    print(f"[basic] Combined cache parquet:  {combined_cache_path}", flush=True)
    print(f"[basic] GEOID reference:         {geoid_csv}", flush=True)
    print(f"[basic] ZIP->county source:      {zip_to_county_csv}", flush=True)
    print(f"[basic] Tax year min:           {args.tax_year_min}", flush=True)
    print(f"[basic] Tax year max:           {args.tax_year_max}", flush=True)
    print(f"[basic] chunksize argument is ignored in Polars mode: {args.chunksize}", flush=True)
    print(f"[basic] Skip if unchanged: {args.skip_if_unchanged}", flush=True)
    print(f"[basic] Force rebuild: {args.force_rebuild}", flush=True)

    geoid_df = load_gt_geoid_reference(geoid_csv)
    zip_df = load_gt_zip_to_fips(zip_to_county_csv)
    roi_zip_df = build_gt_roi_zip_map(zip_df, geoid_df)
    print(f"[basic] Benchmark counties:      {geoid_df.height:,}", flush=True)
    print(f"[basic] ROI ZIP map rows:       {roi_zip_df.height:,}", flush=True)
    ensure_combined_normalized_cache(
        combined_csv_path=combined_path,
        output_path=combined_cache_path,
        manifest_path=combined_cache_manifest_path,
        force_rebuild=args.force_rebuild,
    )

    source_paths = {"catalog_csv": Path(args.catalog_csv)}
    for row in basic_rows:
        local_file = raw_dir / url_basename(row.get("download_url", ""))
        if not local_file.exists():
            raise FileNotFoundError(f"Missing basic raw file: {local_file}")
        source_paths[f"raw_basic_{row.get('form_type', '').lower()}"] = local_file
    source_paths["combined_cache_parquet"] = combined_cache_path
    source_paths["geoid_csv"] = geoid_csv
    source_paths["zip_to_county_csv"] = zip_to_county_csv
    input_signature = build_input_signature(source_paths)
    build_options = {
        "output_path": str(out_path),
        "script": "06_build_basic_allforms_presilver",
        "tax_year_min": int(args.tax_year_min),
        "tax_year_max": int(args.tax_year_max),
        "combined_cache_parquet": str(combined_cache_path),
    }
    if args.skip_if_unchanged and not args.force_rebuild and manifest_matches_expectation(
        manifest_path,
        expected_input_signature=input_signature,
        expected_options=build_options,
        required_outputs=[out_path],
    ):
        cached = read_json(manifest_path)
        print("[basic] Inputs match cached build manifest and output already exists; skipping rebuild.", flush=True)
        print(f"[basic] Cached row count: {cached.get('rows_output', '')}", flush=True)
        print_elapsed(start, "Step 06")
        return

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
    output_cols = union_cols + [c for c in extra_cols if c not in union_cols] + [
        c for c in GT_BASIC_PRESILVER_METADATA_COLUMNS if c not in union_cols and c not in extra_cols
    ]
    print(f"[basic] Union schema columns: {len(output_cols)}", flush=True)

    print("[basic] Deriving Combined ROI-admitted filing keys before reading Basic raw files...", flush=True)
    roi_zip_flags = roi_zip_df.select("zip5").with_columns(pl.lit("1").alias("__combined_geo_match"))
    combined_returntype_norm = _normalize_returntype_expr(pl.col("RETURNTYPE"))
    combined_roi_keys = (
        pl.scan_parquet(str(combined_cache_path))
        .with_columns(
            [
                _normalize_ein_expr(pl.col("FILEREIN")).alias("ein"),
                _normalize_tax_year_expr(pl.col("TAXYEAR")).alias("tax_year"),
                combined_returntype_norm.alias("returntype_norm"),
                gt_normalize_zip5_expr(pl.col("FILERUSZIP")).alias("zip5"),
                gt_normalize_tax_year_expr(pl.col("TAXYEAR")).cast(pl.Int64, strict=False).alias("__tax_year_num"),
            ]
        )
        # The Combined cache is still the upstream nationwide source of truth,
        # but step 06 only uses the subset whose own geography admits them into
        # the benchmark ROI. That keeps the later Basic combine stage narrow.
        .join(roi_zip_flags.lazy(), on="zip5", how="inner")
        .filter((pl.col("__tax_year_num") >= args.tax_year_min) & (pl.col("__tax_year_num") <= args.tax_year_max))
        .filter(pl.all_horizontal(_present_key_exprs()))
        .select(KEY_COLS)
        .unique(subset=KEY_COLS, keep="first", maintain_order=True)
        .collect()
    )
    combined_key_flags = combined_roi_keys.with_columns(pl.lit("1").alias("presilver_admitted_by_combined_key"))
    print(f"[basic] Combined ROI-admitted key count: {combined_roi_keys.height:,}", flush=True)

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
            roi_basic_flags = roi_zip_df.select("zip5").with_columns(pl.lit("1").alias("presilver_admitted_by_basic_geography"))
            dataset_lf = scan_lf.with_columns(
                _low_risk_normalization_exprs(scan_cols)
                + [
                    _normalize_ein_expr(_source_expr(scan_cols, "FILEREIN")).alias("ein"),
                    _normalize_tax_year_expr(_source_expr(scan_cols, "TAXYEAR")).alias("tax_year"),
                    returntype_norm.alias("returntype_norm"),
                    _infer_form_type_expr(returntype_norm).alias("form_type"),
                    gt_normalize_zip5_expr(_source_expr(scan_cols, "FILERUSZIP")).alias("__zip5"),
                    gt_normalize_tax_year_expr(_source_expr(scan_cols, "TAXYEAR")).cast(pl.Int64, strict=False).alias("__tax_year_num"),
                    pl.lit(row.get("dataset_id", "")).alias("source_dataset_id"),
                    pl.lit(title).alias("source_dataset_title"),
                    pl.lit(file_path.name).alias("source_filename"),
                    pl.lit("1").alias("from_basic"),
                    pl.lit("0").alias("from_combined"),
                ]
            )
            dataset_lf = (
                dataset_lf
                .filter((pl.col("__tax_year_num") >= args.tax_year_min) & (pl.col("__tax_year_num") <= args.tax_year_max))
                # Admit Basic rows by their own benchmark geography first, then
                # preserve any extra Basic rows whose filing key is already
                # admitted by the ROI-scoped Combined source. That keeps rescue
                # behavior available downstream without re-broadening the stage.
                .join(roi_basic_flags.lazy(), left_on="__zip5", right_on="zip5", how="left")
                .join(combined_key_flags.lazy(), on=KEY_COLS, how="left")
                .with_columns(
                    [
                        pl.col("presilver_admitted_by_basic_geography").fill_null("0"),
                        pl.col("presilver_admitted_by_combined_key").fill_null("0"),
                    ]
                )
                .with_columns(
                    [
                        pl.when(
                            (pl.col("presilver_admitted_by_basic_geography") == "1")
                            & (pl.col("presilver_admitted_by_combined_key") == "1")
                        )
                        .then(pl.lit("both"))
                        .when(pl.col("presilver_admitted_by_basic_geography") == "1")
                        .then(pl.lit("basic_geography"))
                        .when(pl.col("presilver_admitted_by_combined_key") == "1")
                        .then(pl.lit("combined_key"))
                        .otherwise(pl.lit(""))
                        .alias("presilver_admission_basis"),
                    ]
                )
                .filter(pl.col("presilver_admission_basis") != "")
                .drop(["__zip5", "__tax_year_num"])
            )
            dataset_lfs.append(_align_columns_as_string(dataset_lf, output_cols))
            pbar.update(1)

    print("[basic] Concatenating admitted Basic rows and writing pre-dedupe parquet...", flush=True)
    final_lf = pl.concat(dataset_lfs, how="vertical_relaxed")
    temp_out_path = out_path.with_name(f"{out_path.stem}__pre_dedup.parquet")
    if temp_out_path.exists():
        temp_out_path.unlink()
    try:
        final_lf.sink_parquet(str(temp_out_path), compression="snappy")
    except Exception as e:
        # Compatibility fallback for older Polars builds.
        print(f"[basic] sink_parquet unavailable ({type(e).__name__}), falling back to collect().write_parquet()", flush=True)
        final_lf.collect().write_parquet(str(temp_out_path), compression="snappy")

    presilver_counts = (
        pl.scan_parquet(str(temp_out_path))
        .select(
            [
                pl.len().alias("rows_before_dedupe"),
                pl.col("presilver_admitted_by_basic_geography").eq("1").sum().alias("basic_geography_admitted_rows"),
                pl.col("presilver_admitted_by_combined_key").eq("1").sum().alias("combined_key_admitted_rows"),
                pl.col("presilver_admission_basis").eq("both").sum().alias("both_admitted_rows"),
            ]
        )
        .collect()
        .row(0, named=True)
    )
    print(
        "[basic] Admitted rows before dedupe | "
        f"rows={int(presilver_counts['rows_before_dedupe']):,} "
        f"basic_geography={int(presilver_counts['basic_geography_admitted_rows']):,} "
        f"combined_key={int(presilver_counts['combined_key_admitted_rows']):,} "
        f"both={int(presilver_counts['both_admitted_rows']):,}",
        flush=True,
    )

    print("[basic] Applying upstream amended/final filing ranking dedupe...", flush=True)
    all_cols = list(output_cols)
    score_cols = [
        c
        for c in all_cols
        if c not in KEY_COLS
        and c not in (
            "source_dataset_id",
            "source_dataset_title",
            "source_filename",
            "from_basic",
            "from_combined",
            *GT_BASIC_PRESILVER_METADATA_COLUMNS,
        )
    ]
    priority_cols = [c for c in BASIC_DEDUP_PRIORITY_COLUMNS if c in all_cols]
    score_terms = [f"CASE WHEN {_blank_sql('b', c)} THEN 0 ELSE 1 END" for c in score_cols]
    priority_terms = [f"CASE WHEN {_blank_sql('b', c)} THEN 0 ELSE 1 END" for c in priority_cols]
    score_sql = " + ".join(score_terms) if score_terms else "0"
    priority_sql = " + ".join(priority_terms) if priority_terms else "0"
    amended_sql = _trueish_sql("b", "AMENDERETURN") if "AMENDERETURN" in all_cols else "FALSE"
    final_sql = _trueish_sql("b", "FINALRRETURN") if "FINALRRETURN" in all_cols else "FALSE"
    initial_sql = _trueish_sql("b", "INITIARETURN") if "INITIARETURN" in all_cols else "FALSE"
    filing_preference_sql = (
        f"CASE "
        f"WHEN {amended_sql} THEN 3 "
        f"WHEN {final_sql} THEN 2 "
        f"WHEN {initial_sql} THEN 1 "
        f"ELSE 0 END"
    )
    select_cols_sql = ", ".join(f"{_qid(col)}" for col in all_cols)
    keyed_present_sql = (
        f"NOT {_blank_sql('b', 'ein')} AND "
        f"NOT {_blank_sql('b', 'tax_year')} AND "
        f"NOT {_blank_sql('b', 'returntype_norm')}"
    )
    order_terms = [
        "_filing_preference DESC",
        "_row_nonblank_score DESC",
        "_row_priority_score DESC",
    ]
    for col_name in ("TAXPEREND", "OFFICERSIGNDATE"):
        if col_name in all_cols:
            order_terms.append(f"{_qid(col_name)} DESC NULLS LAST")
    for col_name in ("source_filename", "source_dataset_id", "URL"):
        if col_name in all_cols:
            order_terms.append(f"{_qid(col_name)} ASC")
    con = duckdb.connect()
    try:
        con.execute("SET preserve_insertion_order=false")
        con.execute("PRAGMA threads=1")
        con.execute(f"PRAGMA temp_directory={_qstr(str(out_path.parent / '_tmp_duckdb_step06'))}")
        con.execute("PRAGMA max_temp_directory_size='200GiB'")
        con.execute("PRAGMA memory_limit='8GiB'")
        con.execute(f"CREATE OR REPLACE TEMP VIEW base AS SELECT * FROM read_parquet({_qstr(str(temp_out_path))})")

        keyed_groups = con.execute(
            f"""
            SELECT DISTINCT tax_year, returntype_norm
            FROM base b
            WHERE {keyed_present_sql}
            ORDER BY tax_year, returntype_norm
            """
        ).fetchall()

        if out_path.exists():
            out_path.unlink()
        writer: pq.ParquetWriter | None = None
        for tax_year, returntype_norm in keyed_groups:
            tax_year_sql = _qstr("" if tax_year is None else str(tax_year))
            returntype_sql = _qstr("" if returntype_norm is None else str(returntype_norm))
            batch_sql = f"""
            WITH keyed AS (
                SELECT
                    b.*,
                    cast(({filing_preference_sql}) AS BIGINT) AS _filing_preference,
                    cast(({score_sql}) AS BIGINT) AS _row_nonblank_score,
                    cast(({priority_sql}) AS BIGINT) AS _row_priority_score
                FROM base b
                WHERE {keyed_present_sql}
                  AND b.tax_year = {tax_year_sql}
                  AND b.returntype_norm = {returntype_sql}
            ),
            ranked AS (
                SELECT
                    *,
                    row_number() OVER (
                        PARTITION BY {", ".join(_qid(k) for k in KEY_COLS)}
                        ORDER BY {", ".join(order_terms)}
                    ) AS _rn
                FROM keyed
            )
            SELECT {select_cols_sql}
            FROM ranked
            WHERE _rn = 1
            """
            reader = con.execute(batch_sql).to_arrow_reader(batch_size=100_000)
            for batch in reader:
                if batch.num_rows == 0:
                    continue
                if writer is None:
                    writer = pq.ParquetWriter(str(out_path), batch.schema, compression="snappy")
                writer.write_table(pa.Table.from_batches([batch]))

        unkeyed_sql = f"""
        SELECT {select_cols_sql}
        FROM base b
        WHERE NOT ({keyed_present_sql})
        """
        reader = con.execute(unkeyed_sql).to_arrow_reader(batch_size=100_000)
        for batch in reader:
            if batch.num_rows == 0:
                continue
            if writer is None:
                writer = pq.ParquetWriter(str(out_path), batch.schema, compression="snappy")
            writer.write_table(pa.Table.from_batches([batch]))

        if writer is not None:
            writer.close()
        else:
            empty_arrays = [pa.array([], type=pa.string()) for _ in all_cols]
            empty_table = pa.Table.from_arrays(empty_arrays, names=all_cols)
            pq.write_table(empty_table, str(out_path), compression="snappy")
    finally:
        con.close()
        temp_out_path.unlink(missing_ok=True)

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
    write_json(
        manifest_path,
        {
            "created_at_utc": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
            "script": "06_build_basic_allforms_presilver",
            "output_path": str(out_path),
            "rows_output": int(total_rows),
            "columns_output": int(len(output_cols)),
            "combined_roi_key_count": int(combined_roi_keys.height),
            "rows_before_dedupe": int(presilver_counts["rows_before_dedupe"]),
            "basic_geography_admitted_rows": int(presilver_counts["basic_geography_admitted_rows"]),
            "combined_key_admitted_rows": int(presilver_counts["combined_key_admitted_rows"]),
            "both_admitted_rows": int(presilver_counts["both_admitted_rows"]),
            "input_signature": input_signature,
            "build_options": build_options,
        },
    )
    print(f"[basic] Wrote build manifest:     {manifest_path}", flush=True)
    print_elapsed(start, "Step 06")


if __name__ == "__main__":
    main()
