"""
Step 08: Build one or both benchmark-filtered GT output artifacts locally.

Outputs:
- givingtuesday_990_basic_allforms_benchmark.parquet
- givingtuesday_990_filings_benchmark.parquet
- one manifest JSON per emitted artifact

Design note:
- the mixed input from step 07 is now already ROI-scoped
- step 08 keeps the canonical Silver mixed output contract stable by validating
  and finalizing that ROI-scoped stage, then dropping internal ROI-only
  metadata columns from the final mixed Silver parquet
"""

from __future__ import annotations

import argparse
import time
from decimal import Decimal, InvalidOperation
from pathlib import Path

import polars as pl
from tqdm import tqdm

from common import (
    FILTER_TARGET_BASIC,
    FILTER_TARGET_MIXED,
    GEOID_REFERENCE_CSV,
    GT_BASIC_PRESILVER_METADATA_COLUMNS,
    ZIP_TO_COUNTY_CSV,
    banner,
    build_gt_roi_zip_map,
    build_input_signature,
    emit_targets,
    FILTERED_BASIC_SCHEMA_ARTIFACT,
    FILTERED_MIXED_SCHEMA_ARTIFACT,
    GT_MIXED_ROI_METADATA_COLUMNS,
    gt_normalize_tax_year_expr,
    gt_normalize_zip5_expr,
    load_gt_geoid_reference,
    load_gt_zip_to_fips,
    manifest_matches_expectation,
    print_elapsed,
    read_json,
    stale_output_warnings,
    write_schema_snapshot,
    write_json,
)


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


def _parse_decimal_text(value: object) -> Decimal | None:
    """Parse a source-faithful amount string into Decimal when possible."""
    raw = "" if value is None else str(value).strip()
    if not raw or raw.upper() == "NA":
        return None
    cleaned = raw.replace(",", "").replace("$", "")
    if cleaned.startswith("(") and cleaned.endswith(")"):
        cleaned = "-" + cleaned[1:-1]
    try:
        return Decimal(cleaned)
    except InvalidOperation:
        return None


def _format_decimal_text(value: Decimal) -> str:
    """Format a Decimal as a compact plain numeric string."""
    text = format(value, "f")
    if "." in text:
        text = text.rstrip("0").rstrip(".")
    return "0" if text == "-0" else text


def _derive_income_amount(row: dict[str, object]) -> str:
    """Return GT income as revenue minus expenses when both are parseable."""
    revenue_amount = _parse_decimal_text(row.get("__harm_revenue_amount"))
    expense_amount = _parse_decimal_text(row.get("__harm_expenses_amount"))
    if revenue_amount is None or expense_amount is None:
        return ""
    return _format_decimal_text(revenue_amount - expense_amount)


def _derive_income_provenance(row: dict[str, object]) -> str:
    """Return the explicit GT derivation provenance label for derived income."""
    derived_value = "" if row.get("derived_income_amount") is None else str(row.get("derived_income_amount")).strip()
    if not derived_value:
        return ""
    revenue_source = "" if row.get("__harm_revenue_source_column") is None else str(row.get("__harm_revenue_source_column")).strip()
    expense_source = "" if row.get("__harm_expense_source_column") is None else str(row.get("__harm_expense_source_column")).strip()
    if not revenue_source or not expense_source:
        return ""
    return f"derived:{revenue_source}-minus-{expense_source}"


def _filter_one_target(
    *,
    target_id: str,
    input_path: Path,
    output_path: Path,
    manifest_path: Path,
    geoid_df: pl.DataFrame,
    roi_zip_df: pl.DataFrame,
    geoid_csv: Path,
    zip_to_county_csv: Path,
    tax_year_min: int,
    tax_year_max: int,
    skip_if_unchanged: bool,
    force_rebuild: bool,
) -> None:
    """
    Filter one GT input parquet to one benchmark output artifact.

    The mixed target is now expected to arrive from step 07 as ROI-scoped
    content already. This step still applies the canonical geography join so the
    Silver artifact remains explicitly county/region-resolved and fully
    validated against the benchmark map.
    """
    if not input_path.exists():
        raise FileNotFoundError(f"Input parquet not found: {input_path}")

    input_signature = build_input_signature(
        {
            "input_parquet": input_path,
            "geoid_csv": geoid_csv,
            "zip_to_county_csv": zip_to_county_csv,
        }
    )
    build_options = {
        "target_id": target_id,
        "tax_year_min": int(tax_year_min),
        "tax_year_max": int(tax_year_max),
    }
    if skip_if_unchanged and not force_rebuild and manifest_matches_expectation(
        manifest_path,
        expected_input_signature=input_signature,
        expected_options=build_options,
        required_outputs=[output_path],
    ):
        cached = read_json(manifest_path)
        print(f"[filter:{target_id}] Inputs match cached manifest and output already exists; skipping rebuild.", flush=True)
        print(f"[filter:{target_id}] Cached row count: {cached.get('rows_output', '')}", flush=True)
        return

    output_path.parent.mkdir(parents=True, exist_ok=True)
    input_lf = pl.scan_parquet(str(input_path))
    input_cols = input_lf.collect_schema().names()
    zip_col_used = _pick_zip_column(input_cols)
    tax_col = "tax_year" if "tax_year" in input_cols else ("TAXYEAR" if "TAXYEAR" in input_cols else "")
    if tax_col:
        tax_num_expr = gt_normalize_tax_year_expr(pl.col(tax_col)).cast(pl.Int64, strict=False).alias("__tax_num")
    else:
        tax_num_expr = pl.lit(None, dtype=pl.Int64).alias("__tax_num")
    rows_summary = (
        input_lf.with_columns([tax_num_expr])
        .select(
            [
                pl.len().alias("rows_input"),
                pl.when((pl.col("__tax_num") >= tax_year_min) & (pl.col("__tax_num") <= tax_year_max)).then(1).otherwise(0).sum().alias("rows_after_tax_filter"),
            ]
        )
        .collect()
        .row(0, named=True)
    )
    rows_input = int(rows_summary["rows_input"])
    rows_after_tax = int(rows_summary["rows_after_tax_filter"])

    if target_id == "mixed":
        mixed_input_schema = set(input_cols)
        if {"roi_admitted_by_basic", "roi_admitted_by_combined", "roi_admission_basis"}.issubset(mixed_input_schema):
            admission_counts = (
                input_lf.select(
                    [
                        pl.len().alias("rows_input"),
                        pl.col("roi_admitted_by_basic").cast(pl.Utf8, strict=False).eq("1").sum().alias("roi_admitted_by_basic_count"),
                        pl.col("roi_admitted_by_combined").cast(pl.Utf8, strict=False).eq("1").sum().alias("roi_admitted_by_combined_count"),
                        pl.col("roi_admission_basis").cast(pl.Utf8, strict=False).eq("both").sum().alias("roi_admission_basis_both_count"),
                    ]
                )
                .collect()
                .row(0, named=True)
            )
            print(
                f"[filter:{target_id}] ROI-scoped mixed input admission counts | "
                f"basic={int(admission_counts['roi_admitted_by_basic_count']):,} "
                f"combined={int(admission_counts['roi_admitted_by_combined_count']):,} "
                f"both={int(admission_counts['roi_admission_basis_both_count']):,}",
                flush=True,
            )

    tax_filtered_lf = (
        input_lf.with_columns([tax_num_expr])
        .filter((pl.col("__tax_num") >= tax_year_min) & (pl.col("__tax_num") <= tax_year_max))
        .drop("__tax_num")
    )
    geo_joined_lf = (
        tax_filtered_lf.with_columns([gt_normalize_zip5_expr(pl.col(zip_col_used)).alias("zip5")])
        .join(roi_zip_df.lazy(), on="zip5", how="inner")
    )
    geo_filtered_lf = geo_joined_lf.filter(pl.col("county_fips").is_not_null() & pl.col("region").is_not_null())
    form_type_expr = pl.col("form_type").cast(pl.Utf8, strict=False).fill_null("")
    input_schema = set(input_cols)

    def amount_expr(column_name: str) -> pl.Expr:
        """Return a source amount expression, or blank when an older fixture lacks the column."""
        if column_name in input_schema:
            return pl.col(column_name)
        return pl.lit("")

    # Keep the income derivation source-faithful by using PF-specific GT
    # standard-field columns for Form 990PF. The earlier non-EZ fallback used
    # the 990 columns for PF too, which left PF revenue and expense blank even
    # though the raw PF extract carries its own total revenue/expense fields.
    revenue_expr = (
        pl.when(form_type_expr.eq("990EZ"))
        .then(amount_expr("TOTALRREVENU"))
        .when(form_type_expr.eq("990PF"))
        .then(amount_expr("ANREEXTOREEX"))
        .otherwise(amount_expr("TOTREVCURYEA"))
    )
    expense_expr = (
        pl.when(form_type_expr.eq("990EZ"))
        .then(amount_expr("TOTALEEXPENS"))
        .when(form_type_expr.eq("990PF"))
        .then(amount_expr("ARETEREXPNSS"))
        .otherwise(amount_expr("TOTEXPCURYEA"))
    )
    revenue_source_expr = (
        pl.when(form_type_expr.eq("990EZ"))
        .then(pl.lit("TOTALRREVENU"))
        .when(form_type_expr.eq("990PF"))
        .then(pl.lit("ANREEXTOREEX"))
        .otherwise(pl.lit("TOTREVCURYEA"))
    )
    expense_source_expr = (
        pl.when(form_type_expr.eq("990EZ"))
        .then(pl.lit("TOTALEEXPENS"))
        .when(form_type_expr.eq("990PF"))
        .then(pl.lit("ARETEREXPNSS"))
        .otherwise(pl.lit("TOTEXPCURYEA"))
    )
    geo_filtered_lf = geo_filtered_lf.with_columns(
        [
            revenue_expr.cast(pl.Utf8, strict=False).fill_null("").alias("__harm_revenue_amount"),
            expense_expr.cast(pl.Utf8, strict=False).fill_null("").alias("__harm_expenses_amount"),
            revenue_source_expr.alias("__harm_revenue_source_column"),
            expense_source_expr.alias("__harm_expense_source_column"),
        ]
    )
    geo_filtered_lf = geo_filtered_lf.with_columns(
        [
            pl.struct(["__harm_revenue_amount", "__harm_expenses_amount"])
            .map_elements(_derive_income_amount, return_dtype=pl.Utf8)
            .alias("derived_income_amount"),
        ]
    )
    geo_filtered_lf = geo_filtered_lf.with_columns(
        [
            pl.struct(
                [
                    "__harm_revenue_source_column",
                    "__harm_expense_source_column",
                    "derived_income_amount",
                ]
            )
            .map_elements(_derive_income_provenance, return_dtype=pl.Utf8)
            .alias("derived_income_derivation"),
        ]
    )
    geo_filtered_lf = geo_filtered_lf.drop(
        [
            "__harm_revenue_amount",
            "__harm_expenses_amount",
            "__harm_revenue_source_column",
            "__harm_expense_source_column",
        ]
    )
    if target_id == "basic":
        drop_cols = [col for col in GT_BASIC_PRESILVER_METADATA_COLUMNS if col in geo_filtered_lf.collect_schema().names()]
        if drop_cols:
            print(
                f"[filter:{target_id}] Dropping internal pre-Silver Basic admission columns from canonical Silver output: {', '.join(drop_cols)}",
                flush=True,
            )
            geo_filtered_lf = geo_filtered_lf.drop(drop_cols)
    if target_id == "mixed":
        drop_cols = [col for col in GT_MIXED_ROI_METADATA_COLUMNS if col in geo_filtered_lf.collect_schema().names()]
        if drop_cols:
            print(
                f"[filter:{target_id}] Dropping internal ROI metadata columns from canonical Silver output: {', '.join(drop_cols)}",
                flush=True,
            )
            geo_filtered_lf = geo_filtered_lf.drop(drop_cols)

    output_cols = geo_filtered_lf.collect_schema().names()
    final_lf = _align_columns_as_string(geo_filtered_lf, output_cols)
    try:
        final_lf.sink_parquet(str(output_path), compression="snappy")
    except Exception as exc:
        print(
            f"[filter:{target_id}] sink_parquet unavailable ({type(exc).__name__}), "
            "falling back to collect().write_parquet()",
            flush=True,
        )
        final_lf.collect().write_parquet(str(output_path), compression="snappy")

    rows_output = pl.scan_parquet(str(output_path)).select(pl.len().alias("n")).collect().item(0, 0)
    manifest = {
        "created_at_utc": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "target_id": target_id,
        "input_path": str(input_path),
        "output_path": str(output_path),
        "manifest_path": str(manifest_path),
        "tax_year_min": int(tax_year_min),
        "tax_year_max": int(tax_year_max),
        "zip_column_used": zip_col_used or "",
        "rows_input": int(rows_input),
        "rows_after_tax_filter": int(rows_after_tax),
        "rows_after_geo_filter": int(rows_output),
        "rows_output": int(rows_output),
        "target_county_fips_count": int(geoid_df.height),
        "target_zip5_count": int(roi_zip_df.height),
        "input_signature": input_signature,
        "build_options": build_options,
    }
    write_json(manifest_path, manifest)
    if target_id == "basic":
        schema_artifact = FILTERED_BASIC_SCHEMA_ARTIFACT
    else:
        schema_artifact = FILTERED_MIXED_SCHEMA_ARTIFACT
    write_schema_snapshot(
        schema_artifact.local_path,
        artifact_id=schema_artifact.artifact_id,
        output_path=output_path,
        columns=output_cols,
    )
    print(
        f"[filter:{target_id}] Wrote {output_path.name} | rows_input={int(rows_input):,} "
        f"rows_after_tax={int(rows_after_tax):,} rows_output={int(rows_output):,}",
        flush=True,
    )
    print(f"[filter:{target_id}] Wrote manifest: {manifest_path}", flush=True)
    print(f"[filter:{target_id}] Wrote schema snapshot: {schema_artifact.local_path}", flush=True)


def main() -> None:
    parser = argparse.ArgumentParser(description="Build one or both GT benchmark-filtered output artifacts locally.")
    parser.add_argument("--zip-to-county", default=str(ZIP_TO_COUNTY_CSV), help="ZIP->county FIPS CSV")
    parser.add_argument("--geoid-csv", default=str(GEOID_REFERENCE_CSV), help="GEOID reference CSV")
    parser.add_argument("--tax-year-min", type=int, default=2022, help="Minimum tax year to keep")
    parser.add_argument("--tax-year-max", type=int, default=2024, help="Maximum tax year to keep")
    parser.add_argument(
        "--emit",
        default="both",
        choices=sorted(["basic", "mixed", "both"]),
        help="Which filtered outputs to emit",
    )
    parser.add_argument(
        "--skip-if-unchanged",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="Skip rebuilding targets whose cached manifest still matches current inputs",
    )
    parser.add_argument("--force-rebuild", action="store_true", help="Rebuild even when the cached manifest matches")
    parser.add_argument(
        "--batch-size",
        type=int,
        default=200_000,
        help="Legacy compatibility argument (not used in Polars lazy mode)",
    )
    args = parser.parse_args()

    start = time.perf_counter()
    banner("STEP 08 - FILTER BENCHMARK OUTPUTS LOCAL")
    print(f"[filter] Emit mode:          {args.emit}", flush=True)
    print(f"[filter] Skip if unchanged: {args.skip_if_unchanged}", flush=True)
    print(f"[filter] Force rebuild:     {args.force_rebuild}", flush=True)
    print(f"[filter] Tax year min:      {args.tax_year_min}", flush=True)
    print(f"[filter] Tax year max:      {args.tax_year_max}", flush=True)
    print(f"[filter] batch-size argument is ignored in Polars mode: {args.batch_size}", flush=True)
    for warning in stale_output_warnings(args.emit):
        print(f"[filter] Warning: {warning}", flush=True)

    geoid_csv = Path(args.geoid_csv)
    zip_to_county_csv = Path(args.zip_to_county)
    geoid_df = load_gt_geoid_reference(geoid_csv)
    zip_df = load_gt_zip_to_fips(zip_to_county_csv)
    roi_zip_df = build_gt_roi_zip_map(zip_df, geoid_df)
    print(f"[filter] Target benchmark counties: {geoid_df.height}", flush=True)
    print(f"[filter] ZIP->FIPS map size:       {zip_df.height:,}", flush=True)
    print(f"[filter] ROI ZIP map size:        {roi_zip_df.height:,}", flush=True)

    target_by_id = {
        FILTER_TARGET_BASIC.target_id: FILTER_TARGET_BASIC,
        FILTER_TARGET_MIXED.target_id: FILTER_TARGET_MIXED,
    }
    for target_id in tqdm(emit_targets(args.emit), desc="filter GT targets", unit="target"):
        target = target_by_id[target_id]
        _filter_one_target(
            target_id=target.target_id,
            input_path=target.input_path,
            output_path=target.output_artifact.local_path,
            manifest_path=target.manifest_artifact.local_path,
            geoid_df=geoid_df,
            roi_zip_df=roi_zip_df,
            geoid_csv=geoid_csv,
            zip_to_county_csv=zip_to_county_csv,
            tax_year_min=args.tax_year_min,
            tax_year_max=args.tax_year_max,
            skip_if_unchanged=args.skip_if_unchanged,
            force_rebuild=args.force_rebuild,
        )

    print_elapsed(start, "Step 08")


if __name__ == "__main__":
    main()
