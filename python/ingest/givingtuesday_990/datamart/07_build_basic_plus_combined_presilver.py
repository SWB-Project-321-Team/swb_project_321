"""
Step 07: Build the mixed GT pre-Silver parquet from admitted inputs.

Strategy:
- Base rows = Combined rows admitted before the heavy merge
- Restrict the heavy merge to ROI-eligible keys as early as possible
- Build the admitted key set from the union of:
  - keys already admitted into the Basic pre-Silver stage
  - keys admitted by Combined geography
- Enrich Combined rows from Basic rows using key:
  (ein, tax_year, returntype_norm)
- Keep unmatched admitted Basic rows for admitted keys
- Preserve provenance flags and explicit ROI-admission metadata

Important contract note:
- The script filename is historical for step-number continuity, but the mixed
  output itself is a filtered/admitted pre-Silver artifact.
"""

from __future__ import annotations

import argparse
import time
from pathlib import Path

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq
from tqdm import tqdm

from common import (
    BASIC_ALLFORMS_PARQUET,
    BASIC_PLUS_COMBINED_BUILD_MANIFEST_JSON,
    BASIC_PLUS_COMBINED_PARQUET,
    BASIC_DEDUP_PRIORITY_COLUMNS,
    CATALOG_CSV,
    COMBINED_NORMALIZED_CACHE_MANIFEST_JSON,
    COMBINED_NORMALIZED_CACHE_PARQUET,
    DATE_COLUMNS,
    GEOID_REFERENCE_CSV,
    GT_BASIC_PRESILVER_METADATA_COLUMNS,
    GT_MIXED_ROI_METADATA_COLUMNS,
    NAME_LIKE_COLUMNS,
    PHONE_NUMBER_COLUMNS,
    RAW_DIR,
    STATE_CODE_COLUMNS,
    ZIP_TO_COUNTY_CSV,
    ZIP_CODE_COLUMNS,
    banner,
    build_gt_roi_zip_map,
    build_input_signature,
    ensure_combined_normalized_cache,
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

def _qid(name: str) -> str:
    """Safely quote an identifier for DuckDB SQL."""
    return '"' + name.replace('"', '""') + '"'


def _qstr(text: str) -> str:
    """Safely quote a SQL string literal."""
    return "'" + text.replace("'", "''") + "'"


def _table_columns(con: duckdb.DuckDBPyConnection, table_name: str) -> list[str]:
    """Return ordered column names for a table/view."""
    rows = con.execute(f"PRAGMA table_info({_qstr(table_name)})").fetchall()
    return [r[1] for r in rows]


def _blank_sql(alias: str, col: str) -> str:
    """SQL expression for blank-like check."""
    qcol = f"{alias}.{_qid(col)}"
    return f"({qcol} IS NULL OR trim({qcol}) = '')"


def _trueish_sql(alias: str, col: str) -> str:
    """SQL expression for GT filing flags that use truthy markers like X/Y/1."""
    qcol = f"{alias}.{_qid(col)}"
    normalized = f"upper(trim(coalesce({qcol}, '')))"
    return f"{normalized} IN ('1', 'Y', 'YES', 'TRUE', 'T', 'X')"


def _collapsed_text_sql(alias: str, col: str) -> str:
    """SQL expression that trims and collapses internal whitespace."""
    qcol = f"{alias}.{_qid(col)}"
    return f"trim(regexp_replace(coalesce({qcol}, ''), '\\\\s+', ' ', 'g'))"


def _normalized_name_sql(alias: str, col: str) -> str:
    """SQL expression that canonicalizes name-like fields."""
    collapsed = _collapsed_text_sql(alias, col)
    return f"CASE WHEN {collapsed} = '' THEN '' ELSE upper({collapsed}) END"


def _normalized_state_sql(alias: str, col: str) -> str:
    """SQL expression that canonicalizes state codes."""
    collapsed = _collapsed_text_sql(alias, col)
    return f"CASE WHEN {collapsed} = '' THEN '' ELSE upper({collapsed}) END"


def _normalized_zip_sql(alias: str, col: str) -> str:
    """SQL expression that canonicalizes ZIP codes to the first five digits."""
    qcol = f"{alias}.{_qid(col)}"
    digits = f"regexp_replace(coalesce({qcol}, ''), '[^0-9]', '', 'g')"
    return f"CASE WHEN length({digits}) < 5 THEN '' ELSE substr({digits}, 1, 5) END"


def _normalized_phone_sql(alias: str, col: str) -> str:
    """SQL expression that canonicalizes phone numbers to digits only."""
    qcol = f"{alias}.{_qid(col)}"
    digits = f"regexp_replace(coalesce({qcol}, ''), '[^0-9]', '', 'g')"
    return f"CASE WHEN {digits} = '' THEN '' ELSE {digits} END"


def _normalized_date_sql(alias: str, col: str) -> str:
    """SQL expression that canonicalizes date-like values to YYYY-MM-DD when parseable."""
    collapsed = _collapsed_text_sql(alias, col)
    parsed = (
        f"coalesce("
        f"strftime(try_strptime({collapsed}, '%Y-%m-%d'), '%Y-%m-%d'), "
        f"strftime(try_strptime({collapsed}, '%Y/%m/%d'), '%Y-%m-%d'), "
        f"strftime(try_strptime({collapsed}, '%m/%d/%Y'), '%Y-%m-%d'), "
        f"strftime(try_strptime({collapsed}, '%m-%d-%Y'), '%Y-%m-%d')"
        f")"
    )
    return f"CASE WHEN {collapsed} = '' THEN '' ELSE coalesce({parsed}, {collapsed}) END"


def _combined_normalization_replace_sql(columns: list[str]) -> str:
    """Return DuckDB REPLACE expressions for low-risk Combined normalization."""
    replace_parts: list[str] = []
    for col in NAME_LIKE_COLUMNS:
        if col in columns:
            replace_parts.append(f"{_normalized_name_sql('base', col)} AS {_qid(col)}")
    for col in STATE_CODE_COLUMNS:
        if col in columns:
            replace_parts.append(f"{_normalized_state_sql('base', col)} AS {_qid(col)}")
    for col in ZIP_CODE_COLUMNS:
        if col in columns:
            replace_parts.append(f"{_normalized_zip_sql('base', col)} AS {_qid(col)}")
    for col in PHONE_NUMBER_COLUMNS:
        if col in columns:
            replace_parts.append(f"{_normalized_phone_sql('base', col)} AS {_qid(col)}")
    for col in DATE_COLUMNS:
        if col in columns:
            replace_parts.append(f"{_normalized_date_sql('base', col)} AS {_qid(col)}")
    if not replace_parts:
        return ""
    return " REPLACE (" + ", ".join(replace_parts) + ")"


def main() -> None:
    parser = argparse.ArgumentParser(description="Build the mixed GT pre-Silver parquet from admitted Basic and Combined inputs.")
    parser.add_argument("--catalog-csv", default=str(CATALOG_CSV), help="Catalog CSV path")
    parser.add_argument("--raw-dir", default=str(RAW_DIR), help="Raw files directory")
    parser.add_argument("--zip-to-county", default=str(ZIP_TO_COUNTY_CSV), help="ZIP->county FIPS CSV")
    parser.add_argument("--geoid-csv", default=str(GEOID_REFERENCE_CSV), help="Benchmark GEOID reference CSV")
    parser.add_argument("--tax-year-min", type=int, default=2022, help="Minimum tax year to keep in the ROI-scoped mixed stage")
    parser.add_argument("--tax-year-max", type=int, default=2024, help="Maximum tax year to keep in the ROI-scoped mixed stage")
    parser.add_argument("--basic-parquet", default=str(BASIC_ALLFORMS_PARQUET), help="Input admitted Basic pre-Silver parquet")
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
    parser.add_argument("--output", default=str(BASIC_PLUS_COMBINED_PARQUET), help="Output unified parquet")
    parser.add_argument("--build-manifest-json", default=str(BASIC_PLUS_COMBINED_BUILD_MANIFEST_JSON), help="Local build manifest JSON path")
    parser.add_argument(
        "--duckdb-threads",
        type=int,
        default=1,
        help="DuckDB worker threads for the large merge stage",
    )
    parser.add_argument(
        "--duckdb-memory-limit",
        default="10GiB",
        help="DuckDB memory_limit value for the merge stage",
    )
    parser.add_argument(
        "--duckdb-temp-dir",
        default="",
        help="Optional DuckDB temp directory override; defaults beside the output parquet",
    )
    parser.add_argument(
        "--duckdb-max-temp-directory-size",
        default="200GiB",
        help="DuckDB max_temp_directory_size value for large spill-to-disk merges",
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
    banner("STEP 07 - BUILD BASIC+COMBINED ROI-SCOPED FILE")
    load_env_from_secrets()

    catalog_rows = load_catalog_rows(Path(args.catalog_csv))
    required = select_required_datasets(catalog_rows)
    combined_row = next((r for r in required if r.get("title") == "Combined Forms Datamart"), None)
    if combined_row is None:
        raise RuntimeError("Required dataset not found: Combined Forms Datamart")
    combined_path = Path(args.raw_dir) / url_basename(combined_row.get("download_url", ""))
    if not combined_path.exists():
        raise FileNotFoundError(f"Combined raw file not found: {combined_path}")

    basic_path = Path(args.basic_parquet)
    if not basic_path.exists():
        raise FileNotFoundError(f"Basic-allforms parquet not found: {basic_path}")
    combined_cache_path = Path(args.combined_cache_parquet)
    combined_cache_manifest_path = Path(args.combined_cache_manifest_json)
    geoid_csv = Path(args.geoid_csv)
    zip_to_county_csv = Path(args.zip_to_county)
    geoid_df = load_gt_geoid_reference(geoid_csv)
    zip_df = load_gt_zip_to_fips(zip_to_county_csv)
    roi_zip_df = build_gt_roi_zip_map(zip_df, geoid_df)
    cache_manifest = ensure_combined_normalized_cache(
        combined_csv_path=combined_path,
        output_path=combined_cache_path,
        manifest_path=combined_cache_manifest_path,
        force_rebuild=args.force_rebuild,
    )

    print("[merge] Using DuckDB disk-spilling merge path for large-scale reliability.", flush=True)
    print("[merge] This stage now combines only admitted Basic and Combined rows.", flush=True)
    print(f"[merge] Basic pre-Silver source: {basic_path}", flush=True)
    print(f"[merge] Combined CSV source:     {combined_path}", flush=True)
    print(f"[merge] Combined cache parquet:  {combined_cache_path}", flush=True)
    print(f"[merge] GEOID reference:        {geoid_csv}", flush=True)
    print(f"[merge] ZIP->county source:     {zip_to_county_csv}", flush=True)
    print(f"[merge] Tax year min:          {args.tax_year_min}", flush=True)
    print(f"[merge] Tax year max:          {args.tax_year_max}", flush=True)
    print(f"[merge] Benchmark counties:    {geoid_df.height:,}", flush=True)
    print(f"[merge] ROI ZIP map rows:      {roi_zip_df.height:,}", flush=True)
    print(f"[merge] Skip if unchanged:      {args.skip_if_unchanged}", flush=True)
    print(f"[merge] Force rebuild:         {args.force_rebuild}", flush=True)
    print(f"[merge] DuckDB threads:        {args.duckdb_threads}", flush=True)
    print(f"[merge] DuckDB memory limit:   {args.duckdb_memory_limit}", flush=True)
    print(f"[merge] DuckDB temp limit:     {args.duckdb_max_temp_directory_size}", flush=True)

    out_path = Path(args.output)
    manifest_path = Path(args.build_manifest_json)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    input_signature = build_input_signature(
        {
            "catalog_csv": Path(args.catalog_csv),
            "basic_parquet": basic_path,
            "combined_raw_csv": combined_path,
            "geoid_csv": geoid_csv,
            "zip_to_county_csv": zip_to_county_csv,
        }
    )
    build_options = {
        "output_path": str(out_path),
        "script": "07_build_basic_plus_combined_presilver",
        "combined_cache_parquet": str(combined_cache_path),
        "tax_year_min": int(args.tax_year_min),
        "tax_year_max": int(args.tax_year_max),
        "duckdb_threads": int(args.duckdb_threads),
        "duckdb_memory_limit": str(args.duckdb_memory_limit),
        "duckdb_max_temp_directory_size": str(args.duckdb_max_temp_directory_size),
    }
    if args.skip_if_unchanged and not args.force_rebuild and manifest_matches_expectation(
        manifest_path,
        expected_input_signature=input_signature,
        expected_options=build_options,
        required_outputs=[out_path],
    ):
        cached = read_json(manifest_path)
        print("[merge] Inputs match cached build manifest and output already exists; skipping rebuild.", flush=True)
        print(f"[merge] Cached row count: {cached.get('rows_output', '')}", flush=True)
        print_elapsed(start, "Step 07")
        return

    tmp_dir = Path(args.duckdb_temp_dir) if args.duckdb_temp_dir else out_path.parent / "_tmp_duckdb_step07"
    tmp_dir.mkdir(parents=True, exist_ok=True)
    tmp_db = tmp_dir / "step07.duckdb"
    if tmp_db.exists():
        tmp_db.unlink()
    con = duckdb.connect(str(tmp_db))

    try:
        # These settings are surfaced as CLI arguments so we can tune the
        # heaviest GT merge stage without editing code or weakening the current
        # output contract.
        con.execute(f"PRAGMA temp_directory={_qstr(str(tmp_dir))}")
        con.execute(f"PRAGMA max_temp_directory_size={_qstr(str(args.duckdb_max_temp_directory_size))}")
        con.execute(f"PRAGMA memory_limit={_qstr(str(args.duckdb_memory_limit))}")
        con.execute(f"PRAGMA threads={int(args.duckdb_threads)}")
        con.execute("SET preserve_insertion_order=false")
        con.register("roi_zip_source", roi_zip_df.to_arrow())

        with tqdm(total=11, desc="Step 07 stages", unit="stage") as pbar:
            print("[merge] Loading basic and combined source relations...", flush=True)
            con.execute(f"CREATE OR REPLACE TEMP VIEW basic_src AS SELECT * FROM read_parquet({_qstr(str(basic_path))})")
            con.execute(
                f"CREATE OR REPLACE TEMP VIEW combined_src AS "
                f"SELECT * FROM read_parquet({_qstr(str(combined_cache_path))})"
            )
            con.execute("CREATE OR REPLACE TEMP TABLE roi_zip_map AS SELECT * FROM roi_zip_source")
            combined_src_cols = _table_columns(con, "combined_src")
            pbar.update(1)

            print("[merge] Normalizing combined keys and provenance fields...", flush=True)
            combined_replace_sql = _combined_normalization_replace_sql(combined_src_cols)
            con.execute(
                f"""
                CREATE OR REPLACE TEMP TABLE combined AS
                WITH base AS (
                    SELECT
                        *,
                        regexp_replace(coalesce(FILEREIN, ''), '[^0-9]', '', 'g') AS _ein_digits,
                        trim(coalesce(TAXYEAR, '')) AS _tax_raw,
                        upper(replace(replace(trim(coalesce(RETURNTYPE, '')), '-', ''), ' ', '')) AS _rt_norm
                    FROM combined_src
                )
                SELECT
                    * EXCLUDE (_ein_digits, _tax_raw, _rt_norm){combined_replace_sql},
                    CASE
                        WHEN length(_ein_digits) = 0 THEN ''
                        WHEN length(_ein_digits) < 9 THEN lpad(_ein_digits, 9, '0')
                        ELSE substr(_ein_digits, 1, 9)
                    END AS ein,
                    CASE
                        WHEN _tax_raw = '' THEN ''
                        WHEN try_cast(_tax_raw AS DOUBLE) IS NOT NULL THEN cast(cast(try_cast(_tax_raw AS DOUBLE) AS BIGINT) AS VARCHAR)
                        ELSE _tax_raw
                    END AS tax_year,
                    _rt_norm AS returntype_norm,
                    CASE
                        WHEN _rt_norm LIKE '990EZ%' THEN '990EZ'
                        WHEN _rt_norm LIKE '990PF%' THEN '990PF'
                        WHEN _rt_norm LIKE '990N%' THEN '990N'
                        WHEN _rt_norm LIKE '990%' THEN '990'
                        WHEN _rt_norm = '' THEN 'UNKNOWN'
                        ELSE _rt_norm
                    END AS form_type,
                    {_qstr(combined_row.get('title', ''))} AS source_dataset_title,
                    {_qstr(combined_row.get('dataset_id', ''))} AS source_dataset_id,
                    {_qstr(combined_path.name)} AS source_filename,
                    '1' AS from_combined
                FROM base
                """
            )
            pbar.update(1)

            print("[merge] Building admitted key sets from the filtered Basic stage and the Combined geography stage...", flush=True)
            basic_tax_expr = "try_cast(nullif(trim(coalesce(b.tax_year, '')), '') AS BIGINT)"
            combined_tax_expr = "try_cast(nullif(trim(coalesce(c.tax_year, '')), '') AS BIGINT)"
            basic_src_cols = _table_columns(con, "basic_src")
            missing_basic_metadata = [c for c in GT_BASIC_PRESILVER_METADATA_COLUMNS if c not in basic_src_cols]
            if missing_basic_metadata:
                raise RuntimeError(
                    "Basic pre-Silver input is missing required admission metadata columns: "
                    + ", ".join(missing_basic_metadata)
                )
            con.execute(
                f"""
                CREATE OR REPLACE TEMP TABLE basic_roi_keys AS
                SELECT DISTINCT
                    b.ein,
                    b.tax_year,
                    b.returntype_norm
                FROM basic_src b
                WHERE {basic_tax_expr} >= {int(args.tax_year_min)}
                  AND {basic_tax_expr} <= {int(args.tax_year_max)}
                  AND trim(coalesce(b.presilver_admission_basis, '')) != ''
                """
            )
            con.execute(
                f"""
                CREATE OR REPLACE TEMP TABLE combined_roi_keys AS
                SELECT DISTINCT
                    c.ein,
                    c.tax_year,
                    c.returntype_norm
                FROM combined c
                INNER JOIN roi_zip_map rz
                    ON c.FILERUSZIP = rz.zip5
                WHERE {combined_tax_expr} >= {int(args.tax_year_min)}
                  AND {combined_tax_expr} <= {int(args.tax_year_max)}
                """
            )
            con.execute(
                """
                CREATE OR REPLACE TEMP TABLE roi_keys AS
                WITH unioned AS (
                    SELECT ein, tax_year, returntype_norm, 1 AS admitted_by_basic, 0 AS admitted_by_combined
                    FROM basic_roi_keys
                    UNION ALL
                    SELECT ein, tax_year, returntype_norm, 0 AS admitted_by_basic, 1 AS admitted_by_combined
                    FROM combined_roi_keys
                )
                SELECT
                    ein,
                    tax_year,
                    returntype_norm,
                    CASE WHEN max(admitted_by_basic) = 1 THEN '1' ELSE '0' END AS roi_admitted_by_basic,
                    CASE WHEN max(admitted_by_combined) = 1 THEN '1' ELSE '0' END AS roi_admitted_by_combined,
                    CASE
                        WHEN max(admitted_by_basic) = 1 AND max(admitted_by_combined) = 1 THEN 'both'
                        WHEN max(admitted_by_basic) = 1 THEN 'basic'
                        WHEN max(admitted_by_combined) = 1 THEN 'combined'
                        ELSE ''
                    END AS roi_admission_basis
                FROM unioned
                GROUP BY ein, tax_year, returntype_norm
                """
            )
            roi_counts = con.execute(
                """
                SELECT
                    (SELECT count(*) FROM basic_roi_keys) AS basic_roi_key_count,
                    (SELECT count(*) FROM combined_roi_keys) AS combined_roi_key_count,
                    (SELECT count(*) FROM roi_keys) AS roi_key_union_count
                """
            ).fetchone()
            print(
                "[merge] ROI key counts | "
                f"basic={int(roi_counts[0]):,} combined={int(roi_counts[1]):,} union={int(roi_counts[2]):,}",
                flush=True,
            )
            basic_admission_counts = con.execute(
                """
                SELECT
                    sum(CASE WHEN presilver_admitted_by_basic_geography = '1' THEN 1 ELSE 0 END) AS basic_geography_rows,
                    sum(CASE WHEN presilver_admitted_by_combined_key = '1' THEN 1 ELSE 0 END) AS combined_key_rows,
                    sum(CASE WHEN presilver_admission_basis = 'both' THEN 1 ELSE 0 END) AS both_rows
                FROM basic_src
                """
            ).fetchone()
            print(
                "[merge] Basic pre-Silver admission counts | "
                f"basic_geography={int(basic_admission_counts[0] or 0):,} "
                f"combined_key={int(basic_admission_counts[1] or 0):,} "
                f"both={int(basic_admission_counts[2] or 0):,}",
                flush=True,
            )
            pbar.update(1)

            print("[merge] Deduplicating ROI-scoped Basic and Combined rows by key (deterministic best-row selection)...", flush=True)
            basic_non_key = [c for c in basic_src_cols if c not in KEY_COLS]
            score_cols = [
                c for c in basic_non_key
                if c not in (
                    "source_dataset_id",
                    "source_dataset_title",
                    "source_filename",
                    "from_basic",
                    "from_combined",
                    *GT_BASIC_PRESILVER_METADATA_COLUMNS,
                )
            ]
            score_terms = [f"CASE WHEN {_blank_sql('s', c)} THEN 0 ELSE 1 END" for c in score_cols]
            score_sql = " + ".join(score_terms) if score_terms else "0"
            priority_terms = [
                f"CASE WHEN {_blank_sql('s', c)} THEN 0 ELSE 1 END"
                for c in BASIC_DEDUP_PRIORITY_COLUMNS
                if c in basic_src_cols
            ]
            priority_sql = " + ".join(priority_terms) if priority_terms else "0"
            amended_sql = _trueish_sql("s", "AMENDERETURN") if "AMENDERETURN" in basic_src_cols else "FALSE"
            final_sql = _trueish_sql("s", "FINALRRETURN") if "FINALRRETURN" in basic_src_cols else "FALSE"
            initial_sql = _trueish_sql("s", "INITIARETURN") if "INITIARETURN" in basic_src_cols else "FALSE"
            filing_preference_sql = (
                f"CASE "
                f"WHEN {amended_sql} THEN 3 "
                f"WHEN {final_sql} THEN 2 "
                f"WHEN {initial_sql} THEN 1 "
                f"ELSE 0 END"
            )
            dedup_group = ", ".join(_qid(k) for k in KEY_COLS)
            order_terms = [
                "_filing_preference DESC",
                "_row_nonblank_score DESC",
                "_row_priority_score DESC",
            ]
            for col in ("TAXPEREND", "OFFICERSIGNDATE"):
                if col in basic_src_cols:
                    order_terms.append(f"{_qid(col)} DESC NULLS LAST")
            for col in ("source_filename", "source_dataset_id", "URL"):
                if col in basic_src_cols:
                    order_terms.append(f"{_qid(col)} ASC")
            con.execute(
                f"""
                CREATE OR REPLACE TEMP VIEW basic_dedup AS
                WITH scoped AS (
                    SELECT s.*
                    FROM basic_src s
                    INNER JOIN roi_keys rk
                        USING (ein, tax_year, returntype_norm)
                ),
                scored AS (
                    SELECT
                        s.*,
                        cast(({filing_preference_sql}) AS BIGINT) AS _filing_preference,
                        cast(({score_sql}) AS BIGINT) AS _row_nonblank_score,
                        cast(({priority_sql}) AS BIGINT) AS _row_priority_score
                    FROM scoped s
                )
                SELECT * EXCLUDE (_filing_preference, _row_nonblank_score, _row_priority_score, _rn)
                FROM (
                    SELECT
                        *,
                        row_number() OVER (
                            PARTITION BY {dedup_group}
                            ORDER BY {", ".join(order_terms)}
                        ) AS _rn
                    FROM scored
                )
                WHERE _rn = 1
                """
            )
            con.execute(
                """
                CREATE OR REPLACE TEMP TABLE combined_roi AS
                SELECT
                    c.*,
                    rk.roi_admitted_by_basic,
                    rk.roi_admitted_by_combined,
                    rk.roi_admission_basis
                FROM combined c
                INNER JOIN roi_keys rk
                    USING (ein, tax_year, returntype_norm)
                """
            )
            combined_roi_cols = _table_columns(con, "combined_roi")
            combined_non_key = [c for c in combined_roi_cols if c not in KEY_COLS]
            combined_score_cols = [
                c for c in combined_non_key
                if c not in (
                    "source_dataset_id",
                    "source_dataset_title",
                    "source_filename",
                    "from_basic",
                    "from_combined",
                    *GT_MIXED_ROI_METADATA_COLUMNS,
                )
            ]
            combined_score_terms = [f"CASE WHEN {_blank_sql('c', c)} THEN 0 ELSE 1 END" for c in combined_score_cols]
            combined_score_sql = " + ".join(combined_score_terms) if combined_score_terms else "0"
            combined_priority_terms = [
                f"CASE WHEN {_blank_sql('c', c)} THEN 0 ELSE 1 END"
                for c in BASIC_DEDUP_PRIORITY_COLUMNS
                if c in combined_roi_cols
            ]
            combined_priority_sql = " + ".join(combined_priority_terms) if combined_priority_terms else "0"
            combined_amended_sql = _trueish_sql("c", "AMENDERETURN") if "AMENDERETURN" in combined_roi_cols else "FALSE"
            combined_final_sql = _trueish_sql("c", "FINALRRETURN") if "FINALRRETURN" in combined_roi_cols else "FALSE"
            combined_initial_sql = _trueish_sql("c", "INITIARETURN") if "INITIARETURN" in combined_roi_cols else "FALSE"
            combined_filing_preference_sql = (
                f"CASE "
                f"WHEN {combined_amended_sql} THEN 3 "
                f"WHEN {combined_final_sql} THEN 2 "
                f"WHEN {combined_initial_sql} THEN 1 "
                f"ELSE 0 END"
            )
            combined_order_terms = [
                "_filing_preference DESC",
                "_row_nonblank_score DESC",
                "_row_priority_score DESC",
            ]
            for col in ("TAXPEREND", "OFFICERSIGNDATE"):
                if col in combined_roi_cols:
                    combined_order_terms.append(f"{_qid(col)} DESC NULLS LAST")
            for col in ("source_filename", "source_dataset_id", "URL"):
                if col in combined_roi_cols:
                    combined_order_terms.append(f"{_qid(col)} ASC")
            con.execute(
                f"""
                CREATE OR REPLACE TEMP VIEW combined_dedup AS
                WITH scored AS (
                    SELECT
                        c.*,
                        cast(({combined_filing_preference_sql}) AS BIGINT) AS _filing_preference,
                        cast(({combined_score_sql}) AS BIGINT) AS _row_nonblank_score,
                        cast(({combined_priority_sql}) AS BIGINT) AS _row_priority_score
                    FROM combined_roi c
                )
                SELECT * EXCLUDE (_filing_preference, _row_nonblank_score, _row_priority_score, _rn)
                FROM (
                    SELECT
                        *,
                        row_number() OVER (
                            PARTITION BY {dedup_group}
                            ORDER BY {", ".join(combined_order_terms)}
                        ) AS _rn
                    FROM scored
                )
                WHERE _rn = 1
                """
            )
            scoped_counts = con.execute(
                """
                SELECT
                    (SELECT count(*) FROM basic_dedup) AS basic_scoped_rows,
                    (SELECT count(*) FROM combined_dedup) AS combined_scoped_rows
                """
            ).fetchone()
            print(
                "[merge] ROI-scoped source rows | "
                f"basic_dedup={int(scoped_counts[0]):,} combined_dedup={int(scoped_counts[1]):,}",
                flush=True,
            )
            pbar.update(1)

            combined_cols = _table_columns(con, "combined_dedup")
            basic_cols_all = _table_columns(con, "basic_dedup")
            basic_cols = [c for c in basic_cols_all if c not in KEY_COLS]
            overlap_cols = [c for c in basic_cols if c in combined_cols]
            basic_only_cols = [
                c
                for c in basic_cols
                if c not in combined_cols
                and c not in ("field_fill_count_from_basic",)
                and c not in GT_BASIC_PRESILVER_METADATA_COLUMNS
            ]
            print(f"[merge] Combined cols: {len(combined_cols):,}; Basic cols: {len(basic_cols):,}", flush=True)
            print(f"[merge] Overlap cols for fill: {len(overlap_cols):,}; Basic-only cols retained: {len(basic_only_cols):,}", flush=True)
            pbar.update(1)

            print("[merge] Building merged output (Combined <- Basic fill)...", flush=True)
            merged_select_parts: list[str] = []
            fill_parts: list[str] = []
            roi_override_parts: list[str] = []
            for col in tqdm(combined_cols, desc="Merged select cols", unit="col"):
                if col in overlap_cols:
                    fill_cond = f"{_blank_sql('c', col)} AND NOT {_blank_sql('b', col)}"
                    roi_override_cond = "FALSE"
                    if col in {"FILERUSZIP", "FILERUSSTATE"}:
                        roi_override_cond = (
                            "(c.roi_admitted_by_basic = '1' "
                            "AND c.roi_admitted_by_combined = '0' "
                            f"AND NOT {_blank_sql('b', col)} "
                            f"AND NOT {_blank_sql('c', col)} "
                            f"AND b.{_qid(col)} <> c.{_qid(col)})"
                        )
                    merged_select_parts.append(
                        f"CASE WHEN ({fill_cond}) OR ({roi_override_cond}) THEN b.{_qid(col)} ELSE c.{_qid(col)} END AS {_qid(col)}"
                    )
                    fill_parts.append(f"CASE WHEN {fill_cond} THEN 1 ELSE 0 END")
                    if col in {"FILERUSZIP", "FILERUSSTATE"}:
                        roi_override_parts.append(f"CASE WHEN {roi_override_cond} THEN 1 ELSE 0 END")
                else:
                    merged_select_parts.append(f"c.{_qid(col)} AS {_qid(col)}")
            for col in basic_only_cols:
                merged_select_parts.append(f"b.{_qid(col)} AS {_qid(col)}")
            merged_select_parts.append("CASE WHEN b.source_dataset_id IS NOT NULL THEN '1' ELSE '0' END AS from_basic")
            fill_sum = " + ".join(fill_parts) if fill_parts else "0"
            merged_select_parts.append(f"cast(({fill_sum}) AS VARCHAR) AS field_fill_count_from_basic")
            roi_override_sum = " + ".join(roi_override_parts) if roi_override_parts else "0"
            merged_select_parts.append(
                f"CASE WHEN ({roi_override_sum}) > 0 THEN '1' ELSE '0' END AS roi_geography_override_from_basic"
            )
            merged_select_parts.append(
                f"cast(({roi_override_sum}) AS VARCHAR) AS roi_geography_override_field_count_from_basic"
            )
            pbar.update(1)

            print("[merge] Preparing unmatched Basic anti-join query...", flush=True)
            exclude_candidates = [c for c in ("from_basic", "from_combined", "field_fill_count_from_basic") if c in basic_src_cols]
            exclude_sql = ""
            if exclude_candidates:
                exclude_sql = " EXCLUDE (" + ", ".join(_qid(c) for c in exclude_candidates) + ")"
            pbar.update(1)

            merged_out_cols = list(dict.fromkeys(combined_cols + basic_only_cols + ["from_basic", "field_fill_count_from_basic"]))
            unmatched_out_cols = list(
                dict.fromkeys(
                    [c for c in basic_src_cols if c not in exclude_candidates]
                    + GT_MIXED_ROI_METADATA_COLUMNS
                    + ["from_combined", "from_basic", "field_fill_count_from_basic"]
                )
            )
            merged_out_cols = list(
                dict.fromkeys(
                    merged_out_cols
                    + ["roi_geography_override_from_basic", "roi_geography_override_field_count_from_basic"]
                )
            )
            final_cols = list(dict.fromkeys(merged_out_cols + unmatched_out_cols))
            print(f"[merge] Final union columns: {len(final_cols):,}", flush=True)
            pbar.update(1)

            merged_aligned = [
                (f"m.{_qid(c)} AS {_qid(c)}" if c in merged_out_cols else f"cast(NULL AS VARCHAR) AS {_qid(c)}")
                for c in final_cols
            ]
            unmatched_aligned = [
                (f"u.{_qid(c)} AS {_qid(c)}" if c in unmatched_out_cols else f"cast(NULL AS VARCHAR) AS {_qid(c)}")
                for c in final_cols
            ]

            years = [r[0] for r in con.execute(
                "SELECT DISTINCT tax_year FROM roi_keys ORDER BY tax_year"
            ).fetchall()]
            print(f"[merge] Tax-year groups to process: {len(years):,}", flush=True)
            pbar.update(1)

            print(f"[merge] Writing unified output parquet in tax-year batches: {out_path}", flush=True)
            if out_path.exists():
                out_path.unlink()
            writer: pq.ParquetWriter | None = None
            for tax_year in tqdm(years, desc="Write tax_year groups", unit="year"):
                year_value = "" if tax_year is None else str(tax_year)
                year_lit = _qstr(year_value)
                batch_sql = f"""
                WITH
                combined_y AS (
                    SELECT * FROM combined_dedup WHERE tax_year = {year_lit}
                ),
                basic_y AS (
                    SELECT * FROM basic_dedup WHERE tax_year = {year_lit}
                ),
                merged_y AS (
                    SELECT
                        {', '.join(merged_select_parts)}
                    FROM combined_y c
                    LEFT JOIN basic_y b
                    USING (ein, tax_year, returntype_norm)
                ),
                unmatched_y AS (
                    SELECT
                        b.*{exclude_sql},
                        rk.roi_admitted_by_basic,
                        rk.roi_admitted_by_combined,
                        rk.roi_admission_basis,
                        '0' AS roi_geography_override_from_basic,
                        '0' AS roi_geography_override_field_count_from_basic,
                        '0' AS from_combined,
                        '1' AS from_basic,
                        '0' AS field_fill_count_from_basic
                    FROM basic_y b
                    INNER JOIN roi_keys rk
                        USING (ein, tax_year, returntype_norm)
                    LEFT JOIN (
                        SELECT DISTINCT ein, tax_year, returntype_norm
                        FROM combined_y
                    ) ck
                    USING (ein, tax_year, returntype_norm)
                    WHERE ck.ein IS NULL
                )
                SELECT {', '.join(merged_aligned)} FROM merged_y m
                UNION ALL
                SELECT {', '.join(unmatched_aligned)} FROM unmatched_y u
                """
                reader = con.execute(batch_sql).to_arrow_reader(batch_size=100_000)
                for batch in reader:
                    if batch.num_rows == 0:
                        continue
                    if writer is None:
                        writer = pq.ParquetWriter(str(out_path), batch.schema, compression="snappy")
                    writer.write_table(pa.Table.from_batches([batch]))
            if writer is not None:
                writer.close()
            else:
                empty_arrays = [pa.array([], type=pa.string()) for _ in final_cols]
                empty_table = pa.Table.from_arrays(empty_arrays, names=final_cols)
                pq.write_table(empty_table, str(out_path), compression="snappy")
            pbar.update(1)

            output_rows = con.execute(f"SELECT count(*) FROM read_parquet({_qstr(str(out_path))})").fetchone()[0]
            print("[merge] Completed merged + unmatched union write.", flush=True)
            pbar.update(1)
    finally:
        con.close()
        if tmp_db.exists():
            tmp_db.unlink(missing_ok=True)
        try:
            if tmp_dir.exists() and not any(tmp_dir.iterdir()):
                tmp_dir.rmdir()
        except OSError:
            pass

    print(f"[merge] Wrote unified output: {out_path}", flush=True)
    print(f"[merge] Output rows: {int(output_rows):,}", flush=True)
    print(f"[merge] Output cols: {len(final_cols):,}", flush=True)
    write_json(
        manifest_path,
        {
            "created_at_utc": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
            "script": "07_build_basic_plus_combined_presilver",
            "output_path": str(out_path),
            "rows_output": int(output_rows),
            "columns_output": int(len(final_cols)),
            "tax_year_min": int(args.tax_year_min),
            "tax_year_max": int(args.tax_year_max),
            "target_county_fips_count": int(geoid_df.height),
            "target_zip5_count": int(roi_zip_df.height),
            "roi_basic_key_count": int(roi_counts[0]),
            "roi_combined_key_count": int(roi_counts[1]),
            "roi_key_union_count": int(roi_counts[2]),
            "roi_scoped_basic_rows": int(scoped_counts[0]),
            "roi_scoped_combined_rows": int(scoped_counts[1]),
            "input_signature": input_signature,
            "build_options": build_options,
            "combined_cache_manifest": cache_manifest,
        },
    )
    print(f"[merge] Wrote build manifest: {manifest_path}", flush=True)
    print_elapsed(start, "Step 07")


if __name__ == "__main__":
    main()
