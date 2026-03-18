"""
Step 07: Build unfiltered Basic+Combined unified parquet.

Strategy:
- Base rows = Combined file
- Enrich Combined missing values from Basic rows using key:
  (ein, tax_year, returntype_norm)
- Keep unmatched Basic rows (do not drop Basic-only keys)
- Preserve provenance flags
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
    BASIC_PLUS_COMBINED_PARQUET,
    CATALOG_CSV,
    RAW_DIR,
    banner,
    load_catalog_rows,
    load_env_from_secrets,
    print_elapsed,
    select_required_datasets,
    url_basename,
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


def main() -> None:
    parser = argparse.ArgumentParser(description="Build unified unfiltered Basic+Combined parquet.")
    parser.add_argument("--catalog-csv", default=str(CATALOG_CSV), help="Catalog CSV path")
    parser.add_argument("--raw-dir", default=str(RAW_DIR), help="Raw files directory")
    parser.add_argument("--basic-parquet", default=str(BASIC_ALLFORMS_PARQUET), help="Input basic-allforms parquet")
    parser.add_argument("--output", default=str(BASIC_PLUS_COMBINED_PARQUET), help="Output unified parquet")
    args = parser.parse_args()

    start = time.perf_counter()
    banner("STEP 07 - BUILD BASIC+COMBINED UNFILTERED FILE")
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

    print("[merge] Using DuckDB disk-spilling merge path for large-scale reliability.", flush=True)
    print(f"[merge] Basic parquet source:    {basic_path}", flush=True)
    print(f"[merge] Combined CSV source:     {combined_path}", flush=True)

    out_path = Path(args.output)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    tmp_dir = out_path.parent / "_tmp_duckdb_step07"
    tmp_dir.mkdir(parents=True, exist_ok=True)
    tmp_db = tmp_dir / "step07.duckdb"
    if tmp_db.exists():
        tmp_db.unlink()
    con = duckdb.connect(str(tmp_db))

    try:
        con.execute(f"PRAGMA temp_directory={_qstr(str(tmp_dir))}")
        con.execute("PRAGMA max_temp_directory_size='200GiB'")
        con.execute("PRAGMA memory_limit='10GiB'")
        con.execute("PRAGMA threads=1")
        con.execute("SET preserve_insertion_order=false")

        with tqdm(total=9, desc="Step 07 stages", unit="stage") as pbar:
            print("[merge] Loading basic and combined source relations...", flush=True)
            con.execute(f"CREATE OR REPLACE TEMP VIEW basic_src AS SELECT * FROM read_parquet({_qstr(str(basic_path))})")
            con.execute(
                f"CREATE OR REPLACE TEMP VIEW combined_src AS "
                f"SELECT * FROM read_csv_auto({_qstr(str(combined_path))}, header=true, all_varchar=true, sample_size=-1)"
            )
            pbar.update(1)

            print("[merge] Normalizing combined keys and provenance fields...", flush=True)
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
                    * EXCLUDE (_ein_digits, _tax_raw, _rt_norm),
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

            print("[merge] Deduplicating basic rows by key (grouped any_value, no wide sort spill)...", flush=True)
            basic_src_cols = _table_columns(con, "basic_src")
            basic_non_key = [c for c in basic_src_cols if c not in KEY_COLS]
            dedup_select = ", ".join(
                [_qid(k) for k in KEY_COLS]
                + [f"any_value({_qid(c)}) AS {_qid(c)}" for c in basic_non_key]
            )
            dedup_group = ", ".join(_qid(k) for k in KEY_COLS)
            con.execute(
                f"""
                CREATE OR REPLACE TEMP VIEW basic_dedup AS
                SELECT {dedup_select}
                FROM basic_src
                GROUP BY {dedup_group}
                """
            )
            pbar.update(1)

            combined_cols = _table_columns(con, "combined")
            basic_cols_all = _table_columns(con, "basic_dedup")
            basic_cols = [c for c in basic_cols_all if c not in KEY_COLS]
            overlap_cols = [c for c in basic_cols if c in combined_cols]
            basic_only_cols = [c for c in basic_cols if c not in combined_cols and c not in ("field_fill_count_from_basic",)]
            print(f"[merge] Combined cols: {len(combined_cols):,}; Basic cols: {len(basic_cols):,}", flush=True)
            print(f"[merge] Overlap cols for fill: {len(overlap_cols):,}; Basic-only cols retained: {len(basic_only_cols):,}", flush=True)
            pbar.update(1)

            print("[merge] Building merged output (Combined <- Basic fill)...", flush=True)
            merged_select_parts: list[str] = []
            fill_parts: list[str] = []
            for col in tqdm(combined_cols, desc="Merged select cols", unit="col"):
                if col in overlap_cols:
                    cond = f"{_blank_sql('c', col)} AND NOT {_blank_sql('b', col)}"
                    merged_select_parts.append(
                        f"CASE WHEN {cond} THEN b.{_qid(col)} ELSE c.{_qid(col)} END AS {_qid(col)}"
                    )
                    fill_parts.append(f"CASE WHEN {cond} THEN 1 ELSE 0 END")
                else:
                    merged_select_parts.append(f"c.{_qid(col)} AS {_qid(col)}")
            for col in basic_only_cols:
                merged_select_parts.append(f"b.{_qid(col)} AS {_qid(col)}")
            merged_select_parts.append("CASE WHEN b.source_dataset_id IS NOT NULL THEN '1' ELSE '0' END AS from_basic")
            fill_sum = " + ".join(fill_parts) if fill_parts else "0"
            merged_select_parts.append(f"cast(({fill_sum}) AS VARCHAR) AS field_fill_count_from_basic")
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
                    + ["from_combined", "from_basic", "field_fill_count_from_basic"]
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
                "SELECT DISTINCT tax_year FROM (SELECT tax_year FROM combined UNION SELECT tax_year FROM basic_dedup) ORDER BY tax_year"
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
                    SELECT * FROM combined WHERE tax_year = {year_lit}
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
                        '0' AS from_combined,
                        '1' AS from_basic,
                        '0' AS field_fill_count_from_basic
                    FROM basic_y b
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
                reader = con.execute(batch_sql).fetch_record_batch(rows_per_batch=100_000)
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
    print_elapsed(start, "Step 07")


if __name__ == "__main__":
    main()
