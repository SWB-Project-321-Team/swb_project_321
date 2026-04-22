"""
Step 12 (ad hoc utility): compare raw Basic CSVs to the step-06 Basic-allforms parquet.

This audit is row-preserving. It checks whether the combined Basic-allforms output
kept the same row counts as the three raw Basic files and summarizes which
overlapping raw columns changed after upstream normalization.
"""

from __future__ import annotations

import argparse
import time
from pathlib import Path

import duckdb

from common import (
    BASIC_ALLFORMS_PARQUET,
    BASIC_RAW_VS_COMBINED_SAMPLES_CSV,
    BASIC_RAW_VS_COMBINED_SUMMARY_CSV,
    CATALOG_CSV,
    RAW_DIR,
    banner,
    ensure_dirs,
    load_catalog_rows,
    load_env_from_secrets,
    print_elapsed,
    select_required_datasets,
    url_basename,
    write_csv,
)


def _required_basic_rows(catalog_rows: list[dict[str, str]]) -> list[dict[str, str]]:
    """Subset required rows to the three Basic datasets."""
    required = select_required_datasets(catalog_rows)
    return [row for row in required if row.get("title", "") == "Basic Fields"]


def _qstr(text: str) -> str:
    """Safely quote a SQL string literal."""
    return "'" + text.replace("'", "''") + "'"


def _qid(name: str) -> str:
    """Safely quote a SQL identifier."""
    return '"' + name.replace('"', '""') + '"'


def _table_columns(con: duckdb.DuckDBPyConnection, table_name: str) -> list[str]:
    """Return ordered column names for one DuckDB table/view."""
    rows = con.execute(f"PRAGMA table_info({_qstr(table_name)})").fetchall()
    return [row[1] for row in rows]


def main() -> None:
    parser = argparse.ArgumentParser(description="Audit raw Basic GT CSVs vs the step-06 Basic-allforms parquet.")
    parser.add_argument("--catalog-csv", default=str(CATALOG_CSV), help="Catalog CSV path")
    parser.add_argument("--raw-dir", default=str(RAW_DIR), help="Raw files directory")
    parser.add_argument("--basic-parquet", default=str(BASIC_ALLFORMS_PARQUET), help="Basic-allforms parquet path")
    parser.add_argument("--summary-csv", default=str(BASIC_RAW_VS_COMBINED_SUMMARY_CSV), help="Summary CSV output path")
    parser.add_argument("--samples-csv", default=str(BASIC_RAW_VS_COMBINED_SAMPLES_CSV), help="Sample-differences CSV output path")
    parser.add_argument("--sample-limit", type=int, default=5, help="Max sample differences to write per form/column")
    args = parser.parse_args()

    start = time.perf_counter()
    banner("STEP 12 - AUDIT RAW BASIC VS BASIC-ALLFORMS")
    load_env_from_secrets()
    ensure_dirs()

    basic_path = Path(args.basic_parquet)
    if not basic_path.exists():
        raise FileNotFoundError(f"Basic-allforms parquet not found: {basic_path}")

    catalog_rows = load_catalog_rows(Path(args.catalog_csv))
    basic_rows = _required_basic_rows(catalog_rows)
    if len(basic_rows) != 3:
        raise RuntimeError(f"Expected 3 required Basic rows, found {len(basic_rows)}")

    summary_rows: list[dict[str, object]] = []
    sample_rows: list[dict[str, object]] = []

    con = duckdb.connect()
    try:
        con.execute("PRAGMA threads=8")
        con.execute(f"CREATE OR REPLACE TEMP VIEW basic_out AS SELECT * FROM read_parquet({_qstr(str(basic_path))})")
        built_cols = _table_columns(con, "basic_out")

        for row in basic_rows:
            form_type = row.get("form_type", "")
            raw_path = Path(args.raw_dir) / url_basename(row.get("download_url", ""))
            if not raw_path.exists():
                raise FileNotFoundError(f"Missing raw Basic file: {raw_path}")

            print(f"[audit] Comparing raw {form_type} file: {raw_path.name}", flush=True)
            con.execute(
                f"CREATE OR REPLACE TEMP VIEW raw_src AS "
                f"SELECT * FROM read_csv_auto({_qstr(str(raw_path))}, header=true, all_varchar=true, sample_size=-1)"
            )
            raw_cols = _table_columns(con, "raw_src")
            compare_cols = [col for col in raw_cols if col in built_cols]

            con.execute(
                f"""
                CREATE OR REPLACE TEMP VIEW raw_ranked AS
                SELECT *, row_number() OVER () AS _row_id
                FROM raw_src
                """
            )
            con.execute(
                f"""
                CREATE OR REPLACE TEMP VIEW built_ranked AS
                SELECT *, row_number() OVER () AS _row_id
                FROM basic_out
                WHERE form_type = {_qstr(form_type)}
                  AND source_filename = {_qstr(raw_path.name)}
                """
            )

            raw_row_count = con.execute("SELECT count(*) FROM raw_ranked").fetchone()[0]
            built_row_count = con.execute("SELECT count(*) FROM built_ranked").fetchone()[0]
            if raw_row_count != built_row_count:
                raise RuntimeError(
                    f"Row-count mismatch for {form_type}: raw={raw_row_count} built={built_row_count}"
                )

            for col_name in compare_cols:
                qcol = _qid(col_name)
                metrics = con.execute(
                    f"""
                    SELECT
                        count(*) AS compared_rows,
                        sum(CASE WHEN trim(coalesce(r.{qcol}, '')) != trim(coalesce(b.{qcol}, '')) THEN 1 ELSE 0 END) AS changed_rows,
                        sum(CASE WHEN trim(coalesce(r.{qcol}, '')) != '' AND trim(coalesce(b.{qcol}, '')) = '' THEN 1 ELSE 0 END) AS raw_nonblank_built_blank,
                        sum(CASE WHEN trim(coalesce(r.{qcol}, '')) = '' AND trim(coalesce(b.{qcol}, '')) != '' THEN 1 ELSE 0 END) AS raw_blank_built_nonblank
                    FROM raw_ranked r
                    INNER JOIN built_ranked b USING (_row_id)
                    """
                ).fetchone()
                compared_rows = int(metrics[0] or 0)
                changed_rows = int(metrics[1] or 0)
                raw_nonblank_built_blank = int(metrics[2] or 0)
                raw_blank_built_nonblank = int(metrics[3] or 0)

                summary_rows.append(
                    {
                        "form_type": form_type,
                        "raw_filename": raw_path.name,
                        "column_name": col_name,
                        "raw_rows": raw_row_count,
                        "built_rows": built_row_count,
                        "compared_rows": compared_rows,
                        "identical_rows": compared_rows - changed_rows,
                        "changed_rows": changed_rows,
                        "raw_nonblank_built_blank": raw_nonblank_built_blank,
                        "raw_blank_built_nonblank": raw_blank_built_nonblank,
                    }
                )

                if changed_rows == 0:
                    continue

                sample_sql = f"""
                    SELECT
                        coalesce(r.FILEREIN, '') AS filerein,
                        coalesce(r.TAXYEAR, '') AS taxyear,
                        coalesce(r.RETURNTYPE, '') AS returntype,
                        coalesce(r.{qcol}, '') AS raw_value,
                        coalesce(b.{qcol}, '') AS built_value
                    FROM raw_ranked r
                    INNER JOIN built_ranked b USING (_row_id)
                    WHERE trim(coalesce(r.{qcol}, '')) != trim(coalesce(b.{qcol}, ''))
                    LIMIT {int(args.sample_limit)}
                """
                for sample in con.execute(sample_sql).fetchall():
                    sample_rows.append(
                        {
                            "form_type": form_type,
                            "raw_filename": raw_path.name,
                            "column_name": col_name,
                            "filerein": sample[0],
                            "taxyear": sample[1],
                            "returntype": sample[2],
                            "raw_value": sample[3],
                            "built_value": sample[4],
                        }
                    )
    finally:
        con.close()

    summary_rows.sort(key=lambda row: (str(row["form_type"]), -int(row["changed_rows"]), str(row["column_name"])))
    sample_rows.sort(key=lambda row: (str(row["form_type"]), str(row["column_name"]), str(row["filerein"]), str(row["taxyear"])))

    write_csv(
        Path(args.summary_csv),
        summary_rows,
        [
            "form_type",
            "raw_filename",
            "column_name",
            "raw_rows",
            "built_rows",
            "compared_rows",
            "identical_rows",
            "changed_rows",
            "raw_nonblank_built_blank",
            "raw_blank_built_nonblank",
        ],
    )
    write_csv(
        Path(args.samples_csv),
        sample_rows,
        [
            "form_type",
            "raw_filename",
            "column_name",
            "filerein",
            "taxyear",
            "returntype",
            "raw_value",
            "built_value",
        ],
    )
    print_elapsed(start, "Step 12")


if __name__ == "__main__":
    main()
