# src/clean_cms/executor.py
#
# Single responsibility: take a SQL string, run it in DuckDB,
# write the output parquet. Knows about files and DuckDB connections
# but nothing about cleaning logic or column strategies.

from __future__ import annotations
import logging
from pathlib import Path

import duckdb

from .utils import describe_table

logger = logging.getLogger(__name__)


def load_source(con: duckdb.DuckDBPyConnection, input_path: Path) -> list[dict]:
    """
    Register the input parquet as a DuckDB view called 'source'.
    Returns the schema as a list of {name, type} dicts.
    """
    con.execute(
        f"CREATE OR REPLACE VIEW source AS SELECT * FROM read_parquet('{input_path}')"
    )
    schema = describe_table(con, "source")
    logger.info("Loaded source: %s (%d columns)", input_path, len(schema))
    return schema


def validate_sql(con: duckdb.DuckDBPyConnection, sql: str) -> None:
    """
    Validate SQL via EXPLAIN without executing. Raises on invalid SQL.
    """
    con.execute(f"EXPLAIN {sql}")
    logger.info("SQL validated via EXPLAIN.")


def write_output(
    con: duckdb.DuckDBPyConnection,
    sql: str,
    output_path: Path,
) -> None:
    """
    Execute the final SQL and write results to a parquet file.
    Creates output directory if it does not exist.
    """
    output_path.parent.mkdir(parents=True, exist_ok=True)
    con.execute(f"COPY ({sql}) TO '{output_path}' (FORMAT PARQUET)")
    logger.info("Written: %s", output_path)


def run(
    input_path: str | Path,
    output_path: str | Path,
    sql: str,
    dry_run: bool = False,
) -> None:
    """
    Main entry point for executor.

    Opens a DuckDB connection, registers the input parquet as a view,
    then either validates the SQL (dry_run=True) or executes it and
    writes the output parquet.

    Args:
        input_path:  Path to the _array.parquet file.
        output_path: Path to write the _clean.parquet file.
        sql:         Final DuckDB SQL string from query_builder.
        dry_run:     If True, validate SQL via EXPLAIN but do not write output.
    """
    input_path = Path(input_path)
    output_path = Path(output_path)

    con = duckdb.connect()

    try:
        load_source(con, input_path)
        logger.info("Final SQL:\n%s", sql)

        if dry_run:
            validate_sql(con, sql)
            logger.info("dry_run=True: skipping write.")
        else:
            write_output(con, sql, output_path)
            logger.info("Done.")
    finally:
        con.close()
