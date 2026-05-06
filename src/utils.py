# src/clean_cms/utils.py
from __future__ import annotations
import logging
import duckdb

logger = logging.getLogger(__name__)


def is_array_type(dtype: str) -> bool:
    dtype_upper = dtype.upper()
    return "[]" in dtype_upper or dtype_upper.startswith("LIST")


def describe_table(con: duckdb.DuckDBPyConnection, table_name: str) -> list[dict]:
    rows = con.execute(f"DESCRIBE {table_name}").fetchall()
    return [{"name": row[0], "type": row[1]} for row in rows]


def check_schema_coverage(
    schema: list[dict],
    primary_key: list[str],
    variables_conf: dict,
    default_strategy: str | None,
) -> None:
    uncovered = []
    for col in schema:
        name = col["name"]
        if name in primary_key:
            continue
        if name.startswith("n_distinct_"):
            continue
        if not is_array_type(col["type"]):
            continue
        if name not in variables_conf and default_strategy is None:
            uncovered.append(name)
    if uncovered:
        logger.warning(
            "Array columns with no strategy and no default_strategy "
            "(will error during query building): %s", uncovered,
        )


def setup_logging(log_level: str = "INFO") -> None:
    numeric_level = getattr(logging, log_level.upper(), logging.INFO)
    logging.basicConfig(
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
        level=numeric_level,
    )


def build_input_filename(table: str, year_min: int, year_max: int) -> str:
    return f"{table}_{year_min}_{year_max}_array.parquet"


def build_output_filename(table: str, year_min: int, year_max: int) -> str:
    return f"{table}_{year_min}_{year_max}_clean.parquet"
