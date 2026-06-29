# src/clean_cms/utils.py
from __future__ import annotations
import logging
from pathlib import Path

import duckdb
import yaml

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


# ---------------------------------------------------------------------------
# Filename helpers
#
# Sharded mode:   one file per year  → {table}_{year}_{suffix}.parquet
# Unsharded mode: one file per range → {table}_{year_min}_{year_max}_{suffix}.parquet
# ---------------------------------------------------------------------------

def _year_stem(table: str, year_min: int, year_max: int, sharded: bool) -> str:
    """Base name fragment that encodes the year(s)."""
    if sharded:
        return f"{table}_{year_min}"
    return f"{table}_{year_min}_{year_max}"


def build_input_filename(table: str, year_min: int, year_max: int, sharded: bool = False) -> str:
    """Array-mode input: {stem}_array.parquet."""
    return f"{_year_stem(table, year_min, year_max, sharded)}_array.parquet"


def build_flat_input_filename(table: str, year_min: int, year_max: int, sharded: bool = False) -> str:
    """Flat-mode input (no suffix): {stem}.parquet."""
    return f"{_year_stem(table, year_min, year_max, sharded)}.parquet"


def build_intermediate_filename(
    table: str, year_min: int, year_max: int, sharded: bool, stage: str
) -> str:
    """Intermediate file for a given pipeline stage: {stem}_{stage}.parquet."""
    return f"{_year_stem(table, year_min, year_max, sharded)}_{stage}.parquet"


def build_output_filename(table: str, year_min: int, year_max: int, sharded: bool = False) -> str:
    """Final clean output: {stem}_clean.parquet."""
    return f"{_year_stem(table, year_min, year_max, sharded)}_clean.parquet"


# ---------------------------------------------------------------------------
# Run context loader
# ---------------------------------------------------------------------------

def load_run_context(cfg) -> dict:
    """
    Merge the Hydra runner config with the per-table cleaning YAML.

    Returns a dict with keys:
        table, year_min, year_max, sharded, dry_run, cleaning_config
    """
    table = cfg.table
    year_min = int(cfg.year_min)
    year_max = int(cfg.year_max)
    sharded = bool(cfg.get("sharded", False))
    dry_run = bool(cfg.get("dry_run", False))

    cleaning_config_path = Path("conf/cleaning") / f"{table}.yaml"
    if not cleaning_config_path.exists():
        raise FileNotFoundError(
            f"No cleaning config found for table '{table}' at {cleaning_config_path}"
        )
    with open(cleaning_config_path) as f:
        cleaning_config = yaml.safe_load(f)

    return {
        "table": table,
        "year_min": year_min,
        "year_max": year_max,
        "sharded": sharded,
        "dry_run": dry_run,
        "cleaning_config": cleaning_config,
    }
