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


def build_input_filename(
    table: str,
    year_min: int,
    year_max: int,
    shard: str | None = None,
    aggregated: bool = True,
) -> str:
    """
    Mirror the flat-file naming produced by normalization-cms's build_tables.py. Sharded
    tables are written internally via DuckDB's native PARTITION_BY (single-pass, avoids one
    filtered re-scan per shard value) but then flattened back to one file per shard directly
    in the output directory, so this stays a flat filename either way. `aggregated` applies the
    same `_array` suffix in both cases — aggregated sharded tables (e.g. eligibility) get it via
    write_sharded_parquet's `suffix=` param in normalizecms/build_tables.py, exactly like
    non-sharded aggregated tables do:
      sharded + aggregated:         {table}_{shard}_array.parquet
      sharded + not aggregated:     {table}_{shard}.parquet
      non-sharded + aggregated:     {table}_{year_min}_{year_max}_array.parquet
      non-sharded + not aggregated: {table}_{year_min}_{year_max}.parquet
    """
    suffix = "_array" if aggregated else ""
    if shard is not None:
        return f"{table}_{shard}{suffix}.parquet"
    return f"{table}_{year_min}_{year_max}{suffix}.parquet"


def build_intermediate_filename(
    table: str,
    year_min: int,
    year_max: int,
    stage: str,
    shard: str | None = None,
) -> str:
    """
    Intermediate file naming, consistent with input convention (minus aggregated suffix):
      sharded:     {table}_{shard}_{stage}.parquet
      non-sharded: {table}_{year_min}_{year_max}_{stage}.parquet
    """
    if shard is not None:
        return f"{table}_{shard}_{stage}.parquet"
    return f"{table}_{year_min}_{year_max}_{stage}.parquet"


def build_output_filename(
    table: str,
    year_min: int,
    year_max: int,
    shard: str | None = None,
) -> str:
    if shard is not None:
        return f"{table}_{shard}_clean.parquet"
    return f"{table}_{year_min}_{year_max}_clean.parquet"


def build_stage_dirs(datapaths_name: str) -> dict[str, str]:
    """
    Derive the three stage directories from a datapaths config's `name`, matching
    the local folders create_dir_paths.py sets up under data/{name}/: input/,
    intermediate/, cleaned/. Single source of truth so conf/config.yaml doesn't
    need to repeat these paths alongside conf/datapaths/{name}.yaml.
    """
    root = f"data/{datapaths_name}"
    return {
        "input_dir": f"{root}/input",
        "intermediate_dir": f"{root}/intermediate",
        "output_dir": f"{root}/cleaned",
    }


def load_run_context(cfg) -> dict:
    """
    Extract runtime parameters from Hydra config and load the table cleaning YAML.

    `mode` (array|flat) is the single source of truth for whether a table's input
    is an aggregated _array.parquet from build_tables.py or an already-flat parquet.
    Cleaning configs are namespaced by dataset: conf/cleaning/{dataset}/{table}.yaml.

    Returns a dict with keys:
        table, year_min, year_max, shard, aggregated, dry_run, cleaning_config,
        input_dir, intermediate_dir, output_dir
    """
    table    = cfg.table
    year_min = cfg.year_min
    year_max = cfg.year_max
    shard    = cfg.get("shard", None)
    dry_run  = cfg.get("dry_run", False)
    dataset  = cfg.dataset

    cleaning_config_path = Path("conf/cleaning") / dataset / f"{table}.yaml"
    if not cleaning_config_path.exists():
        raise FileNotFoundError(
            f"No cleaning config found for table '{table}' in dataset '{dataset}' "
            f"at {cleaning_config_path}"
        )
    with open(cleaning_config_path) as f:
        cleaning_config = yaml.safe_load(f)

    mode = cleaning_config.get("mode")
    if mode not in ("array", "flat"):
        raise ValueError(
            f"cleaning config for '{table}' must set mode: array|flat (got {mode!r})"
        )
    aggregated = mode == "array"

    stage_dirs = build_stage_dirs(cfg.datapaths.name)

    return {
        "table": table,
        "year_min": year_min,
        "year_max": year_max,
        "shard": shard,
        "aggregated": aggregated,
        "dry_run": dry_run,
        "cleaning_config": cleaning_config,
        **stage_dirs,
    }
