# src/clean_cms/cleaner.py
#
# Orchestrator for the cleaning step.
# Contains NO logic — only calls other modules in order.
#
# Flow:
#   1. executor      → load parquet, get schema
#   2. utils         → check schema coverage
#   3. resolver      → decide SQL expression per column
#   4. query_builder → assemble final SQL
#   5. executor      → run query, write output

from __future__ import annotations
import logging
from pathlib import Path

import duckdb
import yaml

from . import utils, resolver, query_builder, executor

logger = logging.getLogger(__name__)


def load_cleaning_config(config_path: str | Path) -> dict:
    """Load table cleaning config from YAML using safe_load."""
    with open(config_path) as f:
        return yaml.safe_load(f)


def is_array_type(dtype: str) -> bool:
    """Heuristic: DuckDB LIST types contain '[]' or start with 'LIST'."""
    dtype_upper = dtype.upper()
    return "[]" in dtype_upper or dtype_upper.startswith("LIST")


def resolve_table(
    input_path: str | Path,
    output_path: str | Path,
    cleaning_config: dict,
    dry_run: bool = False,
) -> None:
    """
    Stage 1: resolve array columns to scalar values.

    Applies array-resolution strategies and across-years rules only.
    Across-columns rules are NOT applied here — that is Stage 2.
    """
    input_path = Path(input_path)

    primary_key      = cleaning_config["primary_key"]
    variables_conf   = cleaning_config.get("variables", {})
    default_strategy = cleaning_config.get("default_strategy")
    across_years_conf = cleaning_config.get("cleaning_rules", {}).get("across_years", {})

    con = duckdb.connect()
    schema = executor.load_source(con, input_path)
    con.close()

    utils.check_schema_coverage(schema, primary_key, variables_conf, default_strategy)

    resolved = resolver.resolve_all(
        schema=schema,
        primary_key=primary_key,
        variables_conf=variables_conf,
        across_years_conf=across_years_conf,
        default_strategy=default_strategy,
    )

    passthrough = query_builder.get_passthrough_cols(schema, primary_key, is_array_type)

    sql = query_builder.build_resolution_select(
        table_name="source",
        primary_key=primary_key,
        resolved_expressions=resolved,
        passthrough_cols=passthrough,
    )

    executor.run(input_path=input_path, output_path=output_path, sql=sql, dry_run=dry_run)


def apply_rules(
    input_path: str | Path,
    output_path: str | Path,
    cleaning_config: dict,
    dry_run: bool = False,
) -> None:
    """
    Stage 2: apply across-columns rules to the already-flat input.

    Works on both array-resolved intermediate files and flat-mode direct inputs.
    Wraps SELECT * FROM source with a REPLACE clause for each rule rewrite.
    """
    across_cols_conf = cleaning_config.get("cleaning_rules", {}).get("across_columns", {})
    base_sql = "SELECT * FROM source"
    sql = query_builder.wrap_across_columns(base_sql, across_cols_conf)

    executor.run(input_path=input_path, output_path=output_path, sql=sql, dry_run=dry_run)


def apply_entity_checks(
    input_path: str | Path,
    output_path: str | Path,
    cleaning_config: dict,
    dry_run: bool = False,
) -> None:
    """
    Stage 3: entity-level consistency checks (passthrough pending implementation).
    """
    sql = "SELECT * FROM source"
    executor.run(input_path=input_path, output_path=output_path, sql=sql, dry_run=dry_run)


def clean_table(
    input_path: str | Path,
    output_path: str | Path,
    cleaning_config: dict,
    dry_run: bool = False,
) -> None:
    """
    Orchestrate the full cleaning pipeline for one table.

    Args:
        input_path:      Path to the _array.parquet file.
        output_path:     Path to write the _clean.parquet file.
        cleaning_config: Loaded cleaning YAML as a dict.
        dry_run:         If True, validate SQL but do not write output.
    """
    input_path = Path(input_path)

    # --- unpack config ---
    primary_key       = cleaning_config["primary_key"]
    variables_conf    = cleaning_config.get("variables", {})
    default_strategy  = cleaning_config.get("default_strategy")
    cleaning_rules    = cleaning_config.get("cleaning_rules", {})
    across_years_conf = cleaning_rules.get("across_years", {})
    across_cols_conf  = cleaning_rules.get("across_columns", {})

    # 1. load source parquet, get schema
    con = duckdb.connect()
    schema = executor.load_source(con, input_path)
    con.close()

    # 2. check every array column has a strategy
    utils.check_schema_coverage(
        schema, primary_key, variables_conf, default_strategy
    )

    # 3. resolve SQL expression for each array column
    resolved = resolver.resolve_all(
        schema=schema,
        primary_key=primary_key,
        variables_conf=variables_conf,
        across_years_conf=across_years_conf,
        default_strategy=default_strategy,
    )

    # 4. identify passthrough columns (non-array, non-pk, non-n_distinct)
    passthrough = query_builder.get_passthrough_cols(
        schema, primary_key, is_array_type
    )

    # 5. assemble final SQL
    sql = query_builder.build_final_query(
        table_name="source",
        primary_key=primary_key,
        resolved_expressions=resolved,
        passthrough_cols=passthrough,
        across_columns_conf=across_cols_conf,
    )

    # 6. run query and write output
    executor.run(
        input_path=input_path,
        output_path=output_path,
        sql=sql,
        dry_run=dry_run,
    )
