# src/clean_cms/cleaner.py
#
# Orchestrator for the cleaning step.
# Contains NO logic — only calls other modules in order.
#
# Flow (monolithic):
#   1. executor      → load parquet, get schema
#   2. utils         → check schema coverage
#   3. resolver      → decide SQL expression per column
#   4. query_builder → assemble final SQL
#   5. executor      → run query, write output
#
# Two-stage pipeline equivalents:
#   resolve_table    → stages 1-4, resolution only (no across_columns wrap)
#   apply_rules      → across_columns rules on the resolved intermediate; writes
#                       the final _clean.parquet

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


def clean_table(
    input_path: str | Path,
    output_path: str | Path,
    cleaning_config: dict,
    dry_run: bool = False,
) -> None:
    """
    Orchestrate the full cleaning pipeline for one table in a single pass.

    Args:
        input_path:      Path to the input parquet file (_array or flat).
        output_path:     Path to write the _clean.parquet file.
        cleaning_config: Loaded cleaning YAML as a dict.
        dry_run:         If True, validate SQL but do not write output.
    """
    input_path = Path(input_path)

    primary_key       = cleaning_config.get("primary_key", [])
    variables_conf    = cleaning_config.get("variables", {})
    default_strategy  = cleaning_config.get("default_strategy")
    cleaning_rules    = cleaning_config.get("cleaning_rules", {})
    across_years_conf = cleaning_rules.get("across_years", {})
    across_cols_conf  = cleaning_rules.get("across_columns", {})

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

    sql = query_builder.build_final_query(
        table_name="source",
        primary_key=primary_key,
        resolved_expressions=resolved,
        passthrough_cols=passthrough,
        across_columns_conf=across_cols_conf,
    )

    executor.run(
        input_path=input_path,
        output_path=output_path,
        sql=sql,
        dry_run=dry_run,
    )


def resolve_table(
    input_path: str | Path,
    output_path: str | Path,
    cleaning_config: dict,
    dry_run: bool = False,
) -> None:
    """
    Stage 1: resolve array columns to scalar values, write _resolved.parquet.

    Applies per-column strategies and across_years rules. Does NOT apply
    across_columns rules — that is Stage 2 (apply_rules).
    For flat (non-aggregated) tables there are no array columns, so all columns
    are passed through and this stage is a clean copy.
    """
    input_path = Path(input_path)

    primary_key       = cleaning_config.get("primary_key", [])
    variables_conf    = cleaning_config.get("variables", {})
    default_strategy  = cleaning_config.get("default_strategy")
    cleaning_rules    = cleaning_config.get("cleaning_rules", {})
    across_years_conf = cleaning_rules.get("across_years", {})

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

    executor.run(
        input_path=input_path,
        output_path=output_path,
        sql=sql,
        dry_run=dry_run,
    )


def apply_rules(
    input_path: str | Path,
    output_path: str | Path,
    cleaning_config: dict,
    dry_run: bool = False,
) -> None:
    """
    Stage 2 (final): apply across_columns cleaning rules to an already-flat table.

    Reads _resolved.parquet, rewrites columns per across_columns rules
    (e.g. date ordering checks), and writes the final _clean.parquet.
    If no across_columns rules are configured, the data passes through unchanged.
    """
    cleaning_rules   = cleaning_config.get("cleaning_rules", {})
    across_cols_conf = cleaning_rules.get("across_columns", {})

    sql = query_builder.wrap_across_columns("SELECT * FROM source", across_cols_conf)

    executor.run(
        input_path=input_path,
        output_path=output_path,
        sql=sql,
        dry_run=dry_run,
    )
