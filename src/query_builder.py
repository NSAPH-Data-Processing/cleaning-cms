# src/clean_cms/query_builder.py
#
# Single responsibility: take resolved column expressions and assemble
# them into a complete DuckDB SQL string. Knows SQL structure but does
# not run anything or touch files.

from __future__ import annotations
import logging

from .rules import build_across_columns_rewrites

logger = logging.getLogger(__name__)


def build_resolution_select(
    table_name: str,
    primary_key: list[str],
    resolved_expressions: dict[str, str],
    passthrough_cols: list[str],
) -> str:
    """
    Build the main SELECT that resolves all array columns into scalar values.

    Args:
        table_name:           DuckDB table/view name for the input.
        primary_key:          List of primary key column names (passed through as-is).
        resolved_expressions: Dict of col -> SQL expression from resolver.py.
        passthrough_cols:     Non-array, non-pk, non-n_distinct columns to pass through.

    Returns:
        A DuckDB SELECT string.
    """
    select_exprs = []

    # primary key columns: pass through as-is
    select_exprs.extend(primary_key)

    # resolved array columns
    for col, expr in resolved_expressions.items():
        select_exprs.append(f"({expr}) AS {col}")

    # non-array columns: pass through as-is
    select_exprs.extend(passthrough_cols)

    exprs_sql = ",\n    ".join(select_exprs)
    sql = f"SELECT\n    {exprs_sql}\nFROM {table_name}"

    logger.debug("Built resolution SELECT with %d expressions", len(select_exprs))
    return sql


def wrap_across_columns(
    resolution_sql: str,
    across_columns_conf: dict,
) -> str:
    """
    Wrap the resolution SELECT in an outer SELECT that applies
    across_columns rewrites (e.g. null out death_dt if before bene_dob).

    If no across_columns rules are configured, returns the original SQL unchanged.

    Args:
        resolution_sql:      The inner SELECT from build_resolution_select().
        across_columns_conf: Dict of rule_name -> rule config from cleaning YAML.

    Returns:
        A DuckDB SQL string (possibly wrapped in an outer SELECT).
    """
    if not across_columns_conf:
        return resolution_sql

    rewrites = build_across_columns_rewrites(across_columns_conf)
    if not rewrites:
        return resolution_sql

    replace_clause = ", ".join(
        f"({expr}) AS {col}" for col, expr in rewrites.items()
    )
    wrapped = (
        f"SELECT * REPLACE ({replace_clause})\n"
        f"FROM (\n    {resolution_sql}\n)"
    )

    logger.debug(
        "Wrapped with across_columns rewrites for: %s", list(rewrites)
    )
    return wrapped


def build_final_query(
    table_name: str,
    primary_key: list[str],
    resolved_expressions: dict[str, str],
    passthrough_cols: list[str],
    across_columns_conf: dict,
) -> str:
    """
    Full pipeline: resolution SELECT → across_columns wrap → final SQL.

    This is the main entry point for query_builder. It composes
    build_resolution_select and wrap_across_columns in order.

    Returns:
        The final DuckDB SQL string ready to execute.
    """
    resolution_sql = build_resolution_select(
        table_name=table_name,
        primary_key=primary_key,
        resolved_expressions=resolved_expressions,
        passthrough_cols=passthrough_cols,
    )

    final_sql = wrap_across_columns(
        resolution_sql=resolution_sql,
        across_columns_conf=across_columns_conf,
    )

    return final_sql


def get_passthrough_cols(
    schema: list[dict],
    primary_key: list[str],
    is_array_type_fn,
) -> list[str]:
    """
    Identify columns that should be passed through without transformation:
    not primary key, not array-valued, not n_distinct_* companions.
    """
    return [
        col["name"] for col in schema
        if col["name"] not in primary_key
        and not col["name"].startswith("n_distinct_")
        and not is_array_type_fn(col["type"])
    ]
