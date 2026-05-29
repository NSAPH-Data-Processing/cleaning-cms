# src/clean_cms/resolver.py
#
# Single responsibility: given a column and its config, return the right
# SQL expression. Talks to registry.py and rules.py but knows nothing
# about DuckDB connections, files, or query assembly.

from __future__ import annotations
import logging

from .registry import build_expression
from .rules import build_across_years_expression

logger = logging.getLogger(__name__)


def resolve_column(
    col: str,
    var_conf: dict,
    across_years_conf: dict,
    n_distinct_col: str | None,
) -> str:
    """
    Return a SQL expression for a single column.

    Priority order:
      1. across_years rule (overrides default strategy when present)
      2. explicit variable config (strategy or sql:)
      3. raises ValueError if neither exists

    Args:
        col:               Column name.
        var_conf:          Variable config dict from cleaning YAML, or {}.
        across_years_conf: Dict of col -> across_years rule config.
        n_distinct_col:    Name of n_distinct companion column, if available.

    Returns:
        A DuckDB SQL expression string.
    """
    if col in across_years_conf:
        logger.debug("Column '%s': using across_years rule", col)
        return build_across_years_expression(col, across_years_conf[col])

    if var_conf:
        return build_expression(col, var_conf, n_distinct_col=n_distinct_col)

    raise ValueError(
        f"Column '{col}' has no strategy and no across_years rule. "
        f"Add an explicit entry to the cleaning config or set default_strategy."
    )


def resolve_all(
    schema: list[dict],
    primary_key: list[str],
    variables_conf: dict,
    across_years_conf: dict,
    default_strategy: str | None,
) -> dict[str, str]:
    """
    Resolve SQL expressions for every array column in the schema.

    Returns:
        Dict mapping column_name -> SQL expression string.
        Only includes array-valued columns (not primary key or n_distinct_*).
    """
    from .utils import is_array_type  # avoid circular at module level

    resolved: dict[str, str] = {}

    for col_info in schema:
        col = col_info["name"]

        # skip primary key columns — passed through as-is
        if col in primary_key:
            continue

        # skip n_distinct_* companion columns — not output columns
        if col.startswith("n_distinct_"):
            continue

        # skip non-array columns — passed through as-is
        if not is_array_type(col_info["type"]):
            continue

        n_distinct_col = f"n_distinct_{col}"
        has_n_distinct = any(c["name"] == n_distinct_col for c in schema)

        # build var_conf: explicit entry, or default strategy, or empty
        if col in variables_conf:
            var_conf = variables_conf[col]
        elif default_strategy is not None:
            logger.debug(
                "Column '%s': no explicit config, using default_strategy='%s'",
                col, default_strategy,
            )
            var_conf = {"strategy": default_strategy}
        else:
            var_conf = {}

        resolved[col] = resolve_column(
            col=col,
            var_conf=var_conf,
            across_years_conf=across_years_conf,
            n_distinct_col=n_distinct_col if has_n_distinct else None,
        )

    logger.debug("Resolved %d array columns", len(resolved))
    return resolved
