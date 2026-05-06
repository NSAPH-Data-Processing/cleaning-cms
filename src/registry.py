# src/clean_cms/registry.py
#
# Strategy registry for Approach A.
# Each strategy function takes a column name (and optional kwargs from config)
# and returns a DuckDB SQL expression string.
#
# The expression operates on an array-valued column produced by build_tables,
# alongside its companion n_distinct_{col} indicator.

from __future__ import annotations
import logging

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Strategy functions
# Each returns a DuckDB SQL expression string for use inside a SELECT clause.
# ---------------------------------------------------------------------------

def strategy_mode(col: str, **_) -> str:
    """Most frequent non-null value in the array."""
    return (
        f"list_mode(list_filter({col}, x -> x IS NOT NULL))"
    )


def strategy_min(col: str, **_) -> str:
    """Smallest non-null value in the array."""
    return (
        f"list_min(list_filter({col}, x -> x IS NOT NULL))"
    )


def strategy_max(col: str, **_) -> str:
    """Largest non-null value in the array."""
    return (
        f"list_max(list_filter({col}, x -> x IS NOT NULL))"
    )


def strategy_most_recent(col: str, **_) -> str:
    """Last non-null value in the array (relies on time-ordered arrays upstream)."""
    return (
        f"list_filter({col}, x -> x IS NOT NULL)[-1]"
    )


def strategy_prefer_nonmissing(col: str, missing_codes: list | None = None, **_) -> str:
    """
    First non-null value that is also not in missing_codes.
    Falls back to mode of all non-null values if every entry is a missing code.
    Falls back to NULL if all entries are null.
    """
    if missing_codes:
        codes_sql = ", ".join(str(c) for c in missing_codes)
        filtered = (
            f"list_filter({col}, x -> x IS NOT NULL AND x NOT IN ({codes_sql}))"
        )
    else:
        filtered = f"list_filter({col}, x -> x IS NOT NULL)"

    fallback = f"list_mode(list_filter({col}, x -> x IS NOT NULL))"
    return f"COALESCE({filtered}[1], {fallback})"


def strategy_ever_nonzero(col: str, **_) -> str:
    """
    Returns 1 if any non-null value in the array is nonzero, else 0.
    Useful for ever-enrolled / ever-eligible indicators.
    """
    return (
        f"CASE WHEN list_count(list_filter({col}, x -> x IS NOT NULL AND x != 0)) > 0 "
        f"THEN 1 ELSE 0 END"
    )


def strategy_require_stable(col: str, n_distinct_col: str | None = None, **_) -> str:
    """
    Returns the single value if it is stable (n_distinct == 1), else NULL.
    Requires the n_distinct_{col} companion column to be present.
    """
    if n_distinct_col is None:
        raise ValueError(
            f"require_stable strategy for '{col}' needs n_distinct_{col} column"
        )
    return (
        f"CASE WHEN {n_distinct_col} <= 1 "
        f"THEN list_filter({col}, x -> x IS NOT NULL)[1] "
        f"ELSE NULL END"
    )


# ---------------------------------------------------------------------------
# Registry
# ---------------------------------------------------------------------------

STRATEGY_REGISTRY: dict[str, callable] = {
    "mode": strategy_mode,
    "min": strategy_min,
    "max": strategy_max,
    "most_recent": strategy_most_recent,
    "prefer_nonmissing": strategy_prefer_nonmissing,
    "ever_nonzero": strategy_ever_nonzero,
    "require_stable": strategy_require_stable,
}


def build_expression(
    col: str,
    var_conf: dict,
    n_distinct_col: str | None = None,
) -> str:
    """
    Build a DuckDB SELECT expression for a single column.

    Checks for a raw `sql:` override first, then falls back to the registry.

    Args:
        col:            Column name (array-valued).
        var_conf:       Variable config dict from the cleaning YAML.
        n_distinct_col: Name of the n_distinct companion column, if available.

    Returns:
        A DuckDB SQL expression string.
    """
    # sql: escape hatch — raw expression provided directly in config
    if "sql" in var_conf:
        logger.debug("Column '%s': using raw sql override", col)
        return var_conf["sql"]

    strategy_name = var_conf.get("strategy")
    if strategy_name is None:
        raise ValueError(
            f"Column '{col}' has no strategy and no sql override in cleaning config."
        )

    if strategy_name not in STRATEGY_REGISTRY:
        raise ValueError(
            f"Unknown strategy '{strategy_name}' for column '{col}'. "
            f"Available: {sorted(STRATEGY_REGISTRY)}"
        )

    fn = STRATEGY_REGISTRY[strategy_name]
    kwargs = {k: v for k, v in var_conf.items() if k != "strategy"}
    if n_distinct_col:
        kwargs["n_distinct_col"] = n_distinct_col

    logger.debug("Column '%s': strategy=%s kwargs=%s", col, strategy_name, kwargs)
    return fn(col, **kwargs)
