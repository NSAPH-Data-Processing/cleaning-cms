# src/clean_cms/rules.py
#
# Handlers for explicit cleaning rules defined in the `cleaning_rules:` section
# of the table config. These run after the main array-resolution SELECT.
#
# Each handler returns a DuckDB SQL fragment or a full UPDATE/CREATE statement
# that can be applied to the resolved (flat) intermediate table.

from __future__ import annotations
import logging

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Across-years rule handlers
#
# These operate on the array columns BEFORE resolution, so they can
# influence how the final value is chosen. They return a SELECT expression
# just like registry strategies — they are applied during the main query.
# ---------------------------------------------------------------------------

def across_years_prefer_nonmissing(
    col: str,
    missing_codes: list | None = None,
    **_,
) -> str:
    """
    Across-year variant of prefer_nonmissing.
    If a column changes across years, pick the most recent non-missing code.
    """
    if missing_codes:
        codes_sql = ", ".join(str(c) for c in missing_codes)
        filtered = (
            f"list_filter({col}, x -> x IS NOT NULL AND x NOT IN ({codes_sql}))"
        )
    else:
        filtered = f"list_filter({col}, x -> x IS NOT NULL)"
    fallback = f"list_mode(list_filter({col}, x -> x IS NOT NULL))"
    return f"COALESCE({filtered}[-1], {fallback})"


def across_years_mode(col: str, **_) -> str:
    """
    Across-year variant of mode.
    If a column changes across years, take the modal value.
    """
    return f"list_mode(list_filter({col}, x -> x IS NOT NULL))"


ACROSS_YEARS_REGISTRY: dict[str, callable] = {
    "prefer_nonmissing": across_years_prefer_nonmissing,
    "mode": across_years_mode,
}


def build_across_years_expression(col: str, rule_conf: dict) -> str:
    """
    Return a SELECT expression that applies an across-years action for `col`.
    These expressions replace the default strategy expression for that column
    when an across_years rule is defined.
    """
    action = rule_conf.get("action")
    if action not in ACROSS_YEARS_REGISTRY:
        raise ValueError(
            f"Unknown across_years action '{action}' for column '{col}'. "
            f"Available: {sorted(ACROSS_YEARS_REGISTRY)}"
        )
    kwargs = {k: v for k, v in rule_conf.items() if k != "action"}
    return ACROSS_YEARS_REGISTRY[action](col, **kwargs)


# ---------------------------------------------------------------------------
# Across-columns rule handlers
#
# These run as a second pass on the already-resolved flat table.
# They return SQL UPDATE expressions or CASE expressions for use in a
# follow-up SELECT that rewrites the resolved table.
# ---------------------------------------------------------------------------

def build_date_order_check(rule_conf: dict) -> dict:
    """
    Enforce that `earlier` column <= `later` column.
    On violation, apply the `on_violation` action.

    on_violation options:
        null_later   - set the `later` column to NULL
        null_earlier - set the `earlier` column to NULL
        null_both    - set both columns to NULL

    Returns a dict describing the rewrite expressions to inject into
    the post-resolution SELECT.
    """
    earlier = rule_conf["earlier"]
    later = rule_conf["later"]
    on_violation = rule_conf.get("on_violation", "null_later")

    violation_condition = f"{later} IS NOT NULL AND {earlier} IS NOT NULL AND {later} < {earlier}"

    rewrites = {}
    if on_violation == "null_later":
        rewrites[later] = f"CASE WHEN {violation_condition} THEN NULL ELSE {later} END"
    elif on_violation == "null_earlier":
        rewrites[earlier] = f"CASE WHEN {violation_condition} THEN NULL ELSE {earlier} END"
    elif on_violation == "null_both":
        rewrites[earlier] = f"CASE WHEN {violation_condition} THEN NULL ELSE {earlier} END"
        rewrites[later] = f"CASE WHEN {violation_condition} THEN NULL ELSE {later} END"
    else:
        raise ValueError(f"Unknown on_violation action: '{on_violation}'")

    logger.debug(
        "date_order check: %s <= %s, on_violation=%s, rewrites=%s",
        earlier, later, on_violation, rewrites,
    )
    return rewrites


ACROSS_COLUMNS_REGISTRY: dict[str, callable] = {
    "date_order": build_date_order_check,
}


def build_across_columns_rewrites(rules_conf: dict) -> dict[str, str]:
    """
    Process all across_columns rules and collect column rewrite expressions.
    Later rules override earlier ones for the same column (log a warning).

    Returns:
        A dict mapping column_name -> rewrite SQL expression.
    """
    all_rewrites: dict[str, str] = {}

    for rule_name, rule_conf in rules_conf.items():
        rule_type = rule_conf.get("rule")
        if rule_type not in ACROSS_COLUMNS_REGISTRY:
            raise ValueError(
                f"Unknown across_columns rule type '{rule_type}' in rule '{rule_name}'. "
                f"Available: {sorted(ACROSS_COLUMNS_REGISTRY)}"
            )
        rewrites = ACROSS_COLUMNS_REGISTRY[rule_type](rule_conf)
        for col, expr in rewrites.items():
            if col in all_rewrites:
                logger.warning(
                    "across_columns: column '%s' rewritten by multiple rules; "
                    "rule '%s' will override the previous expression.",
                    col, rule_name,
                )
            all_rewrites[col] = expr

    return all_rewrites
