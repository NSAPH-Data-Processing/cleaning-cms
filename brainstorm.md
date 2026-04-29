# cleaning-cms

A configurable post-normalization cleaning step for CMS-style administrative data.

---

## Where it fits

```text
raw parquet
    -> harmonize
harmonized parquet
    -> normalize / select_variables
per-year column-selected parquet
    -> normalize / build_tables
{table}_{year_min}_{year_max}.parquet
{table}_{year_min}_{year_max}_array.parquet
    -> clean-cms
{table}_{year_min}_{year_max}_clean.parquet
    -> materialize
analysis-ready tables
```

`clean-cms` sits between table-building and materialization. It takes the `_array.parquet` output from `build_tables` and writes a flat `_clean.parquet` with one row per primary key. Downstream steps can assume that row-level conflicts have already been resolved.

The cleaning step should also address broader data problems that may first be surfaced by QC in `normalization-cms`: not just how to collapse arrays, but also how to handle across-year inconsistencies, row-level anomalies, and within-row logical checks.

---

## What this step is responsible for

The cleaning step has two related jobs:

1. **Resolve array-valued columns into one value per primary key.**
2. **Apply explicit cleaning rules for data problems identified upstream,** especially when those problems are first surfaced by QC in `normalization-cms`.

In other words, normalization can report where inconsistencies or implausible patterns exist, and `clean-cms` is where the pipeline decides what to do about them.

---

## Common conflict patterns

In the normalization step, `build_tables` groups records by primary key and stores non-key columns as arrays. It also emits an `n_distinct_{col}` indicator for each aggregated column so that conflicts are visible.

| values               | `n_distinct_*` | interpretation                    |
| -------------------- | -------------: | --------------------------------- |
| `[2, 2, 2]`          |            `1` | stable value                      |
| `[1, 2, 2]`          |            `2` | conflicting recorded values       |
| `[null, 2, null]`    |            `1` | nulls ignored; effectively stable |
| `[null, null, null]` |            `0` | always missing                    |

Conflicts are expected in longitudinal administrative data. Some reflect real change over time, such as address moves or changing enrollment status. Others reflect inconsistent coding, partial missingness, or system noise.

Beyond simple array conflicts, the cleaning step may also need to address:

* **Across years:** a variable changes over time for the same entity.
* **Across rows:** multiple records for the same entity disagree or overlap.
* **Across columns:** values within a row violate a logical rule.

---

## Two approaches

### Approach A: strategy registry

Configuration assigns each variable or check a named strategy such as `mode`, `min`, `most_recent`, or `no_overlap_check`. Python maps those names to DuckDB SQL expressions or validation routines and builds the final query or QC step.

This keeps the config readable and centralizes logic in Python.

### Approach D: SQL templates

Configuration stores DuckDB SQL directly. Python assembles and runs it with little or no abstraction.

This gives maximum flexibility, especially when rules are table-specific, depend on several variables at once, or need multi-step validation.

### Practical middle ground

A hybrid is the most natural starting point: use Approach A by default, with a per-column or per-check `sql:` escape hatch for the cases the registry cannot express cleanly.

---

## Problem types that cleaning should address
These are some categories of issues that may be detected during normalization and then need an explicit handling rule in cleaning.

### A. Across years

These checks show whether the same entity is coded consistently over time.

Examples:

* proportion of entities with changing sex across years
* proportion with changing race across years
* proportion with changing HMO indicator across years
* magnitude of discrepancies for each variable across years
* enrollment continuity checks across years

In `clean-cms`, these could be handled like:

* choose the most recent value
* choose the modal value
* treat some codes as missing and prefer a non-missing value
* null out the value when disagreement is too severe
* carry a flag forward for downstream filtering or sensitivity analysis

### B. Collapsed array columns

These checks show how much disagreement remains inside array-collapsed values after aggregation.

Examples:

* count and proportion of records with `n_distinct_* > 1`
* summarize how many distinct values appear per variable
* identify columns with the highest conflict burden

In `clean-cms`, these indicators become inputs to cleaning rules. For example, a variable might use `mode`, `most_recent`, or `prefer_nonmissing`, or it might use `require_no_conflict` so that conflicting values are set to null instead of forced into a single answer.

### C. Across rows

These checks show whether multiple rows for the same entity conflict with each other.

Examples:

* overlapping enrollment periods for the same entity
* duplicate admissions
* multiple records for the same entity and same effective date
* gaps or overlaps in coverage periods
* inconsistent death/enrollment relationships across rows

In `clean-cms`, these issues need a handling rule: collapse duplicate rows, choose a preferred row, null impossible values, or exclude records that violate a required condition.

### D. Across columns

These checks show whether values within a single row satisfy basic logical rules.

Examples:

* discharge date >= admission date
* enrollment end date >= enrollment start date
* date of death >= enrollment start date
* indicator combinations that should not co-occur

In `clean-cms`, these become row-level cleaning rules. Some can be handled with simple conditional logic; others are the strongest argument for custom SQL or multi-step validation because they depend on relationships between multiple resolved columns.

---

## How each approach handles these problem types

### Across years

* **Approach A** fits well when the response can be expressed as a reusable rule such as `mode`, `most_recent`, `prefer_nonmissing`, or `require_no_conflict`.
* **D1** also works well when the logic is still column-by-column but easier to express directly in SQL.
* **D2** is usually unnecessary unless the rule depends on several intermediate steps or on combining multiple resolved values.

### Collapsed array columns

* **Approach A** is the most natural fit because it already works from array-valued columns plus `n_distinct_*` indicators.
* **D1** can do the same work, but with more SQL repeated in config.
* **D2** is usually more power than needed unless collapsed-array handling is part of a larger validation workflow.

### Across rows

* **Approach A** works only when the rule can be standardized into a reusable handler.
* **D1** can help for simpler row-selection or nulling logic, but it is still awkward when the check depends on joins, ordering, or multiple passes.
* **D2** is usually the best fit when row relationships matter, because CTEs make it easier to express overlap checks, duplicate handling, and period logic.

### Across columns

* **Approach A** can cover simple reusable checks, especially if the response is straightforward and repeated across tables.
* **D1** works well for single-pass conditional logic inside one `SELECT`.
* **D2** is the strongest option when the rule depends on resolved values from multiple groups of columns or benefits from a multi-step validation flow.

In practice, this points to a simple division of labor: use **Approach A** for standard array resolution and common cleaning rules, use **D1** for one-off column logic, and use **D2** when the cleaning rule is fundamentally relational or multi-step.

---

## Approach A: strategy registry

### High-level idea

A registry maps strategy names to SQL expression builders or cleaning-rule handlers. The config assigns one strategy or check per variable, optionally with parameters. At runtime, Python inspects the schema of the `_array.parquet`, looks up each variable’s strategy, and generates a DuckDB query plus any configured cleaning logic tied to reported problem types.

### Why it is attractive

* Config stays readable for collaborators who do not want to write SQL.
* Common logic lives in one place.
* New columns can be caught automatically if no strategy is defined.
* Common responses to reported data problems can be standardized across tables.
* Misconfiguration usually fails early, before DuckDB runs the full query.

### Config shape

```yaml
tables:
  beneficiaries:
    primary_key: [bene_id]
    default_strategy: null

    variables:
      bene_dob:
        strategy: min
      sex_cd:
        strategy: prefer_nonmissing
        missing_codes: [0]
      state_cd:
        strategy: most_recent
      dual_elgbl_cd:
        strategy: ever_nonzero

    cleaning_rules:
      across_years:
        sex_cd:
          action: prefer_nonmissing
        race_cd:
          action: mode
      across_rows:
        enrollment_periods:
          rule: no_overlap
      across_columns:
        death_after_start:
          rule: date_order
          left: death_dt
          right: enrollment_start_dt
```

The exact config schema can stay lightweight. The important point is that cleaning config can cover both value-resolution rules and explicit responses to problems identified upstream.

### Registry pattern

```python
def strategy_mode(col: str, **_) -> str:
    return f"list_mode(list_filter({col}, x -> x IS NOT NULL))"


def strategy_most_recent(col: str, **_) -> str:
    return f"list_filter({col}, x -> x IS NOT NULL)[-1]"


STRATEGY_REGISTRY = {
    "mode": strategy_mode,
    "most_recent": strategy_most_recent,
}
```

A similar registry can be used for reusable cleaning rules:

```python
RULE_REGISTRY = {
    "no_overlap": build_no_overlap_check,
    "date_order": build_date_order_check,
}
```

### Query and QC assembly

```python
for col in data_cols:
    expr = build_expression(col, variables[col], n_distinct_col=f"n_distinct_{col}")
    select_exprs.append(f"({expr}) AS {col}")

for rule_name, rule_conf in cleaning_rules.items():
    rule_queries.append(build_cleaning_rule(rule_conf))
```

Conceptually:

```text
inspect schema
    -> choose resolution strategy for each variable
    -> build standard cleaning rules from config
    -> run cleaning query
    -> write cleaned output
```

### Escape hatch

A variable or check can use raw SQL instead of a named strategy:

```yaml
elgblty_grp_cd:
  sql: "list_mode(list_filter(elgblty_grp_cd, x -> x IS NOT NULL AND x NOT IN (0, 99)))"
```

This preserves the overall Approach A structure while allowing one-off custom logic.

### Main caveats

* `most_recent` depends on arrays being ordered consistently upstream.
* The registry must grow when new reusable logic is needed.
* Complex across-row and across-column checks can become awkward if everything is forced into named handlers.

---

## Approach D: SQL templates

### High-level idea

Instead of strategy names, the config contains DuckDB SQL directly. Python either fills in one SQL expression per column or executes a complete query template.

This comes in two forms:

* **D1: per-column SQL** — each variable maps to a DuckDB expression.
* **D2: full-query SQL** — the table config points to a full SQL query, usually in an external `.sql` file.

### Why it is attractive

* No abstraction layer to maintain.
* Very flexible for custom logic.
* Naturally supports cross-column and multi-step validation.
* Easy to dry-run by printing the generated SQL.
* A natural fit for across-row and across-column cleaning logic.

### D1: per-column SQL

```yaml
tables:
  beneficiaries:
    primary_key: [bene_id]
    mode: per_column
    variables:
      bene_dob: "list_min({col})"
      sex_cd: |
        COALESCE(
          list_mode(list_filter({col}, x -> x IS NOT NULL AND x NOT IN (0))),
          list_mode(list_filter({col}, x -> x IS NOT NULL))
        )
      state_cd: "list_filter({col}, x -> x IS NOT NULL)[-1]"
```

Pseudocode:

```text
for each column:
    read SQL template from config
    substitute {col}
    add expression to SELECT
run query
```

This works well for array resolution and for simple rule-based nulling or flagging.

### D2: full-query SQL

For more complex validation, the config can point to an external SQL file:

```yaml
tables:
  beneficiaries:
    mode: full_query
    sql_file: "sql/beneficiaries_clean.sql"
```

And the SQL can use CTEs:

```sql
WITH resolved_demo AS (...),
     resolved_enroll AS (...),
     validated AS (...)
SELECT * FROM validated
```

This is the cleanest option when logic depends on resolved values from multiple groups of columns or when cleaning rules need joins across grouped rows.

### Main caveats

* Inline SQL in YAML is harder to read and maintain.
* Errors often surface later, at DuckDB parse or execution time.
* Schema coverage is manual unless extra checks are added.
* D2 is best kept in external `.sql` files rather than embedded YAML.

---

## Comparison

|                               | A: strategy registry             | D1: per-column SQL        | D2: full-query SQL               |
| ----------------------------- | -------------------------------- | ------------------------- | -------------------------------- |
| Config readability            | high                             | medium                    | medium to high with `.sql` files |
| Common logic reuse            | high                             | low                       | low                              |
| Flexibility                   | medium                           | high                      | very high                        |
| Across-years cleaning rules   | good                             | good                      | very good                        |
| Across-rows cleaning rules    | limited to standardized handlers | moderate                  | very good                        |
| Across-columns cleaning rules | moderate                         | good                      | very good                        |
| Multi-step / CTE logic        | no                               | no                        | yes                              |
| Schema coverage checks        | strong                           | manual                    | manual                           |
| Failure mode                  | early Python/config errors       | SQL-time errors           | SQL-time errors                  |
| Best fit                      | standard repeated cleaning rules | custom per-variable logic | complex validation workflows     |

---

## Recommendation

Start with **Approach A** as the default design.

It keeps the config easy to read, matches the existing Hydra plus DuckDB pattern, and makes it easier to detect missing or newly introduced columns. It is also a natural place to standardize common responses to reported data issues, especially for array conflicts and straightforward across-year inconsistencies.

Use the `sql:` escape hatch for a small number of exceptional variables or checks.

Move to **D2** only when across-row or across-column cleaning logic becomes complex enough that a registry starts to feel forced. That is the point where explicit SQL and CTEs become easier to reason about than a growing collection of special-case handlers.

---

## Operational notes

### Configuration handling

Hydra should manage the runner config, but the table-cleaning YAML should be loaded with `yaml.safe_load` rather than as a Hydra config group. That avoids interpolation and type-coercion issues when raw SQL strings are present.

### Minimal testing plan

1. Unit tests for strategy functions.
2. One integration test with a small synthetic `_array.parquet` containing deliberate conflicts.
3. A schema coverage check for Approach A.
4. SQL validation via `EXPLAIN` for Approach D.
5. Small fixture-based tests for across-row and across-column cleaning rules.

---

## Summary

`clean-cms` sits between normalization and downstream materialization. Its job is not only to turn grouped array-valued columns into one clean row per primary key, but also to apply explicit rules for data problems that were identified upstream during normalization.

The core design choice is whether those rules should live primarily in a **Python strategy registry** or directly in **DuckDB SQL templates**. Approach A is the better default for a maintainable first implementation, while Approach D is the better fit for tables that need more complex cleaning logic across years, rows, or columns.
